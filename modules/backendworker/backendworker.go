package backendworker

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	backendscheduler_client "github.com/grafana/tempo/modules/backendscheduler/client"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/modules/storage"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding/common"
	vblockpack "github.com/grafana/tempo/tempodb/encoding/vblockpack"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
)

const (
	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 2

	// We use a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	ringNumTokens = 512

	backendWorkerRingKey = "backend-worker"

	// postgresJobReportTimeout bounds reportPostgresJobOutcome's final
	// Store.Fail/Store.Complete call (2026-07-14 fix). This call must run on
	// a FRESH context, not the job's own (possibly already-expired) ctx: if
	// RunCubeBackfill/RunViBackfill failed because that ctx's deadline was
	// exceeded, reusing the same expired ctx for the reporting call would
	// make the SQL UPDATE itself fail too, leaving the row stuck in
	// 'claimed' until its much longer (30m) lease expires -- see
	// jobstore.claimJobSQL's lease_expires_at reclaim window. 10s is ample
	// for a single-row UPDATE/transaction against Postgres while still
	// bounding how long a worker can block on a reporting call gone bad.
	postgresJobReportTimeout = 10 * time.Second

	// leaseRenewTimeout bounds each individual renewLeasePeriodically tick's
	// Store.RenewLease call, mirroring postgresJobReportTimeout's own "fresh,
	// short-lived context" reasoning (issue #520): a renewal call must not be
	// tied to the job's own (possibly long-running or near-deadline) ctx.
	leaseRenewTimeout = 10 * time.Second
)

// leaseRenewInterval is how often dispatchPostgresJob's renewal loop extends a
// claimed job's lease while it's being processed (issue #520). A package-level
// var, not a const, so tests can shrink it well below the 30-minute lease TTL
// (jobstore.claimJobSQL) without a real 30-minute wait. 10 minutes leaves ample
// margin in production: even a single missed tick still has 20 minutes of
// slack before the lease actually expires.
var leaseRenewInterval = 10 * time.Minute

var ringOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

type BackendWorker struct {
	services.Service

	cfg              Config
	s3Cfg            *s3backend.Config
	store            storage.Store
	overrides        overrides.Interface
	backendScheduler tempopb.BackendSchedulerClient

	workerID string

	// jobStore is nil when Postgres is not configured (cfg.Postgres == nil)
	// -- the same nil-means-disabled convention as backendscheduler's
	// catalogLister. Non-nil means processJobs tries a direct Postgres claim
	// for vi_backfill/cube_backfill before falling back to the existing gRPC
	// Next() path (#181 Phase 4).
	jobStore *jobstore.Store
	pgPool   *pgxpool.Pool

	// fileCatalogStore is nil under the exact same condition as jobStore
	// (cfg.Postgres == nil) -- issue #522's blockpack_file_catalog Postgres
	// store, used by every VI/VCNT/cube compaction handler's write path.
	fileCatalogStore *blockpack.FileCatalogStore

	// catalogObjectStore is nil when w.s3Cfg is nil (mirrors every other
	// S3-only Postgres-job handler's "if w.s3Cfg == nil, fail" convention --
	// processViBackfillJobPostgres/processCubeBackfillJobPostgres are
	// S3-only today too). Used by the VI/VCNT/cube compaction write path
	// (issue #522 Phase 0.4) to fetch merge inputs. A field (not a per-call
	// construction) so tests can override it with a fake, mirroring
	// jobStore's own override-in-tests convention.
	catalogObjectStore vblockpack.CatalogObjectStore

	// Ring used for sharding tenant index writing.
	ringLifecycler *ring.BasicLifecycler
	Ring           *ring.Ring

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

// var tracer = otel.Tracer("modules/backendworker")

// New creates a new BackendWorker. s3cfg is optional; when non-nil it enables
// cube backfill job execution.
func New(cfg Config, schedulerClientCfg backendscheduler_client.Config, s3cfg *s3backend.Config, store storage.Store, overrides overrides.Interface, reg prometheus.Registerer) (*BackendWorker, error) {
	err := ValidateConfig(&cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	w := &BackendWorker{
		cfg:       cfg,
		s3Cfg:     s3cfg,
		store:     store,
		overrides: overrides,
	}

	workerID, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	w.workerID = workerID

	level.Info(log.Logger).Log("msg", "backend worker starting", "worker_id", w.workerID)

	schedulerClient, err := backendscheduler_client.New(cfg.BackendSchedulerAddr, schedulerClientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend scheduler client: %w", err)
	}
	w.backendScheduler = schedulerClient

	// Postgres job store (#181 Phase 4): opt-in, nil when cfg.Postgres is
	// nil. Not the migration owner (backend-scheduler's New() applies
	// backend_jobs.sql, per #181 §3.3) -- a worker only ever claims rows an
	// already-migrated schema exposes.
	if cfg.Postgres != nil {
		pool, perr := postgres.NewPool(context.Background(), cfg.Postgres)
		if perr != nil {
			level.Warn(log.Logger).Log("msg", "postgres job store disabled -- pool init failed", "err", perr)
		} else {
			w.pgPool = pool
			w.jobStore = jobstore.New(pool)
			w.fileCatalogStore = blockpack.NewFileCatalogStore(pool)
		}
	}

	if s3cfg != nil {
		catalogObjectStore, cerr := vblockpack.NewCatalogObjectStoreS3(s3cfg)
		if cerr != nil {
			level.Warn(log.Logger).Log("msg", "catalog object store disabled -- client init failed", "err", cerr)
		} else {
			w.catalogObjectStore = catalogObjectStore
		}
	}

	if w.isSharded() {
		reg = prometheus.WrapRegistererWithPrefix("tempo_", reg)

		lifecyclerStore, err := kv.NewClient(
			cfg.Ring.KVStore,
			ring.GetCodec(),
			kv.RegistererWithKVName(reg, backendWorkerRingKey+"-lifecycler"),
			log.Logger,
		)
		if err != nil {
			return nil, err
		}

		// Define lifecycler delegates in reverse order (last to be called defined first because they're
		// chained via "next delegate").
		delegate := ring.BasicLifecyclerDelegate(w)
		delegate = ring.NewLeaveOnStoppingDelegate(delegate, log.Logger)
		delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.Ring.HeartbeatTimeout, delegate, log.Logger)

		lifecyclerCfg, err := toBasicLifecyclerConfig(cfg.Ring, log.Logger)
		if err != nil {
			return nil, fmt.Errorf("invalid ring lifecycler config: %w", err)
		}

		w.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, backendWorkerRingKey, cfg.OverrideRingKey, lifecyclerStore, delegate, log.Logger, prometheus.WrapRegistererWithPrefix("tempo_", reg))
		if err != nil {
			return nil, fmt.Errorf("unable to initialize backend-worker ring lifecycler: %w", err)
		}

		w.Ring, err = ring.New(cfg.Ring.ToLifecyclerConfig().RingConfig, backendWorkerRingKey, cfg.OverrideRingKey, log.Logger, reg)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize backend-worker ring: %w", err)
		}
	}

	w.Service = services.NewBasicService(w.starting, w.running, w.stopping)

	return w, nil
}

func (w *BackendWorker) starting(ctx context.Context) (err error) {
	defer func() {
		if err == nil || w.subservices == nil {
			return
		}

		if stopErr := services.StopManagerAndAwaitStopped(context.Background(), w.subservices); stopErr != nil {
			level.Error(log.Logger).Log("msg", "failed to gracefully stop backend-worker dependencies", "err", stopErr)
		}
	}()

	if w.isSharded() {
		w.subservices, err = services.NewManager(w.ringLifecycler, w.Ring)
		if err != nil {
			return fmt.Errorf("failed to create subservices: %w", err)
		}
		w.subservicesWatcher = services.NewFailureWatcher()
		w.subservicesWatcher.WatchManager(w.subservices)

		if err := services.StartManagerAndAwaitHealthy(ctx, w.subservices); err != nil {
			level.Warn(log.Logger).Log("msg", "backend-worker failed to start ring subservices, proceeding anyway", "err", err)
		} else {
			// Wait until the ring client detected this instance in the ACTIVE state.
			level.Info(log.Logger).Log("msg", "waiting until backend-worker is ACTIVE in the ring")
			ctxWithTimeout, cancel := context.WithTimeout(ctx, w.cfg.Ring.WaitActiveInstanceTimeout)
			if err := ring.WaitInstanceState(ctxWithTimeout, w.Ring, w.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
				level.Warn(log.Logger).Log("msg", "backend-worker did not become ACTIVE in the ring, proceeding anyway", "err", err)
			} else {
				level.Info(log.Logger).Log("msg", "backend-worker is ACTIVE in the ring")

				// In the event of a cluster cold start we may end up in a situation where each new backend-worker
				// instance starts at a slightly different time and thus each one starts with a different state
				// of the ring. It's better to just wait the ring stability for a short time.
				if w.cfg.Ring.WaitStabilityMinDuration > 0 {
					minWaiting := w.cfg.Ring.WaitStabilityMinDuration
					maxWaiting := w.cfg.Ring.WaitStabilityMaxDuration

					level.Info(log.Logger).Log("msg", "waiting until backend-worker ring topology is stable", "min_waiting", minWaiting.String(), "max_waiting", maxWaiting.String())
					if err := ring.WaitRingStability(ctx, w.Ring, ringOp, minWaiting, maxWaiting); err != nil {
						level.Warn(log.Logger).Log("msg", "backend-worker ring topology is not stable after the max waiting time, proceeding anyway")
					} else {
						level.Info(log.Logger).Log("msg", "backend-worker ring topology is stable")
					}
				}
			}
			cancel()
		}
	}

	w.store.EnablePolling(ctx, w, false)

	return nil
}

func (w *BackendWorker) running(ctx context.Context) error {
	level.Info(log.Logger).Log("msg", "backend worker running")

	b := backoff.New(ctx, w.cfg.Backoff)

	jobCtx := ctx
	if w.cfg.FinishOnShutdownTimeout > 0 {
		var jobsCancel context.CancelFunc
		jobCtx, jobsCancel = createShutdownContext(ctx, w.cfg.FinishOnShutdownTimeout)
		defer jobsCancel()
	}

	if w.subservices != nil {
		for {
			select {
			case <-ctx.Done():
				return nil
			case err := <-w.subservicesWatcher.Chan():
				return fmt.Errorf("worker subservices failed: %w", err)
			default:
				if err := w.processJobs(jobCtx); err != nil {
					level.Error(log.Logger).Log("msg", "error processing jobs", "err", err, "backoff", b.NextDelay())
					b.Wait()
					continue
				}

				b.Reset()
			}
		}
	} else {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := w.processJobs(jobCtx); err != nil {
					level.Error(log.Logger).Log("msg", "error processing jobs", "err", err, "backoff", b.NextDelay())
					b.Wait()
					continue
				}

				b.Reset()
			}
		}
	}
}

func (w *BackendWorker) processJobs(ctx context.Context) error {
	if w.jobStore != nil {
		job, err := w.tryClaimPostgresJob(ctx)
		if err != nil {
			return err
		}
		if job != nil {
			return w.dispatchPostgresJob(ctx, job)
		}
		// No Postgres job claimable right now -- fall through to the
		// existing gRPC path below, unchanged.
	}

	var (
		resp *tempopb.NextJobResponse
		err  error
	)

	// Request next job
	err = w.callSchedulerWithBackoff(ctx, func(ctx context.Context) error {
		var funcErr error
		resp, funcErr = w.backendScheduler.Next(ctx, &tempopb.NextJobRequest{
			WorkerId: w.workerID,
		})
		if funcErr != nil {
			if errStatus, ok := status.FromError(funcErr); ok {
				if errStatus.Code() == codes.NotFound {
					return errStatus.Err()
				}
			}

			return fmt.Errorf("error getting next job: %w", funcErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed processing jobs: %w", err)
	}

	if resp == nil || resp.JobId == "" {
		return fmt.Errorf("no jobs available")
	}

	metricWorkerJobsTotal.WithLabelValues().Inc()

	switch resp.Type {
	case tempopb.JobType_JOB_TYPE_COMPACTION:
		return w.processCompactionJob(ctx, resp)
	case tempopb.JobType_JOB_TYPE_RETENTION:
		return w.processRetentionJob(ctx, resp)
	case tempopb.JobType_JOB_TYPE_REDACTION:
		return w.processRedactionJob(ctx, resp)
	default:
		return fmt.Errorf("unknown job type: %s", resp.Type.String())
	}
}

// postgresJobClaimPriority is the order tryClaimPostgresJob tries each
// Postgres job type in -- arbitrary priority (revisit if real production
// data shows one starving another -- no evidence either way today).
// catalog_reconcile (issue #522, trace/span-only per #154) is tried last:
// it isn't latency-sensitive the way vi_backfill/cube_backfill chaining is.
// catalog_reap was removed outright by #154: it processed only vi/vcnt/cube
// rows against blockpack_file_catalog, which now belongs entirely to
// blockpack's own compaction-worker (plan.md Section G.4) -- keeping it here
// too would race the same rows into two independent delete pipelines.
// vi_compaction was removed by #155: candidate-selection and execution both
// moved into blockpack's own compaction-planner/compaction-worker, so this
// job type no longer exists in tempo's jobstore at all.
// trace_compaction (issue #522 #158, staged rollout) is tried right after the
// existing backfill chaining and before catalog_reconcile -- pairwise
// trace/span compaction for vblockpack-encoded tenants, alongside (not yet
// replacing) the legacy gRPC CompactionProvider path.
var postgresJobClaimPriority = []jobstore.JobType{
	jobstore.JobTypeViBackfill,
	jobstore.JobTypeCubeBackfill,
	jobstore.JobTypeTraceCompaction,
	jobstore.JobTypeCatalogReconcile,
}

// tryClaimPostgresJob tries each of postgresJobClaimPriority in order.
// Returns (nil, nil) if none has claimable work.
func (w *BackendWorker) tryClaimPostgresJob(ctx context.Context) (*jobstore.Job, error) {
	for _, jobType := range postgresJobClaimPriority {
		job, err := w.jobStore.Claim(ctx, jobType, w.workerID)
		if err != nil {
			return nil, fmt.Errorf("claim %s: %w", jobType, err)
		}
		if job != nil {
			return job, nil
		}
	}
	return nil, nil
}

// dispatchPostgresJob runs job to completion and reports the result back to
// Postgres directly (Store.Complete/Fail) -- NOT via w.backendScheduler.
// UpdateJob. There is no scheduler-mediated status update for Postgres-
// claimed jobs, since the scheduler was never the one that handed the job
// out in this branch.
func (w *BackendWorker) dispatchPostgresJob(ctx context.Context, job *jobstore.Job) error {
	// Issue #520: renew job.ID's lease every leaseRenewInterval for as long as
	// this function is actively processing it, so a job genuinely running
	// longer than the 30-minute lease TTL is never double-claimed by a second
	// worker. renewCtx is derived from ctx (not context.Background()) so the
	// loop also stops if the job's own ctx is canceled/expires independently
	// of this function returning. defer stopRenew() covers every exit path --
	// normal return, an error return, and a panic unwinding through this frame
	// -- so the goroutine below is never leaked running past this call.
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	go w.renewLeasePeriodically(renewCtx, job.ID)

	var err error
	switch job.Type {
	case jobstore.JobTypeViBackfill:
		err = w.processViBackfillJobPostgres(ctx, job)
	case jobstore.JobTypeCubeBackfill:
		err = w.processCubeBackfillJobPostgres(ctx, job)
	case jobstore.JobTypeTraceCompaction:
		err = w.processTraceCompactionJobPostgres(ctx, job)
	case jobstore.JobTypeCatalogReconcile:
		err = w.processCatalogReconcileJobPostgres(ctx, job)
	default:
		err = fmt.Errorf("unknown postgres job type: %s", job.Type)
	}
	return w.reportPostgresJobOutcome(job.ID, err)
}

// renewLeasePeriodically extends jobID's lease every leaseRenewInterval until
// ctx is done (issue #520). Each renewal call runs on its own short-lived,
// fresh context (leaseRenewTimeout), not ctx itself -- mirrors
// reportPostgresJobOutcome's identical "fresh context" reasoning, so a ctx
// that's already near its own deadline doesn't also starve the renewal call
// meant to buy the job more time. A failed renewal is logged and NOT
// otherwise retried before the next scheduled tick: a single missed tick
// still leaves ample margin (30-minute lease vs 10-minute interval) before
// the lease could actually expire.
func (w *BackendWorker) renewLeasePeriodically(ctx context.Context, jobID string) {
	ticker := time.NewTicker(leaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), leaseRenewTimeout)
			if err := w.jobStore.RenewLease(renewCtx, jobID); err != nil {
				level.Warn(log.Logger).Log("msg", "failed to renew backend_jobs lease", "job_id", jobID, "err", err)
			}
			cancel()
		}
	}
}

// reportPostgresJobOutcome reports a Postgres-claimed job's execution result
// directly to w.jobStore (Complete on success, Fail-with-retry on error) --
// never to w.backendScheduler.UpdateJob, since the scheduler never handed
// the job out in this branch.
//
// The Store.Fail/Store.Complete call itself runs on a fresh, short-lived
// context (postgresJobReportTimeout), NOT the job's own ctx passed in here
// (2026-07-14 fix): a job that failed because ITS OWN ctx's deadline was
// exceeded must still be able to successfully record that failure, which is
// impossible if the reporting call reuses that same already-expired ctx.
//
// Issue #518: this function's Complete/Fail-only shape is intentional and
// unchanged -- it never enqueued a chained continuation job, before or after
// #518. Chain-enqueue responsibility belongs entirely to the new job-planner
// component's own poll loop, which decides independently (by re-reading
// viusage_entries/cube_entries) whether more history remains to backfill.
func (w *BackendWorker) reportPostgresJobOutcome(jobID string, jobErr error) error {
	reportCtx, cancel := context.WithTimeout(context.Background(), postgresJobReportTimeout)
	defer cancel()

	if jobErr != nil {
		level.Error(log.Logger).Log("msg", "postgres job failed", "job_id", jobID, "err", jobErr)
		return w.jobStore.Fail(reportCtx, jobID, jobErr.Error())
	}
	return w.jobStore.Complete(reportCtx, jobID)
}

// processViBackfillJobPostgres unmarshals job.Detail (JSONB) into
// jobstore.ViBackfillDetail and runs the backfill; success/failure is
// reported by dispatchPostgresJob's caller via w.jobStore, not
// w.backendScheduler -- there is no gRPC-path equivalent anymore (deleted,
// mirroring cube_backfill's earlier removal, once nothing emits
// JOB_TYPE_VI_BACKFILL into the scheduler's job stream).
func (w *BackendWorker) processViBackfillJobPostgres(ctx context.Context, job *jobstore.Job) error {
	if job.Tenant == "" {
		return fmt.Errorf("vi backfill job missing tenant")
	}

	var detail jobstore.ViBackfillDetail
	if err := json.Unmarshal(job.Detail, &detail); err != nil {
		return fmt.Errorf("vi backfill: unmarshal detail: %w", err)
	}

	if w.s3Cfg == nil {
		return fmt.Errorf("vi backfill: S3 not configured on worker")
	}

	entry := blockpack.Entry{
		Tenant:     job.Tenant,
		ColumnHash: detail.ColumnHash,
		ColumnName: detail.ColumnName,
		ColumnType: detail.ColumnType,
	}

	level.Info(log.Logger).Log("msg", "processing vi backfill job (postgres)",
		"job_id", job.ID, "tenant", job.Tenant, "column", entry.ColumnName)

	deps, err := vblockpack.NewViBackfillDepsS3(w.s3Cfg)
	if err != nil {
		return fmt.Errorf("vi backfill: failed to construct deps: %w", err)
	}
	// 2026-07-17: use the SAME Postgres-backed registry the querier-side trigger (onShouldBackfill)
	// already writes entries through -- without this, RunViBackfill falls back to a fresh
	// blob-backed registry that has never heard of an entry Postgres already created, and every
	// watermark-persist call fails with "entry ... not found" (see NewViBackfillDepsWithPgRegistry's
	// own doc comment for the full history).
	deps = vblockpack.NewViBackfillDepsWithPgRegistry(deps, w.pgPool, job.Tenant)

	// #518: entry above is built fresh from job.Detail's column-identity fields only,
	// so its Backfill (in particular WatermarkSec, the new AnchorSec source) is always
	// the zero value -- load the REAL, already-persisted entry so a chained
	// continuation job anchors to where the last one left off instead of silently
	// falling back to "anchor to now" every time, exactly the bug this whole feature
	// exists to fix. Mirrors processCubeBackfillJobPostgres's own blockpack.LoadCubeEntry
	// call below, which already does this correctly for cube_backfill.
	if deps.Registry != nil {
		loaded, ok, loadErr := loadViUsageEntry(ctx, deps.Registry, entry.ColumnHash, entry.ColumnType)
		if loadErr != nil {
			return fmt.Errorf("vi backfill: load entry: %w", loadErr)
		}
		if ok {
			entry.Backfill = loaded.Backfill
		}
	}

	if err := vblockpack.RunViBackfill(ctx, entry, deps, detail.WindowSeconds); err != nil {
		return fmt.Errorf("vi backfill failed: %w", err)
	}

	return nil
}

// loadViUsageEntry finds the entry matching (colHash, colType) in registry's own
// tenant (a *blockpack.Registry is bound to exactly one tenant at construction), or
// (zero-value, false, nil) if no such entry exists yet -- a safe, conservative
// fallback (AnchorSec stays 0, i.e. "anchor to now") rather than a hard failure,
// since a genuinely first-ever backfill for a column is expected to have no entry
// yet the very first time this runs.
func loadViUsageEntry(ctx context.Context, registry *blockpack.Registry, colHash, colType string) (blockpack.Entry, bool, error) {
	entries, _, err := registry.Load(ctx)
	if err != nil {
		return blockpack.Entry{}, false, err
	}
	for _, e := range entries {
		if e.ColumnHash == colHash && e.ColumnType == colType {
			return e, true, nil
		}
	}
	return blockpack.Entry{}, false, nil
}

// processCubeBackfillJobPostgres mirrors processViBackfillJobPostgres's shape
// for cube_backfill: job.Detail (JSONB) is unmarshaled into
// jobstore.CubeBackfillDetail, and success/failure is reported by
// dispatchPostgresJob's caller via w.jobStore, never via w.backendScheduler.
// #181 Phase 5 deleted the old gRPC-path equivalent (processCubeBackfillJob,
// which read tempopb.JobDetail.CubeBackfill) once cube_backfill moved
// entirely off the gRPC Next()/JobDetail path.
func (w *BackendWorker) processCubeBackfillJobPostgres(ctx context.Context, job *jobstore.Job) error {
	if job.Tenant == "" {
		return fmt.Errorf("cube backfill job missing tenant")
	}

	var detail jobstore.CubeBackfillDetail
	if err := json.Unmarshal(job.Detail, &detail); err != nil {
		return fmt.Errorf("cube backfill: unmarshal detail: %w", err)
	}

	if w.s3Cfg == nil {
		return fmt.Errorf("cube backfill: S3 not configured on worker")
	}
	// w.pgPool backs the cube registry (issue #504: Postgres is now the only supported cube
	// registry backend, no blob/index.json fallback) -- distinct from w.jobStore's own nil
	// check above (jobStore is the durable job QUEUE; pgPool here is the cube registry itself,
	// both opt-in on the SAME cfg.Postgres != nil condition, see w.pgPool's field doc comment).
	if w.pgPool == nil {
		return fmt.Errorf("cube backfill: postgres not configured on worker")
	}

	level.Info(log.Logger).Log("msg", "processing cube backfill job (postgres)",
		"job_id", job.ID, "tenant", job.Tenant, "cube_id", detail.CubeID)

	// Load the real registry entry (dimensions, AggAttrs, filters). A missing
	// entry is a hard, immediate failure (2026-07-14 fix) -- proceeding into
	// RunCubeBackfill with a placeholder entry lacking AggAttrs would fail
	// cube.Backfiller's per-minute validation on every single minute of the
	// backfill window, burning the job's entire ctx budget for nothing.
	entry, err := blockpack.LoadCubeEntry(ctx, w.pgPool, job.Tenant, detail.CubeID)
	if err != nil {
		return fmt.Errorf("cube backfill: no registry entry found for cube %s: %w", detail.CubeID, err)
	}

	// Bound the backfill window by the tenant's actual effective retention (2026-07-17,
	// follow-up to #512): blocks physically cannot exist past this point, so bounding the
	// backfill window here is not an artificial cap, just an accurate one -- an unbounded
	// window still terminates (empty per-minute lookups are cheap), but wastes serial
	// iterations discovering that on its own instead of knowing it upfront.
	retentionMinutes := effectiveBlockRetentionMinutes(w.cfg.Compactor.BlockRetention, w.BlockRetentionForTenant(job.Tenant))

	// Run backfill synchronously (the worker goroutine is already async).
	// detail.WindowMinutes is the per-job bound (job-planner's chained continuation
	// jobs, or math.MaxUint32 for the reactive trigger's first job -- cubequerypath.go's
	// existing literal, unchanged); retentionMinutes remains an outer ceiling a job's
	// window can never exceed, regardless of what WindowMinutes requests (issue #518,
	// Correction 1: WindowMinutes was previously write-only dead JSONB).
	if err := vblockpack.RunCubeBackfill(ctx, entry, w.s3Cfg, w.pgPool, detail.WindowMinutes, retentionMinutes); err != nil {
		return fmt.Errorf("cube backfill failed: %w", err)
	}

	return nil
}

func (w *BackendWorker) processCompactionJob(ctx context.Context, resp *tempopb.NextJobResponse) error {
	if resp.Detail.Tenant == "" {
		metricWorkerBadJobsReceived.WithLabelValues("no_tenant").Inc()
		return w.failJob(ctx, resp.JobId, "received compaction job with empty tenant")
	}

	level.Debug(log.Logger).Log("msg", "received job", "job_id", resp.JobId, "tenant", resp.Detail.Tenant)

	blockMetas := w.store.BlockMetas(resp.Detail.Tenant)

	// Collect the metas which match the IDs in the job
	var sourceMetas []*backend.BlockMeta
	for _, blockMeta := range blockMetas {
		for _, blockID := range resp.Detail.Compaction.Input {
			if blockMeta.BlockID.String() == blockID {
				sourceMetas = append(sourceMetas, blockMeta)
			}
		}
	}

	// Execute compaction using existing logic
	newCompacted, err := w.compact(ctx, sourceMetas, resp.Detail.Tenant)
	if err != nil {
		return w.failJob(ctx, resp.JobId, fmt.Sprintf("error compacting blocks: %v", err))
	}

	var newIDs []string
	for _, blockMeta := range newCompacted {
		newIDs = append(newIDs, blockMeta.BlockID.String())
	}

	// Mark job as complete
	err = w.callSchedulerWithBackoff(ctx, func(ctx context.Context) error {
		_, err = w.backendScheduler.UpdateJob(ctx, &tempopb.UpdateJobStatusRequest{
			JobId:  resp.JobId,
			Status: tempopb.JobStatus_JOB_STATUS_SUCCEEDED,
			Compaction: &tempopb.CompactionDetail{
				Output: newIDs,
			},
		})
		if err != nil {
			return fmt.Errorf("failed marking job %q as complete: %w", resp.JobId, err)
		}

		return nil
	})
	if err != nil {
		return w.failJob(ctx, resp.JobId, fmt.Sprintf("error marking job as complete: %v", err))
	}

	return nil
}

func (w *BackendWorker) processRetentionJob(ctx context.Context, resp *tempopb.NextJobResponse) error {
	tenantID := resp.Detail.Tenant
	level.Debug(log.Logger).Log("msg", "received retention job", "job_id", resp.JobId, "tenant", tenantID)

	// Per-tenant path (new behaviour): run retention for only the specified tenant.
	// Fallback path (rollout compatibility): if tenant is empty this is a legacy
	// global job emitted by an older scheduler binary; retain all tenants as before.
	if tenantID != "" {
		w.store.RetainTenantWithConfig(ctx, tenantID, &w.cfg.Compactor, ownsEverythingSharder{}, w)
	} else {
		w.store.RetainWithConfig(ctx, &w.cfg.Compactor, ownsEverythingSharder{}, w)
	}

	err := w.callSchedulerWithBackoff(ctx, func(ctx context.Context) error {
		_, err := w.backendScheduler.UpdateJob(ctx, &tempopb.UpdateJobStatusRequest{
			JobId:  resp.JobId,
			Status: tempopb.JobStatus_JOB_STATUS_SUCCEEDED,
		})
		if err != nil {
			return fmt.Errorf("failed marking job %q as complete: %w", resp.JobId, err)
		}

		return nil
	})
	if err != nil {
		return w.failJob(ctx, resp.JobId, fmt.Sprintf("error marking job as complete: %v", err))
	}

	return nil
}

func (w *BackendWorker) processRedactionJob(ctx context.Context, resp *tempopb.NextJobResponse) error {
	tenantID := resp.Detail.Tenant
	if tenantID == "" {
		metricWorkerBadJobsReceived.WithLabelValues("no_tenant").Inc()
		return w.failJob(ctx, resp.JobId, "received redaction job with empty tenant")
	}
	if resp.Detail.Redaction == nil {
		return w.failJob(ctx, resp.JobId, "received redaction job with nil redaction detail")
	}

	blockIDStr := resp.Detail.Redaction.BlockId
	if blockIDStr == "" {
		return w.failJob(ctx, resp.JobId, "received redaction job with empty block_id")
	}

	blockMetas := w.store.BlockMetas(tenantID)
	var meta *backend.BlockMeta
	for _, m := range blockMetas {
		if m.BlockID.String() == blockIDStr {
			meta = m
			break
		}
	}
	if meta == nil {
		// Block no longer present (e.g. already compacted away); treat as clean.
		level.Debug(log.Logger).Log("msg", "redaction block not found, completing as no-op", "job_id", resp.JobId, "block_id", blockIDStr)
		return w.completeRedactionJob(ctx, resp.JobId, 0)
	}

	traceIDs := make([]common.ID, 0, len(resp.Detail.Redaction.TraceIds))
	for _, b := range resp.Detail.Redaction.TraceIds {
		if len(b) > 0 {
			traceIDs = append(traceIDs, common.ID(b))
		}
	}

	level.Debug(log.Logger).Log("msg", "processing redaction job", "job_id", resp.JobId, "block_id", blockIDStr, "trace_ids_count", len(traceIDs))

	_, tracesFound, _, err := w.store.RedactBlock(ctx, meta, tenantID, traceIDs)
	if err != nil {
		return w.failJob(ctx, resp.JobId, fmt.Sprintf("redact block: %v", err))
	}

	level.Debug(log.Logger).Log("msg", "redaction block processed", "job_id", resp.JobId, "block_id", blockIDStr, "rewrote", tracesFound > 0, "traces_found", tracesFound)
	return w.completeRedactionJob(ctx, resp.JobId, tracesFound)
}

func (w *BackendWorker) completeRedactionJob(ctx context.Context, jobID string, tracesFound int) error {
	return w.callSchedulerWithBackoff(ctx, func(ctx context.Context) error {
		_, err := w.backendScheduler.UpdateJob(ctx, &tempopb.UpdateJobStatusRequest{
			JobId:  jobID,
			Status: tempopb.JobStatus_JOB_STATUS_SUCCEEDED,
			Redaction: &tempopb.RedactionResult{
				TracesFound: int32(tracesFound),
			},
		})
		if err != nil {
			return fmt.Errorf("failed marking redaction job %q as complete: %w", jobID, err)
		}
		return nil
	})
}

func (w *BackendWorker) stopping(_ error) error {
	if w.pgPool != nil {
		w.pgPool.Close()
	}

	if w.subservices != nil {
		return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
	}

	level.Info(log.Logger).Log("msg", "backend worker stopped")

	return nil
}

func (w *BackendWorker) failJob(ctx context.Context, jobID string, errMsg string) error {
	level.Error(log.Logger).Log("msg", "job failed", "job_id", jobID, "error", errMsg)

	err := w.callSchedulerWithBackoff(ctx, func(ctx context.Context) error {
		_, err := w.backendScheduler.UpdateJob(ctx, &tempopb.UpdateJobStatusRequest{
			JobId:  jobID,
			Status: tempopb.JobStatus_JOB_STATUS_FAILED,
			Error:  errMsg,
		})
		if err != nil {
			return fmt.Errorf("failed marking job %q as failed: %w", jobID, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("error marking job %q as failed: %w", jobID, err)
	}

	return fmt.Errorf("%s", errMsg)
}

func (w *BackendWorker) compact(ctx context.Context, blockMetas []*backend.BlockMeta, tenantID string) ([]*backend.BlockMeta, error) {
	return w.store.CompactWithConfig(ctx, blockMetas, tenantID, &w.cfg.Compactor, w, w)
}

// Owns implements tempodb.CompactorSharder. It IS live today (blockpack#516): w is passed
// as a blocklist.JobSharder to store.EnablePolling (see starting() above), and
// blocklist.Poller.tenantIndexBuilder calls sharder.Owns(job) on every poll cycle, for
// every tenant, to decide whether this instance should (re)build that tenant's index.
// (EnableCompaction's separate Owns-consuming blockSelector loop, tempodb/compactor.go,
// is still never invoked for backend-worker -- backend-worker's own compaction dispatch
// calls store.CompactWithConfig directly, which doesn't consult the sharder's Owns.)
// Since starting() above no longer blocks service startup on ring health (#516), Owns can
// be called here before the ring ever reaches ACTIVE. That's safe: Owns already fails
// closed on ring errors (returns false, see below), and tenantIndexBuilder's own
// PollFallback (defaults true in production, modules/storage/config.go) makes the poller
// build+write the tenant index anyway when ownership can't be determined -- confirmed via
// TestStarting_RingNeverReachesActive_ReturnsNilNotError's logs, which show successful
// "writing tenant index" every poll cycle despite the ring never reaching ACTIVE.
func (w *BackendWorker) Owns(hash string) bool {
	if !w.isSharded() {
		return true
	}

	level.Debug(log.Logger).Log("msg", "checking hash", "hash", hash)

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(hash))
	hash32 := hasher.Sum32()

	rs, err := w.Ring.Get(hash32, ringOp, []ring.InstanceDesc{}, nil, nil)
	if err != nil {
		level.Error(log.Logger).Log("msg", "failed to get ring", "err", err)
		return false
	}

	if len(rs.Instances) != 1 {
		level.Error(log.Logger).Log("msg", "unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
		return false
	}

	ringAddr := w.ringLifecycler.GetInstanceAddr()

	level.Debug(log.Logger).Log("msg", "checking addresses", "owning_addr", rs.Instances[0].Addr, "this_addr", ringAddr)

	return rs.Instances[0].Addr == ringAddr
}

// effectiveBlockRetentionMinutes resolves a tenant's effective block retention, in minutes, for
// bounding a cube backfill window (2026-07-17, follow-up to blockpack#512): tenantOverride wins
// when set (nonzero), else cfgDefault -- the SAME "check for overrides" precedence tempodb.go's
// retainTenant already uses for compaction retention. Returns 0 (RunCubeBackfill's own
// "unbounded" convention) when the resolved retention is itself zero/unset. Pure, extracted so
// this precedence logic has a direct unit test independent of BackendWorker's ring/S3/Postgres
// wiring.
func effectiveBlockRetentionMinutes(cfgDefault, tenantOverride time.Duration) uint32 {
	retention := cfgDefault
	if tenantOverride != 0 {
		retention = tenantOverride
	}
	if retention <= 0 {
		return 0
	}
	return uint32(retention / time.Minute) //nolint:gosec // retention fits uint32 minutes for any realistic config
}

func (w *BackendWorker) RecordDiscardedSpans(count int, tenantID string, traceID string, rootSpanName string, rootServiceName string) {
	level.Warn(log.Logger).Log("msg", "max size of trace exceeded", "tenant", tenantID, "traceId", traceID,
		"rootSpanName", rootSpanName, "rootServiceName", rootServiceName, "discarded_span_count", count)
	overrides.RecordDiscardedSpans(count, overrides.ReasonCompactorDiscardedSpans, tenantID)
}

// BlockRetentionForTenant implements CompactorOverrides
func (w *BackendWorker) BlockRetentionForTenant(tenantID string) time.Duration {
	return w.overrides.BlockRetention(tenantID)
}

// CompactionDisabledForTenant implements CompactorOverrides
func (w *BackendWorker) CompactionDisabledForTenant(tenantID string) bool {
	return w.overrides.CompactionDisabled(tenantID)
}

func (w *BackendWorker) MaxBytesPerTraceForTenant(tenantID string) int {
	return w.overrides.MaxBytesPerTrace(tenantID)
}

func (w *BackendWorker) MaxCompactionRangeForTenant(tenantID string) time.Duration {
	return w.overrides.MaxCompactionRange(tenantID)
}

// DedicatedColumnsForTenant implements CompactorOverrides.
// Returns the current per-tenant dedicated columns so compaction always re-indexes
// output blocks according to the live config rather than copying from input blocks.
func (w *BackendWorker) DedicatedColumnsForTenant(tenantID string) backend.DedicatedColumns {
	return w.overrides.DedicatedColumns(tenantID)
}

func (w *BackendWorker) callSchedulerWithBackoff(ctx context.Context, f func(context.Context) error) error {
	var (
		b   = backoff.New(ctx, w.cfg.Backoff)
		err error
	)

	for b.Ongoing() {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err = f(ctx); err != nil {
				if ctx.Err() != nil {
					// Parent was canceled while executing, return
					return nil
				}

				level.Error(log.Logger).Log("msg", "error calling scheduler", "err", err, "backoff", b.NextDelay())
				metricWorkerCallRetries.WithLabelValues().Inc()
				// Add jitter so all workers don't all retry at once and cause a thundering herd.
				time.Sleep(time.Duration(rand.Float32() * float32(1*time.Second)))
				b.Wait()
				continue
			}

			b.Reset()
			return nil
		}
	}

	return fmt.Errorf("backoff terminated: %w, %w", b.Err(), err)
}

func (w *BackendWorker) isSharded() bool {
	store := w.cfg.Ring.KVStore.Store
	return store != "" && store != "inmemory"
}

// OnRingInstanceRegister is called while the lifecycler is registering the
// instance within the ring and should return the state and set of tokens to
// use for the instance itself.
func (w *BackendWorker) OnRingInstanceRegister(_ *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, _ string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	// When we initialize the compactor instance in the ring we want to start from
	// a clean situation, so whatever is the state we set it ACTIVE, while we keep existing
	// tokens (if any) or the ones loaded from file.
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	takenTokens := ringDesc.GetTokens()
	gen := ring.NewRandomTokenGenerator()
	newTokens := gen.GenerateTokens(ringNumTokens-len(tokens), takenTokens)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.ACTIVE, tokens
}

// OnRingInstanceTokens is called once the instance tokens are set and are
// stable within the ring (honoring the observe period, if set).
func (w *BackendWorker) OnRingInstanceTokens(*ring.BasicLifecycler, ring.Tokens) {}

// OnRingInstanceStopping is called while the lifecycler is stopping. The lifecycler
// will continue to heartbeat the ring the this function is executing and will proceed
// to unregister the instance from the ring only after this function has returned.
func (w *BackendWorker) OnRingInstanceStopping(*ring.BasicLifecycler) {}

// OnRingInstanceHeartbeat is called while the instance is updating its heartbeat
// in the ring.
func (w *BackendWorker) OnRingInstanceHeartbeat(*ring.BasicLifecycler, *ring.Desc, *ring.InstanceDesc) {
}

type ownsEverythingSharder struct {
	w *BackendWorker
}

var _ tempodb.CompactorSharder = ownsEverythingSharder{}

func (ownsEverythingSharder) Owns(_ string) bool {
	return true
}

func (s ownsEverythingSharder) RecordDiscardedSpans(count int, tenantID string, traceID string, rootSpanName string, rootServiceName string) {
	s.w.RecordDiscardedSpans(count, tenantID, traceID, rootSpanName, rootServiceName)
}

// createShutdownContext creates a context that starts a timeout only after parentCtx is cancelled
func createShutdownContext(parentCtx context.Context, shutdownTimeout time.Duration) (context.Context, context.CancelFunc) {
	jobsCtx, jobsCancel := context.WithCancel(context.Background())

	go func() {
		<-parentCtx.Done() // Wait for parent cancellation

		// Now start the shutdown timeout
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer timeoutCancel()

		select {
		case <-timeoutCtx.Done():
			// Timeout expired, force cancel jobs
			level.Warn(log.Logger).Log("msg", "job timeout expired")
			jobsCancel()
		case <-jobsCtx.Done():
			// Jobs completed gracefully before timeout
			return
		}
	}()

	return jobsCtx, jobsCancel
}
