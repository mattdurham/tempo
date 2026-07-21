package jobplanner

// service.go — issue #518's job-planner poll loop: chain-enqueues the next
// bounded-window vi_backfill/cube_backfill job for columns/cubes already
// triggered at least once with more history left to backfill. This is the
// ENTIRE scope of job-planner -- it does not execute any backfill work
// itself, does not touch object storage, and does not decide whether a
// never-before-queried column/cube gets its very first job (that stays with
// the existing reactive query-path trigger, vi_usage_hook.go/
// cubequerypath.go). Safe to run with N replicas: InsertViBackfill/
// InsertCubeBackfill's dedup_key partial unique index makes redundant inserts
// a harmless no-op, so concurrent planner replicas polling the same tick never
// double-create a job.

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	blockpack "github.com/grafana/blockpack"

	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

var (
	metricColumnsPlanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "columns_planned_total",
		Help:      "Total number of chained-continuation vi_backfill jobs enqueued by job-planner.",
	})
	metricCubesPlanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "cubes_planned_total",
		Help:      "Total number of chained-continuation cube_backfill jobs enqueued by job-planner.",
	})
	metricPollErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "poll_errors_total",
		Help:      "Total number of poll ticks that encountered an error. Never aborts the loop.",
	})
	metricCatalogReconcilesPlanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "catalog_reconciles_planned_total",
		Help:      "Total number of catalog_reconcile jobs enqueued by job-planner (issue #522).",
	})
	metricCatalogPollErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "catalog_poll_errors_total",
		Help:      "Total number of catalog-maintenance poll ticks that encountered an error. Never aborts the loop.",
	})
	metricTraceCompactionsPlanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "tempodb",
		Subsystem: "jobplanner",
		Name:      "trace_compactions_planned_total",
		Help:      "Total number of trace_compaction jobs enqueued by job-planner (issue #522 #158).",
	})
)

// Service is the job-planner poll loop.
type Service struct {
	pool             *pgxpool.Pool
	jobStore         *jobstore.Store
	fileCatalogStore *blockpack.FileCatalogStore
	cfg              common.JobPlannerConfig

	// pollFn defaults to s.pollOnce; overridable in tests so Run's
	// ticker/error-handling logic is unit-testable without a real Postgres
	// connection.
	pollFn func(ctx context.Context) error

	// catalogPollFn defaults to s.catalogPollOnce; overridable in tests for the
	// same reason as pollFn above (issue #522's catalog-maintenance ticker).
	catalogPollFn func(ctx context.Context) error
}

// New constructs a Service backed by pool.
func New(pool *pgxpool.Pool, cfg common.JobPlannerConfig) *Service {
	s := &Service{
		pool: pool, jobStore: jobstore.New(pool), fileCatalogStore: blockpack.NewFileCatalogStore(pool), cfg: cfg,
	}
	s.pollFn = s.pollOnce
	s.catalogPollFn = s.catalogPollOnce
	return s
}

// Run ticks pollFn every cfg.PollInterval and catalogPollFn every
// cfg.CatalogPollInterval, on two independent tickers (mirroring
// backendscheduler.running's multi-ticker select loop) -- catalog
// maintenance (issue #522's trace/span catalog_reconcile, per #154) has no
// tight latency requirement, so it runs on its own, slower cadence rather
// than sharing PollInterval's tick. Either tick's error is logged and never
// aborts the loop (mirrors backendscheduler's "one bad tick doesn't kill the
// poller" posture) -- a transient Postgres blip should not require the whole
// process to restart.
func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled {
		<-ctx.Done()
		return ctx.Err()
	}
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	catalogTicker := time.NewTicker(s.cfg.CatalogPollInterval)
	defer catalogTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.pollFn(ctx); err != nil {
				metricPollErrors.Inc()
				level.Warn(util_log.Logger).Log("msg", "jobplanner: poll tick failed", "err", err)
			}
		case <-catalogTicker.C:
			if err := s.catalogPollFn(ctx); err != nil {
				metricCatalogPollErrors.Inc()
				level.Warn(util_log.Logger).Log("msg", "jobplanner: catalog poll tick failed", "err", err)
			}
		}
	}
}

// PollOnce runs a single poll tick synchronously and returns its error
// directly (Run's own loop only logs it) -- exported so integration tests in
// other packages (e.g. modules/backendworker's chained-backward-progress e2e
// tests) can drive job-planner's real planning decision without waiting on
// Run's ticker or needing access to the unexported pollFn/planVi/planCube
// methods.
func (s *Service) PollOnce(ctx context.Context) error {
	return s.pollFn(ctx)
}

// pollOnce plans vi_backfill/cube_backfill continuations for one tick. Each part runs
// independently -- a failure planning one must not prevent the other from being planned in the
// same tick. Compaction candidate selection (VI/VCNT/cube, and -- as of this same session --
// trace/span too) no longer lives here at all: it moved entirely to blockpack's own
// compaction-planner, which plans directly from blockpack_file_catalog and dispatches to the
// same shared compaction-worker pool regardless of subsystem.
func (s *Service) pollOnce(ctx context.Context) error {
	viErr := s.planVi(ctx)
	cubeErr := s.planCube(ctx)
	if viErr != nil {
		return viErr
	}
	return cubeErr
}

// CatalogPollOnce runs a single catalog-maintenance tick synchronously and
// returns its error directly, mirroring PollOnce's exported-for-integration-
// tests rationale above.
func (s *Service) CatalogPollOnce(ctx context.Context) error {
	return s.catalogPollFn(ctx)
}

// catalogPollOnce plans catalog_reconcile for one tick (issue #522,
// trace/span-only per #154). catalog_reap was removed outright: it
// processed only vi/vcnt/cube rows against blockpack_file_catalog, which now
// belongs entirely to blockpack's own compaction-planner (plan.md Section
// G.3) -- keeping it here too would race the same rows into two independent
// delete pipelines.
func (s *Service) catalogPollOnce(ctx context.Context) error {
	return s.planCatalogReconcile(ctx)
}
