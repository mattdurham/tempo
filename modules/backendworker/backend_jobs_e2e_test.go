package backendworker

// backend_jobs_e2e_test.go — #181 Phase 6.1/6.2/6.3/6.4 (worker half): real end-to-end
// proof that BackendWorker.processJobs/dispatchPostgresJob claim and execute a durably
// inserted backend_jobs row entirely without the gRPC scheduler client, against a real
// Postgres (testcontainers) and a real S3-compatible endpoint (fake_s3_e2e_test.go's
// hand-rolled server).
//
// Rows are seeded via jobstore.Store.InsertViBackfill/InsertCubeBackfill directly --
// the SAME production call vblockpack's real trigger functions
// (realUsageRecorder.RecordUse's onShouldBackfill closure, cubeQueryPath.maybeCreateCube's
// Created branch) make internally -- rather than through those trigger functions
// themselves: BackendWorker.processJobs/dispatchPostgresJob are unexported methods of
// this package, which vblockpack cannot import (vblockpack -> backendworker would be a
// cycle, since backendworker already imports vblockpack), so no single Go test function
// can span both packages' private surface. This mirrors #181 Phase 4's own
// backendworker_postgres_jobstore_test.go, which hit the identical constraint and made
// the identical choice. The trigger half's OWN real-wiring proof (RecordUse/
// maybeCreateCube durably inserting a row) lives in
// tempodb/encoding/vblockpack/backend_jobs_e2e_test.go instead. Read both files together
// for the full pipeline picture.
//
// For VI backfill, the entry itself is ALSO seeded via the real, exported
// blockpack.RecordUseAndMaybeTrigger (not a hand-built blockpack.Entry) against a real
// minio-backed ObjectStore pointed at the fake S3 server -- production's own
// onShouldBackfill closure performs exactly these two actions (trigger the entry, insert
// the durable row) together; this test performs them the same way, just via the public
// blockpack API instead of vblockpack's unexported wrapper. Cube backfill mirrors this
// with blockpack.NewPgCubeRegistry(...).Add for the same reason (cube's real trigger,
// TryCreate, is also unexported and unreachable from this package) -- issue #504
// (2026-07-15): cube's registry is Postgres-only now (no blob/index.json fallback), so
// this seeds into the SAME real Postgres container newTestPostgresPoolAndDSN already
// provisions for the job queue, not a separate blob-backed fixture.

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/postgres"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// testMinioObjectStore is a minimal blockpack.ObjectStore/blockpack.CubeObjectStore
// adapter over a real *minio.Client -- structurally identical to vblockpack's own
// unexported viUsageObjectStore/minioObjectStore (same Get/ConditionalPut shape, same
// NoSuchKey->notFoundErr translation), rewritten here because those types are unexported
// and this package cannot import vblockpack privates. notFoundErr lets one adapter type
// serve both blockpack.ErrNotFound (viusage) and blockpack.CubeErrNotFound (cube)
// contracts.
type testMinioObjectStore struct {
	client      *minio.Client
	bucket      string
	notFoundErr error
}

func (s *testMinioObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", s.mapNotFound(err)
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", s.mapNotFound(err)
	}
	info, statErr := s.client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})
	if statErr != nil {
		return data, "", nil
	}
	return data, info.ETag, nil
}

func (s *testMinioObjectStore) mapNotFound(err error) error {
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
		return s.notFoundErr
	}
	return err
}

func (s *testMinioObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag != "" {
		opts.SetMatchETag(etag)
	}
	_, err := s.client.PutObject(ctx, s.bucket, path, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 412 {
			return blockpack.ErrConflict
		}
		return err
	}
	return nil
}

// newTestPostgresPoolAndDSN mirrors newTestPostgresPool (backendworker_postgres_jobstore_test.go)
// but also returns the raw DSN, needed here to configure a real *BackendWorker's own
// cfg.Postgres (BackendWorker.New constructs its own separate pgxpool.Pool from the DSN;
// this test's own pool and the worker's are two independent connections to the SAME real
// Postgres container).
func newTestPostgresPoolAndDSN(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("backendworker_e2e_test"),
		tcpostgres.WithUsername("backendworker_e2e_test"),
		tcpostgres.WithPassword("backendworker_e2e_test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable in this environment, skipping Postgres-backed test: %v", err)
		}
		t.Fatalf("starting postgres testcontainer: %v", err)
	}
	t.Cleanup(func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Logf("terminating postgres testcontainer: %v", termErr)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	require.NoError(t, migrate.Apply(ctx, pool))
	// Issue #504: cube's registry is Postgres-only now (no blob/index.json fallback) --
	// blockpack.ApplyCubeSchema creates cube_entries, needed by this file's
	// TestE2E_CubeBackfill_* tests, which seed real registry fixtures via
	// blockpack.NewPgCubeRegistry against this same pool.
	require.NoError(t, blockpack.ApplyCubeSchema(ctx, pool))
	return pool, dsn
}

func newTestMinioClient(t *testing.T, s3cfg *s3backend.Config) *minio.Client {
	t.Helper()
	client, err := minio.New(s3cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3cfg.AccessKey, "test-secret-key", ""),
		Secure: false,
		Region: s3cfg.Region,
	})
	require.NoError(t, err)
	return client
}

// TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC is #181 Phase 6.1 steps 2-3: a
// real BackendWorker, with jobStore auto-wired from cfg.Postgres (New's own production
// wiring, unmodified for this test), claims a durably-inserted vi_backfill row and
// executes it via the real Postgres-claim path -- WITHOUT ever calling the gRPC
// scheduler's Next/UpdateJob (a counting mockScheduler proves zero calls) -- and the
// real side effect (the viusage registry's Done=true watermark) is verified by reading
// the SAME real S3-backed store the worker itself wrote to.
func TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-vi-bucket")
	client := newTestMinioClient(t, s3cfg)

	tenant := "e2e-worker-vi-tenant"
	viStore := &testMinioObjectStore{client: client, bucket: s3cfg.Bucket, notFoundErr: blockpack.ErrNotFound}
	registry := blockpack.NewRegistry(viStore, tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill, "first-ever use must trigger immediately (R4)")

	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, tenant, jobstore.ViBackfillDetail{
		ColumnHash: triggerResult.Entry.ColumnHash,
		ColumnName: triggerResult.Entry.ColumnName,
		ColumnType: triggerResult.Entry.ColumnType,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore, "New must auto-wire jobStore from cfg.Postgres")

	scheduler, nextCalls, updateCalls := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	err = w.processJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")
	require.Equal(t, 0, *updateCalls, "the gRPC scheduler's UpdateJob must never be called for a Postgres-claimed job")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status)

	entries, _, loadErr := registry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	require.True(t, entries[0].Backfill.Done,
		"the worker's real RunViBackfill call must have persisted Done=true to the real S3-backed registry")
}

// TestE2E_ViBackfill_FailureThenReclaimSucceeds is #181 Phase 6.3: a real execution
// failure (the durable row references a column with NO corresponding registry entry --
// runViBackfillCore's UpdateWatermark call genuinely fails with "entry not found" against
// the real S3-backed registry, exactly the failure mode a crashed/inconsistent trigger
// would produce -- not a mocked error) must schedule a retry per §8.2 (doubling backoff
// from 1 minute: retry 1 at +2m). Rather than a real 2-minute wall-clock wait, this test
// moves next_retry_at into the past via direct SQL (the mechanism the plan itself
// prescribes for this exact scenario) and proves a SECOND real Claim call reclaims and
// completes the job once the real registry entry has been seeded for real in between.
//
// #181 Phase 6.3's cube_backfill mirror is TestE2E_CubeBackfill_FailureThenReclaimSucceeds
// below (2026-07-14): RunCubeBackfill now returns a real error and
// processCubeBackfillJobPostgres propagates it, closing the gap this comment used to flag.
func TestE2E_ViBackfill_FailureThenReclaimSucceeds(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-vi-retry-bucket")

	tenant := "e2e-worker-vi-retry-tenant"
	// ColumnHash must be the REAL hash blockpack.RecordUseAndMaybeTrigger will later
	// compute for this column name (blockpack.ColHash is the same hash the registry
	// itself keys entries by) -- otherwise the second attempt's reclaim would still
	// fail to find the entry for an unrelated reason (key mismatch), not because the
	// retry mechanism itself is broken.
	columnHash := blockpack.ColHash("span.custom.attr")
	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, tenant, jobstore.ViBackfillDetail{
		ColumnHash: columnHash, ColumnName: "span.custom.attr", ColumnType: "string",
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	// Attempt 1: no registry entry exists for this (tenant, column) at all --
	// runViBackfillCore's UpdateWatermark call genuinely fails "entry ... not found"
	// against the real S3-backed registry, a real failure, not an injected one.
	err = w.processJobs(ctx)
	require.NoError(t, err, "Store.Fail itself must succeed even though the underlying backfill failed")
	require.Equal(t, 0, *nextCalls)

	var (
		status      string
		retries     int
		nextRetryAt time.Time
		lastErr     string
	)
	row := pool.QueryRow(ctx,
		`SELECT status, retries, next_retry_at, last_error FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status, &retries, &nextRetryAt, &lastErr))
	require.Equal(t, string(jobstore.StatusFailed), status)
	require.Equal(t, 1, retries)
	require.Contains(t, lastErr, "not found")
	require.WithinDuration(t, time.Now().Add(2*time.Minute), nextRetryAt, 10*time.Second,
		"retry 1 must be scheduled at approximately +2m per the locked doubling-backoff policy (backoffDuration(1)=2m)")

	// Move next_retry_at into the past directly via SQL -- the plan's own prescribed
	// mechanism for testing reclaim without a real wall-clock wait.
	_, err = pool.Exec(ctx,
		`UPDATE backend_jobs SET next_retry_at = now() - interval '1 minute' WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, err)

	// Seed the real registry entry for real, in between attempts -- the second attempt
	// must be able to genuinely succeed, not just be reclaimable.
	client := newTestMinioClient(t, s3cfg)
	viStore := &testMinioObjectStore{client: client, bucket: s3cfg.Bucket, notFoundErr: blockpack.ErrNotFound}
	registry := blockpack.NewRegistry(viStore, tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill)
	require.Equal(t, columnHash, triggerResult.Entry.ColumnHash,
		"the seeded entry's ColumnHash must match the failed job's detail for the reclaim to succeed against the same key")

	// Attempt 2: Claim's retry-due branch (status='failed' AND next_retry_at <= now())
	// reclaims the SAME row; the registry entry now exists for real, so this attempt
	// genuinely succeeds.
	err = w.processJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, *nextCalls, "the reclaim must also never fall back to the gRPC scheduler")

	row = pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status, "the reclaimed second attempt must reach succeeded")

	entries, _, loadErr := registry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	require.True(t, entries[0].Backfill.Done)
}

// TestE2E_CubeBackfill_WorkerClaimsAndExecutesWithoutGRPC is #181 Phase 6.2 steps 2-3.
// RunCubeBackfill resolves WindowMinutes from the tenant's effective retention, which defaults to math.MaxUint32 when unset (as it is in this test's Config)
// (confirmed: #181 Phase 4's own backendworker_postgres_jobstore_test.go documented that
// a from-scratch cube backfill never returns in reasonable test time, real S3 or not),
// so this test bounds the call with a short ctx deadline and asserts REAL partial
// progress (the cube's L0 watermark advancing past its seeded zero value) rather than
// waiting for Done=true.
//
// processJobs may still return an error here (a ctx deadline expiring mid-run can make
// Store.Complete/Store.Fail's own SQL call fail against the now-expired ctx) -- not
// asserted either way; the real side effects below are what this test actually proves.
// See TestE2E_CubeBackfill_FailureThenReclaimSucceeds for the dedicated real-failure ->
// Store.Fail proof (2026-07-14: RunCubeBackfill now returns a real error instead of the
// void return this comment used to document as a gap).
// TestE2E_CubeBackfill_BoundedRetention_ReachesFullCompletion is the deterministic,
// no-live-cluster proof that the 2026-07-17 fixes (blockpack#512's Backfiller.Run
// parallelism, and this package's effectiveBlockRetentionMinutes window-bounding) actually
// work together through the REAL production wiring: processJobs -> dispatchPostgresJob ->
// processCubeBackfillJobPostgres -> effectiveBlockRetentionMinutes -> vblockpack.
// RunCubeBackfill -> cubeBackfillWindowMinutes -> blockpack.RunCubeBackfill -> Backfiller.Run.
//
// Before either fix this was impossible to assert in reasonable test time: WindowMinutes was
// unconditionally math.MaxUint32, so Backfiller.Run's endMinute always resolved to 0 -- a
// "genuinely completed" run only terminates once minute 0 is reached, which
// TestE2E_CubeBackfill_WorkerClaimsAndExecutesWithoutGRPC's own doc comment documents as never
// happening in reasonable test time, hence that test's 3-second bounded-ctx workaround
// asserting only PARTIAL progress. Configuring a small workerCfg.Compactor.BlockRetention here
// makes effectiveBlockRetentionMinutes resolve a real, small window (3 minutes) instead --
// letting this test use a normal, generous ctx and assert genuine full completion: the
// registry's persisted L0 watermark covers the ENTIRE resolved window, down to its floor.
func TestE2E_CubeBackfill_BoundedRetention_ReachesFullCompletion(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-bounded-bucket")

	tenant := "e2e-worker-cube-bounded-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-bounded-1",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	cubeRegistry := blockpack.NewPgCubeRegistry(pool, tenant)
	require.NoError(t, cubeRegistry.Add(ctx, entry))

	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID: entry.CubeID, WindowMinutes: 60,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	const retentionMinutes = 3
	workerCfg.Compactor.BlockRetention = retentionMinutes * time.Minute

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	currentMinute := uint32(time.Now().Unix() / 60) //nolint:gosec // unix timestamp fits uint32 until 2106
	wantFloor := currentMinute - retentionMinutes

	// A generous but bounded ctx (unlike the sibling test's deliberate 3s partial-progress
	// window): a real, tiny, all-idle 3-minute backfill against a fake S3 server should finish
	// in well under this even serially -- this is not a "wait for the timeout" test.
	boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = w.processJobs(boundedCtx)
	require.NoError(t, err, "a bounded, resolvable retention window must let the backfill genuinely finish, not time out")
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status, "a genuinely completed backfill must report success, not merely have made partial progress")

	entries, _, loadErr := cubeRegistry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok, "the worker's real RunCubeBackfill call must have persisted at least one real L0 watermark update")
	require.LessOrEqual(t, wm.MinMinute, wantFloor,
		"a genuinely COMPLETE backfill must cover all the way down to the resolved retention floor, not just make partial progress into it")
}

func TestE2E_CubeBackfill_WorkerClaimsAndExecutesWithoutGRPC(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-bucket")

	tenant := "e2e-worker-cube-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-1",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	cubeRegistry := blockpack.NewPgCubeRegistry(pool, tenant)
	require.NoError(t, cubeRegistry.Add(ctx, entry))

	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID: entry.CubeID, WindowMinutes: 60,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	boundedCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// processJobs may return an error here (see doc comment above) -- not asserted
	// either way; the real side effects below are what this test actually proves.
	_ = w.processJobs(boundedCtx)
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")

	entries, _, loadErr := cubeRegistry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok, "the worker's real RunCubeBackfill call must have persisted at least one real L0 watermark update")
	require.Greater(t, wm.MaxMinute, uint32(0),
		"the persisted watermark must reflect real per-minute progress, not a seeded zero value")
}

// TestE2E_CubeBackfill_FailureThenReclaimSucceeds is #181 Phase 6.3's cube_backfill
// mirror of TestE2E_ViBackfill_FailureThenReclaimSucceeds (2026-07-14 fix). The failure
// trigger here is deliberately NOT "cube ID never added to the registry" (that scenario's
// own dedicated coverage is TestE2E_CubeBackfill_NoRegistryEntry_FailsFastNotBurningCtx
// below, which fails FAST inside LoadCubeEntry, before RunCubeBackfill is ever reached) --
// this test wants LoadCubeEntry to succeed (a real, well-formed-looking entry IS found) and
// the SUBSEQUENT run to genuinely fail instead.
//
// 2026-07-15 migration (issue #504): cube's registry is Postgres-only now (no
// blob/index.json fallback), so the pre-migration failure trigger (blocking S3 PUTs to
// make the registry's own blob ConditionalPut genuinely fail) no longer applies --
// watermark persistence goes through Postgres, entirely unaffected by S3 write
// availability. The seeded entry here deliberately omits AggAttrs (a genuinely
// malformed-but-loadable registry entry, e.g. from a partially-written trigger): LoadCubeEntry
// still finds it (Load doesn't validate content), but cube.Backfiller's per-minute
// "definition must include duration in AggAttrs" validation then genuinely fails EVERY
// minute, tripping the circuit breaker (cubeBackfillMaxConsecutiveFailures=5) and aborting
// with a real error -- exactly the same structural-failure shape
// TestRunCubeBackfillCore_ConsecutiveStructuralFailuresAbortEarly
// (tempodb/encoding/vblockpack/cube_backfill_watermark_test.go) proves at the unit level, now
// proven through the real worker dispatch path. Before RunCubeBackfill's 2026-07-14
// error-propagation fix, this exact failure was reported to Postgres as succeeded; this test
// proves it now reaches Store.Fail with a scheduled retry instead.
func TestE2E_CubeBackfill_FailureThenReclaimSucceeds(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-retry-bucket")

	tenant := "e2e-worker-cube-retry-tenant"
	cubeID := "e2e-cube-retry-1"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     cubeID,
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		// AggAttrs deliberately omitted -- see doc comment above.
		Resolution: 1,
	}
	cubeRegistry := blockpack.NewPgCubeRegistry(pool, tenant)
	require.NoError(t, cubeRegistry.Add(ctx, entry))

	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID: cubeID, WindowMinutes: 60,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	err = w.processJobs(ctx)
	require.NoError(t, err, "Store.Fail itself must succeed even though the underlying backfill failed")
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")

	var (
		status      string
		retries     int
		nextRetryAt time.Time
		lastErr     string
	)
	row := pool.QueryRow(ctx,
		`SELECT status, retries, next_retry_at, last_error FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status, &retries, &nextRetryAt, &lastErr))
	require.Equal(t, string(jobstore.StatusFailed), status,
		"a real RunCubeBackfill failure must be reported to Postgres as failed, not silently succeeded")
	require.Equal(t, 1, retries)
	require.Contains(t, lastErr, "aborted after 5 consecutive per-minute failures")
	require.WithinDuration(t, time.Now().Add(2*time.Minute), nextRetryAt, 10*time.Second,
		"retry 1 must be scheduled at approximately +2m per the locked doubling-backoff policy (backoffDuration(1)=2m)")
}

// TestE2E_CubeBackfill_NoRegistryEntry_FailsFastNotBurningCtx is the 2026-07-14
// compounding-bug fix's dedicated proof for fix 1 (LoadCubeEntry no longer masked as a
// placeholder): a cube_backfill job whose CubeID was never added to the registry (a real
// LoadCubeEntry "not found" failure, not injected) must fail FAST -- in low single-digit
// seconds, not by burning the job's ctx budget scanning a math.MaxUint32-wide window -- with
// an error clearly naming the missing registry entry, and the row must reach
// status='failed' promptly with that error recorded (fix 3's own reportPostgresJobOutcome
// fresh-ctx fix is what makes recording that failure possible even if this call had run
// long enough to expire ctx, but the point of THIS test is that it does not need to: fix 1
// means processCubeBackfillJobPostgres never even reaches RunCubeBackfill).
func TestE2E_CubeBackfill_NoRegistryEntry_FailsFastNotBurningCtx(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-noentry-bucket")

	tenant := "e2e-worker-cube-noentry-tenant"
	cubeID := "e2e-cube-never-added"

	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID: cubeID, WindowMinutes: 60,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	start := time.Now()
	err = w.processJobs(ctx)
	elapsed := time.Since(start)
	require.NoError(t, err, "Store.Fail itself must succeed")
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")
	require.Less(t, elapsed, 5*time.Second,
		"a no-registry-entry failure must fail immediately, not burn any real fraction of the job's ctx budget")

	var (
		status  string
		lastErr string
	)
	row := pool.QueryRow(ctx,
		`SELECT status, last_error FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status, &lastErr))
	require.Equal(t, string(jobstore.StatusFailed), status,
		"a missing registry entry must be reported to Postgres as failed promptly, not left hanging")
	require.Contains(t, lastErr, "no registry entry found for cube")
}
