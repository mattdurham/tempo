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
// blockpack.NewPgViUsageRegistry-backed registry (2026-07-17: was blob/S3-backed, see
// TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC's own doc comment for why) --
// production's own onShouldBackfill closure performs exactly these two actions (trigger
// the entry, insert the durable row) together; this test performs them the same way,
// just via the public blockpack API instead of vblockpack's unexported wrapper. Cube
// backfill mirrors this with blockpack.NewPgCubeRegistry(...).Add for the same reason
// (cube's real trigger, TryCreate, is also unexported and unreachable from this package)
// -- issue #504 (2026-07-15): cube's registry is Postgres-only now (no blob/index.json
// fallback), so this seeds into the SAME real Postgres container newTestPostgresPoolAndDSN
// already provisions for the job queue, not a separate blob-backed fixture.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/dskit/services"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// TestE2E_ViBackfill_ClaimedAndExecuted_WhileRingDegraded is #516's actual proof that
// the production symptom is fixed: a real, durably-inserted vi_backfill row gets claimed
// and executed via the real worker dispatch path even though the ring never reaches
// ACTIVE. Byte-identical setup to TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC
// (above) except for the ring config -- the only variable under test is ring health, not
// the job/registry plumbing. Before #516's fix, w.starting(ctx) below returns a non-nil
// error and, through the real dskit service lifecycle (services.NewBasicService's
// documented contract: "if StartingFn returns error, no other functions are called"),
// running()/processJobs() would never be invoked at all -- this job would be
// permanently unreachable, not merely delayed.
func TestE2E_ViBackfill_ClaimedAndExecuted_WhileRingDegraded(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-vi-ring-degraded-bucket")

	tenant := "e2e-worker-vi-ring-degraded-tenant"
	registry := blockpack.NewPgViUsageRegistry(pool, tenant)
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
	workerCfg, schedulerClientCfg, overridesSvc, _, _ := setupDependencies(ctx, t, limitCfg)
	// setupDependencies's own fixture store already has blocklist polling enabled (see
	// newStoreWithLogger) -- w.starting(ctx) below unconditionally enables it a SECOND time
	// on whatever store it's given, which would race two concurrent pollers on the same
	// underlying blocklist state if given that same store (this test doesn't need
	// blocklist data at all, only the Postgres/ring behavior). storeWithoutPolling gives
	// starting() a store that was never separately polling-enabled, so its own call is the
	// only one.
	workerStore := storeWithoutPolling(t)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	// Ring config matching TestStarting_RingNeverReachesActive_ReturnsNilNotError
	// (backendworker_test.go): nilKVClient can structurally never produce an ACTIVE ring
	// state, so ring.WaitInstanceState deterministically times out.
	workerCfg.Ring.KVStore.Store = "mock"
	workerCfg.Ring.KVStore.Mock = nilKVClient{}
	workerCfg.Ring.WaitActiveInstanceTimeout = 500 * time.Millisecond
	workerCfg.Ring.WaitStabilityMinDuration = 0

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore, "New must auto-wire jobStore from cfg.Postgres")
	require.True(t, w.isSharded(), "precondition: the ring-gated branch of starting() must actually be exercised")

	scheduler, nextCalls, updateCalls := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	err = w.starting(ctx)
	t.Cleanup(func() {
		if w.subservices != nil {
			_ = services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
		}
	})
	require.NoError(t, err, "starting() must unblock the service lifecycle even though the ring never reaches ACTIVE")

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
		"the worker's real RunViBackfill call must have persisted Done=true to the real Postgres-backed registry, proving the job was genuinely claimed and executed while the ring was degraded")
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
	// 2026-07-17: viusage_entries, needed by this file's TestE2E_ViBackfill_* tests once they
	// seed/verify fixtures via blockpack.NewPgViUsageRegistry against this same pool -- matches
	// production once NewViBackfillDepsWithPgRegistry makes backend-worker's own RunViBackfill
	// call site use the SAME Postgres-backed registry the querier-side trigger already does.
	require.NoError(t, blockpack.ApplyViUsageSchema(ctx, pool))
	// issue #522: blockpack_file_catalog, needed by catalog_reconcile/catalog_reap
	// coverage in this package's own test files (VI/VCNT/cube catalog-sync moved
	// entirely to blockpack's own compaction-planner/compaction-worker per #154/#155,
	// but this table remains the shared metadata source of truth those subsystems
	// still write to, and trace/span's own catalog_reconcile still reads it here).
	require.NoError(t, blockpack.ApplyFileCatalogSchema(ctx, pool))
	// issue #522 #158: tempo's OWN file_catalog.sql (a different table from
	// blockpack_file_catalog above) -- trace/span compaction planning moved out of
	// jobplanner entirely (it now lives in blockpack's own compaction-planner, reading
	// blockpack_file_catalog), but jobplanner.Service.CatalogPollOnce still runs
	// planCatalogReconcile against this table for retention/redaction/VCNT-exclusion
	// purposes, so any test driving that path against this pool still needs this
	// schema present.
	applySchemaFile(ctx, t, pool, fileCatalogSchemaPath)
	return pool, dsn
}

// TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC is #181 Phase 6.1 steps 2-3: a
// real BackendWorker, with jobStore auto-wired from cfg.Postgres (New's own production
// wiring, unmodified for this test), claims a durably-inserted vi_backfill row and
// executes it via the real Postgres-claim path -- WITHOUT ever calling the gRPC
// scheduler's Next/UpdateJob (a counting mockScheduler proves zero calls) -- and the
// real side effect (the viusage registry's Done=true watermark) is verified by reading
// the SAME real Postgres-backed registry the worker itself wrote to.
//
// 2026-07-17: switched from a blob(S3)-backed registry to blockpack.NewPgViUsageRegistry,
// matching production now that NewViBackfillDepsWithPgRegistry makes backend-worker's own
// RunViBackfill call site use the Postgres-backed registry whenever cfg.Postgres is
// configured (as it is here) -- before this fix, this test happened to pass only because
// RunViBackfill unconditionally fell back to a fresh blob-backed registry regardless of
// what the caller's own Postgres config said, silently masking the exact bug this test
// would otherwise have caught.
func TestE2E_ViBackfill_WorkerClaimsAndExecutesWithoutGRPC(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-vi-bucket")

	tenant := "e2e-worker-vi-tenant"
	registry := blockpack.NewPgViUsageRegistry(pool, tenant)
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
		"the worker's real RunViBackfill call must have persisted Done=true to the real Postgres-backed registry")
}

// TestE2E_ViBackfill_FailureThenReclaimSucceeds is #181 Phase 6.3: a real execution
// failure (the durable row references a column with NO corresponding registry entry --
// runViBackfillCore's UpdateWatermark call genuinely fails with "entry not found" against
// the real Postgres-backed registry (2026-07-17: was S3-backed; see the sibling test's own
// doc comment for why), exactly the failure mode a crashed/inconsistent trigger
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
	// against the real Postgres-backed registry, a real failure, not an injected one.
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
	// must be able to genuinely succeed, not just be reclaimable. Postgres-backed (2026-07-17),
	// matching what RunViBackfill now actually looks up (see this test's own doc comment).
	registry := blockpack.NewPgViUsageRegistry(pool, tenant)
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

// TestE2E_ViBackfill_ChainedJobAnchorsToPersistedWatermarkNotNow is issue #518's
// real end-to-end proof of Correction 2's fix: a chained continuation job (the
// shape job-planner will enqueue) must anchor its window to the column's
// already-persisted watermark, not wall-clock now. Before this fix,
// processViBackfillJobPostgres built its blockpack.Entry fresh from job.Detail's
// column-identity fields alone, leaving Backfill (and therefore AnchorSec) at its
// zero value on every real dispatch -- silently defeating the whole feature even
// though unit-level tests that construct the entry directly (bypassing the
// worker's real load step) would still pass.
//
// Simulates "job-planner already chained once, more history remains" by directly
// persisting a non-zero watermark via registry.UpdateWatermark(done=false) --
// exactly the state a prior bounded-window job leaves behind -- then dispatches a
// SECOND bounded-window (3600s) job through the real worker. The bucket has zero
// real blocks for this tenant, so the run completes immediately (a real, valid
// "nothing left in this window" outcome, same as every sibling test's empty-bucket
// case) -- what matters is WHERE the resulting watermark lands: WatermarkSec-3600
// (anchored, correct) versus something within 3600 seconds of the current wall
// clock (unanchored, the bug), values that differ by decades and can never be
// confused for each other.
//
// #519 follow-up: Done is asserted FALSE here, not true -- resolved minSec
// (seededWatermarkSec-windowSeconds = 1400) is nonzero, so under #519's corrected
// contract (Done requires minSec==0, not just "this call's own listing was
// exhausted") this bounded window correctly does NOT claim full historical
// coverage. This is the exact property #519 fixed: before it, this assertion was
// (incorrectly) True, which would have made CoversRange trust a column as fully
// covered after just one small chained window.
func TestE2E_ViBackfill_ChainedJobAnchorsToPersistedWatermarkNotNow(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-vi-anchor-bucket")

	tenant := "e2e-worker-vi-anchor-tenant"
	registry := blockpack.NewPgViUsageRegistry(pool, tenant)
	triggerResult, err := blockpack.RecordUseAndMaybeTrigger(
		ctx, registry, tenant, "span.custom.attr", "string", time.Now(),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, triggerResult.ShouldBackfill, "first-ever use must trigger immediately (R4)")

	// Simulate a prior chained job's partial progress: watermark advanced to 5000,
	// not yet Done -- exactly the state job-planner's poll loop would find and
	// continue from.
	const seededWatermarkSec = 5000
	require.NoError(t, registry.UpdateWatermark(
		ctx, tenant, triggerResult.Entry.ColumnHash, triggerResult.Entry.ColumnType,
		seededWatermarkSec, 0, seededWatermarkSec, false,
	))

	const windowSeconds = 3600
	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, tenant, jobstore.ViBackfillDetail{
		ColumnHash: triggerResult.Entry.ColumnHash, ColumnName: triggerResult.Entry.ColumnName,
		ColumnType: triggerResult.Entry.ColumnType, WindowSeconds: windowSeconds,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)

	require.NoError(t, w.processJobs(ctx))

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'vi_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status)

	entries, _, loadErr := registry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	require.False(t, entries[0].Backfill.Done,
		"a bounded window with nonzero resolved minSec must NOT claim Done (#519) -- more history may exist below WatermarkSec")
	require.EqualValues(t, seededWatermarkSec-windowSeconds, entries[0].Backfill.WatermarkSec,
		"the resulting watermark must be seededWatermarkSec-windowSeconds (anchored to the persisted watermark), "+
			"not anywhere near now()-windowSeconds (which would be off by decades) -- this is Correction 2's exact regression")
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
	// Equal, not just LessOrEqual (2026-07-17 mutation-testing fix): idle minutes are cheap
	// against a fake S3 server regardless of window size, so a wrongly-much-LARGER window would
	// ALSO finish inside the bounded ctx and satisfy a mere "<=" check -- only exact equality to
	// the resolved floor actually proves THIS SPECIFIC window size was the one used.
	require.Equal(t, wantFloor, wm.MinMinute,
		"a genuinely COMPLETE backfill's watermark must land exactly on the resolved retention floor, not merely reach or pass it")
}

// TestE2E_CubeBackfill_WindowMinutesIsLoadBearingAndChainsFromExistingWatermark is
// issue #518's single highest-value regression test (plan.md Part 2.5): before this
// fix, jobstore.CubeBackfillDetail.WindowMinutes was written by cubequerypath.go's
// job-creation call sites but never read by processCubeBackfillJobPostgres --
// RunCubeBackfill resolved its window from retentionMinutes alone. Separately (Part
// 2.4/Correction 1), every call site anchored Backfiller.Run to currentMinute=0
// (wall-clock now) regardless of any existing watermark, so a chained job would
// always reprocess the same "most recent WindowMinutes" slice and never make
// backward progress.
//
// This test seeds a cube entry with an EXISTING, non-zero L0 watermark (simulating a
// backfill that already completed one window), inserts a job with a small
// WindowMinutes (5) and leaves tenant retention unset (unbounded ceiling, so retention
// alone could never explain a narrow result), and asserts the resulting watermark
// lands exactly `WindowMinutes` older than the pre-existing watermark -- proving BOTH
// that WindowMinutes is now genuinely load-bearing (not silently ignored) AND that the
// next window resumed from the existing watermark instead of wall-clock now (genuine
// backward progress, pinning Correction 1).
func TestE2E_CubeBackfill_WindowMinutesIsLoadBearingAndChainsFromExistingWatermark(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-chain-bucket")

	tenant := "e2e-worker-cube-chain-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-chain-1",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Resolution: 1,
	}
	cubeRegistry := blockpack.NewPgCubeRegistry(pool, tenant)
	require.NoError(t, cubeRegistry.Add(ctx, entry))

	// Seed an existing L0 watermark far in the past, decoupled from wall-clock now, so
	// the resulting window's placement can only be explained by resuming from THIS
	// value, not by "now" or by any retention-ceiling coincidence.
	const existingMinMinute = uint32(1_000_000)
	const jobWindowMinutes = uint32(5)
	require.NoError(t, cubeRegistry.UpdateWatermarks(
		ctx, entry.CubeID, blockpack.CubeRollupL0, existingMinMinute, existingMinMinute+50,
	))

	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID: entry.CubeID, WindowMinutes: jobWindowMinutes,
	}))

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	// Deliberately leave workerCfg.Compactor.BlockRetention unset (0 -> unbounded
	// ceiling, math.MaxUint32): if the observed result were narrow only because of
	// retention, this test would prove nothing about WindowMinutes actually being read.

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = w.processJobs(boundedCtx)
	require.NoError(t, err, "a small, resolvable job window must let the backfill genuinely finish, not time out")
	require.Equal(t, 0, *nextCalls, "the gRPC scheduler's Next must never be called for a Postgres-claimed job")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status)

	entries, _, loadErr := cubeRegistry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok)
	require.Equal(t, existingMinMinute-jobWindowMinutes, wm.MinMinute,
		"the chained job's window must resume from the existing watermark (currentMinuteAnchor) and be bounded by the job's own WindowMinutes, not retention or wall-clock now")
	require.Less(t, wm.MinMinute, existingMinMinute,
		"the watermark must have made genuine backward progress, not reprocessed the same already-covered slice")
}

// overridesConfigWithPerTenantBlockRetention writes a real per-tenant runtime-config-override
// YAML file (the scoped/new format overridesConfigForTest's own default ConfigType expects) and
// returns an overrides.Config pointing at it -- the same mechanism a production
// per_tenant_override_config file uses, not a hand-built in-memory limits override. The caller
// must start the resulting overrides.Service (services.StartAndAwaitRunning) before
// BlockRetention(tenant) reflects the file's contents: runtimeConfigOverridesManager only loads
// PerTenantOverrideConfig inside its own starting() hook, not at construction time.
func overridesConfigWithPerTenantBlockRetention(t *testing.T, tenant string, retention time.Duration) overrides.Config {
	t.Helper()
	cfg := overridesConfigForTest(t)

	overridesYAML := fmt.Sprintf(`
overrides:
  %s:
    compaction:
      block_retention: %s
`, tenant, retention.String())
	overridesFile := filepath.Join(t.TempDir(), "per-tenant-overrides.yaml")
	require.NoError(t, os.WriteFile(overridesFile, []byte(overridesYAML), 0o600))

	cfg.PerTenantOverrideConfig = overridesFile
	return cfg
}

// TestE2E_CubeBackfill_PerTenantRetentionOverride_BoundsWindow is the per-tenant-override half
// of TestE2E_CubeBackfill_BoundedRetention_ReachesFullCompletion's coverage: that test proves
// effectiveBlockRetentionMinutes' CFG-DEFAULT branch (workerCfg.Compactor.BlockRetention) reaches
// full completion through the real wiring; this test proves the PER-TENANT-OVERRIDE branch does
// too, driven through a real overrides.Service loading a real runtime-config-override YAML file
// (production's actual mechanism), not a hand-built limits struct. The configured cfg default
// (30 days) is deliberately much LARGER than the per-tenant override (2 minutes) so a passing
// test can only mean the override actually won -- effectiveBlockRetentionMinutes' own unit test
// already pins the precedence in isolation; this proves the real overrides.Service wiring
// reaches the same real RunCubeBackfill call path this file's other cube_backfill tests do.
func TestE2E_CubeBackfill_PerTenantRetentionOverride_BoundsWindow(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-tenant-override-bucket")

	tenant := "e2e-worker-cube-tenant-override-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-tenant-override-1",
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

	const tenantRetentionMinutes = 2
	limitCfg := overridesConfigWithPerTenantBlockRetention(t, tenant, tenantRetentionMinutes*time.Minute)
	workerCfg, schedulerClientCfg, overridesSvc, _, workerStore := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}
	workerCfg.Compactor.BlockRetention = 30 * 24 * time.Hour // deliberately much larger -- see doc comment

	require.NoError(t, services.StartAndAwaitRunning(ctx, overridesSvc))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), overridesSvc)
	})
	require.Equal(t, tenantRetentionMinutes*time.Minute, overridesSvc.BlockRetention(tenant),
		"sanity check: the per-tenant override file must actually be loaded before proceeding")

	w, err := New(workerCfg, schedulerClientCfg, s3cfg, workerStore, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w.jobStore)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	currentMinute := uint32(time.Now().Unix() / 60) //nolint:gosec // unix timestamp fits uint32 until 2106
	wantFloor := currentMinute - tenantRetentionMinutes

	boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = w.processJobs(boundedCtx)
	require.NoError(t, err, "the per-tenant override must resolve to a small, genuinely completable window, not the much larger cfg default")
	require.Equal(t, 0, *nextCalls)

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status)

	entries, _, loadErr := cubeRegistry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok)
	// Equal, not LessOrEqual: idle minutes are cheap against a fake S3 server regardless of
	// window size, so even the (wrong) 30-day cfg default would finish inside a 30s bounded ctx
	// against this fast fake server and satisfy a mere "<=" check -- only exact equality to the
	// SMALL per-tenant-override floor proves the override actually won, not the cfg default.
	require.Equal(t, wantFloor, wm.MinMinute,
		"the per-tenant override's small window must be what actually bounded this run, not the 30-day cfg default")
}

// TestE2E_CubeBackfill_TwoDimensions_BoundedRetention_ReachesFullCompletion closes the
// grouped/2-dimension gap in this file's cube_backfill coverage: every other cube_backfill test
// here uses a single-dimension entry. Mirrors TestE2E_CubeBackfill_BoundedRetention_
// ReachesFullCompletion exactly, just with Dimensions holding two columns, proving the real
// wiring (dim2Col resolution, the entry.Dimensions[1] LookupColumn branch inside
// Backfiller.processMinute) doesn't misbehave for a 2-dimension entry specifically -- e.g. a
// panic on entry.Dimensions[1], or an AllDimSentinel/dim2Col mismatch that would only surface
// with a real 2-dimension RegistryEntry. This is an all-idle window (no real per-span VI data
// seeded), matching this file's own established idle-backfill testing convention for the
// windowing/completion contract; join-correctness for real 2-dimension data is already covered
// at the blockpack unit level (internal/modules/cube/backfill_test.go).
func TestE2E_CubeBackfill_TwoDimensions_BoundedRetention_ReachesFullCompletion(t *testing.T) {
	ctx := context.Background()
	pool, dsn := newTestPostgresPoolAndDSN(t)
	s3cfg := newFakeS3Config(t, "e2e-worker-cube-twodim-bucket")

	tenant := "e2e-worker-cube-twodim-tenant"
	entry := blockpack.CubeRegistryEntry{
		CubeID:     "e2e-cube-twodim-1",
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name", "span.name"},
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

	boundedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = w.processJobs(boundedCtx)
	require.NoError(t, err, "a 2-dimension entry must complete a bounded backfill exactly like a 1-dimension one")
	require.Equal(t, 0, *nextCalls)

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status)

	entries, _, loadErr := cubeRegistry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	require.Equal(t, []string{"resource.service.name", "span.name"}, entries[0].Dimensions,
		"sanity check: the loaded entry really is the 2-dimension one this test seeded")
	wm, ok := entries[0].Watermarks[blockpack.CubeRollupL0]
	require.True(t, ok)
	require.Equal(t, wantFloor, wm.MinMinute)
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
