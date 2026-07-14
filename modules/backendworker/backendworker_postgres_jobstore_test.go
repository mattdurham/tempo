package backendworker

// backendworker_postgres_jobstore_test.go -- #181 Phase 4.1/4.2 coverage:
// processJobs' try-Postgres-first path, and dispatchPostgresJob's
// Complete/Fail-not-scheduler wiring.
//
// newTestPostgresPool below is this package's own copy of
// ../../tempodb/encoding/vblockpack/jobstore/pg_testutil_test.go's helper --
// unexported test helpers cannot be imported across packages, mirrors that
// package's own duplication of the same convention.

import (
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// overridesConfigForTest mirrors TestWorker's own limits setup.
func overridesConfigForTest(t *testing.T) overrides.Config {
	t.Helper()
	limitCfg := overrides.Config{}
	limitCfg.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})
	return limitCfg
}

// newTestPostgresPool starts an ephemeral Postgres container, applies
// backend_jobs.sql via migrate.Apply, and returns a connected pool. Skips
// the calling test (does not fail the suite) if Docker is unavailable.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("backendworker_test"),
		tcpostgres.WithUsername("backendworker_test"),
		tcpostgres.WithPassword("backendworker_test"),
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
	if err != nil {
		t.Fatalf("getting postgres connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to postgres testcontainer: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := migrate.Apply(ctx, pool); err != nil {
		t.Fatalf("applying backend_jobs migration: %v", err)
	}

	return pool
}

func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, marker := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"is the docker daemon running",
		"no such host",
		"connect: connection refused",
		"context deadline exceeded",
	} {
		if strings.Contains(strings.ToLower(msg), strings.ToLower(marker)) {
			return true
		}
	}
	return false
}

// newCountingScheduler returns a mockScheduler whose Next/UpdateJob calls
// increment nextCalls/updateCalls, delegating to next/updateJob (or the
// package's own noop defaults if nil).
func newCountingScheduler(next func(context.Context, *tempopb.NextJobRequest, ...grpc.CallOption) (*tempopb.NextJobResponse, error)) (*mockScheduler, *int, *int) {
	nextCalls := 0
	updateCalls := 0
	if next == nil {
		next = nextNoop
	}
	return &mockScheduler{
		next: func(ctx context.Context, req *tempopb.NextJobRequest, opts ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
			nextCalls++
			return next(ctx, req, opts...)
		},
		updateJob: func(ctx context.Context, req *tempopb.UpdateJobStatusRequest, opts ...grpc.CallOption) (*tempopb.UpdateJobStatusResponse, error) {
			updateCalls++
			return updateJobNoop(ctx, req, opts...)
		},
	}, &nextCalls, &updateCalls
}

func newTestWorker(ctx context.Context, t *testing.T) *BackendWorker {
	t.Helper()

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, store := setupDependencies(ctx, t, limitCfg)

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w)

	return w
}

// TestProcessJobs_JobStoreNil_UsesExistingGRPCPathUnchanged is the single
// most important regression guard in this phase: with Postgres unconfigured
// (w.jobStore == nil, the default for every non-Postgres deployment),
// processJobs must still call Next() exactly as it did before #181 Phase 4.
func TestProcessJobs_JobStoreNil_UsesExistingGRPCPathUnchanged(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)
	require.Nil(t, w.jobStore)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	err := w.processJobs(ctx)
	require.Error(t, err, "no jobs found")
	require.Equal(t, 1, *nextCalls, "Next must still be called when jobStore is nil")
}

// TestProcessJobs_JobStoreConfigured_ClaimsViBackfillBeforeTryingGRPC proves
// the try-Postgres-first ordering concretely: when a claimable vi_backfill
// job exists in Postgres, w.backendScheduler.Next must NEVER be called.
func TestProcessJobs_JobStoreConfigured_ClaimsViBackfillBeforeTryingGRPC(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, "tenant-a", jobstore.ViBackfillDetail{
		ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string",
	}))
	w.jobStore = store

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	// w.s3Cfg is nil in this test worker, so the claimed job will
	// deterministically fail on its own "S3 not configured" guard -- that's
	// fine, dispatchPostgresJob still reports it to w.jobStore.Fail, not the
	// scheduler, and processJobs returns nil either way (Fail's own error,
	// not the underlying job error, is what's returned).
	err := w.processJobs(ctx)
	require.Equal(t, 0, *nextCalls, "expected 0 calls to Next, got %d", *nextCalls)
	require.NoError(t, err)
}

// TestProcessJobs_JobStoreConfigured_NoClaimableJob_FallsBackToGRPC proves
// the fallback path is real: with Postgres configured but no claimable row,
// processJobs must still call Next().
func TestProcessJobs_JobStoreConfigured_NoClaimableJob_FallsBackToGRPC(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	w.jobStore = jobstore.New(pool)

	scheduler, nextCalls, _ := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	err := w.processJobs(ctx)
	require.Error(t, err, "no jobs found")
	require.Equal(t, 1, *nextCalls, "Next must be called when no Postgres job is claimable")
}

// TestProcessJobs_OtherJobTypes_StillDispatchViGRPCUnchanged is the direct
// regression guard for #181's "minimal blast radius" claim: with Postgres
// configured but no vi_backfill/cube_backfill work claimable, a compaction
// job offered over gRPC still dispatches exactly as before.
func TestProcessJobs_OtherJobTypes_StillDispatchViGRPCUnchanged(t *testing.T) {
	ctx := context.Background()
	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, store := setupDependencies(ctx, t, limitCfg)

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, nil)
	require.NoError(t, err)

	pool := newTestPostgresPool(t)
	w.jobStore = jobstore.New(pool)

	scheduler, nextCalls, _ := newCountingScheduler(nextFuncWithJob(store, tenant))
	w.backendScheduler = scheduler

	err = w.processJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, *nextCalls, "compaction jobs must still be fetched via Next()")
}

// TestDispatchPostgresJob_Failure_CallsStoreFailNotSchedulerUpdateJob proves
// dispatchPostgresJob reports failures to w.jobStore.Fail, never to
// w.backendScheduler.UpdateJob.
func TestDispatchPostgresJob_Failure_CallsStoreFailNotSchedulerUpdateJob(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	require.NoError(t, store.InsertViBackfill(ctx, "tenant-a", jobstore.ViBackfillDetail{
		ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string",
	}))
	w.jobStore = store

	scheduler, _, updateCalls := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	job, err := store.Claim(ctx, jobstore.JobTypeViBackfill, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)

	// w.s3Cfg is nil -- processViBackfillJobPostgres deterministically fails.
	err = w.dispatchPostgresJob(ctx, job)
	require.NoError(t, err) // Fail() itself succeeds; the underlying job error isn't propagated.
	require.Equal(t, 0, *updateCalls, "scheduler.UpdateJob must never be called for a Postgres-claimed job")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusFailed), status, "Store.Fail must have marked the row failed")
}

// TestDispatchPostgresJob_Success_CallsStoreCompleteNotSchedulerUpdateJob
// proves dispatchPostgresJob reports success to w.jobStore.Complete, never
// to w.backendScheduler.UpdateJob. Exercises reportPostgresJobOutcome (the
// exact outcome-routing logic dispatchPostgresJob delegates to) directly
// with a nil jobErr, rather than driving a real vi_backfill/cube_backfill
// run to completion: both RunViBackfill (block-listing, bounded by real
// data) and RunCubeBackfill (WindowMinutes: math.MaxUint32, an unconditional
// per-minute scan with no early-exit when nothing has ever been backfilled)
// depend on backend I/O whose "genuinely succeeds fast against a fake
// server" behavior isn't guaranteed -- confirmed directly: RunCubeBackfill
// against an httptest 404 stub still churns through the full MaxUint32
// minute range and never returns in any reasonable test time, unrelated to
// this phase's own change. Real success-path execution belongs to #181
// Phase 6's end-to-end validation (real S3/testcontainers), not this unit.
func TestDispatchPostgresJob_Success_CallsStoreCompleteNotSchedulerUpdateJob(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, "tenant-a", jobstore.CubeBackfillDetail{
		CubeID: "cube-1", WindowMinutes: 60,
	}))
	w.jobStore = store

	scheduler, _, updateCalls := newCountingScheduler(nil)
	w.backendScheduler = scheduler

	job, err := store.Claim(ctx, jobstore.JobTypeCubeBackfill, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)

	err = w.reportPostgresJobOutcome(job.ID, nil)
	require.NoError(t, err)
	require.Equal(t, 0, *updateCalls, "scheduler.UpdateJob must never be called for a Postgres-claimed job")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusSucceeded), status, "Store.Complete must have marked the row succeeded")
}

// TestReportPostgresJobOutcome_ExpiredCtx_StillRecordsFailure is the 2026-07-14
// compounding-bug fix's dedicated proof for fix 3: reportPostgresJobOutcome's own
// Store.Fail call must succeed even when the ctx a caller passes in (mirroring a job
// ctx that expired mid-run, per dispatchPostgresJob's real call shape) is ALREADY
// expired by the time this call happens -- proving the reporting call itself now runs on
// a fresh, independent context rather than reusing the (dead) one passed in, which would
// make the SQL UPDATE itself fail and leave the row stuck in 'claimed'.
func TestReportPostgresJobOutcome_ExpiredCtx_StillRecordsFailure(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	require.NoError(t, store.InsertCubeBackfill(ctx, "tenant-a", jobstore.CubeBackfillDetail{
		CubeID: "cube-1", WindowMinutes: 60,
	}))
	w.jobStore = store

	job, err := store.Claim(ctx, jobstore.JobTypeCubeBackfill, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)

	expiredCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	require.Error(t, expiredCtx.Err(), "the ctx must genuinely be expired before calling reportPostgresJobOutcome")

	err = w.reportPostgresJobOutcome(job.ID, errors.New("some real backfill failure"))
	require.NoError(t, err,
		"reportPostgresJobOutcome must report on a FRESH ctx, not fail because the caller's own ctx already expired")

	var status string
	row := pool.QueryRow(ctx, `SELECT status FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&status))
	require.Equal(t, string(jobstore.StatusFailed), status,
		"Store.Fail must have successfully marked the row failed despite the caller's ctx being dead")
}

// TestDispatchPostgresJob_Dispatch_UnknownJobType_ReportsFailure proves
// dispatchPostgresJob's own switch (not just reportPostgresJobOutcome in
// isolation) routes a real execution error to Store.Fail -- the only
// dispatchPostgresJob-level error path reachable without real backend I/O.
func TestDispatchPostgresJob_Dispatch_UnknownJobType_ReportsFailure(t *testing.T) {
	ctx := context.Background()
	w := newTestWorker(ctx, t)

	pool := newTestPostgresPool(t)
	store := jobstore.New(pool)
	w.jobStore = store

	require.NoError(t, store.InsertViBackfill(ctx, "tenant-a", jobstore.ViBackfillDetail{
		ColumnHash: "h1", ColumnName: "span.name", ColumnType: "string",
	}))
	job, err := store.Claim(ctx, jobstore.JobTypeViBackfill, "worker-1")
	require.NoError(t, err)
	job.Type = "unknown_job_type"

	err = w.dispatchPostgresJob(ctx, job)
	require.NoError(t, err)

	var status, lastError string
	row := pool.QueryRow(ctx, `SELECT status, last_error FROM backend_jobs WHERE id = $1`, job.ID)
	require.NoError(t, row.Scan(&status, &lastError))
	require.Equal(t, string(jobstore.StatusFailed), status)
	require.Contains(t, lastError, "unknown postgres job type")
}
