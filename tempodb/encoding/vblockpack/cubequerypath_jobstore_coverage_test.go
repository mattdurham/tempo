package vblockpack

// cubequerypath_jobstore_coverage_test.go — #181 §6.3/§9 Phase 3: the cube creation trigger's
// "already exists" (Created=false) branch must also insert a durable retry job when the
// existing cube has never completed a single backfill pass (no CubeRollupL0 watermark yet) --
// closing the coverage gap identified in §5.3 once cube_backfill's poll is gone.
//
// #508 migration: this branch's logic (and its own jobStore.InsertCubeBackfill call) moved from
// maybeCreateCube (this package) into blockpack.CubeQueryPath's internal trigger, surfaced back
// to tempo only via cubequerypath.go's OnCreateAttempt callback (created=false, hasL0 bool).
// These tests can no longer call a private maybeCreateCube method directly -- they now go
// through the REAL ConfigureCubeQueryPath + tryQueryFromCube production entry points (mirrors
// backend_jobs_e2e_test.go's own #508 migration), driving a query whose window does NOT overlap
// the seeded entry's watermark (so Route declines and the trigger re-evaluation fires) for the
// SAME registry entry identity TryCreate will recognize as "already exists".
//
// 2026-07-15 migration (issue #504): cube's registry is Postgres-only now (no
// blob/index.json fallback) -- these tests seed the pre-existing entry into a real ephemeral
// Postgres instance (newTestPostgresPool, shared with pg_entrystore_test.go).
//
// 2026-07-21 migration (issue #522): the durable job insert now targets blockpack's own
// compaction_jobs queue (pg.InsertCubeBackfillJob) instead of tempo's retired backend_jobs/
// jobstore -- newTestPostgresPool already applies every schema blockpack owns, so no separate
// migration call is needed, and cqp.jobStore (deleted along with jobstore) is gone; the
// duplicate-insert test now seeds directly via cqp.pg.InsertCubeBackfillJob instead.

import (
	"context"
	"sync"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// existingCubeEntry builds a RegistryEntry that TryCreate will recognize as "already
// exists" for (tenant, dims, nil filters) -- the trigger always calls TryCreate with
// only the mandatory duration aggAttr when neededAttrOK is false (buildAggAttrs), so the
// seeded entry's AggAttrs/CubeID must match that exact shape for TryCreate's cubeID
// comparison (trigger.go's `c.CubeID == cubeID` check) to find it.
func existingCubeEntry(tenant string, dims []string, watermarks map[uint32]blockpack.CubeResolutionWatermark) blockpack.CubeRegistryEntry {
	aggAttrs := []string{blockpack.CubeDurationColumn}
	return blockpack.CubeRegistryEntry{
		CubeID:     blockpack.CubeComputeID(tenant, dims, nil, aggAttrs),
		Tenant:     tenant,
		Dimensions: dims,
		AggAttrs:   aggAttrs,
		Resolution: 1,
		CreatedAt:  uint32(time.Now().Add(-time.Hour).Unix()), //nolint:gosec // test fixture timestamp
		Watermarks: watermarks,
	}
}

// TestMaybeCreateCube_AlreadyExists_NoL0Watermark_InsertsRetryJob is the go/no-go fix from
// §6.3: a cube that already exists but has never completed a backfill pass (no
// CubeRollupL0 watermark) gets a durable retry job inserted on this query-path
// re-evaluation, closing the gap left by removing cube_backfill's poll (§5.3).
func TestMaybeCreateCube_AlreadyExists_NoL0Watermark_InsertsRetryJob(t *testing.T) {
	pool := newTestPostgresPool(t)

	tenant := "tenant-cube-nowatermark"
	dims := []string{"resource.service.name"}
	entry := existingCubeEntry(tenant, dims, nil) // no watermarks at all -> no L0 entry
	require.NoError(t, blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant).Add(context.Background(), entry))

	resetCubeQueryPathSingleton(t)
	ConfigureCubeQueryPath(true, newFakeS3Config(t, "e2e-nowatermark-bucket"), blockpack.NewPostgresFromPool(pool))
	cqp := getCubeQueryPath()
	require.NotNil(t, cqp)

	now := time.Now()
	req := &tempopb.QueryRangeRequest{
		Query: `{} | count_over_time() by (resource.service.name)`,
		Start: uint64(now.Add(-time.Hour).UnixNano()),
		End:   uint64(now.UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, _, err := cqp.tryQueryFromCube(context.Background(), tenant, req)
	require.Error(t, err)

	var count int
	require.Eventually(t, func() bool {
		row := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM compaction_jobs WHERE job_type = 'cube_backfill' AND tenant = $1 AND status = 'pending'`, tenant)
		return row.Scan(&count) == nil && count == 1
	}, 5*time.Second, 10*time.Millisecond, "expected a durable retry job to be inserted for a never-backfilled existing cube")
	assert.Equal(t, 1, count)
}

// TestMaybeCreateCube_AlreadyExists_HasL0Watermark_NoInsert is the negative case: a cube
// that has already completed at least one backfill pass (has a CubeRollupL0 watermark) is
// healthy -- re-inserting a job for it on every subsequent query would spam retries for a
// cube that's fine, so no insert must happen. The query window (minutes 5000-5010) deliberately
// does NOT overlap the seeded watermark (100-200), so Route still declines and the trigger
// re-evaluation still fires -- genuinely exercising the hasL0=true branch, not merely skipping
// evaluation because Route already found full coverage.
func TestMaybeCreateCube_AlreadyExists_HasL0Watermark_NoInsert(t *testing.T) {
	pool := newTestPostgresPool(t)

	tenant := "tenant-cube-haswatermark"
	dims := []string{"resource.service.name"}
	watermarks := map[uint32]blockpack.CubeResolutionWatermark{
		blockpack.CubeRollupL0: {MinMinute: 100, MaxMinute: 200},
	}
	entry := existingCubeEntry(tenant, dims, watermarks)
	require.NoError(t, blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant).Add(context.Background(), entry))

	resetCubeQueryPathSingleton(t)
	ConfigureCubeQueryPath(true, newFakeS3Config(t, "e2e-haswatermark-bucket"), blockpack.NewPostgresFromPool(pool))
	cqp := getCubeQueryPath()
	require.NotNil(t, cqp)

	req := &tempopb.QueryRangeRequest{
		Query: `{} | count_over_time() by (resource.service.name)`,
		Start: 5000 * 60 * 1_000_000_000,
		End:   5010 * 60 * 1_000_000_000,
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, _, err := cqp.tryQueryFromCube(context.Background(), tenant, req)
	require.Error(t, err, "the non-overlapping window must still decline (ErrCubeWarming), triggering re-evaluation")

	// Give the background goroutine time to run and (not) insert.
	time.Sleep(200 * time.Millisecond)

	var count int
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM compaction_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 0, count, "a cube that already completed a backfill pass must not get a spurious retry job")
}

// TestMaybeCreateCube_AlreadyExists_NoL0Watermark_ExistingPendingJobIsNotDuplicated proves
// the idempotent-insert interaction at this specific call site: if a durable job is already
// pending for this cube (e.g. from its original creation), re-triggering the "already
// exists, no L0 watermark" path must not create a second row -- the partial unique dedup
// index (pgqueue) absorbs it as a no-op.
func TestMaybeCreateCube_AlreadyExists_NoL0Watermark_ExistingPendingJobIsNotDuplicated(t *testing.T) {
	pool := newTestPostgresPool(t)

	tenant := "tenant-cube-dup"
	dims := []string{"resource.service.name"}
	entry := existingCubeEntry(tenant, dims, nil)
	require.NoError(t, blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant).Add(context.Background(), entry))

	resetCubeQueryPathSingleton(t)
	ConfigureCubeQueryPath(true, newFakeS3Config(t, "e2e-dup-bucket"), blockpack.NewPostgresFromPool(pool))
	cqp := getCubeQueryPath()
	require.NotNil(t, cqp)
	require.NotNil(t, cqp.pg)
	require.NoError(t, cqp.pg.InsertCubeBackfillJob(context.Background(), tenant, blockpack.CubeBackfillDetail{
		CubeID: entry.CubeID, WindowMinutes: 42,
	}))

	now := time.Now()
	req := &tempopb.QueryRangeRequest{
		Query: `{} | count_over_time() by (resource.service.name)`,
		Start: uint64(now.Add(-time.Hour).UnixNano()),
		End:   uint64(now.UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, _, err := cqp.tryQueryFromCube(context.Background(), tenant, req)
	require.Error(t, err)

	// Give the background goroutine time to run and attempt its (deduped) insert.
	time.Sleep(200 * time.Millisecond)

	var count int
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM compaction_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 1, count, "re-triggering must not duplicate an already-pending job for the same cube")
}

// TestOnCreateAttempt_CreatedBranch_RecordsCubeColumnUsage (#511 Step 2D.1): OnCreateAttempt's
// created==true branch must call recordCubeColumnUsage for the newly-created cube's dims and
// AggAttrs, giving Fix 1's LookupColumn(DurationColumn)-anchored backfill data to actually find.
func TestOnCreateAttempt_CreatedBranch_RecordsCubeColumnUsage(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)
	withDedicatedColumnsLookup(t, func(string) backend.DedicatedColumns { return nil })

	pool := newTestPostgresPool(t)

	resetCubeQueryPathSingleton(t)
	ConfigureCubeQueryPath(true, newFakeS3Config(t, "e2e-oncreate-usage-bucket"), blockpack.NewPostgresFromPool(pool))
	cqp := getCubeQueryPath()
	require.NotNil(t, cqp)

	tenant := "tenant-oncreate-usage"
	now := time.Now()
	req := &tempopb.QueryRangeRequest{
		Query: `{} | count_over_time() by (resource.service.name)`,
		Start: uint64(now.Add(-time.Hour).UnixNano()),
		End:   uint64(now.UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, _, err := cqp.tryQueryFromCube(context.Background(), tenant, req)
	require.Error(t, err, "creation triggered on a genuinely empty registry")

	require.Eventually(t, func() bool {
		cols := rec.columns()
		hasDim, hasDuration := false, false
		for _, c := range cols {
			if c == "resource.service.name" {
				hasDim = true
			}
			if c == blockpack.CubeDurationColumn {
				hasDuration = true
			}
		}
		return hasDim && hasDuration
	}, 5*time.Second, 10*time.Millisecond,
		"expected recordCubeColumnUsage to eventually record both the dim and CubeDurationColumn")
}

// TestOnCreateAttempt_ReEvaluationBranch_RecordsCubeColumnUsage (#511 Step 2D.1): mirrors
// TestMaybeCreateCube_AlreadyExists_NoL0Watermark_InsertsRetryJob's exact setup -- usage must
// also be recorded on the re-evaluation branch (a stalled, never-backfilled existing cube), not
// just on first creation.
func TestOnCreateAttempt_ReEvaluationBranch_RecordsCubeColumnUsage(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)
	withDedicatedColumnsLookup(t, func(string) backend.DedicatedColumns { return nil })

	pool := newTestPostgresPool(t)

	tenant := "tenant-reeval-usage"
	dims := []string{"resource.service.name"}
	entry := existingCubeEntry(tenant, dims, nil) // no watermarks at all -> no L0 entry
	require.NoError(t, blockpack.NewPostgresFromPool(pool).CubeRegistry(tenant).Add(context.Background(), entry))

	resetCubeQueryPathSingleton(t)
	ConfigureCubeQueryPath(true, newFakeS3Config(t, "e2e-reeval-usage-bucket"), blockpack.NewPostgresFromPool(pool))
	cqp := getCubeQueryPath()
	require.NotNil(t, cqp)

	now := time.Now()
	req := &tempopb.QueryRangeRequest{
		Query: `{} | count_over_time() by (resource.service.name)`,
		Start: uint64(now.Add(-time.Hour).UnixNano()),
		End:   uint64(now.UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, _, err := cqp.tryQueryFromCube(context.Background(), tenant, req)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		cols := rec.columns()
		hasDim, hasDuration := false, false
		for _, c := range cols {
			if c == "resource.service.name" {
				hasDim = true
			}
			if c == blockpack.CubeDurationColumn {
				hasDuration = true
			}
		}
		return hasDim && hasDuration
	}, 5*time.Second, 10*time.Millisecond,
		"expected recordCubeColumnUsage to eventually record both the dim and CubeDurationColumn on re-evaluation too")
}

// resetCubeQueryPathSingleton clears the process-level cube query path singleton for the
// duration of the test, restoring the previous value on cleanup -- shared by all three tests
// above, each of which needs its own fresh ConfigureCubeQueryPath call.
func resetCubeQueryPathSingleton(t *testing.T) {
	t.Helper()
	processCubeQueryPathMu.Lock()
	prev := processCubeQueryPath
	processCubeQueryPath = nil
	processCubeQueryPathMu.Unlock()
	cubeQueryPathOnce = sync.Once{}
	t.Cleanup(func() {
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = prev
		processCubeQueryPathMu.Unlock()
	})
}
