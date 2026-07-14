package vblockpack

// cubequerypath_jobstore_coverage_test.go — #181 §6.3/§9 Phase 3: maybeCreateCube's
// "already exists" (Created=false) branch must also insert a durable retry job when the
// existing cube has never completed a single backfill pass (no CubeRollupL0 watermark
// yet) -- closing the coverage gap identified in §5.3 once cube_backfill's poll is gone.
// Uses seedCubeEntry/fakeCubeRegistryObjectStore (cube_backfill_watermark_test.go, same
// package) to register a pre-existing entry so TryCreate's "already exists" branch fires.

import (
	"context"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// existingCubeEntry builds a RegistryEntry that TryCreate will recognize as "already
// exists" for (tenant, dims, nil filters) -- maybeCreateCube always calls TryCreate with
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
	require.NoError(t, migrate.Apply(context.Background(), pool))

	store := &fakeCubeRegistryObjectStore{}
	tenant := "tenant-cube-nowatermark"
	dims := []string{"resource.service.name"}
	entry := existingCubeEntry(tenant, dims, nil) // no watermarks at all -> no L0 entry
	seedCubeEntry(t, store, entry)

	cqp := &cubeQueryPath{
		store:      store,
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		jobStore:   jobstore.New(pool),
	}

	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())
	cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)

	var count int
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1 AND status = 'pending'`, tenant)
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 1, count, "expected a durable retry job to be inserted for a never-backfilled existing cube")
}

// TestMaybeCreateCube_AlreadyExists_HasL0Watermark_NoInsert is the negative case: a cube
// that has already completed at least one backfill pass (has a CubeRollupL0 watermark) is
// healthy -- re-inserting a job for it on every subsequent query would spam retries for a
// cube that's fine, so no insert must happen.
func TestMaybeCreateCube_AlreadyExists_HasL0Watermark_NoInsert(t *testing.T) {
	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	store := &fakeCubeRegistryObjectStore{}
	tenant := "tenant-cube-haswatermark"
	dims := []string{"resource.service.name"}
	watermarks := map[uint32]blockpack.CubeResolutionWatermark{
		blockpack.CubeRollupL0: {MinMinute: 100, MaxMinute: 200},
	}
	entry := existingCubeEntry(tenant, dims, watermarks)
	seedCubeEntry(t, store, entry)

	cqp := &cubeQueryPath{
		store:      store,
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		jobStore:   jobstore.New(pool),
	}

	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())
	cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)

	var count int
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 0, count, "a cube that already completed a backfill pass must not get a spurious retry job")
}

// TestMaybeCreateCube_AlreadyExists_NoL0Watermark_ExistingPendingJobIsNotDuplicated proves
// the idempotent-insert interaction at this specific call site: if a durable job is already
// pending for this cube (e.g. from its original creation), re-triggering the "already
// exists, no L0 watermark" path must not create a second row -- the partial unique dedup
// index (jobstore) absorbs it as a no-op.
func TestMaybeCreateCube_AlreadyExists_NoL0Watermark_ExistingPendingJobIsNotDuplicated(t *testing.T) {
	pool := newTestPostgresPool(t)
	require.NoError(t, migrate.Apply(context.Background(), pool))

	store := &fakeCubeRegistryObjectStore{}
	tenant := "tenant-cube-dup"
	dims := []string{"resource.service.name"}
	entry := existingCubeEntry(tenant, dims, nil)
	seedCubeEntry(t, store, entry)

	js := jobstore.New(pool)
	require.NoError(t, js.InsertCubeBackfill(context.Background(), tenant, jobstore.CubeBackfillDetail{
		CubeID: entry.CubeID, WindowMinutes: 42,
	}))

	cqp := &cubeQueryPath{
		store:      store,
		tenants:    make(map[string]*tenantCubeState),
		createSeen: make(map[string]time.Time),
		jobStore:   js,
	}

	now := time.Now()
	minTS, maxTS := uint64(now.Add(-time.Hour).Unix()), uint64(now.Unix())
	cqp.maybeCreateCube(context.Background(), tenant, dims, nil, "", blockpack.CubeAggAttrTypeFloat64, false, minTS, maxTS)

	var count int
	row := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM backend_jobs WHERE job_type = 'cube_backfill' AND tenant = $1`, tenant)
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 1, count, "re-triggering must not duplicate an already-pending job for the same cube")
}
