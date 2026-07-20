package filecatalog

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
)

// newTestBlockMeta defaults Version to "vblockpack" (issue #522 #158's Lister fix scopes
// reconciliation to vblockpack-encoded blocks only) -- every existing test in this file is
// exercising that path, not the "vparquet block must be skipped" case, which has its own
// dedicated test below.
func newTestBlockMeta(blockID uuid.UUID, start, end time.Time, size uint64) *backend.BlockMeta {
	return &backend.BlockMeta{
		BlockID:   backend.UUID(blockID),
		StartTime: start,
		EndTime:   end,
		Size_:     size,
		Version:   "vblockpack",
	}
}

func rowCount(t *testing.T, pool *pgxpool.Pool, tenant string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND deleted_at IS NULL`, tenant).Scan(&n))
	return n
}

func TestLister_RunOnce_InsertsNewBlocks(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	blockID1 := uuid.New()
	blockID2 := uuid.New()
	metas := []*backend.BlockMeta{
		newTestBlockMeta(blockID1, time.Unix(100, 0), time.Unix(200, 0), 1000),
		newTestBlockMeta(blockID2, time.Unix(150, 0), time.Unix(250, 0), 2000),
	}

	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return metas },
		func() []string { return []string{tenant} },
	)

	require.NoError(t, lister.RunOnce(ctx))
	require.Equal(t, 2, rowCount(t, pool, tenant), "both blocks should be inserted as live rows")

	var blockRef string
	var startSec, endSec, sizeBytes int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT block_ref, start_sec, end_sec, size_bytes FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID1.String(),
	).Scan(&blockRef, &startSec, &endSec, &sizeBytes))
	require.Equal(t, tenant+"/"+blockID1.String()+"/data.blockpack", blockRef)
	require.Equal(t, int64(100), startSec)
	require.Equal(t, int64(200), endSec)
	require.Equal(t, int64(1000), sizeBytes)
}

func TestLister_RunOnce_SoftDeletesVanishedBlocks(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	blockID1 := uuid.New()
	blockID2 := uuid.New()
	metas := []*backend.BlockMeta{
		newTestBlockMeta(blockID1, time.Unix(100, 0), time.Unix(200, 0), 1000),
		newTestBlockMeta(blockID2, time.Unix(150, 0), time.Unix(250, 0), 2000),
	}
	currentMetas := metas

	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return currentMetas },
		func() []string { return []string{tenant} },
	)
	require.NoError(t, lister.RunOnce(ctx))
	require.Equal(t, 2, rowCount(t, pool, tenant))

	// blockID2 has vanished (e.g. compacted away) -- blockMetas no longer returns it.
	currentMetas = []*backend.BlockMeta{metas[0]}
	require.NoError(t, lister.RunOnce(ctx))

	require.Equal(t, 1, rowCount(t, pool, tenant), "vanished block must no longer count as live")

	var deletedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT deleted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID2.String(),
	).Scan(&deletedAt))
	require.NotNil(t, deletedAt, "vanished block's row must be soft-deleted (deleted_at set), not physically removed")

	// Confirm it's a SOFT delete: the row still physically exists.
	var stillExists int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID2.String(),
	).Scan(&stillExists))
	require.Equal(t, 1, stillExists, "soft-deleted row must still physically exist")
}

func TestLister_RunOnce_ReappearedBlock_ClearsDeletedAt(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	blockID := uuid.New()
	meta := newTestBlockMeta(blockID, time.Unix(100, 0), time.Unix(200, 0), 1000)

	var currentMetas []*backend.BlockMeta
	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return currentMetas },
		func() []string { return []string{tenant} },
	)

	// First pass: block present.
	currentMetas = []*backend.BlockMeta{meta}
	require.NoError(t, lister.RunOnce(ctx))
	require.Equal(t, 1, rowCount(t, pool, tenant))

	// Second pass: block vanishes -- soft-deleted.
	currentMetas = nil
	require.NoError(t, lister.RunOnce(ctx))
	require.Equal(t, 0, rowCount(t, pool, tenant))

	var deletedAtBefore *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT deleted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID.String(),
	).Scan(&deletedAtBefore))
	require.NotNil(t, deletedAtBefore)

	// Third pass: block reappears (defensive edge case) -- deleted_at must clear.
	currentMetas = []*backend.BlockMeta{meta}
	require.NoError(t, lister.RunOnce(ctx))
	require.Equal(t, 1, rowCount(t, pool, tenant))

	var deletedAtAfter *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT deleted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID.String(),
	).Scan(&deletedAtAfter))
	require.Nil(t, deletedAtAfter, "a reappeared block's deleted_at must be cleared")
}

// TestLister_RunOnce_SkipsNonVblockpackBlocks is issue #522 #158's regression test: a
// vparquet/standard-encoded block must never be reconciled into file_catalog at all -- not
// skipped-with-a-warning, not inserted with a wrong block_ref, simply absent -- while a
// vblockpack-encoded block in the SAME tenant/RunOnce call is still reconciled normally. This is
// what gives file_catalog its "vblockpack-encoded tenants only" scoping property by
// construction (#158's job-planner candidate query depends on it), and closes a real,
// independent pre-existing bug where every block regardless of encoding got a hardcoded
// ".../data.blockpack" block_ref, which is only ever correct for vblockpack blocks.
func TestLister_RunOnce_SkipsNonVblockpackBlocks(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	vblockpackID := uuid.New()
	vparquetID := uuid.New()
	vblockpackMeta := newTestBlockMeta(vblockpackID, time.Unix(100, 0), time.Unix(200, 0), 1000)
	vparquetMeta := &backend.BlockMeta{
		BlockID:   backend.UUID(vparquetID),
		StartTime: time.Unix(100, 0),
		EndTime:   time.Unix(200, 0),
		Size_:     1000,
		Version:   "vParquet4",
	}

	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return []*backend.BlockMeta{vblockpackMeta, vparquetMeta} },
		func() []string { return []string{tenant} },
	)
	require.NoError(t, lister.RunOnce(ctx))

	require.Equal(t, 1, rowCount(t, pool, tenant), "only the vblockpack-encoded block must be reconciled")

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, vparquetID.String(),
	).Scan(&count))
	require.Zero(t, count, "the vparquet-encoded block must never appear in file_catalog at all")

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, vblockpackID.String(),
	).Scan(&count))
	require.Equal(t, 1, count, "the vblockpack-encoded block must still be reconciled normally")
}

// TestLister_RunOnce_NonVblockpackBlocks_BumpsSanityCheckMetric is issue #522 #159's safety-net
// regression test: the poller flip's deployment-level gate assumes no tenant ever mixes
// encodings -- confirmed for this deployment, but reconcileTenant must still make a future
// violation of that assumption loud (metric + log), not silent, since it's the exact precondition
// for the "leftover blocks become invisible to the poller" failure mode this check exists to
// catch.
func TestLister_RunOnce_NonVblockpackBlocks_BumpsSanityCheckMetric(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-mixed-encoding-" + uuid.New().String()

	vblockpackID := uuid.New()
	vparquetID1 := uuid.New()
	vparquetID2 := uuid.New()
	vblockpackMeta := newTestBlockMeta(vblockpackID, time.Unix(100, 0), time.Unix(200, 0), 1000)
	vparquetMeta1 := &backend.BlockMeta{BlockID: backend.UUID(vparquetID1), Version: "vParquet4"}
	vparquetMeta2 := &backend.BlockMeta{BlockID: backend.UUID(vparquetID2), Version: "vParquet4"}

	before := testutil.ToFloat64(metricUnexpectedNonVblockpackBlocks.WithLabelValues(tenant))

	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return []*backend.BlockMeta{vblockpackMeta, vparquetMeta1, vparquetMeta2} },
		func() []string { return []string{tenant} },
	)
	require.NoError(t, lister.RunOnce(ctx))

	after := testutil.ToFloat64(metricUnexpectedNonVblockpackBlocks.WithLabelValues(tenant))
	require.Equal(t, float64(2), after-before, "both non-vblockpack blocks must bump the sanity-check metric")
}

// TestLister_RunOnce_AllVblockpackBlocks_DoesNotBumpSanityCheckMetric proves the check is
// silent (no false positives) for the expected, homogeneous case.
func TestLister_RunOnce_AllVblockpackBlocks_DoesNotBumpSanityCheckMetric(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-homogeneous-" + uuid.New().String()

	meta := newTestBlockMeta(uuid.New(), time.Unix(100, 0), time.Unix(200, 0), 1000)
	before := testutil.ToFloat64(metricUnexpectedNonVblockpackBlocks.WithLabelValues(tenant))

	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return []*backend.BlockMeta{meta} },
		func() []string { return []string{tenant} },
	)
	require.NoError(t, lister.RunOnce(ctx))

	after := testutil.ToFloat64(metricUnexpectedNonVblockpackBlocks.WithLabelValues(tenant))
	require.Equal(t, before, after, "an all-vblockpack tenant must never bump the sanity-check metric")
}

// TestLister_RunOnce_WritesRealCompactionLevel is issue #522 #159's regression test: the real
// BlockMeta.CompactionLevel must round-trip into file_catalog.compaction_level, closing #158's
// "nothing ever writes a real level, every row sits at the schema default 0" gap for this
// reconciliation path. Also proves a re-poll updates compaction_level for an existing row (a
// block observed at level 0 and later re-observed at level 1, e.g. after being merged into a
// higher-level output under a fresh compaction pass while retaining its own BlockID -- edge
// case aside, the mechanism must be a real UPDATE, not just an on-INSERT-only default).
func TestLister_RunOnce_WritesRealCompactionLevel(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	blockID := uuid.New()
	meta := newTestBlockMeta(blockID, time.Unix(100, 0), time.Unix(200, 0), 1000)
	meta.CompactionLevel = 2

	var currentMetas []*backend.BlockMeta
	lister := NewLister(pool,
		func(string) []*backend.BlockMeta { return currentMetas },
		func() []string { return []string{tenant} },
	)

	currentMetas = []*backend.BlockMeta{meta}
	require.NoError(t, lister.RunOnce(ctx))

	var level int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT compaction_level FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID.String(),
	).Scan(&level))
	require.Equal(t, 2, level, "the real BlockMeta.CompactionLevel must round-trip, not the schema default 0")

	// Re-poll with a higher level for the same block -- must UPDATE, not just insert-once.
	meta.CompactionLevel = 5
	require.NoError(t, lister.RunOnce(ctx))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT compaction_level FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, blockID.String(),
	).Scan(&level))
	require.Equal(t, 5, level, "a re-poll must update compaction_level to the block's current real level")
}

// TestLister_Close_ClosesUnderlyingPool is the regression guard for the
// pgxpool leak this fix (task #179) addresses: BackendScheduler.stopping()
// must be able to close the file-catalog Postgres pool on shutdown, mirroring
// tempodb.go's readerWriter.Shutdown() closing its own pgPool. Asserts the
// pool genuinely stops accepting queries after Close() -- not just that
// Close() doesn't panic.
func TestLister_Close_ClosesUnderlyingPool(t *testing.T) {
	pool := newTestPostgresPool(t)
	lister := NewLister(pool, func(string) []*backend.BlockMeta { return nil }, func() []string { return nil })

	require.NoError(t, pool.Ping(context.Background()), "pool must be usable before Close")

	lister.Close()

	err := pool.Ping(context.Background())
	require.Error(t, err, "pool must reject new queries after Close()")
}

// TestLister_Close_NilSafe confirms Close() is safe to call on both a nil
// *Lister and a Lister with a nil pool -- BackendScheduler.stopping() calls
// s.catalogLister.Close() unconditionally, relying on this nil-safety when
// cfg.Postgres was never configured.
func TestLister_Close_NilSafe(t *testing.T) {
	var nilLister *Lister
	require.NotPanics(t, nilLister.Close)

	listerWithNilPool := NewLister(nil, nil, nil)
	require.NotPanics(t, listerWithNilPool.Close)
}
