package filecatalog

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
)

func newTestBlockMeta(blockID uuid.UUID, start, end time.Time, size uint64) *backend.BlockMeta {
	return &backend.BlockMeta{
		BlockID:   backend.UUID(blockID),
		StartTime: start,
		EndTime:   end,
		Size_:     size,
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
