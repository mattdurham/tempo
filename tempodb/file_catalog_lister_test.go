package tempodb

// file_catalog_lister_test.go — TDD coverage for issue #522 #159's Postgres-backed
// fileCatalogBlockLister: proves ListBlockIDs correctly partitions live vs. compacted rows and
// excludes soft-deleted (deleted_at set) rows entirely, against a real Postgres instance and the
// real file_catalog schema.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/schema"
)

// newTestFileCatalogPool starts a real Postgres testcontainer with file_catalog.sql applied,
// for this file's own fileCatalogBlockLister tests.
func newTestFileCatalogPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := newTestPostgresPool(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, schema.ApplyFileCatalog(context.Background(), pool))
	return pool
}

func insertFileCatalogTestRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tenant, blockID string, compacted, deleted bool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, size_bytes, compacted_at, deleted_at)
		VALUES ($1, $2, $2, 100, 200, 1000,
			CASE WHEN $3 THEN now() ELSE NULL END,
			CASE WHEN $4 THEN now() ELSE NULL END)`,
		tenant, blockID, compacted, deleted,
	)
	require.NoError(t, err)
}

func TestFileCatalogBlockLister_PartitionsLiveAndCompacted(t *testing.T) {
	pool := newTestFileCatalogPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	liveID := uuid.New()
	compactedID := uuid.New()
	insertFileCatalogTestRow(ctx, t, pool, tenant, liveID.String(), false, false)
	insertFileCatalogTestRow(ctx, t, pool, tenant, compactedID.String(), true, false)

	lister := newFileCatalogBlockLister(pool)
	live, compacted, err := lister.ListBlockIDs(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{liveID}, live)
	require.Equal(t, []uuid.UUID{compactedID}, compacted)
}

func TestFileCatalogBlockLister_ExcludesSoftDeletedRows(t *testing.T) {
	pool := newTestFileCatalogPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	deletedID := uuid.New()
	insertFileCatalogTestRow(ctx, t, pool, tenant, deletedID.String(), false, true)

	lister := newFileCatalogBlockLister(pool)
	live, compacted, err := lister.ListBlockIDs(ctx, tenant)
	require.NoError(t, err)
	require.Empty(t, live, "a soft-deleted (vanished from filesystem) row must never be returned as live")
	require.Empty(t, compacted, "a soft-deleted row must never be returned as compacted either")
}

func TestFileCatalogBlockLister_EmptyTenantReturnsEmpty(t *testing.T) {
	pool := newTestFileCatalogPool(t)
	lister := newFileCatalogBlockLister(pool)
	live, compacted, err := lister.ListBlockIDs(context.Background(), "no-such-tenant")
	require.NoError(t, err)
	require.Empty(t, live)
	require.Empty(t, compacted)
}
