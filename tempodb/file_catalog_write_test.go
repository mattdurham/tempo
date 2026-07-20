package tempodb

// file_catalog_write_test.go — TDD coverage for issue #522 #159's direct-write-primary mirror
// (writeCompactionToFileCatalog/markFileCatalogDeleted). Constructs a real *readerWriter via
// New() with a real Postgres pool, mirroring TestNew_CacheProviderConfigured_RegistriesGetUnwrappedRawBackend's
// established pattern for cfg.Postgres-configured tests in this package.

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
)

// newTestReaderWriter constructs a real *readerWriter for this file's tests, with
// cfg.Block.Version set to version (either "vblockpack" or a non-vblockpack encoding) and
// cfg.Postgres set to a real, connectable Postgres instance whenever withPostgres is true.
func newTestReaderWriter(t *testing.T, version string, withPostgres bool) *readerWriter {
	t.Helper()
	tempDir := t.TempDir()
	cfg := &Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: path.Join(tempDir, "traces")},
		Block: &common.BlockConfig{
			BloomFP:             .01,
			BloomShardSizeBytes: 100_000,
			Version:             version,
		},
		WAL:           &wal.Config{Filepath: path.Join(tempDir, "wal")},
		BlocklistPoll: 0,
		Search: &SearchConfig{
			ChunkSizeBytes:  1_000_000,
			ReadBufferCount: 8, ReadBufferSizeBytes: 4 * 1024 * 1024,
		},
	}
	if withPostgres {
		cfg.Postgres = &postgres.Config{DSN: newTestPostgresPool(t)}
	}

	r, _, _, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)
	rw, ok := r.(*readerWriter)
	require.True(t, ok, "New() must return a *readerWriter under the Reader interface")
	return rw
}

func newTestBlockMetaWithLevel(level uint32, startSec, endSec int64, size uint64) *backend.BlockMeta {
	return &backend.BlockMeta{
		BlockID:         backend.NewUUID(),
		StartTime:       time.Unix(startSec, 0),
		EndTime:         time.Unix(endSec, 0),
		Size_:           size,
		CompactionLevel: level,
	}
}

// TestWriteCompactionToFileCatalog_InsertsOutputAndMarksInputsCompacted proves the mandatory
// direct-write-primary sequence: a live row for the new output block (with its real
// CompactionLevel), and compacted_at set on each old input block's row.
func TestWriteCompactionToFileCatalog_InsertsOutputAndMarksInputsCompacted(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()
	tenant := "tenant-a"

	oldA := newTestBlockMetaWithLevel(0, 100, 200, 1000)
	oldB := newTestBlockMetaWithLevel(0, 150, 250, 1000)
	newOut := newTestBlockMetaWithLevel(1, 100, 250, 2000)

	// Old blocks must already exist as live rows for compacted_at to have something real to
	// update (mirrors production: markCompacted only ever runs on blocks the poller already
	// discovered and wrote here, via either this same mechanism or filecatalog.Lister).
	rw.writeCompactionToFileCatalog(ctx, tenant, nil, []*backend.BlockMeta{oldA, oldB})

	rw.writeCompactionToFileCatalog(ctx, tenant, []*backend.BlockMeta{oldA, oldB}, []*backend.BlockMeta{newOut})

	var level int
	var compactedAt *time.Time
	require.NoError(t, rw.pgPool.QueryRow(ctx,
		`SELECT compaction_level, compacted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, newOut.BlockID.String(),
	).Scan(&level, &compactedAt))
	require.Equal(t, 1, level, "the new output row must carry its real CompactionLevel")
	require.Nil(t, compactedAt, "a freshly-inserted output row must be live (compacted_at NULL)")

	for _, old := range []*backend.BlockMeta{oldA, oldB} {
		require.NoError(t, rw.pgPool.QueryRow(ctx,
			`SELECT compacted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
			tenant, old.BlockID.String(),
		).Scan(&compactedAt))
		require.NotNil(t, compactedAt, "old input block %s must be marked compacted", old.BlockID)
	}
}

// TestWriteCompactionToFileCatalog_NoOpWhenNotVblockpack proves the deployment-level gate: a
// non-vblockpack deployment (cfg.Block.Version != "vblockpack") must never write to
// file_catalog at all, even with Postgres configured.
func TestWriteCompactionToFileCatalog_NoOpWhenNotVblockpack(t *testing.T) {
	rw := newTestReaderWriter(t, "vParquet4", true)
	ctx := context.Background()
	tenant := "tenant-a"

	newOut := newTestBlockMetaWithLevel(0, 100, 200, 1000)
	rw.writeCompactionToFileCatalog(ctx, tenant, nil, []*backend.BlockMeta{newOut})

	var count int
	require.NoError(t, rw.pgPool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, newOut.BlockID.String(),
	).Scan(&count))
	require.Zero(t, count, "a non-vblockpack deployment must never write to file_catalog")
}

// TestWriteCompactionToFileCatalog_NoOpWhenPostgresNotConfigured proves the nil-pgPool gate:
// calling with no Postgres configured at all must be a safe no-op, never a panic.
func TestWriteCompactionToFileCatalog_NoOpWhenPostgresNotConfigured(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", false)
	ctx := context.Background()

	newOut := newTestBlockMetaWithLevel(0, 100, 200, 1000)
	require.NotPanics(t, func() {
		rw.writeCompactionToFileCatalog(ctx, "tenant-a", nil, []*backend.BlockMeta{newOut})
	})
}

// TestMarkFileCatalogDeleted_SetsDeletedAt proves retention.go's ClearBlock call site mirror:
// deleted_at gets set on the physically-deleted block's row, as a soft delete (row still
// physically exists), mirroring filecatalog.Lister's own soft-delete semantics.
func TestMarkFileCatalogDeleted_SetsDeletedAt(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()
	tenant := "tenant-a"

	meta := newTestBlockMetaWithLevel(2, 100, 200, 1000)
	rw.writeCompactionToFileCatalog(ctx, tenant, nil, []*backend.BlockMeta{meta})

	rw.markFileCatalogDeleted(ctx, tenant, meta.BlockID)

	var deletedAt *time.Time
	require.NoError(t, rw.pgPool.QueryRow(ctx,
		`SELECT deleted_at FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, meta.BlockID.String(),
	).Scan(&deletedAt))
	require.NotNil(t, deletedAt, "the deleted block's row must have deleted_at set")

	var count int
	require.NoError(t, rw.pgPool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2`,
		tenant, meta.BlockID.String(),
	).Scan(&count))
	require.Equal(t, 1, count, "a soft-deleted row must still physically exist")
}

// TestMarkCompacted_WritesToFileCatalog is the integration-level proof that markCompacted
// itself (not just the lower-level helper directly) invokes the file_catalog mirror -- exercises
// the real production call path (CompactWithConfig -> markCompacted), not a bypass. oldBlock is
// deliberately never written as a real block on the backend, so rw.c.MarkBlockCompacted fails
// for it and markCompacted returns an error (its own pre-existing, unchanged behavior) -- but
// the file_catalog mirror runs regardless of that per-block filesystem error, exactly like
// rw.blocklist.Update already does, so the output row must still be inserted.
func TestMarkCompacted_WritesToFileCatalog(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()
	tenant := "tenant-a"

	oldBlock := newTestBlockMetaWithLevel(0, 100, 200, 1000)
	newBlock := newTestBlockMetaWithLevel(1, 100, 200, 2000)

	require.Error(t, markCompacted(ctx, rw, tenant, []*backend.BlockMeta{oldBlock}, []*backend.BlockMeta{newBlock}),
		"oldBlock was never written as a real block, so the filesystem-level MarkBlockCompacted call must fail")

	var count int
	require.NoError(t, rw.pgPool.QueryRow(ctx,
		`SELECT count(*) FROM file_catalog WHERE tenant = $1 AND block_id = $2 AND compacted_at IS NULL`,
		tenant, newBlock.BlockID.String(),
	).Scan(&count))
	require.Equal(t, 1, count, "markCompacted must insert the new output block's live row even when a per-block filesystem error occurred")
}
