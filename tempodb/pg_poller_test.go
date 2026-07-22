package tempodb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"

	"github.com/grafana/tempo/tempodb/backend"
)

// TestPollBlocklistFromPostgres_LiveBlockWithMeta_ReconstructsFullBlockMeta proves the
// primary path (issue #522/#525): a row with its Meta column populated reconstructs a full
// backend.BlockMeta with zero object-storage I/O -- no meta.json fetch.
func TestPollBlocklistFromPostgres_LiveBlockWithMeta_ReconstructsFullBlockMeta(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	blockID := backend.NewUUID()
	tm := blockpack.TraceBlockMeta{
		Version:      "vblockpack",
		TotalObjects: 500,
		TotalRecords: 500,
		DedicatedColumns: []blockpack.TraceDedicatedColumn{
			{Scope: "span", Name: "http.status_code", Type: "int"},
		},
		ReplicationFactor: 1,
	}
	metaJSON, err := json.Marshal(tm)
	require.NoError(t, err)

	objectKey := "tenant-a/" + blockID.String() + "/data.blockpack"
	require.NoError(t, rw.pg.FileCatalogStore().Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "trace", Tenant: "tenant-a", ObjectKey: objectKey,
		MinSec: 1000, MaxSec: 2000, SizeBytes: 12345, Level: 2, Meta: metaJSON,
	}))

	live, compacted, err := rw.pollBlocklistFromPostgres(ctx)
	require.NoError(t, err)
	require.Empty(t, compacted)
	require.Len(t, live["tenant-a"], 1)

	got := live["tenant-a"][0]
	require.Equal(t, blockID, got.BlockID)
	require.Equal(t, "tenant-a", got.TenantID)
	require.Equal(t, "vblockpack", got.Version)
	require.Equal(t, time.Unix(1000, 0), got.StartTime)
	require.Equal(t, time.Unix(2000, 0), got.EndTime)
	require.EqualValues(t, 12345, got.Size_)
	require.EqualValues(t, 2, got.CompactionLevel)
	require.EqualValues(t, 500, got.TotalObjects)
	require.EqualValues(t, 500, got.TotalRecords)
	require.EqualValues(t, 1, got.ReplicationFactor)
	require.Equal(t, backend.DedicatedColumns{
		{Scope: backend.DedicatedColumnScopeSpan, Name: "http.status_code", Type: backend.DedicatedColumnTypeInt},
	}, got.DedicatedColumns)
}

// TestPollBlocklistFromPostgres_CompactedNotDeleted_AppearsInCompactedBlocklist proves a row
// still within its post-compaction grace window (compacted_at set, deleted_at NULL) surfaces
// in the compacted blocklist, not the live one -- mirrors the classic poller's own
// live-vs-compacted distinction, sourced from Postgres instead of meta.json/meta.compacted.json.
func TestPollBlocklistFromPostgres_CompactedNotDeleted_AppearsInCompactedBlocklist(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	blockID := backend.NewUUID()
	metaJSON, err := json.Marshal(blockpack.TraceBlockMeta{Version: "vblockpack"})
	require.NoError(t, err)

	objectKey := "tenant-a/" + blockID.String() + "/data.blockpack"
	require.NoError(t, rw.pg.FileCatalogStore().Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "trace", Tenant: "tenant-a", ObjectKey: objectKey,
		MinSec: 1000, MaxSec: 2000, Meta: metaJSON,
	}))
	require.NoError(t, rw.pg.FileCatalogStore().MarkCompacted(ctx, []string{objectKey}))

	live, compacted, err := rw.pollBlocklistFromPostgres(ctx)
	require.NoError(t, err)
	require.Empty(t, live["tenant-a"], "a compacted-but-not-deleted block must not appear in the live blocklist")
	require.Len(t, compacted["tenant-a"], 1)
	require.Equal(t, blockID, compacted["tenant-a"][0].BlockID)
}

// TestPollBlocklistFromPostgres_HardDeleted_ExcludedEntirely proves a fully-reaped row
// (deleted_at set) never appears in either blocklist -- this is exactly the state the
// original bug's stale query referenced: a block whose row (and object) blockpack's own
// catalog_reap had already removed.
func TestPollBlocklistFromPostgres_HardDeleted_ExcludedEntirely(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	blockID := backend.NewUUID()
	metaJSON, err := json.Marshal(blockpack.TraceBlockMeta{Version: "vblockpack"})
	require.NoError(t, err)

	objectKey := "tenant-a/" + blockID.String() + "/data.blockpack"
	require.NoError(t, rw.pg.FileCatalogStore().Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "trace", Tenant: "tenant-a", ObjectKey: objectKey,
		MinSec: 1000, MaxSec: 2000, Meta: metaJSON,
	}))
	require.NoError(t, rw.pg.FileCatalogStore().MarkCompacted(ctx, []string{objectKey}))
	_, err = rw.pg.Pool().Exec(ctx, `UPDATE blockpack_file_catalog SET deleted_at = now() WHERE object_key = $1`, objectKey)
	require.NoError(t, err)

	live, compacted, err := rw.pollBlocklistFromPostgres(ctx)
	require.NoError(t, err)
	require.Empty(t, live["tenant-a"])
	require.Empty(t, compacted["tenant-a"])
}

// TestPollBlocklistFromPostgres_NoRowsForTenant_ReturnsEmptyNotError proves an idle
// deployment (no trace rows in Postgres yet) is a clean, empty result -- not an error --
// exactly the "cube not yet backfilled" self-healing spirit applied to block listing.
func TestPollBlocklistFromPostgres_NoRowsForTenant_ReturnsEmptyNotError(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)

	live, compacted, err := rw.pollBlocklistFromPostgres(context.Background())
	require.NoError(t, err)
	require.Empty(t, live)
	require.Empty(t, compacted)
}

// TestBlockIDFromTraceObjectKey_MalformedKey_ReturnsError proves a malformed object key
// (not matching the block-builder's own <tenant>/<blockID>/data.blockpack shape) is a clean,
// reported error rather than a panic or silent misparse.
func TestBlockIDFromTraceObjectKey_MalformedKey_ReturnsError(t *testing.T) {
	_, err := blockIDFromTraceObjectKey("not-enough-segments")
	require.Error(t, err)

	_, err = blockIDFromTraceObjectKey("tenant-a/not-a-uuid/data.blockpack")
	require.Error(t, err)
}

// TestRetainTenant_Postgres_AgesOutLiveBlockViaMarkCompacted proves retention's aging-out step
// (issue #522/#525's retention re-pointing) sets compacted_at directly on the Postgres row
// instead of the classic MarkBlockCompacted meta.json rename, which nothing reads for
// vblockpack blocks.
func TestRetainTenant_Postgres_AgesOutLiveBlockViaMarkCompacted(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	blockID := backend.NewUUID()
	metaJSON, err := json.Marshal(blockpack.TraceBlockMeta{Version: "vblockpack"})
	require.NoError(t, err)

	old := time.Now().Add(-24 * time.Hour)
	objectKey := "tenant-a/" + blockID.String() + "/data.blockpack"
	require.NoError(t, rw.pg.FileCatalogStore().Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "trace", Tenant: "tenant-a", ObjectKey: objectKey,
		MinSec: old.Unix() - 1000, MaxSec: old.Unix(), Meta: metaJSON,
	}))

	rw.pollBlocklist(ctx)
	require.Len(t, rw.blocklist.Metas("tenant-a"), 1, "block must be live per Postgres before retention runs")

	rw.RetainTenantWithConfig(ctx, "tenant-a", &CompactorConfig{
		BlockRetention: time.Hour, CompactedBlockRetention: time.Hour,
	}, &mockSharder{}, &mockOverrides{})

	rows, err := rw.pg.FileCatalogStore().ListCompactedNotDeleted(ctx, "trace", "tenant-a")
	require.NoError(t, err)
	require.Len(t, rows, 1, "retention must mark the aged-out block compacted in Postgres")
	require.Equal(t, objectKey, rows[0].ObjectKey)
}

// TestRetainTenant_Postgres_SkipsClearBlockDefersToOwnReaper proves retention's final deletion
// step does NOT call the classic ClearBlock for a Postgres-sourced (vblockpack) block -- issue
// #522/#525's "Postgres is the only source of truth" directive means blockpack's own
// compaction-planner/catalog_reap pipeline is the sole owner of physical deletion; a second,
// independent deleter racing against it would violate that.
func TestRetainTenant_Postgres_SkipsClearBlockDefersToOwnReaper(t *testing.T) {
	rw := newTestReaderWriter(t, "vblockpack", true)
	ctx := context.Background()

	blockID := backend.NewUUID()
	metaJSON, err := json.Marshal(blockpack.TraceBlockMeta{Version: "vblockpack"})
	require.NoError(t, err)

	objectKey := "tenant-a/" + blockID.String() + "/data.blockpack"
	require.NoError(t, rw.pg.FileCatalogStore().Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "trace", Tenant: "tenant-a", ObjectKey: objectKey,
		MinSec: 1000, MaxSec: 2000, Meta: metaJSON,
	}))
	require.NoError(t, rw.pg.FileCatalogStore().MarkCompacted(ctx, []string{objectKey}))
	// Backdate compacted_at well past CompactedBlockRetention so retention's second loop
	// considers this block ready for its final-deletion step.
	_, err = rw.pg.Pool().Exec(ctx,
		`UPDATE blockpack_file_catalog SET compacted_at = now() - interval '24 hours' WHERE object_key = $1`, objectKey)
	require.NoError(t, err)

	rw.pollBlocklist(ctx)
	require.Len(t, rw.blocklist.CompactedMetas("tenant-a"), 1, "block must be in the compacted blocklist before retention runs")

	rw.RetainTenantWithConfig(ctx, "tenant-a", &CompactorConfig{
		BlockRetention: time.Hour, CompactedBlockRetention: time.Hour,
	}, &mockSharder{}, &mockOverrides{})

	require.Len(t, rw.blocklist.CompactedMetas("tenant-a"), 1,
		"ClearBlock's success path (which removes the block from the compacted blocklist) must not run for a Postgres-sourced block")

	rows, err := rw.pg.FileCatalogStore().ListCompactedNotDeleted(ctx, "trace", "tenant-a")
	require.NoError(t, err)
	require.Len(t, rows, 1, "the row must still exist in Postgres -- only blockpack's own catalog_reap may remove it")
}
