package vblockpack

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
)

func TestCatalogBlockFetcher_ListBlocksInRange_RespectsCursor(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	var rowIDs []uint64
	for i := range 4 {
		blockID := uuid.New().String()
		row := pool.QueryRow(ctx, `
			INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec)
			VALUES ($1, $2, $3, $4, $5) RETURNING row_id`,
			tenant, blockID, blockObjectKey(tenant, blockID), uint64(100+i), uint64(200+i))
		var rowID uint64
		require.NoError(t, row.Scan(&rowID))
		rowIDs = append(rowIDs, rowID)
	}

	// cursorRowID = rowIDs[1] -- only rows AFTER the 2nd insert should be returned.
	fetcher := &catalogBlockFetcher{pool: pool, cursorRowID: rowIDs[1]}
	refs, err := fetcher.ListBlocksInRange(ctx, tenant, 0, 1000)
	require.NoError(t, err)
	require.Len(t, refs, 2, "only rows with row_id > cursor should be returned")
}

func TestCatalogBlockFetcher_ListBlocksInRange_SkipsSoftDeletedRows(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	liveID := uuid.New().String()
	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec)
		VALUES ($1, $2, $3, $4, $5)`,
		tenant, liveID, blockObjectKey(tenant, liveID), uint64(100), uint64(200))
	require.NoError(t, err)

	deletedID := uuid.New().String()
	_, err = pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec, deleted_at)
		VALUES ($1, $2, $3, $4, $5, now())`,
		tenant, deletedID, blockObjectKey(tenant, deletedID), uint64(100), uint64(200))
	require.NoError(t, err)

	fetcher := &catalogBlockFetcher{pool: pool}
	refs, err := fetcher.ListBlocksInRange(ctx, tenant, 0, 1000)
	require.NoError(t, err)
	require.Len(t, refs, 1, "soft-deleted rows must be excluded")
	require.Equal(t, blockObjectKey(tenant, liveID), refs[0])
}

func TestCatalogBlockFetcher_MaxRowIDSeen_TracksHighestReturned(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	var lastRowID uint64
	for i := range 3 {
		blockID := uuid.New().String()
		row := pool.QueryRow(ctx, `
			INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec)
			VALUES ($1, $2, $3, $4, $5) RETURNING row_id`,
			tenant, blockID, blockObjectKey(tenant, blockID), uint64(100+i), uint64(200+i))
		require.NoError(t, row.Scan(&lastRowID))
	}

	fetcher := &catalogBlockFetcher{pool: pool}
	require.Equal(t, uint64(0), fetcher.MaxRowIDSeen(), "MaxRowIDSeen must start at zero before any list call")

	_, err := fetcher.ListBlocksInRange(ctx, tenant, 0, 1000)
	require.NoError(t, err)
	require.Equal(t, lastRowID, fetcher.MaxRowIDSeen(), "MaxRowIDSeen must track the highest row_id returned")
}

// TestRunViBackfillCore_CursorNotAdvancedOnPartialFailure is the explicit
// regression pin for Part 3.4's "never advance past an unconfirmed run"
// claim: 3 candidate blocks in file_catalog, real data written for the first
// (newest, processed first) but NOT for the second -- processBlocks aborts on
// the second block's FetchBlock failure (no data at that path), and
// runViBackfillCore must never reach the cursor-persist step. Verified
// through the ACTUAL *catalogBlockFetcher type (not a generic fake
// BlockFetcher) since runViBackfillCore's cursor-persist gate is a type
// assertion on that concrete type -- a generic fake would trivially "pass"
// this test for the wrong reason (the assertion simply never matching).
func TestRunViBackfillCore_CursorNotAdvancedOnPartialFailure(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()
	tenant := "tenant-a"

	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(rawR.Shutdown)

	// Block 1 (newest, processed first): real, valid block data written.
	blockID1 := uuid.New()
	data1 := writeViBackfillTestBlock(t, uint64(300*1e9), "value-1")
	require.NoError(t, rawW.Write(ctx, DataFileName, backend.KeyPathForBlock(blockID1, tenant), bytes.NewReader(data1), int64(len(data1)), nil))

	// Block 2 (processed second): NO data written -- FetchBlock will fail here.
	blockID2 := uuid.New()

	// Block 3 (oldest, never reached): also no data, but irrelevant -- the run
	// aborts at block 2 before ever attempting block 3.
	blockID3 := uuid.New()

	for i, id := range []uuid.UUID{blockID1, blockID2, blockID3} {
		_, err := pool.Exec(ctx, `
			INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec)
			VALUES ($1, $2, $3, $4, $5)`,
			tenant, id.String(), blockObjectKey(tenant, id.String()), uint64(300-i), uint64(300-i+50))
		require.NoError(t, err)
	}

	fetcher := &catalogBlockFetcher{pool: pool, reader: backend.NewReader(rawR)}
	objStore := newFakeViObjectStore()
	entry := blockpack.Entry{
		Tenant: tenant, ColumnHash: "abc123", ColumnType: "string", ColumnName: "custom.attr",
	}

	// Pre-populate the registry entry directly (UpdateWatermark's createIfMissing
	// is nil -- it errors on a not-yet-registered entry, mirroring a real
	// RecordUseAndMaybeTrigger call having already created it before backfill
	// launches). Without this, block 1's own progressFn/UpdateWatermark call
	// would fail BEFORE block 2's FetchBlock is ever reached, which would still
	// make this test pass, but for the wrong reason (never actually exercising
	// the "genuinely partial run" scenario the test's own name claims to cover).
	seedRegistryEntry(t, objStore, entry)
	registry := blockpack.NewRegistry(objStore, entry.Tenant)

	runErr := runViBackfillCore(ctx, entry, fetcher, registry, newFakeViPutter(), defaultValueIndexPref)
	require.Error(t, runErr, "the run must fail overall -- block 2 has no data")
	require.Contains(t, runErr.Error(), "fetch block", "the failure must genuinely occur at FetchBlock, not earlier (e.g. a missing registry entry) -- otherwise this test doesn't exercise the partial-run scenario it claims to")

	// The cursor must be unchanged (still zero) -- confirming
	// UpdateCatalogCursor was never called, despite block 1 having
	// successfully advanced the watermark via its own progressFn call.
	loadedEntries, _, loadErr := registry.Load(ctx)
	require.NoError(t, loadErr)
	require.Len(t, loadedEntries, 1)
	require.Equal(t, uint64(0), loadedEntries[0].Backfill.LastCatalogRowID, "cursor must never advance past an unconfirmed (failed) run")
}

// seedRegistryEntry directly writes entry into objStore's viusage index.json,
// bypassing RecordUseAndMaybeTrigger's trigger/threshold machinery -- this
// test only needs a pre-existing row for UpdateWatermark's createIfMissing:nil
// contract to succeed, not any trigger behavior.
func seedRegistryEntry(t *testing.T, objStore blockpack.ObjectStore, entry blockpack.Entry) {
	t.Helper()
	data, err := json.Marshal(struct {
		Version int               `json:"version"`
		Entries []blockpack.Entry `json:"entries"`
	}{Version: 1, Entries: []blockpack.Entry{entry}})
	require.NoError(t, err)
	require.NoError(t, objStore.ConditionalPut(context.Background(), entry.Tenant+"/viusage/index.json", data, ""))
}
