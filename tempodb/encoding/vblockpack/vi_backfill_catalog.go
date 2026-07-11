package vblockpack

// vi_backfill_catalog.go — catalog-cursor-based blockpack.BlockFetcher
// (2026-07-11), an alternative to viBlockFetcher's live backend.Reader.
// Blocks()/BlockMeta() listing. Structurally fixes the "block deleted between
// list and fetch" failure class: a catalog row only exists while the
// file_catalog lister (backend-scheduler, see modules/backendscheduler/
// filecatalog) still sees the block in its own live, continuously-reconciled
// blocklist snapshot -- soft-deleted rows are excluded from every query below,
// and the lister's reconciliation pass runs well within CompactedBlockRetention's
// safety margin (see filecatalog package doc comment). FetchBlock is
// UNCHANGED from viBlockFetcher (delegates to the same fetchBlockViaReader
// helper) -- only ListBlocksInRange's data source changes.

import (
	"context"
	"fmt"
	"sort"
	"sync"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/tempodb/backend"
)

const catalogListSQL = `
	SELECT row_id, block_ref, start_sec
	FROM file_catalog
	WHERE tenant = $1 AND deleted_at IS NULL AND row_id > $2 AND end_sec >= $3 AND start_sec <= $4
	ORDER BY row_id`

// catalogBlockFetcher implements blockpack.BlockFetcher by querying the
// Postgres file_catalog table via a persisted per-column cursor
// (cursorRowID, from blockpack.Entry.Backfill.LastCatalogRowID) instead of
// live-listing S3/local storage on every backfill run.
type catalogBlockFetcher struct {
	pool        *pgxpool.Pool
	reader      backend.Reader // same reader viBlockFetcher would have used, for FetchBlock only
	cursorRowID uint64         // entry.Backfill.LastCatalogRowID at construction -- never advances mid-run

	mu           sync.Mutex
	maxRowIDSeen uint64 // highest row_id returned by any ListBlocksInRange call on this instance
}

// ListBlocksInRange returns every non-soft-deleted file_catalog row for
// tenant with row_id > f.cursorRowID whose [start_sec, end_sec] overlaps
// [minSec, maxSec], newest-first (same ordering contract as
// viBlockFetcher.ListBlocksInRange).
func (f *catalogBlockFetcher) ListBlocksInRange(
	ctx context.Context, tenant string, minSec, maxSec uint64,
) ([]string, error) {
	rows, err := f.pool.Query(ctx, catalogListSQL, tenant, f.cursorRowID, minSec, maxSec)
	if err != nil {
		return nil, fmt.Errorf("catalogBlockFetcher: query: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		ref      string
		startSec uint64
		rowID    uint64
	}
	var candidates []candidate
	var maxSeen uint64
	for rows.Next() {
		var c candidate
		if scanErr := rows.Scan(&c.rowID, &c.ref, &c.startSec); scanErr != nil {
			return nil, fmt.Errorf("catalogBlockFetcher: scan: %w", scanErr)
		}
		candidates = append(candidates, c)
		if c.rowID > maxSeen {
			maxSeen = c.rowID
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalogBlockFetcher: rows: %w", err)
	}

	f.mu.Lock()
	if maxSeen > f.maxRowIDSeen {
		f.maxRowIDSeen = maxSeen
	}
	f.mu.Unlock()

	// Newest-first, same contract as viBlockFetcher.ListBlocksInRange.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].startSec > candidates[j].startSec })
	refs := make([]string, len(candidates))
	for i, c := range candidates {
		refs[i] = c.ref
	}
	return refs, nil
}

// FetchBlock is identical to viBlockFetcher.FetchBlock -- both delegate to the
// same fetchBlockViaReader helper (vi_backfill.go), since fetch-by-ref logic
// never depended on how the ref was discovered.
func (f *catalogBlockFetcher) FetchBlock(ctx context.Context, sourceRef string) (*blockpack.Reader, error) {
	return fetchBlockViaReader(ctx, f.reader, sourceRef)
}

// MaxRowIDSeen reports the highest file_catalog row_id returned by any
// ListBlocksInRange call on this fetcher instance. NOT part of
// blockpack.BlockFetcher -- a tempo-local accessor runViBackfillCore reads
// after eng.Run succeeds, to persist the new cursor via
// Registry.UpdateCatalogCursor (vi_backfill.go). Mirrors this package's own
// "optional capability, checked via type assertion" convention (e.g.
// rawobjectstore.go's atomicWriter probe).
func (f *catalogBlockFetcher) MaxRowIDSeen() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxRowIDSeen
}

var _ blockpack.BlockFetcher = (*catalogBlockFetcher)(nil)
