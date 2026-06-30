package executor

// search_trace_vi.go — index-driven TraceQL search (NOTE-VI-035, issue #459).
//
// QueryTraceQLFromIndex answers a filter query using value-index data for block
// pruning: it resolves the matching (SourceRef, BlockID, RowIdx) spans from the
// value index (via a pre-populated ValueIndexSource, mirroring the metrics path
// in NOTE-VI-033), then fetches only the blocks that actually contain matches
// and materializes their fields — no full-file scan.
//
// Like ExecuteTraceMetricsFromVI, discovery + download + per-leaf predicate
// application is the caller's job (issue #461 wiring): the source already holds
// predicate-matched results per column. This function consumes the source and
// the data-file Reader for the single SourceRef it was opened against.

import (
	"context"
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/vm"
)

// DefaultMaxIndexHits is the fallback threshold for the index search path. When the
// value index produces more matching spans than this, a full block scan is cheaper
// than per-block fetch-and-materialize, so QueryTraceQLFromIndex returns ok=false.
const DefaultMaxIndexHits = 100_000

// QueryTraceQLFromIndex executes a TraceQL filter query using the value index for
// block pruning. It returns the matching spans as executor SpanMatch values
// (Block + BlockIdx + RowIdx populated for field materialization by the caller).
//
// Parameters:
//   - source: pre-populated ValueIndexSource (one leaf predicate applied per column)
//   - r: the data-file Reader opened against the SourceRef the source's results name
//   - prog: the compiled query program (predicate tree)
//   - sourceRef: the SourceRef whose results in source belong to r; results for any
//     other SourceRef are skipped (a single Reader serves one data file)
//   - wantCols: columns to decode when parsing matched blocks (result materialization)
//   - maxIndexHits: fallback threshold; <= 0 uses DefaultMaxIndexHits
//
// Returns (matches, true, nil) when the query is fully answerable from the index.
// Returns (nil, false, nil) when the caller must fall back to a full block scan:
//   - any leaf column has no index coverage (source.LookupResults ok=false)
//   - the index produced more than maxIndexHits results (scan is cheaper)
//   - a matched span carries no row address (RowIdx unusable for direct access)
func QueryTraceQLFromIndex(
	ctx context.Context,
	source ValueIndexSource,
	r *modules_reader.Reader,
	prog *vm.Program,
	sourceRef string,
	wantCols map[string]struct{},
	maxIndexHits int,
) ([]SpanMatch, bool, error) {
	if source == nil || prog == nil || r == nil {
		return nil, false, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}
	if maxIndexHits <= 0 {
		maxIndexHits = DefaultMaxIndexHits
	}

	// Resolve the matching spans from the value index using the same boolean
	// tree walk as the metrics path (AND intersect, OR union, match-all =
	// AllResults). ok=false ⇒ a leaf column is unindexed or match-all over an
	// empty source ⇒ fall back to a block scan.
	matches, ok := viMatchSpans(source, prog)
	if !ok {
		return nil, false, nil
	}

	// Keep only spans that belong to the data file this Reader was opened for.
	// The source may aggregate results across multiple SourceRefs; a single
	// Reader serves exactly one file.
	filtered := matches[:0:0]
	for _, m := range matches {
		if m.SourceRef != sourceRef {
			continue
		}
		filtered = append(filtered, m)
	}

	// Fallback when the result set is too broad: a full scan amortizes better than
	// many small per-block fetches + materializations.
	if len(filtered) > maxIndexHits {
		return nil, false, nil
	}
	if len(filtered) == 0 {
		// Coverage existed but no span in this file matched — a definitive empty
		// result for this file, not a fallback. Return an empty match set.
		return nil, true, nil
	}

	// Group survivors by block so each block is fetched and parsed once. A span
	// with no usable row address forces a fallback: without RowIdx we cannot do
	// direct row access and would have to scan the block anyway.
	rowsByBlock := make(map[int][]uint16)
	blockOrder := make([]int, 0, len(filtered))
	type spanKey struct {
		blockIdx int
		rowIdx   uint16
	}
	identity := make(map[spanKey]VILookupResult, len(filtered))
	for _, m := range filtered {
		blockIdx := int(m.BlockID)
		key := spanKey{blockIdx: blockIdx, rowIdx: m.RowIdx}
		if _, seen := identity[key]; seen {
			continue // duplicate (TraceID,SpanID) already collapsed by viMatchSpans; guard anyway
		}
		identity[key] = m
		if _, exists := rowsByBlock[blockIdx]; !exists {
			blockOrder = append(blockOrder, blockIdx)
		}
		rowsByBlock[blockIdx] = append(rowsByBlock[blockIdx], m.RowIdx)
	}

	// Validate block indices against the reader before fetching. An out-of-range
	// block index means the index and data file are out of sync — fall back to a
	// scan rather than returning a partial result.
	blockCount := r.BlockCount()
	for _, bi := range blockOrder {
		if bi < 0 || bi >= blockCount {
			return nil, false, nil
		}
	}

	want := modules_reader.WantOnly(wantCols)

	// Coalesced multi-block fetch: adjacent blocks merge into as few round-trips as
	// possible (Reader.ReadBlocks). Then parse each block once with the restricted
	// column set and emit one SpanMatch per matched row.
	rawBlocks, err := r.ReadBlocks(blockOrder)
	if err != nil {
		return nil, false, fmt.Errorf("QueryTraceQLFromIndex: read blocks: %w", err)
	}

	out := make([]SpanMatch, 0, len(filtered))
	for _, blockIdx := range blockOrder {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			return nil, false, nil // missing block ⇒ inconsistent index ⇒ fall back
		}
		meta := r.BlockMeta(blockIdx)
		bwb, parseErr := r.ParseBlockFromBytes(raw, want, meta)
		if parseErr != nil {
			return nil, false, fmt.Errorf("QueryTraceQLFromIndex: parse block %d: %w", blockIdx, parseErr)
		}
		for _, rowIdx := range rowsByBlock[blockIdx] {
			m := identity[spanKey{blockIdx: blockIdx, rowIdx: rowIdx}]
			out = append(out, SpanMatch{
				Block:    bwb.Block,
				BlockIdx: blockIdx,
				RowIdx:   int(rowIdx),
				TraceID:  m.TraceID,
				SpanID:   append([]byte(nil), m.SpanID[:]...),
			})
		}
	}

	return out, true, nil
}
