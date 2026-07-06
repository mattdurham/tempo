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
//
// The value index is AUTHORITATIVE for the columns it covers (NOTE-VI-047, issue
// #474): when every leaf predicate resolves against the index, the returned spans
// are the complete, correct answer for this file — there is no speculative
// "the index answered but a full scan is cheaper" fallback, because a scan would
// only reproduce the identical result at higher cost.
//
// Returns (matches, true, nil) when the query is fully answerable from the index —
// including an empty match set (coverage with no matching span is a definitive
// empty answer, not a fallback).
//
// Returns (nil, false, nil) ONLY when the index genuinely cannot answer the query
// and the caller must fall back to a full block scan:
//   - any leaf column has no index coverage (source.LookupResults ok=false) — e.g.
//     a negation or otherwise unindexable predicate has no fast VI path (documented
//     exception, SPEC-ROOT-019).
//
// Returns (nil, false, err) when the index and the data file are inconsistent — a
// matched span names a block/page that does not exist in the file. This is index
// corruption, not a routine "no coverage" miss: surfacing it as an error (rather
// than silently reproducing the answer via a scan) upholds the authoritative
// contract and makes the inconsistency observable (NOTE-VI-047, issue #474).
func QueryTraceQLFromIndex(
	ctx context.Context,
	source ValueIndexSource,
	r *modules_reader.Reader,
	prog *vm.Program,
	sourceRef string,
	wantCols map[string]struct{},
) ([]SpanMatch, bool, error) {
	if source == nil || prog == nil || r == nil {
		return nil, false, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
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

	if len(filtered) == 0 {
		// Coverage existed but no span in this file matched — a definitive empty
		// result for this file, not a fallback. Return an empty match set.
		return nil, true, nil
	}

	// Group survivors by block so each block is fetched and parsed once. A span
	// with no usable row address forces a fallback: without RowIdx we cannot do
	// direct row access and would have to scan the block anyway.
	// NOTE-VI-045 (#429): the v2 BucketGroup index carries a page-addressed BlockRef
	// (BlockPage), not a block index. Resolve each page to a block index via the reader;
	// a page that names no block start means the index and data file are out of sync.
	// The value index is authoritative (NOTE-VI-047, #474): an out-of-sync page is index
	// corruption, so surface it as an error rather than silently masking it with a scan.
	rowsByBlock := make(map[int][]uint16)
	blockOrder := make([]int, 0, len(filtered))
	type spanKey struct {
		blockIdx int
		rowIdx   uint16
	}
	identity := make(map[spanKey]VILookupResult, len(filtered))
	for _, m := range filtered {
		blockIdx, ok := r.BlockIndexForPage(m.BlockPage)
		if !ok {
			// Page names no block start ⇒ the index and data file are out of sync.
			return nil, false, fmt.Errorf(
				"QueryTraceQLFromIndex: value index names page %d with no block start in %s (index/data inconsistency)",
				m.BlockPage,
				sourceRef,
			)
		}
		key := spanKey{blockIdx: blockIdx, rowIdx: m.RowIdx}
		if _, seen := identity[key]; seen {
			continue // duplicate span already collapsed by viMatchSpans; guard anyway
		}
		identity[key] = m
		if _, exists := rowsByBlock[blockIdx]; !exists {
			blockOrder = append(blockOrder, blockIdx)
		}
		rowsByBlock[blockIdx] = append(rowsByBlock[blockIdx], m.RowIdx)
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
			// Reader returned no bytes for a block the index named ⇒ index/data
			// inconsistency; surface it (NOTE-VI-047, #474) rather than masking it.
			return nil, false, fmt.Errorf(
				"QueryTraceQLFromIndex: value index names block %d absent from %s (index/data inconsistency)",
				blockIdx,
				sourceRef,
			)
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
