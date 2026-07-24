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
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
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
// The value index is AUTHORITATIVE for the columns it covers (NOTE-VI-096, issue
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
// contract and makes the inconsistency observable (NOTE-VI-096, issue #474).
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
	// The value index is authoritative (NOTE-VI-096, #474): an out-of-sync page is index
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

	// task #13 (FIX-459-SPANID): span:id must always be decoded, regardless of the caller's own
	// wantCols, so each match's real span identity can be resolved from its own row below.
	// VILookupResult.SpanID is unconditionally zero for every value-index entry in production —
	// the BucketGroup wire format WriteValueIndexL0 writes has no SpanID field at all
	// (internal/modules/valueindex/writer.go's assembleBucket, NOTE-VI-045/094) — so it can never
	// be trusted as a match's identity; only the real row (already fetched for field
	// materialization) reliably carries it.
	//
	// Issue #534: a residual-carrying match's own column (e.g. span:duration) must also always
	// be decoded, regardless of wantCols — the caller may have asked for a projection that
	// doesn't include it at all, reasoning (correctly, for every OTHER leaf shape) that the
	// value index had already fully resolved the filter. evaluateResidual needs the real column
	// value in hand to re-verify these specific matches below.
	extra := map[string]struct{}{modules_shared.SpanIDColumnName: {}}
	for _, m := range filtered {
		m.Residual.collectColumns(extra)
	}
	want := modules_reader.WantOnly(withColumns(wantCols, extra))

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
			// inconsistency; surface it (NOTE-VI-096, #474) rather than masking it.
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
		spanIDCol := bwb.Block.GetColumn(modules_shared.SpanIDColumnName)
		for _, rowIdx := range rowsByBlock[blockIdx] {
			m := identity[spanKey{blockIdx: blockIdx, rowIdx: rowIdx}]
			// Issue #534: re-verify a residual-carrying match against its REAL raw column
			// value now that its block is already in hand — the millisecond-boundary-bucket
			// enforcement point. bwb.Block was already fetched/parsed above for this same
			// match's own field materialization (SPEC-ROOT's single-I/O-per-block invariant
			// — nothing here issues a second read), so this is a pure in-memory decode +
			// comparison.
			if m.Residual != nil {
				pass, rerr := evaluateResidual(bwb.Block, m.Residual, int(rowIdx))
				if rerr != nil {
					return nil, false, fmt.Errorf(
						"QueryTraceQLFromIndex: residual check block %d row %d: %w", blockIdx, rowIdx, rerr,
					)
				}
				if !pass {
					// Genuinely excluded by the real value — this row was one of the
					// widened VI query's boundary-bucket candidates that the original,
					// exact comparison does not actually satisfy.
					continue
				}
			}
			spanID, spanIDOK := resolveRowSpanID(spanIDCol, int(rowIdx))
			if !spanIDOK {
				return nil, false, fmt.Errorf(
					"QueryTraceQLFromIndex: block %d row %d missing span:id (index/data inconsistency)",
					blockIdx, rowIdx,
				)
			}
			out = append(out, SpanMatch{
				Block:    bwb.Block,
				BlockIdx: blockIdx,
				RowIdx:   int(rowIdx),
				TraceID:  m.TraceID,
				SpanID:   spanID,
			})
		}
	}

	return out, true, nil
}

// withColumns returns a copy of wantCols with every column name in extra added, never
// mutating the caller's own map (wantCols may be reused elsewhere by the caller). Generalizes
// the original task #13 withSpanIDColumn helper (issue #534) so a residual-carrying match's
// own column can be forced into the decode set the same way span:id already was.
func withColumns(wantCols map[string]struct{}, extra map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(wantCols)+len(extra))
	for c := range wantCols {
		out[c] = struct{}{}
	}
	for c := range extra {
		out[c] = struct{}{}
	}
	return out
}

// evaluateResidual (issue #534) re-checks residual — a VILookupResult's optional
// ResidualGroup — against the REAL raw column value(s) it references at row, in an
// already-decoded block. This is the millisecond-boundary-bucket-ambiguity enforcement
// point: vibuilder attaches a non-nil residual only when decidableTimeBucketThreshold
// found a leaf's threshold undecidable at the value index's millisecond-bucket
// granularity, so the widened VI query alone cannot be fully trusted for these specific
// candidates — the real, un-truncated value decides. residual.Evaluate walks the group's
// boolean structure (a single leaf's own trivial one-check group, or a combined AND/OR of
// two leaves' own groups — see ResidualGroup's own doc comment), calling back into
// residualRawValue below for each individual check's own column.
//
// Returns an error only for an index/data inconsistency (a check's own column is absent
// from the block, or unreadable as its declared type, or the type itself is one no
// production caller sets today) — never a silent "treat as excluded" guess, mirroring this
// file's existing resolveRowSpanID/BlockIndexForPage discipline of surfacing corruption
// rather than masking it.
func evaluateResidual(block *modules_reader.Block, residual *ResidualGroup, row int) (bool, error) {
	return residual.Evaluate(func(check *ResidualColumnPredicate) (uint64, bool, error) {
		return residualRawValue(block, check, row)
	})
}

// residualRawValue reads the real raw value of check's own column at row from an
// already-decoded block, decoded per check's declared ColType.
func residualRawValue(block *modules_reader.Block, check *ResidualColumnPredicate, row int) (uint64, bool, error) {
	col := block.GetColumn(check.Column)
	if col == nil {
		return 0, false, fmt.Errorf("residual column %q absent from block (index/data inconsistency)", check.Column)
	}
	switch check.ColType {
	case modules_shared.ColumnTypeUint64:
		v, ok := col.Uint64Value(row)
		if !ok {
			return 0, false, fmt.Errorf("residual column %q row %d unreadable as uint64", check.Column, row)
		}
		return v, true, nil
	default:
		return 0, false, fmt.Errorf(
			"residual column %q: unsupported residual column type %v (index/data inconsistency)",
			check.Column, check.ColType,
		)
	}
}

// resolveRowSpanID reads the real span:id value for rowIdx from an already-decoded span:id
// column. Returns (nil, false) when the column is absent or the row's value cannot be read --
// both are index/data inconsistency at the caller (a real block always carries span:id), never a
// silent zero-value fallback (task #13, mirrors the structural path's own "never trust a
// stale/absent identity, surface the error" discipline).
func resolveRowSpanID(col *modules_reader.Column, rowIdx int) ([]byte, bool) {
	if col == nil {
		return nil, false
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok || len(v) != 8 {
		return nil, false
	}
	return append([]byte(nil), v...), true
}
