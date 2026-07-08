package executor

// structural_traceresolve.go — canonical single-file TraceGroup resolve algorithm (plan-d.md D3,
// issue #489; relocated here from root's materializeTraceGroup per team-lead's D3B checkpoint
// ruling, 2026-07-07).
//
// Root's materializeTraceGroup (reader.go) is now a thin delegate over ResolveTraceGroupSourceRef:
// it converts each ResolvedTraceRow to its own public SpanMatch (SpanFieldsProvider/Clone/pooling
// stay in root — a public-API-facing concern, not internal-module resolve logic, so they did NOT
// move here). MaterializeTraceGroupMultiFile (structural_multifile.go, this package) converts each
// ResolvedTraceRow to the lighter ResolvedSpan shape D3B/D4/D6 consume. This achieves single
// source of truth for the resolve/skew-detection algorithm itself: GetTraceByID's single-file path
// and structural's multi-file path share exactly one implementation, never two independently
// maintained copies.
//
// Why this moved and not the other way around: D3B/D4/D6 all live in this package
// (internal/modules/executor) and need D3's resolved-span shape directly (single-source-of-truth
// requirement in plan-d.md). Root's package already imports internal/modules/executor (api.go,
// query_traceql.go) — the reverse import (executor importing root) would be a cycle. This
// function has zero root-package dependency (modules_reader/modules_shared/valueindex only), so it
// is the one that could move without loss.

import (
	"bytes"
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// ResolvedTraceRow is one SpanEntry from a TraceGroup resolved to its exact parsed Block + row
// within a single Reader, with index/data skew and the defensive trace:id re-verify already
// checked. See ResolveTraceGroupSourceRef.
type ResolvedTraceRow struct {
	Block    *modules_reader.Block
	Span     valueindex.SpanEntry
	BlockIdx int
	RowIdx   int
}

// ResolveTraceGroupSourceRef resolves every SpanEntry in group whose SourceRef matches sourceRef
// (an empty sourceRef disables the filter — v1 back-compat, NOTE-VI-076) to its exact parsed
// Block + row in reader. Every resolve failure — an index-named page that does not resolve, a
// ReadBlocks/parse failure, or a resolved row whose own trace:id column does not match traceID —
// is index/data skew and returns an error (never a silent drop). When sourceRef is non-empty and
// zero spans survive the filter, that is an authoritative "not found in THIS file" ((nil, nil)),
// not skew: a sibling file's own resolve call handles those spans (mirrors materializeTraceGroup's
// original NOTE-VI-076 contract exactly).
//
// Moved verbatim (algorithm unchanged) from root's materializeTraceGroup — see this file's package
// doc comment for why. Error messages here omit the "GetTraceByID: " prefix (this is no longer a
// GetTraceByID-specific primitive); root's thin-delegate materializeTraceGroup re-adds that prefix
// when wrapping, preserving its existing external error text.
func ResolveTraceGroupSourceRef(
	reader *modules_reader.Reader,
	group valueindex.TraceGroup,
	traceID [16]byte,
	sourceRef string,
) ([]ResolvedTraceRow, error) {
	rowsByBlock, blockOrder, spanByBlockRow, err := groupTraceGroupSpansByBlock(reader, group, sourceRef)
	if err != nil {
		return nil, err
	}
	if len(blockOrder) == 0 {
		if sourceRef != "" {
			return nil, nil
		}
		return nil, fmt.Errorf("index/data skew: trace group resolved to zero blocks")
	}

	rawBlocks, readErr := reader.ReadBlocks(blockOrder)
	if readErr != nil {
		return nil, fmt.Errorf("read index-named blocks: %w", readErr)
	}

	rows := make([]ResolvedTraceRow, 0, len(group.Spans))
	for _, blockIdx := range blockOrder {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			return nil, fmt.Errorf("index/data skew: block %d missing from read result", blockIdx)
		}
		bwb, parseErr := reader.ParseBlockFromBytes(raw, modules_reader.WantAll(), reader.BlockMeta(blockIdx))
		if parseErr != nil {
			return nil, fmt.Errorf("parse block %d: %w", blockIdx, parseErr)
		}
		traceIDCol := bwb.Block.GetColumn(modules_shared.TraceIDColumnName)
		for _, rowIdx := range rowsByBlock[blockIdx] {
			if !rowMatchesTraceIDColumn(traceIDCol, rowIdx, traceID) {
				return nil, fmt.Errorf(
					"index/data skew: block %d row %d trace:id does not match index entry", blockIdx, rowIdx,
				)
			}
			rows = append(rows, ResolvedTraceRow{
				Block:    bwb.Block,
				Span:     spanByBlockRow[[2]int{blockIdx, rowIdx}],
				BlockIdx: blockIdx,
				RowIdx:   rowIdx,
			})
		}
	}
	return rows, nil
}

// groupTraceGroupSpansByBlock filters group.Spans to sourceRef (disabled when empty), resolves
// each surviving span's BlockRef to a block index in reader, and dedups (blockIdx, rowIdx) pairs.
// Split out of ResolveTraceGroupSourceRef to keep that function's own cyclomatic complexity down.
func groupTraceGroupSpansByBlock(
	reader *modules_reader.Reader,
	group valueindex.TraceGroup,
	sourceRef string,
) (rowsByBlock map[int][]int, blockOrder []int, spanByBlockRow map[[2]int]valueindex.SpanEntry, err error) {
	rowsByBlock = make(map[int][]int, len(group.Spans))
	blockOrder = make([]int, 0, len(group.Spans))
	spanByBlockRow = make(map[[2]int]valueindex.SpanEntry, len(group.Spans))
	seen := make(map[[2]int]struct{}, len(group.Spans))
	for _, span := range group.Spans {
		if sourceRef != "" && span.SourceRef != sourceRef {
			continue
		}
		blockIdx, ok := reader.BlockIndexForPage(span.BlockRef.PageNum)
		if !ok {
			return nil, nil, nil, fmt.Errorf(
				"index/data skew: index-named page %d does not resolve in file", span.BlockRef.PageNum,
			)
		}
		key := [2]int{blockIdx, int(span.RowIdx)}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		spanByBlockRow[key] = span
		if _, exists := rowsByBlock[blockIdx]; !exists {
			blockOrder = append(blockOrder, blockIdx)
		}
		rowsByBlock[blockIdx] = append(rowsByBlock[blockIdx], int(span.RowIdx))
	}
	return rowsByBlock, blockOrder, spanByBlockRow, nil
}

// rowMatchesTraceIDColumn reports whether col's value at rowIdx equals traceID — the defensive
// re-verify step after direct index-addressed row access. Root's former rowMatchesTraceID was
// removed entirely when materializeTraceGroup became a thin delegate over
// ResolveTraceGroupSourceRef (this function) — there is no longer a root-side twin to mirror; this
// is now the sole implementation of the check, shared by both GetTraceByID's single-file path and
// structural's multi-file path via ResolveTraceGroupSourceRef.
func rowMatchesTraceIDColumn(col *modules_reader.Column, rowIdx int, traceID [16]byte) bool {
	if col == nil {
		return false
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok {
		return false
	}
	return bytes.Equal(v, traceID[:])
}
