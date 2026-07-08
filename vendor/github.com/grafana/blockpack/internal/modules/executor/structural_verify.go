package executor

// structural_verify.go — D3B (plan-d.md, issue #489): candidate verification via targeted row
// reads.
//
// Correction driving this file's existence (2026-07-07, brainstormer-d verified by direct code
// read of vibuilder.BuildSource, team-lead confirmed): the issue's own step 4 ("verify remaining
// leaves against candidates, reusing #484 Phase 3's cost-based threshold") does not correspond to
// any existing mechanism anywhere in the codebase. queryplan's ClassifyWithThreshold/
// ClassifyProgramVCNT remain reusable for the coarser "is this side worth VI-resolving at all"
// decision, but say nothing about how a narrowed candidate set's remaining side gets confirmed
// once you've decided NOT to VI-resolve it. verifyCandidateSpans is that confirmation mechanism,
// designed and implemented here as its own first-class deliverable — NOT a reuse of an existing
// primitive. D4 (positive operators) needs it whenever the non-lead side is too expensive to
// VI-resolve outright; D6 (negated operators) needs it UNCONDITIONALLY (the negated/left side can
// never be VI-led at all, per the audit).

import (
	"context"
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/vm"
)

// verifyCandidateSpans evaluates prog (a non-lead filter leg's compiled program for D4, or the
// negated path's left-filter program for D6, called unconditionally there) against every span in
// spans, using EACH span's own resolved reader (ResolvedSpan.Reader, D3) — not a single shared
// reader — since a multi-file candidate trace (D3, Option A) may need confirming reads across
// several files. Candidate spans are grouped by (Reader, BlockIdx) so a block shared by several
// candidates is fetched and parsed exactly once, then evaluated via the same
// vm.Program.ColumnPredicate + acquireBlockColumnProvider primitive the structural scan path
// (stream_structural.go) already uses for per-block predicate evaluation — reused here per
// resolved block instead of per whole-query block set.
//
// A nil prog matches every span (mirrors compileStructuralPair's match-all convention — do not
// invent a second "nil means X" rule here). An empty spans list returns (nil, nil) without any
// I/O. Any resolve failure (a candidate naming a block absent from its own reader, or a block
// parse/evaluate failure) is an error — same discipline D3 already applies to materialization
// itself: confirmation must not be a "softer" code path that quietly drops a span a reader failed
// to serve. The returned slice preserves spans' original relative order.
func verifyCandidateSpans(
	ctx context.Context,
	prog *vm.Program,
	spans []ResolvedSpan,
) ([]ResolvedSpan, error) {
	if len(spans) == 0 {
		return nil, nil
	}
	if prog == nil {
		out := make([]ResolvedSpan, len(spans))
		copy(out, spans)
		return out, nil
	}

	groups, order := groupResolvedSpansByBlock(spans)
	survived := make([]bool, len(spans))
	wantColumns := ProgramWantColumns(prog)

	// I/O invariant (HIGH finding, go-presubmit.md): batch ReadBlocks per reader across every
	// distinct block index that reader needs, BEFORE any parsing/evaluation happens — mirrors
	// ResolveTraceGroupSourceRef (structural_traceresolve.go) and QueryTraceQLFromIndex's own
	// coalesced multi-block fetch. Never one ReadBlocks([]int{idx}) call per distinct block, which
	// would throw away modules_reader.Reader's adjacent-block coalescing.
	byReader := make(map[*modules_reader.Reader][]int)
	for _, key := range order {
		byReader[key.reader] = append(byReader[key.reader], key.blockIdx)
	}
	rawByReader := make(map[*modules_reader.Reader]map[int][]byte, len(byReader))
	for reader, idxs := range byReader {
		raw, err := reader.ReadBlocks(idxs)
		if err != nil {
			return nil, fmt.Errorf("verifyCandidateSpans: read blocks: %w", err)
		}
		rawByReader[reader] = raw
	}

	for _, key := range order {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		raw, ok := rawByReader[key.reader][key.blockIdx]
		if !ok {
			return nil, fmt.Errorf(
				"verifyCandidateSpans: sourceRef %q block %d: index/data skew: block does not resolve in reader",
				key.sourceRef, key.blockIdx,
			)
		}
		rowSet, err := evaluateProgramAgainstBlock(key.reader, key.blockIdx, raw, prog, wantColumns)
		if err != nil {
			return nil, fmt.Errorf(
				"verifyCandidateSpans: sourceRef %q block %d: %w", key.sourceRef, key.blockIdx, err,
			)
		}
		for _, idx := range groups[key] {
			if rowSet.Contains(int(spans[idx].RowIdx)) {
				survived[idx] = true
			}
		}
	}

	out := make([]ResolvedSpan, 0, len(spans))
	for i, sp := range spans {
		if survived[i] {
			out = append(out, sp)
		}
	}
	return out, nil
}

// verifyBlockKey groups candidate spans that share the same (reader, block) — sourceRef is
// carried only for error messages, since a (reader, blockIdx) pair alone already uniquely
// identifies the block to fetch.
type verifyBlockKey struct {
	reader    *modules_reader.Reader
	sourceRef string
	blockIdx  int
}

// groupResolvedSpansByBlock partitions spans' indices by their (Reader, BlockIdx), so
// verifyCandidateSpans fetches and parses each distinct block at most once regardless of how many
// candidate spans live in it. order preserves each key's first-seen position for deterministic
// iteration.
func groupResolvedSpansByBlock(spans []ResolvedSpan) (map[verifyBlockKey][]int, []verifyBlockKey) {
	groups := make(map[verifyBlockKey][]int, len(spans))
	order := make([]verifyBlockKey, 0, len(spans))
	for i, sp := range spans {
		key := verifyBlockKey{reader: sp.Reader, sourceRef: sp.SourceRef, blockIdx: sp.BlockIdx}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], i)
	}
	return groups, order
}

// evaluateProgramAgainstBlock parses ONE already-fetched block (raw, restricted to prog's own
// wantColumns) and evaluates prog against it, returning the resulting RowSet. Mirrors the same
// parse/ColumnPredicate sequence stream_structural.go's collectBlockStructuralSpanRecs already
// performs per selected block — applied here to a single targeted block instead of a whole
// query's block set. raw must already have been fetched via a per-reader-batched ReadBlocks call
// (verifyCandidateSpans' caller) — this function does no I/O of its own (HIGH finding,
// go-presubmit.md: block reads must be coalesced per reader, never issued one at a time here).
func evaluateProgramAgainstBlock(
	reader *modules_reader.Reader,
	blockIdx int,
	raw []byte,
	prog *vm.Program,
	wantColumns map[string]struct{},
) (vm.RowSet, error) {
	bwb, parseErr := reader.ParseBlockFromBytes(raw, modules_reader.WantOnly(wantColumns), reader.BlockMeta(blockIdx))
	if parseErr != nil {
		return nil, fmt.Errorf("parse block %d: %w", blockIdx, parseErr)
	}
	provider := acquireBlockColumnProvider(bwb.Block)
	defer releaseBlockColumnProvider(provider)
	rowSet, evalErr := prog.ColumnPredicate(provider)
	if evalErr != nil {
		return nil, fmt.Errorf("evaluate block %d: %w", blockIdx, evalErr)
	}
	return rowSet, nil
}
