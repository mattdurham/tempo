package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// structuralSpanRec records per-span data collected during a structural query scan.
type structuralSpanRec struct {
	spanID     []byte
	parentID   []byte // nil after phase 2
	parentIdx  int    // -1 = root; set during phase 2
	leftMatch  bool
	rightMatch bool
}

// StructuralResult is the output of ExecuteStructural.
type StructuralResult struct {
	Matches []SpanMatch
}

// ExecuteStructural executes a structural TraceQL query against a modules blockpack Reader.
//
// The algorithm runs in three phases:
//  1. Collect span records (spanID, parentID, leftMatch, rightMatch) from every block.
//  2. Resolve parentID references to local indices within each trace.
//  3. Evaluate the structural operator per trace; emit matching right-side spans.
//
// All blocks are scanned — structural queries cannot use bloom or range pruning because
// any block may contain spans from either side of the operator.
func (e *Executor) ExecuteStructural(
	r *modules_reader.Reader,
	q *traceqlparser.StructuralQuery,
	opts Options,
) (*StructuralResult, error) {
	if r == nil {
		return &StructuralResult{}, nil
	}
	if q == nil {
		return &StructuralResult{}, nil
	}

	leftProg, rightProg, err := compileStructuralFilterPrograms(q)
	if err != nil {
		return nil, err
	}

	traceSpans, err := collectAllStructuralSpans(r, leftProg, rightProg)
	if err != nil {
		return nil, err
	}

	resolveStructuralParentIndices(traceSpans)

	result := &StructuralResult{}
	if err := evalStructuralMatches(traceSpans, q.Op, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

// compileStructuralFilterPrograms compiles the left and right filter expressions.
// A nil FilterExpression compiles to a nil program (matches all rows).
func compileStructuralFilterPrograms(q *traceqlparser.StructuralQuery) (left, right *vm.Program, err error) {
	if q.Left != nil {
		left, err = vm.CompileTraceQLFilter(q.Left)
		if err != nil {
			return nil, nil, fmt.Errorf("compile structural left filter: %w", err)
		}
	}
	if q.Right != nil {
		right, err = vm.CompileTraceQLFilter(q.Right)
		if err != nil {
			return nil, nil, fmt.Errorf("compile structural right filter: %w", err)
		}
	}
	return left, right, nil
}

// collectAllStructuralSpans fetches all blocks and accumulates per-trace span records.
// Returns a map keyed by [16]byte trace ID.
func collectAllStructuralSpans(
	r *modules_reader.Reader,
	leftProg, rightProg *vm.Program,
) (map[[16]byte][]structuralSpanRec, error) {
	planner := queryplanner.NewPlanner(r)
	// Structural queries must scan all blocks — pass nil predicates and zero TimeRange.
	plan := planner.Plan(nil, queryplanner.TimeRange{})

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	rawBlocks, err := planner.FetchBlocks(plan)
	if err != nil {
		return nil, fmt.Errorf("structural FetchBlocks: %w", err)
	}

	result := make(map[[16]byte][]structuralSpanRec)
	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		if err := collectBlockStructuralSpanRecs(r, blockIdx, raw, leftProg, rightProg, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// collectBlockStructuralSpanRecs parses one block and appends span records to result.
func collectBlockStructuralSpanRecs(
	r *modules_reader.Reader,
	blockIdx int,
	raw []byte,
	leftProg, rightProg *vm.Program,
	result map[[16]byte][]structuralSpanRec,
) error {
	meta := r.BlockMeta(blockIdx)

	// Union predicate columns from both programs plus the identity columns we read.
	wantColumns := ProgramWantColumns(leftProg, "trace:id", "span:id", "span:parent_id")
	if rightCols := ProgramWantColumns(rightProg, "trace:id", "span:id", "span:parent_id"); rightCols != nil {
		if wantColumns == nil {
			wantColumns = rightCols
		} else {
			for c := range rightCols {
				wantColumns[c] = struct{}{}
			}
		}
	}

	// NOTE-020: Reset intern strings before each block parse to bound per-reader memory growth.
	r.ResetInternStrings()
	bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
	if err != nil {
		return fmt.Errorf("structural ParseBlockFromBytes block %d: %w", blockIdx, err)
	}

	provider := newBlockColumnProvider(bwb.Block)

	leftSet, err := evalStructuralProgram(leftProg, provider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural left ColumnPredicate block %d: %w", blockIdx, err)
	}
	rightSet, err := evalStructuralProgram(rightProg, provider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural right ColumnPredicate block %d: %w", blockIdx, err)
	}

	traceCol := bwb.Block.GetColumn("trace:id")
	spanCol := bwb.Block.GetColumn("span:id")
	parentCol := bwb.Block.GetColumn("span:parent_id")

	n := bwb.Block.SpanCount()
	for rowIdx := range n {
		var traceID [16]byte
		if traceCol != nil {
			if v, ok := traceCol.BytesValue(rowIdx); ok && len(v) == 16 {
				copy(traceID[:], v)
			} else {
				continue // skip spans without a valid trace ID
			}
		} else {
			continue
		}

		rec := structuralSpanRec{
			spanID:     bytesColCopy(spanCol, rowIdx),
			parentID:   bytesColCopy(parentCol, rowIdx),
			parentIdx:  -1,
			leftMatch:  leftSet.Contains(rowIdx),
			rightMatch: rightSet.Contains(rowIdx),
		}
		result[traceID] = append(result[traceID], rec)
	}
	return nil
}

// evalStructuralProgram evaluates a compiled program against a column provider,
// or returns an all-rows set when prog is nil (matches all spans).
func evalStructuralProgram(prog *vm.Program, provider vm.ColumnDataProvider, spanCount int) (vm.RowSet, error) {
	if prog != nil {
		return prog.ColumnPredicate(provider)
	}
	return allMatchRowSet(spanCount), nil
}

// allMatchSet is a RowSet that matches every row index in [0, n).
type allMatchSet struct{ n int }

func allMatchRowSet(n int) vm.RowSet       { return &allMatchSet{n: n} }
func (a *allMatchSet) Add(_ int)           {}
func (a *allMatchSet) Contains(_ int) bool { return true }
func (a *allMatchSet) Size() int           { return a.n }
func (a *allMatchSet) IsEmpty() bool       { return a.n == 0 }
func (a *allMatchSet) ToSlice() []int {
	s := make([]int, a.n)
	for i := range a.n {
		s[i] = i
	}
	return s
}

// bytesColCopy returns a copy of the bytes column value at rowIdx, or nil if absent.
func bytesColCopy(col *modules_reader.Column, rowIdx int) []byte {
	if col == nil {
		return nil
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok || len(v) == 0 {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// resolveStructuralParentIndices walks each trace's span list, builds a spanID→index
// map, and sets parentIdx for each span. parentID is cleared after resolution.
func resolveStructuralParentIndices(traceSpans map[[16]byte][]structuralSpanRec) {
	for traceID := range traceSpans {
		spans := traceSpans[traceID]
		byID := make(map[string]int, len(spans))
		for i, sp := range spans {
			if len(sp.spanID) > 0 {
				byID[string(sp.spanID)] = i
			}
		}
		for i := range spans {
			if len(spans[i].parentID) == 0 {
				spans[i].parentIdx = -1
			} else if idx, ok := byID[string(spans[i].parentID)]; ok {
				spans[i].parentIdx = idx
			} else {
				spans[i].parentIdx = -1
			}
			spans[i].parentID = nil
		}
		traceSpans[traceID] = spans
	}
}

// evalStructuralMatches evaluates the structural operator for each trace and
// appends matching right-side spans to result. Stops early if limit is reached.
func evalStructuralMatches(
	traceSpans map[[16]byte][]structuralSpanRec,
	op traceqlparser.StructuralOp,
	opts Options,
	result *StructuralResult,
) error {
	for traceID, spans := range traceSpans {
		rightIndices := applyStructuralOp(spans, op)

		seen := make(map[int]bool, len(rightIndices))
		for _, ri := range rightIndices {
			if seen[ri] {
				continue
			}
			seen[ri] = true

			tid := traceID // copy for addressability
			match := SpanMatch{
				TraceID: tid,
				SpanID:  append([]byte(nil), spans[ri].spanID...),
			}
			result.Matches = append(result.Matches, match)
			if opts.Limit > 0 && len(result.Matches) >= opts.Limit {
				return nil
			}
		}
	}
	return nil
}

// applyStructuralOp returns the right-side span indices matched by the operator.
func applyStructuralOp(spans []structuralSpanRec, op traceqlparser.StructuralOp) []int {
	switch op {
	case traceqlparser.OpDescendant:
		return evalOpDescendantStruct(spans)
	case traceqlparser.OpChild:
		return evalOpChildStruct(spans)
	case traceqlparser.OpSibling:
		return evalOpSiblingStruct(spans)
	case traceqlparser.OpAncestor:
		return evalOpAncestorStruct(spans)
	case traceqlparser.OpParent:
		return evalOpParentStruct(spans)
	case traceqlparser.OpNotSibling:
		return evalOpNotSiblingStruct(spans)
	case traceqlparser.OpNotDescendant:
		return evalOpNotDescendantStruct(spans)
	case traceqlparser.OpNotChild:
		return evalOpNotChildStruct(spans)
	default:
		return nil
	}
}

// evalOpDescendantStruct: R is a descendant of L (>>) — walk R's ancestor chain.
func evalOpDescendantStruct(spans []structuralSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		cur := r.parentIdx
		for cur >= 0 {
			if spans[cur].leftMatch {
				result = append(result, ri)
				break
			}
			cur = spans[cur].parentIdx
		}
	}
	return result
}

// evalOpChildStruct: R's direct parent is L (>).
func evalOpChildStruct(spans []structuralSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch || r.parentIdx < 0 {
			continue
		}
		if spans[r.parentIdx].leftMatch {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpSiblingStruct: R shares a parent with a leftMatch span (~), R != L.
func evalOpSiblingStruct(spans []structuralSpanRec) []int {
	leftParents := make(map[int]bool)
	for _, sp := range spans {
		if sp.leftMatch {
			leftParents[sp.parentIdx] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		if leftParents[r.parentIdx] && !r.leftMatch {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpAncestorStruct: R is an ancestor of L (<<) — walk L's parent chain.
func evalOpAncestorStruct(spans []structuralSpanRec) []int {
	var result []int
	for _, l := range spans {
		if !l.leftMatch {
			continue
		}
		cur := l.parentIdx
		for cur >= 0 {
			if spans[cur].rightMatch {
				result = append(result, cur)
			}
			cur = spans[cur].parentIdx
		}
	}
	return result
}

// evalOpParentStruct: R is the direct parent of L (<).
func evalOpParentStruct(spans []structuralSpanRec) []int {
	var result []int
	for _, l := range spans {
		if !l.leftMatch || l.parentIdx < 0 {
			continue
		}
		if spans[l.parentIdx].rightMatch {
			result = append(result, l.parentIdx)
		}
	}
	return result
}

// evalOpNotSiblingStruct: R is rightMatch with no leftMatch sibling (!~).
func evalOpNotSiblingStruct(spans []structuralSpanRec) []int {
	leftParents := make(map[int]bool)
	for _, sp := range spans {
		if sp.leftMatch {
			leftParents[sp.parentIdx] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if r.rightMatch && !leftParents[r.parentIdx] {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpNotDescendantStruct: R is rightMatch but NOT a descendant of any leftMatch span (!>>).
// Walk R's ancestor chain; if no ancestor is leftMatch, emit R.
func evalOpNotDescendantStruct(spans []structuralSpanRec) []int {
	leftSet := make(map[int]bool)
	for i, sp := range spans {
		if sp.leftMatch {
			leftSet[i] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		isDescendant := false
		cur := r.parentIdx
		for cur >= 0 {
			if leftSet[cur] {
				isDescendant = true
				break
			}
			cur = spans[cur].parentIdx
		}
		if !isDescendant {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpNotChildStruct: R is rightMatch but its direct parent is NOT leftMatch (!>).
// R qualifies if it has no parent, or its parent is not leftMatch.
func evalOpNotChildStruct(spans []structuralSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		if r.parentIdx < 0 || !spans[r.parentIdx].leftMatch {
			result = append(result, ri)
		}
	}
	return result
}
