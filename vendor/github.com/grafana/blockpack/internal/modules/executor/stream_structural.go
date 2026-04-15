package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
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
func ExecuteStructural(
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

	traceSpans, err := collectAllStructuralSpans(
		r,
		leftProg,
		rightProg,
		opts.TimeRange,
		opts.StartBlock,
		opts.BlockCount,
	)
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

// collectAllStructuralSpans fetches blocks (optionally filtered by time range and sub-file
// sharding) and accumulates per-trace span records. Returns a map keyed by [16]byte trace ID.
func collectAllStructuralSpans(
	r *modules_reader.Reader,
	leftProg, rightProg *vm.Program,
	tr queryplanner.TimeRange,
	startBlock, blockCount int,
) (map[[16]byte][]structuralSpanRec, error) {
	planner := queryplanner.NewPlanner(r)
	// Structural queries pass nil predicates (no value-based pruning) but do use time-range pruning.
	plan := planner.Plan(nil, tr)

	// Sub-file sharding: restrict to assigned block range.
	if blockCount > 0 {
		endBlock := startBlock + blockCount
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if bi >= startBlock && bi < endBlock {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
	}

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	rawBlocks, err := planner.FetchBlocks(plan)
	if err != nil {
		return nil, fmt.Errorf("structural FetchBlocks: %w", err)
	}

	result := make(map[[16]byte][]structuralSpanRec, len(plan.SelectedBlocks))
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

	hasIntrinsic := r.HasIntrinsicSection()

	// Union predicate columns from both programs.
	// For files with an intrinsic section, identity columns (trace:id, span:id,
	// span:parent_id) are served from the intrinsic section — omit from wantColumns.
	// For legacy files (no intrinsic section), identity columns live in block payloads
	// and must be decoded — include them in wantColumns.
	wantColumns := ProgramWantColumns(leftProg)
	if rightCols := ProgramWantColumns(rightProg); rightCols != nil {
		if wantColumns == nil {
			wantColumns = rightCols
		} else {
			for c := range rightCols {
				wantColumns[c] = struct{}{}
			}
		}
	}
	if !hasIntrinsic {
		if wantColumns == nil {
			wantColumns = make(map[string]struct{})
		}
		wantColumns["trace:id"] = struct{}{}
		wantColumns["span:id"] = struct{}{}
		wantColumns["span:parent_id"] = struct{}{}
	}

	// NOTE-020: Reset intern strings before each block parse to bound per-reader memory growth.
	r.ResetInternStrings()
	bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
	if err != nil {
		return fmt.Errorf("structural ParseBlockFromBytes block %d: %w", blockIdx, err)
	}

	provider := newBlockColumnProvider(bwb.Block)

	// For files with an intrinsic section, strip intrinsic-column predicates before
	// evaluating against block columns (they are absent from block payloads).
	// For legacy files, intrinsic columns exist in block payloads — evaluate them
	// directly via ColumnPredicate without stripping.
	// userAttrProgram returns nil when all predicates are intrinsic → match-all.
	var uapLeft, uapRight *vm.Program
	if hasIntrinsic {
		uapLeft = userAttrProgram(leftProg)
		uapRight = userAttrProgram(rightProg)
	} else {
		uapLeft = leftProg
		uapRight = rightProg
	}

	leftSet, err := evalStructuralProgram(uapLeft, provider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural left ColumnPredicate block %d: %w", blockIdx, err)
	}
	rightSet, err := evalStructuralProgram(uapRight, provider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural right ColumnPredicate block %d: %w", blockIdx, err)
	}

	n := bwb.Block.SpanCount()

	// Collect intrinsic predicate nodes for post-filtering (intrinsic-section files only).
	// For legacy files, ColumnPredicate already evaluated intrinsic columns from block payloads.
	intrinsicWant := map[string]struct{}{
		"trace:id":       {},
		"span:id":        {},
		"span:parent_id": {},
	}
	var leftIntrinsicNodes, rightIntrinsicNodes []vm.RangeNode
	if hasIntrinsic {
		leftIntrinsicNodes, rightIntrinsicNodes = collectStructuralIntrinsicNodes(
			leftProg, rightProg, intrinsicWant,
		)
	}

	// Resolve identity fields. For files with an intrinsic section, use lookupIntrinsicFields.
	// For legacy files, read identity columns directly from decoded block columns.
	var idFields []map[string]any
	if hasIntrinsic {
		allRefs := make([]modules_shared.BlockRef, n)
		for i := range n {
			allRefs[i] = modules_shared.BlockRef{
				BlockIdx: uint16(blockIdx), //nolint:gosec // safe: blockIdx bounded by file block count (<65535)
				RowIdx:   uint16(i),        //nolint:gosec // safe: i bounded by SpanCount (<65535)
			}
		}
		var intrinsicErr error
		idFields, intrinsicErr = lookupIntrinsicFields(r, allRefs, intrinsicWant)
		if intrinsicErr != nil {
			return fmt.Errorf("structural lookupIntrinsicFields block %d: %w", blockIdx, intrinsicErr)
		}
	} else {
		idFields = identityFieldsFromBlockCols(bwb.Block, n)
	}

	for rowIdx := range n {
		fields := idFields[rowIdx]
		if fields == nil {
			continue
		}
		tidRaw, ok := fields["trace:id"]
		if !ok {
			continue
		}
		tidBytes, ok := tidRaw.([]byte)
		if !ok || len(tidBytes) != 16 {
			continue
		}
		var traceID [16]byte
		copy(traceID[:], tidBytes)

		var spanIDBytes []byte
		if sidRaw, ok2 := fields["span:id"]; ok2 {
			if b, ok3 := sidRaw.([]byte); ok3 && len(b) > 0 {
				spanIDBytes = append([]byte(nil), b...)
			}
		}

		var parentIDBytes []byte
		if pidRaw, ok2 := fields["span:parent_id"]; ok2 {
			if b, ok3 := pidRaw.([]byte); ok3 && len(b) > 0 {
				parentIDBytes = append([]byte(nil), b...)
			}
		}

		// Post-filter block-column matches against intrinsic predicate nodes.
		// For legacy files (no intrinsic section), ColumnPredicate already evaluated
		// intrinsic columns from block payloads; skip the post-filter.
		leftMatch := leftSet.Contains(rowIdx)
		rightMatch := rightSet.Contains(rowIdx)
		if hasIntrinsic {
			leftMatch = leftMatch && rowSatisfiesIntrinsicNodes(leftIntrinsicNodes, fields)
			rightMatch = rightMatch && rowSatisfiesIntrinsicNodes(rightIntrinsicNodes, fields)
		}

		rec := structuralSpanRec{
			spanID:     spanIDBytes,
			parentID:   parentIDBytes,
			parentIdx:  -1,
			leftMatch:  leftMatch,
			rightMatch: rightMatch,
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

// collectStructuralIntrinsicNodes collects intrinsic predicate nodes from both programs
// into leftNodes and rightNodes, and adds their column names to want.
func collectStructuralIntrinsicNodes(
	leftProg, rightProg *vm.Program, want map[string]struct{},
) (leftNodes, rightNodes []vm.RangeNode) {
	if leftProg != nil && leftProg.Predicates != nil {
		collectIntrinsicNodeColumns(leftProg.Predicates.Nodes, want)
		leftNodes = leftProg.Predicates.Nodes
	}
	if rightProg != nil && rightProg.Predicates != nil {
		collectIntrinsicNodeColumns(rightProg.Predicates.Nodes, want)
		rightNodes = rightProg.Predicates.Nodes
	}
	return leftNodes, rightNodes
}

// identityFieldsFromBlockCols builds a per-row identity field map by reading
// trace:id, span:id, and span:parent_id directly from block columns.
// Used as a fallback for legacy files that have no intrinsic TOC section.
func identityFieldsFromBlockCols(block *modules_reader.Block, n int) []map[string]any {
	traceCol := block.GetColumn("trace:id")
	spanCol := block.GetColumn("span:id")
	parentCol := block.GetColumn("span:parent_id")
	out := make([]map[string]any, n)
	for rowIdx := range n {
		fields := make(map[string]any, 3)
		appendBytesField(fields, "trace:id", traceCol, rowIdx)
		appendBytesField(fields, "span:id", spanCol, rowIdx)
		appendBytesField(fields, "span:parent_id", parentCol, rowIdx)
		if len(fields) > 0 {
			out[rowIdx] = fields
		}
	}
	return out
}

// appendBytesField reads a bytes value from col at rowIdx and stores a copy in fields[key].
// No-ops when col is nil or the value is absent/empty.
func appendBytesField(fields map[string]any, key string, col *modules_reader.Column, rowIdx int) {
	if col == nil {
		return
	}
	if v, ok := col.BytesValue(rowIdx); ok && len(v) > 0 {
		fields[key] = append([]byte(nil), v...)
	}
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

		seen := make(map[int]struct{}, len(rightIndices))
		for _, ri := range rightIndices {
			if _, ok := seen[ri]; ok {
				continue
			}
			seen[ri] = struct{}{}

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
	result := make([]int, 0, len(spans))
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
	result := make([]int, 0, len(spans))
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
// A span qualifies as R if it has at least one left-matching sibling OTHER than itself.
// Using a count map handles the case where R also matches L (both sides): it qualifies
// when a distinct second left-matching span shares the same parent.
func evalOpSiblingStruct(spans []structuralSpanRec) []int {
	leftCounts := make(map[int]int)
	for _, sp := range spans {
		if sp.leftMatch {
			leftCounts[sp.parentIdx]++
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		cnt := leftCounts[r.parentIdx]
		// Qualify if there is at least one left-match sibling OTHER than r itself.
		if cnt > 1 || (cnt == 1 && !r.leftMatch) {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpAncestorStruct: R is an ancestor of L (<<) — walk L's parent chain.
func evalOpAncestorStruct(spans []structuralSpanRec) []int {
	result := make([]int, 0, len(spans))
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
	result := make([]int, 0, len(spans))
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
	leftParents := make(map[int]struct{})
	for _, sp := range spans {
		if sp.leftMatch {
			leftParents[sp.parentIdx] = struct{}{}
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if _, hasLeft := leftParents[r.parentIdx]; r.rightMatch && !hasLeft {
			result = append(result, ri)
		}
	}
	return result
}

// SPEC-STRUCT-6: evalOpNotDescendantStruct: R is rightMatch but NOT a descendant of any leftMatch span (!>>).
// Walk R's ancestor chain; if no ancestor is leftMatch, emit R.
func evalOpNotDescendantStruct(spans []structuralSpanRec) []int {
	leftSet := make(map[int]struct{})
	for i, sp := range spans {
		if sp.leftMatch {
			leftSet[i] = struct{}{}
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		isDescendant := false
		cur := r.parentIdx
		for cur >= 0 {
			if _, ok := leftSet[cur]; ok {
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

// SPEC-STRUCT-7: evalOpNotChildStruct: R is rightMatch but its direct parent is NOT leftMatch (!>).
// R qualifies if it has no parent, or its parent is not leftMatch.
func evalOpNotChildStruct(spans []structuralSpanRec) []int {
	result := make([]int, 0, len(spans))
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
