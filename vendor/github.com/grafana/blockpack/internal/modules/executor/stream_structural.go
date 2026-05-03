package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"slices"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// NOTE-093: structuralSpanRec uses [8]byte value types for spanID/parentID — no heap allocation.
// present tracks which identity fields were set (spanIDSet/parentIDSet bits).
// [8]byte{} is the zero value but NOT the absent sentinel — use present bits instead.
const (
	structuralSpanIDPresent   uint8 = 1 << 0
	structuralParentIDPresent uint8 = 1 << 1
)

// structuralSpanRec records per-span data collected during a structural query scan.
// nodeMatch bit i is set if the span matches program[i]; bit 0 = node 0 (left), bit 1 = node 1 (right).
// NOTE-093: spanID/parentID are [8]byte value types stored in slice elements — no per-span allocation.
// present bitmask tracks which fields are valid (not relying on zero-value sentinel).
type structuralSpanRec struct {
	spanID    [8]byte
	parentID  [8]byte // zeroed after phase 2; present bit cleared
	parentIdx int     // -1 = root; set during phase 2
	blockIdx  int
	rowIdx    int
	nodeMatch uint8
	present   uint8 // bitmask: structuralSpanIDPresent, structuralParentIDPresent
}

// StructuralResult is the output of ExecuteStructural.
type StructuralResult struct {
	Matches []SpanMatch
}

// ExecuteStructural executes a structural TraceQL query against a modules blockpack Reader.
//
// The algorithm runs in three phases:
//  1. Collect span records (spanID, parentID, nodeMatch bitmask) from every block.
//  2. Resolve parentID references to local indices within each trace.
//  3. Evaluate the structural operators per trace across the chain; emit matching terminal-node spans.
//
// File-level bloom, range, and intrinsic TOC pruning is applied for ALL programs where safe (NOTE-091, NOTE-095, NOTE-097).
// For the LHS of a negation op (!>>, !>, !~), absent LHS means all RHS spans trivially qualify,
// so file-level rejection is skipped for that program. Within the file, all blocks are scanned —
// parent spans may be in any internal block.
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

	filters, ops := traceqlparser.FlattenChain(q)

	// SPEC-STRUCT-8: Guard against chains that exceed the uint8 bitmask capacity.
	const maxStructuralNodes = 8
	if len(filters) > maxStructuralNodes {
		return nil, fmt.Errorf("structural chain too long: %d nodes (max %d)", len(filters), maxStructuralNodes)
	}

	// SPEC-STRUCT-8: Negation operators have undefined semantics in multi-node chains;
	// reject early rather than silently returning empty results.
	if len(ops) > 1 {
		for _, op := range ops {
			if op == traceqlparser.OpNotSibling || op == traceqlparser.OpNotDescendant ||
				op == traceqlparser.OpNotChild {
				return nil, fmt.Errorf("negation operator %s is not supported in multi-node chains", op)
			}
		}
	}

	programs, err := compileStructuralPrograms(filters)
	if err != nil {
		return nil, err
	}

	traceSpans, parsedBlocks, err := collectAllStructuralSpans(
		r,
		programs,
		ops,
		opts.TimeRange,
		opts.StartBlock,
		opts.BlockCount,
	)
	if err != nil {
		return nil, err
	}

	resolveStructuralParentIndices(traceSpans)

	result := &StructuralResult{}
	if err := evalStructuralMatches(traceSpans, parsedBlocks, ops, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

// compileStructuralPrograms compiles each filter expression to a vm.Program.
// A nil filter compiles to a nil program (matches all rows).
func compileStructuralPrograms(filters []*traceqlparser.FilterExpression) ([]*vm.Program, error) {
	programs := make([]*vm.Program, len(filters))
	for i, f := range filters {
		if f == nil {
			continue
		}
		p, err := vm.CompileTraceQLFilter(f)
		if err != nil {
			return nil, fmt.Errorf("compile structural node %d filter: %w", i, err)
		}
		programs[i] = p
	}
	return programs, nil
}

// NOTE-095: shouldRejectFileForProgram returns true when file-level bloom/range rejection on
// programs[progIdx] is safe — i.e., the file can be skipped if that program rejects it.
// For the LHS of a negation op (!>>, !>, !~), absent LHS means all RHS spans trivially
// qualify, so file-level rejection on LHS is NOT safe and must be skipped.
func shouldRejectFileForProgram(ops []traceqlparser.StructuralOp, progIdx int) bool {
	if progIdx == 0 {
		if len(ops) > 0 && isNegationOp(ops[0]) {
			return false
		}
		return true
	}
	// Program[i] for i > 0 is the RHS of ops[i-1].
	// For both negation and non-negation: missing RHS → no output → safe to reject.
	return true
}

// NOTE-095: isNegationOp returns true for structural operators that negate the relationship.
func isNegationOp(op traceqlparser.StructuralOp) bool {
	return op == traceqlparser.OpNotDescendant ||
		op == traceqlparser.OpNotChild ||
		op == traceqlparser.OpNotSibling
}

// collectAllStructuralSpans fetches blocks (optionally filtered by time range and sub-file
// sharding) and accumulates per-trace span records. Returns a map keyed by [16]byte trace ID.
func collectAllStructuralSpans(
	r *modules_reader.Reader,
	programs []*vm.Program,
	ops []traceqlparser.StructuralOp,
	tr queryplanner.TimeRange,
	startBlock, blockCount int,
) (map[[16]byte][]structuralSpanRec, map[int]*modules_reader.Block, error) {
	planner := queryplanner.NewPlanner(r)
	// NOTE-091, NOTE-095: Apply file-level bloom/range pruning for ALL programs where safe.
	// Block-level pruning is intentionally skipped — parent spans may live in any internal
	// block, so all blocks that survive file-level rejection must be scanned.
	for i, prog := range programs {
		if prog == nil || prog.Predicates == nil {
			continue
		}
		if !shouldRejectFileForProgram(ops, i) {
			continue
		}
		nodes := prog.Predicates.Nodes
		if fileLevelReject(r, nodes) || fileLevelBloomReject(r, nodes) {
			return nil, nil, nil
		}
		// NOTE-097: Intrinsic TOC file-level rejection. If the TOC reports zero blocks contain
		// matching spans for this program, the entire file has no qualifying spans — skip it.
		// Non-nil empty slice = zero blocks survive (definitive rejection).
		// Nil = no TOC info or all blocks survive (cannot reject).
		// Block-level subset (non-empty slice) is intentionally ignored — NOTE-091 forbids
		// block-level pruning on the structural path because parent spans may be in any block.
		if tocBlocks := BlocksFromIntrinsicTOC(r, prog); tocBlocks != nil && len(tocBlocks) == 0 {
			return nil, nil, nil
		}
	}
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
		return nil, nil, nil
	}

	rawBlocks, err := planner.FetchBlocks(plan)
	if err != nil {
		return nil, nil, fmt.Errorf("structural FetchBlocks: %w", err)
	}

	result := make(map[[16]byte][]structuralSpanRec, len(plan.SelectedBlocks))
	// parsedBlocks caches the parsed *Block per blockIdx so evalStructuralMatches can
	// populate SpanMatch.Block. Peak memory is bounded by len(plan.SelectedBlocks) parsed
	// blocks, which is the same set already held in rawBlocks — no additional I/O.
	parsedBlocks := make(map[int]*modules_reader.Block, len(plan.SelectedBlocks))
	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		if err := collectBlockStructuralSpanRecs(r, blockIdx, raw, programs, result, parsedBlocks); err != nil {
			return nil, parsedBlocks, err
		}
	}
	return result, parsedBlocks, nil
}

// collectBlockStructuralSpanRecs parses one block and appends span records to result.
// The parsed block is stored in parsedBlocks keyed by blockIdx for later use.
func collectBlockStructuralSpanRecs(
	r *modules_reader.Reader,
	blockIdx int,
	raw []byte,
	programs []*vm.Program,
	result map[[16]byte][]structuralSpanRec,
	parsedBlocks map[int]*modules_reader.Block,
) error {
	meta := r.BlockMeta(blockIdx)

	hasIntrinsic := r.HasIntrinsicSection()

	// Union predicate columns from all programs.
	// For files with an intrinsic section, identity columns (trace:id, span:id,
	// span:parent_id) are served from the intrinsic section — omit from wantColumns.
	// For legacy files (no intrinsic section), identity columns live in block payloads
	// and must be decoded — include them in wantColumns.
	var wantColumns map[string]struct{}
	for _, prog := range programs {
		cols := ProgramWantColumns(prog)
		if cols == nil {
			continue
		}
		if wantColumns == nil {
			wantColumns = make(map[string]struct{}, len(cols))
		}
		for c := range cols {
			wantColumns[c] = struct{}{}
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

	parsedBlocks[blockIdx] = bwb.Block
	provider := newBlockColumnProvider(bwb.Block)
	spanCount := bwb.Block.SpanCount()

	// Evaluate each program against block columns.
	// For files with an intrinsic section, strip intrinsic-column predicates first.
	sets, err := evaluateStructuralPrograms(programs, hasIntrinsic, provider, spanCount, blockIdx)
	if err != nil {
		return err
	}

	n := spanCount

	// Collect intrinsic predicate nodes for post-filtering (intrinsic-section files only).
	// For legacy files, ColumnPredicate already evaluated intrinsic columns from block payloads.
	intrinsicWant := map[string]struct{}{
		"trace:id":       {},
		"span:id":        {},
		"span:parent_id": {},
	}
	var nodesList [][]vm.RangeNode
	if hasIntrinsic {
		nodesList = collectStructuralIntrinsicNodes(programs, intrinsicWant)
	}

	// Resolve identity fields. For files with an intrinsic section, use lookupIntrinsicFieldsTyped.
	// For legacy files, read identity columns directly from decoded block columns.
	// NOTE-081: typed struct eliminates per-row map allocations in the structural hot path.
	var idFields []intrinsicRowFields
	if hasIntrinsic {
		allRefs := make([]modules_shared.BlockRef, n)
		for i := range n {
			allRefs[i] = modules_shared.BlockRef{
				BlockIdx: uint16(blockIdx), //nolint:gosec // safe: blockIdx bounded by file block count (<65535)
				RowIdx:   uint16(i),        //nolint:gosec // safe: i bounded by SpanCount (<65535)
			}
		}
		var intrinsicErr error
		idFields, intrinsicErr = lookupIntrinsicFieldsTyped(r, allRefs, intrinsicWant)
		if intrinsicErr != nil {
			return fmt.Errorf("structural lookupIntrinsicFieldsTyped block %d: %w", blockIdx, intrinsicErr)
		}
	} else {
		idFields = identityFieldsFromBlockColsTyped(bwb.Block, n)
	}

	for rowIdx := range n {
		row := &idFields[rowIdx]
		if row.present&intrinsicPresentTraceID == 0 {
			continue
		}
		traceID := row.traceID

		// NOTE-093: [8]byte direct copy — no allocation needed.
		var rec structuralSpanRec
		rec.parentIdx = -1
		rec.blockIdx = blockIdx
		rec.rowIdx = rowIdx
		if row.present&intrinsicPresentSpanID != 0 {
			rec.spanID = row.spanID
			rec.present |= structuralSpanIDPresent
		}
		if row.present&intrinsicPresentParentID != 0 {
			rec.parentID = row.parentID
			rec.present |= structuralParentIDPresent
		}
		rec.nodeMatch = computeNodeMatchForRow(sets, nodesList, hasIntrinsic, row, rowIdx)
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

// evaluateStructuralPrograms evaluates all N programs against the block column provider.
func evaluateStructuralPrograms(
	programs []*vm.Program,
	hasIntrinsic bool,
	provider vm.ColumnDataProvider,
	spanCount, blockIdx int,
) ([]vm.RowSet, error) {
	sets := make([]vm.RowSet, len(programs))
	for i, prog := range programs {
		var uap *vm.Program
		if hasIntrinsic {
			uap = userAttrProgram(prog)
		} else {
			uap = prog
		}
		s, err := evalStructuralProgram(uap, provider, spanCount)
		if err != nil {
			return nil, fmt.Errorf("structural node %d ColumnPredicate block %d: %w", i, blockIdx, err)
		}
		sets[i] = s
	}
	return sets, nil
}

// computeNodeMatchForRow computes the nodeMatch bitmask for a single row.
// Bit i is set if sets[i] contains rowIdx and (if hasIntrinsic) the intrinsic nodes pass.
// NOTE-081: accepts *intrinsicRowFields (typed) to avoid per-row map allocations.
func computeNodeMatchForRow(
	sets []vm.RowSet,
	nodesList [][]vm.RangeNode,
	hasIntrinsic bool,
	row *intrinsicRowFields,
	rowIdx int,
) uint8 {
	var nodeMatch uint8
	for i, s := range sets {
		if !s.Contains(rowIdx) {
			continue
		}
		passes := true
		if hasIntrinsic && len(nodesList) > i && len(nodesList[i]) > 0 {
			passes = rowSatisfiesIntrinsicNodesTyped(nodesList[i], row)
		}
		if passes {
			nodeMatch |= 1 << uint(i) //nolint:gosec // safe: i bounded by len(programs) <= 8
		}
	}
	return nodeMatch
}

// collectStructuralIntrinsicNodes collects intrinsic predicate nodes from each program,
// adds their column names to want, and returns a per-program node list.
func collectStructuralIntrinsicNodes(programs []*vm.Program, want map[string]struct{}) [][]vm.RangeNode {
	nodesList := make([][]vm.RangeNode, len(programs))
	for i, prog := range programs {
		if prog != nil && prog.Predicates != nil {
			collectIntrinsicNodeColumns(prog.Predicates.Nodes, want)
			nodesList[i] = prog.Predicates.Nodes
		}
	}
	return nodesList
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

// NOTE-079, NOTE-093: resolveStructuralParentIndices uses map[[8]byte]int, not map[string]int,
// to eliminate per-span string allocations on both insert and lookup.
// Span IDs are [8]byte value types (NOTE-093); the present bitmask (structuralSpanIDPresent /
// structuralParentIDPresent) is the authoritative absent indicator — not the zero value.
// The remaining one make() per trace is unavoidable.
func resolveStructuralParentIndices(traceSpans map[[16]byte][]structuralSpanRec) {
	for traceID := range traceSpans {
		spans := traceSpans[traceID]
		// NOTE-079, NOTE-093: [8]byte map key — zero string allocations on insert or lookup.
		// Use present bits (not zero-value sentinel) to distinguish absent from all-zero IDs.
		byID := make(map[[8]byte]int, len(spans))
		for i, sp := range spans {
			if sp.present&structuralSpanIDPresent != 0 {
				byID[sp.spanID] = i
			}
		}
		for i := range spans {
			if spans[i].present&structuralParentIDPresent != 0 {
				if idx, ok := byID[spans[i].parentID]; ok {
					spans[i].parentIdx = idx
				} else {
					spans[i].parentIdx = -1
				}
			} else {
				spans[i].parentIdx = -1
			}
			spans[i].parentID = [8]byte{}
			spans[i].present &^= structuralParentIDPresent
		}
		traceSpans[traceID] = spans
	}
}

// evalStructuralMatches evaluates the structural operator(s) for each trace and
// appends matching terminal spans to result. Stops early if limit is reached.
func evalStructuralMatches(
	traceSpans map[[16]byte][]structuralSpanRec,
	parsedBlocks map[int]*modules_reader.Block,
	ops []traceqlparser.StructuralOp,
	opts Options,
	result *StructuralResult,
) error {
	for traceID, spans := range traceSpans {
		// NOTE-096: Skip traces that cannot possibly produce a structural match.
		if !traceCanMatch(spans, ops) {
			continue
		}
		rightIndices := applyStructuralOps(spans, ops)

		// NOTE-079: slices.Sort + dedup replaces map[int]struct{} — zero extra allocs.
		// rightIndices is a fresh local slice from applyStructuralOp; sorting it is safe.
		slices.Sort(rightIndices)
		prev := -1
		for _, ri := range rightIndices {
			if ri == prev {
				continue
			}
			prev = ri
			if spans[ri].present&structuralSpanIDPresent == 0 {
				continue
			}
			tid := traceID // copy for addressability
			// NOTE-093: [8]byte → []byte conversion at match-emit time is per-match (acceptable);
			// the hot path per-span clone is eliminated.
			match := SpanMatch{
				Block:    parsedBlocks[spans[ri].blockIdx],
				TraceID:  tid,
				SpanID:   append([]byte(nil), spans[ri].spanID[:]...),
				BlockIdx: spans[ri].blockIdx,
				RowIdx:   spans[ri].rowIdx,
			}
			result.Matches = append(result.Matches, match)
			if opts.Limit > 0 && len(result.Matches) >= opts.Limit {
				return nil
			}
		}
	}
	return nil
}

// NOTE-096: traceCanMatch returns false when a bitmask check guarantees no structural match
// is possible for this trace, allowing Phase 3 to be skipped entirely.
// For positive operators (>>, >, ~, <<, <), all node bits must be present.
// For a single negation op (!>>, !>, !~), only bit 1 (RHS) must be present — absent LHS
// means all RHS spans trivially qualify for the negation.
func traceCanMatch(spans []structuralSpanRec, ops []traceqlparser.StructuralOp) bool {
	var present uint8
	for _, sp := range spans {
		present |= sp.nodeMatch
	}
	numNodes := len(ops) + 1
	if len(ops) == 1 && isNegationOp(ops[0]) {
		return present&0x02 != 0
	}
	var required uint8
	for i := range numNodes {
		required |= 1 << uint(i) //nolint:gosec // safe: numNodes <= 8 enforced in ExecuteStructural
	}
	return present&required == required
}

// applyStructuralOps dispatches to the appropriate evaluator based on chain length.
// For a single op (2-node chain) it delegates to applyStructuralOp (unchanged path).
// For N>1 ops it uses evalOpChain.
func applyStructuralOps(spans []structuralSpanRec, ops []traceqlparser.StructuralOp) []int {
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return applyStructuralOp(spans, ops[0])
	}
	return evalOpChain(spans, ops)
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
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		cur := r.parentIdx
		for cur >= 0 {
			if spans[cur].nodeMatch&0x01 != 0 {
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
		if r.nodeMatch&0x02 == 0 || r.parentIdx < 0 {
			continue
		}
		if spans[r.parentIdx].nodeMatch&0x01 != 0 {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpSiblingStruct: node 1 (nodeMatch&0x02) shares a parent with a node 0 (nodeMatch&0x01) span (~), R != L.
// A span qualifies as R if it has at least one node-0-matching sibling OTHER than itself.
// Using a count map handles the case where R also matches node 0 (both sides): it qualifies
// when a distinct second node-0-matching span shares the same parent.
func evalOpSiblingStruct(spans []structuralSpanRec) []int {
	leftCounts := make(map[int]int)
	for _, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			leftCounts[sp.parentIdx]++
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		cnt := leftCounts[r.parentIdx]
		// Qualify if there is at least one left-match sibling OTHER than r itself.
		if cnt > 1 || (cnt == 1 && r.nodeMatch&0x01 == 0) {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpAncestorStruct: R is an ancestor of L (<<) — walk L's parent chain.
func evalOpAncestorStruct(spans []structuralSpanRec) []int {
	result := make([]int, 0, len(spans))
	for _, l := range spans {
		if l.nodeMatch&0x01 == 0 {
			continue
		}
		cur := l.parentIdx
		for cur >= 0 {
			if spans[cur].nodeMatch&0x02 != 0 {
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
		if l.nodeMatch&0x01 == 0 || l.parentIdx < 0 {
			continue
		}
		if spans[l.parentIdx].nodeMatch&0x02 != 0 {
			result = append(result, l.parentIdx)
		}
	}
	return result
}

// evalOpNotSiblingStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// no span with node 0 bit set (nodeMatch&0x01) shares its parent (!~).
func evalOpNotSiblingStruct(spans []structuralSpanRec) []int {
	leftParents := make(map[int]struct{})
	for _, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			leftParents[sp.parentIdx] = struct{}{}
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if _, hasLeft := leftParents[r.parentIdx]; r.nodeMatch&0x02 != 0 && !hasLeft {
			result = append(result, ri)
		}
	}
	return result
}

// SPEC-STRUCT-6: evalOpNotDescendantStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// none of its ancestors has the node 0 bit set (nodeMatch&0x01) (!>>).
// Walk the span's ancestor chain; if no ancestor carries node 0, emit the span.
func evalOpNotDescendantStruct(spans []structuralSpanRec) []int {
	leftSet := make(map[int]struct{})
	for i, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			leftSet[i] = struct{}{}
		}
	}
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
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

// SPEC-STRUCT-7: evalOpNotChildStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// its direct parent does not have the node 0 bit set (nodeMatch&0x01) (!>).
// A span with no parent also qualifies.
func evalOpNotChildStruct(spans []structuralSpanRec) []int {
	result := make([]int, 0, len(spans))
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		if r.parentIdx < 0 || spans[r.parentIdx].nodeMatch&0x01 == 0 {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpChain evaluates an N-node structural chain (N >= 3) using a left-to-right
// intermediate-match-set approach.
//
// NOTE-080: pairwise chain evaluation via intermediate match sets; see NOTES.md.
// For A OP0 B OP1 C:
//  1. Build initial set: indices where nodeMatch bit 0 is set (node 0 matches).
//  2. For each op, advance to the next node using evalOpChainStep.
//  3. Return the final matched indices (terminal node).
func evalOpChain(spans []structuralSpanRec, ops []traceqlparser.StructuralOp) []int {
	prevSet := make(map[int]struct{}, len(spans))
	for i, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			prevSet[i] = struct{}{}
		}
	}

	nodeIdx := 1
	for _, op := range ops {
		mask := uint8(
			1,
		) << uint(
			nodeIdx,
		) //nolint:gosec // safe: nodeIdx < 8, enforced by len(filters) > 8 guard in ExecuteStructural
		prevSet = evalOpChainStep(spans, prevSet, op, mask)
		if len(prevSet) == 0 {
			return nil
		}
		nodeIdx++
	}

	result := make([]int, 0, len(prevSet))
	for i := range prevSet {
		result = append(result, i)
	}
	return result
}

// evalOpChainStep advances one step in the chain: given the set of "left" span indices
// and an operator, returns the set of "right" span indices where the op holds and the
// span has the target nodeMatch bit set.
// NOTE-080: one step of evalOpChain; see NOTES.md.
func evalOpChainStep(
	spans []structuralSpanRec,
	leftSet map[int]struct{},
	op traceqlparser.StructuralOp,
	rightMask uint8,
) map[int]struct{} {
	nextSet := make(map[int]struct{})
	switch op {
	case traceqlparser.OpDescendant:
		for ri, r := range spans {
			if r.nodeMatch&rightMask == 0 {
				continue
			}
			cur := r.parentIdx
			for cur >= 0 {
				if _, ok := leftSet[cur]; ok {
					nextSet[ri] = struct{}{}
					break
				}
				cur = spans[cur].parentIdx
			}
		}
	case traceqlparser.OpChild:
		for ri, r := range spans {
			if r.nodeMatch&rightMask == 0 || r.parentIdx < 0 {
				continue
			}
			if _, ok := leftSet[r.parentIdx]; ok {
				nextSet[ri] = struct{}{}
			}
		}
	case traceqlparser.OpSibling:
		// Use a count map (matching evalOpSiblingStruct) so that a span which is
		// simultaneously in leftSet and matches rightMask can still qualify when
		// there are 2+ left-match spans sharing the same parent.
		leftParentCounts := make(map[int]int, len(leftSet))
		for li := range leftSet {
			leftParentCounts[spans[li].parentIdx]++
		}
		for ri, r := range spans {
			if r.nodeMatch&rightMask == 0 {
				continue
			}
			cnt := leftParentCounts[r.parentIdx]
			_, isLeft := leftSet[ri]
			if cnt > 1 || (cnt == 1 && !isLeft) {
				nextSet[ri] = struct{}{}
			}
		}
	case traceqlparser.OpAncestor:
		for li := range leftSet {
			cur := spans[li].parentIdx
			for cur >= 0 {
				if spans[cur].nodeMatch&rightMask != 0 {
					nextSet[cur] = struct{}{}
				}
				cur = spans[cur].parentIdx
			}
		}
	case traceqlparser.OpParent:
		for li := range leftSet {
			pi := spans[li].parentIdx
			if pi >= 0 && spans[pi].nodeMatch&rightMask != 0 {
				nextSet[pi] = struct{}{}
			}
		}
	default:
		// Negation operators in chains have undefined semantics; return empty set.
	}
	return nextSet
}
