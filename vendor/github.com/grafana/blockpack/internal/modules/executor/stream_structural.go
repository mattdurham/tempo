package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"math/bits"
	"slices"
	"sync"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
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

// zeroed after phase 2; present bit cleared
// -1 = root; set during phase 2

// bitmask: structuralSpanIDPresent, structuralParentIDPresent

// StructuralResult is the output of ExecuteStructural.

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

	traceSpans = resolveStructuralParentIndices(traceSpans, ops)

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
) ([][]structuralSpanRec, map[int]*modules_reader.Block, error) {
	// NOTE-091: Each structural node is a regular filter program with an added relationship
	// constraint. Block selection uses planBlocks per program — the same bloom/range/intrinsic-TOC
	// pruning as plain filter queries — and the selected block sets are unioned.
	// Any block absent from every program's selected set has no spans matching any node.
	// Limitation: intermediate ancestor spans that do not match any node predicate but are
	// required to link a descendant to an ancestor may reside in a pruned block, causing false
	// negatives for multi-block traces. This is acceptable in the common case where all spans
	// of a trace reside within a single internal block. See NOTE-091 in NOTES.md.
	//
	// Negation-LHS programs (shouldRejectFileForProgram=false) use planBlocks(nil, tr, opts)
	// so the time range is still applied but no predicate pruning is attempted for that node.
	// NOTE-425: capture each program's OWN selected-block set (not just the union). A block
	// absent from program i's set has no span matching node i (block-level range/intrinsic-TOC
	// pruning is exact; bloom carries the already-accepted file-level FPR), so evaluating node
	// i's predicate there is wasted column decode. progBlockSets[i] == nil means "evaluate on
	// every block" — used for negation-LHS nodes whose time-range-only plan does not bound the
	// node-match set, and for the all-blocks {} node.
	unionSet := make(map[int]struct{})
	progBlockSets := make([]map[int]struct{}, len(programs))
	for i, prog := range programs {
		var p *queryplanner.Plan
		gated := shouldRejectFileForProgram(ops, i)
		if !gated {
			// Negation LHS — absent LHS means all RHS spans qualify; use time-range-only plan.
			p = planBlocks(r, nil, tr, queryplanner.PlanOptions{})
		} else {
			p = planBlocks(r, prog, tr, queryplanner.PlanOptions{})
		}
		if len(p.SelectedBlocks) == 0 && gated {
			// planBlocks rejected the file entirely for this non-negation program —
			// no structural match is possible (the node cannot match any span in this file).
			return nil, nil, nil
		}
		// Only a gated (predicate-pruned) node has a block set that bounds where it can match.
		// A negation-LHS node's time-range-only plan does not, so leave its set nil.
		if gated {
			set := make(map[int]struct{}, len(p.SelectedBlocks))
			for _, bi := range p.SelectedBlocks {
				set[bi] = struct{}{}
			}
			progBlockSets[i] = set
		}
		for _, bi := range p.SelectedBlocks {
			unionSet[bi] = struct{}{}
		}
	}

	selectedBlocks := make([]int, 0, len(unionSet))
	for bi := range unionSet {
		selectedBlocks = append(selectedBlocks, bi)
	}
	slices.Sort(selectedBlocks)

	plan := &queryplanner.Plan{SelectedBlocks: selectedBlocks}

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

	fetcher := queryplanner.NewPlanner(r)
	rawBlocks, err := fetcher.FetchBlocks(plan)
	if err != nil {
		return nil, nil, fmt.Errorf("structural FetchBlocks: %w", err)
	}

	// NOTE-373: the per-block fetch/eval plan (wantColumns, the user-attr program set, and
	// the intrinsic post-filter node lists) is identical for every selected block — it derives
	// only from `programs` and `hasIntrinsic`, not from block contents. Compute it ONCE here
	// instead of rebuilding the wantColumns map, re-deriving userAttrProgram, and rebuilding
	// the nodesList on every block (collectBlockStructuralSpanRecs was a top alloc frame: the
	// per-block wantColumns map rebuild alone was ~16MB/op in the structural bench). The shared
	// plan is read-only across blocks.
	bp := buildStructuralBlockPlan(r, programs, progBlockSets)

	// NOTE-373: accumulate span records into a single FLAT slice across all blocks instead of
	// appending into result[traceID] per row. The previous map-append (result[traceID] =
	// append(...)) was the dominant structural-path allocation (~368MB flat in the bench): every
	// distinct trace's slice started at cap 0 and was grown by repeated reallocation, and the
	// map probe ran once per span row. Collecting flat then grouping ONCE (group-by sort on the
	// 16-byte trace ID) replaces O(spans) slice growations with a single contiguous backing
	// array and exactly len(traces) final sub-slices, each sized exactly.
	// NOTE-377: pre-size flat to the exact upper bound on records (sum of SpanCount over the
	// selected blocks, read from BlockMeta without parsing). collectBlockStructuralSpanRecs
	// appends one record per row that carries a trace ID — at most SpanCount per block — so the
	// summed span count is a tight upper bound. Sizing the backing array once eliminates the
	// O(log spans) reallocation+memmove chain that the per-row cross-block append paid as flat
	// grew (collectBlockStructuralSpanRecs→growslice was ~1% of querier CPU on the structural
	// path, profile 2026-06-15). Rows without a trace ID never append, so the slice may finish
	// shorter than capacity — harmless, the surplus is never touched.
	var totalSpans int
	for _, blockIdx := range plan.SelectedBlocks {
		if _, ok := rawBlocks[blockIdx]; ok {
			totalSpans += int(r.BlockMeta(blockIdx).SpanCount)
		}
	}
	// NOTE-436: draw the cross-block accumulator from a pool. It is fully consumed by
	// groupMatchingStructuralTraces below (which scatters survivors into a fresh backing array
	// and returns windows aliasing THAT array, not flat) and is never retained past this call,
	// so it is released before return on every path. structuralSpanRec is pointer-free, so the
	// reused backing pins no string/byte payload between queries.
	flat := acquireStructuralSpanRecs(totalSpans)
	// parsedBlocks caches the parsed *Block per blockIdx so evalStructuralMatches can
	// populate SpanMatch.Block. Peak memory is bounded by len(plan.SelectedBlocks) parsed
	// blocks, which is the same set already held in rawBlocks — no additional I/O.
	parsedBlocks := make(map[int]*modules_reader.Block, len(plan.SelectedBlocks))
	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		var err error
		flat, err = collectBlockStructuralSpanRecs(r, blockIdx, raw, &bp, flat, parsedBlocks)
		if err != nil {
			// NOTE-436: flat holds the latest (possibly reallocated) pooled backing — release it.
			releaseStructuralSpanRecs(flat)
			return nil, parsedBlocks, err
		}
	}

	// NOTE-387: fuse the matched-trace filter and the group-by into a single counting bucket
	// scatter — no comparison sort. A trace can contribute a structural match only if ≥1 of its
	// spans matched a node (nodeMatch != 0); this holds for every op type including negation,
	// where the qualifying side is the RHS bit (0x02), itself a nonzero nodeMatch (see
	// traceCanMatch). NOTE-386 already paid one full hashing pass to build the matched-trace set,
	// then dropped non-matching records in place and handed the survivors to an O(n log n) 16-byte
	// trace-ID sort. groupMatchingStructuralTraces reuses that one hashing pass: it assigns each
	// matched trace a dense slot and accumulates its surviving-record count, then a prefix sum
	// turns the counts into bucket offsets and a single linear scatter places each surviving
	// record directly into its trace's contiguous window in a freshly sized backing array. That
	// replaces (compaction pass + O(n log n) sort) with (one scatter pass + one backing alloc).
	// Whole traces are kept or dropped as a unit: a kept trace retains ALL its spans (including
	// nodeMatch==0 intermediates) so the parent-topology chain resolveStructuralParentIndices
	// walks stays intact. Non-matching records are never copied, so the scatter only touches the
	// survivors. On the all-matching workload every record is a survivor and the scatter is a
	// full copy, but it is still a single linear pass with no comparisons.
	result := groupMatchingStructuralTraces(flat)
	// NOTE-436: flat is fully consumed by groupMatchingStructuralTraces (result aliases a fresh
	// backing array, never flat) — release the pooled accumulator for reuse by the next query.
	releaseStructuralSpanRecs(flat)
	return result, parsedBlocks, nil
}

// groupMatchingStructuralTraces drops records belonging to traces that have no matched span
// (every span's nodeMatch == 0) AND groups the survivors into one contiguous window per trace,
// in a single counting bucket scatter with no comparison sort. See NOTE-387.
//
// A trace qualifies iff at least one of its spans matched a node (nodeMatch != 0); whole
// traces are kept or dropped as a unit so a kept trace retains every span (including the
// nodeMatch==0 intermediates that resolveStructuralParentIndices walks). The returned windows
// alias a freshly allocated backing array (not flat), each clamped to its run length so a
// downstream append could not bleed into the next trace.
//
// Bucket state (dense slot index + running count/cursor) lives in slot-indexed slices, not in a
// per-trace heap struct: the map carries only trace ID → slot (one int value, no pointer
// allocation), and the count and cursor are looked up by slot in flat int slices. This keeps the
// per-matched-trace cost to a single map entry with no satellite allocation.
func groupMatchingStructuralTraces(flat []structuralSpanRec) [][]structuralSpanRec {
	if len(flat) == 0 {
		return nil
	}
	// Pass 1: assign each trace carrying ≥1 matched span a dense slot index on first sighting.
	// A trace is only ever added by a matched span; non-matching records of a trace already in
	// the set are counted in pass 2 (a kept trace retains ALL its spans).
	slotOf := make(map[[16]byte]int)
	for i := range flat {
		if flat[i].nodeMatch == 0 {
			continue
		}
		if _, ok := slotOf[flat[i].traceID]; !ok {
			slotOf[flat[i].traceID] = len(slotOf)
		}
	}
	nSlots := len(slotOf)
	if nSlots == 0 {
		// No trace can match — nothing survives.
		return nil
	}
	// Pass 2: count survivors per matched trace by slot (every span of a kept trace survives).
	// offsets is sized nSlots+1; counts accumulate into offsets[slot+1] so the prefix sum below
	// turns it directly into per-window start offsets.
	//
	// NOTE-422: cache each record's resolved slot in recSlot during this pass so pass 3 can
	// scatter without re-probing the [16]byte map. The map key is a 16-byte trace ID hashed
	// via aeshashbody; the profile (2026-06-16) showed mapaccess2/aeshashbody dominating the
	// structural-grouping self-time because the prior body probed slotOf for EVERY record in
	// BOTH the count pass and the scatter pass (~2n probes total). Recording the slot once
	// (recSlot[i] = slot, or -1 for a record whose trace did not match) drops the scatter pass
	// to a flat int32 read, halving the per-record map probes (count pass keeps the one probe).
	// recSlot costs 4 bytes/record — far cheaper than a 16-byte AES hash per record.
	offsets := make([]int, nSlots+1)
	// NOTE-434: recSlot is a per-query len(flat)-sized int32 scratch (4 bytes/record over the
	// WHOLE flat, including the non-matching records that dominate a low-match structural query
	// like a deep >> chain). It is fully built then consumed by pass 3 before this call returns
	// and nothing retains it, so draw it from compactInt32Pool instead of allocating (and
	// memclr-zeroing then fully overwriting) a fresh array every query. acquireCompactInt32
	// clears the prefix; the loop assigns every element (slot or -1), so the clear is redundant
	// but harmless and the pool amortizes the allocation + its GC churn across query traffic.
	recSlot := acquireCompactInt32(len(flat))
	defer releaseCompactInt32(recSlot)
	total := 0
	for i := range flat {
		if slot, ok := slotOf[flat[i].traceID]; ok {
			offsets[slot+1]++
			total++
			recSlot[i] = int32(slot) //nolint:gosec // slot < nSlots ≤ record count, well within int32
		} else {
			recSlot[i] = -1
		}
	}
	for s := 1; s <= nSlots; s++ {
		offsets[s] += offsets[s-1]
	}
	// cursors[slot] is the live write position for the trace's window; initialized to the
	// window start offset (offsets[slot]).
	cursors := make([]int, nSlots)
	copy(cursors, offsets[:nSlots])
	// Pass 3: scatter survivors into trace-contiguous windows of a single backing array,
	// reading the cached slot (NOTE-422) instead of re-probing the [16]byte map.
	backing := make([]structuralSpanRec, total)
	for i := range flat {
		slot := recSlot[i]
		if slot < 0 {
			continue
		}
		backing[cursors[slot]] = flat[i]
		cursors[slot]++
	}
	// Carve the backing array into per-trace windows using the offsets, clamping cap to length.
	out := make([][]structuralSpanRec, nSlots)
	for s := range nSlots {
		lo := offsets[s]
		hi := offsets[s+1]
		out[s] = backing[lo:hi:hi]
	}
	return out
}

// structuralBlockPlan holds the per-query, block-independent inputs to
// collectBlockStructuralSpanRecs. It is built once per structural query and shared
// (read-only) across every selected block. See NOTE-373.
type structuralBlockPlan struct {
	programs    []*vm.Program
	wantColumns map[string]struct{}
	// intrinsicWant is the identity-column set requested from the intrinsic section
	// (also augmented with the intrinsic predicate columns when hasIntrinsic).
	intrinsicWant map[string]struct{}
	// nodesList[i] holds the intrinsic predicate RangeNodes for program i, used by the
	// post-filter (computeNodeMatchForRow). Nil for legacy (no-intrinsic) files.
	nodesList [][]vm.RangeNode
	// progBlockSets[i] is program i's own selected-block set (NOTE-425). A non-nil set means
	// node i can match a span only in those blocks, so its predicate is skipped on any other
	// block (empty rowset). A nil entry means "evaluate on every block".
	progBlockSets []map[int]struct{}
	hasIntrinsic  bool
}

// buildStructuralBlockPlan computes the block-independent fetch/eval plan once per query.
func buildStructuralBlockPlan(
	r *modules_reader.Reader,
	programs []*vm.Program,
	progBlockSets []map[int]struct{},
) structuralBlockPlan {
	hasIntrinsic := r.HasIntrinsicSection()

	// Union predicate columns from all programs. For intrinsic-section files, identity and
	// intrinsic predicate columns are served from the intrinsic section (NOTE-372), so build
	// wantColumns from userAttrProgram(prog) to omit them from the block fetch. Legacy files
	// have no intrinsic section, so they keep the full set including identity columns.
	var wantColumns map[string]struct{}
	for _, prog := range programs {
		wantProg := prog
		if hasIntrinsic {
			wantProg = userAttrProgram(prog)
		}
		cols := ProgramWantColumns(wantProg)
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

	intrinsicWant := map[string]struct{}{
		"trace:id":       {},
		"span:id":        {},
		"span:parent_id": {},
	}
	var nodesList [][]vm.RangeNode
	if hasIntrinsic {
		nodesList = collectStructuralIntrinsicNodes(programs, intrinsicWant)
	}

	return structuralBlockPlan{
		programs:      programs,
		hasIntrinsic:  hasIntrinsic,
		wantColumns:   wantColumns,
		intrinsicWant: intrinsicWant,
		nodesList:     nodesList,
		progBlockSets: progBlockSets,
	}
}

// collectBlockStructuralSpanRecs parses one block and appends span records to the flat slice,
// returning the (possibly reallocated) slice. The parsed block is stored in parsedBlocks keyed
// by blockIdx for later use. The block-independent plan (bp) is built once per query
// (buildStructuralBlockPlan) and shared read-only across all blocks — see NOTE-373.
//
// NOTE-372: for intrinsic-section files, ALL intrinsic predicate columns (span:kind,
// span:duration, span:status, resource.service.name, span:name, …) and identity columns
// (trace:id, span:id, span:parent_id) are omitted from bp.wantColumns. These are served from
// the intrinsic section via the per-program nodesList post-filter (computeNodeMatchForRow →
// rowSatisfiesIntrinsicNodesTyped reading idFields) and predicate evaluation runs against the
// pre-derived user-attr program set, so the parser never fetches/decodes the redundant
// block-payload copy of each intrinsic column. Legacy (no-intrinsic) files keep the full set
// including identity columns, which must be decoded from block payloads.
func collectBlockStructuralSpanRecs(
	r *modules_reader.Reader,
	blockIdx int,
	raw []byte,
	bp *structuralBlockPlan,
	flat []structuralSpanRec,
	parsedBlocks map[int]*modules_reader.Block,
) ([]structuralSpanRec, error) {
	meta := r.BlockMeta(blockIdx)

	hasIntrinsic := bp.hasIntrinsic

	// NOTE-020: Reset intern strings before each block parse to bound per-reader memory growth.
	r.ResetInternStrings()
	bwb, err := r.ParseBlockFromBytes(raw, modules_reader.WantOnly(bp.wantColumns), meta)
	if err != nil {
		return flat, fmt.Errorf("structural ParseBlockFromBytes block %d: %w", blockIdx, err)
	}

	parsedBlocks[blockIdx] = bwb.Block
	provider := acquireBlockColumnProvider(bwb.Block)
	spanCount := bwb.Block.SpanCount()

	// Evaluate each program against block columns.
	// For files with an intrinsic section, strip intrinsic-column predicates first.
	sets, err := evaluateStructuralPrograms(
		bp.programs, hasIntrinsic, provider, spanCount, blockIdx, bp.progBlockSets,
	)
	if err != nil {
		releaseBlockColumnProvider(provider)
		return flat, err
	}

	n := spanCount

	nodesList := bp.nodesList
	intrinsicWant := bp.intrinsicWant

	// Resolve identity fields. For files with an intrinsic section, use
	// lookupIntrinsicFieldsTypedForBlock (NOTE-100): one binary search per column instead
	// of one per span, eliminating the allRefs allocation and O(N×log(B×N)) binary searches.
	// For legacy files, read identity columns directly from decoded block columns.
	// NOTE-081: typed struct eliminates per-row map allocations in the structural hot path.
	var idFields []intrinsicRowFields
	if hasIntrinsic {
		var intrinsicErr error
		idFields, intrinsicErr = lookupIntrinsicFieldsTypedForBlock(
			r,
			uint16(blockIdx), //nolint:gosec // safe: blockIdx bounded by file block count (<65535)
			n,
			intrinsicWant,
		)
		if intrinsicErr != nil {
			releaseBlockColumnProvider(provider)
			return flat, fmt.Errorf(
				"structural lookupIntrinsicFieldsTypedForBlock block %d: %w",
				blockIdx,
				intrinsicErr,
			)
		}
	} else {
		idFields = identityFieldsFromBlockColsTyped(bwb.Block, n)
	}
	// NOTE-349: idFields is pooled scratch (both branches draw from the pool). It is fully
	// consumed by the row loop below and copied out into structuralSpanRec entries; nothing
	// retains a reference past this function, so release it on every exit path.
	defer putIntrinsicRowFields(idFields)

	// NOTE-432: precompute the per-row predicate-match bitmask in O(sum of set sizes) so the
	// row loop reads a bit with one array index instead of binary-searching every program's
	// RowSet per row (the per-block hot loop on the union-of-block-sets structural path).
	predBits := computeStructuralPredBits(sets, n)
	defer releaseStructuralPredBits(predBits)

	for rowIdx := range n {
		row := &idFields[rowIdx]
		if row.present&intrinsicPresentTraceID == 0 {
			continue
		}

		// NOTE-093: [8]byte direct copy — no allocation needed.
		// NOTE-373: carry the trace ID on the record and append to the flat cross-block
		// slice; grouping by trace ID happens once at the end (groupMatchingStructuralTraces).
		var rec structuralSpanRec
		rec.traceID = row.traceID
		rec.parentIdx = -1
		rec.blockIdx = uint16(blockIdx) //nolint:gosec // blockIdx bounded by file block count (<65535)
		rec.rowIdx = uint16(rowIdx)     //nolint:gosec // rowIdx bounded by SpanCount (≤65535)
		if row.present&intrinsicPresentSpanID != 0 {
			rec.spanID = row.spanID
			rec.present |= structuralSpanIDPresent
		}
		if row.present&intrinsicPresentParentID != 0 {
			rec.parentID = row.parentID
			rec.present |= structuralParentIDPresent
		}
		rec.nodeMatch = computeNodeMatchForRow(predBits[rowIdx], nodesList, hasIntrinsic, row)
		flat = append(flat, rec)
	}
	// Release after the row loop: sets may contain scratch-backed rowSets (single-predicate programs).
	// Releasing earlier would allow pool reuse to overwrite p.scratch before computeNodeMatchForRow
	// reads it via s.Contains().
	releaseBlockColumnProvider(provider)
	return flat, nil
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
//
// NOTE-425: when progBlockSets[i] is non-nil and does not contain blockIdx, node i's predicate
// cannot match any span in this block (block-level pruning excluded it). Skip the ColumnPredicate
// call — which would decode and scan node i's user-attribute columns on this block — and return
// the emptyRowSet sentinel. This is byte-identical to running the predicate (the pruned block has
// no matching span, modulo the bloom FPR already accepted at file level) but avoids the wasted
// per-block decode that dominates the structural CPU profile on union-of-block-sets shapes.
func evaluateStructuralPrograms(
	programs []*vm.Program,
	hasIntrinsic bool,
	provider vm.ColumnDataProvider,
	spanCount, blockIdx int,
	progBlockSets []map[int]struct{},
) ([]vm.RowSet, error) {
	sets := make([]vm.RowSet, len(programs))
	for i, prog := range programs {
		if i < len(progBlockSets) && progBlockSets[i] != nil {
			if _, ok := progBlockSets[i][blockIdx]; !ok {
				sets[i] = emptyRowSet{}
				continue
			}
		}
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

// structuralPredBitsPool pools the per-row predicate-match bitmask scratch used by the
// structural block scan. NOTE-432.
var structuralPredBitsPool sync.Pool //nolint:gochecknoglobals

// acquireStructuralPredBits returns a zeroed []uint8 of length n from the pool.
func acquireStructuralPredBits(n int) []uint8 {
	if v := structuralPredBitsPool.Get(); v != nil {
		if s, ok := v.([]uint8); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]uint8, n)
}

// releaseStructuralPredBits returns the scratch to the pool.
func releaseStructuralPredBits(s []uint8) {
	if cap(s) > compactPoolMaxPooledBytes { // NOTE-355: drop oversized outlier (1 byte/elem)
		return
	}
	structuralPredBitsPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// computeStructuralPredBits builds a per-row predicate-match bitmask in O(sum of set sizes)
// instead of O(spanCount × programs × log(matched-rows)). NOTE-432.
//
// computeNodeMatchForRow's predecessor probed every program's RowSet with a binary search
// (rowSet.Contains) once PER ROW — O(spanCount × programs × log M) over the whole block scan,
// the dominant per-block CPU on the structural union-of-block-sets path (Q9 walks every span of
// every block selected by EITHER node). Each RowSet is already a sorted ascending slice of
// matched rows, so scattering each set's matched rows directly into a per-row bitmask is a single
// linear pass per program (O(M)) that visits only the matched rows — typically far fewer than the
// full span count. The row loop then reads bit i with one array index instead of a binary search.
//
// allMatchSet (the {} node) matches every row: set the bit for all rows in one tight loop without
// materializing its ToSlice() (which would allocate a spanCount-sized int slice). emptyRowSet
// contributes nothing. *rowSet exposes its sorted backing slice via ToSlice() (no copy).
//
// The returned scratch is pooled; the caller must release it.
func computeStructuralPredBits(sets []vm.RowSet, spanCount int) []uint8 {
	predBits := acquireStructuralPredBits(spanCount)
	for i, s := range sets {
		mask := uint8(1) << uint(i) //nolint:gosec // safe: i bounded by len(programs) <= 8
		if am, ok := s.(*allMatchSet); ok {
			// Every row in [0, am.n) matches — set the bit without allocating ToSlice().
			n := am.n
			if n > spanCount {
				n = spanCount
			}
			for r := range n {
				predBits[r] |= mask
			}
			continue
		}
		for _, r := range s.ToSlice() {
			if r >= 0 && r < spanCount {
				predBits[r] |= mask
			}
		}
	}
	return predBits
}

// computeNodeMatchForRow computes the nodeMatch bitmask for a single row, given the
// precomputed predicate-match bits for that row (predBits, bit i = sets[i] contains rowIdx).
// Bit i is set if predBits has bit i and (if hasIntrinsic) the intrinsic nodes pass.
// NOTE-081: accepts *intrinsicRowFields (typed) to avoid per-row map allocations.
// NOTE-432: the predicate membership test is precomputed (computeStructuralPredBits) so this
// only folds in the per-row intrinsic-node check, eliminating the per-row binary search.
func computeNodeMatchForRow(
	predBits uint8,
	nodesList [][]vm.RangeNode,
	hasIntrinsic bool,
	row *intrinsicRowFields,
) uint8 {
	var nodeMatch uint8
	for predBits != 0 {
		i := bits.TrailingZeros8(predBits)
		mask := uint8(1) << uint(i) //nolint:gosec // safe: i bounded by len(programs) <= 8
		predBits &^= mask
		passes := true
		if hasIntrinsic && len(nodesList) > i && len(nodesList[i]) > 0 {
			// NOTE-435: nodesList[i] is pre-pruned to intrinsic leaves (prepareIntrinsicNodes),
			// so the map-free evaluator is exact here.
			passes = rowSatisfiesPreparedIntrinsicNodes(nodesList[i], row)
		}
		if passes {
			nodeMatch |= mask
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
			// NOTE-435: prune the node tree to its intrinsic leaves ONCE here so the per-row
			// computeNodeMatchForRow loop (rowSatisfiesPreparedIntrinsicNodes) never repeats
			// the traceIntrinsicColumns map probe per span of every block in the union.
			nodesList[i] = prepareIntrinsicNodes(prog.Predicates.Nodes)
		}
	}
	return nodesList
}

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
//
// NOTE-271: reuse ONE byID map across all traces instead of make()-ing a fresh map per
// trace. The previous per-trace make(map[[8]byte]int, len(spans)) was one heap allocation
// (plus the bucket array) for every trace in the result — on a structural query (>>) that
// matches many small traces (the measured Q9 workload) this was N map allocations driving
// GC pressure on the structural hot path. The map is span-ID → row-index scoped strictly
// within a single trace's parent resolution: it is fully built then fully read before the
// next trace, so clear()-ing it between traces (Go's clear is a cheap bucket reset that
// retains the backing array) gives identical semantics with a single amortized allocation
// that grows to the largest trace's span count. The clear runs at the TOP of each trace so
// the very first iteration starts empty regardless of pool reuse.
//
// NOTE-382: skip parent resolution for traces that cannot produce a structural match. The
// per-trace work below — clear(byID), build the spanID→index map (one pass), then resolve every
// span's parentID (second pass with a map probe) — is pure waste for a trace whose spans match
// no relevant node, because a non-qualifying trace emits nothing. traceCanMatch is a cheap OR
// over the per-span nodeMatch bits (no map, no allocation); running it FIRST lets the common
// structural workload, where most traces contain no matching span at all (the measured Q9 returns
// 0 traces over a large span population), skip the map clear/build/probe for every such trace.
// ops is threaded in so the qualification predicate is evaluated once; a nil ops disables the
// skip and resolves every trace unconditionally (used by resolution-only unit tests that do not
// populate nodeMatch). See NOTE-383 for how the qualified subset is carried forward to eliminate
// the duplicate scan that evalStructuralMatches previously performed.
// NOTE-383: when ops != nil, resolveStructuralParentIndices COMPACTS traceSpans in place to
// the subset of traces that pass traceCanMatch and returns that prefix. The qualification scan
// (traceCanMatch: an OR over every span's nodeMatch bits) previously ran twice per trace — once
// here to gate parent resolution (NOTE-382) and again in evalStructuralMatches to gate emission
// — duplicating O(spans) work per trace on the dominant zero-match structural case (Q9 returns 0
// traces over a large span population). Carrying the qualified subset forward lets the caller skip
// the second scan entirely: evalStructuralMatches no longer re-runs traceCanMatch because every
// trace it receives is already known to qualify. Compaction is order-preserving and reuses the
// backing array (no allocation). When ops == nil (resolution-only unit tests) the skip is
// disabled and the input is resolved and returned unchanged.
func resolveStructuralParentIndices(
	traceSpans [][]structuralSpanRec,
	ops []traceqlparser.StructuralOp,
) [][]structuralSpanRec {
	// NOTE-384: pre-size byID to the largest per-trace window. The map is allocated ONCE and
	// clear()'d per trace (NOTE-271), but with no capacity hint Go's map grew incrementally on
	// the first trace it filled, rehashing as it crossed each load-factor threshold. Sizing it
	// up front to the widest trace window means the largest trace — and every trace after the
	// first clear() that reuses the already-grown buckets — inserts without a single rehash.
	// maxWin is an O(traces) scan over slice headers (no per-span work); the hint is an upper
	// bound (a trace may carry fewer span-ID-present rows than its length), never an under-size.
	maxWin := 0
	for _, spans := range traceSpans {
		if len(spans) > maxWin {
			maxWin = len(spans)
		}
	}
	byID := make(map[[8]byte]int, maxWin)
	w := 0
	for _, spans := range traceSpans {
		if ops != nil && !traceCanMatch(spans, ops) {
			continue
		}
		// NOTE-079, NOTE-093: [8]byte map key — zero string allocations on insert or lookup.
		// Use present bits (not zero-value sentinel) to distinguish absent from all-zero IDs.
		// NOTE-271: clear (not re-make) so the prior trace's entries do not leak in.
		clear(byID)
		for i, sp := range spans {
			if sp.present&structuralSpanIDPresent != 0 {
				byID[sp.spanID] = i
			}
		}
		for i := range spans {
			if spans[i].present&structuralParentIDPresent != 0 {
				if idx, ok := byID[spans[i].parentID]; ok {
					spans[i].parentIdx = int32(idx) //nolint:gosec // idx is a per-trace span index, well within int32
				} else {
					spans[i].parentIdx = -1
				}
			} else {
				spans[i].parentIdx = -1
			}
			spans[i].parentID = [8]byte{}
			spans[i].present &^= structuralParentIDPresent
		}
		traceSpans[w] = spans
		w++
	}
	return traceSpans[:w]
}

// evalStructuralMatches evaluates the structural operator(s) for each trace and
// appends matching terminal spans to result. Stops early if limit is reached.
func evalStructuralMatches(
	traceSpans [][]structuralSpanRec,
	parsedBlocks map[int]*modules_reader.Block,
	ops []traceqlparser.StructuralOp,
	opts Options,
	result *StructuralResult,
) error {
	// NOTE-385: scratch is a single per-call buffer reused across all traces for the matched
	// right-side index slice. The single-op evaluators (the dominant 2-node path, e.g. {a}>>{b})
	// previously each allocated make([]int, 0, len(spans)) PER qualified trace — sized to the full
	// per-trace span count even though the realized match set is typically tiny or empty. Threading
	// one reusable buffer (reset to [:0] per trace) collapses those O(traces) allocations into a
	// single buffer that grows to the high-water mark and is then reused. It is fully consumed
	// (sorted/deduped/emitted) before the next trace overwrites it, so no result aliases it.
	var scratch []int
	for _, spans := range traceSpans {
		if len(spans) == 0 {
			continue
		}
		// NOTE-377: every record in a window shares the trace ID (grouped contiguously); read it
		// from the first record instead of a map key.
		traceID := spans[0].traceID
		// NOTE-383: traceCanMatch is NOT re-run here. resolveStructuralParentIndices already
		// compacted traceSpans to the qualified subset, so every trace reaching this loop is
		// known to pass the gate. Re-scanning here would duplicate the OR-over-nodeMatch work.
		rightIndices := applyStructuralOps(spans, ops, scratch[:0])
		scratch = rightIndices

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
				Block:    parsedBlocks[int(spans[ri].blockIdx)],
				TraceID:  tid,
				SpanID:   append([]byte(nil), spans[ri].spanID[:]...),
				BlockIdx: int(spans[ri].blockIdx),
				RowIdx:   int(spans[ri].rowIdx),
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
// NOTE-385: dst is a caller-owned reusable buffer (already reset to len 0). The single-op
// evaluators append into it and return the grown slice so the caller can re-supply it next
// trace. The N>1 chain path uses set semantics and ignores dst.
func applyStructuralOps(spans []structuralSpanRec, ops []traceqlparser.StructuralOp, dst []int) []int {
	if len(ops) == 0 {
		return nil
	}
	if len(ops) == 1 {
		return applyStructuralOp(spans, ops[0], dst)
	}
	return evalOpChain(spans, ops)
}

// applyStructuralOp returns the right-side span indices matched by the operator, appended into
// the caller-supplied dst buffer (NOTE-385).
func applyStructuralOp(spans []structuralSpanRec, op traceqlparser.StructuralOp, dst []int) []int {
	switch op {
	case traceqlparser.OpDescendant:
		return evalOpDescendantStruct(spans, dst)
	case traceqlparser.OpChild:
		return evalOpChildStruct(spans, dst)
	case traceqlparser.OpSibling:
		return evalOpSiblingStruct(spans, dst)
	case traceqlparser.OpAncestor:
		return evalOpAncestorStruct(spans, dst)
	case traceqlparser.OpParent:
		return evalOpParentStruct(spans, dst)
	case traceqlparser.OpNotSibling:
		return evalOpNotSiblingStruct(spans, dst)
	case traceqlparser.OpNotDescendant:
		return evalOpNotDescendantStruct(spans, dst)
	case traceqlparser.OpNotChild:
		return evalOpNotChildStruct(spans, dst)
	default:
		return nil
	}
}

// memo tri-state values for the node-0-ancestor existence walk (NOTE-392).
const (
	memoUnknown uint8 = 0
	memoYes     uint8 = 1 // span has a node-0 (LHS) ancestor
	memoNo      uint8 = 2 // span has no node-0 ancestor
)

// hasNode0AncestorMemo returns, for the span at index ri, whether any of its strict ancestors
// carries the node-0 (LHS) bit. memo is a per-trace tri-state scratch slice (len == len(spans))
// seeded along each walk so shared ancestor-chain prefixes are traversed at most once across all
// RHS spans of the trace — turning the naive O(RHS×depth) chain walk into O(spans) amortized.
// See NOTE-392.
func hasNode0AncestorMemo(spans []structuralSpanRec, ri int, memo []uint8) bool {
	// Walk up, stopping early on a cached node or an LHS-matching ancestor; record the resolved
	// outcome for every previously-unknown node on the walked path.
	cur := spans[ri].parentIdx
	found := false
	for cur >= 0 {
		if m := memo[cur]; m != memoUnknown {
			found = m == memoYes
			break
		}
		if spans[cur].nodeMatch&0x01 != 0 {
			found = true
			break
		}
		cur = spans[cur].parentIdx
	}
	outcome := memoNo
	if found {
		outcome = memoYes
	}
	for p := spans[ri].parentIdx; p >= 0 && p != cur; p = spans[p].parentIdx {
		if memo[p] != memoUnknown {
			break
		}
		memo[p] = outcome
	}
	return found
}

// evalOpDescendantStruct: R is a descendant of L (>>) — true when R has a node-0 ancestor.
func evalOpDescendantStruct(spans []structuralSpanRec, dst []int) []int {
	memo := // NOTE-392: memoized ancestor-existence walk; see hasNode0AncestorMemo.
	acquireCompactUint8(len(spans))
	defer releaseCompactUint8(memo)

	result := dst
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		if hasNode0AncestorMemo(spans, ri, memo) {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpChildStruct: R's direct parent is L (>).
func evalOpChildStruct(spans []structuralSpanRec, dst []int) []int {
	result := dst
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
func evalOpSiblingStruct(spans []structuralSpanRec, dst []int) []int {
	leftCounts := make(map[int]int)
	for _, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			leftCounts[int(sp.parentIdx)]++
		}
	}
	result := dst
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		cnt := leftCounts[int(r.parentIdx)]
		// Qualify if there is at least one left-match sibling OTHER than r itself.
		if cnt > 1 || (cnt == 1 && r.nodeMatch&0x01 == 0) {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpAncestorStruct: R is an ancestor of L (<<) — walk L's parent chain.
//
// NOTE-393: memoize the ancestor-chain collection with a per-trace "collected" bitmap, mirroring
// the existence-walk memo of NOTE-392 but adapted to the collect-ALL semantics of <<. Each L-match
// walks its parent chain appending every node-1 ancestor it encounters; when several L-matches
// share an ancestor-chain prefix (a deep trace, or many leaf L-matches under one common ancestor),
// the same prefix — and every node-1 emit on it — was re-walked per L-match: O(L × depth).
//
// A node X's strict-ancestor chain is identical for every span that walks through X. So once any
// L-walk has reached X and continued upward to the root, every node-1 ancestor strictly above X is
// already in result. A later walk arriving at X can therefore stop: it has already contributed
// every node-1 ancestor at-or-above X's parent on a prior pass. The downstream emit (NOTE-079)
// sorts + dedups rightIndices, so the duplicate appends a shared subtree would otherwise produce
// are collapsed — meaning stopping early at a "collected" node yields byte-identical output while
// traversing each parent edge at most once globally → O(spans) amortized.
//
// collected[X] is set the moment a walk *enters* X (before testing/emitting X's own node-1 bit and
// before ascending past it), so the very next walk that reaches X halts immediately. The first walk
// that reaches X still emits X (if node-1) and ascends, seeding collected for the whole prefix above.
func evalOpAncestorStruct(spans []structuralSpanRec, dst []int) []int {
	result := dst
	collected := acquireCompactBool(len(spans))
	defer releaseCompactBool(collected)

	for _, l := range spans {
		if l.nodeMatch&0x01 == 0 {
			continue
		}
		cur := l.parentIdx
		for cur >= 0 {
			if collected[cur] {
				// Every node-1 ancestor at-or-above cur was emitted by a prior walk; the
				// downstream sort+dedup collapses the overlap. Stop re-walking the prefix.
				break
			}
			collected[cur] = true
			if spans[cur].nodeMatch&0x02 != 0 {
				result = append(result, int(cur))
			}
			cur = spans[cur].parentIdx
		}
	}
	return result
}

// evalOpParentStruct: R is the direct parent of L (<).
func evalOpParentStruct(spans []structuralSpanRec, dst []int) []int {
	result := dst
	for _, l := range spans {
		if l.nodeMatch&0x01 == 0 || l.parentIdx < 0 {
			continue
		}
		if spans[l.parentIdx].nodeMatch&0x02 != 0 {
			result = append(result, int(l.parentIdx))
		}
	}
	return result
}

// evalOpNotSiblingStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// no span with node 0 bit set (nodeMatch&0x01) shares its parent (!~).
func evalOpNotSiblingStruct(spans []structuralSpanRec, dst []int) []int {
	leftParents := acquireCompactBool(len(spans) + 1)
	defer releaseCompactBool(leftParents)
	for _, sp := range spans {
		if sp.nodeMatch&0x01 != 0 {
			leftParents[int(sp.parentIdx)+1] = true
		}
	}
	result := dst
	for ri, r := range spans {
		if hasLeft := leftParents[int(r.parentIdx)+1]; r.nodeMatch&0x02 != 0 && !hasLeft {
			result = append(result, ri)
		}
	}
	return result
}

// SPEC-STRUCT-6: evalOpNotDescendantStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// none of its ancestors has the node 0 bit set (nodeMatch&0x01) (!>>).
// Walk the span's ancestor chain; if no ancestor carries node 0, emit the span.
func evalOpNotDescendantStruct(spans []structuralSpanRec, dst []int) []int {
	memo := // NOTE-392: same memoized ancestor-existence walk as the positive descendant op (the node-0
	// membership is already encoded by nodeMatch&0x01, so the prior per-span leftSet map was
	// redundant and is dropped). !>> emits RHS spans with NO node-0 ancestor.
	acquireCompactUint8(len(spans))
	defer releaseCompactUint8(memo)

	result := dst
	for ri, r := range spans {
		if r.nodeMatch&0x02 == 0 {
			continue
		}
		if !hasNode0AncestorMemo(spans, ri, memo) {
			result = append(result, ri)
		}
	}
	return result
}

// SPEC-STRUCT-7: evalOpNotChildStruct: a span with node 1 bit set (nodeMatch&0x02) qualifies when
// its direct parent does not have the node 0 bit set (nodeMatch&0x01) (!>).
// A span with no parent also qualifies.
func evalOpNotChildStruct(spans []structuralSpanRec, dst []int) []int {
	result := dst
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
				if _, ok := leftSet[int(cur)]; ok {
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
			if _, ok := leftSet[int(r.parentIdx)]; ok {
				nextSet[ri] = struct{}{}
			}
		}
	case traceqlparser.OpSibling:
		// Use a count map (matching evalOpSiblingStruct) so that a span which is
		// simultaneously in leftSet and matches rightMask can still qualify when
		// there are 2+ left-match spans sharing the same parent.
		leftParentCounts := make(map[int]int, len(leftSet))
		for li := range leftSet {
			leftParentCounts[int(spans[li].parentIdx)]++
		}
		for ri, r := range spans {
			if r.nodeMatch&rightMask == 0 {
				continue
			}
			cnt := leftParentCounts[int(r.parentIdx)]
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
					nextSet[int(cur)] = struct{}{}
				}
				cur = spans[cur].parentIdx
			}
		}
	case traceqlparser.OpParent:
		for li := range leftSet {
			pi := spans[li].parentIdx
			if pi >= 0 && spans[pi].nodeMatch&rightMask != 0 {
				nextSet[int(pi)] = struct{}{}
			}
		}
	default:
		// Negation operators in chains have undefined semantics; return empty set.
	}
	return nextSet
}
