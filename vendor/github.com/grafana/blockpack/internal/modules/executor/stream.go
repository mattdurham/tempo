package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// errNeedBlockScan is returned by collectIntrinsicPlain and collectIntrinsicTopK when the
// intrinsic fast path is not applicable and the caller should fall through to a full block scan.
// NOTE-039: sentinel error for fallback from intrinsic to block scan.
var errNeedBlockScan = errors.New("intrinsic fast path not applicable")

// collectMode identifies the execution strategy selected by classifyCollect.
type collectMode int

const (
	// modeIntrinsicTopK: intrinsic-only predicate + MostRecent (timestamp sort) + Limit.
	// Uses intrinsic column blobs for both predicate evaluation and timestamp ranking.
	// No full block reads for predicate evaluation.
	modeIntrinsicTopK collectMode = iota

	// modeIntrinsicPlain: intrinsic-only predicate + Limit, no timestamp sort.
	// Uses intrinsic column blobs for predicate evaluation; fetches only blocks
	// containing matching refs.
	modeIntrinsicPlain

	// modeBlockTopK: timestamp sort + Limit, predicate requires full block reads.
	// Heap-based scan over all selected blocks to find globally correct top-K by timestamp.
	modeBlockTopK

	// modeBlockPlain: default sequential scan over selected blocks.
	modeBlockPlain
)

// classifyCollect determines the execution strategy for a Collect call.
// It checks HasIntrinsicSection() definitively so the chosen collectX function
// uses errNeedBlockScan to signal fallback when the fast path is not applicable. See NOTE-038.
// NOTE-039: programCanUseIntrinsicFastPath workaround removed; OR and regex are now handled
// by evalNodeBlockRefs/evalNodeMatchKeys with errNeedBlockScan fallback.
func classifyCollect(r *modules_reader.Reader, program *vm.Program, opts CollectOptions) collectMode {
	if opts.Limit > 0 && ProgramIsIntrinsicOnly(program) && r.HasIntrinsicSection() {
		if opts.TimestampColumn != "" {
			return modeIntrinsicTopK
		}
		return modeIntrinsicPlain
	}
	if opts.TimestampColumn != "" && opts.Limit > 0 {
		return modeBlockTopK
	}
	return modeBlockPlain
}

// computeColumnSets computes wantColumns and secondPassCols once for a Collect call.
// Both values are loop-invariant and must not be recomputed per block. See NOTE-028.
//
// wantColumns: minimal column set needed by the query predicate (nil = all columns).
// secondPassCols: columns to decode in the second pass (nil = all columns).
//
//	nil when AllColumns == true or when wantColumns == nil.
//	Otherwise: searchMetaColumns ∪ wantColumns.
func computeColumnSets(program *vm.Program, opts CollectOptions) (wantColumns, secondPassCols map[string]struct{}) {
	wantColumns = ProgramWantColumns(program)
	if wantColumns != nil && !opts.AllColumns {
		searchCols := searchMetaColumns()
		secondPassCols = make(map[string]struct{}, len(searchCols)+len(wantColumns))
		for k := range searchCols {
			secondPassCols[k] = struct{}{}
		}
		for k := range wantColumns {
			secondPassCols[k] = struct{}{}
		}
	}
	return wantColumns, secondPassCols
}

// CollectOptions configures collect execution for both trace and log signals.
type CollectOptions struct {
	// OnStats is an optional callback invoked after execution with I/O statistics.
	OnStats func(CollectStats)
	// TimestampColumn is the column for per-row time filtering.
	// Empty string disables per-row filtering (trace mode).
	// "log:timestamp" enables per-row filtering (log mode).
	TimestampColumn string
	TimeRange       queryplanner.TimeRange
	Limit           int
	// Direction controls block traversal order. Default (zero value) is Forward.
	Direction queryplanner.Direction
	// NOTE-028: AllColumns controls second-pass decode scope.
	// false (default): second pass decodes searchMetaColumns ∪ wantColumns (predicate columns).
	// true: second pass decodes all columns. Only needed when the callback calls IterateFields()
	// to enumerate every attribute. Search queries never need this.
	AllColumns bool
	// StartBlock is the first internal block index to include (0-based, inclusive).
	// Used by the frontend sharder to partition a single file across multiple jobs.
	// 0 with BlockCount==0 means scan all blocks (no sub-file sharding).
	StartBlock int
	// BlockCount is the number of internal blocks to include starting from StartBlock.
	// 0 means no sub-file sharding (scan all blocks selected by the planner).
	BlockCount int
}

// CollectStats reports block I/O statistics after execution.
type CollectStats struct {
	// Explain is an ASCII trace of how the predicate tree resolved to block sets.
	Explain        string
	TotalBlocks    int
	PrunedByTime   int
	PrunedByIndex  int
	PrunedByFuse   int // blocks eliminated by BinaryFuse8 membership checks
	PrunedByCMS    int // blocks eliminated by Count-Min Sketch zero-estimate checks
	SelectedBlocks int
	// FetchedBlocks is the number of individual blocks actually fetched from storage (actual I/O).
	// It is counted per coalesced group at ReadGroup time, so a group with N blocks contributes N
	// when that group is fetched. FetchedBlocks <= SelectedBlocks when a Limit causes early stop
	// and some groups are never read.
	FetchedBlocks int
}

// MatchedRow holds a single row result from Collect or CollectTopK.
type MatchedRow struct {
	Block *modules_reader.Block
	// IntrinsicFields is set when the result was produced by the intrinsic fast path
	// without reading full blocks. The caller should use this for field lookups
	// when Block is nil.
	IntrinsicFields modules_shared.SpanFieldsProvider
	BlockIdx        int
	RowIdx          int
}

// Collect selects candidate blocks via queryplanner and evaluates program.ColumnPredicate
// against each block's spans, collecting all matched rows into a slice.
//
// SPEC-STREAM-2: Blocks are fetched lazily via CoalescedGroups/ReadGroup (~8 MB per I/O).
// SPEC-STREAM-3: FetchedBlocks <= SelectedBlocks; early stop skips unfetched groups.
// SPEC-STREAM-4: TimestampColumn == "" disables per-row time filtering (trace mode).
// SPEC-STREAM-5: Direction is applied at plan time; rows are reversed within each block for Backward.
// SPEC-STREAM-6: OnStats is deferred; FetchedBlocks reflects actual I/O at completion.
func (e *Executor) Collect(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
) ([]MatchedRow, error) {
	// SPEC-STREAM-1: nil reader returns nil result and nil error.
	if r == nil {
		return nil, nil
	}
	if program == nil {
		return nil, fmt.Errorf("executor.Collect: program must not be nil")
	}
	if opts.StartBlock < 0 || opts.BlockCount < 0 {
		return nil, fmt.Errorf(
			"executor.Collect: invalid shard parameters: StartBlock=%d BlockCount=%d",
			opts.StartBlock,
			opts.BlockCount,
		)
	}
	if opts.BlockCount > 0 && opts.StartBlock+opts.BlockCount < opts.StartBlock {
		return nil, fmt.Errorf(
			"executor.Collect: shard range overflow: StartBlock=%d BlockCount=%d",
			opts.StartBlock,
			opts.BlockCount,
		)
	}

	// Step 1: Classify — determine execution mode upfront. See NOTE-038.
	mode := classifyCollect(r, program, opts)

	// Step 2: Compute column sets once (loop-invariant). See NOTE-028.
	wantColumns, secondPassCols := computeColumnSets(program, opts)

	// Step 3: Plan — block selection via planBlocks. See NOTE-036.
	plan := planBlocks(r, program, opts.TimeRange, queryplanner.PlanOptions{
		Direction: opts.Direction,
		Limit:     opts.Limit,
	})

	// Step 4: Apply sub-file sharding (post-planner, pre-fetch).
	if opts.BlockCount > 0 {
		endBlock := opts.StartBlock + opts.BlockCount
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if bi >= opts.StartBlock && bi < endBlock {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
	}

	// SPEC-STREAM-6: Defer stats callback so FetchedBlocks reflects actual I/O.
	fetchedBlocks := 0
	if opts.OnStats != nil {
		defer func() {
			opts.OnStats(CollectStats{
				TotalBlocks:    plan.TotalBlocks,
				PrunedByTime:   plan.PrunedByTime,
				PrunedByIndex:  plan.PrunedByIndex,
				PrunedByFuse:   plan.PrunedByFuse,
				PrunedByCMS:    plan.PrunedByCMS,
				SelectedBlocks: len(plan.SelectedBlocks),
				FetchedBlocks:  fetchedBlocks,
				Explain:        plan.Explain,
			})
		}()
	}

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	// Step 5: Dispatch to the named collect function.
	// NOTE-039: intrinsic modes return errNeedBlockScan when the fast path is not applicable
	// (e.g., OR/regex predicates that couldn't be evaluated). Fall through to block scan.
	switch mode {
	case modeIntrinsicTopK:
		rows, err := collectIntrinsicTopK(r, program, opts, wantColumns, secondPassCols)
		if err != errNeedBlockScan {
			return rows, err
		}
		// Fall through to block scan.
		groups := r.CoalescedGroups(plan.SelectedBlocks)
		blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
		for gi, g := range groups {
			for _, bi := range g.BlockIDs {
				blockToGroup[bi] = gi
			}
		}
		var scanErr error
		fetchedBlocks, rows, scanErr = collectBlockTopK(
			r, program, wantColumns, opts, plan, groups, blockToGroup,
		)
		if scanErr != nil {
			return nil, scanErr
		}
		return rows, nil
	case modeIntrinsicPlain:
		rows, err := collectIntrinsicPlain(r, program, opts, wantColumns, secondPassCols)
		if err != errNeedBlockScan {
			return rows, err
		}
		// Fall through to block scan.
		groups := r.CoalescedGroups(plan.SelectedBlocks)
		blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
		for gi, g := range groups {
			for _, bi := range g.BlockIDs {
				blockToGroup[bi] = gi
			}
		}
		var scanErr error
		fetchedBlocks, rows, scanErr = collectBlockPlain(
			r, program, wantColumns, secondPassCols, opts, plan, groups, blockToGroup,
		)
		if scanErr != nil {
			return nil, scanErr
		}
		return rows, nil
	default:
		// Block modes need coalesced groups. Build them here, not inside collectX,
		// so they are constructed only when a block mode is active. See NOTE-038.
		groups := r.CoalescedGroups(plan.SelectedBlocks)
		blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
		for gi, g := range groups {
			for _, bi := range g.BlockIDs {
				blockToGroup[bi] = gi
			}
		}
		var results []MatchedRow
		var scanErr error
		switch mode {
		case modeBlockTopK:
			fetchedBlocks, results, scanErr = collectBlockTopK(
				r, program, wantColumns, opts, plan, groups, blockToGroup,
			)
		default: // modeBlockPlain
			fetchedBlocks, results, scanErr = collectBlockPlain(
				r, program, wantColumns, secondPassCols, opts, plan, groups, blockToGroup,
			)
		}
		if scanErr != nil {
			return nil, scanErr
		}
		return results, nil
	}
}

// scanBlocks iterates over selectedBlocks in order, lazily fetching coalesced groups,
// evaluating the program predicate, and appending matched rows to results.
// Returns the number of blocks fetched from storage and any error encountered.
//
// SPEC-STREAM-2: Each group is fetched at most once (~8 MB coalesced I/O).
// SPEC-STREAM-3: fetchedBlocks is incremented at I/O time; early-stopped groups are not counted.
func scanBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	opts CollectOptions,
	selectedBlocks []int,
	groups []modules_shared.CoalescedRead,
	blockToGroup map[int]int,
	fetched map[int][]byte,
	fetchedGroupsSeen map[int]bool,
	results *[]MatchedRow,
) (int, error) {
	fetchedBlocks := 0

	for _, blockIdx := range selectedBlocks {
		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}

		// Lazy group fetch: one ~8 MB coalesced I/O per group, guarded by fetchedGroupsSeen.
		// SPEC-STREAM-3: FetchedBlocks is incremented here (at I/O time) by the number of
		// blocks in the group. Groups that are never fetched (due to early stop) are not counted.
		if !fetchedGroupsSeen[gi] {
			groupRaw, fetchErr := r.ReadGroup(groups[gi])
			if fetchErr != nil {
				return fetchedBlocks, fmt.Errorf("ReadGroup: %w", fetchErr)
			}
			for bi, raw := range groupRaw {
				fetched[bi] = raw
			}
			fetchedBlocks += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = true
		}

		raw, rawOK := fetched[blockIdx]
		if !rawOK {
			continue
		}
		// Free raw bytes immediately after parsing to avoid retaining the entire
		// coalesced group in memory for the duration of the scan.
		delete(fetched, blockIdx)

		meta := r.BlockMeta(blockIdx)
		r.ResetInternStrings()
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if parseErr != nil {
			return fetchedBlocks, fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return fetchedBlocks, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-018: Second pass — decode result columns now that we know this block has matches.
		// NOTE-028: secondPassCols is pre-computed above (searchMetaColumns ∪ wantColumns, or nil for all).
		if wantColumns != nil {
			bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if parseErr != nil {
				return fetchedBlocks, fmt.Errorf("ParseBlockFromBytes (second pass) block %d: %w", blockIdx, parseErr)
			}
		}

		// Copy the slice before sorting: ToSlice returns the backing slice of the
		// RowSet and must not be modified (it may be used for Contains binary search).
		rows := slices.Clone(rowSet.ToSlice())

		// SPEC-STREAM-5: Sort rows by per-row timestamp when TimestampColumn is set.
		var tsCol *modules_reader.Column
		if opts.TimestampColumn != "" {
			tsCol = bwb.Block.GetColumn(opts.TimestampColumn)
		}

		if stop := streamSortedRows(bwb.Block, blockIdx, rows, tsCol, opts, results); stop {
			return fetchedBlocks, nil
		}
	}

	return fetchedBlocks, nil
}

// streamSortedRows sorts rows by timestamp (when tsCol is non-nil) or reverses them
// for Backward direction, then appends matching rows to results with per-row time
// filtering and global limit enforcement.
//
// SPEC-STREAM-4: Per-row time filtering when tsCol is set and TimeRange is non-zero.
// SPEC-STREAM-5: Forward = ascending timestamp; Backward = descending; no tsCol = reverse indices.
//
// Returns true if the Limit was reached and iteration should stop.
func streamSortedRows(
	block *modules_reader.Block,
	blockIdx int,
	rows []int,
	tsCol *modules_reader.Column,
	opts CollectOptions,
	results *[]MatchedRow,
) bool {
	if tsCol != nil {
		backward := opts.Direction == queryplanner.Backward
		slices.SortFunc(rows, func(a, b int) int {
			tsA, okA := tsCol.Uint64Value(a)
			tsB, okB := tsCol.Uint64Value(b)
			switch {
			case !okA && !okB:
				return cmp.Compare(a, b)
			case !okA:
				return -1
			case !okB:
				return 1
			}
			if backward {
				return cmp.Compare(tsB, tsA)
			}
			return cmp.Compare(tsA, tsB)
		})
	} else if opts.Direction == queryplanner.Backward {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}

	for _, rowIdx := range rows {
		if tsCol != nil && (opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0) {
			if ts, tsOK := tsCol.Uint64Value(rowIdx); tsOK {
				if opts.TimeRange.MinNano > 0 && ts < opts.TimeRange.MinNano {
					continue
				}
				if opts.TimeRange.MaxNano > 0 && ts > opts.TimeRange.MaxNano {
					continue
				}
			}
		}

		*results = append(*results, MatchedRow{Block: block, BlockIdx: blockIdx, RowIdx: rowIdx})
		if opts.Limit > 0 && len(*results) >= opts.Limit {
			return true
		}
	}
	return false
}

// collectTopKFromIntrinsicRefs is the fast path for intrinsic-only queries with
// a timestamp sort (MostRecent). It uses only intrinsic column blobs to:
//  1. Get ALL refs matching the predicate (no block I/O, no per-row limit).
//  2. Build a lookup set for O(1) membership checks.
//  3. Read opts.TimestampColumn (a flat sorted-ascending uint64 column).
//  4. Scan from newest (end) or oldest (start) based on Direction, checking membership.
//  5. Collect the top opts.Limit matching refs — these are the globally top-K by timestamp.
//  6. Fetch only the blocks containing those rows for result materialization.
//
// Returns (nil, nil) to indicate an empty result (not a fall-through to block scan) when:
//   - BlockRefsFromIntrinsicTOC returns nil (predicate not evaluable or no intrinsic section)
//   - The timestamp intrinsic column is unavailable or not a flat uint64 column
//   - No matching refs survive the predicate
//
// unionSortedKeys merges two sorted []uint32 slices, deduplicating equal elements.
func unionSortedKeys(a, b []uint32) []uint32 {
	result := make([]uint32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			result = append(result, a[i])
			i++
		case a[i] > b[j]:
			result = append(result, b[j])
			j++
		default:
			result = append(result, a[i])
			i++
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// intersectSortedKeys intersects two sorted []uint32 slices, keeping only common elements.
func intersectSortedKeys(a, b []uint32) []uint32 {
	result := make([]uint32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// evalNodeMatchKeys recursively evaluates a RangeNode tree against intrinsic column blobs,
// returning sorted packed keys (blockIdx<<16 | rowIdx).
// Returns (keys, true) when evaluable; (nil, false) when not.
// OR: union all children; fail if any child is not evaluable.
// AND: intersect evaluable children; skip unevaluable (conservative over-fetch).
//
// NOTE-039: mirrors evalNodeBlockRefs but works with sorted []uint32 keys for binary search.
func evalNodeMatchKeys(r *modules_reader.Reader, node vm.RangeNode) ([]uint32, bool) {
	// Leaf node.
	if len(node.Children) == 0 {
		if node.Column == "" {
			return nil, false
		}
		refs := scanIntrinsicLeafRefs(r, node.Column, node, 0)
		if refs == nil {
			return nil, false
		}
		keys := make([]uint32, len(refs))
		for i, ref := range refs {
			keys[i] = uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
		}
		slices.Sort(keys)
		return keys, true
	}

	if node.IsOR {
		// OR: union — all children must be evaluable.
		var union []uint32
		for _, child := range node.Children {
			childKeys, ok := evalNodeMatchKeys(r, child)
			if !ok {
				return nil, false // any unevaluable child makes OR unevaluable
			}
			if union == nil {
				union = childKeys
			} else {
				union = unionSortedKeys(union, childKeys)
			}
		}
		if union == nil {
			union = []uint32{} // evaluable but empty
		}
		return union, true
	}

	// AND: intersect — skip unevaluable children (conservative over-fetch).
	var result []uint32
	hasResult := false
	for _, child := range node.Children {
		childKeys, ok := evalNodeMatchKeys(r, child)
		if !ok {
			continue // skip unevaluable — conservative
		}
		if !hasResult {
			result = childKeys
			hasResult = true
		} else {
			result = intersectSortedKeys(result, childKeys)
		}
	}
	if !hasResult {
		return nil, false // no evaluable children
	}
	return result, true
}

// buildPredicateMatchSet returns a sorted slice of packed ref keys (blockIdx<<16 | rowIdx)
// for all refs matching the intrinsic predicate. The sorted slice enables
// O(log N) binary search per timestamp ref during the streaming top-K scan.
//
// NOTE-039: now uses evalNodeMatchKeys to support OR and regex predicates.
// Returns nil if the fast path is not available (any top-level node not evaluable).
func buildPredicateMatchSet(r *modules_reader.Reader, program *vm.Program) []uint32 {
	if !r.HasIntrinsicSection() || program == nil || program.Predicates == nil {
		return nil
	}
	if len(program.Predicates.Nodes) == 0 {
		return nil
	}

	// Evaluate each top-level node (AND-combined) and intersect.
	var result []uint32
	hasResult := false
	for _, node := range program.Predicates.Nodes {
		keys, ok := evalNodeMatchKeys(r, node)
		if !ok {
			return nil // any unevaluable top-level node → fast path not applicable
		}
		if !hasResult {
			result = keys
			hasResult = true
		} else {
			result = intersectSortedKeys(result, keys)
		}
	}
	if !hasResult {
		return nil
	}
	return result
}

func collectTopKFromIntrinsicRefs(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	// Build sorted match set from predicate columns, then scan timestamp refs
	// from newest (or oldest), checking each ref via binary search.
	// This is timestamp-first: we scan in time order and check predicates per-ref,
	// stopping as soon as we have opts.Limit matches.
	matchKeys := buildPredicateMatchSet(r, program)
	if matchKeys == nil {
		return nil, errNeedBlockScan // fast path not available — caller should fall through to block scan
	}
	if len(matchKeys) == 0 {
		return nil, nil
	}

	backward := opts.Direction == queryplanner.Backward
	tsBlob, tsBlobErr := r.GetIntrinsicColumnBlob(opts.TimestampColumn)
	if tsBlobErr != nil || tsBlob == nil {
		return nil, nil //nolint:nilerr // intentional fall-through to regular path
	}

	// Block-range filter for sub-file sharding: only include refs within the shard's range.
	startBlock := opts.StartBlock
	endBlock := 0
	if opts.BlockCount > 0 {
		endBlock = startBlock + opts.BlockCount
	}

	selected := modules_shared.ScanFlatColumnRefsFiltered(tsBlob, backward, opts.Limit,
		func(ref modules_shared.BlockRef) bool {
			bi := int(ref.BlockIdx)
			// Sub-file shard filter: skip refs outside the assigned block range.
			if endBlock > 0 && (bi < startBlock || bi >= endBlock) {
				return false
			}
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			_, found := slices.BinarySearch(matchKeys, key)
			return found
		},
	)
	if selected == nil {
		return nil, nil // not a flat column or decode error — fall through
	}
	if len(selected) == 0 {
		return nil, nil // no matches after timestamp filtering — fall through
	}

	// Build results directly from intrinsic columns — no full block reads needed.
	// Look up field values for all selected refs in one pass per column.
	fieldMaps := lookupIntrinsicFields(r, selected)
	results := make([]MatchedRow, 0, len(selected))
	for i, ref := range selected {
		results = append(results, MatchedRow{
			IntrinsicFields: &intrinsicFieldsProvider{fields: fieldMaps[i]},
			BlockIdx:        int(ref.BlockIdx),
			RowIdx:          int(ref.RowIdx),
		})
	}
	return results, nil
}

// collectIntrinsicTopK is the fast path for intrinsic-only queries with a timestamp sort
// (MostRecent/Oldest) and a Limit. classifyCollect guarantees this function is only called
// when HasIntrinsicSection() is true and TimestampColumn is set.
//
// Returns (nil, nil) for valid empty-result cases (no matches, timestamp column unavailable).
// These are not fall-throughs — block scan does NOT run after this function returns.
func collectIntrinsicTopK(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	return collectTopKFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols)
}

// collectIntrinsicPlain is the fast path for intrinsic-only queries with a Limit but no
// timestamp sort. classifyCollect guarantees this function is only called when
// HasIntrinsicSection() is true and TimestampColumn is empty.
//
// Returns (nil, nil) for valid empty-result cases:
//   - refs == nil: intrinsic predicate not evaluable from intrinsic blobs — empty result
//   - len(refs) == 0: no matching refs — empty result
func collectIntrinsicPlain(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	refs := BlockRefsFromIntrinsicTOC(r, program, opts.Limit)
	if refs == nil {
		return nil, errNeedBlockScan // intrinsic predicate not evaluable — fall through to block scan
	}
	if len(refs) == 0 {
		return nil, nil // no matches — empty result
	}

	// Sort refs by BlockIdx so block traversal order is deterministic.
	slices.SortFunc(refs, func(a, b modules_shared.BlockRef) int {
		if a.BlockIdx != b.BlockIdx {
			return int(a.BlockIdx) - int(b.BlockIdx)
		}
		return int(a.RowIdx) - int(b.RowIdx)
	})

	// Filter refs to shard's block range if sub-file sharding is active.
	if opts.BlockCount > 0 {
		endBlock := opts.StartBlock + opts.BlockCount
		filtered := refs[:0]
		for _, ref := range refs {
			bi := int(ref.BlockIdx)
			if bi >= opts.StartBlock && bi < endBlock {
				filtered = append(filtered, ref)
			}
		}
		refs = filtered
		if len(refs) == 0 {
			return nil, nil
		}
	}

	// Group refs by block index.
	blockOrder := make([]int, 0, 4)
	blockRows := make(map[int][]int, 4)
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if _, seen := blockRows[bi]; !seen {
			blockOrder = append(blockOrder, bi)
		}
		blockRows[bi] = append(blockRows[bi], int(ref.RowIdx))
	}

	var results []MatchedRow

	for _, blockIdx := range blockOrder {
		rowIdxs := blockRows[blockIdx]

		// Fetch raw bytes for this block via a single-block coalesced group.
		groups := r.CoalescedGroups([]int{blockIdx})
		if len(groups) == 0 {
			continue
		}
		groupRaw, err := r.ReadGroup(groups[0])
		if err != nil {
			return nil, fmt.Errorf("collectIntrinsicPlain ReadGroup block %d: %w", blockIdx, err)
		}
		raw, ok := groupRaw[blockIdx]
		if !ok {
			continue
		}

		meta := r.BlockMeta(blockIdx)
		r.ResetInternStrings()
		bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if err != nil {
			return nil, fmt.Errorf("collectIntrinsicPlain ParseBlockFromBytes block %d: %w", blockIdx, err)
		}

		// Second pass if needed (same logic as Collect).
		if wantColumns != nil {
			bwb, err = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if err != nil {
				return nil, fmt.Errorf("collectIntrinsicPlain second pass block %d: %w", blockIdx, err)
			}
		}

		for _, rowIdx := range rowIdxs {
			results = append(results, MatchedRow{Block: bwb.Block, BlockIdx: blockIdx, RowIdx: rowIdx})
			if opts.Limit > 0 && len(results) >= opts.Limit {
				return results, nil
			}
		}
	}

	return results, nil
}

// collectBlockTopK implements the heap-based scan for timestamp-sorted queries with a Limit.
// It guarantees globally correct top-K results by scanning all selected blocks.
// Returns (fetchedBlocks, results, error).
//
// NOTE-028: topK second pass decodes all columns (nil), not just secondPassCols,
// because the block is stored in the heap and fields must be accessible on delivery.
func collectBlockTopK(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	opts CollectOptions,
	plan *queryplanner.Plan,
	groups []modules_shared.CoalescedRead,
	blockToGroup map[int]int,
) (fetchedBlocks int, results []MatchedRow, err error) {
	backward := opts.Direction == queryplanner.Backward
	buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}

	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)
	fetchCount := 0

	for _, blockIdx := range plan.SelectedBlocks {
		meta := r.BlockMeta(blockIdx)
		if topKSkipBlock(buf, opts.Limit, backward, meta) {
			delete(fetched, blockIdx) // release bytes if already fetched in this group
			continue
		}

		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}
		if !fetchedGroupsSeen[gi] {
			groupRaw, fetchErr := r.ReadGroup(groups[gi])
			if fetchErr != nil {
				return fetchCount, nil, fmt.Errorf("ReadGroup: %w", fetchErr)
			}
			for bi, raw := range groupRaw {
				fetched[bi] = raw
			}
			fetchCount += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = true
		}

		raw, rawOK := fetched[blockIdx]
		if !rawOK {
			continue
		}
		delete(fetched, blockIdx)

		r.ResetInternStrings()
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if parseErr != nil {
			return fetchCount, nil, fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return fetchCount, nil, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-028: second pass uses nil (all columns) not secondPassCols; block is stored
		// in heap and columns must be available at delivery time.
		if wantColumns != nil {
			bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, nil, meta)
			if parseErr != nil {
				return fetchCount, nil, fmt.Errorf("ParseBlockFromBytes (full) block %d: %w", blockIdx, parseErr)
			}
		}

		tsCol := bwb.Block.GetColumn(opts.TimestampColumn)
		if tsCol == nil {
			continue
		}
		topKScanRows(buf, opts.Limit, backward, bwb.Block, blockIdx, tsCol, opts.TimeRange, rowSet.ToSlice())
	}

	return fetchCount, topKDeliver(buf, backward), nil
}

// collectBlockPlain implements the default sequential scan over selected blocks.
// It allocates the fetch tracking maps and calls scanBlocks.
// Returns (fetchedBlocks, results, error).
func collectBlockPlain(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	opts CollectOptions,
	plan *queryplanner.Plan,
	groups []modules_shared.CoalescedRead,
	blockToGroup map[int]int,
) (fetchedBlocks int, results []MatchedRow, err error) {
	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)

	var scanErr error
	fetchedBlocks, scanErr = scanBlocks(
		r, program, wantColumns, secondPassCols, opts,
		plan.SelectedBlocks, groups, blockToGroup,
		fetched, fetchedGroupsSeen,
		&results,
	)
	if scanErr != nil {
		return fetchedBlocks, nil, scanErr
	}
	return fetchedBlocks, results, nil
}

// intrinsicFieldsProvider implements SpanFieldsProvider by looking up field values
// from intrinsic columns. Used by the intrinsic fast path to avoid reading full blocks.
type intrinsicFieldsProvider struct {
	fields map[string]any
}

// lookupIntrinsicFields resolves field values for a small set of refs from intrinsic columns.
// For each ref, it scans each intrinsic column to find the value. This is efficient for
// small ref sets (limit=20) since dict columns have few entries (3-50 values) and
// flat column refs are sorted (binary searchable).
func lookupIntrinsicFields(r *modules_reader.Reader, selected []modules_shared.BlockRef) []map[string]any {
	// Build a set of target keys for quick matching.
	targetKeys := make(map[uint32]int, len(selected)) // packed key → index in selected
	for i, ref := range selected {
		targetKeys[uint32(ref.BlockIdx)<<16|uint32(ref.RowIdx)] = i
	}

	result := make([]map[string]any, len(selected))
	for i := range result {
		result[i] = make(map[string]any, 12)
	}

	for _, colName := range r.IntrinsicColumnNames() {
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			continue
		}
		switch col.Format {
		case modules_shared.IntrinsicFormatDict:
			// Dict columns: iterate entries, for each entry scan its refs against targets.
			// With 3-50 entries this is fast even with large ref arrays, because we use
			// the target map for O(1) lookup per ref.
			for _, entry := range col.DictEntries {
				var val any
				if entry.Value != "" {
					val = entry.Value
				} else {
					val = entry.Int64Val
				}
				for _, ref := range entry.BlockRefs {
					key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
					if idx, ok := targetKeys[key]; ok {
						result[idx][colName] = val
					}
				}
			}
		case modules_shared.IntrinsicFormatFlat:
			// Flat columns: scan refs with target map lookup.
			for i, ref := range col.BlockRefs {
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				if idx, ok := targetKeys[key]; ok {
					if len(col.Uint64Values) > i {
						result[idx][colName] = col.Uint64Values[i]
					} else if len(col.BytesValues) > i {
						result[idx][colName] = col.BytesValues[i]
					}
				}
			}
		}
	}
	return result
}

func (p *intrinsicFieldsProvider) GetField(name string) (any, bool) {
	v, ok := p.fields[name]
	return v, ok
}

func (p *intrinsicFieldsProvider) IterateFields(fn func(name string, value any) bool) {
	for k, v := range p.fields {
		if !fn(k, v) {
			return
		}
	}
}
