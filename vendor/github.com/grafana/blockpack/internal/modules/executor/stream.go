package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"fmt"
	"slices"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

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
	Block    *modules_reader.Block
	BlockIdx int
	RowIdx   int
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
	// SPEC-STREAM-1: nil reader — return nil result slice and nil error.
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

	wantColumns := ProgramWantColumns(program)

	// NOTE-028: Compute secondPassCols once — wantColumns and opts.AllColumns are loop-invariant.
	// nil means decode all columns (AllColumns=true or no column filter).
	var secondPassCols map[string]struct{}
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

	// Intrinsic fast path: for intrinsic-only queries with a limit, read only the
	// intrinsic column section (small blobs, no full block I/O) to get matching
	// (blockIdx, rowIdx) pairs, then fetch only the minimal set of blocks needed.
	// When TimestampColumn is set (MostRecent queries), the timestamp intrinsic column
	// is used to select the globally top-K rows without reading any full blocks.
	// Falls through to the regular planner path when not applicable.
	if opts.Limit > 0 && opts.BlockCount == 0 && ProgramIsIntrinsicOnly(program) {
		if rows, err := collectFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols); rows != nil || err != nil {
			return rows, err
		}
	}

	planner := queryplanner.NewPlanner(r)
	predicates := buildPredicates(r, program)
	plan := planner.PlanWithOptions(predicates, opts.TimeRange, queryplanner.PlanOptions{
		Direction: opts.Direction,
		Limit:     opts.Limit,
	})

	// Intrinsic-column pruning: use the file-level intrinsic column index to eliminate
	// blocks whose column values cannot possibly satisfy the query predicates.
	// This runs after the planner so we intersect with, not replace, the planner's selection.
	// Returns nil when no pruning is possible (e.g. no intrinsic section, no intrinsic
	// predicates, or all blocks survive), in which case we skip the intersection step.
	if intrinsicBlocks := BlocksFromIntrinsicTOC(r, program); intrinsicBlocks != nil {
		// Build a keep-set from the intrinsic result for O(1) membership tests.
		keepSet := make(map[int]struct{}, len(intrinsicBlocks))
		for _, bi := range intrinsicBlocks {
			keepSet[bi] = struct{}{}
		}
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if _, ok := keepSet[bi]; ok {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
	}

	// Sub-file sharding: if the caller specified a block range, filter the planner's
	// selected blocks to only include indices within [StartBlock, StartBlock+BlockCount).
	// This lets the frontend sharder split a single blockpack file across multiple jobs.
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

	// SPEC-STREAM-2: Partition selected blocks into ~8 MB coalesced groups for lazy batched I/O.
	groups := r.CoalescedGroups(plan.SelectedBlocks)

	// Map blockIdx -> group index for lazy fetching.
	blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			blockToGroup[bi] = gi
		}
	}

	// Heap-based scan for timestamp-sorted queries (MostRecent/Oldest with a limit).
	// Guarantees globally correct top-K by scanning all blocks and maintaining a priority
	// queue. The intrinsic fast path (above) already handles intrinsic-only queries without
	// full block I/O; this path handles all other timestamp-sorted queries.
	if opts.TimestampColumn != "" && opts.Limit > 0 {
		backward := opts.Direction == queryplanner.Backward
		buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}
		fc, scanErr := topKScanBlocks(r, program, wantColumns, opts, plan, buf, groups, blockToGroup, backward)
		fetchedBlocks = fc
		if scanErr != nil {
			return nil, scanErr
		}
		return topKDeliver(buf, backward), nil
	}

	var results []MatchedRow

	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)

	for _, blockIdx := range plan.SelectedBlocks {
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
				return nil, fmt.Errorf("ReadGroup: %w", fetchErr)
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
			return nil, fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return nil, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-018: Second pass — decode result columns now that we know this block has matches.
		// NOTE-028: secondPassCols is pre-computed above (searchMetaColumns ∪ wantColumns, or nil for all).
		if wantColumns != nil {
			bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if parseErr != nil {
				return nil, fmt.Errorf("ParseBlockFromBytes (second pass) block %d: %w", blockIdx, parseErr)
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

		if stop := streamSortedRows(bwb.Block, blockIdx, rows, tsCol, opts, &results); stop {
			return results, nil
		}
	}

	return results, nil
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

// collectFromIntrinsicRefs is the fast path for intrinsic-only queries with a limit.
// It reads only the tiny intrinsic column blobs to get matching BlockRefs, then
// fetches only the blocks containing those refs — skipping all unneeded block I/O.
//
// When opts.TimestampColumn is set (MostRecent queries), delegates to
// collectTopKFromIntrinsicRefs which selects the globally top-K rows by timestamp
// using only intrinsic column data — no full block reads for predicate evaluation.
//
// Returns (nil, nil) to signal the caller to fall through to the regular Collect path when:
//   - The file has no intrinsic section
//   - BlockRefsFromIntrinsicTOC returns nil (predicate not evaluable from intrinsic data)
//   - No matching refs are found (caller should still run regular path to be safe)
func collectFromIntrinsicRefs(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	if opts.TimestampColumn != "" {
		return collectTopKFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols)
	}
	refs := BlockRefsFromIntrinsicTOC(r, program, opts.Limit)
	if refs == nil {
		return nil, nil // fast path not available — fall through
	}
	if len(refs) == 0 {
		return nil, nil // no matches: fall through so regular path can confirm
	}

	// Group refs by block index.
	type blockEntry struct {
		blockIdx int
		rowIdxs  []int
	}
	// Maintain insertion order so we respect block order.
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
			return nil, fmt.Errorf("collectFromIntrinsicRefs ReadGroup block %d: %w", blockIdx, err)
		}
		raw, ok := groupRaw[blockIdx]
		if !ok {
			continue
		}

		meta := r.BlockMeta(blockIdx)
		r.ResetInternStrings()
		bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if err != nil {
			return nil, fmt.Errorf("collectFromIntrinsicRefs ParseBlockFromBytes block %d: %w", blockIdx, err)
		}

		// Second pass if needed (same logic as Collect).
		if wantColumns != nil {
			bwb, err = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if err != nil {
				return nil, fmt.Errorf("collectFromIntrinsicRefs second pass block %d: %w", blockIdx, err)
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

// collectTopKFromIntrinsicRefs is the fast path for intrinsic-only queries with
// a timestamp sort (MostRecent). It uses only intrinsic column blobs to:
//  1. Get ALL refs matching the predicate (no block I/O, no per-row limit).
//  2. Build a lookup set for O(1) membership checks.
//  3. Read opts.TimestampColumn (a flat sorted-ascending uint64 column).
//  4. Scan from newest (end) or oldest (start) based on Direction, checking membership.
//  5. Collect the top opts.Limit matching refs — these are the globally top-K by timestamp.
//  6. Fetch only the blocks containing those rows for result materialization.
//
// Returns (nil, nil) to fall through to the regular Collect path when:
//   - BlockRefsFromIntrinsicTOC returns nil (predicate not evaluable or no intrinsic section)
//   - The timestamp intrinsic column is unavailable or not a flat uint64 column
//   - No matching refs survive the predicate
func collectTopKFromIntrinsicRefs(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	// limit=0 → all refs (no per-row cutoff); top-K selection happens via tsCol scan.
	allRefs := BlockRefsFromIntrinsicTOC(r, program, 0)
	if allRefs == nil {
		return nil, nil // fast path not available
	}
	if len(allRefs) == 0 {
		return nil, nil // no matches: fall through so regular path can confirm
	}

	// Build lookup set for O(1) membership checks.
	type refKey struct{ blockIdx, rowIdx uint16 }
	refSet := make(map[refKey]struct{}, len(allRefs))
	for _, ref := range allRefs {
		refSet[refKey{ref.BlockIdx, ref.RowIdx}] = struct{}{}
	}

	// Scan timestamp column refs directly from raw bytes — skips value decode entirely.
	// The sorted order of refs mirrors the sorted timestamp values (parallel arrays).
	backward := opts.Direction == queryplanner.Backward
	tsBlob, tsBlobErr := r.GetIntrinsicColumnBlob(opts.TimestampColumn)
	if tsBlobErr != nil || tsBlob == nil {
		return nil, nil // no timestamp intrinsic column — fall through
	}

	selected := modules_shared.ScanFlatColumnRefsFiltered(tsBlob, backward, opts.Limit,
		func(ref modules_shared.BlockRef) bool {
			_, ok := refSet[refKey{ref.BlockIdx, ref.RowIdx}]
			return ok
		},
	)
	if selected == nil {
		return nil, nil // not a flat column or decode error — fall through
	}
	if len(selected) == 0 {
		return nil, nil // no matches after timestamp filtering — fall through
	}

	// Collect the unique block indices needed (preserving selected order for later).
	uniqueBlocks := make([]int, 0, 4)
	seenBlocks := make(map[int]struct{}, 4)
	for _, ref := range selected {
		bi := int(ref.BlockIdx)
		if _, seen := seenBlocks[bi]; !seen {
			seenBlocks[bi] = struct{}{}
			uniqueBlocks = append(uniqueBlocks, bi)
		}
	}

	// Fetch and parse each needed block once.
	parsedBlocks := make(map[int]*modules_reader.Block, len(uniqueBlocks))
	for _, blockIdx := range uniqueBlocks {
		groups := r.CoalescedGroups([]int{blockIdx})
		if len(groups) == 0 {
			continue
		}
		groupRaw, fetchErr := r.ReadGroup(groups[0])
		if fetchErr != nil {
			return nil, fmt.Errorf("collectTopKFromIntrinsicRefs ReadGroup block %d: %w", blockIdx, fetchErr)
		}
		raw, ok := groupRaw[blockIdx]
		if !ok {
			continue
		}
		meta := r.BlockMeta(blockIdx)
		r.ResetInternStrings()
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("collectTopKFromIntrinsicRefs ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}
		if wantColumns != nil {
			bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if parseErr != nil {
				return nil, fmt.Errorf("collectTopKFromIntrinsicRefs second pass block %d: %w", blockIdx, parseErr)
			}
		}
		parsedBlocks[blockIdx] = bwb.Block
	}

	// Build results in selected order (preserves timestamp ordering from the scan).
	results := make([]MatchedRow, 0, len(selected))
	for _, ref := range selected {
		bi := int(ref.BlockIdx)
		block, ok := parsedBlocks[bi]
		if !ok {
			continue
		}
		results = append(results, MatchedRow{Block: block, BlockIdx: bi, RowIdx: int(ref.RowIdx)})
	}
	return results, nil
}
