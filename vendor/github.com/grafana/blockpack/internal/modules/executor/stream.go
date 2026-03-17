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

// errNeedBlockScan is returned by collectFromIntrinsicRefs when the intrinsic pre-filter
// is not applicable and the caller should fall through to a full block scan.
// NOTE-038: sentinel error for fallback from intrinsic pre-filter to block scan.
var errNeedBlockScan = errors.New("intrinsic fast path not applicable")

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

	// Intrinsic pre-filter fast path: for queries with at least one intrinsic predicate
	// (resource.service.name, span:duration, span:kind, etc.) and a limit, read only the
	// intrinsic column section to get candidate (blockIdx, rowIdx) pairs, then fetch only
	// the minimal set of blocks needed.
	//
	// For pure intrinsic + sorted (Case B): zero block reads — candidate rows returned
	// directly from intrinsic column data via timestamp scan.
	// For pure intrinsic + unsorted (Case A): reads only candidate blocks (not all blocks).
	// For mixed queries (Cases C/D): candidate blocks read, VM ColumnPredicate re-evaluates.
	//
	// NOTE-038: 4-case dispatch inside collectFromIntrinsicRefs based on
	// (ProgramIsIntrinsicOnly × opts.TimestampColumn != "").
	// errNeedBlockScan signals the pre-filter is not applicable; fall through to full scan.
	if opts.Limit > 0 && hasSomeIntrinsicPredicates(program) {
		rows, err := collectFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols)
		if err != errNeedBlockScan {
			if rows != nil || err != nil {
				return rows, err
			}
		}
	}

	plan := planBlocks(r, program, opts.TimeRange, queryplanner.PlanOptions{
		Direction: opts.Direction,
		Limit:     opts.Limit,
	})

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

	var scanErr error
	fetchedBlocks, scanErr = scanBlocks(
		r, program, wantColumns, secondPassCols, opts,
		plan.SelectedBlocks, groups, blockToGroup,
		fetched, fetchedGroupsSeen,
		&results,
	)
	if scanErr != nil {
		return nil, scanErr
	}

	return results, nil
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

// countUniqueBlockIdxs returns the number of distinct BlockIdx values in refs.
func countUniqueBlockIdxs(refs []modules_shared.BlockRef) int {
	if len(refs) == 0 {
		return 0
	}
	seen := make(map[uint16]struct{}, 16)
	for _, ref := range refs {
		seen[ref.BlockIdx] = struct{}{}
	}
	return len(seen)
}

// collectFromIntrinsicRefs is the unified intrinsic pre-filter fast path.
// It dispatches based on (ProgramIsIntrinsicOnly × opts.TimestampColumn != ""):
//
//	Case A: pure intrinsic + no sort  → BlockRefsFromIntrinsicTOC → fetch candidate
//	        blocks → return MatchedRows (minimal block reads)
//	Case B: pure intrinsic + sort     → BlockRefsFromIntrinsicTOC → pack into sorted
//	        keys → ScanFlatColumnRefsFiltered → return IntrinsicFields MatchedRows
//	        (ZERO block reads)
//	Case C: mixed + no sort           → blockRefsFromIntrinsicPartial → fetch candidate
//	        blocks → ColumnPredicate re-eval → intersect → collect up to limit
//	Case D: mixed + sort              → blockRefsFromIntrinsicPartial → fetch candidate
//	        blocks → ColumnPredicate re-eval → topKScanRows → topKDeliver
//
// Returns (nil, errNeedBlockScan) when no intrinsic constraint is available (fall through
// to full block scan). Returns (nil, nil) for valid empty-result cases.
//
// NOTE-038: The partial-AND pre-filter for mixed queries is a superset; ColumnPredicate
// re-evaluation in Cases C/D provides correctness. Global top-K is preserved for Case D
// because the pre-filter never excludes true matches (it is a superset, never a subset).
func collectFromIntrinsicRefs(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	isPureIntrinsic := ProgramIsIntrinsicOnly(program)
	hasSort := opts.TimestampColumn != ""

	// Step 1: Get candidate refs using strict (pure intrinsic) or partial (mixed) eval.
	// Case B (pure intrinsic + sort) uses limit=0 to get ALL matching refs — the
	// timestamp scan needs the complete match set or it misses the newest spans
	// (BlockRefsFromIntrinsicTOC returns refs in block order, not timestamp order;
	// if we truncate at K refs we get the oldest K, and scanning backward finds 0).
	var refs []modules_shared.BlockRef
	if isPureIntrinsic {
		refLimit := opts.Limit
		if hasSort {
			// Case B needs ALL matching refs for the timestamp scan — if refs are
			// truncated to K, the oldest K are returned (block-order, not timestamp-
			// order) and the backward scan finds 0 of the newest K results.
			// Use limit=0 (unlimited) so scanIntrinsicLeafRefs appends dynamically
			// instead of pre-allocating a large slice via make([]BlockRef, 0, overFetch).
			refLimit = 0
		}
		refs = BlockRefsFromIntrinsicTOC(r, program, refLimit)
	} else {
		refs = blockRefsFromIntrinsicPartial(r, program, opts.Limit)
	}
	if refs == nil {
		return nil, errNeedBlockScan
	}
	if len(refs) == 0 {
		return nil, nil
	}

	// Selectivity guard: if the partial pre-filter covers more than half the internal
	// blocks it offers no I/O benefit for Cases C/D, which must read blocks for VM eval.
	// Fall through to the regular block scan (coalesced I/O, planBlocks pruning).
	if !isPureIntrinsic {
		uniqueBlocks := countUniqueBlockIdxs(refs)
		if uniqueBlocks*2 > r.BlockCount() {
			return nil, errNeedBlockScan
		}
	}

	// Step 2: Dispatch based on (isPureIntrinsic, hasSort).
	if isPureIntrinsic && !hasSort {
		return collectIntrinsicPlain(r, refs, opts, wantColumns, secondPassCols)
	}
	if isPureIntrinsic && hasSort {
		return collectIntrinsicTopK(r, refs, opts, secondPassCols)
	}
	if !hasSort {
		// Case C: mixed + no sort
		return collectMixedPlain(r, program, refs, opts, wantColumns, secondPassCols)
	}
	// Case D: mixed + sort
	return collectMixedTopK(r, program, refs, opts, wantColumns, secondPassCols)
}

// collectIntrinsicPlain handles Case A: pure intrinsic + no sort.
// Refs are exact matches from intrinsic data; fetch the candidate blocks and return rows.
func collectIntrinsicPlain(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	// Sort refs by BlockIdx for deterministic block traversal order.
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

// sortScanThreshold is the max number of matching refs for which the map-then-sort path
// is used. Below this threshold: build a packed-key→timestamp map from tsCol (O(N)),
// look up each of the M refs in O(1), sort M pairs (O(M log M)). Above this threshold:
// use ScanFlatColumnRefsFiltered — cheaper when M is large relative to N.
// Crossover: O(N) map build vs O(N) scan — the map path wins by avoiding repeated blob
// decompression; for M >= 8K the scan path is preferred as it stops early after K results.
const sortScanThreshold = 8000

// collectIntrinsicTopK handles Case B: pure intrinsic + timestamp sort.
// Returns IntrinsicFields MatchedRows with ZERO full block reads.
//
// For small ref sets (M < sortScanThreshold): decodes the timestamp intrinsic column,
// builds a packed-key→timestamp map (O(N)), looks up each matching ref's timestamp in
// O(1), sorts M pairs by timestamp, takes top K. Avoids re-decompression since the
// decoded column is cached.
//
// For large ref sets (M >= sortScanThreshold): packs refs into a sorted key set
// and runs ScanFlatColumnRefsFiltered backward, stopping after K results.
// O(K/rate × log M) — ideal for common predicates where rate is high.
func collectIntrinsicTopK(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	backward := opts.Direction == queryplanner.Backward
	limit := opts.Limit

	var selected []modules_shared.BlockRef

	if len(refs) < sortScanThreshold {
		// Map-then-sort path: O(N) build + O(M log M) sort.
		// tsCol.BlockRefs is stored in ascending timestamp order (NOT packed-key order).
		// Build a packed-key → timestamp map from the full column, then look up each
		// matching ref's timestamp in O(1), sort, and take top K.
		// NOTE-042: replaces O(N) scan of the blob for the small-M sort path.
		tsCol, tsErr := r.GetIntrinsicColumn(opts.TimestampColumn)
		if tsErr != nil || tsCol == nil || len(tsCol.Uint64Values) < len(tsCol.BlockRefs) {
			return nil, errNeedBlockScan
		}
		// Build packed-key → timestamp map from the full column (O(N)).
		// Packing invariant: BlockRef fields are uint16, so BlockIdx occupies bits 31-16
		// and RowIdx occupies bits 15-0 — no collision is possible.
		tsMap := make(map[uint32]uint64, len(tsCol.BlockRefs))
		for i, br := range tsCol.BlockRefs {
			tsMap[uint32(br.BlockIdx)<<16|uint32(br.RowIdx)] = tsCol.Uint64Values[i]
		}
		type refTS struct {
			ref modules_shared.BlockRef
			ts  uint64
		}
		pairs := make([]refTS, 0, len(refs))
		for _, ref := range refs {
			bi := int(ref.BlockIdx)
			if opts.BlockCount > 0 && (bi < opts.StartBlock || bi >= opts.StartBlock+opts.BlockCount) {
				continue
			}
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			if ts, ok := tsMap[key]; ok {
				pairs = append(pairs, refTS{ref: ref, ts: ts})
			}
		}
		if len(pairs) == 0 {
			return nil, nil
		}
		// Sort by timestamp: descending for backward (MostRecent), ascending for forward.
		if backward {
			slices.SortFunc(pairs, func(a, b refTS) int { return cmp.Compare(b.ts, a.ts) })
		} else {
			slices.SortFunc(pairs, func(a, b refTS) int { return cmp.Compare(a.ts, b.ts) })
		}
		if limit > 0 && len(pairs) > limit {
			pairs = pairs[:limit]
		}
		selected = make([]modules_shared.BlockRef, len(pairs))
		for i, p := range pairs {
			selected[i] = p.ref
		}
	} else {
		// Scan path: pack matchKeys, scan timestamp blob from newest/oldest end.
		// Used for large M (>= sortScanThreshold).
		matchKeys := make([]uint32, len(refs))
		for i, ref := range refs {
			matchKeys[i] = uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
		}
		slices.Sort(matchKeys)

		tsBlob, tsBlobErr := r.GetIntrinsicColumnBlob(opts.TimestampColumn)
		if tsBlobErr != nil || tsBlob == nil {
			return nil, errNeedBlockScan
		}

		startBlock := opts.StartBlock
		endBlock := 0
		if opts.BlockCount > 0 {
			endBlock = startBlock + opts.BlockCount
		}
		selected = modules_shared.ScanFlatColumnRefsFiltered(tsBlob, backward, limit,
			func(ref modules_shared.BlockRef) bool {
				bi := int(ref.BlockIdx)
				if endBlock > 0 && (bi < startBlock || bi >= endBlock) {
					return false
				}
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				_, found := slices.BinarySearch(matchKeys, key)
				return found
			},
		)
	}

	if len(selected) == 0 {
		return nil, nil
	}

	fieldMaps := lookupIntrinsicFields(r, selected, secondPassCols)
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

// collectMixedPlain handles Case C: mixed predicate + no sort.
// The refs from blockRefsFromIntrinsicPartial are a superset (partial-AND pre-filter).
// For each candidate block, ColumnPredicate re-evaluates the full predicate to eliminate
// false positives.
func collectMixedPlain(
	r *modules_reader.Reader,
	program *vm.Program,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	// Sort refs and apply shard filter (same as collectIntrinsicPlain).
	slices.SortFunc(refs, func(a, b modules_shared.BlockRef) int {
		if a.BlockIdx != b.BlockIdx {
			return int(a.BlockIdx) - int(b.BlockIdx)
		}
		return int(a.RowIdx) - int(b.RowIdx)
	})
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

	// Group candidate row indices by block.
	blockOrder := make([]int, 0, 4)
	blockCandidates := make(map[int][]int, 4)
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if _, seen := blockCandidates[bi]; !seen {
			blockOrder = append(blockOrder, bi)
		}
		blockCandidates[bi] = append(blockCandidates[bi], int(ref.RowIdx))
	}

	var results []MatchedRow
	// Coalesce all candidate blocks for efficient batch I/O.
	groups := r.CoalescedGroups(blockOrder)
	for _, group := range groups {
		fetched, err := r.ReadGroup(group)
		if err != nil {
			return nil, fmt.Errorf("collectMixedPlain ReadGroup: %w", err)
		}
		for _, blockIdx := range group.BlockIDs {
			candidateRows := blockCandidates[blockIdx]
			raw, ok := fetched[blockIdx]
			if !ok {
				continue
			}
			meta := r.BlockMeta(blockIdx)
			r.ResetInternStrings()
			bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
			if parseErr != nil {
				return nil, fmt.Errorf("collectMixedPlain ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
			}
			if wantColumns != nil {
				bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
				if parseErr != nil {
					return nil, fmt.Errorf("collectMixedPlain second pass block %d: %w", blockIdx, parseErr)
				}
			}

			// Re-evaluate the full predicate against this block.
			provider := newBlockColumnProvider(bwb.Block)
			rowSet, evalErr := program.ColumnPredicate(provider)
			if evalErr != nil {
				return nil, fmt.Errorf("collectMixedPlain ColumnPredicate block %d: %w", blockIdx, evalErr)
			}

			// Intersect VM result with candidate rows from intrinsic pre-filter.
			for _, rowIdx := range candidateRows {
				if !rowSet.Contains(rowIdx) {
					continue // eliminated by VM re-evaluation
				}
				results = append(results, MatchedRow{Block: bwb.Block, BlockIdx: blockIdx, RowIdx: rowIdx})
				if opts.Limit > 0 && len(results) >= opts.Limit {
					return results, nil
				}
			}
		}
	}
	return results, nil
}

// collectMixedTopK handles Case D: mixed predicate + timestamp sort.
// Fetches only candidate blocks (from partial-AND pre-filter), re-evaluates the full
// predicate, runs topKScanRows on the qualifying rows, and delivers top-K by timestamp.
//
// Global top-K correctness is preserved: the partial-AND pre-filter is a superset, so
// all true matching rows (those satisfying both intrinsic and non-intrinsic conditions)
// are present among the candidate blocks. ColumnPredicate eliminates false positives.
// topKScanRows then finds the globally correct top-K timestamp order within those candidates.
func collectMixedTopK(
	r *modules_reader.Reader,
	program *vm.Program,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, error) {
	backward := opts.Direction == queryplanner.Backward

	// Apply shard filter.
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

	// Group candidate rows by block.
	blockOrder := make([]int, 0, 4)
	blockCandidates := make(map[int][]int, 4)
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if _, seen := blockCandidates[bi]; !seen {
			blockOrder = append(blockOrder, bi)
		}
		blockCandidates[bi] = append(blockCandidates[bi], int(ref.RowIdx))
	}

	buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}

	// Coalesce all candidate blocks for efficient batch I/O.
	groups := r.CoalescedGroups(blockOrder)
	for _, group := range groups {
		fetched, err := r.ReadGroup(group)
		if err != nil {
			return nil, fmt.Errorf("collectMixedTopK ReadGroup: %w", err)
		}
		for _, blockIdx := range group.BlockIDs {
			candidateRows := blockCandidates[blockIdx]
			raw, ok := fetched[blockIdx]
			if !ok {
				continue
			}
			meta := r.BlockMeta(blockIdx)
			r.ResetInternStrings()
			bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
			if parseErr != nil {
				return nil, fmt.Errorf("collectMixedTopK ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
			}
			if wantColumns != nil {
				bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
				if parseErr != nil {
					return nil, fmt.Errorf("collectMixedTopK second pass block %d: %w", blockIdx, parseErr)
				}
			}

			// Re-evaluate the full predicate.
			provider := newBlockColumnProvider(bwb.Block)
			rowSet, evalErr := program.ColumnPredicate(provider)
			if evalErr != nil {
				return nil, fmt.Errorf("collectMixedTopK ColumnPredicate block %d: %w", blockIdx, evalErr)
			}

			// Collect rows that pass both the pre-filter and full predicate.
			var qualifying []int
			for _, rowIdx := range candidateRows {
				if rowSet.Contains(rowIdx) {
					qualifying = append(qualifying, rowIdx)
				}
			}
			if len(qualifying) == 0 {
				continue
			}

			// Get timestamp column for heap ordering.
			tsCol := bwb.Block.GetColumn(opts.TimestampColumn)
			if tsCol == nil {
				continue
			}

			topKScanRows(buf, opts.Limit, backward, bwb.Block, blockIdx, tsCol, opts.TimeRange, qualifying)
		}
	}

	return topKDeliver(buf, backward), nil
}

// intrinsicFieldsProvider implements SpanFieldsProvider by looking up field values
// from intrinsic columns. Used by the intrinsic fast path to avoid reading full blocks.
type intrinsicFieldsProvider struct {
	fields map[string]any
}

// lookupIntrinsicFields reads intrinsic column values for the given refs and returns one
// map[string]any per ref. wantCols limits which columns are loaded — when non-nil only
// columns present in wantCols are fetched (skipping expensive GetIntrinsicColumn calls
// and the mergeIntrinsicColumns allocations they trigger for unwanted columns).
// Pass nil to fetch all intrinsic columns (e.g. FindTraceByID needs every field).
func lookupIntrinsicFields(r *modules_reader.Reader, selected []modules_shared.BlockRef, wantCols map[string]struct{}) []map[string]any {
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
		// Skip columns not required by this query. wantCols == nil means "all columns"
		// (used for FindTraceByID and match-all queries that need every field).
		if wantCols != nil {
			if _, needed := wantCols[colName]; !needed {
				continue
			}
		}
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
