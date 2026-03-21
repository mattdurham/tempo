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

// errLimitReached is used as an early-stop sentinel inside forEachBlockInGroups callbacks.
// It signals that the results limit has been satisfied; it is never returned to callers.
var errLimitReached = errors.New("limit reached")

// SortScanThreshold is the max number of matching refs for which the KLL path
// is used in collectIntrinsicTopK (Case B). Below this threshold: group refs by block,
// sort blocks by BlockMeta.MaxStart DESC (newest first), build a packed-key→timestamp
// map from tsCol (O(N)), look up each of the M refs in O(1), sort M pairs (O(M log M)).
// Above this threshold: use ScanFlatColumnRefsFiltered with a hash-set filter — avoids
// materializing the full decoded timestamp column for large M. Both paths are O(N); the
// scan path is preferred for large M because it skips the decoded-column allocation.
// Exported so tests can override it to force the scan path. See NOTE-043.
var SortScanThreshold = 8000

// blockRefCompare orders BlockRefs by (BlockIdx, RowIdx) ascending.
func blockRefCompare(a, b modules_shared.BlockRef) int {
	if n := cmp.Compare(a.BlockIdx, b.BlockIdx); n != 0 {
		return n
	}
	return cmp.Compare(a.RowIdx, b.RowIdx)
}

// filterRefsByShardRange filters refs to those within [opts.StartBlock, opts.StartBlock+opts.BlockCount).
// Returns refs unchanged when BlockCount is 0 (no sharding active).
func filterRefsByShardRange(refs []modules_shared.BlockRef, opts CollectOptions) []modules_shared.BlockRef {
	if opts.BlockCount == 0 {
		return refs
	}
	endBlock := opts.StartBlock + opts.BlockCount
	filtered := refs[:0]
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if bi >= opts.StartBlock && bi < endBlock {
			filtered = append(filtered, ref)
		}
	}
	return filtered
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
	Explain string
	// ExecutionPath identifies which code path ran for this query. One of:
	// "intrinsic-plain" (Case A), "intrinsic-topk-kll" (Case B KLL path),
	// "intrinsic-topk-scan" (Case B scan path), "mixed-plain" (Case C),
	// "mixed-topk" (Case D), "block-plain" (block-scan no sort),
	// "block-topk" (block-scan with topK heap),
	// "intrinsic-need-block-scan" (fast path fell through to block scan).
	// NOTE-043, NOTE-044: see also SortScanThreshold.
	ExecutionPath  string
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
	// IntrinsicRefCount is the number of matching refs from BlockRefsFromIntrinsicTOC
	// (M for Case B). Set for intrinsic-topk-sort and intrinsic-topk-scan. Zero otherwise.
	IntrinsicRefCount int
	// IntrinsicScanCount is the number of entries visited by ScanFlatColumnRefsFiltered
	// in the Case B scan path. Zero for the map path and all other paths.
	IntrinsicScanCount int
	// MixedCandidateBlocks is the number of unique candidate blocks from the partial-AND
	// pre-filter for Cases C and D (mixed queries). Zero for all other paths.
	MixedCandidateBlocks int
}

// MatchedRow holds a single row result from Collect.
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
func Collect(
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
		searchCols := searchMetaCols
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
		var fastStats CollectStats
		rows, err := collectFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols,
			&fastStats)
		if err != errNeedBlockScan {
			// Fast path produced a definitive result (rows, empty result, or error).
			// Call OnStats synchronously before returning. SPEC-STREAM-6 extended.
			if opts.OnStats != nil {
				opts.OnStats(fastStats)
			}
			return rows, err
		}
		// errNeedBlockScan: fall through to full block scan. fastStats is discarded.
		// The block-scan defer below will call OnStats with the block-scan stats.
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
	// blockScanPath is set just before the scan branch executes (see NOTE-043).
	fetchedBlocks := 0
	blockScanPath := ""
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
				ExecutionPath:  blockScanPath,
			})
		}()
	}

	if len(plan.SelectedBlocks) == 0 {
		blockScanPath = "block-pruned"
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
		blockScanPath = "block-topk"
		backward := opts.Direction == queryplanner.Backward
		buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}
		fc, scanErr := topKScanBlocks(r, program, wantColumns, opts, plan, buf, groups, blockToGroup, backward)
		fetchedBlocks = fc
		if scanErr != nil {
			return nil, scanErr
		}
		return topKDeliver(buf, backward), nil
	}

	blockScanPath = "block-plain"
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
	stats *CollectStats,
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
		// Cases C/D: pass limit=0 (unlimited) so the partial pre-filter returns a true
		// superset. Passing opts.Limit would truncate the candidate set in block order,
		// turning a superset into a subset — the VM re-eval in collectMixedPlain /
		// collectMixedTopK cannot recover false negatives (true matches silently dropped).
		refs = blockRefsFromIntrinsicPartial(r, program, 0)
	}
	if refs == nil {
		stats.ExecutionPath = "intrinsic-need-block-scan"
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
			stats.ExecutionPath = "intrinsic-need-block-scan"
			return nil, errNeedBlockScan
		}
	}

	// Step 2: Dispatch based on (isPureIntrinsic, hasSort).
	if isPureIntrinsic && !hasSort {
		return collectIntrinsicPlain(r, refs, opts, wantColumns, secondPassCols, stats)
	}
	if isPureIntrinsic && hasSort {
		return collectIntrinsicTopK(r, refs, opts, secondPassCols, stats)
	}
	if !hasSort {
		// Case C: mixed + no sort
		return collectMixedPlain(r, program, refs, opts, wantColumns, secondPassCols, stats)
	}
	// Case D: mixed + sort
	return collectMixedTopK(r, program, refs, opts, wantColumns, secondPassCols, stats)
}

// parsedBlock holds the result of the two-pass parse for one block.
type parsedBlock struct {
	Block    *modules_reader.Block
	BlockIdx int
}

// groupRefsByBlock converts a slice of BlockRefs into a stable block traversal order
// and a map from block index to row indices. The order preserves first-seen block order.
func groupRefsByBlock(refs []modules_shared.BlockRef) (blockOrder []int, blockRows map[int][]int) {
	blockOrder = make([]int, 0, 4)
	blockRows = make(map[int][]int, 4)
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if _, seen := blockRows[bi]; !seen {
			blockOrder = append(blockOrder, bi)
		}
		blockRows[bi] = append(blockRows[bi], int(ref.RowIdx))
	}
	return blockOrder, blockRows
}

// forEachBlockInGroups iterates over coalesced block groups, performing the standard
// two-pass fetch+parse for each block (first pass: wantColumns; second pass: secondPassCols),
// and invokes fn for each successfully parsed block with its candidate row indices.
// If fn returns a non-nil error, iteration stops and that error is returned.
// callerName is used only for error context strings.
func forEachBlockInGroups(
	r *modules_reader.Reader,
	blockOrder []int,
	blockCandidates map[int][]int,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	callerName string,
	fn func(pb parsedBlock, candidateRows []int) error,
) error {
	groups := r.CoalescedGroups(blockOrder)
	for _, group := range groups {
		fetched, err := r.ReadGroup(group)
		if err != nil {
			return fmt.Errorf("%s ReadGroup: %w", callerName, err)
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
				return fmt.Errorf("%s ParseBlockFromBytes block %d: %w", callerName, blockIdx, parseErr)
			}
			if wantColumns != nil {
				bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
				if parseErr != nil {
					return fmt.Errorf("%s second pass block %d: %w", callerName, blockIdx, parseErr)
				}
			}
			if err := fn(parsedBlock{Block: bwb.Block, BlockIdx: blockIdx}, candidateRows); err != nil {
				return err
			}
		}
	}
	return nil
}

// collectIntrinsicPlain handles Case A: pure intrinsic + no sort.
// Groups refs by internal blockIdx and calls forEachBlockInGroups, which reads
// only the specific internal blocks (typically 1-3, ~100-500KB each) containing
// the matched refs. This is cheaper than scanning the full intrinsic section
// (10-50MB) for all N spans when the result limit is small.
func collectIntrinsicPlain(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	stats *CollectStats,
) ([]MatchedRow, error) {
	stats.ExecutionPath = "intrinsic-plain"
	// Sort refs by (BlockIdx, RowIdx) for deterministic traversal order.
	slices.SortFunc(refs, blockRefCompare)

	// Filter refs to shard's block range if sub-file sharding is active.
	refs = filterRefsByShardRange(refs, opts)
	if len(refs) == 0 {
		return nil, nil
	}

	// Apply limit: truncate refs before field lookup to avoid unnecessary work.
	if opts.Limit > 0 && len(refs) > opts.Limit {
		refs = refs[:opts.Limit]
	}

	// Group refs by internal block index using the shared helper.
	// M=20 refs typically span 1-3 internal blocks (~100-500KB each), far cheaper
	// than scanning the full intrinsic section (all N spans in the file).
	blockOrder, blockCandidates := groupRefsByBlock(refs)

	var results []MatchedRow
	err := forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectIntrinsicPlain",
		func(pb parsedBlock, candidateRows []int) error {
			for _, rowIdx := range candidateRows {
				results = append(results, MatchedRow{
					Block:    pb.Block,
					BlockIdx: pb.BlockIdx,
					RowIdx:   rowIdx,
				})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// collectIntrinsicTopK handles Case B: pure intrinsic + timestamp sort.
// Returns IntrinsicFields MatchedRows with ZERO full block reads.
//
// For small ref sets (M < SortScanThreshold): KLL path — groups refs by BlockIdx, orders
// blocks by BlockMeta.MaxStart DESC (newest block first via KLL sketch metadata), builds
// a packed-key→timestamp map from tsCol (O(N)), looks up each matching ref's timestamp in
// O(1), sorts M pairs by timestamp, takes top K. Avoids re-decompression since the decoded
// column is cached. NOTE-044: block-level ordering via MaxStart.
//
// For large ref sets (M >= SortScanThreshold): packs refs into a sorted key set
// and runs ScanFlatColumnRefsFiltered backward, stopping after K results.
// O(K/rate × log M) — ideal for common predicates where rate is high.
func collectIntrinsicTopK(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	secondPassCols map[string]struct{},
	stats *CollectStats,
) ([]MatchedRow, error) {
	var (
		selected []modules_shared.BlockRef
		err      error
	)
	if len(refs) < SortScanThreshold {
		selected, err = collectIntrinsicTopKKLL(r, refs, opts, stats)
	} else {
		selected, err = collectIntrinsicTopKScan(r, refs, opts, stats)
	}
	if err != nil {
		return nil, err
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

// collectIntrinsicTopKKLL handles the KLL path of collectIntrinsicTopK: M < SortScanThreshold.
// Groups refs by block, orders blocks by BlockMeta.MaxStart DESC, builds a packed-key→timestamp
// map from tsCol in O(N), looks up each ref's timestamp in O(1), applies time filter,
// sorts M pairs by timestamp, and truncates to limit. See NOTE-044.
func collectIntrinsicTopKKLL(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	stats *CollectStats,
) ([]modules_shared.BlockRef, error) {
	backward := opts.Direction == queryplanner.Backward
	limit := opts.Limit

	// NOTE-044: MaxStart from BlockMeta is the KLL-sketch upper bound on span:start
	// within a block. Ordering by MaxStart DESC biases collection toward newer blocks
	// first, but correctness requires a final sort over all M pairs since per-row
	// timestamps within a block may span a wide range.
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

	// Group M refs by BlockIdx and collect unique block indices.
	// NOTE-044: group so we can sort blockIdxs by MaxStart before collecting pairs.
	blockOrder := make([]int, 0, 8)
	blockRefs := make(map[uint16][]modules_shared.BlockRef, 8)
	for _, ref := range refs {
		bi := int(ref.BlockIdx)
		if opts.BlockCount > 0 && (bi < opts.StartBlock || bi >= opts.StartBlock+opts.BlockCount) {
			continue
		}
		if _, seen := blockRefs[ref.BlockIdx]; !seen {
			blockOrder = append(blockOrder, bi)
		}
		blockRefs[ref.BlockIdx] = append(blockRefs[ref.BlockIdx], ref)
	}

	// Sort blockIdxs by BlockMeta.MaxStart DESC: process newest blocks first.
	// NOTE-044: MaxStart is the maximum span:start in the block (KLL upper bound).
	// This ordering biases iteration toward newer blocks, enabling early termination
	// in future extensions, but correctness here requires the final sort over all pairs.
	slices.SortFunc(blockOrder, func(a, b int) int {
		metaA := r.BlockMeta(a)
		metaB := r.BlockMeta(b)
		// Descending: newer (larger MaxStart) first.
		return cmp.Compare(metaB.MaxStart, metaA.MaxStart)
	})

	type refTS struct {
		ref modules_shared.BlockRef
		ts  uint64
	}
	pairs := make([]refTS, 0, len(refs))
	for _, bi := range blockOrder {
		for _, ref := range blockRefs[uint16(bi)] { //nolint:gosec // bi is bounded by block count
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			if ts, ok := tsMap[key]; ok {
				pairs = append(pairs, refTS{ref: ref, ts: ts})
			}
		}
	}
	// Apply time range filter: O(M) pass to drop rows outside opts.TimeRange.
	// Zero values for MinNano/MaxNano mean "no bound" (open interval).
	if opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0 {
		filtered := pairs[:0]
		for _, p := range pairs {
			if (opts.TimeRange.MinNano == 0 || p.ts >= opts.TimeRange.MinNano) &&
				(opts.TimeRange.MaxNano == 0 || p.ts <= opts.TimeRange.MaxNano) {
				filtered = append(filtered, p)
			}
		}
		pairs = filtered
	}
	stats.ExecutionPath = "intrinsic-topk-kll"
	stats.IntrinsicRefCount = len(refs)
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
	selected := make([]modules_shared.BlockRef, len(pairs))
	for i, p := range pairs {
		selected[i] = p.ref
	}
	// IntrinsicScanCount stays 0 — KLL path does not scan the blob.
	return selected, nil
}

// collectIntrinsicTopKScan handles the scan path of collectIntrinsicTopK: M >= SortScanThreshold.
// Builds a hash set of M packed (BlockIdx<<16|RowIdx) keys and runs ScanFlatColumnRefsFiltered
// backward/forward, stopping after K results. Each row in the timestamp column is checked
// against the hash set in O(1), so the total cost is O(N) where N is the total span count.
// See NOTE-043.
func collectIntrinsicTopKScan(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	stats *CollectStats,
) ([]modules_shared.BlockRef, error) {
	backward := opts.Direction == queryplanner.Backward
	limit := opts.Limit

	// Build a hash set for O(1) per-row membership tests during the blob scan.
	// Previously this was a sorted []uint32 + slices.BinarySearch (O(log M) per row),
	// which caused 43% CPU flat in pprof for files with 1M spans. See NOTE-043.
	matchSet := make(map[uint32]struct{}, len(refs))
	for _, ref := range refs {
		matchSet[uint32(ref.BlockIdx)<<16|uint32(ref.RowIdx)] = struct{}{}
	}

	tsBlob, tsBlobErr := r.GetIntrinsicColumnBlob(opts.TimestampColumn)
	if tsBlobErr != nil || tsBlob == nil {
		return nil, errNeedBlockScan
	}

	startBlock := opts.StartBlock
	endBlock := 0
	if opts.BlockCount > 0 {
		endBlock = startBlock + opts.BlockCount
	}
	scanCount := 0
	selected := modules_shared.ScanFlatColumnRefsFiltered(tsBlob, backward, limit,
		func(ref modules_shared.BlockRef) bool {
			scanCount++
			bi := int(ref.BlockIdx)
			if endBlock > 0 && (bi < startBlock || bi >= endBlock) {
				return false
			}
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			_, found := matchSet[key]
			return found
		},
	)
	stats.ExecutionPath = "intrinsic-topk-scan"
	stats.IntrinsicRefCount = len(refs)
	stats.IntrinsicScanCount = scanCount

	// Apply time-range filter: the scan path does not have per-ref timestamps in scope,
	// so we post-filter the selected refs by decoding the timestamp column.
	// selected is bounded by limit (small), so this is cheap.
	if len(selected) > 0 && (opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0) {
		tsCol, tsErr := r.GetIntrinsicColumn(opts.TimestampColumn)
		if tsErr == nil && tsCol != nil && len(tsCol.Uint64Values) == len(tsCol.BlockRefs) {
			tsMap := make(map[uint32]uint64, len(tsCol.BlockRefs))
			for i, br := range tsCol.BlockRefs {
				tsMap[uint32(br.BlockIdx)<<16|uint32(br.RowIdx)] = tsCol.Uint64Values[i]
			}
			filtered := selected[:0]
			for _, ref := range selected {
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				ts, ok := tsMap[key]
				if !ok {
					continue
				}
				if opts.TimeRange.MinNano > 0 && ts < opts.TimeRange.MinNano {
					continue
				}
				if opts.TimeRange.MaxNano > 0 && ts > opts.TimeRange.MaxNano {
					continue
				}
				filtered = append(filtered, ref)
			}
			selected = filtered
		}
	}
	return selected, nil
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
	stats *CollectStats,
) ([]MatchedRow, error) {
	// Sort refs and apply shard filter (same as collectIntrinsicPlain).
	slices.SortFunc(refs, blockRefCompare)
	refs = filterRefsByShardRange(refs, opts)
	if len(refs) == 0 {
		return nil, nil
	}

	blockOrder, blockCandidates := groupRefsByBlock(refs)
	stats.ExecutionPath = "mixed-plain"
	stats.MixedCandidateBlocks = len(blockOrder)

	var results []MatchedRow
	// Coalesce all candidate blocks for efficient batch I/O.
	err := forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectMixedPlain",
		func(pb parsedBlock, candidateRows []int) error {
			// Re-evaluate the full predicate against this block.
			provider := newBlockColumnProvider(pb.Block)
			rowSet, evalErr := program.ColumnPredicate(provider)
			if evalErr != nil {
				return fmt.Errorf("collectMixedPlain ColumnPredicate block %d: %w", pb.BlockIdx, evalErr)
			}

			// Intersect VM result with candidate rows from intrinsic pre-filter.
			for _, rowIdx := range candidateRows {
				if !rowSet.Contains(rowIdx) {
					continue // eliminated by VM re-evaluation
				}
				results = append(results, MatchedRow{Block: pb.Block, BlockIdx: pb.BlockIdx, RowIdx: rowIdx})
				if opts.Limit > 0 && len(results) >= opts.Limit {
					return errLimitReached
				}
			}
			return nil
		},
	)
	if err != nil && err != errLimitReached {
		return nil, err
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
	stats *CollectStats,
) ([]MatchedRow, error) {
	backward := opts.Direction == queryplanner.Backward

	// Apply shard filter.
	refs = filterRefsByShardRange(refs, opts)
	if len(refs) == 0 {
		return nil, nil
	}

	blockOrder, blockCandidates := groupRefsByBlock(refs)
	stats.ExecutionPath = "mixed-topk"
	stats.MixedCandidateBlocks = len(blockOrder)

	buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}

	// Coalesce all candidate blocks for efficient batch I/O.
	if err := forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectMixedTopK",
		func(pb parsedBlock, candidateRows []int) error {
			// Re-evaluate the full predicate.
			provider := newBlockColumnProvider(pb.Block)
			rowSet, evalErr := program.ColumnPredicate(provider)
			if evalErr != nil {
				return fmt.Errorf("collectMixedTopK ColumnPredicate block %d: %w", pb.BlockIdx, evalErr)
			}

			// Collect rows that pass both the pre-filter and full predicate.
			var qualifying []int
			for _, rowIdx := range candidateRows {
				if rowSet.Contains(rowIdx) {
					qualifying = append(qualifying, rowIdx)
				}
			}
			if len(qualifying) == 0 {
				return nil
			}

			// Get timestamp column for heap ordering.
			tsCol := pb.Block.GetColumn(opts.TimestampColumn)
			if tsCol == nil {
				return nil
			}

			topKScanRows(buf, opts.Limit, backward, pb.Block, pb.BlockIdx, tsCol, opts.TimeRange, qualifying)
			return nil
		},
	); err != nil {
		return nil, err
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
//
// The lookup index uses map[uint32]int (packed key → result position) for O(1)
// access per scan entry. This eliminated the slices.BinarySearchFunc hotspot
// that was previously 43% flat CPU when using a sorted []keyIdx slice.
func lookupIntrinsicFields(r *modules_reader.Reader, selected []modules_shared.BlockRef, wantCols map[string]struct{}) []map[string]any {
	m := len(selected)
	index := make(map[uint32]int, m)
	for i, ref := range selected {
		index[uint32(ref.BlockIdx)<<16|uint32(ref.RowIdx)] = i
	}

	// searchIndex returns the original position in selected for the given packed key,
	// or -1 if not found.
	searchIndex := func(key uint32) int {
		if idx, ok := index[key]; ok {
			return idx
		}
		return -1
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
			// Hash map lookup (searchIndex) gives O(1) per ref.
			for _, entry := range col.DictEntries {
				var val any
				if entry.Value != "" {
					val = entry.Value
				} else {
					val = entry.Int64Val
				}
				for _, ref := range entry.BlockRefs {
					key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
					if idx := searchIndex(key); idx >= 0 {
						result[idx][colName] = val
					}
				}
			}
		case modules_shared.IntrinsicFormatFlat:
			// Flat columns: scan refs with O(1) hash map lookup per ref.
			for i, ref := range col.BlockRefs {
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				if idx := searchIndex(key); idx >= 0 {
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
