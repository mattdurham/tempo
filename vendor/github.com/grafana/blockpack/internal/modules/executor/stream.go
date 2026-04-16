package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// errNeedBlockScan is returned by collectFromIntrinsicRefs when the intrinsic pre-filter
// is not applicable and the caller should fall through to a full block scan.
// NOTE-038: sentinel error for fallback from intrinsic pre-filter to block scan.
var errNeedBlockScan = errors.New("intrinsic fast path not applicable")

// errLimitReached is used as an early-stop sentinel inside block-scan pipeline callbacks
// (scanBlocks, forEachBlockInGroups, collectMixedPlain). It signals that the results limit
// has been satisfied. It is never returned to callers; blockGroupPipeline translates it to nil.
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
	// TimestampColumn is the column for per-row time filtering.
	// Empty string disables per-row filtering (trace mode).
	// "log:timestamp" enables per-row filtering (log mode).
	TimestampColumn string
	// SelectColumns limits which output columns are decoded from block blobs.
	// nil or empty means all columns are returned (no projection applied).
	// A nil slice and a non-nil empty slice are equivalent: both mean all columns are returned.
	// Two-path strategy (see stream.go Collect for implementation):
	//   - No predicate columns (e.g. "{}"): SelectColumns becomes the sole first-pass filter,
	//     preventing all other columns from being decoded on every scanned block.
	//   - Predicate columns present: SelectColumns is deferred to the second parse pass so
	//     output-only columns are only decoded for blocks that match the filter, not for every
	//     scanned block. Predicate columns are always included regardless of this list.
	// NOTE: When AllColumns=true, SelectColumns has no effect (see AllColumns doc above).
	SelectColumns []string
	TimeRange     queryplanner.TimeRange
	// Limit caps the number of returned rows. 0 means no limit (return all matches).
	// Negative values are treated as 0 (unlimited) — the executor does not validate sign.
	Limit int
	// StartBlock is the first internal block index to include (0-based, inclusive).
	// Used by the frontend sharder to partition a single file across multiple jobs.
	// 0 with BlockCount==0 means scan all blocks (no sub-file sharding).
	StartBlock int
	// BlockCount is the number of internal blocks to include starting from StartBlock.
	// 0 means no sub-file sharding (scan all blocks selected by the planner).
	BlockCount int
	// Direction controls block traversal order. Default (zero value) is Forward.
	Direction queryplanner.Direction
	// NOTE-028: AllColumns controls second-pass decode scope.
	// false (default): second pass decodes searchMetaColumns ∪ wantColumns (predicate columns).
	// true: second pass decodes all columns. Only needed when the callback calls IterateFields()
	// to enumerate every attribute. Search queries never need this.
	// NOTE: When AllColumns=true, computeColumnFilters returns early after computing wantColumns
	// (predicate columns only) and sets secondPassCols=nil. A nil secondPassCols causes the second
	// parse pass to decode all columns unconditionally. For queries with predicate columns,
	// SelectColumns is NOT in wantColumns (it would normally be deferred to secondPassCols), so
	// output-only columns are not pre-decoded in the first pass either. Net effect: AllColumns=true
	// guarantees full column availability after the second pass; SelectColumns has no influence.
	AllColumns bool
}

// MatchedRow holds a single row result from Collect.
type MatchedRow struct {
	Block *modules_reader.Block
	// IntrinsicFields is set when the result was produced by the intrinsic fast path
	// without reading full blocks. The caller should use this for field lookups
	// when Block is nil.
	IntrinsicFields modules_shared.SpanFieldsProvider
	// Score is the cosine similarity for VECTOR() queries. Zero for non-vector queries.
	Score    float32
	BlockIdx int
	RowIdx   int
}

// Collect selects candidate blocks via queryplanner and evaluates program.ColumnPredicate
// against each block's spans, collecting all matched rows into a slice.
//
// SPEC-STREAM-2: Blocks are fetched lazily via CoalescedGroups/ReadGroup (~8 MB per I/O).
// SPEC-STREAM-3: FetchedBlocks <= SelectedBlocks; early stop skips unfetched groups.
// SPEC-STREAM-4: TimestampColumn == "" disables per-row time filtering (trace mode).
// computeColumnFilters derives wantColumns (first-pass eager-decode set) and secondPassCols
// (second-pass decode set) from the program and CollectOptions.
//
// SelectColumns two-path strategy (see CollectOptions.SelectColumns):
//   - No predicate columns (wantColumns == nil, e.g. "{}"): promote SelectColumns to the sole
//     first-pass filter so only the requested output columns are decoded from every block blob.
//   - Predicate columns present: do NOT add SelectColumns to wantColumns — output-only columns
//     are deferred to secondPassCols so they are decoded only for blocks that match the filter.
func computeColumnFilters(program *vm.Program, opts CollectOptions) (wantColumns, secondPassCols map[string]struct{}) {
	wantColumns = ProgramWantColumns(program)
	if len(opts.SelectColumns) > 0 && wantColumns == nil {
		wantColumns = make(map[string]struct{}, len(opts.SelectColumns))
		for _, col := range opts.SelectColumns {
			wantColumns[col] = struct{}{}
		}
	}
	// NOTE-028: secondPassCols is nil when AllColumns=true or there is no column filter.
	if wantColumns == nil || opts.AllColumns {
		return
	}
	searchCols := searchMetaCols
	secondPassCols = make(map[string]struct{}, len(searchCols)+len(wantColumns)+len(opts.SelectColumns)+2)
	for k := range searchCols {
		secondPassCols[k] = struct{}{}
	}
	for k := range wantColumns {
		secondPassCols[k] = struct{}{}
	}
	// When predicate columns are present, SelectColumns was deferred from wantColumns.
	// Add them here so they are decoded in the second pass (matching blocks only).
	for _, col := range opts.SelectColumns {
		secondPassCols[col] = struct{}{}
	}
	// NOTE-050: Include trace intrinsic columns for lookupIntrinsicFields (trace:id, span:id,
	// span:start, span:name etc.) and the sort timestamp column for Case B (TopK) ordering.
	for k := range traceIntrinsicColumns {
		secondPassCols[k] = struct{}{}
	}
	if opts.TimestampColumn != "" {
		secondPassCols[opts.TimestampColumn] = struct{}{}
	}
	return
}

// Collect executes program against all blocks in r and returns matched rows.
// SPEC-STREAM-5: Direction is applied at plan time; rows are reversed within each block for Backward.
// SPEC-STREAM-6: QueryStats is returned as the third return value with execution metrics.
func Collect(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
) ([]MatchedRow, QueryStats, error) {
	// SPEC-STREAM-1: nil reader — return nil result slice and nil error.
	if r == nil {
		return nil, QueryStats{}, nil
	}
	if program == nil {
		return nil, QueryStats{}, fmt.Errorf("executor.Collect: program must not be nil")
	}
	if opts.StartBlock < 0 || opts.BlockCount < 0 {
		return nil, QueryStats{}, fmt.Errorf(
			"executor.Collect: invalid shard parameters: StartBlock=%d BlockCount=%d",
			opts.StartBlock,
			opts.BlockCount,
		)
	}
	if opts.BlockCount > 0 && opts.StartBlock+opts.BlockCount < opts.StartBlock {
		return nil, QueryStats{}, fmt.Errorf(
			"executor.Collect: shard range overflow: StartBlock=%d BlockCount=%d",
			opts.StartBlock,
			opts.BlockCount,
		)
	}

	queryStart := time.Now()

	wantColumns, secondPassCols := computeColumnFilters(program, opts)

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
	// NOTE-050: Pure intrinsic queries always use the fast path regardless of Limit —
	// intrinsic columns are no longer in block payloads, so the block scan path would
	// evaluate nil columns and return 0 results for any intrinsic predicate.
	// Mixed queries (Cases C/D) still require Limit > 0 to bound the pre-filter cost.
	if hasSomeIntrinsicPredicates(program) && (opts.Limit > 0 || ProgramIsIntrinsicOnly(program)) {
		rows, fastQS, err := collectWithBloomCheck(r, program, opts, wantColumns, secondPassCols)
		if err != errNeedBlockScan {
			// Fast path produced a definitive result (rows, empty result, or error).
			fastQS.TotalDuration = time.Since(queryStart)
			return rows, fastQS, err
		}
		// SPEC-ROOT-010: slog.Warn when intrinsic fast path falls through to block scan.
		slog.Warn("intrinsic fast path fell through to full block scan",
			"execution_path", ExecPathIntrinsicNeedBlock,
			"total_blocks", r.BlockCount(),
		)
		// errNeedBlockScan: fall through to full block scan. fastQS is discarded.
	}

	var qs QueryStats

	// --- Plan step ---
	planStart := time.Now()
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

	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNamePlan,
		Duration: time.Since(planStart),
		Metadata: map[string]any{
			"total_blocks":    plan.TotalBlocks,
			"pruned_by_time":  plan.PrunedByTime,
			"pruned_by_index": plan.PrunedByIndex,
			"pruned_by_fuse":  plan.PrunedByFuse,
			"selected_blocks": len(plan.SelectedBlocks),
			"explain":         plan.Explain,
		},
	})

	if len(plan.SelectedBlocks) == 0 {
		qs.ExecutionPath = ExecPathBlockPruned
		qs.TotalDuration = time.Since(queryStart)
		return nil, qs, nil
	}

	// SPEC-STREAM-2: Partition selected blocks into ~8 MB coalesced groups for lazy batched I/O.
	groups := r.CoalescedGroups(plan.SelectedBlocks)

	// --- Block-scan step ---
	scanStart := time.Now()
	var fetchedGroups int
	var fetchedBlocks int
	var bytesRead int64
	var results []MatchedRow

	// Heap-based scan for timestamp-sorted queries (MostRecent/Oldest with a limit).
	// Guarantees globally correct top-K by scanning all blocks and maintaining a priority
	// queue. The intrinsic fast path (above) already handles intrinsic-only queries without
	// full block I/O; this path handles all other timestamp-sorted queries.
	// VECTOR() queries always use the plain scan path to enable correct cosine top-K;
	// timestamp-sorted top-K and cosine-similarity top-K have incompatible semantics.
	if shouldUseTopKPath(opts, program) {
		qs.ExecutionPath = ExecPathBlockTopK
		backward := opts.Direction == queryplanner.Backward
		buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}
		var scanErr error
		fetchedGroups, fetchedBlocks, bytesRead, scanErr = topKScanBlocks(
			r,
			program,
			wantColumns,
			secondPassCols,
			opts,
			plan,
			buf,
			groups,
			backward,
		)
		if scanErr != nil {
			qs.TotalDuration = time.Since(queryStart)
			return nil, qs, scanErr
		}
		results = topKDeliver(buf, backward)
	} else {
		qs.ExecutionPath = ExecPathBlockPlain
		// NOTE-054: when Limit > 0, preallocate exactly opts.Limit (exact upper bound for
		// results). When unlimited, leave nil — result count is unbounded (append handles it).
		if opts.Limit > 0 {
			results = make([]MatchedRow, 0, opts.Limit)
		}
		var scanErr error
		fetchedGroups, fetchedBlocks, bytesRead, scanErr = scanBlocks(
			r, program, wantColumns, secondPassCols, opts,
			plan.SelectedBlocks, groups, &results,
		)
		if scanErr != nil {
			qs.TotalDuration = time.Since(queryStart)
			return nil, qs, scanErr
		}
	}

	qs.Steps = append(qs.Steps, StepStats{
		Name:      stepNameBlockScan,
		Duration:  time.Since(scanStart),
		BytesRead: bytesRead,
		IOOps:     fetchedGroups,
		Metadata: map[string]any{
			"fetched_blocks": fetchedBlocks,
			"matched_rows":   len(results),
		},
	})

	// Vector post-processing: select global top-K across all blocks by score.
	// Scoring is already done per-block in scanBlocks via VectorScorer.
	if program.VectorScorer != nil {
		results = vectorTopKFromScoredRows(results, program.VectorLimit)
	}

	qs.TotalDuration = time.Since(queryStart)
	return results, qs, nil
}

// shouldUseTopKPath returns true when a heap-sorted timestamp top-K scan should be used.
// VECTOR() queries are excluded: cosine-similarity top-K and timestamp top-K are
// semantically incompatible — both would restrict results but by different criteria.
func shouldUseTopKPath(opts CollectOptions, program *vm.Program) bool {
	return opts.TimestampColumn != "" && opts.Limit > 0 && !program.HasVector
}

// scanBlocks iterates over selectedBlocks in order, concurrently fetching coalesced groups
// via blockGroupPipeline, evaluating the program predicate, and appending matched rows.
// Returns the number of groups fetched from storage, total blocks fetched, bytes read, and any error.
//
// SPEC-STREAM-2: Each group is fetched at most once (~8 MB coalesced I/O).
// SPEC-STREAM-3: fetchedGroups counts ReadGroup calls; fetchedBlocks counts individual blocks fetched.
// Groups that are never fetched (due to early stop) are not counted in either.
// SPEC-STREAM-11: I/O is concurrent across defaultPipelineWorkers goroutines; parse is sequential.
func scanBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	opts CollectOptions,
	selectedBlocks []int,
	groups []modules_shared.CoalescedRead,
	results *[]MatchedRow,
) (int, int, int64, error) {
	// Pre-build a blockToGroup map and a groupToBlocks index (maintaining selectedBlocks order)
	// so processGroup iterates only the ~N/G blocks relevant to its group rather than all N.
	// This restores O(N) total work across all groups (O(N/G) per group × G groups).
	blockToGroup := make(map[int]int, len(selectedBlocks))
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			blockToGroup[bi] = gi
		}
	}
	groupToBlocks := make([][]int, len(groups))
	for _, bi := range selectedBlocks {
		gi, ok := blockToGroup[bi]
		if !ok {
			continue
		}
		groupToBlocks[gi] = append(groupToBlocks[gi], bi)
	}

	// processGroup is called sequentially (never concurrently) by blockGroupPipeline.
	// It iterates groupToBlocks[groupIdx] — blocks in selectedBlocks order for this group only.
	// This preserves block-then-row traversal order (SPEC-STREAM-2, §4.2 match ordering).
	processGroup := func(groupIdx int, groupRaw map[int][]byte) error {
		for _, blockIdx := range groupToBlocks[groupIdx] {
			raw, ok := groupRaw[blockIdx]
			if !ok {
				continue
			}
			// Free raw bytes immediately after access — avoids retaining the full
			// coalesced group in memory for the duration of the block scan loop.
			delete(groupRaw, blockIdx)

			meta := r.BlockMeta(blockIdx)
			r.ResetInternStrings()

			// NOTE-006: Acquire a pooled intern map for this block's lifetime. The map must
			// remain alive through both parse passes and the entire row-emission loop, because
			// lazy columns (registered during first pass) call decodeNow() during row iteration
			// and reference the intern map. Release after streamSortedRows completes.
			internPtr := modules_reader.AcquireInternMap()
			intern := *internPtr

			bwb, parseErr := r.ParseBlockFromBytesWithIntern(raw, wantColumns, meta, intern)
			if parseErr != nil {
				modules_reader.ReleaseInternMap(internPtr)
				return fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
			}

			provider := newBlockColumnProvider(bwb.Block)
			// Only strip intrinsic predicates when the file has an intrinsic section.
			// Log files do not have an intrinsic section; their block columns still hold
			// all label values and ColumnPredicate must evaluate them directly.
			var rowSet vm.RowSet
			var evalErr error
			if r.HasIntrinsicSection() {
				uap := userAttrProgram(program)
				if uap == nil {
					rowSet = provider.FullScan()
				} else {
					rowSet, evalErr = uap.ColumnPredicate(provider)
				}
			} else {
				rowSet, evalErr = program.ColumnPredicate(provider)
			}
			if evalErr != nil {
				modules_reader.ReleaseInternMap(internPtr)
				return fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
			}

			if rowSet.Size() == 0 {
				modules_reader.ReleaseInternMap(internPtr)
				continue
			}

			// Post-filter rowSet against any intrinsic predicates stripped by userAttrProgram.
			// Only applies when the file has an intrinsic section (trace files with new storage format).
			// Log files and legacy files evaluate intrinsic predicates directly via ColumnPredicate above.
			intrNodes := programIntrinsicNodes(program)
			if len(intrNodes) > 0 && r.HasIntrinsicSection() {
				rowSet = filterRowSetByIntrinsicNodes(r, blockIdx, rowSet, intrNodes)
				if rowSet.Size() == 0 {
					modules_reader.ReleaseInternMap(internPtr)
					continue
				}
			}

			// NOTE-018: Second pass — decode result columns now that we know this block has matches.
			// NOTE-028: secondPassCols is pre-computed above (searchMetaColumns ∪ wantColumns, or nil for all).
			if wantColumns != nil {
				bwb, parseErr = r.ParseBlockFromBytesWithIntern(bwb.RawBytes, secondPassCols, meta, intern)
				if parseErr != nil {
					modules_reader.ReleaseInternMap(internPtr)
					return fmt.Errorf("ParseBlockFromBytes (second pass) block %d: %w", blockIdx, parseErr)
				}
			}

			// Vector post-filter: when the query has a VectorScorer, score only the candidate
			// rows (survivors of ColumnPredicate + intrinsic filter) via point lookup.
			// Non-vector queries take the streamSortedRows path unchanged.
			if program.VectorScorer != nil {
				scoredRows := applyVectorScorerToBlock(bwb.Block, program, rowSet)
				modules_reader.ReleaseInternMap(internPtr)
				for _, sr := range scoredRows {
					*results = append(*results, MatchedRow{
						Block:    bwb.Block,
						BlockIdx: blockIdx,
						RowIdx:   sr.RowIdx,
						Score:    sr.Score,
					})
				}
				continue
			}

			// NOTE: rowSet is not used after ToSlice() — safe to sort in-place without clone.
			// streamSortedRows sorts and reverses rows in-place via slices.SortFunc and index swap,
			// which mutates the backing slice returned by ToSlice(). This is intentional: rowSet
			// is never accessed again (no Contains calls) after this point in scanBlocks.
			// If rowSet reuse is added in future, restore slices.Clone here to preserve the
			// ascending-sorted invariant required by rowSet.Contains.
			rows := rowSet.ToSlice()

			// SPEC-STREAM-5: Sort rows by per-row timestamp when TimestampColumn is set.
			var tsCol *modules_reader.Column
			if opts.TimestampColumn != "" {
				tsCol = bwb.Block.GetColumn(opts.TimestampColumn)
			}

			stop := streamSortedRows(bwb.Block, blockIdx, rows, tsCol, opts, results)
			// Release intern map after all lazy decodes in streamSortedRows are complete.
			modules_reader.ReleaseInternMap(internPtr)
			if stop {
				return errLimitReached
			}
		}
		return nil
	}

	// SPEC-STREAM-11: concurrent I/O via blockGroupPipeline; processGroup called sequentially.
	// TODO: propagate caller context (NOTE-058: Collect does not yet accept context.Context).
	return blockGroupPipeline(context.Background(), r, groups, defaultPipelineWorkers, processGroup)
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
	seen := make(map[uint16]struct{}, min(len(refs), 64))
	for _, ref := range refs {
		seen[ref.BlockIdx] = struct{}{}
	}
	return len(seen)
}

// collectFromIntrinsicRefs is the unified intrinsic pre-filter fast path.
// It dispatches based on (ProgramIsIntrinsicOnly × opts.TimestampColumn != ""):
//
//	Case A: pure intrinsic + no sort  → BlockRefsFromIntrinsicTOC → lookupIntrinsicFields
//	        → return IntrinsicFields MatchedRows (ZERO block reads; fields from objectcache)
//	Case B: pure intrinsic + sort     → BlockRefsFromIntrinsicTOC → pack into sorted
//	        keys → ScanFlatColumnRefsFiltered → return IntrinsicFields MatchedRows
//	        (ZERO block reads only when wantColumns AND secondPassCols are both nil)
//	Case C: mixed + no sort           → blockRefsFromIntrinsicPartial → fetch candidate
//	        blocks → ColumnPredicate re-eval → intersect → collect up to limit
//	Case D: mixed + sort              → blockRefsFromIntrinsicPartial → fetch candidate
//	        blocks → ColumnPredicate re-eval → topKScanRows → topKDeliver
//
// Returns (nil, errNeedBlockScan) when no intrinsic constraint is available (fall through
// to full block scan). Returns (nil, nil) for valid empty-result cases.
//
// collectWithBloomCheck runs the intrinsic fast path with a FileBloom pre-check.
// SPEC-INTRINSIC-004: reject the file in O(1) if bloom says no span matches.
// SPEC-STREAM-6: QueryStats is returned as part of the result.
func collectWithBloomCheck(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
) ([]MatchedRow, QueryStats, error) {
	var qs QueryStats
	if program.Predicates != nil && fileLevelBloomReject(r, program.Predicates.Nodes) {
		qs.ExecutionPath = ExecPathBloomRejected
		return nil, qs, nil
	}
	return collectFromIntrinsicRefs(r, program, opts, wantColumns, secondPassCols, &qs)
}

// NOTE-038: The partial-AND pre-filter for mixed queries is a superset; ColumnPredicate
// re-evaluation in Cases C/D provides correctness. Global top-K is preserved for Case D
// because the pre-filter never excludes true matches (it is a superset, never a subset).
func collectFromIntrinsicRefs(
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	qs *QueryStats,
) ([]MatchedRow, QueryStats, error) {
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
		qs.ExecutionPath = ExecPathIntrinsicNeedBlock
		return nil, *qs, errNeedBlockScan
	}
	if len(refs) == 0 {
		if isPureIntrinsic {
			if hasSort {
				qs.ExecutionPath = ExecPathIntrinsicTopKKLL // would have been Case B
			} else {
				qs.ExecutionPath = ExecPathIntrinsicPlain // would have been Case A
			}
		} else {
			if hasSort {
				qs.ExecutionPath = ExecPathMixedTopK // would have been Case D
			} else {
				qs.ExecutionPath = ExecPathMixedPlain // would have been Case C
			}
		}
		return nil, *qs, nil
	}

	// Selectivity guard: if the partial pre-filter covers more than half the internal
	// blocks it offers no I/O benefit for Cases C/D, which must read blocks for VM eval.
	// Fall through to the regular block scan (coalesced I/O, planBlocks pruning).
	if !isPureIntrinsic {
		uniqueBlocks := countUniqueBlockIdxs(refs)
		if uniqueBlocks*2 > r.BlockCount() {
			qs.ExecutionPath = ExecPathIntrinsicNeedBlock
			return nil, *qs, errNeedBlockScan
		}
	}

	// Apply sub-file shard filtering before computing selected_blocks so that
	// sharded queries report accurate counts in the plan step.
	refs = filterRefsByShardRange(refs, opts)

	// Plan step for the intrinsic fast path: records block counts for observability.
	// Unlike the block-scan path, no time-range or bloom pruning occurs here — the
	// intrinsic TOC already filters refs by predicate. selected_blocks reflects the
	// number of distinct blocks containing matching refs after shard filtering.
	qs.Steps = append(qs.Steps, StepStats{
		Name: stepNamePlan,
		Metadata: map[string]any{
			"total_blocks":    r.BlockCount(),
			"selected_blocks": countUniqueBlockIdxs(refs),
		},
	})

	// Step 2: Dispatch based on (isPureIntrinsic, hasSort).
	if isPureIntrinsic && !hasSort {
		rows, err := collectIntrinsicPlain(r, refs, opts, wantColumns, secondPassCols, qs)
		return rows, *qs, err
	}
	if isPureIntrinsic && hasSort {
		rows, err := collectIntrinsicTopK(r, refs, opts, wantColumns, secondPassCols, qs)
		return rows, *qs, err
	}
	if !hasSort {
		// Case C: mixed + no sort
		rows, err := collectMixedPlain(r, program, refs, opts, wantColumns, secondPassCols, qs)
		return rows, *qs, err
	}
	// Case D: mixed + sort
	rows, err := collectMixedTopK(r, program, refs, opts, wantColumns, secondPassCols, qs)
	return rows, *qs, err
}

// parsedBlock holds the result of the two-pass parse for one block.
type parsedBlock struct {
	Block    *modules_reader.Block
	BlockIdx int
}

// groupRefsByBlock converts a slice of BlockRefs into a stable block traversal order
// and a map from block index to row indices. The order preserves first-seen block order.
func groupRefsByBlock(refs []modules_shared.BlockRef) (blockOrder []int, blockRows map[int][]int) {
	// NOTE-054: cap at min(len(refs), 64). Distinct block count ≤ len(refs) but is typically
	// far smaller. 64 avoids pathological over-allocation (e.g. 1000 refs across 5 blocks).
	blockOrder = make([]int, 0, min(len(refs), 64))
	blockRows = make(map[int][]int, min(len(refs), 64))
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
//
// SPEC-STREAM-11: I/O is concurrent across defaultPipelineWorkers goroutines; parse is sequential.
//
// SPEC-STREAM-12: Second-pass decode gate.
// When preFn is non-nil it is called after the first-pass decode with the parsed block and
// candidate row indices. If preFn returns false, the second-pass decode and fn are both
// skipped for that block. When preFn is nil it is not called and the second-pass decode
// proceeds as usual (preserves behavior for intrinsic-only callers).
func forEachBlockInGroups(
	r *modules_reader.Reader,
	blockOrder []int,
	blockCandidates map[int][]int,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	callerName string,
	preFn func(pb parsedBlock, candidateRows []int) bool,
	fn func(pb parsedBlock, candidateRows []int) error,
) error {
	groups := r.CoalescedGroups(blockOrder)
	if len(groups) == 0 {
		return nil
	}

	// processGroup is called sequentially (never concurrently) by blockGroupPipeline.
	// Reader.ParseBlockFromBytes is not concurrency-safe; sequential invocation is required.
	// NOTE-048: ParseBlockFromBytes allocates a fresh local intern map per call.
	// Iterate groups[groupIdx].BlockIDs (not range groupRaw) to preserve deterministic
	// block-processing order within a group, matching the pre-migration behavior.
	processGroup := func(groupIdx int, groupRaw map[int][]byte) error {
		for _, blockIdx := range groups[groupIdx].BlockIDs {
			raw, ok := groupRaw[blockIdx]
			if !ok {
				continue
			}
			candidateRows := blockCandidates[blockIdx]
			meta := r.BlockMeta(blockIdx)
			r.ResetInternStrings()
			bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
			if parseErr != nil {
				return fmt.Errorf("%s ParseBlockFromBytes block %d: %w", callerName, blockIdx, parseErr)
			}
			pb := parsedBlock{Block: bwb.Block, BlockIdx: blockIdx}
			// SPEC-STREAM-12: gate second-pass decode on preFn result.
			// When preFn is non-nil, call it with the first-pass block; skip the
			// second-pass decode and fn entirely if it returns false.
			if preFn != nil && !preFn(pb, candidateRows) {
				delete(groupRaw, blockIdx)
				continue
			}
			// M-18: Skip second parse when candidateRows is empty — no rows passed the first-pass
			// predicate, so decoding additional columns would produce no output for this block.
			if wantColumns != nil && len(candidateRows) > 0 {
				bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
				if parseErr != nil {
					return fmt.Errorf("%s second pass block %d: %w", callerName, blockIdx, parseErr)
				}
				pb = parsedBlock{Block: bwb.Block, BlockIdx: blockIdx}
			}
			// Release raw bytes now. Safety: bwb (the local variable) holds a strong
			// reference to bwb.RawBytes, which keeps the backing array alive through the
			// fn call below. Lazily-decoded columns (NOTE-001 rawEncoding) slice into
			// RawBytes via bwb, not via the raw map entry — the delete is safe.
			// Lets GC reclaim block bytes before the next group is processed. NOTE-048.
			delete(groupRaw, blockIdx)
			if err := fn(pb, candidateRows); err != nil {
				return err
			}
		}
		return nil
	}

	// SPEC-STREAM-11: concurrent I/O via blockGroupPipeline; processGroup called sequentially.
	// TODO: propagate caller context (NOTE-058: forEachBlockInGroups callers do not yet accept context.Context).
	_, _, _, err := blockGroupPipeline(context.Background(), r, groups, defaultPipelineWorkers, processGroup)
	return err
}

// collectIntrinsicPlain handles Case A: pure intrinsic + no sort.
//
// Field values are always resolved from objectcache-backed intrinsic columns via
// lookupIntrinsicFields — zero S3 I/O after warmup. lookupIntrinsicFields uses
// LookupRefFast for O(M log N) binary search per ref, which caches the ref index
// on the column object (EnsureRefIndex is called internally on first use).
func collectIntrinsicPlain(
	r *modules_reader.Reader,
	refs []modules_shared.BlockRef,
	opts CollectOptions,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	qs *QueryStats,
) ([]MatchedRow, error) {
	qs.ExecutionPath = ExecPathIntrinsicPlain
	stepStart := time.Now()
	// Sort refs by (BlockIdx, RowIdx) for deterministic traversal order.
	// Shard filtering was already applied by collectFromIntrinsicRefs before this call.
	slices.SortFunc(refs, blockRefCompare)

	selectedBlocks := countUniqueBlockIdxs(refs)
	if len(refs) == 0 {
		qs.Steps = append(qs.Steps, StepStats{
			Name:     stepNameIntrinsic,
			Duration: time.Since(stepStart),
			Metadata: map[string]any{"selected_blocks": selectedBlocks},
		})
		return nil, nil
	}

	// Apply limit: truncate refs before field lookup to avoid unnecessary work.
	if opts.Limit > 0 && len(refs) > opts.Limit {
		refs = refs[:opts.Limit]
	}

	// Block reads for field population — groups refs by block, reads each block once,
	// and returns MatchedRow.Block populated. O(M) where M is the result count.
	blockOrder, blockCandidates := groupRefsByBlock(refs)
	results := make([]MatchedRow, 0, len(refs))
	err := forEachBlockInGroups(
		r,
		blockOrder,
		blockCandidates,
		wantColumns,
		secondPassCols,
		"collectIntrinsicPlain",
		nil,
		func(pb parsedBlock, candidateRows []int) error {
			for _, rowIdx := range candidateRows {
				results = append(results, MatchedRow{
					Block:    pb.Block,
					BlockIdx: pb.BlockIdx,
					RowIdx:   rowIdx,
				})
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNameIntrinsic,
		Duration: time.Since(stepStart),
		Metadata: map[string]any{"selected_blocks": selectedBlocks},
	})
	return results, nil
}

// collectIntrinsicTopK handles Case B: pure intrinsic + timestamp sort.
// Selects the top-K refs via KLL or scan path, then uses block reads to
// populate MatchedRow.Block — same forEachBlockInGroups approach as collectIntrinsicPlain.
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
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	qs *QueryStats,
) ([]MatchedRow, error) {
	var (
		selected []modules_shared.BlockRef
		err      error
	)
	if len(refs) < SortScanThreshold {
		selected, err = collectIntrinsicTopKKLL(r, refs, opts, qs)
	} else {
		selected, err = collectIntrinsicTopKScan(r, refs, opts, qs)
	}
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, nil
	}
	// Block reads for field population — same as collectIntrinsicPlain.
	slices.SortFunc(selected, blockRefCompare)
	blockOrder, blockCandidates := groupRefsByBlock(selected)
	results := make([]MatchedRow, 0, len(selected))
	err = forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectIntrinsicTopK", nil,
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
	// Re-sort results by timestamp to restore the TopK ordering that KLL/scan paths
	// established before block reads re-ordered rows by (BlockIdx, RowIdx).
	// span:start is stored as a regular block column, so it is available on row.Block
	// after forEachBlockInGroups populates MatchedRow.Block above.
	if opts.TimestampColumn != "" && len(results) > 1 {
		backward := opts.Direction == queryplanner.Backward
		slices.SortStableFunc(results, func(a, b MatchedRow) int {
			var tsA, tsB uint64
			if a.Block != nil {
				if col := a.Block.GetColumn(opts.TimestampColumn); col != nil {
					tsA, _ = col.Uint64Value(a.RowIdx)
				}
			}
			if b.Block != nil {
				if col := b.Block.GetColumn(opts.TimestampColumn); col != nil {
					tsB, _ = col.Uint64Value(b.RowIdx)
				}
			}
			if backward {
				return cmp.Compare(tsB, tsA) // descending: newer first
			}
			return cmp.Compare(tsA, tsB) // ascending: older first
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
	qs *QueryStats,
) ([]modules_shared.BlockRef, error) {
	stepStart := time.Now()
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
	// Build the ref index once (O(N log N), cached on tsCol via sync.Once), then use
	// O(log N) LookupRefFast per ref — avoiding per-call allocations vs the old O(N) map build.
	// LookupRefFast calls EnsureRefIndex internally; no explicit call needed here.

	// Group M refs by BlockIdx and collect unique block indices.
	// NOTE-044: group so we can sort blockIdxs by MaxStart before collecting pairs.
	// Cap at min(len(refs), 64): distinct block count is always ≤ len(refs) but typically
	// far smaller. 64 is a reasonable upper bound for per-query block fan-out. NOTE-054.
	blockOrder := make([]int, 0, min(len(refs), 64))
	blockRefs := make(map[uint16][]modules_shared.BlockRef, min(len(refs), 64))
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
			packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
			if val, ok := tsCol.LookupRefFast(packed); ok {
				pairs = append(pairs, refTS{ref: ref, ts: val.(uint64)})
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
	qs.ExecutionPath = ExecPathIntrinsicTopKKLL
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNameIntrinsic,
		Duration: time.Since(stepStart),
		Metadata: map[string]any{
			"ref_count":  len(refs),
			"scan_count": 0, // KLL path does not scan the blob
		},
	})
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
	qs *QueryStats,
) ([]modules_shared.BlockRef, error) {
	stepStart := time.Now()
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
	qs.ExecutionPath = ExecPathIntrinsicTopKScan
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNameIntrinsic,
		Duration: time.Since(stepStart),
		Metadata: map[string]any{
			"ref_count":  len(refs),
			"scan_count": scanCount,
		},
	})

	// Apply time-range filter: the scan path does not have per-ref timestamps in scope,
	// so we post-filter the selected refs by decoding the timestamp column.
	// selected is bounded by limit (small), so this is cheap.
	if len(selected) > 0 && (opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0) {
		tsCol, tsErr := r.GetIntrinsicColumn(opts.TimestampColumn)
		if tsErr == nil && tsCol != nil && len(tsCol.Uint64Values) == len(tsCol.BlockRefs) {
			// Use LookupRefFast: O(K log N) avoiding per-call allocations
			// vs the old O(N) map build over 3.3M entries.
			// LookupRefFast calls EnsureRefIndex internally.
			filtered := selected[:0]
			for _, ref := range selected {
				packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				val, ok := tsCol.LookupRefFast(packed)
				if !ok {
					continue
				}
				ts := val.(uint64)
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
	qs *QueryStats,
) ([]MatchedRow, error) {
	// Sort refs and apply shard filter (same as collectIntrinsicPlain).
	slices.SortFunc(refs, blockRefCompare)
	refs = filterRefsByShardRange(refs, opts)
	if len(refs) == 0 {
		return nil, nil
	}

	prefilterStart := time.Now()
	blockOrder, blockCandidates := groupRefsByBlock(refs)
	qs.ExecutionPath = ExecPathMixedPlain
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNameMixedPrefilter,
		Duration: time.Since(prefilterStart),
		Metadata: map[string]any{
			"candidate_blocks": len(blockOrder),
		},
	})

	resultsCap := len(refs)
	if opts.Limit > 0 && opts.Limit < resultsCap {
		resultsCap = opts.Limit
	}
	results := make([]MatchedRow, 0, resultsCap)
	// rowSet and preFnErr are shared between preFn (which evaluates the predicate) and fn
	// (which intersects). processGroup is called sequentially so no lock is needed.
	var mixedPlainRowSet vm.RowSet
	var mixedPlainPreFnErr error
	// Coalesce all candidate blocks for efficient batch I/O.
	err := forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectMixedPlain",
		func(pb parsedBlock, candidateRows []int) bool {
			// Re-evaluate the full predicate on the first-pass block to gate second-pass decode.
			provider := newBlockColumnProvider(pb.Block)
			mixedPlainPreFnErr = nil
			if r.HasIntrinsicSection() {
				uap := userAttrProgram(program)
				if uap == nil {
					mixedPlainRowSet = provider.FullScan()
				} else {
					mixedPlainRowSet, mixedPlainPreFnErr = uap.ColumnPredicate(provider)
				}
			} else {
				mixedPlainRowSet, mixedPlainPreFnErr = program.ColumnPredicate(provider)
			}
			if mixedPlainPreFnErr != nil {
				// Treat evaluation errors conservatively: allow fn to run so it can
				// surface the error with full block context.
				return true
			}
			return mixedPlainRowSet.Size() > 0
		},
		func(pb parsedBlock, candidateRows []int) error {
			if mixedPlainPreFnErr != nil {
				return fmt.Errorf("collectMixedPlain ColumnPredicate block %d: %w", pb.BlockIdx, mixedPlainPreFnErr)
			}
			// mixedPlainRowSet was set by preFn; we arrive here only when Size() > 0.
			// Intersect VM result with candidate rows from intrinsic pre-filter.
			for _, rowIdx := range candidateRows {
				if !mixedPlainRowSet.Contains(rowIdx) {
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
	// errLimitReached is consumed inside blockGroupPipeline (translated to nil); err is
	// therefore never errLimitReached here.
	if err != nil {
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
	qs *QueryStats,
) ([]MatchedRow, error) {
	backward := opts.Direction == queryplanner.Backward

	// Apply shard filter.
	refs = filterRefsByShardRange(refs, opts)
	if len(refs) == 0 {
		return nil, nil
	}

	prefilterStart := time.Now()
	blockOrder, blockCandidates := groupRefsByBlock(refs)
	qs.ExecutionPath = ExecPathMixedTopK
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNameMixedPrefilter,
		Duration: time.Since(prefilterStart),
		Metadata: map[string]any{
			"candidate_blocks": len(blockOrder),
		},
	})

	buf := &topKHeap{entries: make([]topKEntry, 0, opts.Limit), backward: backward}

	// rowSet and preFnErr are shared between preFn and fn; processGroup is sequential so no lock needed.
	var mixedTopKRowSet vm.RowSet
	var mixedTopKPreFnErr error
	// Coalesce all candidate blocks for efficient batch I/O.
	if err := forEachBlockInGroups(r, blockOrder, blockCandidates, wantColumns, secondPassCols, "collectMixedTopK",
		func(pb parsedBlock, candidateRows []int) bool {
			// Re-evaluate the full predicate on the first-pass block to gate second-pass decode.
			provider := newBlockColumnProvider(pb.Block)
			mixedTopKPreFnErr = nil
			if r.HasIntrinsicSection() {
				uap := userAttrProgram(program)
				if uap == nil {
					mixedTopKRowSet = provider.FullScan()
				} else {
					mixedTopKRowSet, mixedTopKPreFnErr = uap.ColumnPredicate(provider)
				}
			} else {
				mixedTopKRowSet, mixedTopKPreFnErr = program.ColumnPredicate(provider)
			}
			if mixedTopKPreFnErr != nil {
				// Treat evaluation errors conservatively: allow fn to run so it can
				// surface the error with full block context.
				return true
			}
			return mixedTopKRowSet.Size() > 0
		},
		func(pb parsedBlock, candidateRows []int) error {
			if mixedTopKPreFnErr != nil {
				return fmt.Errorf("collectMixedTopK ColumnPredicate block %d: %w", pb.BlockIdx, mixedTopKPreFnErr)
			}
			// mixedTopKRowSet was set by preFn; we arrive here only when Size() > 0.
			// Collect rows that pass both the pre-filter and full predicate.
			qualifying := make([]int, 0, len(candidateRows))
			for _, rowIdx := range candidateRows {
				if mixedTopKRowSet.Contains(rowIdx) {
					qualifying = append(qualifying, rowIdx)
				}
			}
			if len(qualifying) == 0 {
				return nil
			}

			// Get timestamp column for heap ordering.
			// Falls back to intrinsic section for files that store span:start exclusively there.
			tsCol := pb.Block.GetColumn(opts.TimestampColumn)
			if tsCol == nil {
				topKScanRowsFromIntrinsic(buf, opts.Limit, backward, r, pb.Block, pb.BlockIdx,
					opts.TimestampColumn, opts.TimeRange, qualifying)
			} else {
				topKScanRows(buf, opts.Limit, backward, pb.Block, pb.BlockIdx, tsCol, opts.TimeRange, qualifying)
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return topKDeliver(buf, backward), nil
}

// filterRowSetByIntrinsicNodes filters rowSet against the given predicate nodes,
// evaluating each node against the intrinsic section of the reader.
// Returns a new RowSet containing only rows that satisfy the intrinsic predicates.
// Non-intrinsic leaf nodes in nodes are ignored (rowSatisfiesIntrinsicNodes skips them).
// Designed to be called after block-column ColumnPredicate. With dual storage (restored
// after PR #172 rollback), intrinsic columns are present in block payloads for new files,
// so userAttrProgram no longer strips them and this function is a no-op for those files.
// It is kept as a backward-compatibility safety net for files written between the
// PR #172 merge and this fix, where intrinsic columns (e.g. resource.service.name) were
// absent from block payloads and could only be enforced via the intrinsic section here.
func filterRowSetByIntrinsicNodes(
	r *modules_reader.Reader, blockIdx int, rowSet vm.RowSet, nodes []vm.RangeNode,
) vm.RowSet {
	rows := rowSet.ToSlice()
	if len(rows) == 0 {
		return rowSet
	}
	// Build BlockRef slice for the candidate rows.
	refs := make([]modules_shared.BlockRef, len(rows))
	for i, rowIdx := range rows {
		refs[i] = modules_shared.BlockRef{
			BlockIdx: uint16(blockIdx), //nolint:gosec // bounded by file block count
			RowIdx:   uint16(rowIdx),   //nolint:gosec // bounded by SpanCount
		}
	}
	// Collect which intrinsic columns are needed.
	want := make(map[string]struct{}, 4)
	collectIntrinsicNodeColumns(nodes, want)
	if len(want) == 0 {
		return rowSet // no intrinsic columns in nodes
	}
	fields, err := lookupIntrinsicFields(r, refs, want)
	if err != nil {
		// I/O error reading intrinsic columns: conservative fallback — return all
		// candidates unpruned rather than incorrectly excluding matches.
		slog.Error("filterRowSetByIntrinsicNodes: lookupIntrinsicFields failed, skipping intrinsic filter",
			"err", err)
		return rowSet
	}
	// Build filtered RowSet. Rows without intrinsic data for the wanted columns are
	// treated as "absent" and fail the predicate (absent value != any predicate value).
	filtered := newRowSetWithCap(len(rows))
	for i, rowIdx := range rows {
		if rowSatisfiesIntrinsicNodes(nodes, fields[i]) {
			filtered.Add(rowIdx)
		}
	}
	return filtered
}

// lookupIntrinsicFields reads intrinsic column values for the given refs and returns one
// map[string]any per ref. wantCols limits which columns are loaded — when non-nil only
// columns present in wantCols are fetched (skipping expensive GetIntrinsicColumn calls
// for unwanted columns).
// Pass nil to fetch all intrinsic columns (e.g. FindTraceByID needs every field).
//
// For each requested intrinsic column, GetIntrinsicColumn returns an objectcache-backed
// column where EnsureRefIndex builds a sorted-by-ref lookup table once (O(N log N));
// subsequent lookups use O(log N) binary search per selected ref via LookupRefFast.
// Total per column: O(M log N) for M target refs.
//
// SPEC-ROOT-010: I/O errors must not be silently swallowed. Returns an error if any
// GetIntrinsicColumn call fails so callers can take appropriate action.
func lookupIntrinsicFields(
	r *modules_reader.Reader,
	selected []modules_shared.BlockRef,
	wantCols map[string]struct{},
) ([]map[string]any, error) {
	result := make([]map[string]any, len(selected))
	innerCap := 12
	if wantCols != nil {
		innerCap = len(wantCols)
	}
	for i := range result {
		result[i] = make(map[string]any, innerCap)
	}

	// lookupColumn populates result entries for one intrinsic column using O(log N)
	// binary search. LookupRefFast calls EnsureRefIndex internally.
	lookupColumn := func(colName string, col *modules_shared.IntrinsicColumn) {
		for i, ref := range selected {
			packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
			if val, ok := col.LookupRefFast(packed); ok {
				result[i][colName] = val
			}
		}
	}

	for _, colName := range r.IntrinsicColumnNames() {
		// Skip columns not required by this query. wantCols == nil means "all columns"
		// (used for FindTraceByID and match-all queries that need every field).
		if wantCols != nil {
			if _, needed := wantCols[colName]; !needed {
				continue
			}
		}
		// Use objectcache-backed full decode (GetIntrinsicColumn caches via parsedIntrinsicCache).
		// EnsureRefIndex builds a sorted-by-ref lookup table once (O(N log N), cached in
		// objectcache alongside the column). Subsequent calls use O(log N) binary search
		// per target ref instead of the previous O(N) full-column scan.
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil {
			// SPEC-ROOT-010: I/O errors must not be silently swallowed.
			return nil, fmt.Errorf("lookupIntrinsicFields: GetIntrinsicColumn %q: %w", colName, err)
		}
		if col == nil {
			continue
		}
		lookupColumn(colName, col)
	}

	// span:end is synthesized from span:start + span:duration and is NOT listed in
	// IntrinsicColumnNames() (it has no TOC entry). Handle it explicitly here so that
	// predicates on span:end are evaluated correctly.
	if wantCols == nil {
		col, err := r.GetIntrinsicColumn("span:end")
		if err != nil {
			// SPEC-ROOT-010: I/O errors must not be silently swallowed.
			return nil, fmt.Errorf("lookupIntrinsicFields: GetIntrinsicColumn %q: %w", "span:end", err)
		}
		if col != nil {
			lookupColumn("span:end", col)
		}
	} else if _, needed := wantCols["span:end"]; needed {
		col, err := r.GetIntrinsicColumn("span:end")
		if err != nil {
			// SPEC-ROOT-010: I/O errors must not be silently swallowed.
			return nil, fmt.Errorf("lookupIntrinsicFields: GetIntrinsicColumn %q: %w", "span:end", err)
		}
		if col != nil {
			lookupColumn("span:end", col)
		}
	}

	return result, nil
}
