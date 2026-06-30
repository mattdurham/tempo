package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// NOTE-376: packedRefSet is a dense bitset membership test over packed
// (blockIdx<<16|rowIdx) ref keys, replacing the map[uint32]struct{} match-set in
// collectIntrinsicTopKScan. For a high-cardinality intrinsic equality (e.g. a
// service-name match covering a large fraction of spans), the match set holds M refs —
// up to the full span count of the shard — and was built as a hash map (~48B/entry plus
// per-row hash+probe on the subsequent timestamp blob scan). Packed keys are a bounded
// 32-bit value and, for the single-block query-frontend shard, span the tight contiguous
// range [block<<16, block<<16+maxRow]; offsetting the bitset by the minimum packed key
// sizes it to (maxPacked-minPacked+1) bits (~spanCount/8 bytes) regardless of how high
// the block index sits. Build is a sequential bit-set per ref and membership is a
// branch-free word load + shift — both strictly cheaper than the hashmap on the dominant
// dense path. The backing []uint64 is pooled; on acquire only the in-range word span is
// cleared (the set is a zero-sentinel membership set), so a reused dirty buffer costs only
// the words this call actually addresses.
type packedRefSet struct {
	words  []uint64
	offset uint32 // subtracted from packed key before indexing (== minPacked)
}

var packedRefSetWordsPool = sync.Pool{
	New: func() any {
		s := make([]uint64, 0)
		return &s
	},
}

// packedRefSetWordCap bounds the backing array kept in the pool. A pathologically wide
// packed-key range (sparse refs across many high block indices) could grow the bitset far
// beyond steady-state need; such buffers are dropped on release so the pool footprint stays
// bounded (mirrors the cap-guard discipline used elsewhere, e.g. radixBufCap).
const packedRefSetWordCap = 1 << 20 // 1Mi words = 8 MiB = up to ~64M packed keys

// buildPackedRefSet returns a membership set over the packed keys of refs. The caller MUST
// call release() once it is done probing the set so the backing buffer returns to the pool.
func buildPackedRefSet(refs []modules_shared.BlockRef) packedRefSet {
	if len(refs) == 0 {
		return packedRefSet{}
	}
	minPK := uint32(refs[0].BlockIdx)<<16 | uint32(refs[0].RowIdx)
	maxPK := minPK
	for _, ref := range refs[1:] {
		pk := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
		if pk < minPK {
			minPK = pk
		} else if pk > maxPK {
			maxPK = pk
		}
	}
	span := maxPK - minPK + 1
	nWords := int((span + 63) / 64)

	wp := packedRefSetWordsPool.Get().(*[]uint64)
	if cap(*wp) < nWords {
		*wp = make([]uint64, nWords)
	} else {
		*wp = (*wp)[:nWords]
		// Zero-sentinel set: clear only the words this call addresses; a reused dirty
		// buffer's bits all lie within [0,nWords) since every member maps into that span.
		clear(*wp)
	}
	words := *wp

	for _, ref := range refs {
		pk := (uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)) - minPK
		words[pk>>6] |= 1 << (pk & 63)
	}
	return packedRefSet{words: words, offset: minPK}
}

// contains reports whether packed key pk is a member. pk values outside the built range
// are not members (the offset-relative index would underflow/overrun), so they are gated.
func (s packedRefSet) contains(pk uint32) bool {
	if s.words == nil || pk < s.offset {
		return false
	}
	rel := pk - s.offset
	w := int(rel >> 6)
	if w >= len(s.words) {
		return false
	}
	return s.words[w]>>(rel&63)&1 != 0
}

// release returns the backing buffer to the pool. A zero-value set (nil words) is a no-op.
func (s packedRefSet) release() {
	if s.words == nil {
		return
	}
	if cap(s.words) > packedRefSetWordCap {
		return // drop oversized buffer; let it be GC'd
	}
	w := s.words[:0]
	packedRefSetWordsPool.Put(&w)
}

// errLimitReached is used as an early-stop sentinel inside block-scan pipeline callbacks
// (scanBlocks, forEachBlockInGroups, collectMixedPlain). It signals that the results limit
// has been satisfied. It is never returned to callers; blockGroupPipeline translates it to nil.
var errLimitReached = errors.New("limit reached")

// CollectOptions configures collect execution.

// TimestampColumn is the column for per-row time filtering.
// Empty string disables per-row filtering.

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

// Limit caps the number of returned rows. 0 means no limit (return all matches).
// Negative values are treated as 0 (unlimited) — the executor does not validate sign.

// StartBlock is the first internal block index to include (0-based, inclusive).
// Used by the frontend sharder to partition a single file across multiple jobs.
// 0 with BlockCount==0 means scan all blocks (no sub-file sharding).

// BlockCount is the number of internal blocks to include starting from StartBlock.
// 0 means no sub-file sharding (scan all blocks selected by the planner).

// Direction controls block traversal order. Default (zero value) is Forward.

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

// MatchedRow holds a single row result from Collect.

// IntrinsicFields is set when the result was produced by the intrinsic fast path
// without reading full blocks. The caller should use this for field lookups
// when Block is nil.

// Score is the cosine similarity for VECTOR() queries. Zero for non-vector queries.

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
		return wantColumns, secondPassCols
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
	return wantColumns, secondPassCols
}

// Collect executes program against all blocks in r and returns matched rows.
// SPEC-STREAM-5: Direction is applied at plan time; rows are reversed within each block for Backward.
// SPEC-STREAM-6: QueryStats is returned as the third return value with execution metrics.
// SPEC-OBS-001: ctx is the first parameter to enable OTel context propagation; nil is normalized to Background.
// SPEC-OBS-002: on the planBlocks → scanBlocks path emits blockpack.query/planner/block spans.
func Collect(
	ctx context.Context,
	r *modules_reader.Reader,
	program *vm.Program,
	opts CollectOptions,
) ([]MatchedRow, QueryStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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

	// NOTE-449: top-level query span; child of ctx (inherits Tempo's distributed trace).
	ctx, querySpan := tracer.Start(ctx, "blockpack.query")
	defer querySpan.End()
	if querySpan.IsRecording() {
		querySpan.SetAttributes(
			attribute.Int64("blockpack.query.start_ns", int64(opts.TimeRange.MinNano)), //nolint:gosec
			attribute.Int64("blockpack.query.end_ns", int64(opts.TimeRange.MaxNano)),   //nolint:gosec
			attribute.Int("blockpack.query.limit", opts.Limit),
			attribute.Int("blockpack.query.shard_start", opts.StartBlock),
			attribute.Int("blockpack.query.shard_count", opts.BlockCount),
		)
	}

	queryStart := time.Now()

	wantColumns, secondPassCols := computeColumnFilters(program, opts)

	// NOTE-436: the intrinsic-TOC pre-filter fast paths (match-all top-N and the
	// 4-case collectFromIntrinsicRefs dispatch) were removed. In v2 there is no
	// intrinsic section — all columns live in inner blocks and are queried uniformly
	// by the block scan below (or by the value-index pipeline upstream).

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

	// NOTE-449: emit planner span with pruning counts for OTel distributed traces.
	// NOTE-464 (issue #383): this is the full block-scan path — no row-level candidate bitmap
	// was built, so full_fetch_skipped is false and bitmap_selectivity is omitted (stats nil).
	emitPlannerSpan(ctx, plan, nil)

	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNamePlan,
		Duration: time.Since(planStart),
		Metadata: map[string]any{
			"total_blocks":        plan.TotalBlocks,
			"pruned_by_time":      plan.PrunedByTime,
			"pruned_by_index":     plan.PrunedByIndex,
			metaKeySelectedBlocks: len(plan.SelectedBlocks),
			"explain":             plan.Explain,
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
			ctx,
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
			ctx, r, program, wantColumns, secondPassCols, opts,
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
	// NOTE-472 (issue #393): gate on WantSort, not TimestampColumn. TimestampColumn may be
	// set even for unsorted queries (as the match-all ref source / time-filter column), so
	// the topK heap scan only fires when the caller actually wants timestamp ordering.
	return opts.WantSort && opts.TimestampColumn != "" && opts.Limit > 0 && !program.HasVector
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
	ctx context.Context,
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	opts CollectOptions,
	selectedBlocks []int,
	groups []modules_shared.CoalescedRead,
	results *[]MatchedRow,
) (int, int, int64, error) {
	// Pre-build a blockToGroup index and a groupToBlocks index (maintaining selectedBlocks order)
	// so processGroup iterates only the ~N/G blocks relevant to its group rather than all N.
	// This restores O(N) total work across all groups (O(N/G) per group × G groups).
	// NOTE-105: flat []int replaces map[int]int — block IDs are bounded by r.BlockCount().
	blockCount := r.BlockCount()
	blockToGroupSlice := make([]int, blockCount)
	for i := range blockToGroupSlice {
		blockToGroupSlice[i] = -1
	}
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			if bi < blockCount {
				blockToGroupSlice[bi] = gi
			}
		}
	}
	groupToBlocks := make([][]int, len(groups))
	for _, bi := range selectedBlocks {
		if bi >= blockCount {
			continue
		}
		gi := blockToGroupSlice[bi]
		if gi == -1 {
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
			// NOTE-449: per-block OTel span. Uses a local closure so defer span.End() fires
			// at the end of each iteration, not when processGroup returns (as defer would in a loop).
			if blockErr := func() error {
				_, blockSpan := startBlockSpan(ctx, blockIdx)
				defer blockSpan.End()
				// TODO(NOTE-449): blockpack.block cache attrs (hits/misses/bytes) are zero
				// until CacheStats can be threaded out of ReadGroupColumnar worker goroutines.
				// See NOTE-449 deferred items.
				// Free raw bytes immediately after access — avoids retaining the full
				// coalesced group in memory for the duration of the block scan loop.
				delete(groupRaw, blockIdx)

				meta := r.BlockMeta(blockIdx)
				if blockSpan.IsRecording() {
					blockSpan.SetAttributes(
						attribute.Int64("blockpack.block.length_bytes", int64(meta.Length)), //nolint:gosec
						attribute.Int("blockpack.block.span_count", int(meta.SpanCount)),
					)
				}
				r.ResetInternStrings()

				// NOTE-006: Acquire a pooled intern map for this block's lifetime. The map must
				// remain alive through both parse passes and the entire row-emission loop, because
				// lazy columns (registered during first pass) call decodeNow() during row iteration
				// and reference the intern map. Release after streamSortedRows completes.
				internPtr := modules_reader.AcquireInternMap()
				intern := *internPtr

				bwb, parseErr := r.ParseBlockFromBytesWithIntern(raw, modules_reader.WantOnly(wantColumns), meta, intern)
				if parseErr != nil {
					modules_reader.ReleaseInternMap(internPtr)
					return fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
				}

				// NOTE-102: use pooled provider to avoid per-block heap allocation.
				provider := acquireBlockColumnProvider(bwb.Block)
				// NOTE-436: all predicate columns live in block columns; evaluate the
				// full program directly against them.
				var rowSet vm.RowSet
				var evalErr error
				rowSet, evalErr = program.ColumnPredicate(provider)
				if evalErr != nil {
					releaseBlockColumnProvider(provider)
					modules_reader.ReleaseInternMap(internPtr)
					return fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
				}

				if rowSet.Size() == 0 {
					releaseBlockColumnProvider(provider)
					modules_reader.ReleaseInternMap(internPtr)
					return nil
				}

				// NOTE-436: there is no intrinsic-section post-filter — all predicate
				// columns (intrinsic and attribute alike) are evaluated directly by
				// ColumnPredicate above against the block columns.

				// NOTE-018: Second pass — decode result columns now that we know this block has matches.
				// NOTE-028: secondPassCols is pre-computed above (searchMetaColumns ∪ wantColumns, or nil for all).
				if wantColumns != nil {
					bwb, parseErr = r.ParseBlockFromBytesWithIntern(
						bwb.RawBytes,
						modules_reader.WantOnly(secondPassCols),
						meta,
						intern,
					)
					if parseErr != nil {
						releaseBlockColumnProvider(provider)
						modules_reader.ReleaseInternMap(internPtr)
						return fmt.Errorf("ParseBlockFromBytes (second pass) block %d: %w", blockIdx, parseErr)
					}
				}

				// Vector post-filter: when the query has a VectorScorer, score only the candidate
				// rows (survivors of ColumnPredicate + intrinsic filter) via point lookup.
				// Non-vector queries take the streamSortedRows path unchanged.
				if program.VectorScorer != nil {
					scoredRows := applyVectorScorerToBlock(bwb.Block, program, rowSet)
					releaseBlockColumnProvider(provider)
					modules_reader.ReleaseInternMap(internPtr)
					for _, sr := range scoredRows {
						*results = append(*results, MatchedRow{
							Block:    bwb.Block,
							BlockIdx: blockIdx,
							RowIdx:   sr.RowIdx,
							Score:    sr.Score,
						})
					}
					return nil
				}

				// NOTE: rowSet is not used after ToSlice() — safe to sort in-place without clone.
				// streamSortedRows sorts and reverses rows in-place via slices.SortFunc and index swap,
				// which mutates the backing slice returned by ToSlice(). This is intentional: rowSet
				// is never accessed again (no Contains calls) after this point in scanBlocks.
				// If rowSet reuse is added in future, restore slices.Clone here to preserve the
				// ascending-sorted invariant required by rowSet.Contains.
				// NOTE-107: rows may point into p.scratch; provider must not be released until after
				// streamSortedRows completes, so that no concurrent goroutine can overwrite p.scratch.
				rows := rowSet.ToSlice()

				// SPEC-STREAM-5: Sort rows by per-row timestamp when TimestampColumn is set.
				var tsCol *modules_reader.Column
				if opts.TimestampColumn != "" {
					tsCol = bwb.Block.GetColumn(opts.TimestampColumn)
				}

				stop := streamSortedRows(bwb.Block, blockIdx, rows, tsCol, opts, results)
				releaseBlockColumnProvider(provider)
				// Release intern map after all lazy decodes in streamSortedRows are complete.
				modules_reader.ReleaseInternMap(internPtr)
				if stop {
					return errLimitReached
				}
				return nil
			}(); blockErr != nil {
				return blockErr
			}
		}
		return nil
	}

	// Pass union of wantColumns ∪ secondPassCols so FilterBlockColumns retains bytes for
	// both the first pass (predicate) and second pass (output columns).
	var filterCols map[string]struct{}
	if wantColumns != nil || secondPassCols != nil {
		filterCols = make(map[string]struct{}, len(wantColumns)+len(secondPassCols))
		for k := range wantColumns {
			filterCols[k] = struct{}{}
		}
		for k := range secondPassCols {
			filterCols[k] = struct{}{}
		}
	}
	// SPEC-STREAM-11: concurrent I/O via blockGroupPipeline; processGroup called sequentially.
	// NOTE-449: ctx is now propagated from Collect (resolves NOTE-058).
	return blockGroupPipeline(ctx, r, groups, defaultPipelineWorkers, filterCols, processGroup)
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
	// NOTE-472 (issue #393): sort by per-row timestamp only when the caller requested a
	// sort (WantSort). tsCol may still be non-nil for the time-range filter below even when
	// no sort is requested, so the sort gate is WantSort, not tsCol != nil.
	if tsCol != nil && opts.WantSort {
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

// parsedBlock holds the result of the two-pass parse for one block.
