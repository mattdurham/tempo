package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// Intrinsic fast path for ExecuteTraceMetrics.
// NOTE-046: When all needed columns (span:start, aggregate field, group-by) are in the
// intrinsic section, metrics queries skip full block reads entirely. See NOTES.md NOTE-046.
// See also NOTE-055 for the streamHistogramGroupBy dict-amortization extension.

import (
	"cmp"
	"context"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// ctxCheckInterval is how often (in spans) to check for context cancellation in hot loops.
// Large enough to bound overhead; small enough to bound cancellation latency.
const ctxCheckInterval = 100_000

// metricsColumnsAreIntrinsic reports whether all columns in wantColumns are available
// in this file's intrinsic section, enabling the zero-block-read fast path.
// It checks the file's actual TOC metadata directly, so both standard intrinsic columns
// and dedicated columns written by the writer are eligible. This also handles older files
// that may be missing optional intrinsic columns (they fall back to block scan).
//
// NOTE-067: Uses HasIntrinsicColumn (pure map lookup, zero I/O) rather than
// IntrinsicColumnMeta to avoid triggering a blob read for each column just to
// check existence. With N intrinsic columns per file and M files, IntrinsicColumnMeta
// would issue N×M GCS reads here; HasIntrinsicColumn issues zero.
func metricsColumnsAreIntrinsic(r *modules_reader.Reader, wantColumns map[string]struct{}) bool {
	if !r.HasIntrinsicSection() {
		return false
	}
	for col := range wantColumns {
		if !r.HasIntrinsicColumn(col) {
			return false
		}
	}
	return true
}

// executeTraceMetricsIntrinsic is the zero-block-read fast path for ExecuteTraceMetrics.
// It accumulates metrics directly from the intrinsic column section without reading any
// full blocks.
//
// Map allocation budget (all paths also allocate one buckets map; predicate-filtered paths
// pay a one-time merge-join cost of slices.Clone+[]refIdx per file — see NOTE-070):
//
//	count/rate, no group-by:                0 extra maps; 1 []int64 slice of size numSteps (span:start streamed inline)
//	count/rate, N group-by:                 2 maps (keyToBucket + groupKeyMap) + 1 map[string][]int64 of size numGroups
//	agg field,  no group-by:                1 map  (keyToBucket; aggregate column streamed directly)
//	agg field,  N group-by (histogram):     2 maps (keyToBucket + groupKeyMap; seen inside streamHistogramGroupBy) — NOTE-056 fast path eliminates colVals for single group-by
//	agg field,  N group-by (other):         3 maps (keyToBucket + groupKeyMap + aggVals)
//
// Returns (result, true, nil) when the fast path succeeds.
// Returns (nil, false, nil) when the fast path is not applicable — caller falls through to block scan.
// Returns (nil, false, err) on a hard error.
//
// NOTE-046: fast path is applicable when all wantColumns are intrinsic AND either:
//   - program has no filter predicates (match-all { }): enumerate via span:start flat column.
//   - program has only intrinsic predicates: BlockRefsFromIntrinsicTOC evaluates them.
func executeTraceMetricsIntrinsic(
	ctx context.Context,
	r *modules_reader.Reader,
	program *vm.Program,
	querySpec *vm.QuerySpec,
	wantColumns map[string]struct{},
) (*TraceMetricsResult, bool, error) {
	if !metricsColumnsAreIntrinsic(r, wantColumns) {
		return nil, false, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	tsCol, err := r.GetIntrinsicColumn("span:start")
	if err != nil {
		return nil, false, err
	}
	if tsCol == nil {
		return nil, false, nil
	}

	hasPreds := program != nil && program.Predicates != nil && len(program.Predicates.Nodes) > 0

	// NOTE-070: filteredRefs is applied via merge-join after binary search narrows inRangeRefs.
	var filteredRefs []modules_shared.BlockRef
	if hasPreds {
		filteredRefs = BlockRefsFromIntrinsicTOC(r, program, 0)
		if filteredRefs == nil {
			return nil, false, nil
		}
	}

	tb := querySpec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return &TraceMetricsResult{}, true, nil
	}

	agg := querySpec.Aggregate
	isCountRate := agg.Function == vm.FuncNameCOUNT || agg.Function == vm.FuncNameRATE

	// span:start is a flat (sorted ascending) column. Validate the column before use:
	// BlockRefs and Uint64Values must be parallel arrays of equal length.
	tsVals := tsCol.Uint64Values
	if len(tsVals) != len(tsCol.BlockRefs) {
		// Malformed intrinsic column — fall back to block scan rather than panic.
		return nil, false, nil
	}

	// Binary-search to find the index range that overlaps (StartTime, EndTime].
	// Intervals are right-closed — matches Tempo semantics.
	lo := sort.Search(len(tsVals), func(i int) bool {
		return int64(tsVals[i]) > tb.StartTime //nolint:gosec
	})
	hi := sort.Search(len(tsVals), func(i int) bool {
		return int64(tsVals[i]) > tb.EndTime //nolint:gosec
	})
	inRangeRefs := tsCol.BlockRefs[lo:hi]
	inRangeVals := tsVals[lo:hi]

	// Apply predicate filter via merge-join (NOTE-070).
	// mergeJoinFilteredRefsWithVals sorts both slices by packKey before walking;
	// inRangeRefs is timestamp-sorted (types.go:228), not packKey-sorted.
	if filteredRefs != nil {
		inRangeRefs, inRangeVals = mergeJoinFilteredRefsWithVals(filteredRefs, inRangeRefs, inRangeVals)
		if len(inRangeRefs) == 0 {
			return &TraceMetricsResult{}, true, nil
		}
	}

	buckets := make(map[string]*aggBucketState)
	if err := dispatchIntrinsicAccumulate(ctx, r, tsCol, lo, hi, filteredRefs, isCountRate, inRangeRefs, inRangeVals, querySpec, buckets); err != nil {
		return nil, false, err
	}
	if isCountRate && len(agg.GroupBy) == 0 && len(buckets) == 0 {
		return &TraceMetricsResult{}, true, nil
	}

	result := &TraceMetricsResult{}
	if querySpec.Aggregate.Function == vm.FuncNameHISTOGRAM {
		result.Series = traceHistogramSeries(buckets, querySpec)
	} else {
		result.Series = traceBuildDenseSeries(buckets, querySpec)
	}
	return result, true, nil
}

// dispatchIntrinsicAccumulate routes to the appropriate accumulation path based on query shape.
// Extracted from executeTraceMetricsIntrinsic to keep cyclomatic complexity within gocyclo limit.
// NOTE-089: direct paths (N=0 histogram, N=1) eliminate inRangeRefs materialization and hash maps.
func dispatchIntrinsicAccumulate(
	ctx context.Context,
	r *modules_reader.Reader,
	tsCol *modules_shared.IntrinsicColumn,
	lo, hi int,
	filteredRefs []modules_shared.BlockRef,
	isCountRate bool,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	switch {
	case isCountRate && len(agg.GroupBy) == 0:
		// NOTE-068: flat []int64 hot loop — no per-span string allocs or hash lookups.
		return streamCountRateNoGroupBy(ctx, inRangeRefs, inRangeVals, tb, buckets)
	case filteredRefs == nil && len(agg.GroupBy) == 0 && agg.Function == vm.FuncNameHISTOGRAM:
		// NOTE-089: N=0 no-predicate histogram direct path.
		numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
		if numSteps <= 0 {
			return nil
		}
		return accumulateHistogramDirectN0(ctx, tb, agg, tsCol, numSteps, r, lo, hi, buckets)
	case filteredRefs == nil && len(agg.GroupBy) == 1:
		// NOTE-085/089: N=1 no-predicate direct path.
		ok, err := accumulateIntrinsicBucketsDirect(ctx, r, tsCol, lo, hi, querySpec, buckets)
		if err != nil || ok {
			return err
		}
		return accumulateIntrinsicBucketsViaKeyMap(ctx, r, inRangeRefs, inRangeVals, tb, querySpec, buckets)
	default:
		return accumulateIntrinsicBucketsViaKeyMap(ctx, r, inRangeRefs, inRangeVals, tb, querySpec, buckets)
	}
}

// accumulateIntrinsicBucketsViaKeyMap builds a packKey→timeBucketIndex map from
// inRangeRefs/inRangeVals and delegates to accumulateIntrinsicBuckets.
// Used by the N=1 fallback path and the general N>1/predicate-filtered path to
// avoid duplicating the keyToBucket loop.
func accumulateIntrinsicBucketsViaKeyMap(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	tb vm.TimeBucketSpec,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	keyToBucket := make(map[uint32]int64, len(inRangeRefs))
	for i, ref := range inRangeRefs {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		keyToBucket[pk] = timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
	}
	if len(keyToBucket) == 0 {
		return nil
	}
	return accumulateIntrinsicBuckets(ctx, r, keyToBucket, inRangeRefs, inRangeVals, querySpec, buckets)
}

// packKey packs (blockIdx, rowIdx) into a uint32 for use as a map key.
func packKey(blockIdx, rowIdx uint16) uint32 {
	return uint32(blockIdx)<<16 | uint32(rowIdx)
}

// mergeJoinFilteredRefsWithVals returns the subset of (inRangeRefs, inRangeVals)
// whose packKey appears in filteredRefs.
//
// NOTE-070: Replaces filteredKeys map[uint32]struct{} to eliminate hash allocation
// and O(N_all) hash lookups on the filtered intrinsic path.
//
// Sort invariant: inRangeRefs is timestamp-sorted (types.go:228), not packKey-sorted.
// filteredRefs order is unspecified (intersectBlockRefSets may return any order).
// Both must be sorted by packKey before the two-pointer walk.
//
// Mutation rules:
//   - filteredRefs is cloned before sorting (caller's slice is not modified).
//   - inRangeRefs is NOT sorted in-place (it is a sub-slice of a shared intrinsic
//     column). A sorted index ([]refIdx) is built over original positions so that
//     inRangeVals alignment is preserved.
func mergeJoinFilteredRefsWithVals(
	filteredRefs []modules_shared.BlockRef,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
) (outRefs []modules_shared.BlockRef, outVals []uint64) {
	if len(filteredRefs) == 0 || len(inRangeRefs) == 0 {
		return nil, nil
	}

	// Sort a copy of filteredRefs by packKey (do not mutate caller's slice).
	sortedFilter := slices.Clone(filteredRefs)
	slices.SortFunc(sortedFilter, func(a, b modules_shared.BlockRef) int {
		return cmp.Compare(packKey(a.BlockIdx, a.RowIdx), packKey(b.BlockIdx, b.RowIdx))
	})

	// Build a sorted index over inRangeRefs by packKey.
	// Using an index avoids reordering inRangeVals (which must stay parallel to inRangeRefs).
	type refIdx struct {
		pk  uint32
		pos int
	}
	idx := make([]refIdx, len(inRangeRefs))
	for i, r := range inRangeRefs {
		idx[i] = refIdx{packKey(r.BlockIdx, r.RowIdx), i}
	}
	slices.SortFunc(idx, func(a, b refIdx) int {
		return cmp.Compare(a.pk, b.pk)
	})

	outCap := min(len(sortedFilter), len(idx))
	outRefs = make([]modules_shared.BlockRef, 0, outCap)
	outVals = make([]uint64, 0, outCap)

	fi := 0
	for _, ri := range idx {
		// Advance filteredRefs pointer past any keys smaller than ri.pk.
		for fi < len(sortedFilter) && packKey(sortedFilter[fi].BlockIdx, sortedFilter[fi].RowIdx) < ri.pk {
			fi++
		}
		if fi < len(sortedFilter) && packKey(sortedFilter[fi].BlockIdx, sortedFilter[fi].RowIdx) == ri.pk {
			outRefs = append(outRefs, inRangeRefs[ri.pos])
			outVals = append(outVals, inRangeVals[ri.pos])
		}
	}
	return outRefs, outVals
}

// intrinsicGetOrCreateBucket returns the bucket for compositeKey, creating it if absent.
func intrinsicGetOrCreateBucket(buckets map[string]*aggBucketState, compositeKey string) *aggBucketState {
	b, exists := buckets[compositeKey]
	if !exists {
		b = &aggBucketState{
			min: math.MaxFloat64,
			max: -math.MaxFloat64,
		}
		buckets[compositeKey] = b
	}
	return b
}

// streamCountRateNoGroupBy is the hot loop for count/rate queries with no group-by.
// It accumulates per-step counts into a flat []int64 slice (size = numSteps) rather
// than a map[string]*aggBucketState, eliminating one string allocation and one hash
// lookup per span. After the loop, non-zero entries are written into buckets.
//
// NOTE-068: This is where the 18% ctrlGroupMatchH2 and most of the 28% GC pressure
// originated before this optimization (pprof, 2026-04-16). ~150M iterations for M1.
func streamCountRateNoGroupBy(
	ctx context.Context,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 {
		return nil
	}
	counts := make([]int64, numSteps)
	for i := range inRangeRefs {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ts := int64(inRangeVals[i]) //nolint:gosec
		idx := timeBucketIndex(ts, tb.StartTime, tb.StepSizeNanos)
		if idx >= 0 && idx < numSteps {
			counts[idx]++
		}
	}
	// Populate buckets from non-zero entries only. At most numSteps iterations (e.g. 360
	// for 6h/60s), negligible vs the savings above.
	for idx, c := range counts {
		if c > 0 {
			key := strconv.FormatInt(int64(idx), 10) + "\x00" //nolint:gosec
			intrinsicGetOrCreateBucket(buckets, key).count = c
		}
	}
	return nil
}

// streamCountRateGroupBy is the hot loop for count/rate queries with group-by.
// It accumulates per-(group, step) counts into a map[string][]int64 — one []int64
// slice of size numSteps per unique group value — rather than building a composite
// string key per span.
//
// NOTE-069: The old loop did `FormatInt(bucketIdx)+"\x00"+groupKeyMap[pk]` for every
// span, allocating a new string each time. With ~150M spans for M5 ({} | rate() by
// (kind)), that was ~150M string allocs. The new approach allocates one []int64 per
// unique group value (e.g. 6 for kind, ~200 for service.name), then writes
// counts[bucketIdx]++ — zero per-span allocations in the hot path.
func streamCountRateGroupBy(
	ctx context.Context,
	keyToBucket map[uint32]int64,
	groupKeyMap map[uint32]string,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 {
		return nil
	}
	// groupCounts: group key → per-step count slice. One entry per unique group value.
	groupCounts := make(map[string][]int64)
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		gk := groupKeyMap[pk]
		counts := groupCounts[gk]
		if counts == nil {
			counts = make([]int64, numSteps)
			groupCounts[gk] = counts
		}
		if bucketIdx >= 0 && bucketIdx < numSteps {
			counts[bucketIdx]++
		}
	}
	// Populate buckets from non-zero entries only.
	for gk, counts := range groupCounts {
		for idx, c := range counts {
			if c > 0 {
				key := strconv.FormatInt(int64(idx), 10) + "\x00" + gk //nolint:gosec
				intrinsicGetOrCreateBucket(buckets, key).count = c
			}
		}
	}
	return nil
}

// accumulateIntrinsicBuckets handles queries that need keyToBucket: those with group-by
// columns or an aggregate field. Count/rate with no group-by is handled inline in the
// caller and never reaches this function.
//
// Strategy matrix:
//
//	count/rate, N=1 group-by:     buildDictIdxForRefs + streamByRefSliceCountRate (NOTE-085)
//	  → accumulateCountRateDirect fast path when accumulateIntrinsicBucketsDirect succeeds
//	count/rate, N group-by (N≤8): buildGroupIDMap + streamCountRateGroupByID (NOTE-074 fast path)
//	count/rate, N group-by (N>8): buildGroupKeyMap + streamCountRateGroupBy (string-keyed fallback)
//	agg field,  no group-by:      stream aggregate column against keyToBucket (no aggVals map)
//	agg field,  N=1 group-by (histogram): buildDictIdxForRefs + streamByRefSliceHistogram (NOTE-087/088)
//	  → accumulateHistogramDirect fast path when accumulateIntrinsicBucketsDirect succeeds (NOTE-089)
//	agg field,  N group-by (histogram, N≤8): buildGroupIDMap + streamHistogramGroupByID (NOTE-074)
//	agg field,  N group-by (histogram, N>8): buildGroupKeyMap + streamHistogramGroupBy (fallback)
//	agg field,  N=1 group-by (other): buildDictIdxForRefs + streamByRefSliceAgg (NOTE-085)
//	  → accumulateAggDirect fast path when accumulateIntrinsicBucketsDirect succeeds (NOTE-089)
//	agg field,  N group-by (other, N≤8):    buildGroupIDMap + streamAggGroupByID (NOTE-074)
//	agg field,  N group-by (other, N>8):    buildGroupKeyMap + aggVals iteration (fallback)
func accumulateIntrinsicBuckets(
	ctx context.Context,
	r *modules_reader.Reader,
	keyToBucket map[uint32]int64,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	isCountRate := agg.Function == vm.FuncNameCOUNT || agg.Function == vm.FuncNameRATE

	if len(agg.GroupBy) == 0 {
		// Aggregate field, no group-by: stream the aggregate column directly against
		// keyToBucket. No aggVals map is allocated — values are consumed inline.
		return streamAggColumnNoGroupBy(r, agg, keyToBucket, buckets)
	}

	// NOTE-085: N=1 unified ref-slice path — all aggregate functions use the same dense
	// packKey array to eliminate hash map lookups from the hot accumulation loop.
	if len(agg.GroupBy) == 1 {
		tb := querySpec.TimeBucketing
		numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
		if numSteps <= 0 {
			return nil
		}
		groupByCol, colErr := r.GetIntrinsicColumn(agg.GroupBy[0])
		if colErr != nil {
			return colErr
		}
		dictIdxForRef, dict, dictByPK, maxPK, buildErr := buildDictIdxForRefs(groupByCol, agg.GroupBy[0], inRangeRefs)
		if buildErr != nil {
			return buildErr
		}
		var aggValsForRef []float64
		var aggPresent []bool
		// NOTE-087: HISTOGRAM now scans the aggregate column directly inside streamByRefSliceHistogram
		// using bucketByPK+dictByPK dense arrays — buildAggValsForRef (which allocated ~7.7MB valByPK
		// and performed 300M extra array ops) is skipped for HISTOGRAM.
		// All other non-count/rate functions still use buildAggValsForRef (aggValsForRef/aggPresent).
		if !isCountRate && agg.Function != vm.FuncNameHISTOGRAM {
			var avErr error
			aggValsForRef, aggPresent, avErr = buildAggValsForRef(r, agg.Field, inRangeRefs)
			if avErr != nil {
				return avErr
			}
		}
		return streamByRefSlice(ctx, r, inRangeRefs, inRangeVals, dictIdxForRef, dict, agg, aggValsForRef, aggPresent, dictByPK, maxPK, numSteps, tb, buckets)
	}

	// Try dict-ID fast path for N > 1 dims (N ≤ 8 group-by dims, all intrinsic).
	// NOTE-074: Falls back to string-keyed path for N > 8 only (ok=false, nil error).
	// Hard errors are returned directly — there is no error-triggered fallback.
	// SPEC-ETM-13.1/13.2: transparent to callers — buckets output is byte-identical.
	groupIDMap, dicts, ok, err := buildGroupIDMap(r, agg.GroupBy, keyToBucket)
	if err != nil {
		return err
	}
	if ok {
		if isCountRate {
			return streamCountRateGroupByID(ctx, keyToBucket, groupIDMap, dicts, querySpec.TimeBucketing, buckets)
		}
		if agg.Function == vm.FuncNameHISTOGRAM {
			return streamHistogramGroupByID(ctx, r, agg.Field, keyToBucket, groupIDMap, dicts, buckets)
		}
		return streamAggGroupByID(ctx, r, agg, keyToBucket, groupIDMap, dicts, buckets)
	}

	// Fallback: string-keyed path for N > 8 dims.
	// Queries with group-by: build one composite groupKeyMap by iterating dict/flat
	// column entries directly. This replaces N separate groupVals[i] maps with one.
	groupKeyMap, groupKeyErr := buildGroupKeyMap(r, agg.GroupBy, keyToBucket)
	if groupKeyErr != nil {
		return groupKeyErr
	}

	if isCountRate {
		// count/rate with group-by: see streamCountRateGroupBy.
		// NOTE-069: uses map[string][]int64 keyed by group value to eliminate
		// per-span composite-string allocations (~150M for M5 {} | rate() by (kind)).
		return streamCountRateGroupBy(ctx, keyToBucket, groupKeyMap, querySpec.TimeBucketing, buckets)
	}

	// HISTOGRAM with group-by: single-pass column scan — no aggVals map allocated.
	// NOTE-055: streamHistogramGroupBy amortizes boundary computation across dict entries.
	if agg.Function == vm.FuncNameHISTOGRAM {
		return streamHistogramGroupBy(r, agg.Field, keyToBucket, groupKeyMap, buckets)
	}

	// Other aggregate functions with group-by: three maps total (keyToBucket + groupKeyMap + aggVals).
	aggVals, aggErr := buildAggValsMap(r, agg.Field, keyToBucket)
	if aggErr != nil {
		return aggErr
	}

	for pk, bucketIdx := range keyToBucket {
		groupKey := groupKeyMap[pk]
		compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKey
		// Always create the bucket so the series/group key is emitted even when
		// the aggregate field is absent (matching block-scan: bucket.count==0 → NaN).
		bucket := intrinsicGetOrCreateBucket(buckets, compositeKey)
		if v, ok := aggVals[pk]; ok {
			updateAggBucket(bucket, agg.Function, v)
		}
	}
	return nil
}

// streamAggColumnNoGroupBy accumulates aggregate values by streaming the aggregate
// intrinsic column directly against keyToBucket. No aggVals map is allocated.
// Handles both flat (uint64) and dict (int64/string) column formats.
//
// For HISTOGRAM: spans with an absent or non-positive aggregate field are counted into
// the boundary-0 bucket, matching the block-scan path (traceHistogramBucket returns 0
// for absent/non-positive values and the span is still counted).
// For other aggregates: spans with an absent field are skipped, matching traceFieldFloat64
// which returns (0, false) for absent columns.
func streamAggColumnNoGroupBy(
	r *modules_reader.Reader,
	agg vm.AggregateSpec,
	keyToBucket map[uint32]int64,
	buckets map[string]*aggBucketState,
) error {
	isHistogram := agg.Function == vm.FuncNameHISTOGRAM

	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}
	if col == nil {
		if isHistogram {
			// All spans have absent field → all go into boundary-0 bucket.
			for _, bucketIdx := range keyToBucket {
				key := strconv.FormatInt(bucketIdx, 10) + "\x00\x000"
				intrinsicGetOrCreateBucket(buckets, key).count++
			}
		} else {
			// Non-histogram: create buckets with count==0 so traceBuildDenseSeries emits
			// NaN values, matching block-scan behavior for spans with absent agg fields.
			for _, bucketIdx := range keyToBucket {
				key := strconv.FormatInt(bucketIdx, 10) + "\x00"
				intrinsicGetOrCreateBucket(buckets, key)
			}
		}
		return nil
	}

	// Track which pks were covered by the column for histogram absent-row handling.
	var covered map[uint32]struct{}
	if isHistogram {
		covered = make(map[uint32]struct{}, len(keyToBucket))
	}

	switch col.Format {
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			bucketIdx, ok := keyToBucket[pk]
			if !ok {
				continue
			}
			v := float64(col.Uint64Values[i])
			if isHistogram {
				covered[pk] = struct{}{}
				boundary := intrinsicHistogramBoundary(v, agg.Field)
				key := strconv.FormatInt(bucketIdx, 10) + "\x00\x00" +
					strconv.FormatFloat(boundary, 'g', -1, 64)
				intrinsicGetOrCreateBucket(buckets, key).count++
			} else {
				key := strconv.FormatInt(bucketIdx, 10) + "\x00"
				updateAggBucket(intrinsicGetOrCreateBucket(buckets, key), agg.Function, v)
			}
		}
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					// Non-numeric dict entry: skip, matching block-scan path which
					// returns (0, false) for non-numeric columns.
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				bucketIdx, ok := keyToBucket[pk]
				if !ok {
					continue
				}
				if isHistogram {
					covered[pk] = struct{}{}
					boundary := intrinsicHistogramBoundary(v, agg.Field)
					key := strconv.FormatInt(bucketIdx, 10) + "\x00\x00" +
						strconv.FormatFloat(boundary, 'g', -1, 64)
					intrinsicGetOrCreateBucket(buckets, key).count++
				} else {
					key := strconv.FormatInt(bucketIdx, 10) + "\x00"
					updateAggBucket(intrinsicGetOrCreateBucket(buckets, key), agg.Function, v)
				}
			}
		}
	}

	// For HISTOGRAM: count any in-range spans absent from the aggregate column into
	// boundary-0 bucket, matching traceHistogramBucket which returns 0 for absent fields.
	if isHistogram {
		for pk, bucketIdx := range keyToBucket {
			if _, ok := covered[pk]; !ok {
				key := strconv.FormatInt(bucketIdx, 10) + "\x00\x000"
				intrinsicGetOrCreateBucket(buckets, key).count++
			}
		}
	}
	return nil
}

// intrinsicInt64ColToString converts a stored int64 enum value to its canonical OTel string
// name for span:kind and span:status columns. All other columns fall back to strconv.FormatInt
// to preserve existing behavior.
// NOTE-083: enum resolution at emit time — no data migration needed.
func intrinsicInt64ColToString(colName string, v int64) string {
	switch colName {
	case colNameSpanKind:
		switch v {
		case 0:
			return "unspecified"
		case 1:
			return "internal"
		case 2:
			return "server"
		case 3:
			return "client"
		case 4:
			return "producer"
		case 5:
			return "consumer"
		}
	case colNameSpanStatus:
		switch v {
		case 0:
			// 0=unset: writer may omit status=0 entries but the fallback is still defined.
			return "unset"
		case 1:
			return "ok"
		case 2:
			return "error"
		}
	}
	return strconv.FormatInt(v, 10)
}

// scanIntrinsicColVals scans a single intrinsic column's dict/flat entries and writes
// packKey → string value into dst for every ref that appears in keyToBucket.
// Rows absent from the column are not written (callers handle absence as empty string).
func scanIntrinsicColVals(col *modules_shared.IntrinsicColumn, colName string, keyToBucket map[uint32]int64, dst map[uint32]string) {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			val := entry.Value
			if val == "" {
				val = intrinsicInt64ColToString(colName, entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if _, ok := keyToBucket[pk]; ok {
					dst[pk] = val
				}
			}
		}
	// Flat-format columns (e.g. span:start, span:duration) store numeric values, not enums.
	// intrinsicInt64ColToString is not applied here because flat values are always numeric,
	// not OTel enum integers. Enum columns are always stored in dict format.
	case modules_shared.IntrinsicFormatFlat:
		for j, ref := range col.BlockRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if _, ok := keyToBucket[pk]; !ok {
				continue
			}
			var val string
			if j < len(col.Uint64Values) {
				val = strconv.FormatUint(col.Uint64Values[j], 10)
			} else if j < len(col.BytesValues) {
				val = string(col.BytesValues[j])
				// else: val stays empty — row absent from this column (Tempo convention)
			}
			dst[pk] = val
		}
	}
}

// maxGroupByDimsFastPath is the maximum number of group-by columns for the dict-ID
// fast path. Queries with more dimensions fall back to the string-keyed path.
// [8]uint32 = 32 bytes, fits in one cache line.
// NOTE-074: SPEC-ETM-13.2 — used by buildGroupIDMap (task #6).
const maxGroupByDimsFastPath = 8

// groupIDKey encodes up to 8 group-by dimension dict IDs as a comparable map key.
// dims[i] = 0 means "absent / empty string" (sentinel). Dict IDs for present rows
// start at 1 (index 0 in each column's dicts slice is reserved for the empty string).
// SPEC-ETM-13: Dict-ID group map fast path invariants.
type groupIDKey [maxGroupByDimsFastPath]uint32

// scanIntrinsicColDictIDs scans a single intrinsic column and writes
// packKey → uint32 dict-entry index into dst. Index 0 is reserved for the
// absent/empty-string sentinel; present dict entries start at index 1.
// The dict slice passed in must already have "" pre-appended at index 0.
// Returns the populated dict slice (may grow).
// NOTE-074: Dict-ID fast path scanner — called by buildGroupIDMap (task #6).
func scanIntrinsicColDictIDs(
	col *modules_shared.IntrinsicColumn,
	colName string,
	keyToBucket map[uint32]int64,
	dst map[uint32]uint32,
	dict []string,
) []string {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			// Only add this dict entry if at least one of its refs is in keyToBucket.
			// This avoids allocating dict slots for values not present in the query window.
			val := entry.Value
			if val == "" {
				val = intrinsicInt64ColToString(colName, entry.Int64Val)
			}
			var dictIdx uint32
			assigned := false
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if _, ok := keyToBucket[pk]; ok {
					if !assigned {
						dictIdx = uint32(len(dict)) //nolint:gosec
						dict = append(dict, val)
						assigned = true
					}
					dst[pk] = dictIdx
				}
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		seen := make(map[string]uint32)
		for j, ref := range col.BlockRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if _, ok := keyToBucket[pk]; !ok {
				continue
			}
			var val string
			if j < len(col.Uint64Values) {
				val = strconv.FormatUint(col.Uint64Values[j], 10)
			} else if j < len(col.BytesValues) {
				val = string(col.BytesValues[j])
			}
			// Use absent-sentinel (index 0, "") for empty values so they map to
			// the same groupIDKey slot as absent rows, not a separate dict entry.
			if val == "" {
				continue
			}
			id, exists := seen[val]
			if !exists {
				id = uint32(len(dict)) //nolint:gosec
				dict = append(dict, val)
				seen[val] = id
			}
			dst[pk] = id
		}
	}
	return dict
}

// buildGroupIDMap builds a packKey → groupIDKey map for the dict-ID fast path.
// Returns (idMap, dicts, true, nil) on success where dicts[i] maps dictIdx → string value
// for the i-th group-by column (used at series-emit time to resolve IDs back to strings).
// Returns (nil, nil, false, nil) when len(groupBy) > maxGroupByDimsFastPath (caller falls
// back to buildGroupKeyMap).
//
// NOTE-074: Replaces buildGroupKeyMap for the intrinsic fast path. See NOTES.md.
// SPEC-ETM-13.1/13.2: fast path for N≤8; fallback for N>8.
func buildGroupIDMap(
	r *modules_reader.Reader,
	groupBy []string,
	keyToBucket map[uint32]int64,
) (map[uint32]groupIDKey, [][]string, bool, error) {
	// NOTE-082: N=1 is handled by buildGroupIDMapSingle in accumulateIntrinsicBuckets
	// before this function is called, so only N>1 cases reach here.
	if len(groupBy) > maxGroupByDimsFastPath {
		return nil, nil, false, nil
	}

	// Multi-group-by path (N ≥ 2).
	out := make(map[uint32]groupIDKey, len(keyToBucket))
	dicts := make([][]string, len(groupBy))

	// Initialize all pks to zero-key (all dims = 0 = empty/absent sentinel).
	for pk := range keyToBucket {
		out[pk] = groupIDKey{}
	}

	for i, colName := range groupBy {
		dict := []string{""}
		colMap := make(map[uint32]uint32, len(keyToBucket))
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil {
			return nil, nil, false, err
		}
		if col != nil {
			dict = scanIntrinsicColDictIDs(col, colName, keyToBucket, colMap, dict)
		}
		// Apply dim i to each pk's key.
		for pk, key := range out {
			if id, ok := colMap[pk]; ok {
				key[i] = id
				out[pk] = key
			}
		}
		dicts[i] = dict
	}
	return out, dicts, true, nil
}

// buildGroupIDMapSingle builds a packKey → dictIdx map for the N=1 group-by fast path.
// Returns map[uint32]uint32 (pk → single dict ID) instead of map[uint32]groupIDKey,
// eliminating the [8]uint32 wrapper and its 32-byte memcmp overhead on every span.
// NOTE-082: replaces buildGroupIDMap for N=1 — called from accumulateIntrinsicBuckets.
func buildGroupIDMapSingle(
	r *modules_reader.Reader,
	colName string,
	keyToBucket map[uint32]int64,
) (map[uint32]uint32, []string, error) {
	dict := []string{""}
	colMap := make(map[uint32]uint32, len(keyToBucket))
	col, err := r.GetIntrinsicColumn(colName)
	if err != nil {
		return nil, nil, err
	}
	if col != nil {
		dict = scanIntrinsicColDictIDs(col, colName, keyToBucket, colMap, dict)
	}
	return colMap, dict, nil
}

// buildDictIdxForRefs builds a []uint32 parallel to inRangeRefs by scanning the group-by
// column's dict entries into a dense array indexed by packKey. The array is bounded by
// MaxSpans per file, so all array ops are L3-cache-resident — far faster than probing a
// hash map for each span.
// NOTE-085: replaces hash map ops with direct array access; zero map lookups in the hot path.
// Returns dictIdxForRef (parallel to inRangeRefs), dict (string values), dictByPK (dense
// packKey→dictIdx+1 array, 0=absent), and maxPK (upper bound of valid packKeys in inRangeRefs).
// dictByPK and maxPK are available for callers that need packKey-indexed group lookup.
func buildDictIdxForRefs(
	col *modules_shared.IntrinsicColumn,
	colName string,
	inRangeRefs []modules_shared.BlockRef,
) (dictIdxForRef []uint32, dict []string, dictByPK []uint32, maxPK uint32, err error) {
	dictIdxForRef = make([]uint32, len(inRangeRefs))
	dict = []string{""}

	if len(inRangeRefs) == 0 {
		return dictIdxForRef, dict, nil, 0, nil
	}

	// Find the max packKey to size the dense array. packKey = (blockIdx<<16)|rowIdx.
	// Computed before the nil col check so callers always receive a properly-sized
	// dictByPK (all zeros = all absent) even when the column is missing.
	for _, ref := range inRangeRefs {
		if pk := packKey(ref.BlockIdx, ref.RowIdx); pk > maxPK {
			maxPK = pk
		}
	}
	// Dense array: dictByPK[pk] = dictIdx+1 (0 = absent sentinel).
	dictByPK = make([]uint32, maxPK+1) //nolint:gosec

	if col == nil {
		// Column absent: all spans map to the empty-string group (dictByPK stays all zeros).
		return dictIdxForRef, dict, dictByPK, maxPK, nil
	}

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		// Scan ALL dict entries → array writes (cache-friendly: array fits in L3).
		for _, entry := range col.DictEntries {
			val := entry.Value
			if val == "" {
				val = intrinsicInt64ColToString(colName, entry.Int64Val)
			}
			if val == "" {
				continue
			}
			dictIdx := uint32(len(dict)) //nolint:gosec
			dictAssigned := false
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				// pk <= maxPK is intentional: pks from blocks outside the query time range are
				// included but bucketByPK[pk] will be 0 for those, so they are skipped
				// during accumulation and never contribute to any bucket. This may inflate
				// the dict slightly but avoids a second pass over BlockRefs.
				if pk <= maxPK {
					if !dictAssigned {
						dict = append(dict, val)
						dictAssigned = true
					}
					dictByPK[pk] = dictIdx + 1
				}
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		// Flat columns: build val→idx mapping on the fly.
		valToIdx := make(map[string]uint32, 16)
		for i, ref := range col.BlockRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk > maxPK {
				continue
			}
			var val string
			if i < len(col.Uint64Values) {
				val = strconv.FormatUint(col.Uint64Values[i], 10)
			} else if i < len(col.BytesValues) {
				val = string(col.BytesValues[i])
			}
			if val == "" {
				continue
			}
			idx, ok := valToIdx[val]
			if !ok {
				idx = uint32(len(dict)) //nolint:gosec
				dict = append(dict, val)
				valToIdx[val] = idx
			}
			dictByPK[pk] = idx + 1
		}
	}

	// Single pass: array read per ref — zero hash map lookups.
	for i, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if raw := dictByPK[pk]; raw > 0 {
			dictIdxForRef[i] = raw - 1
		}
		// else: absent, leave dictIdxForRef[i]=0 (empty-string group)
	}
	return dictIdxForRef, dict, dictByPK, maxPK, nil
}

// accumulateIntrinsicBucketsDirect is the no-predicate N=1 fast path.
// Builds bucketByPK directly from the span:start column slice [lo, hi] without
// materializing inRangeRefs/inRangeVals/dictIdxForRef. Returns (true, nil) on
// success, (false, nil) when the group-by column is flat-format (caller falls back).
// NOTE-085: eliminates ~3GB of intermediate allocations per file for common queries.
func accumulateIntrinsicBucketsDirect(
	ctx context.Context,
	r *modules_reader.Reader,
	tsCol *modules_shared.IntrinsicColumn,
	lo, hi int,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) (bool, error) {
	agg := querySpec.Aggregate
	isCountRate := agg.Function == vm.FuncNameCOUNT || agg.Function == vm.FuncNameRATE
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 {
		return true, nil
	}

	if lo >= hi {
		return true, nil // no refs in time range
	}

	// Find maxPK from the time-range slice (single sequential scan, no array).
	var maxPK uint32
	for _, ref := range tsCol.BlockRefs[lo:hi] {
		if pk := packKey(ref.BlockIdx, ref.RowIdx); pk > maxPK {
			maxPK = pk
		}
	}

	// Build bucketByPK directly — no inRangeRefs materialized.
	// Always allocate even when maxPK==0 (packKey=0 is a valid span key).
	bucketByPK := make([]int64, maxPK+1) //nolint:gosec
	inRangeCount := 0
	for i, ref := range tsCol.BlockRefs[lo:hi] {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if pk > maxPK {
			continue
		}
		bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = bk + 1
			inRangeCount++
		}
	}
	if inRangeCount == 0 {
		return true, nil
	}

	// Get the single group-by column.
	groupByCol, err := r.GetIntrinsicColumn(agg.GroupBy[0])
	if err != nil {
		return false, err
	}
	if groupByCol == nil || groupByCol.Format != modules_shared.IntrinsicFormatDict {
		return false, nil // flat-format group-by: fall back
	}

	// Build dict and dictByPK from group-by column.
	dict := []string{""}
	dictByPK := make([]uint32, maxPK+1) //nolint:gosec
	for _, entry := range groupByCol.DictEntries {
		val := entry.Value
		if val == "" {
			val = intrinsicInt64ColToString(agg.GroupBy[0], entry.Int64Val)
		}
		if val == "" {
			continue
		}
		dictIdx := uint32(len(dict)) //nolint:gosec
		assigned := false
		for _, ref := range entry.BlockRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk <= maxPK {
				if !assigned {
					dict = append(dict, val)
					assigned = true
				}
				dictByPK[pk] = dictIdx + 1
			}
		}
	}

	if isCountRate {
		return true, accumulateCountRateDirect(ctx, groupByCol, dictByPK, bucketByPK, maxPK, inRangeCount, dict, numSteps, buckets)
	}
	// NOTE-089: HISTOGRAM and general agg now handled by direct column scan.
	if agg.Function == vm.FuncNameHISTOGRAM {
		return true, accumulateHistogramDirect(ctx, r, agg, dictByPK, bucketByPK, maxPK, dict, numSteps, tb, buckets)
	}
	return true, accumulateAggDirect(ctx, r, agg, dictByPK, bucketByPK, maxPK, dict, numSteps, buckets)
}

// accumulateCountRateDirect scans the group-by dict entries directly and accumulates
// count/rate into a 2D slice. No inRangeRefs, no dictIdxForRef — zero span storage.
// NOTE-085: single pass, all array ops, no hash maps.
func accumulateCountRateDirect(
	ctx context.Context,
	groupByCol *modules_shared.IntrinsicColumn,
	dictByPK []uint32,
	bucketByPK []int64,
	maxPK uint32,
	inRangeCount int,
	dict []string,
	numSteps int64,
	buckets map[string]*aggBucketState,
) error {
	numGroups := len(dict)
	// Pre-allocate 2D: groupCounts[groupIdx][timeIdx].
	groupCounts := make([][]int64, numGroups)
	for i := range groupCounts {
		groupCounts[i] = make([]int64, numSteps)
	}
	seenByPK := make([]bool, maxPK+1) //nolint:gosec
	seenCount := 0

	// Scan dict entries: compute once per entry, accumulate per matching ref.
	spanCount := 0
	for _, entry := range groupByCol.DictEntries {
		// Find this entry's dictIdx in dict (it was assigned during build).
		// Use dictByPK to get the group index for any ref in this entry.
		for _, ref := range entry.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk > maxPK {
				continue
			}
			bk := bucketByPK[pk]
			if bk == 0 {
				continue
			}
			if !seenByPK[pk] {
				seenByPK[pk] = true
				seenCount++
			}
			var gIdx uint32
			if raw := dictByPK[pk]; raw > 0 {
				gIdx = raw - 1
			}
			if int(gIdx) < numGroups { //nolint:gosec
				groupCounts[gIdx][bk-1]++
			}
		}
	}

	// Absent-row pass: pks in time range not seen in any dict entry → empty-string group.
	if seenCount < inRangeCount {
		for pk, bk := range bucketByPK {
			if bk == 0 || seenByPK[pk] {
				continue
			}
			groupCounts[0][bk-1]++
		}
	}

	// Emit.
	for gIdx, counts := range groupCounts {
		gk := ""
		if gIdx < len(dict) {
			gk = dict[gIdx]
		}
		hasAny := false
		for _, c := range counts {
			if c > 0 {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}
		for timeIdx, c := range counts {
			if c == 0 {
				continue
			}
			k := strconv.FormatInt(int64(timeIdx), 10) + "\x00" + gk //nolint:gosec
			intrinsicGetOrCreateBucket(buckets, k).count += c
		}
	}
	return nil
}

// accumulateHistogramDirect is the direct-path HISTOGRAM accumulator for N=1 group-by queries.
// Receives pre-built dictByPK and bucketByPK from accumulateIntrinsicBucketsDirect — avoids
// rebuilding bucketByPK from inRangeRefs (eliminates O(inRangeCount) redundant array writes).
// Absent-row pass walks bucketByPK directly, matching accumulateCountRateDirect's pattern.
// NOTE-089: extends accumulateIntrinsicBucketsDirect to HISTOGRAM without inRangeRefs.
func accumulateHistogramDirect(
	ctx context.Context,
	r *modules_reader.Reader,
	agg vm.AggregateSpec,
	dictByPK []uint32,
	bucketByPK []int64,
	maxPK uint32,
	dict []string,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	numGroups := len(dict)
	stride2 := numSteps
	stride1 := int64(histFlatStride) * numSteps
	groupCountsFlat := make([]int64, int64(numGroups)*stride1) //nolint:gosec

	boundaryCache := make(map[float64]int64, 32)
	boundaries := make([]float64, 0, 32)

	getBoundaryIdx := func(v float64) int64 {
		b := intrinsicHistogramBoundary(v, agg.Field)
		idx, ok := boundaryCache[b]
		if !ok {
			if len(boundaries) >= histFlatStride {
				idx = int64(histFlatStride)
				boundaryCache[b] = idx
				return idx
			}
			boundaries = append(boundaries, b)
			idx = int64(len(boundaries))
			boundaryCache[b] = idx
		}
		return idx
	}

	seenByPK := make([]bool, maxPK+1) //nolint:gosec

	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}
	if col != nil {
		if err := streamByRefSliceHistogramScanDict(ctx, col, bucketByPK, dictByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride1, stride2); err != nil {
			return err
		}
	}

	// Absent-row pass: walk bucketByPK directly (no inRangeRefs) — bIdx=0 sentinel.
	for pk, bk := range bucketByPK {
		if bk == 0 || seenByPK[pk] {
			continue
		}
		var gIdx int64
		if raw := dictByPK[pk]; raw > 0 {
			gIdx = int64(raw - 1) //nolint:gosec
		}
		if gIdx >= int64(numGroups) { //nolint:gosec
			continue
		}
		groupCountsFlat[gIdx*stride1+(bk-1)]++
	}

	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, numGroups, dict, boundaries, buckets)
}

// accumulateHistogramDirectN0 is the direct-path HISTOGRAM accumulator for N=0 (no group-by) queries.
// Eliminates inRangeRefs materialization by building bucketByPK directly from tsCol.BlockRefs[lo:hi]
// and scanning the histogram agg column with a single dense array pass — no keyToBucket hash map.
//
// Layout: groupCountsFlat[bIdx*numSteps + (bk-1)] (single group, gIdx always 0).
// bIdx=0 is the absent/boundary-0 sentinel; bIdx=1..N are actual boundaries.
// stride1 = histFlatStride*numSteps (group stride, numGroups=1 so outer loop is trivial).
// stride2 = numSteps (boundary stride).
//
// NOTE-089: extends the direct-path pattern (accumulateHistogramDirect) to N=0 histogram queries,
// eliminating the streamAggColumnNoGroupBy map path for {} | histogram_over_time(duration).
func accumulateHistogramDirectN0(
	ctx context.Context,
	tb vm.TimeBucketSpec,
	agg vm.AggregateSpec,
	tsCol *modules_shared.IntrinsicColumn,
	numSteps int64,
	r *modules_reader.Reader,
	lo, hi int,
	buckets map[string]*aggBucketState,
) error {
	if lo >= hi {
		return nil
	}

	// Step 1: find maxPK from the time-range slice.
	var maxPK uint32
	for _, ref := range tsCol.BlockRefs[lo:hi] {
		if pk := packKey(ref.BlockIdx, ref.RowIdx); pk > maxPK {
			maxPK = pk
		}
	}

	// Step 2: build bucketByPK directly — no inRangeRefs materialized.
	// bucketByPK[pk] = timeBucketIndex+1; 0 = out of range (sentinel).
	bucketByPK := make([]int64, maxPK+1) //nolint:gosec
	for i, ref := range tsCol.BlockRefs[lo:hi] {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = bk + 1
		}
	}

	// Step 3: pre-allocate flat accumulator for 1 group.
	// Layout: groupCountsFlat[bIdx*numSteps + timeIdx] (gIdx always 0).
	stride2 := numSteps
	stride1 := int64(histFlatStride) * numSteps
	groupCountsFlat := make([]int64, stride1) //nolint:gosec

	// Step 4: build boundary cache and getBoundaryIdx closure — same pattern as accumulateHistogramDirect.
	boundaryCache := make(map[float64]int64, 32)
	boundaries := make([]float64, 0, 32)

	getBoundaryIdx := func(v float64) int64 {
		b := intrinsicHistogramBoundary(v, agg.Field)
		idx, ok := boundaryCache[b]
		if !ok {
			if len(boundaries) >= histFlatStride {
				idx = int64(histFlatStride)
				boundaryCache[b] = idx
				return idx
			}
			boundaries = append(boundaries, b)
			idx = int64(len(boundaries))
			boundaryCache[b] = idx
		}
		return idx
	}

	seenByPK := make([]bool, maxPK+1) //nolint:gosec

	// Step 5: scan the histogram agg column using the N=0 specialized scanner.
	// N=0: gIdx is always 0 — no dictByPK allocation or lookup needed.
	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}
	if col != nil {
		if err := scanHistogramN0(ctx, col, bucketByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride2); err != nil {
			return err
		}
	}

	// Step 6: absent-row pass — walk bucketByPK for pks not seen in the agg column.
	// bIdx=0 sentinel: groupCountsFlat[0*stride2 + (bk-1)] = groupCountsFlat[bk-1].
	for pk, bk := range bucketByPK {
		if bk == 0 || seenByPK[pk] {
			continue
		}
		groupCountsFlat[bk-1]++
	}

	// Step 7: emit — single group, dict = [""], numGroups = 1.
	dict := []string{""}
	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, 1, dict, boundaries, buckets)
}

// accumulateAggDirect is the direct-path general agg accumulator (SUM/AVG/MIN/MAX/STDDEV/QUANTILE)
// for N=1 group-by queries. Scans the agg column directly using dictByPK/bucketByPK dense arrays,
// eliminating buildAggValsForRef (7.7 MB), inRangeRefs materialization, and the 150 M hash map
// probes of streamByRefSliceAgg.
// NOTE-089: implements direct agg accumulation matching accumulateCountRateDirect's pattern.
func accumulateAggDirect(
	ctx context.Context,
	r *modules_reader.Reader,
	agg vm.AggregateSpec,
	dictByPK []uint32,
	bucketByPK []int64,
	maxPK uint32,
	dict []string,
	numSteps int64,
	buckets map[string]*aggBucketState,
) error {
	numGroups := len(dict)
	groupBuckets := make([][]*aggBucketState, numGroups)
	for i := range groupBuckets {
		groupBuckets[i] = make([]*aggBucketState, numSteps)
	}
	seenByPK := make([]bool, maxPK+1) //nolint:gosec

	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}
	if col != nil {
		if err := accumulateAggDirectScanCol(ctx, col, agg.Function, dictByPK, bucketByPK, maxPK, numSteps, numGroups, groupBuckets, seenByPK); err != nil {
			return err
		}
	}

	// Absent-row pass: create count=0 bucket for in-range spans with no agg value,
	// matching streamByRefSliceAgg's NaN-emit behavior.
	for pk, bk := range bucketByPK {
		if bk == 0 || seenByPK[pk] {
			continue
		}
		var gIdx int
		if raw := dictByPK[pk]; raw > 0 {
			gIdx = int(raw - 1) //nolint:gosec
		}
		if gIdx < numGroups && groupBuckets[gIdx][bk-1] == nil {
			groupBuckets[gIdx][bk-1] = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
		}
	}

	for gIdx, row := range groupBuckets {
		gk := ""
		if gIdx < len(dict) {
			gk = dict[gIdx]
		}
		for timeIdx, bucket := range row {
			if bucket == nil {
				continue
			}
			k := strconv.FormatInt(int64(timeIdx), 10) + "\x00" + gk //nolint:gosec
			buckets[k] = bucket
		}
	}
	return nil
}

// accumulateAggDirectScanCol scans an intrinsic column and accumulates directly into groupBuckets.
// Extracted to keep accumulateAggDirect cyclomatic complexity bounded.
// seenByPK[pk] is set true for every pk that appears in the column and is in time range.
func accumulateAggDirectScanCol(
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	fn string,
	dictByPK []uint32,
	bucketByPK []int64,
	maxPK uint32,
	numSteps int64,
	numGroups int,
	groupBuckets [][]*aggBucketState,
	seenByPK []bool,
) error {
	spanCount := 0
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var fval float64
			if entry.Value != "" {
				// Match buildAggValsForRef: parse failure keeps fval=0 and still marks spans present.
				if v, parseErr := strconv.ParseFloat(entry.Value, 64); parseErr == nil {
					fval = v
				}
			} else {
				fval = float64(entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk > maxPK {
					continue
				}
				bk := bucketByPK[pk]
				if bk == 0 {
					continue
				}
				seenByPK[pk] = true
				var gIdx int
				if raw := dictByPK[pk]; raw > 0 {
					gIdx = int(raw - 1) //nolint:gosec
				}
				if gIdx >= numGroups || bk-1 >= numSteps {
					continue
				}
				if groupBuckets[gIdx][bk-1] == nil {
					groupBuckets[gIdx][bk-1] = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
				}
				updateAggBucket(groupBuckets[gIdx][bk-1], fn, fval)
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk > maxPK {
				continue
			}
			bk := bucketByPK[pk]
			if bk == 0 {
				continue
			}
			seenByPK[pk] = true
			var gIdx int
			if raw := dictByPK[pk]; raw > 0 {
				gIdx = int(raw - 1) //nolint:gosec
			}
			if gIdx >= numGroups || bk-1 >= numSteps {
				continue
			}
			if groupBuckets[gIdx][bk-1] == nil {
				groupBuckets[gIdx][bk-1] = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
			}
			updateAggBucket(groupBuckets[gIdx][bk-1], fn, float64(col.Uint64Values[i])) //nolint:gosec
		}
	}
	return nil
}

// buildAggValsForRef builds a []float64 parallel to inRangeRefs containing the aggregate
// field value for each ref, using the same dense packKey array as buildDictIdxForRefs.
// aggPresent[i]=false means the span is absent from the aggregate column.
// NOTE-085: eliminates aggVals map[uint32]float64 from the accumulation hot path.
func buildAggValsForRef(
	r *modules_reader.Reader,
	fieldName string,
	inRangeRefs []modules_shared.BlockRef,
) ([]float64, []bool, error) {
	aggVals := make([]float64, len(inRangeRefs))
	aggPresent := make([]bool, len(inRangeRefs))
	if fieldName == "" {
		return aggVals, aggPresent, nil
	}
	col, err := r.GetIntrinsicColumn(fieldName)
	if err != nil || col == nil {
		return aggVals, aggPresent, err
	}
	var maxPK uint32
	for _, ref := range inRangeRefs {
		if pk := packKey(ref.BlockIdx, ref.RowIdx); pk > maxPK {
			maxPK = pk
		}
	}
	valByPK := make([]float64, maxPK+1) //nolint:gosec
	hasByPK := make([]bool, maxPK+1)    //nolint:gosec
	switch col.Format {
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk <= maxPK {
				valByPK[pk] = float64(col.Uint64Values[i])
				hasByPK[pk] = true
			}
		}
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var fval float64
			if entry.Value != "" {
				if v, parseErr := strconv.ParseFloat(entry.Value, 64); parseErr == nil {
					fval = v
				}
			} else {
				fval = float64(entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk <= maxPK {
					valByPK[pk] = fval
					hasByPK[pk] = true
				}
			}
		}
	}
	for i, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if hasByPK[pk] {
			aggVals[i] = valByPK[pk]
			aggPresent[i] = true
		}
	}
	return aggVals, aggPresent, nil
}

// streamByRefSlice is the unified single-pass accumulator for all N=1 group-by aggregate
// functions. Iterates inRangeRefs sequentially with zero map lookups in the hot loop for
// COUNT/RATE and HISTOGRAM branches. The general aggregate branch (streamByRefSliceAgg)
// still probes an idBuckets map keyed by (dictIdx, bucket).
// NOTE-085: replaces separate streamCountRateByRefSlice, streamAggGroupByIDSingle, and
// streamHistogramGroupByIDSingle for the dict-format group-by path.
// NOTE-087: r, dictByPK, maxPK added for the HISTOGRAM branch — streamByRefSliceHistogram now
// scans the aggregate column directly using dense array lookups, eliminating buildAggValsForRef.
func streamByRefSlice(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	dictIdxForRef []uint32,
	dict []string,
	agg vm.AggregateSpec,
	aggValsForRef []float64, // nil for count/rate and HISTOGRAM
	aggPresent []bool, // nil for count/rate and HISTOGRAM
	dictByPK []uint32, // dense packKey→dictIdx+1 array for HISTOGRAM; ignored for other functions
	maxPK uint32, // upper bound of dictByPK; 0 if dictByPK is nil
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	switch agg.Function {
	case vm.FuncNameCOUNT, vm.FuncNameRATE:
		return streamByRefSliceCountRate(ctx, inRangeRefs, inRangeVals, dictIdxForRef, dict, numSteps, tb, buckets)
	case vm.FuncNameHISTOGRAM:
		// NOTE-087: streamByRefSliceHistogram now scans the aggregate column directly via r,
		// using dictByPK for O(1) group lookups and a bucketByPK dense array for O(1) time-bucket
		// lookups. buildAggValsForRef (7.7MB valByPK + 300M extra array ops) is eliminated.
		return streamByRefSliceHistogram(ctx, r, inRangeRefs, inRangeVals, dictByPK, maxPK, dict, agg, numSteps, tb, buckets)
	default:
		return streamByRefSliceAgg(ctx, inRangeRefs, inRangeVals, dictIdxForRef, dict, agg, aggValsForRef, aggPresent, numSteps, tb, buckets)
	}
}

// streamByRefSliceCountRate accumulates count/rate into [][]int64 indexed by dictIdx.
// Pre-allocates all count slices to keep the hot loop to zero map lookups.
// NOTE-085: count/rate branch of streamByRefSlice.
func streamByRefSliceCountRate(
	ctx context.Context,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	dictIdxForRef []uint32,
	dict []string,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	groupCounts := make([][]int64, len(dict))
	for i := range groupCounts {
		groupCounts[i] = make([]int64, numSteps)
	}
	for i := range inRangeRefs {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		bucketIdx := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bucketIdx >= 0 && bucketIdx < numSteps {
			groupCounts[dictIdxForRef[i]][bucketIdx]++
		}
	}
	for dictIdx, counts := range groupCounts {
		hasAny := false
		for _, c := range counts {
			if c > 0 {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}
		gk := ""
		if dictIdx < len(dict) { //nolint:gosec
			gk = dict[dictIdx]
		}
		for bucketIdx, c := range counts {
			if c == 0 {
				continue
			}
			k := strconv.FormatInt(int64(bucketIdx), 10) + "\x00" + gk //nolint:gosec
			intrinsicGetOrCreateBucket(buckets, k).count += c
		}
	}
	return nil
}

// histFlatStride is the fixed boundary-slot cap for the flat accumulator in
// streamByRefSliceHistogram. 64 covers all realistic log2 histogram boundaries
// for span:duration (max ~46 distinct values) with headroom.
// NOTE-088: flat accumulator replaces histSpanEntry intermediate slice — eliminates
// 1.5 GB allocation at 150 M spans by accumulating directly during the column scan.
const histFlatStride = 64

// streamByRefSliceHistogram accumulates histogram counts for the N=1 group-by path using a
// pre-allocated flat accumulator: groupCountsFlat[gIdx*histFlatStride*numSteps + bIdx*numSteps + timeIdx].
//
// NOTE-087: replaces the old pre-scan over inRangeRefs (which required buildAggValsForRef's
// 7.7 MB valByPK intermediate array and 300 M extra array ops) with a single direct scan of
// the aggregate intrinsic column. Dense arrays replace all hash map lookups:
//
//   - bucketByPK[pk] = timeBucket+1 (0 = not in range) — built from inRangeRefs/inRangeVals
//   - dictByPK[pk]   = dictIdx+1    (0 = absent)        — passed in from buildDictIdxForRefs
//   - seenByPK[pk]                                       — dense bool for absent-row pass
//
// NOTE-088: groupCountsFlat is pre-allocated before the column scan using histFlatStride=64
// as the fixed boundary cap. This eliminates the histSpanEntry intermediate slice (10 bytes ×
// 150M spans = 1.5 GB) and the subsequent second-pass accumulation loop. Single-pass, flat
// layout, cache-friendly.
//
// Absent-row pass: spans in inRangeRefs not seen in the aggregate column are counted into
// the boundary-0 bucket (bIdx=0), matching the block-scan and old-path behavior.
func streamByRefSliceHistogram(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	dictByPK []uint32, // dense packKey→dictIdx+1; 0=absent; from buildDictIdxForRefs
	maxPK uint32,
	dict []string,
	agg vm.AggregateSpec,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	if len(inRangeRefs) == 0 {
		return nil
	}

	// Step 1: build bucketByPK from inRangeRefs — O(len(inRangeRefs)) array writes.
	// bucketByPK[pk] = timeBucketIndex+1 so that 0 means "not in range".
	// Always allocate when inRangeRefs is non-empty: packKey=0 is a valid span key.
	bucketByPK := make([]int64, maxPK+1) //nolint:gosec
	for i, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = bk + 1 // +1: sentinel 0 = out of range
		}
	}

	// Step 2: pre-allocate flat accumulator BEFORE the column scan.
	// Layout: [gIdx * histFlatStride * numSteps + bIdx * numSteps + timeIdx]
	// bIdx=0 is the absent/boundary-0 sentinel; bIdx=1..N are actual boundaries.
	// NOTE-088: pre-allocation with fixed histFlatStride avoids needing to know boundary
	// count upfront, so we can accumulate directly during the scan (single pass).
	numGroups := len(dict)
	stride2 := numSteps                                        // steps per boundary slot
	stride1 := int64(histFlatStride) * numSteps                // slots per group
	groupCountsFlat := make([]int64, int64(numGroups)*stride1) //nolint:gosec

	// Step 3: scan the aggregate column directly — memoize boundaries (~30 unique values).
	// boundaryCache maps boundary float64 → 1-based index into boundaries slice.
	// boundaryByIdx maps 1-based bIdx → float64 boundary (for emit).
	boundaryCache := make(map[float64]int64, 32)
	boundaries := make([]float64, 0, 32)

	getBoundaryIdx := func(v float64) int64 {
		b := intrinsicHistogramBoundary(v, agg.Field)
		idx, ok := boundaryCache[b]
		if !ok {
			// When boundary count reaches histFlatStride, return histFlatStride as a
			// discard sentinel — not histFlatStride-1 (which is the last valid slot).
			// streamByRefSliceHistogramScanDict guards with bIdx >= histFlatStride and
			// skips those entries, so the flat accumulator is never overrun. The emit
			// loop iterates up to len(boundaries) which is capped at histFlatStride,
			// so no out-of-bounds access occurs there either.
			if len(boundaries) >= histFlatStride {
				idx = int64(histFlatStride) // 1-based cap; scanDict will discard this
				boundaryCache[b] = idx
				return idx
			}
			boundaries = append(boundaries, b)
			idx = int64(len(boundaries)) // 1-based; 0 is absent sentinel
			boundaryCache[b] = idx
		}
		return idx
	}

	// seenByPK: dense absent-row tracking — avoids a hash set for len(inRangeRefs) pks.
	// Always allocate when inRangeRefs is non-empty: packKey=0 is a valid span key.
	seenByPK := make([]bool, maxPK+1) //nolint:gosec

	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}

	if col != nil {
		if err := streamByRefSliceHistogramScanDict(ctx, col, bucketByPK, dictByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride1, stride2); err != nil {
			return err
		}
	}

	// Absent-row pass: spans not seen in the aggregate column → bIdx=0 (boundary-0 sentinel).
	// pk <= maxPK is always true here since bucketByPK is sized maxPK+1 and inRangeRefs
	// was used to compute maxPK.
	for _, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if !seenByPK[pk] {
			bk := bucketByPK[pk]
			if bk == 0 {
				continue // out of range
			}
			var gIdx int64
			if raw := dictByPK[pk]; raw > 0 {
				gIdx = int64(raw - 1) //nolint:gosec
			}
			// bIdx=0 is the absent sentinel; flat index: gIdx*stride1 + 0*stride2 + (bk-1)
			groupCountsFlat[gIdx*stride1+(bk-1)]++
		}
	}

	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, numGroups, dict, boundaries, buckets)
}

// scanHistogramN0 is a specialized scanner for N=0 (no group-by) histogram accumulation.
// Eliminates the dictByPK allocation and per-span lookup of streamByRefSliceHistogramScanDict —
// gIdx is always 0, so groupCountsFlat[bIdx*stride2+(bk-1)] is the only write target.
// NOTE-089: N=0 direct path — no dictByPK, no gIdx computation.
func scanHistogramN0(
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	bucketByPK []int64,
	maxPK uint32,
	seenByPK []bool,
	getBoundaryIdx func(float64) int64,
	groupCountsFlat []int64,
	stride2 int64,
) error {
	spanCount := 0
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			bIdx := getBoundaryIdx(v)
			if bIdx >= int64(histFlatStride) {
				continue
			}
			base := bIdx * stride2
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk > maxPK {
					continue
				}
				bk := bucketByPK[pk]
				if bk == 0 {
					continue
				}
				seenByPK[pk] = true
				groupCountsFlat[base+(bk-1)]++
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk > maxPK {
				continue
			}
			bk := bucketByPK[pk]
			if bk == 0 {
				continue
			}
			bIdx := getBoundaryIdx(float64(col.Uint64Values[i]))
			if bIdx >= int64(histFlatStride) {
				continue
			}
			seenByPK[pk] = true
			groupCountsFlat[bIdx*stride2+(bk-1)]++
		}
	}
	return nil
}

// streamByRefSliceHistogramScanDict scans an intrinsic column (both dict and flat formats) and
// accumulates directly into groupCountsFlat. Extracted to keep streamByRefSliceHistogram's
// cyclomatic complexity bounded.
// seenByPK[pk] is set true for every pk that appears in the column and is in range.
// NOTE-088: accumulates inline during the scan — no intermediate histSpanEntry slice.
func streamByRefSliceHistogramScanDict(
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	bucketByPK []int64,
	dictByPK []uint32,
	maxPK uint32,
	seenByPK []bool,
	getBoundaryIdx func(float64) int64,
	groupCountsFlat []int64,
	stride1, stride2 int64,
) error {
	spanCount := 0
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			bIdx := getBoundaryIdx(v) // memoized — O(unique dict entries), not O(spans)
			if bIdx >= int64(histFlatStride) {
				continue // guard: boundary cap exceeded (should not happen for span:duration)
			}
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk > maxPK {
					continue
				}
				bk := bucketByPK[pk]
				if bk == 0 {
					continue // out of range
				}
				seenByPK[pk] = true
				var gIdx int64
				if raw := dictByPK[pk]; raw > 0 {
					gIdx = int64(raw - 1) //nolint:gosec
				}
				groupCountsFlat[gIdx*stride1+bIdx*stride2+(bk-1)]++
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk > maxPK {
				continue
			}
			bk := bucketByPK[pk]
			if bk == 0 {
				continue // out of range
			}
			seenByPK[pk] = true
			var gIdx int64
			if raw := dictByPK[pk]; raw > 0 {
				gIdx = int64(raw - 1) //nolint:gosec
			}
			bIdx := getBoundaryIdx(float64(col.Uint64Values[i]))
			if bIdx >= int64(histFlatStride) {
				continue // guard: boundary cap exceeded
			}
			groupCountsFlat[gIdx*stride1+bIdx*stride2+(bk-1)]++
		}
	}
	return nil
}

// streamByRefSliceHistogramFlatEmit emits non-zero counts from the flat accumulator into buckets.
// Layout: groupCountsFlat[gIdx*stride1 + bIdx*stride2 + timeIdx].
// bIdx=0 is the absent/boundary-0 sentinel; bIdx=1..len(boundaries) are actual boundaries.
// NOTE-088: replaces streamByRefSliceHistogramEmit (3D slice) — same emit semantics, flat layout.
func streamByRefSliceHistogramFlatEmit(
	groupCountsFlat []int64,
	stride1, stride2 int64,
	numGroups int,
	dict []string,
	boundaries []float64,
	buckets map[string]*aggBucketState,
) error {
	numBoundaries := int64(len(boundaries)) + 1             // +1 for absent sentinel at bIdx=0
	for gIdx := int64(0); gIdx < int64(numGroups); gIdx++ { //nolint:gosec
		gk := ""
		if int(gIdx) < len(dict) { //nolint:gosec
			gk = dict[gIdx]
		}
		for bIdx := int64(0); bIdx < numBoundaries; bIdx++ {
			var boundary float64
			if bIdx > 0 && int(bIdx-1) < len(boundaries) { //nolint:gosec
				boundary = boundaries[bIdx-1]
			}
			boundaryStr := strconv.FormatFloat(boundary, 'g', -1, 64)
			base := gIdx*stride1 + bIdx*stride2
			for timeIdx := int64(0); timeIdx < stride2; timeIdx++ {
				count := groupCountsFlat[base+timeIdx]
				if count == 0 {
					continue
				}
				k := strconv.FormatInt(timeIdx, 10) + "\x00" + gk + "\x00" + boundaryStr
				intrinsicGetOrCreateBucket(buckets, k).count += count
			}
		}
	}
	return nil
}

// streamByRefSliceAgg accumulates general aggregate functions (sum, avg, min, max, etc.)
// keyed by (dictIdx, bucket).
// NOTE-085: general agg branch of streamByRefSlice.
func streamByRefSliceAgg(
	ctx context.Context,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	dictIdxForRef []uint32,
	dict []string,
	agg vm.AggregateSpec,
	aggValsForRef []float64,
	aggPresent []bool,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	type aggKey struct {
		dictIdx uint32
		bucket  int64
	}
	idBuckets := make(map[aggKey]*aggBucketState)
	for i := range inRangeRefs {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		bucketIdx := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bucketIdx < 0 || bucketIdx >= numSteps {
			continue
		}
		ak := aggKey{dictIdx: dictIdxForRef[i], bucket: bucketIdx}
		bucket := idBuckets[ak]
		if bucket == nil {
			bucket = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
			idBuckets[ak] = bucket
		}
		if aggPresent != nil && aggPresent[i] {
			updateAggBucket(bucket, agg.Function, aggValsForRef[i])
		}
	}
	for ak, bucket := range idBuckets {
		gk := ""
		if int(ak.dictIdx) < len(dict) { //nolint:gosec
			gk = dict[ak.dictIdx]
		}
		compositeKey := strconv.FormatInt(ak.bucket, 10) + "\x00" + gk
		buckets[compositeKey] = bucket
	}
	return nil
}

// resolveGroupIDKey resolves a groupIDKey back to the composite string group key
// using the per-dimension dicts. The format is identical to buildGroupKeyMap output:
// values joined by "\x00" separators, matching traceBuildDenseSeries expectations.
// NOTE-074: O(unique groups), not O(spans) — called only at series-emit time.
func resolveGroupIDKey(key groupIDKey, dicts [][]string) string {
	if len(dicts) == 0 {
		return ""
	}
	vals := make([]string, len(dicts))
	for i, dict := range dicts {
		id := key[i]
		if int(id) < len(dict) { //nolint:gosec
			vals[i] = dict[id]
		}
	}
	return strings.Join(vals, "\x00")
}

// buildGroupKeyMap builds a packKey → compositeGroupKey map by iterating each group-by
// column's dict/flat entries directly against keyToBucket. One map replaces N per-column maps.
//
// The composite key always has exactly len(groupBy)-1 "\x00" separators, matching the
// strings.Join(attrVals, "\x00") produced by the block-scan path. Rows absent from a column
// contribute an empty string for that position (Tempo convention).
//
// NOTE-056: single-group-by fast path — writes directly to out, eliminating the colVals
// intermediate map for the common case of one group-by column.
func buildGroupKeyMap(
	r *modules_reader.Reader,
	groupBy []string,
	keyToBucket map[uint32]int64,
) (map[uint32]string, error) {
	out := make(map[uint32]string, len(keyToBucket))

	// Fast path: single group-by column — write directly to out, skip colVals map.
	if len(groupBy) == 1 {
		col, err := r.GetIntrinsicColumn(groupBy[0])
		if err != nil {
			return nil, err
		}
		if col != nil {
			scanIntrinsicColVals(col, groupBy[0], keyToBucket, out)
		}
		// Fill absent pks with empty string (Tempo convention).
		for pk := range keyToBucket {
			if _, ok := out[pk]; !ok {
				out[pk] = ""
			}
		}
		return out, nil
	}

	// Multi-group-by path: build colVals per column.
	for i, colName := range groupBy {
		// Collect this column's values into a temporary per-pk map.
		// Rows absent from the column get the empty string (Tempo convention).
		colVals := make(map[uint32]string, len(keyToBucket))
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil {
			return nil, err
		}
		if col != nil {
			scanIntrinsicColVals(col, colName, keyToBucket, colVals)
		}

		// Apply this column's values to out. Iterating keyToBucket ensures every in-range
		// row gets the separator for this position, even when absent from the column.
		for pk := range keyToBucket {
			val := colVals[pk] // empty string if row absent from this column
			if i == 0 {
				out[pk] = val
			} else {
				out[pk] += "\x00" + val
			}
		}
	}
	return out, nil
}

// buildAggValsMap builds a packKey → float64 map for the aggregate field column.
// Used only when both a group-by and an aggregate field are present — otherwise
// streamAggColumnNoGroupBy avoids this allocation.
func buildAggValsMap(
	r *modules_reader.Reader,
	fieldName string,
	keyToBucket map[uint32]int64,
) (map[uint32]float64, error) {
	out := make(map[uint32]float64, len(keyToBucket))
	col, err := r.GetIntrinsicColumn(fieldName)
	if err != nil {
		return nil, err
	}
	if col == nil {
		return out, nil
	}
	switch col.Format {
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if _, ok := keyToBucket[pk]; ok {
				out[pk] = float64(col.Uint64Values[i])
			}
		}
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var fval float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					// Non-numeric dict entry: skip entirely so absent rows are not
					// treated as 0, matching the block-scan path's ok-check behavior.
					continue
				}
				fval = parsed
			} else {
				fval = float64(entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if _, ok := keyToBucket[pk]; ok {
					out[pk] = fval
				}
			}
		}
	}
	return out, nil
}

// streamHistogramGroupBy accumulates histogram buckets for the group-by path by
// streaming the aggregate intrinsic column directly. Replaces the buildAggValsMap +
// for-loop two-step for the HISTOGRAM function, eliminating the map[uint32]float64
// allocation and amortizing boundary/boundaryStr computation across dict entries.
//
// NOTE-055: dict amortization — boundary and boundaryStr are computed once per dict
// entry (typically ~30 distinct duration buckets) instead of once per span.
func streamHistogramGroupBy(
	r *modules_reader.Reader,
	fieldName string,
	keyToBucket map[uint32]int64,
	groupKeyMap map[uint32]string,
	buckets map[string]*aggBucketState,
) error {
	col, err := r.GetIntrinsicColumn(fieldName)
	if err != nil {
		return err
	}
	if col == nil {
		// All spans absent from field → boundary-0 for every pk.
		for pk, bucketIdx := range keyToBucket {
			key := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk] + "\x000"
			intrinsicGetOrCreateBucket(buckets, key).count++
		}
		return nil
	}

	seen := make(map[uint32]struct{}, len(keyToBucket))

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					// Non-numeric: absent-row pass will emit boundary-0.
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			boundary := intrinsicHistogramBoundary(v, fieldName)
			boundaryStr := strconv.FormatFloat(boundary, 'g', -1, 64)
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				bucketIdx, ok := keyToBucket[pk]
				if !ok {
					continue
				}
				seen[pk] = struct{}{}
				key := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk] + "\x00" + boundaryStr
				intrinsicGetOrCreateBucket(buckets, key).count++
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			bucketIdx, ok := keyToBucket[pk]
			if !ok {
				continue
			}
			seen[pk] = struct{}{}
			v := float64(col.Uint64Values[i])
			boundary := intrinsicHistogramBoundary(v, fieldName)
			key := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk] + "\x00" +
				strconv.FormatFloat(boundary, 'g', -1, 64)
			intrinsicGetOrCreateBucket(buckets, key).count++
		}
	}

	// Absent-row pass: pks not seen in the column → boundary-0.
	// Skip entirely if every pk was found (common for universal columns like span:duration).
	if len(seen) < len(keyToBucket) {
		for pk, bucketIdx := range keyToBucket {
			if _, ok := seen[pk]; !ok {
				key := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk] + "\x000"
				intrinsicGetOrCreateBucket(buckets, key).count++
			}
		}
	}
	return nil
}

// histGroupIDKey is the map key for dict-ID keyed histogram group-by.
// Replaces the composite string key used in streamHistogramGroupBy.
// boundary is always a power-of-2 or 0 (never NaN), so float64 comparison is safe.
// SPEC-ETM-13.4: float64 boundary key is safe because intrinsicHistogramBoundary never returns NaN.
type histGroupIDKey struct {
	dims      groupIDKey
	boundary  float64
	bucketIdx int64
}

// aggGroupIDKey is the map key for dict-ID keyed non-histogram aggregate group-by.
// NOTE-074: Accumulates SUM/AVG/MIN/MAX/QUANTILE/STDDEV keyed by (groupIDKey, bucketIdx);
// resolves to string at emit time (O(unique groups), not O(spans)).
type aggGroupIDKey struct {
	dims      groupIDKey
	bucketIdx int64
}

// histSingleGroupIDKey is the map key for the N=1 dict-ID histogram group-by fast path.
// Replaces histGroupIDKey for single-dimension queries, reducing key size from 48 to 24 bytes.
// NOTE-082: N=1 fast path — smaller key means faster hash and less memcmp.
// SPEC-ETM-13.4: float64 boundary key is safe (same invariant as histGroupIDKey).
type histSingleGroupIDKey struct {
	dictIdx   uint32
	boundary  float64
	bucketIdx int64
}

// aggSingleGroupIDKey is the map key for the N=1 dict-ID aggregate group-by fast path.
// NOTE-082: N=1 fast path — 16 bytes vs 40 bytes for aggGroupIDKey.
type aggSingleGroupIDKey struct {
	bucketIdx int64
	dictIdx   uint32
}

// histSingleAbsentKey is the map key for the N=1 nil-column histogram group-by fast path.
// NOTE-082: N=1 fast path — 16 bytes vs 48 bytes for histGroupIDKey.
type histSingleAbsentKey struct {
	bucketIdx int64
	dictIdx   uint32
}

// streamCountRateGroupByID is the dict-ID variant of streamCountRateGroupBy.
// Uses map[groupIDKey][]int64 instead of map[string][]int64 for groupCounts,
// eliminating string hash operations in the hot loop (~150M for M8).
//
// NOTE-074: Dict-ID fast path — see NOTES.md. dicts[i] maps dictIdx → string
// for resolution at series-emit time.
// NOTE-082: N=1 dispatched via accumulateIntrinsicBuckets → buildGroupIDMapSingle before this is called.
func streamCountRateGroupByID(
	ctx context.Context,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]groupIDKey,
	dicts [][]string,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 {
		return nil
	}
	groupCounts := make(map[groupIDKey][]int64)
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		gid := groupIDMap[pk]
		counts := groupCounts[gid]
		if counts == nil {
			counts = make([]int64, numSteps)
			groupCounts[gid] = counts
		}
		if bucketIdx >= 0 && bucketIdx < numSteps {
			counts[bucketIdx]++
		}
	}
	// Resolve IDs back to strings at series-emit time. O(unique groups), not O(spans).
	for gid, counts := range groupCounts {
		gk := resolveGroupIDKey(gid, dicts)
		for idx, c := range counts {
			if c > 0 {
				key := strconv.FormatInt(int64(idx), 10) + "\x00" + gk //nolint:gosec
				intrinsicGetOrCreateBucket(buckets, key).count = c
			}
		}
	}
	return nil
}

// streamCountRateGroupByIDSingle is the N=1 helper for streamCountRateGroupByID.
// Uses [][]int64 indexed by dictIdx instead of map[uint32][]int64.
// NOTE-082: uint32 key (vs [8]uint32) reduced memcmp cost.
// NOTE-085: slice accumulator eliminates groupCounts map probe (150M/file for M5).
func streamCountRateGroupByIDSingle(
	ctx context.Context,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]uint32,
	dict []string,
	numSteps int64,
	buckets map[string]*aggBucketState,
) error {
	// NOTE-085: [][]int64 indexed by dictIdx — eliminates 150M map probes per M5 file.
	// len(dict) is small (≤few thousand); pre-allocation cost is negligible vs span count.
	groupCounts := make([][]int64, len(dict))
	for i := range groupCounts {
		groupCounts[i] = make([]int64, numSteps)
	}
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		dictIdx := groupIDMap[pk]
		if bucketIdx >= 0 && bucketIdx < numSteps {
			groupCounts[dictIdx][bucketIdx]++
		}
	}
	// Resolve IDs back to strings at emit time. O(unique groups).
	for dictIdx, counts := range groupCounts {
		gk := dict[dictIdx]
		for idx, c := range counts {
			if c > 0 {
				k := strconv.FormatInt(int64(idx), 10) + "\x00" + gk //nolint:gosec
				intrinsicGetOrCreateBucket(buckets, k).count = c
			}
		}
	}
	return nil
}

// streamHistogramGroupByID is the dict-ID variant of streamHistogramGroupBy.
// Uses map[histGroupIDKey]int64 keyed by (groupIDKey, boundary, bucketIdx) to eliminate
// both string hash operations for group dims AND FormatFloat per span for the boundary.
//
// NOTE-074: Eliminates string hashing for group dims and FormatFloat+string
// hashing for the histogram boundary. The boundary float64 is used directly as
// part of the map key (safe: values are always powers-of-2 or 0, never NaN — SPEC-ETM-13.4).
// NOTE-082: N=1 dispatched via accumulateIntrinsicBuckets → buildGroupIDMapSingle before this is called.
func streamHistogramGroupByID(
	ctx context.Context,
	r *modules_reader.Reader,
	fieldName string,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]groupIDKey,
	dicts [][]string,
	buckets map[string]*aggBucketState,
) error {
	col, err := r.GetIntrinsicColumn(fieldName)
	if err != nil {
		return err
	}
	if col == nil {
		// NOTE-074: absent column → boundary is fixed at 0 for every span, so accumulate
		// by (groupIDKey, bucketIdx) and resolve strings once per unique group at emit.
		// This preserves the O(groups) resolution contract in the function header.
		absentCounts := make(map[histGroupIDKey]int64)
		i := 0
		for pk, bucketIdx := range keyToBucket {
			if i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			i++
			gid := groupIDMap[pk]
			hk := histGroupIDKey{dims: gid, boundary: 0, bucketIdx: bucketIdx}
			absentCounts[hk]++
		}
		for hk, count := range absentCounts {
			gk := resolveGroupIDKey(hk.dims, dicts)
			key := strconv.FormatInt(hk.bucketIdx, 10) + "\x00" + gk + "\x000"
			intrinsicGetOrCreateBucket(buckets, key).count += count
		}
		return nil
	}

	// histCounts: (group, boundary, timeBucket) → count.
	histCounts := make(map[histGroupIDKey]int64)
	seen := make(map[uint32]struct{}, len(keyToBucket))
	spanCount := 0

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			boundary := intrinsicHistogramBoundary(v, fieldName)
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				bucketIdx, ok := keyToBucket[pk]
				if !ok {
					continue
				}
				seen[pk] = struct{}{}
				gid := groupIDMap[pk]
				hk := histGroupIDKey{dims: gid, boundary: boundary, bucketIdx: bucketIdx}
				histCounts[hk]++
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			bucketIdx, ok := keyToBucket[pk]
			if !ok {
				continue
			}
			seen[pk] = struct{}{}
			v := float64(col.Uint64Values[i])
			boundary := intrinsicHistogramBoundary(v, fieldName)
			gid := groupIDMap[pk]
			hk := histGroupIDKey{dims: gid, boundary: boundary, bucketIdx: bucketIdx}
			histCounts[hk]++
		}
	}

	// Absent-row pass: pks not seen → boundary-0.
	if len(seen) < len(keyToBucket) {
		for pk, bucketIdx := range keyToBucket {
			if _, ok := seen[pk]; !ok {
				gid := groupIDMap[pk]
				hk := histGroupIDKey{dims: gid, boundary: 0, bucketIdx: bucketIdx}
				histCounts[hk]++
			}
		}
	}

	// Emit: resolve IDs back to strings at series-emit time.
	for hk, count := range histCounts {
		gk := resolveGroupIDKey(hk.dims, dicts)
		boundaryStr := strconv.FormatFloat(hk.boundary, 'g', -1, 64)
		key := strconv.FormatInt(hk.bucketIdx, 10) + "\x00" + gk + "\x00" + boundaryStr
		intrinsicGetOrCreateBucket(buckets, key).count += count
	}
	return nil
}

// streamHistogramGroupByIDSingle is the N=1 helper for streamHistogramGroupByID.
// Uses histSingleGroupIDKey (24 bytes) instead of histGroupIDKey (48 bytes) for the hot-loop map.
// NOTE-082: gid[0] used directly as uint32 map key field; eliminates 28-byte zero padding.
func streamHistogramGroupByIDSingle(
	ctx context.Context,
	r *modules_reader.Reader,
	fieldName string,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]uint32,
	dict []string,
	buckets map[string]*aggBucketState,
) error {
	col, err := r.GetIntrinsicColumn(fieldName)
	if err != nil {
		return err
	}
	if col == nil {
		// Absent column — all spans get boundary-0; accumulate by (dictIdx, bucketIdx).
		absentCounts := make(map[histSingleAbsentKey]int64)
		i := 0
		for pk, bucketIdx := range keyToBucket {
			if i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			i++
			hk := histSingleAbsentKey{dictIdx: groupIDMap[pk], bucketIdx: bucketIdx}
			absentCounts[hk]++
		}
		for hk, count := range absentCounts {
			gk := ""
			if int(hk.dictIdx) < len(dict) {
				gk = dict[hk.dictIdx]
			}
			k := strconv.FormatInt(hk.bucketIdx, 10) + "\x00" + gk + "\x000"
			intrinsicGetOrCreateBucket(buckets, k).count += count
		}
		return nil
	}

	histCounts := make(map[histSingleGroupIDKey]int64)
	seen := make(map[uint32]struct{}, len(keyToBucket))
	spanCount := 0

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, parseErr := strconv.ParseFloat(entry.Value, 64)
				if parseErr != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			boundary := intrinsicHistogramBoundary(v, fieldName)
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				bucketIdx, ok := keyToBucket[pk]
				if !ok {
					continue
				}
				seen[pk] = struct{}{}
				hk := histSingleGroupIDKey{dictIdx: groupIDMap[pk], boundary: boundary, bucketIdx: bucketIdx}
				histCounts[hk]++
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for i, ref := range col.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			bucketIdx, ok := keyToBucket[pk]
			if !ok {
				continue
			}
			seen[pk] = struct{}{}
			v := float64(col.Uint64Values[i])
			boundary := intrinsicHistogramBoundary(v, fieldName)
			hk := histSingleGroupIDKey{dictIdx: groupIDMap[pk], boundary: boundary, bucketIdx: bucketIdx}
			histCounts[hk]++
		}
	}

	// Absent-row pass: pks not seen → boundary-0.
	if len(seen) < len(keyToBucket) {
		for pk, bucketIdx := range keyToBucket {
			if _, ok := seen[pk]; !ok {
				hk := histSingleGroupIDKey{dictIdx: groupIDMap[pk], boundary: 0, bucketIdx: bucketIdx}
				histCounts[hk]++
			}
		}
	}

	// Emit: resolve IDs back to strings at series-emit time.
	for hk, count := range histCounts {
		gk := ""
		if int(hk.dictIdx) < len(dict) {
			gk = dict[hk.dictIdx]
		}
		boundaryStr := strconv.FormatFloat(hk.boundary, 'g', -1, 64)
		k := strconv.FormatInt(hk.bucketIdx, 10) + "\x00" + gk + "\x00" + boundaryStr
		intrinsicGetOrCreateBucket(buckets, k).count += count
	}
	return nil
}

// streamAggGroupByID handles non-histogram aggregate functions (SUM, AVG, MIN, MAX,
// QUANTILE, STDDEV) with group-by using the dict-ID fast path.
// NOTE-074: Two-stage accumulation: hot loop accumulates into map[aggGroupIDKey]*aggBucketState
// (no string allocation per span); resolveGroupIDKey is called only at emit time (O(unique groups)).
// NOTE-082: N=1 dispatched via accumulateIntrinsicBuckets → buildGroupIDMapSingle before this is called.
func streamAggGroupByID(
	ctx context.Context,
	r *modules_reader.Reader,
	agg vm.AggregateSpec,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]groupIDKey,
	dicts [][]string,
	buckets map[string]*aggBucketState,
) error {
	aggVals, err := buildAggValsMap(r, agg.Field, keyToBucket)
	if err != nil {
		return err
	}

	// Accumulate into ID-keyed intermediate map — no string allocation per span.
	idBuckets := make(map[aggGroupIDKey]*aggBucketState)
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		gid := groupIDMap[pk]
		ak := aggGroupIDKey{dims: gid, bucketIdx: bucketIdx}
		bucket := idBuckets[ak]
		if bucket == nil {
			bucket = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
			idBuckets[ak] = bucket
		}
		if v, ok := aggVals[pk]; ok {
			updateAggBucket(bucket, agg.Function, v)
		}
	}

	// Emit: resolve IDs back to strings at series-emit time (O(unique groups)).
	for ak, bucket := range idBuckets {
		gk := resolveGroupIDKey(ak.dims, dicts)
		compositeKey := strconv.FormatInt(ak.bucketIdx, 10) + "\x00" + gk
		buckets[compositeKey] = bucket
	}
	return nil
}

// streamAggGroupByIDSingle is the N=1 helper for streamAggGroupByID.
// Uses aggSingleGroupIDKey (16 bytes) instead of aggGroupIDKey (40 bytes) for the hot-loop map.
// NOTE-082: gid[0] used directly as uint32 map key field.
func streamAggGroupByIDSingle(
	ctx context.Context,
	keyToBucket map[uint32]int64,
	groupIDMap map[uint32]uint32,
	dict []string,
	agg vm.AggregateSpec,
	aggVals map[uint32]float64,
	buckets map[string]*aggBucketState,
) error {
	idBuckets := make(map[aggSingleGroupIDKey]*aggBucketState)
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		ak := aggSingleGroupIDKey{dictIdx: groupIDMap[pk], bucketIdx: bucketIdx}
		bucket := idBuckets[ak]
		if bucket == nil {
			bucket = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
			idBuckets[ak] = bucket
		}
		if v, ok := aggVals[pk]; ok {
			updateAggBucket(bucket, agg.Function, v)
		}
	}

	// Emit: resolve IDs back to strings at series-emit time (O(unique groups)).
	for ak, bucket := range idBuckets {
		gk := ""
		if int(ak.dictIdx) < len(dict) {
			gk = dict[ak.dictIdx]
		}
		compositeKey := strconv.FormatInt(ak.bucketIdx, 10) + "\x00" + gk
		buckets[compositeKey] = bucket
	}
	return nil
}

// updateAggBucket updates a bucket with a single numeric value for non-count functions.
func updateAggBucket(bucket *aggBucketState, fn string, v float64) {
	switch fn {
	case vm.FuncNameSUM, vm.FuncNameAVG:
		bucket.count++
		bucket.sum += v
	case vm.FuncNameMIN:
		bucket.count++
		if v < bucket.min {
			bucket.min = v
		}
	case vm.FuncNameMAX:
		bucket.count++
		if v > bucket.max {
			bucket.max = v
		}
	case vm.FuncNameQUANTILE:
		bucket.count++
		bucket.values = append(bucket.values, v)
	case vm.FuncNameSTDDEV:
		// Welford's online algorithm (mirrors traceUpdateBucket).
		bucket.count++
		delta := v - bucket.mean
		bucket.mean += delta / float64(bucket.count)
		bucket.m2 += delta * (v - bucket.mean)
	default:
		// Fallback for unrecognized aggregate functions: count only.
		// NOTE: HISTOGRAM never reaches this path — it short-circuits to streamHistogramGroupBy.
		bucket.count++
	}
}

// intrinsicHistogramBoundary computes the log2 lower-boundary for a histogram cell.
// Mirrors traceHistogramBucket for the intrinsic fast path.
func intrinsicHistogramBoundary(v float64, fieldName string) float64 {
	if v <= 0 {
		return 0
	}
	if fieldName == "span:duration" {
		vSec := v / 1e9
		if vSec <= 0 {
			return 0
		}
		return math.Pow(2, math.Floor(math.Log2(vSec)))
	}
	return math.Pow(2, math.Floor(math.Log2(math.Abs(v))))
}
