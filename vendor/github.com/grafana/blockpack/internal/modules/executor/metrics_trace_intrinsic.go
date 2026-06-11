package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// Intrinsic fast path for ExecuteTraceMetrics.
// NOTE-046: When all needed columns (span:start, aggregate field, group-by) are in the
// intrinsic section, metrics queries skip full block reads entirely. See NOTES.md NOTE-046.
// See also NOTE-055 for the streamHistogramGroupBy dict-amortization extension.

import (
	"context"
	"math"
	"math/bits"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// groupCountsFlatPool pools large flat accumulation arrays across blocks.
// NOTE-124: Per-block allocation of 3-68 MB groupCountsFlat arrays generates heavy
// GC pressure for warm multi-block queries. The pool eliminates repeated
// allocations by reusing the backing array; the caller clears it before use.
// sync.Pool GC-collects items at each GC cycle, so no permanent memory leak.
var groupCountsFlatPool sync.Pool

// acquireGroupCountsFlat returns a zeroed []int64 of at least size n, either
// from the pool (if a large-enough slice is available) or freshly allocated.
func acquireGroupCountsFlat(n int64) []int64 {
	if v := groupCountsFlatPool.Get(); v != nil {
		if s, ok := v.([]int64); ok && int64(cap(s)) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]int64, n)
}

// releaseGroupCountsFlat returns s to the pool for reuse.
func releaseGroupCountsFlat(s []int64) {
	groupCountsFlatPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// compactUint32Pool pools []uint32 for sortedPKs and dictIdxByPos in compact-path functions.
// NOTE-125: Per-block allocation of 28+ MB uint32 arrays (4 bytes × n, n up to 7.2 M) generates
// GC pressure for warm multi-block queries. See compactInt32Pool, compactUint64Pool,
// compactFloat64Pool, compactBoolPool for companion pools covering the remaining arrays.
var compactUint32Pool sync.Pool

// compactInt32Pool pools []int32 for timeBucketByPos in compact-path functions.
// NOTE-125: ~29 MB per call at n=7.2 M. See compactUint32Pool.
var compactInt32Pool sync.Pool

// compactUint64Pool pools []uint64 for pkOrder (sort scratch) in unfiltered compact-path functions,
// for idxPacked (sort scratch) in mergeJoinFilteredRefsWithVals, and for pkBitset (pre-filter)
// in scanAggColHistogramCompact and scanGroupByColCompact.
// NOTE-125: ~57 MB per call at n=7.2 M; released immediately after sort inside block scope.
// NOTE-128: idxPacked ~57 MB per call at n=7.2 M in mergeJoinFilteredRefsWithVals.
// NOTE-134/135: pkBitset ~2 MB per block at maxPK=16M; requires clear() on acquire (zero-sentinel bits).
var compactUint64Pool sync.Pool

// compactFloat64Pool pools []float64 for aggValByPos in agg compact-path functions.
// NOTE-125: ~57 MB per call at n=7.2 M (agg paths only). See compactUint32Pool.
var compactFloat64Pool sync.Pool

// compactBoolPool pools []bool for aggPresentByPos and seenByPos in compact-path functions.
// NOTE-125: ~7 MB per call at n=7.2 M (agg and histogram paths only). See compactUint32Pool.
var compactBoolPool sync.Pool

// compactBlockRefPool pools []modules_shared.BlockRef for outRefs in mergeJoinFilteredRefsWithVals.
// NOTE-130: ~14 MB per call at outCap=3.5 M; pooled to eliminate GC pressure.
var compactBlockRefPool sync.Pool

func acquireCompactUint32(n int) []uint32 {
	if v := compactUint32Pool.Get(); v != nil {
		if s, ok := v.([]uint32); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]uint32, n)
}

func releaseCompactUint32(s []uint32) {
	compactUint32Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// acquireCompactUint32NoClear returns a []uint32 of length n from the pool WITHOUT zeroing
// it. NOTE-164: the sortedPKs buffers in streamCountRateN1Compact /
// streamCountRateN1CompactFromRefs / streamHistogramN1CompactFromRefs are written for every
// index [0,n) (one packKey per in-range ref) before any read, so the clear() in
// acquireCompactUint32 is pure waste — ~28 MB of zeroing per file at n≈7.2 M on the warm
// M6/M8/M9 rate-by/histogram scan path. Only callers that fully overwrite [0:n) before reading
// may use this; sentinel-using buffers (dictIdxByPos) must keep acquireCompactUint32. NOTE-195
// migrated rankPrefix to this NoClear variant once its build loop was confirmed to write all
// n+1 elements before any read.
func acquireCompactUint32NoClear(n int) []uint32 {
	if v := compactUint32Pool.Get(); v != nil {
		if s, ok := v.([]uint32); ok && cap(s) >= n {
			return s[:n]
		}
	}
	return make([]uint32, n)
}

func acquireCompactInt32(n int) []int32 {
	if v := compactInt32Pool.Get(); v != nil {
		if s, ok := v.([]int32); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]int32, n)
}

func releaseCompactInt32(s []int32) {
	compactInt32Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// acquireCompactUint64 returns a []uint64 of length n from the pool.
// Note: no clear is performed here — most callers (pkOrder, idxPacked sort scratch) fully
// overwrite every element before reading. Callers that use zero as a sentinel (e.g. pkBitset
// in scanAggColHistogramCompact and scanGroupByColCompact, NOTE-134/135) must call clear(s) themselves after acquire.
func acquireCompactUint64(n int) []uint64 {
	if v := compactUint64Pool.Get(); v != nil {
		if s, ok := v.([]uint64); ok && cap(s) >= n {
			return s[:n]
		}
	}
	return make([]uint64, n)
}

func releaseCompactUint64(s []uint64) {
	compactUint64Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

func acquireCompactFloat64(n int) []float64 {
	if v := compactFloat64Pool.Get(); v != nil {
		if s, ok := v.([]float64); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]float64, n)
}

func releaseCompactFloat64(s []float64) {
	compactFloat64Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

func acquireCompactBool(n int) []bool {
	if v := compactBoolPool.Get(); v != nil {
		if s, ok := v.([]bool); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]bool, n)
}

func releaseCompactBool(s []bool) {
	compactBoolPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// acquireCompactBlockRef returns a []modules_shared.BlockRef of length n from the pool.
// No clear is performed — callers use [:0]+append, fully overwriting before read.
// NOTE-130: output buffers for mergeJoinFilteredRefsWithVals; see also acquireCompactUint64.
func acquireCompactBlockRef(n int) []modules_shared.BlockRef {
	if v := compactBlockRefPool.Get(); v != nil {
		if s, ok := v.([]modules_shared.BlockRef); ok && cap(s) >= n {
			return s[:n]
		}
	}
	return make([]modules_shared.BlockRef, n)
}

func releaseCompactBlockRef(s []modules_shared.BlockRef) {
	compactBlockRefPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// NOTE-129: directInt16Pool pools []int16 for bucketByPK in direct-path accumulation.
// ~32 MB per call at maxPK=16M entries. Mirrors compactBoolPool but sized by maxPK.
var directInt16Pool sync.Pool

// NOTE-129: directBoolPool pools []bool for seenByPK in direct-path accumulation.
// ~16 MB per call at maxPK=16M entries.
var directBoolPool sync.Pool

func acquireDirectInt16(n int) []int16 {
	if v := directInt16Pool.Get(); v != nil {
		if s, ok := v.([]int16); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]int16, n)
}

func releaseDirectInt16(s []int16) {
	directInt16Pool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

func acquireDirectBool(n int) []bool {
	if v := directBoolPool.Get(); v != nil {
		if s, ok := v.([]bool); ok && cap(s) >= n {
			s = s[:n]
			clear(s)
			return s
		}
	}
	return make([]bool, n)
}

func releaseDirectBool(s []bool) {
	directBoolPool.Put(s[:cap(s)]) //nolint:staticcheck // SA6002: slice is pointer-sized
}

// ctxCheckInterval is how often (in spans) to check for context cancellation in hot loops.
// Large enough to bound overhead; small enough to bound cancellation latency.
const ctxCheckInterval = 100_000

// NOTE-143: parallel histogram scan tunables.
// histParallelWorkers caps the worker count; mirrors defaultPipelineWorkers (NOTE-058).
// histParallelMinItems is the size guard below which the goroutine+reduce overhead exceeds
// the scan cost, so the scan runs single-threaded (byte-identical to the pre-NOTE-143 path).
const (
	histParallelWorkers  = 8
	histParallelMinItems = 1 << 16 // 65536
)

// normalizeIntrinsicFieldName maps Tempo's short TraceQL field names to blockpack's
// internal intrinsic column names. The TraceQL parser produces short names (e.g. "duration")
// but GetIntrinsicColumn expects the full column name (e.g. "span:duration").
// This is the inverse of intrinsicLabelName in metrics_trace.go.
func normalizeIntrinsicFieldName(field string) string {
	switch field {
	case "duration":
		return colNameSpanDuration
	case "kind":
		return colNameSpanKind
	case "status":
		return colNameSpanStatus
	case "name":
		return colNameSpanName
	default:
		return field
	}
}

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

	// NOTE-197: prefetch the whole per-file intrinsic working set in one cache
	// round-trip before the per-column GetIntrinsicColumn calls below. The query reads
	// span:start, every predicate-leaf intrinsic column, and each group-by column; each
	// would otherwise resolve through its own memcache fetch. Batching collapses those
	// N round-trips into one. Names not present in the file or already decoded are
	// skipped inside PrefetchIntrinsicColumns, and any miss falls through to the normal
	// per-name path, so behavior is unchanged.
	prefetchIntrinsicWorkingSet(r, program, querySpec)

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

	// Field names are already normalized by ExecuteTraceMetrics before this function is called.
	agg := querySpec.Aggregate
	// Normalize short TraceQL field names to full intrinsic column names before any lookups.
	// The TraceQL parser produces "duration" but GetIntrinsicColumn expects "span:duration".
	agg.Field = normalizeIntrinsicFieldName(agg.Field)
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
		// NOTE-113: N=0 count/rate with a selective predicate — hash-filter directly
		// instead of merge-sort-join. streamCountRateNoGroupBy does not need sorted order,
		// so the O(N log N + F log F) sort in mergeJoinFilteredRefsWithVals is wasted work.
		// Threshold F ≤ N/4: at 25% selectivity the hash map (F×~20B) fits in L3 cache,
		// making hash lookups faster than sorting N in-range refs.
		n := hi - lo
		if isCountRate && len(agg.GroupBy) == 0 && len(filteredRefs)*4 <= n {
			return streamCountRateN0HashFilter(ctx, tsCol, lo, hi, filteredRefs, tb, querySpec)
		}
		var releaseFiltered func()
		inRangeRefs, inRangeVals, releaseFiltered = mergeJoinFilteredRefsWithVals(
			filteredRefs,
			inRangeRefs,
			inRangeVals,
		)
		defer releaseFiltered()
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

// prefetchIntrinsicWorkingSet batch-loads the intrinsic columns this metrics query will
// read — span:start, the aggregate field, every group-by column, and every predicate-leaf
// intrinsic column — in a single cache round-trip via Reader.PrefetchIntrinsicColumns.
// Column names are normalized to their full intrinsic form (e.g. "duration" -> "span:duration").
// NOTE-197.
func prefetchIntrinsicWorkingSet(r *modules_reader.Reader, program *vm.Program, querySpec *vm.QuerySpec) {
	seen := make(map[string]struct{}, 8)
	names := make([]string, 0, 8)
	add := func(field string) {
		name := normalizeIntrinsicFieldName(field)
		if name == "" {
			return
		}
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	add("span:start")
	add(querySpec.Aggregate.Field)
	for _, g := range querySpec.Aggregate.GroupBy {
		add(g)
	}
	if program != nil && program.Predicates != nil {
		predCols := make(map[string]struct{}, len(program.Predicates.Columns))
		collectNodeColumns(program.Predicates.Nodes, predCols)
		for _, c := range program.Predicates.Columns {
			predCols[c] = struct{}{}
		}
		for c := range predCols {
			add(c)
		}
	}

	r.PrefetchIntrinsicColumns(names)
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
		if filteredRefs == nil {
			// NOTE-149: unfiltered → inRangeVals is timestamp-sorted (tsVals[lo:hi]); count
			// per bucket via boundary binary search (O(numSteps·log N)) instead of the O(N)
			// per-span loop. The filtered path below is packKey-sorted, not time-sorted.
			return streamCountRateNoGroupBySorted(ctx, inRangeVals, tb, buckets)
		}
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
		// NOTE-108: compact fallback for count/rate — avoids the ~4 GB allocations
		// (keyToBucket hash map + dictByPK dense array) that accumulateIntrinsicBucketsViaKeyMap
		// produces for large production files where maxPK > maxDirectArrayEntries.
		if isCountRate {
			return streamCountRateN1Compact(ctx, r, tsCol, lo, hi, querySpec, buckets)
		}
		// NOTE-109: compact fallback for general agg (max/min/sum/avg etc.) — avoids ~3.4 GB
		// allocations (keyToBucket hash map + dictByPK + valByPK + hasByPK dense arrays)
		// produced by accumulateIntrinsicBucketsViaKeyMap for large files (maxPK > maxDirectArrayEntries).
		if agg.Function != vm.FuncNameHISTOGRAM {
			return streamAggN1Compact(ctx, r, tsCol, lo, hi, querySpec, buckets)
		}
		// NOTE-132: compact fallback for no-predicate N=1 histogram — avoids the ~3 GB
		// keyToBucket map[uint32]int64 allocation inside accumulateIntrinsicBucketsViaKeyMap
		// for large files where accumulateIntrinsicBucketsDirect returned false (maxPK >
		// maxDirectArrayEntries). streamHistogramN1Compact is already the canonical compact
		// histogram path (NOTE-092, NOTE-114); the only omission was not wiring it here.
		numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
		if numSteps <= 0 {
			return nil
		}
		groupByCol, colErr := r.GetIntrinsicColumn(agg.GroupBy[0])
		if colErr != nil {
			return colErr
		}
		return streamHistogramN1Compact(ctx, r, inRangeRefs, inRangeVals, groupByCol, agg, numSteps, tb, buckets)
	case filteredRefs != nil && len(agg.GroupBy) == 1 && isCountRate:
		// NOTE-110: predicate-filtered N=1 count/rate compact path.
		// inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals — skip the
		// O(n log n) pkOrder sort used by streamCountRateN1Compact and replace the
		// keyToBucket hash map + buildDictIdxForRefs dense arrays with compact binary search.
		// Reduces peak memory from ~1.5 GB to ~0.8 GB per goroutine per file.
		return streamCountRateN1CompactFromRefs(ctx, r, inRangeRefs, inRangeVals, querySpec, buckets)
	case filteredRefs != nil && len(agg.GroupBy) == 1 && !isCountRate && agg.Function != vm.FuncNameHISTOGRAM:
		// NOTE-112: predicate-filtered N=1 agg compact path (min/max/sum/avg/etc.).
		// Same principle as NOTE-110: inRangeRefs is pre-sorted, so skip pkOrder sort.
		// Replaces keyToBucket hash map + dictByPK + aggVals dense arrays (~203 MB)
		// with compact binary search arrays (~94 MB, 54% reduction).
		return streamAggN1CompactFromRefs(ctx, r, inRangeRefs, inRangeVals, querySpec, buckets)
	case filteredRefs != nil && len(agg.GroupBy) == 1 && agg.Function == vm.FuncNameHISTOGRAM:
		// NOTE-114: predicate-filtered N=1 histogram compact path.
		// inRangeRefs is pre-sorted from mergeJoinFilteredRefsWithVals — build sortedPKs in O(n).
		// Bypasses accumulateIntrinsicBucketsViaKeyMap (~90 MB keyToBucket) and the O(n log n)
		// pkOrder sort inside streamHistogramN1Compact.
		return streamHistogramN1CompactFromRefs(ctx, r, inRangeRefs, inRangeVals, querySpec, buckets)
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

// searchSortedUint32 searches a sorted []uint32 for pk using interpolation search.
// Returns the index and true if found, or -1 and false if not.
//
// NOTE-122: Interpolation search replaces binary search for compact-path lookups.
// PackKeys (blockIdx<<16|rowIdx) are approximately uniformly distributed across blocks
// when spans are evenly distributed, giving O(log log n) ≈ 3-5 probes average vs
// O(log n) ≈ 22 probes for binary search on 3.75M-entry arrays. Falls back to binary
// search for the final window when keys are not perfectly uniform.
func searchSortedUint32(s []uint32, pk uint32) (int, bool) {
	lo, hi := 0, len(s)-1
	for lo <= hi && pk >= s[lo] && pk <= s[hi] {
		if s[lo] == s[hi] {
			if s[lo] == pk {
				return lo, true
			}
			return -1, false
		}
		// Interpolate position — all operands are uint64 to prevent overflow.
		pos := lo + int(uint64(hi-lo)*uint64(pk-s[lo])/uint64(s[hi]-s[lo])) //nolint:gosec
		if s[pos] == pk {
			return pos, true
		}
		if s[pos] < pk {
			lo = pos + 1
		} else {
			hi = pos - 1
		}
	}
	// Linear fallback for out-of-range or converged window.
	if lo <= hi && lo < len(s) && s[lo] == pk {
		return lo, true
	}
	return -1, false
}

// scanGroupByColCompact scans col and populates dictIdxByPos for each packKey that
// appears in sortedPKs (sorted ascending), using binary search. Returns updated dict
// and populates dictIdxByPos (1-based index into dict; 0 = absent sentinel).
func scanGroupByColCompact(
	col *modules_shared.IntrinsicColumn,
	colName string,
	sortedPKs []uint32,
	dict *[]string,
	valToIdx map[string]uint32,
	dictIdxByPos []uint32,
) {
	if len(sortedPKs) == 0 {
		return
	}
	// Pre-compute the packKey range of in-range refs. Any column ref outside
	// [minPK, maxPK] is guaranteed absent from sortedPKs — skip binary search.
	// This is O(1) vs O(log N) per ref, saving the bulk of binary-search cost
	// when a file covers more time than the query window (M_total >> N).
	minPK, maxPK := sortedPKs[0], sortedPKs[len(sortedPKs)-1]

	// NOTE-135/140: build pkBitset + POPCNT rank index from sortedPKs (shared read-only).
	// Dict refs are not packKey-sorted; ~50% fail the pre-filter at 50% selectivity.
	// NOTE-140: rankPrefix[i] = cumulative popcount of pkBitset[0..i-1], enabling O(1) rank
	// lookup to replace searchSortedUint32 in both Dict and Flat/DeltaUint64 inner loops.
	n := int((maxPK >> 6) + 1) //nolint:gosec
	pkBitset := acquireCompactUint64(n)
	defer releaseCompactUint64(pkBitset)
	clear(pkBitset) // NOTE-135: zero-sentinel — must clear stale pool bits before setting
	for _, pk := range sortedPKs {
		pkBitset[pk>>6] |= uint64(1) << (pk & 63)
	}
	// NOTE-140: POPCNT rank index. Build cost: O(maxPK/64) ≈ 250K iterations at maxPK=16M → ~0.5µs.
	// NOTE-195: rankPrefix is fully overwritten before any read — the loop writes
	// rankPrefix[0:len(pkBitset)] (== [0:n)) and the trailing assignment writes
	// rankPrefix[len(pkBitset)] (== [n]), covering all n+1 elements. The clear() inside
	// acquireCompactUint32 is therefore pure waste here (~1 MB zeroed per call at maxPK=16M,
	// on the M4/M6/M9 group-by scan path), so use the NoClear variant.
	rankPrefix := acquireCompactUint32NoClear(n + 1)
	defer releaseCompactUint32(rankPrefix)
	var cum uint32
	for i, w := range pkBitset {
		rankPrefix[i] = cum
		cum += uint32(bits.OnesCount64(w)) //nolint:gosec
	}
	rankPrefix[len(pkBitset)] = cum

	// NOTE-148: parallelize the Dict per-ref inner loop — the bulk of the scan cost — across
	// min(NumCPU, histParallelWorkers) workers. The M4/M6/M9/M10 rate-by-group queries group by
	// low-cardinality columns (resource.service.name, http.request.method): few DictEntries but
	// millions of refs, so the work is in the REFS — shard by ref count (NOT entry count, unlike the
	// histogram Dict path in NOTE-143 whose work IS per-entry). Output (dict + dictIdxByPos) is
	// byte-identical to the serial path. The Flat path stays serial: Flat group-by is high-cardinality
	// and rare.
	//
	// Gate on totalRefs >= 2*len(sortedPKs): the per-ref work is memory-bandwidth-bound (filter +
	// array write), not CPU-bound like the histogram boundary math, so parallelism only pays when the
	// column walks meaningfully more refs than it writes — i.e. the file spans more time than the
	// query window (a 2h window over a ~24h file walks ~12x the in-range refs). At full coverage
	// (totalRefs ≈ n) the goroutine + per-position-write contention + translation pass make it a
	// regression, so we stay serial. Microbench (BenchmarkScanGroupByColCompact): 2x→2.0x, 4x→1.8x,
	// 12x→2.7x faster; full-coverage stays serial.
	if col.Format == modules_shared.IntrinsicFormatDict &&
		len(sortedPKs) >= histParallelMinItems && len(col.DictEntries) >= 2 {
		w := min(runtime.NumCPU(), histParallelWorkers)
		totalRefs := 0
		for i := range col.DictEntries {
			totalRefs += len(col.DictEntries[i].BlockRefs)
		}
		if w > 1 && totalRefs >= 2*len(sortedPKs) {
			scanGroupByColCompactDictParallel(
				col, colName, minPK, maxPK, pkBitset, rankPrefix, dict, valToIdx, dictIdxByPos, w,
			)
			return
		}
	}

	scanGroupByColCompactSerial(col, colName, minPK, maxPK, pkBitset, rankPrefix, dict, valToIdx, dictIdxByPos)
}

// scanGroupByColCompactSerial is the single-threaded body of scanGroupByColCompact. pkBitset and
// rankPrefix are the shared read-only POPCNT index over sortedPKs (NOTE-135/140). This is the
// byte-identical pre-NOTE-148 path; the parallel Dict path reproduces its output exactly.
func scanGroupByColCompactSerial(
	col *modules_shared.IntrinsicColumn,
	colName string,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
	dict *[]string,
	valToIdx map[string]uint32,
	dictIdxByPos []uint32,
) {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			val := entry.Value
			if val == "" {
				val = intrinsicInt64ColToString(colName, entry.Int64Val)
			}
			if val == "" {
				continue
			}
			var dictIdx uint32
			dictAssigned := false
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk < minPK || pk > maxPK {
					continue
				}
				word := pk >> 6
				bit := pk & 63
				if len(pkBitset) > 0 && pkBitset[word]&(uint64(1)<<bit) == 0 {
					continue // NOTE-135/140: fast pre-filter: pk not in sortedPKs
				}
				// NOTE-140: O(1) rank replaces O(log n) searchSortedUint32.
				r := rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1))) //nolint:gosec
				pos := int(r)                                                                         //nolint:gosec
				if !dictAssigned {
					idx, ok := valToIdx[val]
					if !ok {
						idx = uint32(len(*dict)) //nolint:gosec
						*dict = append(*dict, val)
						valToIdx[val] = idx
					}
					dictIdx = idx + 1 // +1: 0 is absent sentinel
					dictAssigned = true
				}
				dictIdxByPos[pos] = dictIdx
			}
		}
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatXORBytes,
		modules_shared.IntrinsicFormatDeltaUint64:
		for i, ref := range col.BlockRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk < minPK || pk > maxPK {
				continue
			}
			word := pk >> 6
			bit := pk & 63
			if len(pkBitset) > 0 && pkBitset[word]&(uint64(1)<<bit) == 0 {
				continue // NOTE-140: bitset pre-filter before rank lookup
			}
			// NOTE-140: O(1) rank replaces O(log n) searchSortedUint32.
			pos := int(rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1)))) //nolint:gosec
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
				idx = uint32(len(*dict)) //nolint:gosec
				*dict = append(*dict, val)
				valToIdx[val] = idx
			}
			dictIdxByPos[pos] = idx + 1
		}
	}
}

// scanGroupByColCompactDictParallel is the parallel Dict-format path for scanGroupByColCompact
// (NOTE-148). It parallelizes the per-ref inner loop — the bulk of the scan cost — across `workers`
// goroutines while keeping dict construction serial, so the output (dict + dictIdxByPos) is
// byte-identical to scanGroupByColCompactSerial.
//
// Why it is race-free without per-worker copies + reduction: each in-range span position is owned
// by exactly one ref (a span has a single value per column), so the dictIdxByPos[pos] writes are
// disjoint across workers. Workers take contiguous DictEntries ranges balanced by cumulative ref
// count, so each entry is owned by exactly one worker — the per-entry entryPassed[] writes are
// disjoint too. pkBitset, rankPrefix, and col.DictEntries are read-only. intrinsicInt64ColToString
// and packKey are pure.
//
//   - Phase A (parallel): for each owned entry with a non-empty value, write entryIdx+1 into
//     dictIdxByPos[pos] for every passing ref (pk in range + bitset member) and record entryPassed.
//   - Phase B (serial, O(numEntries)+O(n)): build dict in entry order — identical to the serial
//     path's first-passing-ref order, since it iterates entries in order and assigns on first pass —
//     then translate the temporary entryIdx+1 markers in dictIdxByPos to the final dictIdx+1.
func scanGroupByColCompactDictParallel(
	col *modules_shared.IntrinsicColumn,
	colName string,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
	dict *[]string,
	valToIdx map[string]uint32,
	dictIdxByPos []uint32,
	workers int,
) {
	entries := col.DictEntries
	numEntries := len(entries)

	// Cumulative ref counts for ref-balanced contiguous entry sharding.
	cumRefs := make([]int, numEntries+1)
	for i := range entries {
		cumRefs[i+1] = cumRefs[i] + len(entries[i].BlockRefs)
	}
	totalRefs := cumRefs[numEntries]

	entryPassed := make([]bool, numEntries)

	// Phase A: parallel per-ref writes. Contiguous entry ranges balanced by cumulative ref count
	// (low-cardinality dicts have a few ref-heavy entries; even-count chunking would leave one
	// worker doing most of the work). Each range is owned by exactly one worker.
	var wg sync.WaitGroup
	prev := 0
	for wk := 0; wk < workers && prev < numEntries; wk++ {
		end := numEntries
		if wk < workers-1 {
			target := totalRefs * (wk + 1) / workers
			end = prev
			for end < numEntries && cumRefs[end+1] <= target {
				end++
			}
			if end <= prev {
				end = prev + 1 // guarantee forward progress for an oversized entry
			}
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for e := lo; e < hi; e++ {
				entry := &entries[e]
				val := entry.Value
				if val == "" {
					val = intrinsicInt64ColToString(colName, entry.Int64Val)
				}
				if val == "" {
					continue // matches serial: empty values are skipped entirely
				}
				passed := false
				marker := uint32(e + 1) //nolint:gosec // entryIdx+1, bounded by numEntries
				for _, ref := range entry.BlockRefs {
					pk := packKey(ref.BlockIdx, ref.RowIdx)
					if pk < minPK || pk > maxPK {
						continue
					}
					word := pk >> 6
					bit := pk & 63
					if pkBitset[word]&(uint64(1)<<bit) == 0 {
						continue
					}
					// NOTE-140: O(1) rank replaces O(log n) searchSortedUint32.
					lowerBits := bits.OnesCount64(pkBitset[word] & ((uint64(1) << bit) - 1))
					rank := rankPrefix[word] + uint32(lowerBits) //nolint:gosec
					dictIdxByPos[int(rank)] = marker
					passed = true
				}
				entryPassed[e] = passed
			}
		}(prev, end)
		prev = end
	}
	wg.Wait()

	// Phase B (serial): build the dict in entry order — byte-identical to the serial path, whose
	// first-passing-ref encounter order IS entry order (it iterates DictEntries sequentially). Only
	// entries with a passing ref get a slot, so fully-filtered groups never enlarge groupCountsFlat.
	entryFinalIdx := make([]uint32, numEntries)
	for e := range entries {
		if !entryPassed[e] {
			continue
		}
		entry := &entries[e]
		val := entry.Value
		if val == "" {
			val = intrinsicInt64ColToString(colName, entry.Int64Val)
		}
		// val != "" is guaranteed: entryPassed[e] is only set for non-empty values.
		idx, ok := valToIdx[val]
		if !ok {
			idx = uint32(len(*dict)) //nolint:gosec
			*dict = append(*dict, val)
			valToIdx[val] = idx
		}
		entryFinalIdx[e] = idx + 1
	}
	// Translate entryIdx+1 markers to final dictIdx+1. Positions left 0 by Phase A stay absent.
	for pos := range dictIdxByPos {
		if v := dictIdxByPos[pos]; v > 0 {
			dictIdxByPos[pos] = entryFinalIdx[v-1]
		}
	}
}

// scanAggColHistogramCompact is the compact-path equivalent of streamByRefSliceHistogramScanDict.
// Uses a pkBitset + POPCNT rank index over sortedPKs instead of a dense bucketByPK array.
//
// NOTE-143: parallelized across min(runtime.NumCPU(), 8) workers. The driver builds the read-only
// pkBitset/rankPrefix ONCE (was rebuilt per Dict and Flat/Delta branch) and shares it across
// workers; each worker gets a private pooled groupCountsFlat/seenByPos that are sum/OR-reduced
// into the caller's arrays after wg.Wait(). The caller's getBoundaryIdx closure is a correctness
// hazard (it assigns boundary indices in first-encounter order and the resulting boundaries slice
// is read by emit), so the driver serially pre-warms a frozen read-only boundary lookup
// (buildFrozenBoundaryIdx) before any goroutine starts. A serial fallback keeps small inputs and
// single-core machines byte-identical to the pre-NOTE-143 path.
func scanAggColHistogramCompact( //nolint:gocyclo
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	sortedPKs []uint32,
	timeBucketByPos []int32,
	dictIdxByPos []uint32,
	seenByPos []bool,
	getBoundaryIdx func(float64) int64,
	groupCountsFlat []int64,
	stride1, stride2 int64,
	discardStride int64,
	fieldName string, // NOTE-143: callers pass agg.Field; used to rebuild boundaries for worker reads
	roBoundary func(float64) int64, // NOTE-182: race-free per-row lookup for parallel workers; nil → frozen-map fallback
) error {
	if len(sortedPKs) == 0 {
		return nil
	}
	minPK, maxPK := sortedPKs[0], sortedPKs[len(sortedPKs)-1]

	// NOTE-143: build pkBitset + POPCNT rankPrefix ONCE (was rebuilt per branch at the old
	// 678-697 / 751-773). Pure function of sortedPKs → identical for every worker; shared read-only.
	// NOTE-134/140: zero-bit membership sentinel — must clear stale pool bits before setting.
	nWords := int((maxPK >> 6) + 1) //nolint:gosec
	pkBitset := acquireCompactUint64(nWords)
	defer releaseCompactUint64(pkBitset)
	clear(pkBitset)
	for _, pk := range sortedPKs {
		pkBitset[pk>>6] |= uint64(1) << (pk & 63)
	}
	// NOTE-195: fully overwritten before read (see scanGroupByColCompact) — skip the clear.
	rankPrefix := acquireCompactUint32NoClear(nWords + 1)
	defer releaseCompactUint32(rankPrefix)
	var cum uint32
	for i, w := range pkBitset {
		rankPrefix[i] = cum
		cum += uint32(bits.OnesCount64(w)) //nolint:gosec
	}
	rankPrefix[len(pkBitset)] = cum

	var numItems int
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		numItems = len(col.DictEntries)
	case modules_shared.IntrinsicFormatFlat, modules_shared.IntrinsicFormatDeltaUint64:
		numItems = len(col.BlockRefs)
	default:
		return nil
	}

	w := runtime.NumCPU()
	if w > histParallelWorkers {
		w = histParallelWorkers
	}

	// NOTE-143: serial fallback — byte-identical to the pre-NOTE-143 path. Writes directly into
	// the caller's groupCountsFlat/seenByPos over the full range using the ORIGINAL closure.
	if w <= 1 || numItems < histParallelMinItems {
		return scanAggColHistogramShard(ctx, col, 0, numItems, minPK, maxPK,
			pkBitset, rankPrefix, timeBucketByPos, dictIdxByPos, seenByPos,
			getBoundaryIdx, groupCountsFlat, stride1, stride2, discardStride)
	}

	// NOTE-143: parallel path. Serial pre-warm so workers do pure read-only boundary lookups, and
	// so the caller's boundaries[] ends up identical to a serial run (emit is byte-identical). The
	// pre-warm applies the SAME row filters the scan applies, so it visits exactly the rows the
	// workers will reach (passing the pk-range/bitset/time-bucket filter), in scan order.
	// NOTE-182: when the driver supplies a race-free read-only lookup (the boundaryIndexer's
	// exponent-keyed lookup), use it directly — it shares the pre-warmed dense table and needs
	// no per-row float64 hashing. The pre-warm below (getBoundaryIdx == bi.index) populates that
	// table. Callers without a boundaryIndexer (the parity test) pass nil and fall back to the
	// legacy frozen map[float64]int64 rebuilt from the closure.
	if roBoundary == nil {
		frozen := buildFrozenBoundaryIdx(
			col, fieldName, getBoundaryIdx, minPK, maxPK, pkBitset, rankPrefix, timeBucketByPos,
		)
		roBoundary = func(v float64) int64 {
			b := intrinsicHistogramBoundary(v, fieldName)
			if idx, ok := frozen[b]; ok {
				return idx
			}
			return discardStride // unreachable: pre-warm visited every value the scan will reach
		}
	} else {
		// Serial pre-warm using getBoundaryIdx (bi.index) so bi.boundaries ends up identical to a
		// serial run and bi.lookup sees every assigned exponent slot before any worker reads it.
		prewarmBoundaries(col, fieldName, minPK, maxPK, pkBitset, rankPrefix, timeBucketByPos, func(v float64) {
			getBoundaryIdx(v)
		})
	}

	chunk := (numItems + w - 1) / w
	var wg sync.WaitGroup
	errs := make([]error, w)
	privGCF := make([][]int64, w)
	privSeen := make([][]bool, w)
	for k := range w {
		start := k * chunk
		if start >= numItems {
			break
		}
		end := start + chunk
		if end > numItems {
			end = numItems
		}
		gcf := acquireGroupCountsFlat(int64(len(groupCountsFlat))) // zeroed by pool
		seen := acquireCompactBool(len(seenByPos))                 // zeroed by pool
		privGCF[k] = gcf
		privSeen[k] = seen
		wg.Add(1)
		go func(k, start, end int, gcf []int64, seen []bool) {
			defer wg.Done()
			errs[k] = scanAggColHistogramShard(ctx, col, start, end, minPK, maxPK,
				pkBitset, rankPrefix, timeBucketByPos, dictIdxByPos, seen,
				roBoundary, gcf, stride1, stride2, discardStride)
		}(k, start, end, gcf, seen)
	}
	wg.Wait()

	// NOTE-143: reduce — sum groupCountsFlat (+=), OR seenByPos; release worker buffers.
	// Collect the first worker error (caller treats a non-nil error as fatal and discards the
	// partial result, matching the legacy ctx-cancel mid-scan semantics).
	var firstErr error
	for k := range w {
		if privGCF[k] == nil {
			continue
		}
		if errs[k] != nil && firstErr == nil {
			firstErr = errs[k]
		}
		for i, v := range privGCF[k] {
			groupCountsFlat[i] += v
		}
		for i, s := range privSeen[k] {
			if s {
				seenByPos[i] = true
			}
		}
		releaseGroupCountsFlat(privGCF[k])
		releaseCompactBool(privSeen[k])
	}
	return firstErr
}

// histRefPassPos applies the Flat/Delta scan's per-ref membership filter EXACTLY (pk range +
// pkBitset membership) and returns the POPCNT rank position. ok is false when the ref is filtered
// out (pk out of range or not a member of sortedPKs). The expressions here are the single source of
// truth shared by the scan loop in scanAggColHistogramShard AND the pre-warm in
// buildFrozenBoundaryIdx, so the two can never drift (NOTE-143).
//
// The caller is still responsible for the timeBucketByPos[pos] != 0 check (it needs pos to do so).
func histRefPassPos(
	ref modules_shared.BlockRef,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
) (int, bool) {
	pk := packKey(ref.BlockIdx, ref.RowIdx)
	if pk < minPK || pk > maxPK {
		return 0, false
	}
	word := pk >> 6
	bit := pk & 63
	if pkBitset[word]&(uint64(1)<<bit) == 0 {
		return 0, false // NOTE-139: bitset pre-filter; non-members skip rank lookup
	}
	// NOTE-139: O(1) rank replaces O(log n) searchSortedUint32. Membership confirmed above.
	pos := int(rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1)))) //nolint:gosec
	return pos, true
}

// scanAggColHistogramShard scans col.DictEntries[start:end] (Dict) or col.BlockRefs[start:end)
// (Flat/Delta), writing into the supplied (possibly per-worker private) groupCountsFlat/seenByPos.
// minPK/maxPK/pkBitset/rankPrefix are built once by the driver and shared read-only.
// NOTE-143: extracted verbatim from scanAggColHistogramCompact for parallel sharding.
func scanAggColHistogramShard( //nolint:gocyclo
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	start, end int,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
	timeBucketByPos []int32,
	dictIdxByPos []uint32,
	seenByPos []bool,
	getBoundaryIdx func(float64) int64,
	groupCountsFlat []int64,
	stride1, stride2 int64,
	discardStride int64,
) error {
	spanCount := 0
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		// NOTE-135/140: Dict refs are not packKey-sorted; the pkBitset pre-filter + POPCNT rank
		// replace searchSortedUint32. Build hoisted to the driver (NOTE-143).
		for _, entry := range col.DictEntries[start:end] {
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
			if bIdx >= discardStride {
				continue
			}
			for _, ref := range entry.BlockRefs {
				if spanCount%ctxCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				spanCount++
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk < minPK || pk > maxPK {
					continue
				}
				word := pk >> 6
				bit := pk & 63
				if len(pkBitset) > 0 && pkBitset[word]&(uint64(1)<<bit) == 0 {
					continue // NOTE-135/140: fast pre-filter: pk not in sortedPKs
				}
				// NOTE-140: O(1) rank replaces O(log n) searchSortedUint32.
				r := rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1))) //nolint:gosec
				pos := int(r)                                                                         //nolint:gosec
				bk := timeBucketByPos[pos]
				if bk == 0 {
					continue
				}
				seenByPos[pos] = true
				var gIdx int64
				if raw := dictIdxByPos[pos]; raw > 0 {
					gIdx = int64(raw - 1) //nolint:gosec
				}
				groupCountsFlat[gIdx*stride1+bIdx*stride2+int64(bk)-1]++ //nolint:gosec
			}
		}
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatDeltaUint64:
		// NOTE-123/139: DeltaUint64 refs are value-sorted; pkBitset + POPCNT rank replace
		// searchSortedUint32. The i < len(Uint64Values) guard is preserved. Build hoisted (NOTE-143).
		for i := start; i < end; i++ {
			ref := col.BlockRefs[i]
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			if i >= len(col.Uint64Values) {
				continue
			}
			pos, ok := histRefPassPos(ref, minPK, maxPK, pkBitset, rankPrefix)
			if !ok {
				continue
			}
			bk := timeBucketByPos[pos]
			if bk == 0 {
				continue
			}
			bIdx := getBoundaryIdx(float64(col.Uint64Values[i]))
			if bIdx >= discardStride {
				continue
			}
			seenByPos[pos] = true
			var gIdx int64
			if raw := dictIdxByPos[pos]; raw > 0 {
				gIdx = int64(raw - 1) //nolint:gosec
			}
			groupCountsFlat[gIdx*stride1+bIdx*stride2+int64(bk)-1]++ //nolint:gosec
		}
	}
	return nil
}

// buildFrozenBoundaryIdx walks the column in legacy scan order, applying the SAME per-row filters
// the scan applies (pk range + pkBitset membership via histRefPassPos, then timeBucketByPos[pos]!=0),
// driving the caller's original getBoundaryIdx closure ONLY for rows that pass — so the caller's
// boundaryCache/boundaries end up exactly as a serial scan would leave them (emit reads boundaries
// afterward). It then returns a private frozen copy of the boundary→index mapping for race-free
// read-only worker lookups.
//
// The filter is load-bearing: a serial scan appends to boundaries[] in first-encounter order of
// PASSING rows only; recording boundaries for filtered-out rows would corrupt that order and produce
// wrong M8 output for any real filtered query (NOTE-143).
//
// Dict: the legacy Dict scan calls getBoundaryIdx once per ENTRY before its inner ref loop,
// unconditionally (whether or not any ref in the entry passes), so recording on every entry in order
// is already faithful — no filtering is applied to the Dict arm.
func buildFrozenBoundaryIdx(
	col *modules_shared.IntrinsicColumn,
	fieldName string,
	getBoundaryIdx func(float64) int64,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
	timeBucketByPos []int32,
) map[float64]int64 {
	frozen := make(map[float64]int64, 64)
	prewarmBoundaries(col, fieldName, minPK, maxPK, pkBitset, rankPrefix, timeBucketByPos,
		func(v float64) {
			idx := getBoundaryIdx(v) // mutates caller's boundaryCache/boundaries (serial, single-threaded)
			frozen[intrinsicHistogramBoundary(v, fieldName)] = idx
		})
	return frozen
}

// prewarmBoundaries walks the column in legacy scan order applying the SAME per-row filters
// the scan applies, invoking record(v) for exactly the rows (in the order) a serial scan would
// record a boundary for (NOTE-143). Extracted from buildFrozenBoundaryIdx so the boundaryIndexer
// fast path (NOTE-182) can pre-warm its dense exponent table without also building the legacy
// float64 frozen map.
func prewarmBoundaries(
	col *modules_shared.IntrinsicColumn,
	fieldName string,
	minPK, maxPK uint32,
	pkBitset []uint64,
	rankPrefix []uint32,
	timeBucketByPos []int32,
	record func(v float64),
) {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		// Dict entries are walked in order (≤ a few hundred); the legacy scan records per-entry
		// unconditionally, so this is faithful without filtering. Cheap, simple in-order walk.
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, err := strconv.ParseFloat(entry.Value, 64)
				if err != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			record(v)
		}
	case modules_shared.IntrinsicFormatDeltaUint64:
		// NOTE-123/143: DeltaUint64 values are value-sorted ascending and intrinsicHistogramBoundary
		// is monotonic non-decreasing. Apply the SAME per-ref filter the scan does; among PASSING rows
		// (still ascending in value) the boundary is non-decreasing, so record only when a passing
		// row's boundary differs from the previously-recorded passing row's boundary. getBoundaryIdx
		// is idempotent for an already-seen boundary, so this yields a boundaries[] slice byte-identical
		// to driving the closure on every passing row — with O(numBoundaries) transcendental calls.
		var prevBoundary float64
		first := true
		for i := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				break
			}
			pos, ok := histRefPassPos(col.BlockRefs[i], minPK, maxPK, pkBitset, rankPrefix)
			if !ok || timeBucketByPos[pos] == 0 {
				continue
			}
			v := float64(col.Uint64Values[i])
			b := intrinsicHistogramBoundary(v, fieldName)
			if first || b != prevBoundary {
				record(v)
				prevBoundary = b
				first = false
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		// NOTE-143: Flat values are NOT guaranteed value-sorted, so the monotonic shortcut does not
		// apply — walk every PASSING value in BlockRefs/Uint64Values order (the order and filter the
		// scan visits them with) so the first-encounter index assignment matches the legacy serial scan.
		for i := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				break
			}
			pos, ok := histRefPassPos(col.BlockRefs[i], minPK, maxPK, pkBitset, rankPrefix)
			if !ok || timeBucketByPos[pos] == 0 {
				continue
			}
			record(float64(col.Uint64Values[i]))
		}
	}
}

// streamCountRateN1Compact is the compact-memory fallback for count/rate N=1 group-by
// queries when accumulateIntrinsicBucketsDirect fails (maxPK > maxDirectArrayEntries).
//
// The keyMap fallback (accumulateIntrinsicBucketsViaKeyMap) allocates a 150 M-entry
// keyToBucket hash map (~3 GB) plus a maxPK+1-element dictByPK dense array (~944 MB),
// totalling ~4 GB per goroutine for typical production files — approaching the 13 GiB
// GOMEMLIMIT when multiple files are processed concurrently.
//
// This compact path sorts the n in-range refs by packKey once (O(n log n)) and uses
// binary search for group-column lookups (O(log n) per ref). With n ≪ total_refs, the
// sorted array fits comfortably in L3 cache.
//
// Memory profile (7.2 M in-range refs, 281 groups, 1440 steps):
//
//	keyMap path:  keyToBucket(~3 GB) + dictByPK(~944 MB) + dictIdxForRef(28 MB) ≈ 4 GB
//	compact path: pkOrder(57 MB, freed) + sortedPKs(28 MB) + timeBucketByPos(29 MB) +
//	              dictIdxByPos(28 MB) + groupCountsFlat(3.2 MB) ≈ 88 MB peak
//
// NOTE-108: companion to streamHistogramN1Compact for count/rate queries.
func streamCountRateN1Compact(
	ctx context.Context,
	r *modules_reader.Reader,
	tsCol *modules_shared.IntrinsicColumn,
	lo, hi int,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || lo >= hi {
		return nil
	}
	n := hi - lo

	// Sort the in-range refs by packKey for binary search.
	// tsCol.BlockRefs[lo:hi] is timestamp-sorted (flat column), not packKey-sorted.
	// Block scope limits pkOrder lifetime so it can be GC'd before the group-column I/O below.
	// NOTE-125: pool to avoid per-block allocations of sortedPKs (~28 MB) and
	// timeBucketByPos (~29 MB) at n=7.2 M.
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	{
		// Pack pk (high 32 bits) and relative index within tsCol.BlockRefs[lo:hi] (low 32 bits).
		// uint64 is half the size of pkPos{uint32,int} (8 vs 16 bytes/entry), halving sort array
		// memory and moving less data per swap — measurable for n in the millions.
		// NOTE-125: pool pkOrder (~57 MB) — fully overwritten before use, so no clear needed.
		pkOrder := acquireCompactUint64(n)
		for i, ref := range tsCol.BlockRefs[lo:hi] {
			pkOrder[i] = uint64(packKey(ref.BlockIdx, ref.RowIdx))<<32 | uint64(uint32(i)) //nolint:gosec
		}
		// NOTE-175: radix sort by packKey (high 32 bits) — closure-free O(N) vs slices.Sort O(N log N).
		radixSortByPackKey(pkOrder)
		for i, packed := range pkOrder {
			sortedPKs[i] = uint32(packed >> 32)
			relIdx := int(uint32(packed))                                                               //nolint:gosec
			bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+relIdx]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
			if bk >= 0 && bk < numSteps {
				timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
			}
		}
		releaseCompactUint64(pkOrder)
	}

	return streamCountRateN1CompactCore(ctx, r, sortedPKs, timeBucketByPos, agg.GroupBy[0], numSteps, buckets)
}

// streamCountRateN1CompactFromRefs is the predicate-filtered compact path for N=1 count/rate.
// Unlike streamCountRateN1Compact (which sorts tsCol.BlockRefs[lo:hi] by packKey), this function
// receives inRangeRefs already packKey-sorted from mergeJoinFilteredRefsWithVals, so it builds
// sortedPKs and timeBucketByPos in a single O(n) pass — no pkOrder allocation or O(n log n) sort.
//
// The current fallback path (accumulateIntrinsicBucketsViaKeyMap → buildDictIdxForRefs →
// streamByRefSliceCountRate) allocates a keyToBucket map[uint32]int64 (~24 bytes/entry), a
// dictByPK dense array (maxPK+1 × 4 bytes), and dictIdxForRef (n × 4 bytes). For a filtered
// set of 75 M refs this totals ~2 GB. This compact path replaces them with binary search over
// sortedPKs (n × 4 bytes) and a flat groupCounts array.
//
// NOTE-110: predicate-filtered companion to streamCountRateN1Compact.
func streamCountRateN1CompactFromRefs(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || len(inRangeRefs) == 0 {
		return nil
	}
	n := len(inRangeRefs)

	// inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals — build
	// sortedPKs and timeBucketByPos in a single O(n) pass without allocating pkOrder.
	// NOTE-125: pool to avoid per-block allocations of sortedPKs (~28 MB) and
	// timeBucketByPos (~29 MB) at n=7.2 M.
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	for i, ref := range inRangeRefs {
		sortedPKs[i] = packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
		}
	}
	return streamCountRateN1CompactCore(ctx, r, sortedPKs, timeBucketByPos, agg.GroupBy[0], numSteps, buckets)
}

// streamCountRateN1CompactCore is the shared accumulation core for compact N=1 count/rate paths.
// Receives sortedPKs (packKey-sorted) and timeBucketByPos (parallel to sortedPKs, 0=absent sentinel),
// scans the group-by column via binary search, accumulates into a flat 2D array, and emits buckets.
// Called by streamCountRateN1Compact (after sorting tsCol refs) and streamCountRateN1CompactFromRefs
// (with pre-sorted filtered refs from mergeJoinFilteredRefsWithVals).
func streamCountRateN1CompactCore(
	ctx context.Context,
	r *modules_reader.Reader,
	sortedPKs []uint32,
	timeBucketByPos []int32,
	groupByColName string,
	numSteps int64,
	buckets map[string]*aggBucketState,
) error {
	n := len(sortedPKs)

	// Get the group-by column.
	groupByCol, err := r.GetIntrinsicColumn(groupByColName)
	if err != nil {
		return err
	}

	// Build group dict and dictIdxByPos by scanning the group-by column.
	// NOTE-125: pool dictIdxByPos (~28 MB at n=7.2 M) — absent entries are 0 (sentinel),
	// acquireCompactUint32 clears before use.
	dict := []string{""}
	dictIdxByPos := acquireCompactUint32(n)
	defer releaseCompactUint32(dictIdxByPos)
	if groupByCol != nil {
		valToIdx := make(map[string]uint32, 32)
		scanGroupByColCompact(groupByCol, groupByColName, sortedPKs, &dict, valToIdx, dictIdxByPos)
	}
	numGroups := int64(len(dict)) //nolint:gosec

	// NOTE-124: pool to avoid per-block allocation of numGroups×numSteps int64 flat array.
	groupCountsFlat := acquireGroupCountsFlat(numGroups * numSteps)
	defer releaseGroupCountsFlat(groupCountsFlat)

	// Hot loop: iterate sortedPK positions sequentially.
	// dictIdxByPos and timeBucketByPos are accessed sequentially → cache-friendly.
	// groupCountsFlat access: for runs of same-group refs (sorted by packKey),
	// gIdx stays constant → numSteps-element subarray stays in L1/L2 cache.
	spanCount := 0
	for pos := range n {
		if spanCount%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		spanCount++
		bk := timeBucketByPos[pos]
		if bk == 0 {
			continue
		}
		gIdx := int64(0)
		if raw := dictIdxByPos[pos]; raw > 0 {
			gIdx = int64(raw - 1) //nolint:gosec
		}
		groupCountsFlat[gIdx*numSteps+int64(bk)-1]++ //nolint:gosec
	}

	// Emit non-zero entries to buckets.
	for gIdx := range numGroups {
		gk := dict[gIdx]
		base := gIdx * numSteps
		hasAny := false
		for bk := range numSteps {
			if groupCountsFlat[base+bk] > 0 {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}
		for bk := range numSteps {
			c := groupCountsFlat[base+bk]
			if c == 0 {
				continue
			}
			k := strconv.FormatInt(bk, 10) + "\x00" + gk
			intrinsicGetOrCreateBucket(buckets, k).count += c
		}
	}
	return nil
}

// streamAggN1Compact is the compact-memory fallback for general agg (min/max/sum/avg/etc.)
// N=1 group-by queries when accumulateIntrinsicBucketsDirect fails (maxPK > maxDirectArrayEntries).
//
// The keyMap fallback (accumulateIntrinsicBucketsViaKeyMap) allocates:
//
//   - keyToBucket hash map: ~3 GB for 150 M in-range spans
//   - dictByPK dense array: up to 128 MB (maxPK+1 × 4 bytes)
//   - valByPK + hasByPK from buildAggValsForRef: up to 288 MB (maxPK+1 × 9 bytes)
//
// totalling ~3.4 GB per goroutine for typical large production files.
//
// This compact path sorts the n in-range refs by packKey once (O(n log n)) and uses
// binary search for group and aggregate column lookups (O(log n) per ref).
//
// Memory profile (7.2 M in-range refs, 281 groups, 1440 steps):
//
//	keyMap path:  keyToBucket(~3 GB) + dictByPK(128 MB) + valByPK+hasByPK(288 MB) ≈ 3.4 GB
//	compact path: pkOrder(86 MB, freed) + sortedPKs(28 MB) + timeBucketByPos(29 MB) +
//	              dictIdxByPos(28 MB) + aggValByPos(57 MB) + aggPresentByPos(7 MB) ≈ 235 MB peak
//
// NOTE-109: companion to streamCountRateN1Compact for general agg (non-count/rate, non-histogram).
func streamAggN1Compact(
	ctx context.Context,
	r *modules_reader.Reader,
	tsCol *modules_shared.IntrinsicColumn,
	lo, hi int,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || lo >= hi {
		return nil
	}
	n := hi - lo

	// Sort the in-range refs by packKey for binary search.
	// Block scope limits pkOrder lifetime so it can be GC'd before the column I/O below.
	// NOTE-125: pool to avoid per-block allocations of sortedPKs (~28 MB) and
	// timeBucketByPos (~29 MB) at n=7.2 M.
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	{
		// Pack pk (high 32 bits) and relative index within tsCol.BlockRefs[lo:hi] (low 32 bits).
		// uint64 is half the size of pkPos{uint32,int} (8 vs 16 bytes/entry).
		// NOTE-125: pool pkOrder (~57 MB) — fully overwritten before use, so no clear needed.
		pkOrder := acquireCompactUint64(n)
		for i, ref := range tsCol.BlockRefs[lo:hi] {
			pkOrder[i] = uint64(packKey(ref.BlockIdx, ref.RowIdx))<<32 | uint64(uint32(i)) //nolint:gosec
		}
		// NOTE-175: radix sort by packKey (high 32 bits) — closure-free O(N) vs slices.Sort O(N log N).
		radixSortByPackKey(pkOrder)
		for i, packed := range pkOrder {
			sortedPKs[i] = uint32(packed >> 32)
			relIdx := int(uint32(packed))                                                               //nolint:gosec
			bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+relIdx]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
			if bk >= 0 && bk < numSteps {
				timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
			}
		}
		releaseCompactUint64(pkOrder)
	}

	// Get the group-by column.
	groupByCol, err := r.GetIntrinsicColumn(agg.GroupBy[0])
	if err != nil {
		return err
	}

	// Build group dict and dictIdxByPos by scanning the group-by column.
	// NOTE-125: pool dictIdxByPos (~28 MB at n=7.2 M).
	dict := []string{""}
	dictIdxByPos := acquireCompactUint32(n)
	defer releaseCompactUint32(dictIdxByPos)
	if groupByCol != nil {
		valToIdx := make(map[string]uint32, 32)
		scanGroupByColCompact(groupByCol, agg.GroupBy[0], sortedPKs, &dict, valToIdx, dictIdxByPos)
	}
	numGroups := len(dict)

	// Get aggregate column and populate aggValByPos/aggPresentByPos via binary search.
	// NOTE-125: pool aggValByPos (~57 MB) and aggPresentByPos (~7 MB) at n=7.2 M.
	aggValByPos := acquireCompactFloat64(n)
	defer releaseCompactFloat64(aggValByPos)
	aggPresentByPos := acquireCompactBool(n)
	defer releaseCompactBool(aggPresentByPos)
	if agg.Field != "" {
		aggCol, aggErr := r.GetIntrinsicColumn(agg.Field)
		if aggErr != nil {
			return aggErr
		}
		if aggCol != nil {
			scanAggColCompact(aggCol, sortedPKs, aggValByPos, aggPresentByPos)
		}
	}

	// Accumulate into groupBuckets[gIdx][bk-1].
	groupBuckets := make([][]*aggBucketState, numGroups)
	for i := range groupBuckets {
		groupBuckets[i] = make([]*aggBucketState, numSteps)
	}

	spanCount := 0
	for pos := range n {
		if spanCount%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		spanCount++
		bk := timeBucketByPos[pos]
		if bk == 0 {
			continue
		}
		gIdx := 0
		if raw := dictIdxByPos[pos]; raw > 0 {
			gIdx = int(raw - 1) //nolint:gosec
		}
		if gIdx >= numGroups {
			continue
		}
		if groupBuckets[gIdx][bk-1] == nil {
			groupBuckets[gIdx][bk-1] = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
		}
		if aggPresentByPos[pos] {
			updateAggBucket(groupBuckets[gIdx][bk-1], agg.Function, aggValByPos[pos])
		}
		// absent field: bucket stays count=0, emits NaN — matches streamByRefSliceAgg behavior
	}

	// Emit.
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

// streamAggN1CompactFromRefs is the predicate-filtered compact path for N=1 general agg queries.
// Analogous to streamCountRateN1CompactFromRefs (NOTE-110) but for min/max/sum/avg/etc.
// inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals — builds
// sortedPKs/timeBucketByPos in O(n) without allocating or sorting pkOrder.
//
// The accumulateIntrinsicBucketsViaKeyMap fallback allocates:
//
//   - keyToBucket map[uint32]int64: ~24 bytes/entry × filtered spans
//   - buildDictIdxForRefs: dictByPK (maxPK+1 × 4 bytes) + dictIdxForRef (n × 4 bytes)
//   - buildAggValsForRef: aggVals (n × 8 bytes) + aggPresent (n × 1 byte)
//
// For 3.75 M filtered refs totalling ~203 MB. This compact path replaces them with
// sortedPKs (n × 4) + timeBucketByPos (n × 8) + dictIdxByPos (n × 4) + aggValByPos (n × 8)
// + aggPresentByPos (n × 1) ≈ 94 MB — 53% reduction.
//
// NOTE-112: predicate-filtered companion to streamAggN1Compact.
func streamAggN1CompactFromRefs(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || len(inRangeRefs) == 0 {
		return nil
	}
	n := len(inRangeRefs)

	// inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals.
	// NOTE-125: pool to avoid per-block allocations (~28 MB sortedPKs, ~29 MB timeBucketByPos).
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	for i, ref := range inRangeRefs {
		sortedPKs[i] = packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
		}
	}

	groupByCol, err := r.GetIntrinsicColumn(agg.GroupBy[0])
	if err != nil {
		return err
	}

	// NOTE-125: pool dictIdxByPos (~28 MB), aggValByPos (~57 MB), aggPresentByPos (~7 MB).
	dict := []string{""}
	dictIdxByPos := acquireCompactUint32(n)
	defer releaseCompactUint32(dictIdxByPos)
	if groupByCol != nil {
		valToIdx := make(map[string]uint32, 32)
		scanGroupByColCompact(groupByCol, agg.GroupBy[0], sortedPKs, &dict, valToIdx, dictIdxByPos)
	}
	numGroups := len(dict)

	aggValByPos := acquireCompactFloat64(n)
	defer releaseCompactFloat64(aggValByPos)
	aggPresentByPos := acquireCompactBool(n)
	defer releaseCompactBool(aggPresentByPos)
	if agg.Field != "" {
		aggCol, aggErr := r.GetIntrinsicColumn(agg.Field)
		if aggErr != nil {
			return aggErr
		}
		if aggCol != nil {
			scanAggColCompact(aggCol, sortedPKs, aggValByPos, aggPresentByPos)
		}
	}

	groupBuckets := make([][]*aggBucketState, numGroups)
	for i := range groupBuckets {
		groupBuckets[i] = make([]*aggBucketState, numSteps)
	}

	spanCount := 0
	for pos := range n {
		if spanCount%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		spanCount++
		bk := timeBucketByPos[pos]
		if bk == 0 {
			continue
		}
		gIdx := 0
		if raw := dictIdxByPos[pos]; raw > 0 {
			gIdx = int(raw - 1) //nolint:gosec
		}
		if gIdx >= numGroups {
			continue
		}
		if groupBuckets[gIdx][bk-1] == nil {
			groupBuckets[gIdx][bk-1] = &aggBucketState{min: math.MaxFloat64, max: -math.MaxFloat64}
		}
		if aggPresentByPos[pos] {
			updateAggBucket(groupBuckets[gIdx][bk-1], agg.Function, aggValByPos[pos])
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

// streamHistogramN1CompactFromRefs is the predicate-filtered companion to streamHistogramN1Compact.
//
// NOTE-114: inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals,
// so sortedPKs/timeBucketByPos are built in O(n) without allocating pkOrder or sorting.
// Bypasses accumulateIntrinsicBucketsViaKeyMap entirely (saving ~90 MB keyToBucket hash map
// build for 3.75 M filtered refs) and eliminates the O(n log n) pkOrder sort that
// streamHistogramN1Compact performs for the unfiltered case.
//
// Memory profile (3.75 M filtered refs, 280 groups, 1440 steps, ~20 boundaries):
//
//	keyMap path: keyToBucket(~90 MB) + pkOrder(~29 MB sort) + sortedPKs(~15 MB) ≈ 134 MB
//	compact path (this): sortedPKs(~15 MB) + timeBucketByPos(~15 MB) + dictIdxByPos(~15 MB) ≈ 45 MB
//
// Queries affected: M8 {span.kind = server} | histogram_over_time(duration) by (service.name).
func streamHistogramN1CompactFromRefs(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) error {
	agg := querySpec.Aggregate
	tb := querySpec.TimeBucketing
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || len(inRangeRefs) == 0 {
		return nil
	}
	n := len(inRangeRefs)

	// inRangeRefs is already packKey-sorted from mergeJoinFilteredRefsWithVals.
	// Build sortedPKs and timeBucketByPos in O(n) without pkOrder alloc or sort.
	// NOTE-125: pool to avoid per-block allocations (~28 MB sortedPKs, ~29 MB timeBucketByPos).
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	for i, ref := range inRangeRefs {
		sortedPKs[i] = packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
		}
	}

	groupByCol, err := r.GetIntrinsicColumn(agg.GroupBy[0])
	if err != nil {
		return err
	}

	// NOTE-125: pool dictIdxByPos (~28 MB) and seenByPos (~7 MB).
	dict := []string{""}
	dictIdxByPos := acquireCompactUint32(n)
	defer releaseCompactUint32(dictIdxByPos)
	if groupByCol != nil {
		valToIdx := make(map[string]uint32, 32)
		scanGroupByColCompact(groupByCol, agg.GroupBy[0], sortedPKs, &dict, valToIdx, dictIdxByPos)
	}
	numGroups := len(dict)

	aggCol, aggErr := r.GetIntrinsicColumn(agg.Field)
	if aggErr != nil {
		return aggErr
	}
	var actualStride int64
	if aggCol != nil {
		actualStride = int64(countIntrinsicHistogramBoundaries(aggCol, agg.Field)) + 1
	} else {
		actualStride = 1
	}
	stride2 := numSteps
	stride1 := actualStride * numSteps
	// NOTE-124: pool to avoid per-block allocation of numGroups×stride1 int64 array (up to 68MB).
	groupCountsFlat := acquireGroupCountsFlat(int64(numGroups) * stride1) //nolint:gosec
	defer releaseGroupCountsFlat(groupCountsFlat)

	// NOTE-182: exponent-indexed boundary lookup replaces the per-row map[float64]int64.
	bi := newBoundaryIndexer(agg.Field, actualStride)
	getBoundaryIdx := bi.index

	// NOTE-125: pool seenByPos (~7 MB at n=7.2 M).
	seenByPos := acquireCompactBool(n)
	defer releaseCompactBool(seenByPos)
	if aggCol != nil {
		if err := scanAggColHistogramCompact(
			ctx, aggCol, sortedPKs, timeBucketByPos, dictIdxByPos, seenByPos,
			getBoundaryIdx, groupCountsFlat, stride1, stride2, actualStride, agg.Field, bi.lookup,
		); err != nil {
			return err
		}
	}
	boundaries := bi.boundaries

	// Absent-row pass: positions not seen in the aggregate column → boundary-0 bucket.
	for pos, seen := range seenByPos {
		if seen {
			continue
		}
		bk := timeBucketByPos[pos]
		if bk == 0 {
			continue
		}
		var gIdx int64
		if raw := dictIdxByPos[pos]; raw > 0 {
			gIdx = int64(raw - 1) //nolint:gosec
		}
		if gIdx < int64(numGroups) { //nolint:gosec
			groupCountsFlat[gIdx*stride1+int64(bk)-1]++ //nolint:gosec
		}
	}

	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, numGroups, dict, boundaries, buckets)
}

// scanAggColCompact scans an intrinsic column and populates aggValByPos/aggPresentByPos for each
// packKey that appears in sortedPKs (sorted ascending), using a POPCNT rank index (NOTE-141).
// Matches buildAggValsForRef semantics: for dict parse failures, fval=0 but present=true.
func scanAggColCompact(
	col *modules_shared.IntrinsicColumn,
	sortedPKs []uint32,
	aggValByPos []float64,
	aggPresentByPos []bool,
) {
	if len(sortedPKs) == 0 {
		return
	}
	minPK, maxPK := sortedPKs[0], sortedPKs[len(sortedPKs)-1]

	// NOTE-141: build pkBitset + POPCNT rank index from sortedPKs, mirroring NOTE-139/140
	// in scanGroupByColCompact / scanAggColHistogramCompact. Replaces searchSortedUint32 in
	// both the Dict and Flat/DeltaUint64 inner loops with an O(1) rank lookup, and pre-filters
	// non-member refs with the bitset before any rank work.
	var pkBitset []uint64
	var rankPrefix []uint32
	if len(sortedPKs) > 0 {
		n := int((maxPK >> 6) + 1) //nolint:gosec
		pkBitset = acquireCompactUint64(n)
		defer releaseCompactUint64(pkBitset)
		clear(
			pkBitset,
		) // NOTE-141: zero-sentinel — acquireCompactUint64 does NOT clear; stale bits would false-positive the pre-filter and corrupt rankPrefix
		for _, pk := range sortedPKs {
			pkBitset[pk>>6] |= uint64(1) << (pk & 63)
		}
		// NOTE-141: POPCNT rank index. rankPrefix[i] = cumulative popcount of pkBitset[0..i-1].
		// Build cost: O(maxPK/64) ≈ 250K iterations at maxPK=16M → ~0.5µs per block.
		// NOTE-195: fully overwritten before read (see scanGroupByColCompact) — skip the clear.
		rankPrefix = acquireCompactUint32NoClear(n + 1)
		defer releaseCompactUint32(rankPrefix)
		var cum uint32
		for i, w := range pkBitset {
			rankPrefix[i] = cum
			cum += uint32(bits.OnesCount64(w)) //nolint:gosec
		}
		rankPrefix[len(pkBitset)] = cum
	}

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var fval float64
			if entry.Value != "" {
				// Match buildAggValsForRef: parse failure keeps fval=0 but still marks present.
				if v, parseErr := strconv.ParseFloat(entry.Value, 64); parseErr == nil {
					fval = v
				}
			} else {
				fval = float64(entry.Int64Val)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if pk < minPK || pk > maxPK {
					continue
				}
				word := pk >> 6
				bit := pk & 63
				if pkBitset[word]&(uint64(1)<<bit) == 0 {
					continue // NOTE-141: bitset pre-filter — pk not in sortedPKs
				}
				// NOTE-141: O(1) rank replaces O(log n) searchSortedUint32.
				r := rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1))) //nolint:gosec
				pos := int(r)                                                                         //nolint:gosec
				aggValByPos[pos] = fval
				aggPresentByPos[pos] = true
			}
		}
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatDeltaUint64:
		for i, ref := range col.BlockRefs {
			if i >= len(col.Uint64Values) {
				continue
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk < minPK || pk > maxPK {
				continue
			}
			word := pk >> 6
			bit := pk & 63
			if pkBitset[word]&(uint64(1)<<bit) == 0 {
				continue // NOTE-141: bitset pre-filter — pk not in sortedPKs
			}
			// NOTE-141: O(1) rank replaces O(log n) searchSortedUint32.
			pos := int(rankPrefix[word] + uint32(bits.OnesCount64(pkBitset[word]&((uint64(1)<<bit)-1)))) //nolint:gosec
			aggValByPos[pos] = float64(col.Uint64Values[i])                                              //nolint:gosec
			aggPresentByPos[pos] = true
		}
	}
}

// streamHistogramN1Compact accumulates histogram counts for the N=1 group-by case
// without allocating large dense arrays indexed by packKey.
//
// The dense path (streamByRefSliceHistogram) allocates three arrays of size maxPK+1:
// dictByPK (128 MB), bucketByPK (262 MB), and seenByPK (32 MB) for a file with 500
// blocks × 2000 rows/block, even though only ~1 M spans are actually present. This
// compact path replaces them with arrays of size len(inRangeRefs) and uses binary
// search (O(log N) instead of O(1)) to locate spans in the aggregate column.
//
// Memory profile (1 M refs, 280 groups, 1440 steps, 30 boundaries):
//
//	dense:   dictByPK(128 MB) + bucketByPK(262 MB) + seenByPK(32 MB) + groupCountsFlat(98 MB) ≈ 520 MB
//	compact: sortedPKs(4 MB) + timeBuckets(4 MB) + dictIdxByPos(4 MB) + seenByPos(1 MB) + groupCountsFlat(98 MB) ≈ 111 MB
//
// NOTE-092: replaces the dense-array histogram path for N=1 group-by in accumulateIntrinsicBuckets.
func streamHistogramN1Compact(
	ctx context.Context,
	r *modules_reader.Reader,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	groupByCol *modules_shared.IntrinsicColumn,
	agg vm.AggregateSpec,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	if len(inRangeRefs) == 0 {
		return nil
	}

	n := len(inRangeRefs)

	// Sort inRangeRefs by packKey so binary search works.
	// inRangeRefs from merge-join is already packKey-sorted; timestamp-sorted otherwise.
	// A sort-then-binary-search pattern is correct for both orderings.
	// Block scope limits pkOrder lifetime so it can be GC'd before the group-column I/O below.
	// NOTE-125: pool to avoid per-block allocations of sortedPKs (~28 MB) and
	// timeBucketByPos (~29 MB) at n=7.2 M.
	sortedPKs := acquireCompactUint32NoClear(n)
	defer releaseCompactUint32(sortedPKs)
	timeBucketByPos := acquireCompactInt32(n) // sentinel 0 = out of range; 1..numSteps = bucket+1 (NOTE-116)
	defer releaseCompactInt32(timeBucketByPos)
	{
		// Pack pk (high 32 bits) and index within inRangeRefs (low 32 bits).
		// uint64 is half the size of pkPos{uint32,int} (8 vs 16 bytes/entry).
		// NOTE-125: pool pkOrder (~57 MB) — fully overwritten before use, so no clear needed.
		pkOrder := acquireCompactUint64(n)
		for i, ref := range inRangeRefs {
			pkOrder[i] = uint64(packKey(ref.BlockIdx, ref.RowIdx))<<32 | uint64(uint32(i)) //nolint:gosec
		}
		// NOTE-175: radix sort by packKey (high 32 bits) — closure-free O(N) vs slices.Sort O(N log N).
		radixSortByPackKey(pkOrder)
		for i, packed := range pkOrder {
			sortedPKs[i] = uint32(packed >> 32)
			relIdx := int(uint32(packed))                                                     //nolint:gosec
			bk := timeBucketIndex(int64(inRangeVals[relIdx]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
			if bk >= 0 && bk < numSteps {
				timeBucketByPos[i] = int32(bk + 1) //nolint:gosec
			}
		}
		releaseCompactUint64(pkOrder)
	}

	// Build group dict and dictIdxByPos by scanning the group-by column.
	// dict[0]="" is the absent/default group; dict[1..] are actual group values.
	// NOTE-125: pool dictIdxByPos (~28 MB) and seenByPos (~7 MB).
	dict := []string{""}
	dictIdxByPos := acquireCompactUint32(n)
	defer releaseCompactUint32(dictIdxByPos)
	if groupByCol != nil {
		valToIdx := make(map[string]uint32, 32)
		scanGroupByColCompact(groupByCol, agg.GroupBy[0], sortedPKs, &dict, valToIdx, dictIdxByPos)
	}
	numGroups := len(dict)

	// Fetch aggregate column and pre-scan boundaries.
	aggCol, aggErr := r.GetIntrinsicColumn(agg.Field)
	if aggErr != nil {
		return aggErr
	}
	var actualStride int64
	if aggCol != nil {
		actualStride = int64(countIntrinsicHistogramBoundaries(aggCol, agg.Field)) + 1
	} else {
		actualStride = 1
	}
	stride2 := numSteps
	stride1 := actualStride * numSteps
	// NOTE-124: pool to avoid per-block allocation.
	groupCountsFlat := acquireGroupCountsFlat(int64(numGroups) * stride1) //nolint:gosec
	defer releaseGroupCountsFlat(groupCountsFlat)

	// NOTE-182: exponent-indexed boundary lookup replaces the per-row map[float64]int64.
	bi := newBoundaryIndexer(agg.Field, actualStride)
	getBoundaryIdx := bi.index

	// NOTE-125: pool seenByPos (~7 MB at n=7.2 M).
	seenByPos := acquireCompactBool(n)
	defer releaseCompactBool(seenByPos)
	if aggCol != nil {
		if err := scanAggColHistogramCompact(ctx, aggCol, sortedPKs, timeBucketByPos, dictIdxByPos, seenByPos, getBoundaryIdx, groupCountsFlat, stride1, stride2, actualStride, agg.Field, bi.lookup); err != nil {
			return err
		}
	}
	boundaries := bi.boundaries

	// Absent-row pass: positions not seen in the aggregate column → bIdx=0 sentinel.
	for pos, seen := range seenByPos {
		if seen {
			continue
		}
		bk := timeBucketByPos[pos]
		if bk == 0 {
			continue
		}
		var gIdx int64
		if raw := dictIdxByPos[pos]; raw > 0 {
			gIdx = int64(raw - 1) //nolint:gosec
		}
		if gIdx < int64(numGroups) { //nolint:gosec
			groupCountsFlat[gIdx*stride1+int64(bk)-1]++ //nolint:gosec
		}
	}

	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, numGroups, dict, boundaries, buckets)
}

// mergeJoinFilteredRefsWithVals returns the subset of (inRangeRefs, inRangeVals)
// whose packKey appears in filteredRefs.
//
// NOTE-070: Replaces filteredKeys map[uint32]struct{} to eliminate hash allocation
// and O(N_all) hash lookups on the filtered intrinsic path.
//
// Sort invariant: inRangeRefs is timestamp-sorted (types.go:228), not packKey-sorted.
// filteredRefs order is unspecified (intersectBlockRefSets may return any order).
// The returned outRefs/outVals are packKey-sorted (NOTE-166): downstream compact paths
// (NOTE-110/112/114) build sortedPKs in O(n) assuming this order.
//
// Mutation rules:
//   - filteredRefs is copied into a sorted []uint32 of packKeys (caller's slice is not modified).
//   - inRangeRefs is NOT sorted in-place (it is a sub-slice of a shared intrinsic
//     column). Matches are collected as packed (pk<<32 | pos) and only that subset is
//     sorted, so inRangeVals alignment is preserved via the recorded positions.
//
// NOTE-130: outRefs/outVals are pooled. Caller must invoke release() after consuming the slices.
func mergeJoinFilteredRefsWithVals(
	filteredRefs []modules_shared.BlockRef,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
) (outRefs []modules_shared.BlockRef, outVals []uint64, release func()) {
	release = func() {}
	if len(filteredRefs) == 0 || len(inRangeRefs) == 0 {
		return nil, nil, release
	}

	// Build a []uint32 of filteredRefs packKeys — cheaper than cloning BlockRef
	// (same 4 bytes/entry; packKey is pre-computed so the bitset build below avoids
	// recomputing it per entry).
	//
	// NOTE-203: drop the slices.Sort(filteredPKs). The only consumers of filteredPKs are
	// (a) the [flo, fhi] range gate, which needs the min and max packKey, and (b) the
	// bit-set build loop, which sets one bit per packKey and is order-independent. The
	// former O(N log N) sort existed solely so flo/fhi could be read from the first/last
	// element of the sorted slice. min/max is an O(N) reduction, so we fold it into the
	// same pass that computes the packKeys — removing the comparison sort entirely (the
	// querier CPU profile attributed ~1.6% self-time to mergeJoinFilteredRefsWithVals, of
	// which slices.Sort over the F-element filtered set is the dominant component on the
	// predicate-filtered metrics path). The bitset and every downstream consumer are
	// byte-for-byte identical: bit membership does not depend on the order filteredPKs was
	// produced in, and flo/fhi are the same min/max either way.
	filteredPKs := acquireCompactUint32(
		len(filteredRefs),
	) // NOTE-128: ~15 MB at F=3.75 M; pooled to eliminate GC pressure
	flo := packKey(filteredRefs[0].BlockIdx, filteredRefs[0].RowIdx)
	fhi := flo
	for i, ref := range filteredRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		filteredPKs[i] = pk
		if pk < flo {
			flo = pk
		}
		if pk > fhi {
			fhi = pk
		}
	}

	// NOTE-166: probe-then-sort-matches. The former implementation built a packed
	// []uint64 index over ALL N in-range refs (idxPacked, ~57 MB at N=7.2 M) and ran
	// slices.Sort over that full array — an O(N log N) sort. NOTE-166 replaced that with a
	// per-survivor binary search of filteredPKs, then sorted only the matched subset.
	//
	// NOTE-167: replace NOTE-166's per-survivor sort.Search with an O(1) membership bit-test.
	// A 2026-06-10 querier CPU profile (post-NOTE-166) attributed sort.Search at ~4.93% of
	// TOTAL querier CPU — now the single largest self-cost — reached via this function's
	// per-in-range-ref probe of filteredPKs. The closure-based sort.Search performs ~log2(F)
	// (~22 at F=3.75 M) indirect-call probes per survivor. Instead, build a presence bitset
	// over filteredPKs (one bit per packKey, indexed by pk; ~maxPK/64 words) plus the [flo,fhi]
	// range gate, then test membership with a single shift+mask+AND per in-range ref — O(1),
	// branch-light, no closure call, no log2(F) probes. This is the same pkBitset technique
	// already proven on the group-by/histogram compact scan (scanGroupByColCompact NOTE-135/140
	// and scanAggColHistogramCompact). The bitset spans only the [flo, fhi] packKey range
	// (offset by flo>>6 so unused low words are not allocated). filteredPKs is no longer needed
	// after the bitset is built, so it is released early. flo/fhi were computed in the
	// packKey build pass above (NOTE-203) rather than read from a now-removed sorted slice.
	// Bitset indexed by (pk - baseWord*64); word baseWord covers flo, so it spans only the
	// active range. nWords = (fhi>>6) - (flo>>6) + 1.
	baseWord := int(flo >> 6) //nolint:gosec
	nWords := int(fhi>>6) - baseWord + 1
	pkBitset := acquireCompactUint64(nWords) // pooled
	clear(pkBitset)                          // zero-sentinel — must clear stale pool bits before setting
	for _, pk := range filteredPKs {
		w := int(pk>>6) - baseWord //nolint:gosec
		pkBitset[w] |= uint64(1) << (pk & 63)
	}
	releaseCompactUint32(filteredPKs)

	// NOTE-194: size `matched` to F (=len(filteredRefs)) rather than N (=len(inRangeRefs)).
	// A matched entry is an in-range ref whose packKey is present in the filtered set. Within a
	// block group's intrinsic scan inRangeRefs are per-span (distinct blockIdx/rowIdx, hence
	// distinct packKeys), and each match's packKey is a member of filteredPKs (F distinct keys),
	// so the match count m satisfies m ≤ min(N, F). On the predicate-filtered metrics path
	// (M6/M9/M10 and any `... by (...)` with a predicate) F ≪ N — span:duration's in-range set
	// spans every matching-time span while filteredRefs is just the predicate survivors — so
	// sizing to F instead of N collapses a multi-MB pooled allocation (NOTE-128: ~57 MB at
	// N=7.2 M) to F entries. The defensive grow guard below keeps the result correct even if a
	// future caller ever passed inRangeRefs containing duplicate packKeys (m > F): we grow rather
	// than drop matches, so output is byte-identical to the prior len(inRangeRefs) sizing.
	matchedCap := len(filteredRefs)
	if matchedCap > len(inRangeRefs) {
		matchedCap = len(inRangeRefs)
	}
	matched := acquireCompactUint64(matchedCap) // cap min(N,F); trimmed to m below. Pooled.
	m := 0
	for i, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		// Skip the bit-test entirely for packKeys outside [flo, fhi]; the merge-join
		// frequently sees in-range refs whose blocks were pruned away by the predicate.
		if pk < flo || pk > fhi {
			continue
		}
		// O(1) membership: a single word fetch + shift + mask, no closure / no log2(F) probes.
		w := int(pk>>6) - baseWord //nolint:gosec
		if pkBitset[w]&(uint64(1)<<(pk&63)) != 0 {
			if m == len(matched) {
				// Defensive: only reachable if inRangeRefs held duplicate packKeys (m > min(N,F)).
				// Grow to N (the strict upper bound) so no match is ever dropped.
				grown := acquireCompactUint64(len(inRangeRefs))
				copy(grown, matched[:m])
				releaseCompactUint64(matched)
				matched = grown
			}
			matched[m] = uint64(pk)<<32 | uint64(uint32(i)) //nolint:gosec
			m++
		}
	}
	releaseCompactUint64(pkBitset)
	matched = matched[:m]
	// Restore packKey-sorted output order. M is the match count (≤ F ≪ N), so this sort is
	// far cheaper than sorting the full N-element in-range array.
	// NOTE-175: radix sort by packKey (high 32 bits) over the matched subset.
	radixSortByPackKey(matched)

	outRefsBacking := acquireCompactBlockRef(m) // NOTE-130: pooled
	outValsBacking := acquireCompactUint64(m)   // NOTE-130: pooled; reuses compactUint64Pool
	outRefs = outRefsBacking[:0]
	outVals = outValsBacking[:0]
	release = func() {
		releaseCompactBlockRef(outRefsBacking)
		releaseCompactUint64(outValsBacking)
	}

	for _, packed := range matched {
		pos := int(uint32(packed)) //nolint:gosec
		outRefs = append(outRefs, inRangeRefs[pos])
		outVals = append(outVals, inRangeVals[pos])
	}
	releaseCompactUint64(matched)
	return outRefs, outVals, release
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

// streamCountRateNoGroupBySorted is the O(numSteps · log N) fast path for the unfiltered
// N=0 count/rate query (e.g. M1 `{} | rate()`). It replaces streamCountRateNoGroupBy's
// O(N) per-span timeBucketIndex loop (~150M iterations for M1, NOTE-068) with one binary
// search per bucket boundary.
//
// NOTE-149: span:start is a flat ascending-sorted column (types.go) and timeBucketIndex is
// monotonically non-decreasing in ts, so the in-range timestamps fall into contiguous,
// non-overlapping runs — one run per bucket. The count for bucket b is therefore the number
// of timestamps in (StartTime+b·step, StartTime+(b+1)·step], which a single sort.Search over
// the remaining sorted tail finds directly. Only valid when the input is timestamp-sorted:
// the predicate-filtered path receives packKey-sorted refs from mergeJoinFilteredRefsWithVals
// and must keep the linear loop; this is dispatched only when filteredRefs == nil.
//
// Boundary derivation: timeBucketIndex(ts)=(ts-StartTime-1)/step, so idx==b ⟺
// StartTime+b·step < ts ≤ StartTime+(b+1)·step. The upper bound (inclusive) for bucket b is
// StartTime+(b+1)·step; `val > boundary` places val==boundary into bucket b (right-closed),
// matching streamCountRateNoGroupBy exactly. Callers guarantee every val ∈ (StartTime,EndTime]
// via the lo/hi binary search, so all timestamps land in [0,numSteps) with no clamping needed.
func streamCountRateNoGroupBySorted(
	ctx context.Context,
	sortedVals []uint64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n := len(sortedVals)
	prev := 0 // first index not yet assigned to a bucket
	for b := int64(0); b < numSteps && prev < n; b++ {
		boundary := tb.StartTime + (b+1)*tb.StepSizeNanos
		tail := sortedVals[prev:]
		// First offset in tail whose value exceeds the bucket's inclusive upper bound.
		off := sort.Search(len(tail), func(i int) bool {
			return int64(tail[i]) > boundary //nolint:gosec
		})
		if off > 0 {
			key := strconv.FormatInt(b, 10) + "\x00"
			intrinsicGetOrCreateBucket(buckets, key).count = int64(off)
		}
		prev += off
	}
	return nil
}

// streamCountRateN0HashFilter is the fast path for N=0 count/rate with a selective predicate.
// Instead of merge-sort-joining N in-range refs with F filtered refs (O(N log N + F log F)),
// it builds a hash set from filteredRefs and scans tsCol[lo:hi] sequentially (O(N + F)).
//
// NOTE-113: Only used when F ≤ N/4 (25% selectivity), so the hash set (F × ~20 bytes)
// fits in L3 cache and lookups are fast. Example: service.name = "grafana" matches
// ~500K of 7.5M in-range spans; hash map = 10 MB (fits in L3 easily).
//
// Unlike streamCountRateNoGroupBy (which receives already-merged inRangeRefs),
// this function reads directly from tsCol[lo:hi] and filters inline — eliminating
// the O(N log N) pkOrder sort that mergeJoinFilteredRefsWithVals would perform.
func streamCountRateN0HashFilter(
	ctx context.Context,
	tsCol *modules_shared.IntrinsicColumn,
	lo, hi int,
	filteredRefs []modules_shared.BlockRef,
	tb vm.TimeBucketSpec,
	querySpec *vm.QuerySpec,
) (*TraceMetricsResult, bool, error) {
	numSteps := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numSteps <= 0 || lo >= hi || len(filteredRefs) == 0 {
		return &TraceMetricsResult{}, true, nil
	}

	// NOTE-136: Replace filteredPKs map with pkBitset from filteredRefs.
	// map[uint32]struct{} at 24B/entry costs ~12 MB for 500K refs; a bitset costs 2 MB at
	// maxPK=16M — same pattern as NOTE-134/135 in scanAggColHistogramCompact/scanGroupByColCompact.
	var maxPK uint32
	for _, ref := range filteredRefs {
		if pk := packKey(ref.BlockIdx, ref.RowIdx); pk > maxPK {
			maxPK = pk
		}
	}
	var pkBitset []uint64
	// len(filteredRefs) > 0 is guaranteed by the early-return above; guard handles maxPK=0 edge
	if len(filteredRefs) > 0 {
		n := int((maxPK >> 6) + 1) //nolint:gosec
		pkBitset = acquireCompactUint64(n)
		defer releaseCompactUint64(pkBitset)
		clear(pkBitset) // NOTE-136: zero-sentinel — must clear stale pool bits before setting
		for _, ref := range filteredRefs {
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			pkBitset[pk>>6] |= uint64(1) << (pk & 63)
		}
	}

	// Sequential scan of tsCol[lo:hi]: for each in-range span, check bitset membership
	// and accumulate its time bucket. NOTE-136: bitset replaces map lookup (~2ns vs ~10ns).
	counts := make([]int64, numSteps)
	for i, ref := range tsCol.BlockRefs[lo:hi] {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if pk > maxPK || pkBitset[pk>>6]&(uint64(1)<<(pk&63)) == 0 {
			continue // NOTE-136: fast bitset pre-filter: pk not in filteredRefs
		}
		ts := int64(tsCol.Uint64Values[lo+i]) //nolint:gosec
		bk := timeBucketIndex(ts, tb.StartTime, tb.StepSizeNanos)
		if bk >= 0 && bk < numSteps {
			counts[bk]++
		}
	}

	// Emit non-zero buckets.
	buckets := make(map[string]*aggBucketState)
	for idx, c := range counts {
		if c > 0 {
			key := strconv.FormatInt(int64(idx), 10) + "\x00" //nolint:gosec
			intrinsicGetOrCreateBucket(buckets, key).count = c
		}
	}
	if len(buckets) == 0 {
		return &TraceMetricsResult{}, true, nil
	}
	return &TraceMetricsResult{Series: traceBuildDenseSeries(buckets, querySpec)}, true, nil
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

		// NOTE-092: HISTOGRAM uses the compact path to avoid three large dense arrays
		// (dictByPK, bucketByPK, seenByPK) each sized maxPK+1 (up to 32 M entries).
		// The compact path uses binary search over sorted packKeys instead, reducing
		// peak memory from ~520 MB to ~115 MB per goroutine for M8-style queries.
		if agg.Function == vm.FuncNameHISTOGRAM {
			return streamHistogramN1Compact(ctx, r, inRangeRefs, inRangeVals, groupByCol, agg, numSteps, tb, buckets)
		}

		dictIdxForRef, dict, dictByPK, maxPK, buildErr := buildDictIdxForRefs(groupByCol, agg.GroupBy[0], inRangeRefs)
		if buildErr != nil {
			return buildErr
		}
		// NOTE-129: release dictByPK back to the pool when dispatchIntrinsicAccumulate returns.
		// Size guard (16_000_001 = maxDirectArrayEntries+1) avoids caching an oversized slice
		// from large-maxPK non-direct-path calls.
		if dictByPK != nil && cap(dictByPK) <= 16_000_001 {
			defer releaseCompactUint32(dictByPK)
		}
		var aggValsForRef []float64
		var aggPresent []bool
		// NOTE-087: HISTOGRAM now scans the aggregate column directly inside streamByRefSliceHistogram
		// using bucketByPK+dictByPK dense arrays — buildAggValsForRef (which allocated ~7.7MB valByPK
		// and performed 300M extra array ops) is skipped for HISTOGRAM.
		// All other non-count/rate functions still use buildAggValsForRef (aggValsForRef/aggPresent).
		if !isCountRate {
			var avErr error
			aggValsForRef, aggPresent, avErr = buildAggValsForRef(r, agg.Field, inRangeRefs)
			if avErr != nil {
				return avErr
			}
		}
		return streamByRefSlice(
			ctx,
			r,
			inRangeRefs,
			inRangeVals,
			dictIdxForRef,
			dict,
			agg,
			aggValsForRef,
			aggPresent,
			dictByPK,
			maxPK,
			numSteps,
			tb,
			buckets,
		)
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
// For other aggregates: spans with an absent field are skipped, matching traceFieldFloat64Col
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
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatDeltaUint64:
		// NOTE-120: DeltaUint64 exposes the same Uint64Values/BlockRefs layout after decode.
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
func scanIntrinsicColVals(
	col *modules_shared.IntrinsicColumn,
	colName string,
	keyToBucket map[uint32]int64,
	dst map[uint32]string,
) {
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
	// Flat-format, XOR-bytes-format, and DeltaUint64-format columns expose the same
	// Uint64Values/BytesValues layout after decode. XOR-decoded bytes columns (e.g. span:id,
	// trace:id with > IntrinsicPageSize rows) and DeltaUint64 columns (e.g. span:start,
	// span:duration) must be handled identically to flat columns here.
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatXORBytes,
		modules_shared.IntrinsicFormatDeltaUint64:
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
	// Flat-format, XOR-bytes-format, and DeltaUint64-format columns expose the same
	// Uint64Values/BytesValues layout after decode. XOR-decoded bytes columns (e.g. span:id,
	// trace:id with > IntrinsicPageSize rows) and DeltaUint64 columns (e.g. span:start,
	// span:duration) must be handled identically to flat columns here.
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatXORBytes,
		modules_shared.IntrinsicFormatDeltaUint64:
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
	dictByPK = acquireCompactUint32(int(maxPK) + 1) //nolint:gosec // NOTE-129

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

// directAggExceedsL3Threshold reports whether the direct path should be skipped because
// its working array would exceed L3 cache capacity, causing DRAM-bound random access.
//
// NOTE-117: non-histogram agg (max/min/sum/avg) uses dictByPK (uint32, 4 bytes/entry).
// Threshold 4M: dictByPK = 16MB ≤ typical L3 → direct OK; beyond → compact preferred.
//
// NOTE-133: count/rate uses bucketByPK (int16, 2 bytes/entry).
// Threshold 4M: bucketByPK = 8MB ≤ L3 alongside co-residents → direct OK; beyond → compact.
func directAggExceedsL3Threshold(isCountRate bool, fn string, maxPK uint32) bool {
	const maxDirectAggEntries = 4_000_000
	const maxDirectCountRateEntries = 4_000_000
	if isCountRate {
		return int64(maxPK)+1 > maxDirectCountRateEntries //nolint:gosec
	}
	return fn != vm.FuncNameHISTOGRAM && int64(maxPK)+1 > maxDirectAggEntries //nolint:gosec
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

	// NOTE-090: guard against oversized dense arrays from sparse block layouts.
	// packKey space is 32-bit (blockIdx<<16|rowIdx), so a file with many blocks and
	// few rows per block can produce maxPK far exceeding the actual span count.
	// count/rate:      bucketByPK(int16) = 2 bytes/entry; 16M entries costs ~32MB.
	// histogram/agg:   bucketByPK(int16) + dictByPK(uint32) = 6 bytes/entry; 16M entries costs ~96MB.
	// Beyond 16M entries the keyMap fallback (accumulateIntrinsicBucketsViaKeyMap)
	// is far more memory-efficient.
	// NOTE-115: bucketByPK stores bk+1 (1..numSteps ≤ 1440) or 0 (absent). numSteps ≤ 1440
	// fits in int16 (max 32767), so int16 saves 75% vs int64 with identical semantics.
	const maxDirectArrayEntries = 16_000_000
	if int64(maxPK)+1 > maxDirectArrayEntries { //nolint:gosec
		return false, nil
	}

	// NOTE-117/NOTE-131: route to compact path when dictByPK or bucketByPK exceeds L3.
	if directAggExceedsL3Threshold(isCountRate, agg.Function, maxPK) {
		return false, nil
	}

	// Build bucketByPK directly — no inRangeRefs materialized.
	// Always allocate even when maxPK==0 (packKey=0 is a valid span key).
	// NOTE-091: stepCounts[bk] tracks total in-range spans per time bucket.
	// Used by accumulateCountRateDirect to compute absent-group counts in
	// O(numSteps×numGroups) instead of O(maxPK), and eliminates seenByPK.
	bucketByPK := acquireDirectInt16(int(maxPK) + 1) // NOTE-129
	defer releaseDirectInt16(bucketByPK)
	stepCounts := make([]int64, numSteps)
	inRangeCount := 0
	minPK := maxPK // minPK of in-range refs; used as lower-bound in column scans
	for i, ref := range tsCol.BlockRefs[lo:hi] {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if pk > maxPK {
			continue
		}
		bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = int16(bk + 1) //nolint:gosec // bk+1 ≤ numSteps ≤ 1440, fits int16
			stepCounts[bk]++
			inRangeCount++
			if pk < minPK {
				minPK = pk
			}
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

	// Build dict and (for histogram/agg) dictByPK from the group-by column.
	// entryGIdx[i] = dictIdx+1 for DictEntries[i] (0 = entry has no refs within maxPK).
	// NOTE-091: dictByPK is only needed for accumulateHistogramDirect/accumulateAggDirect;
	// count/rate uses entryGIdx exclusively (set in accumulateCountRateDirect), so
	// skipping dictByPK for that path saves up to (maxPK+1)×4 bytes per file.
	dict := []string{""}
	var dictByPK []uint32
	if !isCountRate {
		dictByPK = acquireCompactUint32(int(maxPK) + 1) // NOTE-129
		defer releaseCompactUint32(dictByPK)
	}
	entryGIdx := make([]uint32, len(groupByCol.DictEntries))
	for i, entry := range groupByCol.DictEntries {
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
					entryGIdx[i] = dictIdx + 1
					assigned = true
				}
				if dictByPK != nil {
					dictByPK[pk] = dictIdx + 1
				}
			}
		}
	}

	if isCountRate {
		return true, accumulateCountRateDirect(
			ctx,
			groupByCol,
			entryGIdx,
			bucketByPK,
			minPK,
			maxPK,
			inRangeCount,
			stepCounts,
			dict,
			numSteps,
			buckets,
		)
	}
	// NOTE-089: HISTOGRAM and general agg now handled by direct column scan.
	if agg.Function == vm.FuncNameHISTOGRAM {
		return true, accumulateHistogramDirect(ctx, r, agg, dictByPK, bucketByPK, maxPK, dict, numSteps, tb, buckets)
	}
	return true, accumulateAggDirect(ctx, r, agg, dictByPK, bucketByPK, minPK, maxPK, dict, numSteps, buckets)
}

// accumulateCountRateDirect scans the group-by dict entries directly and accumulates
// count/rate into a 2D slice. No inRangeRefs, no dictIdxForRef, no seenByPK — zero span storage.
// NOTE-085: single pass, all array ops, no hash maps.
// entryGIdx[i] is dictIdx+1 for DictEntries[i] (0 = entry has no refs ≤ maxPK).
// Using entryGIdx eliminates the per-span dictByPK random lookup from the hot loop —
// all refs within one dict entry share the same gIdx, so one lookup per entry suffices.
// NOTE-091: stepCounts[bk] (pre-built from tsCol scan) replaces seenByPK for absent-row
// detection, reducing absent-group computation from O(maxPK) to O(numSteps×numGroups)
// and eliminating the seenByPK bool array allocation.
func accumulateCountRateDirect(
	ctx context.Context,
	groupByCol *modules_shared.IntrinsicColumn,
	entryGIdx []uint32,
	bucketByPK []int16,
	minPK uint32,
	maxPK uint32,
	inRangeCount int,
	stepCounts []int64,
	dict []string,
	numSteps int64,
	buckets map[string]*aggBucketState,
) error {
	numGroups := int64(len(dict)) //nolint:gosec
	// NOTE-119/124: flat 2D pooled array — eliminates per-block allocation of
	// numGroups×numSteps int64 flat array that dominates GC pressure for warm queries.
	groupCountsFlat := acquireGroupCountsFlat(numGroups * numSteps)
	defer releaseGroupCountsFlat(groupCountsFlat)
	totalSeen := int64(0)

	// Scan dict entries using pre-computed per-entry gIdx.
	// All refs in one dict entry share the same group, so entryGIdx[i] gives the
	// gIdx directly — no per-span dictByPK random array lookup needed.
	spanCount := 0
	for i, entry := range groupByCol.DictEntries {
		gIdxRaw := entryGIdx[i]
		if gIdxRaw == 0 {
			continue // entry has no refs ≤ maxPK
		}
		gIdx := int64(gIdxRaw - 1) //nolint:gosec
		base := gIdx * numSteps
		for _, ref := range entry.BlockRefs {
			if spanCount%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			spanCount++
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			if pk < minPK || pk > maxPK {
				continue
			}
			bk := int64(bucketByPK[pk])
			if bk == 0 {
				continue
			}
			totalSeen++
			groupCountsFlat[base+bk-1]++
		}
	}

	// Absent-row pass: spans in time range with no group-by value → empty-string group.
	// NOTE-091: uses stepCounts (per-step in-range span counts built from tsCol) to
	// compute absent counts in O(numSteps×numGroups) instead of O(maxPK) scan of seenByPK.
	if totalSeen < int64(inRangeCount) {
		for bk := range numSteps {
			presentAtBk := int64(0)
			for gIdx := int64(1); gIdx < numGroups; gIdx++ {
				presentAtBk += groupCountsFlat[gIdx*numSteps+bk]
			}
			absentAtBk := stepCounts[bk] - presentAtBk
			if absentAtBk > 0 {
				groupCountsFlat[bk] += absentAtBk
			}
		}
	}

	// Emit.
	for gIdx := range numGroups {
		gk := ""
		if gIdx < int64(len(dict)) { //nolint:gosec
			gk = dict[gIdx]
		}
		base := gIdx * numSteps
		hasAny := false
		for bk := range numSteps {
			if groupCountsFlat[base+bk] > 0 {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}
		for bk := range numSteps {
			c := groupCountsFlat[base+bk]
			if c == 0 {
				continue
			}
			k := strconv.FormatInt(bk, 10) + "\x00" + gk //nolint:gosec
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
	bucketByPK []int16,
	maxPK uint32,
	dict []string,
	numSteps int64,
	tb vm.TimeBucketSpec,
	buckets map[string]*aggBucketState,
) error {
	// Fetch col before allocation so we can pre-scan actual boundary count.
	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}

	numGroups := len(dict)
	var actualStride int64
	if col != nil {
		actualStride = int64(countIntrinsicHistogramBoundaries(col, agg.Field)) + 1 // +1 for absent sentinel
	} else {
		actualStride = 1
	}
	stride2 := numSteps
	stride1 := actualStride * numSteps
	groupCountsFlat := acquireGroupCountsFlat(int64(numGroups) * stride1) //nolint:gosec
	defer releaseGroupCountsFlat(groupCountsFlat)

	boundaryCache := make(map[float64]int64, 32)
	boundaries := make([]float64, 0, int(actualStride))

	getBoundaryIdx := func(v float64) int64 {
		b := intrinsicHistogramBoundary(v, agg.Field)
		idx, ok := boundaryCache[b]
		if !ok {
			if int64(len(boundaries)) >= actualStride-1 {
				idx = actualStride // discard sentinel
				boundaryCache[b] = idx
				return idx
			}
			boundaries = append(boundaries, b)
			idx = int64(len(boundaries))
			boundaryCache[b] = idx
		}
		return idx
	}

	seenByPK := acquireDirectBool(int(maxPK) + 1) // NOTE-129

	if col != nil {
		if err := streamByRefSliceHistogramScanDict(ctx, col, bucketByPK, dictByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride1, stride2, actualStride); err != nil {
			releaseDirectBool(seenByPK)
			return err
		}
	}

	// Absent-row pass: walk bucketByPK directly (no inRangeRefs) — bIdx=0 sentinel.
	for pk, bk16 := range bucketByPK {
		bk := int64(bk16)
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

	// NOTE-129: release seenByPK before emit — frees 16 MB before non-trivial emit work.
	releaseDirectBool(seenByPK)
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
	// NOTE-115: int16 stores bk+1 ≤ numSteps ≤ 1440; saves 75% vs int64 (8→2 bytes/entry).
	bucketByPK := acquireDirectInt16(int(maxPK) + 1) // NOTE-129
	for i, ref := range tsCol.BlockRefs[lo:hi] {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(tsCol.Uint64Values[lo+i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = int16(bk + 1) //nolint:gosec // bk+1 ≤ numSteps ≤ 1440, fits int16
		}
	}

	// Step 3: pre-allocate flat accumulator for 1 group (pooled — NOTE-124).
	// Layout: groupCountsFlat[bIdx*numSteps + timeIdx] (gIdx always 0).
	stride2 := numSteps
	stride1 := int64(histFlatStride) * numSteps
	groupCountsFlat := acquireGroupCountsFlat(stride1)
	defer releaseGroupCountsFlat(groupCountsFlat)

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

	seenByPK := acquireDirectBool(int(maxPK) + 1) // NOTE-129

	// Step 5: scan the histogram agg column using the N=0 specialized scanner.
	// N=0: gIdx is always 0 — no dictByPK allocation or lookup needed.
	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		releaseDirectInt16(bucketByPK)
		releaseDirectBool(seenByPK)
		return err
	}
	if col != nil {
		if err := scanHistogramN0(ctx, col, bucketByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride2); err != nil {
			releaseDirectInt16(bucketByPK)
			releaseDirectBool(seenByPK)
			return err
		}
	}

	// Step 6: absent-row pass — walk bucketByPK for pks not seen in the agg column.
	// bIdx=0 sentinel: groupCountsFlat[0*stride2 + (bk-1)] = groupCountsFlat[bk-1].
	for pk, bk16 := range bucketByPK {
		bk := int64(bk16)
		if bk == 0 || seenByPK[pk] {
			continue
		}
		groupCountsFlat[bk-1]++
	}

	// NOTE-129: release before emit — frees 32+16 MB before non-trivial emit work.
	releaseDirectInt16(bucketByPK)
	releaseDirectBool(seenByPK)
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
	bucketByPK []int16,
	minPK uint32,
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
	seenByPK := acquireDirectBool(int(maxPK) + 1) // NOTE-129
	defer releaseDirectBool(seenByPK)

	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		return err
	}
	if col != nil {
		if err := accumulateAggDirectScanCol(ctx, col, agg.Function, dictByPK, bucketByPK, minPK, maxPK, numSteps, numGroups, groupBuckets, seenByPK); err != nil {
			return err
		}
	}

	// Absent-row pass: create count=0 bucket for in-range spans with no agg value,
	// matching streamByRefSliceAgg's NaN-emit behavior.
	// Start from minPK: entries below minPK are guaranteed absent from bucketByPK.
	for pk := int(minPK); pk <= int(maxPK); pk++ { //nolint:gosec
		bk := int64(bucketByPK[pk])
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
	bucketByPK []int16,
	minPK uint32,
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
				if pk < minPK || pk > maxPK {
					continue
				}
				bk := int64(bucketByPK[pk])
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
			if pk < minPK || pk > maxPK {
				continue
			}
			bk := int64(bucketByPK[pk])
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
		return streamByRefSliceHistogram(
			ctx,
			r,
			inRangeRefs,
			inRangeVals,
			dictByPK,
			maxPK,
			dict,
			agg,
			numSteps,
			tb,
			buckets,
		)
	default:
		return streamByRefSliceAgg(
			ctx,
			inRangeRefs,
			inRangeVals,
			dictIdxForRef,
			dict,
			agg,
			aggValsForRef,
			aggPresent,
			numSteps,
			tb,
			buckets,
		)
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
	// NOTE-202: single contiguous pooled accumulator [groupCounts[dictIdx*numSteps+bucketIdx]]
	// instead of one make([]int64, numSteps) per dict entry. The per-entry slice form paid
	// len(dict) separate heap allocations per file (the row-emission fallback fires on the
	// predicate-filtered N=1 group-by path when maxPK exceeds the direct-array L3 threshold —
	// M6/M9-class queries) and scattered the per-group counters across the heap, hurting the
	// emit-time scan's cache locality. The flat array is reused from groupCountsFlatPool
	// (NOTE-124, already proven on the direct path) so warm queries pay zero allocation, and
	// the [base+bk] indexing matches accumulateCountRateDirect's contiguous layout.
	numGroups := int64(len(dict)) //nolint:gosec
	groupCounts := acquireGroupCountsFlat(numGroups * numSteps)
	defer releaseGroupCountsFlat(groupCounts)
	for i := range inRangeRefs {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		bucketIdx := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bucketIdx >= 0 && bucketIdx < numSteps {
			groupCounts[int64(dictIdxForRef[i])*numSteps+bucketIdx]++
		}
	}
	for dictIdx := int64(0); dictIdx < numGroups; dictIdx++ {
		base := dictIdx * numSteps
		hasAny := false
		for bk := int64(0); bk < numSteps; bk++ {
			if groupCounts[base+bk] > 0 {
				hasAny = true
				break
			}
		}
		if !hasAny {
			continue
		}
		gk := dict[dictIdx]
		for bk := int64(0); bk < numSteps; bk++ {
			c := groupCounts[base+bk]
			if c == 0 {
				continue
			}
			k := strconv.FormatInt(bk, 10) + "\x00" + gk //nolint:gosec
			intrinsicGetOrCreateBucket(buckets, k).count += c
		}
	}
	return nil
}

// histFlatStride is the hard-cap on distinct histogram boundaries for the flat accumulator.
// Pre-scanned actual counts are always ≤ histFlatStride; histFlatStride is the safety ceiling.
// NOTE-088: flat accumulator replaces histSpanEntry intermediate slice — eliminates
// 1.5 GB allocation at 150 M spans by accumulating directly during the column scan.
const histFlatStride = 64

// countIntrinsicHistogramBoundaries pre-scans col's dict entries to count the number of
// distinct histogram boundaries that will appear during a full column scan.
// For dict-format columns this is O(numDictEntries) — typically a few hundred, not millions.
// For flat-format columns this is O(numValues) — a fallback that returns histFlatStride early
// if cardinality is high, so the caller can allocate the full cap instead.
// The return value is in [0, histFlatStride]; callers add 1 for the absent sentinel slot.
func countIntrinsicHistogramBoundaries(col *modules_shared.IntrinsicColumn, fieldName string) int {
	seen := make(map[float64]struct{}, 32)
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			var v float64
			if entry.Value != "" {
				parsed, err := strconv.ParseFloat(entry.Value, 64)
				if err != nil {
					continue
				}
				v = parsed
			} else {
				v = float64(entry.Int64Val)
			}
			seen[intrinsicHistogramBoundary(v, fieldName)] = struct{}{}
			if len(seen) >= histFlatStride {
				return histFlatStride
			}
		}
	case modules_shared.IntrinsicFormatFlat:
		for _, u := range col.Uint64Values {
			seen[intrinsicHistogramBoundary(float64(u), fieldName)] = struct{}{}
			if len(seen) >= histFlatStride {
				return histFlatStride
			}
		}
	case modules_shared.IntrinsicFormatDeltaUint64:
		// NOTE-123: DeltaUint64 values are sorted ascending. intrinsicHistogramBoundary is
		// monotonically non-decreasing, so boundary transitions are detected by comparing
		// consecutive values — only O(numBoundaries) map insertions, not O(numValues).
		// This avoids scanning all 7.5M values with O(n) map lookups (75ms) while still
		// returning the accurate boundary count for proper groupCountsFlat sizing.
		var prevBoundary float64
		first := true
		for _, u := range col.Uint64Values {
			b := intrinsicHistogramBoundary(float64(u), fieldName)
			if first || b != prevBoundary {
				seen[b] = struct{}{}
				prevBoundary = b
				first = false
				if len(seen) >= histFlatStride {
					return histFlatStride
				}
			}
		}
	}
	return len(seen)
}

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
	// NOTE-115: int16 stores bk+1 ≤ numSteps ≤ 1440; saves 75% vs int64 (8→2 bytes/entry).
	bucketByPK := acquireDirectInt16(int(maxPK) + 1) // NOTE-129
	for i, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		bk := timeBucketIndex(int64(inRangeVals[i]), tb.StartTime, tb.StepSizeNanos) //nolint:gosec
		if bk >= 0 && bk < numSteps {
			bucketByPK[pk] = int16(bk + 1) //nolint:gosec // bk+1 ≤ numSteps ≤ 1440, fits int16
		}
	}

	// Step 2: fetch col early so we can pre-scan boundary count before allocating.
	col, err := r.GetIntrinsicColumn(agg.Field)
	if err != nil {
		releaseDirectInt16(bucketByPK)
		return err
	}

	// Step 2b: pre-scan distinct boundaries from the column dictionary (O(numDictEntries),
	// typically a few hundred — not O(spans)). Allocate exactly (actualStride × numSteps)
	// per group instead of the fixed histFlatStride=64 cap. For M8 with ~280 groups × 1440
	// steps, this reduces groupCountsFlat from 206 MB to ~67 MB (3× less), preventing OOM.
	numGroups := len(dict)
	var actualStride int64
	if col != nil {
		actualStride = int64(countIntrinsicHistogramBoundaries(col, agg.Field)) + 1 // +1 for absent sentinel at bIdx=0
	} else {
		actualStride = 1 // only the absent sentinel, no values
	}
	stride2 := numSteps                                                   // steps per boundary slot
	stride1 := actualStride * numSteps                                    // slots per group (now actualStride, not histFlatStride)
	groupCountsFlat := acquireGroupCountsFlat(int64(numGroups) * stride1) //nolint:gosec
	defer releaseGroupCountsFlat(groupCountsFlat)

	// Step 3: scan the aggregate column directly — memoize boundaries (~20 unique values).
	// boundaryCache maps boundary float64 → 1-based index into boundaries slice.
	// Discard sentinel is actualStride (= actualBoundaryCount+1), never written to the flat array.
	boundaryCache := make(map[float64]int64, 32)
	boundaries := make([]float64, 0, int(actualStride))

	getBoundaryIdx := func(v float64) int64 {
		b := intrinsicHistogramBoundary(v, agg.Field)
		idx, ok := boundaryCache[b]
		if !ok {
			// actualStride-1 is the number of actual boundary slots (bIdx=1..actualStride-1).
			// When the cap is reached, return actualStride as the discard sentinel.
			if int64(len(boundaries)) >= actualStride-1 {
				idx = actualStride // discard sentinel; scanDict will skip this
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
	seenByPK := acquireDirectBool(int(maxPK) + 1) // NOTE-129

	if col != nil {
		if err := streamByRefSliceHistogramScanDict(ctx, col, bucketByPK, dictByPK, maxPK, seenByPK, getBoundaryIdx, groupCountsFlat, stride1, stride2, actualStride); err != nil {
			releaseDirectInt16(bucketByPK)
			releaseDirectBool(seenByPK)
			return err
		}
	}

	// Absent-row pass: spans not seen in the aggregate column → bIdx=0 (boundary-0 sentinel).
	// pk <= maxPK is always true here since bucketByPK is sized maxPK+1 and inRangeRefs
	// was used to compute maxPK.
	for _, ref := range inRangeRefs {
		pk := packKey(ref.BlockIdx, ref.RowIdx)
		if !seenByPK[pk] {
			bk := int64(bucketByPK[pk])
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

	// NOTE-129: release before emit — frees 32+16 MB before non-trivial emit work.
	releaseDirectInt16(bucketByPK)
	releaseDirectBool(seenByPK)
	return streamByRefSliceHistogramFlatEmit(groupCountsFlat, stride1, stride2, numGroups, dict, boundaries, buckets)
}

// scanHistogramN0 is a specialized scanner for N=0 (no group-by) histogram accumulation.
// Eliminates the dictByPK allocation and per-span lookup of streamByRefSliceHistogramScanDict —
// gIdx is always 0, so groupCountsFlat[bIdx*stride2+(bk-1)] is the only write target.
// NOTE-089: N=0 direct path — no dictByPK, no gIdx computation.
func scanHistogramN0(
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	bucketByPK []int16,
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
				bk := int64(bucketByPK[pk])
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
			bk := int64(bucketByPK[pk])
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
// discardStride is the boundary index at which a span's boundary is discarded (out-of-bounds guard).
// NOTE-088: accumulates inline during the scan — no intermediate histSpanEntry slice.
func streamByRefSliceHistogramScanDict(
	ctx context.Context,
	col *modules_shared.IntrinsicColumn,
	bucketByPK []int16,
	dictByPK []uint32,
	maxPK uint32,
	seenByPK []bool,
	getBoundaryIdx func(float64) int64,
	groupCountsFlat []int64,
	stride1, stride2 int64,
	discardStride int64,
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
			if bIdx >= discardStride {
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
				bk := int64(bucketByPK[pk])
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
			bk := int64(bucketByPK[pk])
			if bk == 0 {
				continue // out of range
			}
			seenByPK[pk] = true
			var gIdx int64
			if raw := dictByPK[pk]; raw > 0 {
				gIdx = int64(raw - 1) //nolint:gosec
			}
			bIdx := getBoundaryIdx(float64(col.Uint64Values[i]))
			if bIdx >= discardStride {
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
	numBoundaries := int64(len(boundaries)) + 1 // +1 for absent sentinel at bIdx=0
	// NOTE-142: reuse a pooled []byte scratch to build the composite key with strconv.AppendInt
	// instead of FormatInt + a 3-way string concat per non-zero cell. The key bytes are identical
	// (timeIdx\x00gk\x00boundaryStr), so traceHistogramSeries parsing and all bucket counts are
	// unchanged; only the genuine map-miss path allocates the retained key string. Mirrors the
	// callback emitter pattern (NOTE-067/073) in metrics_trace.go.
	scratch := acquireCompositeKeyScratch()
	defer releaseCompositeKeyScratch(scratch)
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
				// Build "timeIdx\x00gk\x00boundaryStr" into the reused buffer — byte-identical to
				// strconv.FormatInt(timeIdx,10) + "\x00" + gk + "\x00" + boundaryStr.
				*scratch = strconv.AppendInt((*scratch)[:0], timeIdx, 10)
				*scratch = append(*scratch, '\x00')
				*scratch = append(*scratch, gk...)
				*scratch = append(*scratch, '\x00')
				*scratch = append(*scratch, boundaryStr...)
				// Zero-alloc lookup on hit; Go elides string(*scratch) when the key does not escape.
				b, exists := buckets[string(*scratch)]
				if !exists {
					k := string(*scratch) // NOTE-073: intentional alloc — key retained in map
					b = &aggBucketState{
						min: math.MaxFloat64,
						max: -math.MaxFloat64,
					}
					buckets[k] = b
				}
				b.count += count
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

// aggGroupIDKey is the map key for dict-ID keyed non-histogram aggregate group-by.
// NOTE-074: Accumulates SUM/AVG/MIN/MAX/QUANTILE/STDDEV keyed by (groupIDKey, bucketIdx);
// resolves to string at emit time (O(unique groups), not O(spans)).

// histSingleGroupIDKey is the map key for the N=1 dict-ID histogram group-by fast path.
// Replaces histGroupIDKey for single-dimension queries, reducing key size from 48 to 24 bytes.
// NOTE-082: N=1 fast path — smaller key means faster hash and less memcmp.
// SPEC-ETM-13.4: float64 boundary key is safe (same invariant as histGroupIDKey).

// aggSingleGroupIDKey is the map key for the N=1 dict-ID aggregate group-by fast path.
// NOTE-082: N=1 fast path — 16 bytes vs 40 bytes for aggGroupIDKey.

// histSingleAbsentKey is the map key for the N=1 nil-column histogram group-by fast path.
// NOTE-082: N=1 fast path — 16 bytes vs 48 bytes for histGroupIDKey.

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
	// NOTE-085: slice accumulator indexed by dictIdx — eliminates 150M map probes per M5 file.
	// NOTE-202: single contiguous pooled flat array [dictIdx*numSteps+bucketIdx] instead of
	// one make([]int64, numSteps) per dict entry — collapses len(dict) heap allocations into
	// one pooled reuse (groupCountsFlatPool, NOTE-124) and keeps the per-group counters
	// contiguous for the emit-time scan, matching accumulateCountRateDirect's layout.
	numGroups := int64(len(dict)) //nolint:gosec
	groupCounts := acquireGroupCountsFlat(numGroups * numSteps)
	defer releaseGroupCountsFlat(groupCounts)
	i := 0
	for pk, bucketIdx := range keyToBucket {
		if i%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		dictIdx := int64(groupIDMap[pk]) //nolint:gosec
		if bucketIdx >= 0 && bucketIdx < numSteps {
			groupCounts[dictIdx*numSteps+bucketIdx]++
		}
	}
	// Resolve IDs back to strings at emit time. O(unique groups).
	for dictIdx := int64(0); dictIdx < numGroups; dictIdx++ {
		gk := dict[dictIdx]
		base := dictIdx * numSteps
		for bk := int64(0); bk < numSteps; bk++ {
			if c := groupCounts[base+bk]; c > 0 {
				k := strconv.FormatInt(bk, 10) + "\x00" + gk //nolint:gosec
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

// pow2Floor returns the largest power of 2 that is <= v, for v > 0.
// NOTE-181: this is the value of 2**floor(log2(v)) computed branch-free without
// any transcendental call. math.Frexp decomposes v = frac * 2**exp with
// frac in [0.5, 1); since log2(frac) in [-1, 0), floor(log2(v)) == exp-1 exactly
// (including the exact-power-of-2 case, where frac == 0.5 and exp is one larger),
// so the boundary is math.Ldexp(1, exp-1). Replaces the
// math.Pow(2, math.Floor(math.Log2(v))) form, which a CPU profile attributed to
// ~10% of total querier CPU (math.pow 5.99% + math.archLog 4.15% + math.log2 1.29%,
// minus the ~1.6% reintroduced by frexp/ldexp). Verified bit-exact to the old form
// over 20M random values spanning the full ns-duration range, exact powers of 2,
// and denormals.
func pow2Floor(v float64) float64 {
	_, exp := math.Frexp(v)
	return math.Ldexp(1, exp-1)
}

// boundaryIndexer maps a histogram-cell value to a dense first-encounter boundary
// index, replacing the per-row map[float64]int64 lookup in the histogram scan hot loop.
//
// NOTE-182: a CPU profile (process_cpu, 30m) attributed ~14% of total querier CPU to the
// histogram aggregation scan (scanAggColHistogramShard 5.6% + buildFrozenBoundaryIdx 3.8% +
// histRefPassPos 2.3% + scanAggColHistogramCompact 1.2%). Inside the per-ref hot loop the
// getBoundaryIdx closure hashed a float64 boundary through a map[float64]int64 on EVERY
// passing row. The boundary is always either 0 (for v<=0) or an exact power of two
// 2**(exp-1) where exp comes from math.Frexp(v) (NOTE-181). The binary exponent is therefore
// a perfect dense integer key: this indexer keeps a slice indexed by (exp-expBase) instead
// of hashing the float, collapsing the per-row hash+probe to a single bounds-checked array
// read. First-encounter ordering of boundaries[] (load-bearing per NOTE-143: the emit step
// reads boundaries in append order) and the actualStride-1 overflow/discard sentinel are
// preserved byte-for-byte.
type boundaryIndexer struct {
	boundaries  []float64 // first-encounter order; read by the emit step (NOTE-143)
	byExp       []int64   // dense index by (exp-expBase); 0 means "unassigned"
	zeroIdx     int64     // assigned index for the boundary-0 cell, 0 until first seen
	maxStride   int64     // actualStride: at most maxStride-1 distinct boundaries before discard
	discardIdx  int64     // sentinel index returned once maxStride-1 boundaries are recorded
	expBase     int       // exponent of byExp[0]
	zeroSeen    bool
	scaleByNano bool // span:duration values are divided by 1e9 before pow2Floor (NOTE-181)
}

// newBoundaryIndexer builds an indexer for one histogram field. fieldName selects the
// duration scaling; actualStride bounds the number of recordable boundaries.
func newBoundaryIndexer(fieldName string, actualStride int64) *boundaryIndexer {
	// float64 binary exponents span roughly [-1074, 1024]; size the dense table to cover
	// that whole range so any value maps without a second allocation. The table is int64
	// per slot (~17 KiB) — negligible and built once per scan, not per row.
	const expLo, expHi = -1075, 1025
	return &boundaryIndexer{
		boundaries:  make([]float64, 0, int(actualStride)),
		byExp:       make([]int64, expHi-expLo),
		expBase:     expLo,
		maxStride:   actualStride,
		discardIdx:  actualStride,
		scaleByNano: fieldName == colNameSpanDuration,
	}
}

// index returns the dense first-encounter boundary index for v, mirroring the exact
// semantics of the former getBoundaryIdx closure (boundary 0 for v<=0; otherwise the
// power-of-two boundary keyed by exponent; discard sentinel once maxStride-1 boundaries
// are recorded). Single math.Frexp per call — no transcendental, no float64 hashing.
func (bi *boundaryIndexer) index(v float64) int64 {
	// Mirror intrinsicHistogramBoundary exactly: the original value is tested <= 0 FIRST
	// (negatives and zero map to the boundary-0 cell regardless of field), then duration
	// values are scaled by 1e9; the math.Abs in the plain branch is dead since v > 0 here.
	if v <= 0 {
		if !bi.zeroSeen {
			bi.zeroIdx = bi.record(0)
			bi.zeroSeen = true
		}
		return bi.zeroIdx
	}
	if bi.scaleByNano {
		v /= 1e9
		if v <= 0 {
			if !bi.zeroSeen {
				bi.zeroIdx = bi.record(0)
				bi.zeroSeen = true
			}
			return bi.zeroIdx
		}
	}
	_, exp := math.Frexp(v)
	slot := exp - bi.expBase
	// byExp slots hold the assigned index (>0) or the discard sentinel (== maxStride) once
	// seen; only an unassigned slot (== 0) triggers a record() call, matching the former
	// closure where boundaryCache memoized both real indices and the overflow sentinel.
	if idx := bi.byExp[slot]; idx != 0 {
		return idx
	}
	idx := bi.record(math.Ldexp(1, exp-1))
	bi.byExp[slot] = idx
	return idx
}

// record assigns the next first-encounter index for boundary b, appending to boundaries[]
// unless the maxStride-1 cap is reached (then it returns the discard sentinel without
// appending), matching the former closure's overflow branch exactly.
func (bi *boundaryIndexer) record(b float64) int64 {
	if int64(len(bi.boundaries)) >= bi.maxStride-1 {
		return bi.discardIdx
	}
	bi.boundaries = append(bi.boundaries, b)
	return int64(len(bi.boundaries))
}

// lookup is the read-only counterpart of index used by parallel workers after a serial
// pre-warm has populated byExp/zeroIdx (NOTE-143). It performs the same exponent decode
// but never mutates indexer state, so it is safe to call concurrently from every worker.
// The pre-warm visits exactly the rows the workers reach, so a slot is always assigned;
// an unassigned slot returns the discard sentinel (the former roBoundary's unreachable
// fallback), never corrupting shared state.
func (bi *boundaryIndexer) lookup(v float64) int64 {
	if v <= 0 {
		if bi.zeroSeen {
			return bi.zeroIdx
		}
		return bi.discardIdx
	}
	if bi.scaleByNano {
		v /= 1e9
		if v <= 0 {
			if bi.zeroSeen {
				return bi.zeroIdx
			}
			return bi.discardIdx
		}
	}
	_, exp := math.Frexp(v)
	slot := exp - bi.expBase
	if idx := bi.byExp[slot]; idx != 0 {
		return idx
	}
	return bi.discardIdx
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
		return pow2Floor(vSec)
	}
	return pow2Floor(math.Abs(v))
}
