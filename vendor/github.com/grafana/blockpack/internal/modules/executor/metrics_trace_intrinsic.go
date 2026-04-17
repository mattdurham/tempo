package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// Intrinsic fast path for ExecuteTraceMetrics.
// NOTE-046: When all needed columns (span:start, aggregate field, group-by) are in the
// intrinsic section, metrics queries skip full block reads entirely. See NOTES.md NOTE-046.
// See also NOTE-055 for the streamHistogramGroupBy dict-amortization extension.

import (
	"context"
	"math"
	"sort"
	"strconv"
	"sync"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// ctxCheckInterval is how often (in spans) to check for context cancellation in hot loops.
// Large enough to bound overhead; small enough to bound cancellation latency.
const ctxCheckInterval = 100_000

// NOTE-074: Pool cap constants for intersectBitmapPool.
// intersectBitmapDefaultWords: 4096 words = 32 KB initial capacity.
// intersectBitmapMaxWords: 16 MB cap — covers files up to ~134M spans (20 GB × ~200 B/span,
// per SPEC-ROOT-016). Files exceeding this cap skip the pool and allocate directly.
// Pool memory at typical load: 9 concurrent queries × 10 files/query × 12.5 MB ≈ 1.1 GB
// worst-case pool residency, acceptable at the 20 GB file bound.
const (
	intersectBitmapDefaultWords = 4096
	intersectBitmapMaxWords     = 2097152 // 16 MB cap — SPEC-ROOT-016
)

//nolint:gochecknoglobals
var intersectBitmapPool = &sync.Pool{
	New: func() any {
		b := make([]uint64, 0, intersectBitmapDefaultWords)
		return &b
	},
}

func acquireIntersectBitmap(wordLen int) *[]uint64 {
	//nolint:forcetypeassert
	p := intersectBitmapPool.Get().(*[]uint64)
	if cap(*p) < wordLen {
		*p = make([]uint64, wordLen)
	} else {
		*p = (*p)[:wordLen]
		clear(*p)
	}
	return p
}

func releaseIntersectBitmap(p *[]uint64) {
	if cap(*p) > intersectBitmapMaxWords {
		*p = make([]uint64, 0, intersectBitmapDefaultWords)
	} else {
		*p = (*p)[:0]
	}
	intersectBitmapPool.Put(p)
}

// blockOffsets returns a prefix-sum slice where blockOffsets[i] is the cumulative span count
// starting at block i (i.e. the dense row index of the first span in block i). The second
// return value is the total span count across all blocks (= bitmap size in bits).
// O(numBlocks) walk over in-memory blockMetas — zero I/O.
// SPEC-ROOT-016: sized for 20 GB files (~100M spans).
func blockOffsets(r *modules_reader.Reader) ([]int, int) {
	n := r.BlockCount()
	if n == 0 {
		return nil, 0
	}
	offsets := make([]int, n)
	total := 0
	for i := range n {
		offsets[i] = total
		total += int(r.BlockMeta(i).SpanCount)
	}
	return offsets, total
}

// denseRange returns the dense-index range [start, end) for blockIdx within blkOffsets.
// Returns (0, 0) if blockIdx is out of range — callers treat (end == start) as skip.
func denseRange(blkOffsets []int, totalSpans, blockIdx int) (start, end int) {
	if blockIdx < 0 || blockIdx >= len(blkOffsets) {
		return 0, 0
	}
	start = blkOffsets[blockIdx]
	if blockIdx+1 < len(blkOffsets) {
		end = blkOffsets[blockIdx+1]
	} else {
		end = totalSpans
	}
	return start, end
}

// bitmapIntersectRefsWithVals returns the subset of (inRangeRefs, inRangeVals) whose
// (blockIdx, rowIdx) appears in filteredRefs. O(M+N) vs O(M log M + N log N) for merge-join.
//
// NOTE-074: Replaces mergeJoinFilteredRefsWithVals. No sort allocations.
// blockOffsets is a prefix-sum slice where blockOffsets[blockIdx] is the cumulative span count
// up to (but not including) block blockIdx, forming a dense address space [0, totalSpans).
// totalSpans is the sum of all block SpanCounts. Both are derived from r via blockOffsets(r).
//
// Precondition: rowIdx < blockMeta[blockIdx].SpanCount for every ref (blockpack writer invariant).
// Both BlockIdx and RowIdx are validated against their respective block bounds via denseRange.
// Refs with out-of-range BlockIdx or RowIdx are silently skipped — no panic.
// This prevents a ref with rowIdx >= blockRows from landing in an adjacent block's bitmap range.
func bitmapIntersectRefsWithVals(
	filteredRefs []modules_shared.BlockRef,
	inRangeRefs []modules_shared.BlockRef,
	inRangeVals []uint64,
	blkOffsets []int,
	totalSpans int,
) (outRefs []modules_shared.BlockRef, outVals []uint64) {
	if len(filteredRefs) == 0 || len(inRangeRefs) == 0 {
		return nil, nil
	}
	if len(blkOffsets) == 0 || totalSpans <= 0 {
		return nil, nil
	}

	// Dense bitmap: wordLen = ceil(totalSpans / 64).
	// Typical file (1.4M spans): 21,875 words = 175 KB.
	// 20 GB file (100M spans, SPEC-ROOT-016): 1,562,500 words = 12.5 MB.
	wordLen := (totalSpans + 63) / 64

	var bm *[]uint64
	if wordLen <= intersectBitmapMaxWords {
		bm = acquireIntersectBitmap(wordLen)
		defer releaseIntersectBitmap(bm)
	} else {
		b := make([]uint64, wordLen)
		bm = &b
	}
	words := *bm

	for _, r := range filteredRefs {
		start, end := denseRange(blkOffsets, totalSpans, int(r.BlockIdx))
		rowIdx := int(r.RowIdx)
		if rowIdx < 0 || rowIdx >= end-start {
			continue
		}
		denseIdx := start + rowIdx
		words[denseIdx>>6] |= uint64(1) << (denseIdx & 63)
	}

	outRefs = make([]modules_shared.BlockRef, 0, min(len(filteredRefs), len(inRangeRefs)))
	outVals = make([]uint64, 0, cap(outRefs))
	for i, r := range inRangeRefs {
		start, end := denseRange(blkOffsets, totalSpans, int(r.BlockIdx))
		rowIdx := int(r.RowIdx)
		if rowIdx < 0 || rowIdx >= end-start {
			continue
		}
		denseIdx := start + rowIdx
		if words[denseIdx>>6]&(uint64(1)<<(denseIdx&63)) != 0 {
			outRefs = append(outRefs, r)
			outVals = append(outVals, inRangeVals[i])
		}
	}
	return outRefs, outVals
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
// pay a one-time bitmap-intersect cost (pooled []uint64, O(M+N)) per file — see NOTE-074):
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

	// NOTE-074: filteredRefs is applied via bitmap intersection after binary search narrows inRangeRefs.
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

	// NOTE-074: bitmap intersection replaces merge-join sort; O(M+N), pooled bitmap.
	// SPEC-ROOT-016: blockOffsets sized for 20 GB files.
	if filteredRefs != nil {
		offsets, total := blockOffsets(r)
		inRangeRefs, inRangeVals = bitmapIntersectRefsWithVals(filteredRefs, inRangeRefs, inRangeVals, offsets, total)
		if len(inRangeRefs) == 0 {
			return &TraceMetricsResult{}, true, nil
		}
	}

	buckets := make(map[string]*aggBucketState)

	// Fast inline path for count/rate with no group-by: see streamCountRateNoGroupBy.
	// NOTE-068: uses a flat []int64 slice instead of map[string]*aggBucketState in the
	// hot loop to eliminate per-span string allocations and hash lookups.
	if isCountRate && len(agg.GroupBy) == 0 {
		if err := streamCountRateNoGroupBy(ctx, inRangeRefs, inRangeVals, tb, buckets); err != nil {
			return nil, false, err
		}
		if len(buckets) == 0 {
			return &TraceMetricsResult{}, true, nil
		}
	} else {
		// All other shapes (group-by, aggregate field) need keyToBucket so that
		// dict/flat aggregate columns can look up the bucket index by pack key.
		keyToBucket := make(map[uint32]int64, len(inRangeRefs))
		for i, ref := range inRangeRefs {
			if i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return nil, false, err
				}
			}
			pk := packKey(ref.BlockIdx, ref.RowIdx)
			ts := int64(inRangeVals[i]) //nolint:gosec
			keyToBucket[pk] = timeBucketIndex(ts, tb.StartTime, tb.StepSizeNanos)
		}
		if len(keyToBucket) == 0 {
			return &TraceMetricsResult{}, true, nil
		}
		if err := accumulateIntrinsicBuckets(ctx, r, keyToBucket, querySpec, buckets); err != nil {
			return nil, false, err
		}
	}

	result := &TraceMetricsResult{}
	if querySpec.Aggregate.Function == vm.FuncNameHISTOGRAM {
		result.Series = traceHistogramSeries(buckets, querySpec)
	} else {
		result.Series = traceBuildDenseSeries(buckets, querySpec)
	}
	return result, true, nil
}

// packKey packs (blockIdx, rowIdx) into a uint32 for use as a map key.
func packKey(blockIdx, rowIdx uint16) uint32 {
	return uint32(blockIdx)<<16 | uint32(rowIdx)
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
//	count/rate, N group-by:  build groupKeyMap (1 map), call streamCountRateGroupBy
//	agg field,  no group-by: stream aggregate column against keyToBucket (no aggVals map)
//	agg field,  N group-by (histogram): streamHistogramGroupBy (2 maps: keyToBucket + groupKeyMap; seen map is inside streamHistogramGroupBy)
//	agg field,  N group-by (other):     build groupKeyMap (1 map) + aggVals (1 map), iterate keyToBucket
func accumulateIntrinsicBuckets(
	ctx context.Context,
	r *modules_reader.Reader,
	keyToBucket map[uint32]int64,
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

	// Queries with group-by: build one composite groupKeyMap by iterating dict/flat
	// column entries directly. This replaces N separate groupVals[i] maps with one.
	groupKeyMap, err := buildGroupKeyMap(r, agg.GroupBy, keyToBucket)
	if err != nil {
		return err
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
	aggVals, err := buildAggValsMap(r, agg.Field, keyToBucket)
	if err != nil {
		return err
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

// scanIntrinsicColVals scans a single intrinsic column's dict/flat entries and writes
// packKey → string value into dst for every ref that appears in keyToBucket.
// Rows absent from the column are not written (callers handle absence as empty string).
func scanIntrinsicColVals(col *modules_shared.IntrinsicColumn, keyToBucket map[uint32]int64, dst map[uint32]string) {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			val := entry.Value
			if val == "" {
				val = strconv.FormatInt(entry.Int64Val, 10)
			}
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if _, ok := keyToBucket[pk]; ok {
					dst[pk] = val
				}
			}
		}
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
			scanIntrinsicColVals(col, keyToBucket, out)
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
			scanIntrinsicColVals(col, keyToBucket, colVals)
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
