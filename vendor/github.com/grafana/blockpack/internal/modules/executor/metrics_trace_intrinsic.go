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
//	count/rate, N group-by (N≤8): buildGroupIDMap + streamCountRateGroupByID (NOTE-074 fast path)
//	count/rate, N group-by (N>8): buildGroupKeyMap + streamCountRateGroupBy (string-keyed fallback)
//	agg field,  no group-by:      stream aggregate column against keyToBucket (no aggVals map)
//	agg field,  N group-by (histogram, N≤8): buildGroupIDMap + streamHistogramGroupByID (NOTE-074)
//	agg field,  N group-by (histogram, N>8): buildGroupKeyMap + streamHistogramGroupBy (fallback)
//	agg field,  N group-by (other, N≤8):    buildGroupIDMap + streamAggGroupByID (NOTE-074)
//	agg field,  N group-by (other, N>8):    buildGroupKeyMap + aggVals iteration (fallback)
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

	// Try dict-ID fast path first (N ≤ 8 group-by dims, all intrinsic).
	// NOTE-074: Falls back to string-keyed path for N > 8 or on any error.
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
	keyToBucket map[uint32]int64,
	dst map[uint32]uint32,
	dict []string,
) []string {
	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		for _, entry := range col.DictEntries {
			val := entry.Value
			if val == "" {
				val = strconv.FormatInt(entry.Int64Val, 10)
			}
			dictIdx := uint32(len(dict)) //nolint:gosec
			dict = append(dict, val)
			for _, ref := range entry.BlockRefs {
				pk := packKey(ref.BlockIdx, ref.RowIdx)
				if _, ok := keyToBucket[pk]; ok {
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
	if len(groupBy) > maxGroupByDimsFastPath {
		return nil, nil, false, nil
	}

	// Fast path: single group-by column — write directly to out via a flat pk→dictIdx map.
	// NOTE-056-style optimization: skip the outer colMap intermediate for N=1.
	if len(groupBy) == 1 {
		dict := []string{""}
		colMap := make(map[uint32]uint32, len(keyToBucket))
		col, err := r.GetIntrinsicColumn(groupBy[0])
		if err != nil {
			return nil, nil, false, err
		}
		if col != nil {
			dict = scanIntrinsicColDictIDs(col, keyToBucket, colMap, dict)
		}
		out := make(map[uint32]groupIDKey, len(keyToBucket))
		for pk := range keyToBucket {
			var key groupIDKey
			if id, ok := colMap[pk]; ok {
				key[0] = id
			}
			out[pk] = key
		}
		return out, [][]string{dict}, true, nil
	}

	// Multi-group-by path.
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
			dict = scanIntrinsicColDictIDs(col, keyToBucket, colMap, dict)
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

// streamCountRateGroupByID is the dict-ID variant of streamCountRateGroupBy.
// Uses map[groupIDKey][]int64 instead of map[string][]int64 for groupCounts,
// eliminating string hash operations in the hot loop (~150M for M8).
//
// NOTE-074: Dict-ID fast path — see NOTES.md. dicts[i] maps dictIdx → string
// for resolution at series-emit time.
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

// streamHistogramGroupByID is the dict-ID variant of streamHistogramGroupBy.
// Uses map[histGroupIDKey]int64 keyed by (groupIDKey, boundary, bucketIdx) to eliminate
// both string hash operations for group dims AND FormatFloat per span for the boundary.
//
// NOTE-074: Eliminates string hashing for group dims and FormatFloat+string
// hashing for the histogram boundary. The boundary float64 is used directly as
// part of the map key (safe: values are always powers-of-2 or 0, never NaN — SPEC-ETM-13.4).
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
		i := 0
		for pk, bucketIdx := range keyToBucket {
			if i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			i++
			gid := groupIDMap[pk]
			gk := resolveGroupIDKey(gid, dicts)
			key := strconv.FormatInt(bucketIdx, 10) + "\x00" + gk + "\x000"
			intrinsicGetOrCreateBucket(buckets, key).count++
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

// streamAggGroupByID handles non-histogram aggregate functions (SUM, AVG, MIN, MAX,
// QUANTILE, STDDEV) with group-by using the dict-ID fast path.
// NOTE-074: Two-stage accumulation: hot loop accumulates into map[aggGroupIDKey]*aggBucketState
// (no string allocation per span); resolveGroupIDKey is called only at emit time (O(unique groups)).
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
