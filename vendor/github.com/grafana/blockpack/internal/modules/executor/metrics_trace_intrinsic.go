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

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// metricsColumnsAreIntrinsic reports whether all columns in wantColumns are available
// in this file's intrinsic section, enabling the zero-block-read fast path.
// It checks the file's actual TOC metadata directly, so both standard intrinsic columns
// and dedicated columns written by the writer are eligible. This also handles older files
// that may be missing optional intrinsic columns (they fall back to block scan).
func metricsColumnsAreIntrinsic(r *modules_reader.Reader, wantColumns map[string]struct{}) bool {
	if !r.HasIntrinsicSection() {
		return false
	}
	for col := range wantColumns {
		if _, present := r.IntrinsicColumnMeta(col); !present {
			return false
		}
	}
	return true
}

// executeTraceMetricsIntrinsic is the zero-block-read fast path for ExecuteTraceMetrics.
// It accumulates metrics directly from the intrinsic column section without reading any
// full blocks.
//
// Map allocation budget (all paths also allocate one buckets map and filteredKeys when
// predicates are present; the counts below exclude those shared allocations):
//
//	count/rate, no group-by:                0 extra maps (span:start streamed inline)
//	count/rate, N group-by:                 2 maps (keyToBucket + groupKeyMap)
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

	var filteredKeys map[uint32]struct{} // non-nil when predicates reduce the row set
	if hasPreds {
		refs := BlockRefsFromIntrinsicTOC(r, program, 0)
		if refs == nil {
			return nil, false, nil
		}
		filteredKeys = make(map[uint32]struct{}, len(refs))
		for _, ref := range refs {
			filteredKeys[packKey(ref.BlockIdx, ref.RowIdx)] = struct{}{}
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

	buckets := make(map[string]*aggBucketState)

	// Fast inline path for count/rate with no group-by: stream span:start directly
	// without building keyToBucket. This is the most common metrics query shape
	// ({ } | count_over_time(), { } | rate()) and avoids any intermediate hash map.
	const ctxCheckInterval = 100_000 // check context every 100K spans to bound cancellation latency
	if isCountRate && len(agg.GroupBy) == 0 {
		for i, ref := range inRangeRefs {
			if i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return nil, false, err
				}
			}
			if filteredKeys != nil {
				if _, ok := filteredKeys[packKey(ref.BlockIdx, ref.RowIdx)]; !ok {
					continue
				}
			}
			ts := int64(inRangeVals[i]) //nolint:gosec
			key := strconv.FormatInt(timeBucketIndex(ts, tb.StartTime, tb.StepSizeNanos), 10) + "\x00"
			intrinsicGetOrCreateBucket(buckets, key).count++
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
			if filteredKeys != nil {
				if _, ok := filteredKeys[pk]; !ok {
					continue
				}
			}
			ts := int64(inRangeVals[i]) //nolint:gosec
			keyToBucket[pk] = timeBucketIndex(ts, tb.StartTime, tb.StepSizeNanos)
		}
		if len(keyToBucket) == 0 {
			return &TraceMetricsResult{}, true, nil
		}
		if err := accumulateIntrinsicBuckets(r, keyToBucket, querySpec, buckets); err != nil {
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

// accumulateIntrinsicBuckets handles queries that need keyToBucket: those with group-by
// columns or an aggregate field. Count/rate with no group-by is handled inline in the
// caller and never reaches this function.
//
// Strategy matrix:
//
//	count/rate, N group-by:  build groupKeyMap (1 map), iterate keyToBucket
//	agg field,  no group-by: stream aggregate column against keyToBucket (no aggVals map)
//	agg field,  N group-by (histogram): streamHistogramGroupBy (2 maps: keyToBucket + groupKeyMap; seen map is inside streamHistogramGroupBy)
//	agg field,  N group-by (other):     build groupKeyMap (1 map) + aggVals (1 map), iterate keyToBucket
func accumulateIntrinsicBuckets(
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
		// count/rate with group-by: two maps total (keyToBucket + groupKeyMap).
		for pk, bucketIdx := range keyToBucket {
			key := strconv.FormatInt(bucketIdx, 10) + "\x00" + groupKeyMap[pk]
			intrinsicGetOrCreateBucket(buckets, key).count++
		}
		return nil
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
