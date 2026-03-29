package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// TraceMetricLabel is a single label key-value pair for a TraceTimeSeries.
// SPEC-ETM-1: Labels are an ordered slice; the same label name will not appear twice.
type TraceMetricLabel struct {
	Name  string
	Value string
}

// TraceTimeSeries is a single time series in a TraceMetricsResult.
// SPEC-ETM-2: Values[i] is NaN when no data exists for bucket i (except COUNT/RATE which use 0).
// SPEC-ETM-3: len(Values) == numBuckets where numBuckets = ceil((EndTime-StartTime)/StepSizeNanos).
type TraceTimeSeries struct {
	Labels []TraceMetricLabel
	Values []float64
}

// TraceMetricsResult is the output of ExecuteTraceMetrics.
// SPEC-ETM-4: Nil reader returns empty TraceMetricsResult with no error.
// SPEC-ETM-5: Nil querySpec returns an error.
type TraceMetricsResult struct {
	Series        []TraceTimeSeries
	BytesRead     int64
	BlocksScanned int
}

// ExecuteTraceMetrics runs a TraceQL metrics query against a blockpack trace file.
// SPEC-ETM-4 through SPEC-ETM-9: for each matched span, accumulates into time-series
// buckets keyed by (bucketIdx, groupByLabels).
//
// querySpec.TimeBucketing must be enabled with a positive StepSizeNanos.
// Supported functions: COUNT, RATE, SUM, AVG, MIN, MAX, HISTOGRAM, QUANTILE, STDDEV.
func ExecuteTraceMetrics(
	r *modules_reader.Reader,
	program *vm.Program,
	querySpec *vm.QuerySpec,
) (*TraceMetricsResult, error) {
	if r == nil {
		return &TraceMetricsResult{}, nil
	}
	if querySpec == nil {
		return nil, fmt.Errorf("ExecuteTraceMetrics: querySpec cannot be nil")
	}
	if program == nil {
		return nil, fmt.Errorf("ExecuteTraceMetrics: program cannot be nil")
	}

	var tr queryplanner.TimeRange
	if querySpec.TimeBucketing.Enabled {
		tr = queryplanner.TimeRange{
			MinNano: uint64(querySpec.TimeBucketing.StartTime), //nolint:gosec
			MaxNano: uint64(querySpec.TimeBucketing.EndTime),   //nolint:gosec
		}
	}
	plan := planBlocks(r, program, tr, queryplanner.PlanOptions{})

	result := &TraceMetricsResult{}
	if len(plan.SelectedBlocks) == 0 {
		return result, nil
	}

	// SPEC-ETM-9: wantColumns must include span:start, the aggregate field, and all GroupBy
	// columns in addition to predicate columns. Missing these = silently wrong results.
	wantColumns := ProgramWantColumns(program)
	extraCols := []string{"span:start"}
	if querySpec.Aggregate.Field != "" {
		extraCols = append(extraCols, querySpec.Aggregate.Field)
	}
	extraCols = append(extraCols, querySpec.Aggregate.GroupBy...)
	if wantColumns == nil {
		wantColumns = make(map[string]struct{}, len(extraCols))
	}
	for _, c := range extraCols {
		wantColumns[c] = struct{}{}
	}

	// NOTE-045: intrinsic fast path — zero block reads when all needed columns are in the
	// intrinsic section (span:start, span:duration, resource.service.name, span:status, etc.).
	if intrinsicResult, used, intrinsicErr := executeTraceMetricsIntrinsic(r, program, querySpec, wantColumns); intrinsicErr != nil {
		return nil, intrinsicErr
	} else if used {
		return intrinsicResult, nil
	}

	rawBlocks, err := r.ReadBlocks(plan.SelectedBlocks)
	if err != nil {
		return nil, fmt.Errorf("FetchBlocks: %w", err)
	}

	// buckets maps a composite key to aggBucketState.
	// For most aggregates: bucketIdxStr + "\x00" + attrGroupKey.
	// For HISTOGRAM:       bucketIdxStr + "\x00" + attrGroupKey + "\x00" + histBoundary.
	// attrGroupKey itself uses "\x00" to separate multiple GroupBy values.
	// See traceAccumulateRow for the authoritative key format.
	buckets := make(map[string]*aggBucketState)

	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		result.BytesRead += int64(len(raw)) //nolint:gosec

		meta := r.BlockMeta(blockIdx)
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

		result.BlocksScanned++

		for _, rowIdx := range rowSet.ToSlice() {
			traceAccumulateRow(r, blockIdx, bwb.Block, rowIdx, querySpec, buckets)
		}
	}

	// NOTE-033: HISTOGRAM uses a dedicated series builder that reconstructs __bucket labels
	// from the 3rd composite-key segment. All other functions use the standard dense-series path.
	if querySpec.Aggregate.Function == vm.FuncNameHISTOGRAM {
		result.Series = traceHistogramSeries(buckets, querySpec)
	} else {
		result.Series = traceBuildDenseSeries(buckets, querySpec)
	}
	return result, nil
}

// traceAccumulateRow accumulates one span's contribution into the buckets map.
func traceAccumulateRow(
	r *modules_reader.Reader,
	blockIdx int,
	block *modules_reader.Block,
	rowIdx int,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
) {
	tb := querySpec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return
	}

	// Read span:start for time bucketing (SPEC-ETM-7).
	// PATTERN: block-column-first with intrinsic-section fallback (shared across
	// compaction/compaction.go, writer/writer.go, executor/executor.go, metrics_trace.go).
	// v3 files store identity columns in block payloads; v4 files store them exclusively
	// in the intrinsic section. Try the block column first for backwards compat.
	// span:start is stored in the intrinsic section; fall back to block column for legacy files.
	var tsNanos int64
	if tsCol := block.GetColumn("span:start"); tsCol != nil {
		tsVal, ok := tsCol.Uint64Value(rowIdx)
		if !ok {
			return
		}
		tsNanos = int64(tsVal) //nolint:gosec
	} else if r != nil {
		tsVal, ok := r.IntrinsicUint64At("span:start", blockIdx, rowIdx)
		if !ok {
			return
		}
		tsNanos = int64(tsVal) //nolint:gosec
	} else {
		return
	}

	// Skip spans outside the query time window (SPEC-ETM-6).
	// Intervals are right-closed: (StartTime, EndTime] — matches Tempo semantics.
	if tsNanos <= tb.StartTime || tsNanos > tb.EndTime {
		return
	}

	bucketIdx := timeBucketIndex(tsNanos, tb.StartTime, tb.StepSizeNanos)

	// Build group key from GroupBy attributes (SPEC-ETM-8).
	groupBy := querySpec.Aggregate.GroupBy
	attrVals := make([]string, len(groupBy))
	for i, attr := range groupBy {
		col := block.GetColumn(attr)
		if col != nil {
			attrVals[i] = metricsColumnString(col, rowIdx)
		}
		// Missing column → empty string label (Tempo convention).
	}
	attrGroupKey := strings.Join(attrVals, "\x00")
	// NOTE-033: HISTOGRAM embeds log2 bucket boundary as a 3rd "\x00" segment.
	// Non-HISTOGRAM functions omit the 3rd segment (existing composite key format unchanged).
	var compositeKey string
	if querySpec.Aggregate.Function == vm.FuncNameHISTOGRAM {
		histBoundary := traceHistogramBucket(block, rowIdx, querySpec.Aggregate.Field)
		compositeKey = strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey + "\x00" +
			strconv.FormatFloat(histBoundary, 'g', -1, 64)
	} else {
		compositeKey = strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey
	}

	bucket, exists := buckets[compositeKey]
	if !exists {
		bucket = &aggBucketState{
			min: math.MaxFloat64,
			max: -math.MaxFloat64,
		}
		buckets[compositeKey] = bucket
	}

	traceUpdateBucket(block, rowIdx, querySpec.Aggregate, bucket)
}

// traceUpdateBucket accumulates one span's value into a bucket based on the aggregate function.
func traceUpdateBucket(
	block *modules_reader.Block,
	rowIdx int,
	agg vm.AggregateSpec,
	bucket *aggBucketState,
) {
	switch agg.Function {
	case vm.FuncNameCOUNT, vm.FuncNameRATE:
		bucket.count++
	case vm.FuncNameSUM, vm.FuncNameAVG:
		v, ok := traceFieldFloat64(block, rowIdx, agg.Field)
		if ok {
			bucket.count++
			bucket.sum += v
		}
	case vm.FuncNameMIN:
		v, ok := traceFieldFloat64(block, rowIdx, agg.Field)
		if ok {
			// count tracks rows with a valid numeric field (not update count);
			// traceRowValue uses count==0 as the NaN sentinel for "no data in bucket".
			bucket.count++
			if v < bucket.min {
				bucket.min = v
			}
		}
	case vm.FuncNameMAX:
		v, ok := traceFieldFloat64(block, rowIdx, agg.Field)
		if ok {
			// count tracks rows with a valid numeric field (not update count);
			// traceRowValue uses count==0 as the NaN sentinel for "no data in bucket".
			bucket.count++
			if v > bucket.max {
				bucket.max = v
			}
		}
	case vm.FuncNameQUANTILE:
		v, ok := traceFieldFloat64(block, rowIdx, agg.Field)
		if ok {
			bucket.count++
			bucket.values = append(bucket.values, v)
		}
	case vm.FuncNameSTDDEV:
		v, ok := traceFieldFloat64(block, rowIdx, agg.Field)
		if ok {
			// Welford's online algorithm for sample variance. NOTE-033.
			bucket.count++
			delta := v - bucket.mean
			bucket.mean += delta / float64(bucket.count)
			bucket.m2 += delta * (v - bucket.mean)
		}
	default:
		// HISTOGRAM uses count-only accumulation; traceHistogramSeries reads bucket.count.
		bucket.count++
	}
}

// traceHistogramBucket computes the log2 lower-boundary for a histogram cell.
// For span:duration (nanoseconds), divides by 1e9 to convert to seconds first.
// For other numeric fields, uses the absolute value directly.
// Returns 0 if v <= 0 or the field is absent/non-numeric.
// NOTE-033: log2 buckets span latency distributions across orders of magnitude.
func traceHistogramBucket(block *modules_reader.Block, rowIdx int, fieldName string) float64 {
	v, ok := traceFieldFloat64(block, rowIdx, fieldName)
	if !ok || v <= 0 {
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

// traceFieldFloat64 reads a numeric field value from a span column.
// Returns (0, false) when the column is absent, not present for this row, or non-numeric.
// SPEC-ETM-8: Non-numeric columns (string, bool, bytes) are silently skipped.
func traceFieldFloat64(block *modules_reader.Block, rowIdx int, fieldName string) (float64, bool) {
	col := block.GetColumn(fieldName)
	if col == nil {
		return 0, false
	}
	if !col.IsPresent(rowIdx) {
		return 0, false
	}
	switch col.Type {
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		v, ok := col.Uint64Value(rowIdx)
		return float64(v), ok
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		v, ok := col.Int64Value(rowIdx)
		return float64(v), ok
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		return col.Float64Value(rowIdx)
	default:
		return 0, false
	}
}

// traceBuildDenseSeries builds a dense time-series slice from accumulated buckets.
// SPEC-ETM-2: COUNT/RATE emit 0 for empty buckets; MIN/MAX/SUM/AVG emit NaN.
// SPEC-ETM-11: Series are sorted by label string for deterministic output.
func traceBuildDenseSeries(
	buckets map[string]*aggBucketState,
	querySpec *vm.QuerySpec,
) []TraceTimeSeries {
	attrGroupKeys, numBuckets := collectGroupKeys(buckets, querySpec.TimeBucketing)
	if attrGroupKeys == nil {
		return nil
	}

	stepSec := float64(querySpec.TimeBucketing.StepSizeNanos) / 1e9
	groupBy := querySpec.Aggregate.GroupBy

	series := make([]TraceTimeSeries, 0, len(attrGroupKeys))
	for _, attrGroupKey := range attrGroupKeys {
		// Build label slice from GroupBy + attrVals.
		// Note: check only len(groupBy) > 0, not attrGroupKey != "". If all GroupBy attributes
		// are absent from a span, attrGroupKey is "" but labels must still be emitted with
		// empty-string values so the series carries the correct label names.
		var labels []TraceMetricLabel
		if len(groupBy) > 0 {
			attrVals := strings.Split(attrGroupKey, "\x00")
			labels = make([]TraceMetricLabel, 0, len(groupBy))
			for i, name := range groupBy {
				val := ""
				if i < len(attrVals) {
					val = attrVals[i]
				}
				labels = append(labels, TraceMetricLabel{Name: name, Value: val})
			}
		}

		values := make([]float64, numBuckets)
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey
			bucket := buckets[compositeKey]
			values[bucketIdx] = traceRowValue(bucket, querySpec.Aggregate.Function, stepSec,
				querySpec.Aggregate.Quantile)
		}

		series = append(series, TraceTimeSeries{Labels: labels, Values: values})
	}

	// Sort for deterministic output (SPEC-ETM-11).
	slices.SortFunc(series, func(a, b TraceTimeSeries) int {
		return cmp.Compare(traceLabelString(a.Labels), traceLabelString(b.Labels))
	})

	return series
}

// traceRowValue computes the final metric value for a single bucket.
// SPEC-ETM-2: COUNT/RATE return 0 for nil/empty buckets; others return NaN.
// NOTE-033: QUANTILE reuses logComputeQuantile; STDDEV uses Welford sample variance.
func traceRowValue(bucket *aggBucketState, funcName string, stepSec, quantile float64) float64 {
	switch funcName {
	case vm.FuncNameCOUNT:
		if bucket == nil {
			return 0.0
		}
		return float64(bucket.count)
	case vm.FuncNameRATE:
		if bucket == nil {
			return 0.0
		}
		if stepSec > 0 {
			return float64(bucket.count) / stepSec
		}
		return 0.0
	default:
		// SUM, AVG, MIN, MAX: NaN for empty buckets.
		if bucket == nil || bucket.count == 0 {
			return math.NaN()
		}
		switch funcName {
		case vm.FuncNameSUM:
			return bucket.sum
		case vm.FuncNameAVG:
			return bucket.sum / float64(bucket.count)
		case vm.FuncNameMIN:
			if bucket.min == math.MaxFloat64 {
				return math.NaN()
			}
			return bucket.min
		case vm.FuncNameMAX:
			if bucket.max == -math.MaxFloat64 {
				return math.NaN()
			}
			return bucket.max
		case vm.FuncNameQUANTILE:
			// Reuse logComputeQuantile (nearest-rank method, same as log metrics). NOTE-033.
			return logComputeQuantile(bucket.values, quantile)
		case vm.FuncNameSTDDEV:
			// Sample stddev undefined for n < 2. NOTE-033.
			if bucket.count < 2 {
				return math.NaN()
			}
			return math.Sqrt(bucket.m2 / float64(bucket.count-1))
		default:
			return math.NaN()
		}
	}
}

// timeBucketIndex returns the right-closed bucket index for ts within (startTime, endTime].
// Precondition: ts > startTime (caller must range-check before calling).
// Intervals: (startTime, startTime+step], (startTime+step, startTime+2*step], …
// A span at exactly startTime+N*step belongs to bucket N-1 (previous bucket).
func timeBucketIndex(ts, startTime, stepNanos int64) int64 {
	offset := ts - startTime
	bkt := offset / stepNanos
	if offset%stepNanos == 0 {
		bkt--
	}
	return bkt
}

// traceLabelString builds a deterministic string key from a label slice for sorting.
func traceLabelString(labels []TraceMetricLabel) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, len(labels))
	for i, l := range labels {
		parts[i] = l.Name + "=" + l.Value
	}
	return strings.Join(parts, ",")
}

// traceHistogramSeries builds histogram time-series from accumulated buckets.
// Composite key format: "bucketIdx\x00attrGroupKey\x00bucketBoundary"
// One series per (attrGroupKey, bucketBoundary) pair. Values use COUNT semantics (0 not NaN).
// SPEC-ETM-11: Series sorted by label string for deterministic output.
// NOTE-033: __bucket label carries the log2 lower-boundary in the field's native unit.
func traceHistogramSeries(
	buckets map[string]*aggBucketState,
	querySpec *vm.QuerySpec,
) []TraceTimeSeries {
	if len(buckets) == 0 {
		return nil
	}

	tb := querySpec.TimeBucketing
	numBuckets := int64(0)
	if tb.Enabled && tb.StepSizeNanos > 0 {
		numBuckets = (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	}
	if numBuckets <= 0 {
		return nil
	}

	type histSeriesKey struct {
		attrGroupKey   string
		bucketBoundary string
	}
	keySet := make(map[histSeriesKey]struct{})
	for compositeKey := range buckets {
		// Key format: "bucketIdx\x00attrGroupKey\x00histBoundary".
		// attrGroupKey may itself contain "\x00" separating multiple GroupBy values,
		// so split on the FIRST "\x00" (strips bucketIdx) and the LAST "\x00" (separates
		// histBoundary from attrGroupKey) rather than using a naive second Cut.
		firstSep := strings.IndexByte(compositeKey, '\x00')
		if firstSep < 0 {
			continue
		}
		rest := compositeKey[firstSep+1:]
		lastSep := strings.LastIndexByte(rest, '\x00')
		if lastSep < 0 {
			continue
		}
		keySet[histSeriesKey{attrGroupKey: rest[:lastSep], bucketBoundary: rest[lastSep+1:]}] = struct{}{}
	}
	if len(keySet) == 0 {
		return nil
	}

	sortedKeys := make([]histSeriesKey, 0, len(keySet))
	for k := range keySet {
		sortedKeys = append(sortedKeys, k)
	}
	slices.SortFunc(sortedKeys, func(a, b histSeriesKey) int {
		if a.attrGroupKey != b.attrGroupKey {
			return cmp.Compare(a.attrGroupKey, b.attrGroupKey)
		}
		// Parse boundaries as float64 to sort numerically, not lexicographically.
		// %g formatting means "4" < "32" lexicographically but 4.0 < 32.0 numerically.
		fi, _ := strconv.ParseFloat(a.bucketBoundary, 64)
		fj, _ := strconv.ParseFloat(b.bucketBoundary, 64)
		return cmp.Compare(fi, fj)
	})

	groupBy := querySpec.Aggregate.GroupBy
	series := make([]TraceTimeSeries, 0, len(sortedKeys))
	for _, sk := range sortedKeys {
		var labels []TraceMetricLabel
		if len(groupBy) > 0 {
			attrVals := strings.Split(sk.attrGroupKey, "\x00")
			labels = make([]TraceMetricLabel, 0, len(groupBy)+1)
			for i, name := range groupBy {
				val := ""
				if i < len(attrVals) {
					val = attrVals[i]
				}
				labels = append(labels, TraceMetricLabel{Name: name, Value: val})
			}
		}
		labels = append(labels, TraceMetricLabel{Name: "__bucket", Value: sk.bucketBoundary})

		values := make([]float64, numBuckets)
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" +
				sk.attrGroupKey + "\x00" + sk.bucketBoundary
			if bucket := buckets[compositeKey]; bucket != nil {
				values[bucketIdx] = float64(bucket.count)
			}
		}
		series = append(series, TraceTimeSeries{Labels: labels, Values: values})
	}

	slices.SortFunc(series, func(a, b TraceTimeSeries) int {
		return cmp.Compare(traceLabelString(a.Labels), traceLabelString(b.Labels))
	})
	return series
}
