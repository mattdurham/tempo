package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// MetricsRow is one row in the dense time-series aggregation grid.
// GroupKey[0] is the 0-indexed time-bucket number; GroupKey[1..] are group-by attribute values.
type MetricsRow struct {
	Values   map[string]float64
	GroupKey []string
}

// MetricsResult is the output of ExecuteMetrics.
type MetricsResult struct {
	Rows          []MetricsRow
	BytesRead     int64
	BlocksScanned int
}

// ExecuteMetrics runs a compiled TraceQL metrics program and returns a dense time-series grid.
// program must be compiled with vm.CompileTraceQLMetrics.
// querySpec provides time-bucketing config, GROUP BY fields, and aggregate function.
func (e *Executor) ExecuteMetrics(
	r *modules_reader.Reader,
	program *vm.Program,
	querySpec *vm.QuerySpec,
) (*MetricsResult, error) {
	if r == nil {
		return &MetricsResult{}, nil
	}
	if querySpec == nil {
		return nil, fmt.Errorf("ExecuteMetrics: querySpec cannot be nil")
	}

	planner := queryplanner.NewPlanner(r)
	plan := planner.Plan(buildPredicates(r, program))

	result := &MetricsResult{}
	if len(plan.SelectedBlocks) == 0 {
		return result, nil
	}

	rawBlocks, err := planner.FetchBlocks(plan)
	if err != nil {
		return nil, fmt.Errorf("FetchBlocks: %w", err)
	}

	// buckets maps composite key (bucketIdxStr + "\x00" + attrGroupKey) to AggBucket.
	buckets := make(map[string]*vm.AggBucket)

	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		result.BytesRead += int64(len(raw)) //nolint:gosec

		meta := r.BlockMeta(blockIdx)
		bwb, parseErr := r.ParseBlockFromBytes(raw, nil, meta)
		if parseErr != nil {
			return nil, fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return nil, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		result.BlocksScanned++

		for _, rowIdx := range rowSet.ToSlice() {
			metricsAccumulateRow(bwb.Block, rowIdx, querySpec, buckets)
		}
	}

	result.Rows = metricsBuildDenseRows(buckets, querySpec)
	return result, nil
}

// metricsAccumulateRow accumulates one span's contribution into the buckets map.
func metricsAccumulateRow(
	block *modules_reader.Block,
	rowIdx int,
	querySpec *vm.QuerySpec,
	buckets map[string]*vm.AggBucket,
) {
	tb := querySpec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return
	}

	// Apply QuerySpec filter when the filter is not match-all.
	// CompileTraceQLMetrics returns a match-all vm.Program, so we must check the filter spec here.
	if !querySpec.Filter.IsMatchAll {
		if !metricsFilterMatches(block, rowIdx, &querySpec.Filter) {
			return
		}
	}

	// Read span:start for time bucketing.
	startCol := block.GetColumn("span:start")
	if startCol == nil {
		return
	}
	spanStart, ok := startCol.Uint64Value(rowIdx)
	if !ok {
		return
	}

	// Skip spans outside the query time window.
	startNanos := int64(spanStart) //nolint:gosec
	if startNanos < tb.StartTime || startNanos >= tb.EndTime {
		return
	}

	bucketIdx := (startNanos - tb.StartTime) / tb.StepSizeNanos
	bucketIdxStr := strconv.FormatInt(bucketIdx, 10)

	// Build the group key from GROUP BY fields.
	groupBy := querySpec.Aggregate.GroupBy
	attrVals := make([]string, len(groupBy))
	for i, field := range groupBy {
		col := block.GetColumn(field)
		if col == nil {
			// Try unscoped lookup (e.g. "duration" → "span:duration").
			col = lookupColumnUnscoped(block, field)
		}
		if col != nil {
			attrVals[i] = metricsColumnString(col, rowIdx)
		}
	}
	attrGroupKey := strings.Join(attrVals, "\x00")

	compositeKey := bucketIdxStr + "\x00" + attrGroupKey

	bucket, exists := buckets[compositeKey]
	if !exists {
		bucket = &vm.AggBucket{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		}
		buckets[compositeKey] = bucket
	}

	metricsUpdateBucket(block, rowIdx, querySpec.Aggregate, bucket)
}

// metricsFilterMatches checks whether the row at rowIdx satisfies all predicates in the filter spec.
// Returns true if all attribute equality and range predicates pass.
func metricsFilterMatches(block *modules_reader.Block, rowIdx int, filter *vm.FilterSpec) bool {
	// Check attribute equality predicates.
	for field, values := range filter.AttributeEquals {
		col := block.GetColumn(field)
		if col == nil {
			col = lookupColumnUnscoped(block, field)
		}
		if col == nil {
			return false
		}
		matched := false
		for _, v := range values {
			if rowMatches(col, rowIdx, v) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check attribute range predicates.
	for field, rangeSpec := range filter.AttributeRanges {
		col := block.GetColumn(field)
		if col == nil {
			col = lookupColumnUnscoped(block, field)
		}
		if col == nil {
			return false
		}
		if rangeSpec.MinValue != nil {
			cmp, ok := rowCompare(col, rowIdx, rangeSpec.MinValue)
			if !ok {
				return false
			}
			if rangeSpec.MinInclusive {
				if cmp < 0 {
					return false
				}
			} else {
				if cmp <= 0 {
					return false
				}
			}
		}
		if rangeSpec.MaxValue != nil {
			cmp, ok := rowCompare(col, rowIdx, rangeSpec.MaxValue)
			if !ok {
				return false
			}
			if rangeSpec.MaxInclusive {
				if cmp > 0 {
					return false
				}
			} else {
				if cmp >= 0 {
					return false
				}
			}
		}
	}

	return true
}

// lookupColumnUnscoped tries resource. and span. prefixes for unscoped column names.
func lookupColumnUnscoped(block *modules_reader.Block, name string) *modules_reader.Column {
	if col := block.GetColumn("resource." + name); col != nil {
		return col
	}
	return block.GetColumn("span." + name)
}

// metricsColumnString converts any column type to its string representation.
func metricsColumnString(col *modules_reader.Column, rowIdx int) string {
	if col == nil || !col.IsPresent(rowIdx) {
		return ""
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		v, ok := col.StringValue(rowIdx)
		if ok {
			return v
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		v, ok := col.Int64Value(rowIdx)
		if ok {
			return strconv.FormatInt(v, 10)
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		v, ok := col.Uint64Value(rowIdx)
		if ok {
			return strconv.FormatUint(v, 10)
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		v, ok := col.Float64Value(rowIdx)
		if ok {
			return strconv.FormatFloat(v, 'f', -1, 64)
		}
	case modules_shared.ColumnTypeBool:
		v, ok := col.BoolValue(rowIdx)
		if ok {
			return strconv.FormatBool(v)
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		v, ok := col.BytesValue(rowIdx)
		if ok {
			return string(v)
		}
	}
	return ""
}

// metricsReadFloat64 reads a numeric column as float64, handling Uint64, Int64, Float64,
// and RangeDuration column types (most common for aggregation fields like span:duration).
func metricsReadFloat64(block *modules_reader.Block, colName string, rowIdx int) (float64, bool) {
	col := block.GetColumn(colName)
	if col == nil {
		return 0, false
	}
	if !col.IsPresent(rowIdx) {
		return 0, false
	}
	switch col.Type {
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		v, ok := col.Uint64Value(rowIdx)
		if ok {
			return float64(v), true
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64:
		v, ok := col.Int64Value(rowIdx)
		if ok {
			return float64(v), true
		}
	case modules_shared.ColumnTypeRangeDuration:
		v, ok := col.Int64Value(rowIdx)
		if ok {
			return float64(v), true
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		v, ok := col.Float64Value(rowIdx)
		if ok {
			return v, true
		}
	}
	return 0, false
}

// metricsUpdateBucket updates bucket for COUNT/AVG/SUM/MIN/MAX.
func metricsUpdateBucket(
	block *modules_reader.Block,
	rowIdx int,
	agg vm.AggregateSpec,
	bucket *vm.AggBucket,
) {
	bucket.Count++

	if agg.Function == vm.FuncNameCOUNT {
		return
	}

	fieldVal, ok := metricsReadFloat64(block, agg.Field, rowIdx)
	if !ok {
		return
	}

	switch agg.Function {
	case vm.FuncNameSUM, vm.FuncNameAVG:
		bucket.Sum += fieldVal
	case vm.FuncNameMIN:
		if fieldVal < bucket.Min {
			bucket.Min = fieldVal
		}
	case vm.FuncNameMAX:
		if fieldVal > bucket.Max {
			bucket.Max = fieldVal
		}
	}
}

// metricsBuildDenseRows builds the dense time-series grid from the accumulated buckets.
// Every (bucket × attrGroupKey) combination gets a row; missing pairs get zero-filled rows.
func metricsBuildDenseRows(buckets map[string]*vm.AggBucket, querySpec *vm.QuerySpec) []MetricsRow {
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

	// Collect unique attrGroupKeys.
	attrGroupKeySet := make(map[string]struct{})
	for compositeKey := range buckets {
		// Strip the leading "bucketIdxStr\x00" prefix.
		sep := strings.IndexByte(compositeKey, '\x00')
		if sep < 0 {
			continue
		}
		attrGroupKey := compositeKey[sep+1:]
		attrGroupKeySet[attrGroupKey] = struct{}{}
	}

	if len(attrGroupKeySet) == 0 {
		return nil
	}

	// Sort attrGroupKeys for deterministic output.
	attrGroupKeys := make([]string, 0, len(attrGroupKeySet))
	for k := range attrGroupKeySet {
		attrGroupKeys = append(attrGroupKeys, k)
	}
	sort.Strings(attrGroupKeys)

	rows := make([]MetricsRow, 0, int(numBuckets)*len(attrGroupKeys)) //nolint:gosec
	for _, attrGroupKey := range attrGroupKeys {
		attrVals := metricsAttrGroupValues(attrGroupKey)
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey

			bucket := buckets[compositeKey] // nil if no spans matched this bucket

			groupKey := make([]string, 1+len(attrVals))
			groupKey[0] = strconv.FormatInt(bucketIdx, 10)
			copy(groupKey[1:], attrVals)

			rows = append(rows, MetricsRow{
				GroupKey: groupKey,
				Values:   metricsRowValues(bucket, querySpec.Aggregate, tb.StepSizeNanos),
			})
		}
	}

	return rows
}

// metricsAttrGroupValues splits a "\x00"-separated composite attribute group key into its parts.
func metricsAttrGroupValues(attrGroupKey string) []string {
	if attrGroupKey == "" {
		return nil
	}
	return strings.Split(attrGroupKey, "\x00")
}

// metricsOutputName returns the output map key name for the aggregation function.
func metricsOutputName(agg vm.AggregateSpec) string {
	switch agg.Function {
	case vm.FuncNameCOUNT:
		return "count"
	case vm.FuncNameAVG:
		return "avg"
	case vm.FuncNameSUM:
		return "sum"
	case vm.FuncNameMIN:
		return "min"
	case vm.FuncNameMAX:
		return "max"
	default:
		return strings.ToLower(agg.Function)
	}
}

// metricsRowValues finalizes one row's values map from a bucket.
// A nil bucket means no spans matched — return zero values.
func metricsRowValues(bucket *vm.AggBucket, agg vm.AggregateSpec, _ int64) map[string]float64 {
	key := metricsOutputName(agg)
	if bucket == nil || bucket.Count == 0 {
		return map[string]float64{key: 0}
	}
	switch agg.Function {
	case vm.FuncNameCOUNT:
		return map[string]float64{key: float64(bucket.Count)}
	case vm.FuncNameAVG:
		if bucket.Count > 0 {
			return map[string]float64{key: bucket.Sum / float64(bucket.Count)}
		}
		return map[string]float64{key: 0}
	case vm.FuncNameSUM:
		return map[string]float64{key: bucket.Sum}
	case vm.FuncNameMIN:
		if bucket.Min == math.MaxFloat64 {
			return map[string]float64{key: 0}
		}
		return map[string]float64{key: bucket.Min}
	case vm.FuncNameMAX:
		if bucket.Max == -math.MaxFloat64 {
			return map[string]float64{key: 0}
		}
		return map[string]float64{key: bucket.Max}
	default:
		return map[string]float64{key: float64(bucket.Count)}
	}
}
