package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// LogMetricsResult is the output of ExecuteLogMetrics.
// SPEC-ELM-1: Nil reader returns empty LogMetricsResult with no error.
// SPEC-ELM-2: Nil querySpec returns an error.
type LogMetricsResult struct {
	Rows          []LogMetricsRow // dense time-series rows
	BytesRead     int64           // total raw bytes read
	BlocksScanned int             // blocks parsed and evaluated
}

// LogMetricsRow is one row in the aggregation grid.
// GroupKey[0] is the 0-indexed time-bucket number; GroupKey[1..] are group-by label values.
type LogMetricsRow struct {
	Values   map[string]float64
	GroupKey []string
}

// logMetricsFunc constants map LogQL function names to their internal representation.
const (
	logFuncCountOverTime    = "count_over_time"
	logFuncRate             = "rate"
	logFuncBytesOverTime    = "bytes_over_time"
	logFuncBytesRate        = "bytes_rate"
	logFuncSumOverTime      = "sum_over_time"
	logFuncAvgOverTime      = "avg_over_time"
	logFuncMinOverTime      = "min_over_time"
	logFuncMaxOverTime      = "max_over_time"
	logFuncQuantileOverTime = "quantile_over_time"
)

// aggBucketState holds accumulation state for a single time bucket.
// SPEC-ELM-6: quantile_over_time requires collecting all values for post-scan percentile computation.
// NOTE-033: mean and m2 support Welford online stddev for trace metrics (zero-init is safe for log metrics).
type aggBucketState struct {
	values []float64 // collected values for quantile_over_time; nil for other functions
	sum    float64
	count  int64
	min    float64
	max    float64
	mean   float64 // Welford running mean (trace STDDEV only; zero for log metrics)
	m2     float64 // Welford sum of squared deviations (trace STDDEV only; zero for log metrics)
}

// ExecuteLogMetrics runs a LogQL metric query against a blockpack log file.
// SPEC-ELM-1 through SPEC-ELM-6: For each matched row, applies pipeline stages, then
// accumulates into time-series buckets keyed by (bucketIdx, groupByLabels).
//
// querySpec.TimeBucketing must be enabled and have a positive StepSizeNanos.
// pipeline may be nil (no post-filter transformation).
// funcName is the LogQL aggregation function (count_over_time, rate, etc.).
// groupBy is the list of label names to group the time series by.
func ExecuteLogMetrics(
	r *modules_reader.Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	querySpec *vm.QuerySpec,
	funcName string,
	groupBy []string,
) (*LogMetricsResult, error) {
	if r == nil {
		return &LogMetricsResult{}, nil
	}
	if querySpec == nil {
		return nil, fmt.Errorf("ExecuteLogMetrics: querySpec cannot be nil")
	}
	if program == nil {
		return nil, fmt.Errorf("ExecuteLogMetrics: program cannot be nil")
	}

	var tr queryplanner.TimeRange
	if querySpec.TimeBucketing.Enabled {
		tr = queryplanner.TimeRange{
			MinNano: uint64(querySpec.TimeBucketing.StartTime), //nolint:gosec
			MaxNano: uint64(querySpec.TimeBucketing.EndTime),   //nolint:gosec
		}
	}
	plan := planBlocks(r, program, tr, queryplanner.PlanOptions{})

	result := &LogMetricsResult{}
	if len(plan.SelectedBlocks) == 0 {
		return result, nil
	}

	wantColumns := ProgramWantColumns(program)

	rawBlocks, err := r.ReadBlocks(plan.SelectedBlocks)
	if err != nil {
		return nil, fmt.Errorf("FetchBlocks: %w", err)
	}

	// buckets maps composite key (bucketIdxStr + "\x00" + attrGroupKey) to aggBucketState.
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

		// NOTE-001: Lazy registration in ParseBlockFromBytes registers all columns with
		// presence-only decode. Full decode is triggered on first value access — no second pass needed.

		result.BlocksScanned++

		colNames, colMap, colCols, _, _ := buildBlockColMapsWithLogCache(bwb.Block)
		attrVals := make([]string, len(groupBy)) // NOTE-054: per-block scratch; cleared at top of logAccumulateRow
		for _, rowIdx := range rowSet.ToSlice() {
			logAccumulateRow(
				bwb.Block,
				rowIdx,
				colNames,
				colMap,
				colCols,
				pipeline,
				querySpec,
				funcName,
				groupBy,
				buckets,
				attrVals,
			)
		}
	}

	stepSec := float64(querySpec.TimeBucketing.StepSizeNanos) / 1e9
	result.Rows = logBuildDenseRows(buckets, querySpec, funcName, groupBy, stepSec, querySpec.Aggregate.Quantile)
	return result, nil
}

// logAccumulateRow accumulates one log row's contribution into the buckets map.
// attrVals is a scratch slice of len(groupBy) reused across calls.
func logAccumulateRow(
	block *modules_reader.Block,
	rowIdx int,
	colNames []string,
	colMap map[string]int,
	colCols []*modules_reader.Column,
	pipeline *logqlparser.Pipeline,
	querySpec *vm.QuerySpec,
	funcName string,
	groupBy []string,
	buckets map[string]*aggBucketState,
	attrVals []string,
) {
	// NOTE-054: clear attrVals at function entry so stale values from prior rows never
	// survive an early return. strings.Join copies the joined string before reuse is safe.
	clear(attrVals)

	tb := querySpec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return
	}

	// Read timestamp from log:timestamp column.
	tsCol := block.GetColumn("log:timestamp")
	if tsCol == nil {
		return
	}
	tsVal, ok := tsCol.Uint64Value(rowIdx)
	if !ok {
		return
	}
	tsNanos := int64(tsVal) //nolint:gosec

	// Skip rows outside the query time window.
	// Log metrics uses left-closed intervals [StartTime, EndTime) — intentionally different
	// from trace metrics which uses right-closed (StartTime, EndTime] (Tempo semantics).
	// Log metrics has no equivalent Tempo/Loki interval alignment requirement.
	if tsNanos < tb.StartTime || tsNanos >= tb.EndTime {
		return
	}

	// Read log body.
	var line string
	if bodyCol := block.GetColumn("log:body"); bodyCol != nil {
		if v, ok := bodyCol.StringValue(rowIdx); ok {
			line = v
		}
	}

	// NOTE-SL-016: acquire from pool — zero map allocation for dropped rows.
	bls := acquireBlockLabelSet(block, rowIdx, colNames, colMap, colCols)
	var labels logqlparser.LabelSet = bls

	// Apply pipeline stages.
	if pipeline != nil {
		var keep bool
		line, labels, keep = pipeline.Process(uint64(tsNanos), line, bls) //nolint:gosec
		if !keep {
			releaseBlockLabelSet(bls)
			return
		}
	}

	bucketIdx := (tsNanos - tb.StartTime) / tb.StepSizeNanos
	bucketIdxStr := strconv.FormatInt(bucketIdx, 10)

	// Build the group key from group-by label values.
	for i, lbl := range groupBy {
		attrVals[i] = labels.Get(lbl)
	}
	attrGroupKey := strings.Join(attrVals, "\x00")
	compositeKey := bucketIdxStr + "\x00" + attrGroupKey

	bucket, exists := buckets[compositeKey]
	if !exists {
		bucket = &aggBucketState{
			min: math.MaxFloat64,
			max: -math.MaxFloat64,
		}
		buckets[compositeKey] = bucket
	}

	logUpdateBucket(labels, line, funcName, bucket)
	releaseBlockLabelSet(bls)
}

// logUpdateBucket accumulates one row into a bucket based on the aggregation function.
// For unwrap-dependent functions (sum/avg/min/max/quantile_over_time), count is only
// incremented when the unwrap value is present and parseable — rows missing the key
// are skipped so they do not inflate denominators (SPEC-ELM-5, SPEC-ELM-6).
func logUpdateBucket(labels logqlparser.LabelSet, line, funcName string, bucket *aggBucketState) {
	switch funcName {
	case logFuncCountOverTime, logFuncRate:
		bucket.count++
	case logFuncBytesOverTime, logFuncBytesRate:
		bucket.count++
		bucket.sum += float64(len(line))
	case logFuncSumOverTime, logFuncAvgOverTime:
		if v, err := strconv.ParseFloat(labels.Get(logqlparser.UnwrapValueKey), 64); err == nil {
			bucket.count++
			bucket.sum += v
		}
	case logFuncMinOverTime:
		if v, err := strconv.ParseFloat(labels.Get(logqlparser.UnwrapValueKey), 64); err == nil {
			bucket.count++
			if v < bucket.min {
				bucket.min = v
			}
		}
	case logFuncMaxOverTime:
		if v, err := strconv.ParseFloat(labels.Get(logqlparser.UnwrapValueKey), 64); err == nil {
			bucket.count++
			if v > bucket.max {
				bucket.max = v
			}
		}
	case logFuncQuantileOverTime:
		if v, err := strconv.ParseFloat(labels.Get(logqlparser.UnwrapValueKey), 64); err == nil {
			bucket.count++
			bucket.values = append(bucket.values, v)
		}
	}
}

// collectGroupKeys returns the sorted unique attr-group-key strings from buckets
// and the number of time buckets. Returns (nil, 0) if the result set is empty.
// compositeKey format: "bucketIdxStr\x00groupVal1[\x00groupVal2...]" — cuts at first \x00 to get attrGroupKey.
func collectGroupKeys(buckets map[string]*aggBucketState, tb vm.TimeBucketSpec) ([]string, int64) {
	if len(buckets) == 0 {
		return nil, 0
	}
	numBuckets := int64(0)
	if tb.Enabled && tb.StepSizeNanos > 0 {
		numBuckets = (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	}
	if numBuckets <= 0 {
		return nil, 0
	}
	keySet := make(map[string]struct{})
	for compositeKey := range buckets {
		_, after, ok := strings.Cut(compositeKey, "\x00")
		if !ok {
			continue
		}
		keySet[after] = struct{}{}
	}
	if len(keySet) == 0 {
		return nil, 0
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys, numBuckets
}

// calcInitialCap returns a safe pre-allocation cap for logBuildDenseRows.
// It avoids int overflow when numBuckets is large by checking the guard condition
// before multiplying. If nAttrKeys is zero or the product would exceed maxInitialCap,
// maxInitialCap is returned.
func calcInitialCap(numBuckets int64, nAttrKeys int) int {
	const maxInitialCap = 1_000_000
	if nAttrKeys == 0 || numBuckets > int64(maxInitialCap/nAttrKeys) {
		return maxInitialCap
	}
	return int(numBuckets) * nAttrKeys
}

// logBuildDenseRows builds the dense time-series grid from accumulated buckets.
func logBuildDenseRows(
	buckets map[string]*aggBucketState,
	querySpec *vm.QuerySpec,
	funcName string,
	_ []string,
	stepSec float64,
	quantile float64,
) []LogMetricsRow {
	attrGroupKeys, numBuckets := collectGroupKeys(buckets, querySpec.TimeBucketing)
	if attrGroupKeys == nil {
		return nil
	}
	// BUG-5 fix: guard against overflow when numBuckets is large (e.g. a very wide
	// time window with fine step granularity). Cap the pre-allocation at 1M rows;
	// the slice will grow naturally via append if needed.
	initialCap := calcInitialCap(numBuckets, len(attrGroupKeys))
	rows := make([]LogMetricsRow, 0, initialCap)
	for _, attrGroupKey := range attrGroupKeys {
		var attrVals []string
		if attrGroupKey != "" {
			attrVals = strings.Split(attrGroupKey, "\x00")
		}
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			compositeKey := strconv.FormatInt(bucketIdx, 10) + "\x00" + attrGroupKey
			bucket := buckets[compositeKey]

			groupKey := make([]string, 1+len(attrVals))
			groupKey[0] = strconv.FormatInt(bucketIdx, 10)
			copy(groupKey[1:], attrVals)

			rows = append(rows, LogMetricsRow{
				GroupKey: groupKey,
				Values:   logRowValues(bucket, funcName, stepSec, quantile),
			})
		}
	}
	return rows
}

// logRowValues computes the final metric value for a single bucket.
// SPEC-ELM-6: quantile_over_time uses exact percentile computation over collected values.
func logRowValues(
	bucket *aggBucketState,
	funcName string,
	stepSec float64,
	quantile float64,
) map[string]float64 {
	if bucket == nil || bucket.count == 0 {
		return map[string]float64{funcName: 0}
	}
	switch funcName {
	case logFuncCountOverTime:
		return map[string]float64{funcName: float64(bucket.count)}
	case logFuncRate:
		if stepSec > 0 {
			return map[string]float64{funcName: float64(bucket.count) / stepSec}
		}
		return map[string]float64{funcName: 0}
	case logFuncBytesOverTime:
		return map[string]float64{funcName: bucket.sum}
	case logFuncBytesRate:
		if stepSec > 0 {
			return map[string]float64{funcName: bucket.sum / stepSec}
		}
		return map[string]float64{funcName: 0}
	case logFuncSumOverTime:
		return map[string]float64{funcName: bucket.sum}
	case logFuncAvgOverTime:
		if bucket.count > 0 {
			return map[string]float64{funcName: bucket.sum / float64(bucket.count)}
		}
		return map[string]float64{funcName: 0}
	case logFuncMinOverTime:
		if bucket.min == math.MaxFloat64 {
			return map[string]float64{funcName: 0}
		}
		return map[string]float64{funcName: bucket.min}
	case logFuncMaxOverTime:
		if bucket.max == -math.MaxFloat64 {
			return map[string]float64{funcName: 0}
		}
		return map[string]float64{funcName: bucket.max}
	case logFuncQuantileOverTime:
		return map[string]float64{funcName: logComputeQuantile(bucket.values, quantile)}
	default:
		return map[string]float64{funcName: float64(bucket.count)}
	}
}

// logComputeQuantile computes the q-th percentile (0 ≤ q ≤ 1) from a slice of values.
// Uses nearest-rank method on sorted values. Returns 0 if values is empty.
func logComputeQuantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	slices.Sort(sorted)
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := max(int(math.Ceil(q*float64(len(sorted))))-1, 0)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
