// Package executor .
package executor

import (
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/mattdurham/blockpack/internal/arena"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
	"github.com/mattdurham/blockpack/internal/quantile"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

func (e *BlockpackExecutor) scanAggregationBlocks(
	ctx *queryContext,
	blockOrder []int,
	agg *aggContext,
	output *scanOutput,
) error {
	if ctx.depth <= 0 {
		return fmt.Errorf("nested blockpack depth exceeds limit %d", maxNestedBlockpackDepth)
	}
	if len(blockOrder) == 0 {
		return nil
	}

	var reusableBlock *blockpackio.Block
	coalescedReads := ctx.reader.CoalesceBlocks(blockOrder, ctx.opts.CoalescedReadSize)

	for _, coalescedRead := range coalescedReads {
		blockBytesMap, err := ctx.reader.ReadCoalescedBlocks(coalescedRead)
		if err != nil {
			return fmt.Errorf("read coalesced blocks: %w", err)
		}

		for blockIdx := coalescedRead.StartBlockIdx; blockIdx < coalescedRead.EndBlockIdx; blockIdx++ {
			cachedBytes, ok := blockBytesMap[blockIdx]
			if !ok {
				return fmt.Errorf("block %d not in coalesced read result", blockIdx)
			}

			if ctx.reader.IsBlockpackEntry(blockIdx) {
				subReader, err := blockpackio.NewReaderFromBytes(cachedBytes)
				if err != nil {
					return fmt.Errorf("decode nested blockpack %d: %w", blockIdx, err)
				}
				subOrder, _ := selectBlocksWithPlan(subReader, ctx.predicates, ctx.opts, ctx.arena)
				subCtx := *ctx
				subCtx.reader = subReader
				subCtx.depth = ctx.depth - 1
				err = e.scanAggregationBlocks(&subCtx, subOrder, agg, output)
				if err != nil {
					return err
				}
				continue
			}

			blockWithBytes, err := ctx.reader.GetBlockWithBytes(
				blockIdx,
				agg.allColumnsNeeded,
				reusableBlock,
				cachedBytes,
				ctx.arena,
			)
			if err != nil {
				return fmt.Errorf("get block %d: %w", blockIdx, err)
			}
			block := blockWithBytes.Block
			reusableBlock = block

			// Block-level pruning by time range when metadata is available
			if agg.startTime > 0 && block.MaxStart < uint64(agg.startTime) { //nolint:gosec
				output.executionSkips = append(output.executionSkips, ExecutionSkip{
					BlockID:    blockIdx,
					SpanCount:  block.SpanCount(),
					SkipReason: PruneBlockTimeMetadata,
				})
				continue
			}
			if agg.endTime > 0 && block.MinStart > uint64(agg.endTime) { //nolint:gosec
				output.executionSkips = append(output.executionSkips, ExecutionSkip{
					BlockID:    blockIdx,
					SpanCount:  block.SpanCount(),
					SkipReason: PruneBlockTimeMetadata,
				})
				continue
			}

			// Skip predicate-based block pruning for OR queries
			if ctx.predicates != nil && len(ctx.predicates.AttributeEquals) > 0 && !ctx.predicates.HasOROperations {
				blockMatches := blockMightMatchValuePredicates(block, ctx.predicates)
				if !blockMatches {
					output.executionSkips = append(output.executionSkips, ExecutionSkip{
						BlockID:    blockIdx,
						SpanCount:  block.SpanCount(),
						SkipReason: PruneBlockValuePredicate,
					})
					continue
				}
			}

			output.blocksScanned++

			// Invoke metrics callback if set
			if ctx.opts.MetricsCallback != nil {
				var currentBytes int64
				if ctx.tracking.detailed != nil {
					currentBytes = ctx.tracking.detailed.BytesRead()
				} else if ctx.tracking.standard != nil {
					currentBytes = ctx.tracking.standard.BytesRead()
				}
				ctx.opts.MetricsCallback(currentBytes, output.blocksScanned)
			}

			// Execute query using streaming callback to avoid RowSet allocation
			columnProvider := NewBlockColumnProvider(block)

			matchCount := 0

			// Use StreamingColumnPredicate if available, otherwise fall back to RowSet
			if ctx.program.StreamingColumnPredicate != nil {
				var aggErr error
				var err error
				matchCount, err = ctx.program.StreamingColumnPredicate(columnProvider, func(rowIdx int) bool {
					_, aggErr = processAggregationRow(
						block, rowIdx, agg.startTime, agg.endTime, agg.useTimeBuckets, agg.stepSizeNs,
						ctx.program.AggregationPlan.GroupByFields, agg.groupKeyBuffer, agg.stringInterner,
						ctx.program.AggregationPlan.Aggregates, agg.buckets, agg.timeBucketStrings,
					)
					if aggErr != nil {
						return false // Stop iteration on cardinality limit
					}
					return true // Continue iteration
				})

				if aggErr != nil {
					return aggErr
				}
				if err != nil {
					return fmt.Errorf("streaming aggregation failed: %w", err)
				}
			} else {
				// Fallback: Use old RowSet-based approach
				rowSet, err := ctx.program.ColumnPredicate(columnProvider)
				if err != nil {
					return fmt.Errorf("closure execution failed: %w", err)
				}
				matchedRowIndices := rowSet.ToSlice()
				matchCount = len(matchedRowIndices)

				// Process matches and update aggregation buckets
				for _, rowIdx := range matchedRowIndices {
					_, err := processAggregationRow(
						block, rowIdx, agg.startTime, agg.endTime, agg.useTimeBuckets, agg.stepSizeNs,
						ctx.program.AggregationPlan.GroupByFields, agg.groupKeyBuffer, agg.stringInterner,
						ctx.program.AggregationPlan.Aggregates, agg.buckets, agg.timeBucketStrings,
					)
					if err != nil {
						return err
					}
				}
			}

			// Skip if no matches
			if matchCount == 0 {
				continue
			}
		}
	}

	return nil
}

// blockpackAttributeProvider mirrors the benchmark VM provider but keeps IDs handy.
// updateBucketCount increments the count in a bucket
func updateBucketCount(bucket *vm.AggBucket) {
	bucket.Count++
}

// updateBucketSum adds a value to the sum in a bucket
func updateBucketSum(bucket *vm.AggBucket, value float64) {
	bucket.Sum += value
}

// updateBucketMin updates the minimum value in a bucket.
// Buckets are initialized with Min=math.MaxFloat64, so simple comparison works.
func updateBucketMin(bucket *vm.AggBucket, value float64) {
	if value < bucket.Min {
		bucket.Min = value
	}
}

// updateBucketMax updates the maximum value in a bucket.
// Buckets are initialized with Max=-math.MaxFloat64, so simple comparison works.
func updateBucketMax(bucket *vm.AggBucket, value float64) {
	if value > bucket.Max {
		bucket.Max = value
	}
}

// finalizeAvg computes the average from sum and count
func finalizeAvg(bucket *vm.AggBucket) float64 {
	if bucket.Count == 0 {
		return math.NaN()
	}
	return bucket.Sum / float64(bucket.Count)
}

// finalizeRate computes the rate from count and time window
func finalizeRate(bucket *vm.AggBucket, timeWindowSeconds float64) float64 {
	if timeWindowSeconds == 0 {
		return 0.0
	}
	return float64(bucket.Count) / timeWindowSeconds
}

// initBucketQuantile initializes a quantile sketch for a field in a bucket.
// The real sketch is stored directly in bucket.Quantiles so that Merge() works correctly.
func initBucketQuantile(bucket *vm.AggBucket, fieldName string) {
	if bucket.Quantiles == nil {
		bucket.Quantiles = make(map[string]*quantile.QuantileSketch)
	}
	bucket.Quantiles[fieldName] = quantile.NewQuantileSketch(0.01)
}

// updateBucketQuantile adds a value to the quantile sketch for a field
func updateBucketQuantile(bucket *vm.AggBucket, fieldName string, value float64) {
	if bucket.Quantiles == nil || bucket.Quantiles[fieldName] == nil {
		initBucketQuantile(bucket, fieldName)
	}
	bucket.Quantiles[fieldName].Add(value)
}

// extractBucketQuantile extracts a quantile value from the sketch for a field
func extractBucketQuantile(bucket *vm.AggBucket, fieldName string, q float64) float64 {
	if bucket.Quantiles == nil || bucket.Quantiles[fieldName] == nil {
		return 0.0
	}
	return bucket.Quantiles[fieldName].Quantile(q)
}

// updateBucketHistogram adds a value to the histogram for a field
func updateBucketHistogram(bucket *vm.AggBucket, fieldName string, value float64) {
	if bucket.Histograms == nil {
		bucket.Histograms = make(map[string]*vm.HistogramData)
	}
	if bucket.Histograms[fieldName] == nil {
		// Initialize histogram with exponential buckets
		// Buckets: 0, 1ms, 10ms, 100ms, 1s, 10s, 100s, ...
		bucket.Histograms[fieldName] = &vm.HistogramData{
			Buckets: []float64{0, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, math.MaxFloat64},
			Counts:  make([]int64, 9), // One more than buckets for overflow
		}
	}

	hist := bucket.Histograms[fieldName]
	// Find the bucket for this value
	for i, boundary := range hist.Buckets {
		if value < boundary {
			hist.Counts[i]++
			return
		}
	}
	// Overflow bucket (last one)
	hist.Counts[len(hist.Counts)-1]++
}

// updateBucketStddev adds a value for standard deviation calculation
func updateBucketStddev(bucket *vm.AggBucket, value float64) {
	bucket.Sum += value
	bucket.SumSq += value * value
	bucket.Count++
}

// finalizeStddev computes the standard deviation from sum, sum of squares, and count
func finalizeStddev(bucket *vm.AggBucket) float64 {
	if bucket.Count == 0 {
		return 0.0
	}
	// Variance = E[X^2] - E[X]^2
	mean := bucket.Sum / float64(bucket.Count)
	meanSq := bucket.SumSq / float64(bucket.Count)
	variance := meanSq - mean*mean
	if variance < 0 {
		variance = 0 // Handle floating point errors
	}
	return math.Sqrt(variance)
}

// readPrecomputedMetricStreams reads pre-computed metric stream results from a metric stream.
// This is the fast path for queries that match pre-computed metric streams.
// Implementation will be added in Phase 6 when Writer support is complete.
func (e *BlockpackExecutor) readPrecomputedMetricStreams(
	stream *MetricStream,
	opts QueryOptions,
) (*BlockpackResult, error) {
	if stream == nil {
		return nil, fmt.Errorf("metric stream cannot be nil")
	}
	if stream.MetricStreamPath == "" {
		return nil, fmt.Errorf("metric stream path is required for precomputed metric streams")
	}
	if stream.Spec == nil {
		return nil, fmt.Errorf("metric stream spec is required for precomputed metric streams")
	}

	providerStorage, ok := e.storage.(ProviderStorage)
	if !ok {
		return nil, fmt.Errorf("storage must implement ProviderStorage interface")
	}

	provider, err := providerStorage.GetProvider(stream.MetricStreamPath)
	if err != nil {
		return nil, fmt.Errorf("read blockpack file: %w", err)
	}

	// Close provider after query execution to prevent file descriptor leaks
	if closeable, ok := provider.(blockpackio.CloseableReaderProvider); ok {
		defer func() {
			if err := closeable.Close(); err != nil { //nolint:govet
				log.Printf("failed to close provider for %s: %v", stream.MetricStreamPath, err)
			}
		}()
	}

	var tracking *blockpackio.TrackingReaderProvider
	if opts.IOLatency > 0 {
		var err error //nolint:govet
		tracking, err = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
		if err != nil {
			return nil, fmt.Errorf("create tracking reader: %w", err)
		}
	} else {
		var err error //nolint:govet
		tracking, err = blockpackio.NewTrackingReaderProvider(provider)
		if err != nil {
			return nil, fmt.Errorf("create tracking reader: %w", err)
		}
	}

	reader, err := blockpackio.NewReaderFromProvider(tracking)
	if err != nil {
		return nil, err
	}

	rawData, err := reader.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}
	if len(rawData) == 0 {
		return nil, fmt.Errorf("no metric stream blocks found")
	}

	streamData, err := findMetricStreamByID(rawData, stream.StreamID)
	if err != nil {
		return nil, err
	}

	parsed, err := parseMetricStreamData(streamData)
	if err != nil {
		return nil, err
	}

	aggSpec, err := aggSpecFromQuerySpec(stream.Spec)
	if err != nil {
		return nil, err
	}

	result, err := buildPrecomputedResult(parsed, aggSpec, opts)
	if err != nil {
		return nil, err
	}

	result.BytesRead = tracking.BytesRead()
	result.IOOperations = tracking.IOOperations()
	return result, nil
}

// executeAggregationQuery executes a query with GROUP BY and/or aggregate functions
func (e *BlockpackExecutor) executeAggregationQuery(
	path string,
	program *vm.Program,
	opts QueryOptions,
) (*BlockpackResult, error) {
	reader, tracking, closeFn, err := e.buildTrackedReader(path, opts)
	if err != nil {
		return nil, err
	}
	if closeFn != nil {
		defer closeFn()
	}

	// Note: File structure info would improve categorization accuracy,
	// but Reader doesn't expose these fields. The detailed tracker will use
	// caller-based heuristics instead (good enough for identifying bottlenecks).

	// Create arena for temporary allocations during query execution
	var queryArena arena.Arena
	defer queryArena.Free()

	startTime := opts.StartTime
	endTime := opts.EndTime

	// Time bucket configuration for automatic time bucketing
	// When StartTime and EndTime are set, automatically bucket results by time intervals
	// to produce time-series results matching TraceQL metrics behavior
	var useTimeBuckets bool
	var stepSizeNs int64
	if startTime > 0 && endTime > 0 {
		useTimeBuckets = true
		stepSizeNs = 60 * 1e9 // 1 minute = 60 seconds in nanoseconds (matches TraceQL metrics)
	}

	// Use predicates extracted directly from AST during compilation
	predicates := program.Predicates
	if predicates == nil {
		predicates = &vm.QueryPredicates{
			AttributeEquals:       make(map[string][]vm.Value),
			DedicatedColumns:      make(map[string][]vm.Value),
			DedicatedColumnsRegex: make(map[string]string),
		}
	}

	// Extract time range from WHERE clause predicates and intersect with opts time range
	// This allows block pruning to use the narrowest time range from both sources:
	// 1. opts.StartTime/EndTime (time bucketing range)
	// 2. WHERE span:start >= X AND span:start < Y (actual filter predicates)
	pruneOpts := opts
	if timeRange, ok := predicates.DedicatedRanges["span:start"]; ok {
		// Intersect with WHERE clause time predicates for block pruning
		if timeRange.MinValue != nil {
			if minTime, ok := timeRange.MinValue.Data.(int64); ok {
				// Use the later of the two start times (narrower range)
				if pruneOpts.StartTime == 0 || minTime > pruneOpts.StartTime {
					pruneOpts.StartTime = minTime
				}
			}
		}
		if timeRange.MaxValue != nil {
			if maxTime, ok := timeRange.MaxValue.Data.(int64); ok {
				// Use the earlier of the two end times (narrower range)
				if pruneOpts.EndTime == 0 || maxTime < pruneOpts.EndTime {
					pruneOpts.EndTime = maxTime
				}
			}
		}
	}

	blockOrder, queryPlan := selectBlocksWithPlan(reader, predicates, pruneOpts, &queryArena)
	blockOrder, _ = applyScanFallback(reader, blockOrder, opts)

	// Copy queryPlan slices out of arena before arena is freed
	queryPlanCopy := copyQueryPlanFromArena(queryPlan)

	executionSkips := make([]ExecutionSkip, 0)

	// Extract columns needed for filtering and aggregation
	// For aggregation queries, we need ALL columns upfront (not two-pass like non-aggregation)
	// because we need both filter columns and aggregation columns for every matching row
	filterColumns := extractFilterColumns(predicates, startTime, endTime)
	aggregationColumns := extractAggregationColumns(program, opts)

	// Merge filter and aggregation columns into single set
	allColumnsNeeded := make(map[string]struct{})
	for col := range filterColumns {
		allColumnsNeeded[col] = struct{}{}
	}
	for col := range aggregationColumns {
		allColumnsNeeded[col] = struct{}{}
	}

	// Map to store aggregation buckets: groupKey -> bucket
	buckets := make(map[string]*vm.AggBucket)

	// Initialize string interner for group keys (reduces duplicate string allocations)
	stringInterner := NewStringInterner()

	// Reusable buffer for group key extraction (avoids per-span allocations)
	maxGroupByFields := len(program.AggregationPlan.GroupByFields)
	groupKeyBuffer := make([]string, maxGroupByFields)

	ctx := &queryContext{
		reader:     reader,
		program:    program,
		predicates: predicates,
		opts:       opts,
		arena:      &queryArena,
		tracking:   tracking,
		depth:      maxNestedBlockpackDepth,
	}
	// Pre-compute time bucket index strings to avoid per-row strconv.FormatInt allocations
	var timeBucketStrings map[int64]string
	if useTimeBuckets && stepSizeNs > 0 {
		numBuckets := (endTime - startTime + stepSizeNs - 1) / stepSizeNs
		if numBuckets > 0 && numBuckets <= 100000 {
			timeBucketStrings = make(map[int64]string, numBuckets)
			for i := int64(0); i < numBuckets; i++ {
				timeBucketStrings[i] = strconv.FormatInt(i, 10)
			}
		}
	}

	agg := &aggContext{
		allColumnsNeeded:  allColumnsNeeded,
		startTime:         startTime,
		endTime:           endTime,
		useTimeBuckets:    useTimeBuckets,
		stepSizeNs:        stepSizeNs,
		groupKeyBuffer:    groupKeyBuffer,
		stringInterner:    stringInterner,
		buckets:           buckets,
		timeBucketStrings: timeBucketStrings,
	}
	output := &scanOutput{
		executionSkips: executionSkips,
	}
	err = e.scanAggregationBlocks(ctx, blockOrder, agg, output)
	if err != nil {
		return nil, err
	}

	// Add execution-time skips to the query plan
	if queryPlanCopy != nil {
		queryPlanCopy.ExecutionSkips = output.executionSkips
	}

	stats := collectTrackingStats(tracking)

	// Build final result from buckets
	result := buildAggregateResult(
		agg.buckets,
		program.AggregationPlan,
		queryPlanCopy,
		output.blocksScanned,
		stats.bytesRead,
		stats.ioOps,
		startTime,
		endTime,
		stepSizeNs,
	)
	result.DetailedIOStats = stats.detailedStats
	result.IOAnalysisText = stats.analysisText

	return result, nil
}

// processAggregationRow handles time filtering, group key extraction, and aggregate updates for a single row.
// Returns true if the row was processed, false if filtered out, and error if cardinality limit exceeded.
func processAggregationRow(
	block *blockpackio.Block,
	rowIdx int,
	startTime, endTime int64,
	useTimeBuckets bool,
	stepSizeNs int64,
	groupByFields []string,
	groupKeyBuffer []string,
	stringInterner *StringInterner,
	aggregates []vm.AggSpec,
	buckets map[string]*vm.AggBucket,
	timeBucketStrings map[int64]string,
) (bool, error) {
	// Apply time filtering and get span start time for bucketing
	var spanStartTime uint64
	if startTime > 0 || endTime > 0 || useTimeBuckets {
		col := block.GetColumn("span:start")
		if col != nil {
			if st, ok := col.Uint64Value(rowIdx); ok {
				spanStartTime = st
				stInt := int64(st) //nolint:gosec
				// Apply time range filter
				if (startTime > 0 && stInt < startTime) || (endTime > 0 && stInt > endTime) {
					return false, nil
				}
			}
		}
	}

	// Extract group key values using reusable buffer
	extractGroupKeyValuesReuse(block, rowIdx, groupByFields, groupKeyBuffer)

	// Build group key with string interning
	var groupKey string
	if useTimeBuckets {
		timeBucket := (int64(spanStartTime) - startTime) / stepSizeNs //nolint:gosec
		// Use pre-computed string if available, fall back to strconv.FormatInt
		timeBucketStr, ok := timeBucketStrings[timeBucket]
		if !ok {
			timeBucketStr = strconv.FormatInt(timeBucket, 10)
		}
		groupKey = stringInterner.InternConcat(
			timeBucketStr,
			"\x00",
			concatenateGroupKeyFromSlice(groupKeyBuffer[:len(groupByFields)]),
		)
	} else {
		groupKey = stringInterner.Intern(
			concatenateGroupKeyFromSlice(groupKeyBuffer[:len(groupByFields)]),
		)
	}

	// Get or create bucket for this group
	bucket, exists := buckets[groupKey]
	if !exists {
		// Check cardinality limit before creating new bucket
		if len(buckets) >= shared.MaxGroupByCardinality {
			return false, fmt.Errorf(
				"GROUP BY cardinality limit exceeded: %d groups (max %d)",
				len(buckets),
				shared.MaxGroupByCardinality,
			)
		}
		bucket = &vm.AggBucket{
			Min: math.MaxFloat64,  // Initialize to +Inf for MIN aggregation
			Max: -math.MaxFloat64, // Initialize to -Inf for MAX aggregation
		}
		buckets[groupKey] = bucket
	}

	// Update aggregates for this row
	updateAggregatesForRow(block, rowIdx, aggregates, bucket)

	return true, nil
}

// extractGroupKeyValuesReuse extracts values for GROUP BY fields into provided buffer
// Reuses the values slice to avoid allocation
func extractGroupKeyValuesReuse(block *blockpackio.Block, rowIdx int, groupByFields []string, values []string) {
	// Ensure buffer is large enough
	if len(values) < len(groupByFields) {
		panic(
			fmt.Sprintf(
				"extractGroupKeyValuesReuse: values buffer too small (got %d, need %d)",
				len(values),
				len(groupByFields),
			),
		)
	}

	for i, field := range groupByFields {
		col := block.GetColumn(field)
		if col == nil {
			// Try alternative column names
			for _, altName := range GetPossibleColumnNames(field) {
				col = block.GetColumn(altName)
				if col != nil {
					break
				}
			}
		}
		if col == nil {
			values[i] = "" // NULL value
			continue
		}

		// Extract value based on column type
		switch col.Type {
		case blockpack.ColumnTypeString:
			if v, ok := col.StringValue(rowIdx); ok {
				values[i] = v
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeInt64:
			if v, ok := col.Int64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeUint64:
			if v, ok := col.Uint64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBool:
			if v, ok := col.BoolValue(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeFloat64:
			if v, ok := col.Float64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBytes:
			if v, ok := col.BytesValueView(rowIdx); ok {
				values[i] = hex.EncodeToString(v)
			} else {
				values[i] = ""
			}
		default:
			values[i] = ""
		}
	}
}

// concatenateGroupKeyFromSlice builds a group key from string slice (no allocation)
func concatenateGroupKeyFromSlice(values []string) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) == 1 {
		return values[0]
	}

	// Calculate total length
	totalLen := len(values) - 1 // separators (null bytes)
	for _, v := range values {
		totalLen += len(v)
	}

	// Build string efficiently
	var b strings.Builder
	b.Grow(totalLen)
	for i, v := range values {
		if i > 0 {
			b.WriteByte('\x00')
		}
		b.WriteString(v)
	}
	return b.String()
}

// formatValueAsString converts a value to string representation
func formatValueAsString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		return hex.EncodeToString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// updateAggregatesForRow updates all aggregate values for a single row
func updateAggregatesForRow(block *blockpackio.Block, rowIdx int, aggregates []vm.AggSpec, bucket *vm.AggBucket) {
	for _, agg := range aggregates {
		switch agg.Function {
		case vm.AggCount:
			updateBucketCount(bucket)

		case vm.AggRate:
			// RATE is count-based like COUNT, but finalized as count/time_range
			updateBucketCount(bucket)

		case vm.AggSum, vm.AggAvg, vm.AggMin, vm.AggMax:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}

			switch agg.Function {
			case vm.AggSum, vm.AggAvg:
				updateBucketSum(bucket, value)
				updateBucketCount(bucket) // Need count for AVG
			case vm.AggMin:
				updateBucketMin(bucket, value)
			case vm.AggMax:
				updateBucketMax(bucket, value)
			}

		case vm.AggQuantile:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketQuantile(bucket, agg.Field, value)

		case vm.AggHistogram:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketHistogram(bucket, agg.Field, value)

		case vm.AggStddev:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketStddev(bucket, value)
		}
	}
}

// convertToFloat64 extracts a column value and converts it to float64
func convertToFloat64(block *blockpackio.Block, rowIdx int, field string) (float64, bool) {
	col := block.GetColumn(field)
	if col == nil {
		// Try alternative column names
		for _, altName := range GetPossibleColumnNames(field) {
			col = block.GetColumn(altName)
			if col != nil {
				break
			}
		}
	}
	if col == nil {
		// Special case: if span:duration column doesn't exist, compute it from span:end - span:start
		if field == "span:duration" {
			startCol := block.GetColumn("span:start")
			endCol := block.GetColumn("span:end")
			if startCol != nil && endCol != nil {
				if start, ok := startCol.Uint64Value(rowIdx); ok {
					if end, ok := endCol.Uint64Value(rowIdx); ok {
						duration := int64(end) - int64(start) //nolint:gosec
						return float64(duration), true
					}
				}
			}
		}
		return 0, false
	}

	switch col.Type {
	case blockpack.ColumnTypeInt64:
		if v, ok := col.Int64Value(rowIdx); ok {
			return float64(v), true
		}
	case blockpack.ColumnTypeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return float64(v), true
		}
	case blockpack.ColumnTypeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			if v {
				return 1.0, true
			}
			return 0.0, true
		}
	}

	return 0, false
}

// buildAggregateResult constructs the final BlockpackResult from aggregation buckets
func buildAggregateResult(
	buckets map[string]*vm.AggBucket,
	plan *vm.AggregationPlan,
	queryPlan *QueryPlan,
	blocksScanned int,
	bytesRead int64,
	ioOperations int64,
	startTime int64,
	endTime int64,
	stepSizeNs int64,
) *BlockpackResult {
	// Determine if time bucketing was used
	useTimeBuckets := (startTime > 0 && endTime > 0)

	// For dense time-series, we need to generate ALL time buckets, not just those with data
	if useTimeBuckets {
		return buildDenseTimeSeriesResult(
			buckets,
			plan,
			queryPlan,
			blocksScanned,
			bytesRead,
			ioOperations,
			startTime,
			endTime,
			stepSizeNs,
		)
	}

	// Original sparse implementation for non-time-series queries
	rows := make([]AggregateRow, 0, len(buckets))

	for groupKey, bucket := range buckets {
		// Parse group key back into values
		var groupKeyValues []string
		if groupKey == "" {
			// Global aggregation (no GROUP BY, no time bucketing)
			groupKeyValues = []string{}
		} else {
			parts := strings.Split(groupKey, "\x00")
			// Filter out empty strings (can occur for global aggregation with time bucketing)
			// Global aggregation with time bucketing: groupKey = "0\x00" -> split to ["0", ""]
			// We want just ["0"]
			groupKeyValues = make([]string, 0, len(parts))
			for _, part := range parts {
				if part != "" {
					groupKeyValues = append(groupKeyValues, part)
				}
			}
		}

		// Build values map from aggregates
		values := make(map[string]float64)
		for _, agg := range plan.Aggregates {
			outputName := aggregationOutputName(agg)

			// Extract final value
			var finalValue float64
			switch agg.Function {
			case vm.AggCount:
				finalValue = float64(bucket.Count)
			case vm.AggRate:
				// RATE = count / time_range_seconds
				// Use entire query range for rate calculation (no time bucketing)
				timeRangeSeconds := float64(endTime-startTime) / 1e9
				finalValue = finalizeRate(bucket, timeRangeSeconds)
			case vm.AggSum:
				finalValue = bucket.Sum
			case vm.AggAvg:
				finalValue = finalizeAvg(bucket)
			case vm.AggMin:
				// If Min was never updated, it's still MaxFloat64 - return 0 or NaN
				if bucket.Min == math.MaxFloat64 {
					finalValue = 0
				} else {
					finalValue = bucket.Min
				}
			case vm.AggMax:
				// If Max was never updated, it's still -MaxFloat64 - return 0 or NaN
				if bucket.Max == -math.MaxFloat64 {
					finalValue = 0
				} else {
					finalValue = bucket.Max
				}
			case vm.AggQuantile:
				finalValue = extractBucketQuantile(bucket, agg.Field, agg.Quantile)
			case vm.AggHistogram:
				// For histogram, we return 0 as a placeholder
				// Real histogram data would be returned in a different format
				finalValue = 0
			case vm.AggStddev:
				finalValue = finalizeStddev(bucket)
			}

			values[outputName] = finalValue
		}

		rows = append(rows, AggregateRow{
			GroupKey: groupKeyValues,
			Values:   values,
		})
	}

	// Sort rows by group key for deterministic output
	sort.Slice(rows, func(i, j int) bool {
		// Compare group keys lexicographically
		for k := 0; k < len(rows[i].GroupKey) && k < len(rows[j].GroupKey); k++ {
			if rows[i].GroupKey[k] != rows[j].GroupKey[k] {
				return rows[i].GroupKey[k] < rows[j].GroupKey[k]
			}
		}
		return len(rows[i].GroupKey) < len(rows[j].GroupKey)
	})

	return &BlockpackResult{
		Matches:       nil, // No individual matches for aggregation queries
		BlocksScanned: blocksScanned,
		BytesRead:     bytesRead,
		IOOperations:  ioOperations,
		QueryPlan:     queryPlan,
		AggregateRows: rows,
		IsAggregated:  true,
	}
}

// buildDenseTimeSeriesResult generates dense time-series with ALL time buckets (including empty ones)
// This matches TraceQL metrics behavior which always returns a value for every time interval
func buildDenseTimeSeriesResult(
	buckets map[string]*vm.AggBucket,
	plan *vm.AggregationPlan,
	queryPlan *QueryPlan,
	blocksScanned int,
	bytesRead int64,
	ioOperations int64,
	startTime int64,
	endTime int64,
	stepSizeNs int64,
) *BlockpackResult {
	numBuckets := denseBucketCount(startTime, endTime, stepSizeNs)
	uniqueAttributeGroups := extractDenseAttributeGroups(buckets)
	rows := buildDenseAggregateRows(buckets, plan, numBuckets, uniqueAttributeGroups, stepSizeNs)
	sortDenseAggregateRows(rows)

	return buildDenseAggregateResult(rows, queryPlan, blocksScanned, bytesRead, ioOperations)
}

func denseBucketCount(startTime, endTime, stepSizeNs int64) int64 {
	return (endTime - startTime + stepSizeNs - 1) / stepSizeNs
}

func extractDenseAttributeGroups(buckets map[string]*vm.AggBucket) map[string]struct{} {
	uniqueAttributeGroups := make(map[string]struct{})
	for groupKey := range buckets {
		attrGroupKey := denseAttributeGroupKey(groupKey)
		uniqueAttributeGroups[attrGroupKey] = struct{}{}
	}
	return uniqueAttributeGroups
}

func denseAttributeGroupKey(groupKey string) string {
	if groupKey == "" {
		return ""
	}
	parts := strings.Split(groupKey, "\x00")
	if len(parts) > 1 {
		return strings.Join(parts[1:], "\x00")
	}
	return ""
}

func buildDenseAggregateRows(
	buckets map[string]*vm.AggBucket,
	plan *vm.AggregationPlan,
	numBuckets int64,
	uniqueAttributeGroups map[string]struct{},
	stepSizeNs int64,
) []AggregateRow {
	rows := make([]AggregateRow, 0, int(numBuckets)*len(uniqueAttributeGroups))
	for attrGroupKey := range uniqueAttributeGroups {
		attrGroupValues := denseAttributeGroupValues(attrGroupKey)
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			fullGroupKey := denseFullGroupKey(bucketIdx, attrGroupKey)
			bucket := buckets[fullGroupKey]
			if bucket == nil {
				bucket = emptyAggBucket()
			}

			groupKeyValues := make([]string, 0, 1+len(attrGroupValues))
			groupKeyValues = append(groupKeyValues, fmt.Sprintf("%d", bucketIdx))
			groupKeyValues = append(groupKeyValues, attrGroupValues...)

			values := buildDenseAggregateValues(bucket, plan, stepSizeNs)
			rows = append(rows, AggregateRow{
				GroupKey: groupKeyValues,
				Values:   values,
			})
		}
	}
	return rows
}

func denseAttributeGroupValues(attrGroupKey string) []string {
	if attrGroupKey == "" {
		return []string{}
	}
	parts := strings.Split(attrGroupKey, "\x00")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func denseFullGroupKey(bucketIdx int64, attrGroupKey string) string {
	if attrGroupKey == "" {
		return fmt.Sprintf("%d\x00", bucketIdx)
	}
	return fmt.Sprintf("%d\x00%s", bucketIdx, attrGroupKey)
}

func emptyAggBucket() *vm.AggBucket {
	return &vm.AggBucket{
		Count: 0,
		Sum:   0,
		Min:   math.MaxFloat64,
		Max:   -math.MaxFloat64,
	}
}

func buildDenseAggregateValues(bucket *vm.AggBucket, plan *vm.AggregationPlan, stepSizeNs int64) map[string]float64 {
	values := make(map[string]float64, len(plan.Aggregates))
	for _, agg := range plan.Aggregates {
		outputName := aggregationOutputName(agg)
		values[outputName] = aggregationValue(bucket, agg, stepSizeNs)
	}
	return values
}

func aggregationOutputName(agg vm.AggSpec) string {
	if agg.Alias != "" {
		return agg.Alias
	}
	fieldName := strings.TrimPrefix(agg.Field, "span:")
	fieldName = strings.TrimPrefix(fieldName, "span.")
	fieldName = strings.TrimPrefix(fieldName, "resource:")
	fieldName = strings.TrimPrefix(fieldName, "resource.")

	switch agg.Function {
	case vm.AggCount:
		return "count"
	case vm.AggRate:
		return "rate"
	case vm.AggSum:
		return "sum_" + fieldName
	case vm.AggAvg:
		return "avg_" + fieldName
	case vm.AggMin:
		return "min_" + fieldName
	case vm.AggMax:
		return "max_" + fieldName
	case vm.AggQuantile:
		return fmt.Sprintf("quantile_%s_%g", fieldName, agg.Quantile)
	case vm.AggHistogram:
		return "histogram_" + fieldName
	case vm.AggStddev:
		return "stddev_" + fieldName
	}
	functionName := strings.ToLower(agg.Function.String())
	if fieldName == "" {
		return "agg_" + functionName
	}
	return fieldName + "_" + functionName
}

func aggregationValue(bucket *vm.AggBucket, agg vm.AggSpec, stepSizeNs int64) float64 {
	switch agg.Function {
	case vm.AggCount:
		return float64(bucket.Count)
	case vm.AggRate:
		timeRangeSeconds := float64(stepSizeNs) / 1e9
		return finalizeRate(bucket, timeRangeSeconds)
	case vm.AggSum:
		return bucket.Sum
	case vm.AggAvg:
		return finalizeAvg(bucket)
	case vm.AggMin:
		if bucket.Min == math.MaxFloat64 {
			return 0
		}
		return bucket.Min
	case vm.AggMax:
		if bucket.Max == -math.MaxFloat64 {
			return 0
		}
		return bucket.Max
	case vm.AggQuantile:
		return extractBucketQuantile(bucket, agg.Field, agg.Quantile)
	case vm.AggHistogram:
		return 0
	case vm.AggStddev:
		return finalizeStddev(bucket)
	}
	return 0
}

func sortDenseAggregateRows(rows []AggregateRow) {
	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(rows[i].GroupKey) && k < len(rows[j].GroupKey); k++ {
			if rows[i].GroupKey[k] != rows[j].GroupKey[k] {
				if k == 0 {
					iVal, err := strconv.ParseInt(rows[i].GroupKey[k], 10, 64)
					if err != nil {
						return false
					}
					jVal, err := strconv.ParseInt(rows[j].GroupKey[k], 10, 64)
					if err != nil {
						return false
					}
					return iVal < jVal
				}
				return rows[i].GroupKey[k] < rows[j].GroupKey[k]
			}
		}
		return len(rows[i].GroupKey) < len(rows[j].GroupKey)
	})
}

func buildDenseAggregateResult(
	rows []AggregateRow,
	queryPlan *QueryPlan,
	blocksScanned int,
	bytesRead int64,
	ioOperations int64,
) *BlockpackResult {
	return &BlockpackResult{
		Matches:       nil,
		BlocksScanned: blocksScanned,
		BytesRead:     bytesRead,
		IOOperations:  ioOperations,
		QueryPlan:     queryPlan,
		AggregateRows: rows,
		IsAggregated:  true,
	}
}
