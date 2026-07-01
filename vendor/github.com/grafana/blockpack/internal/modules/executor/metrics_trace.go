package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// NOTE-071/NOTE-073: compositeKey scratch-buffer pool — eliminates per-span string allocations
// in the metrics accumulation hot loop. string([]byte) used as a map key does not allocate when
// the compiler can prove the string doesn't escape the map index expression. The pool also feeds
// the strings.Join-elimination path below (appends attrVals directly into scratch).

// Pool cap guard constants for compositeKeyScratchPool.
// compositeKeyScratchDefaultCap covers typical keys (3-digit bucket + NUL + service name + NUL + float boundary).
// compositeKeyScratchMaxCap: buffers larger than this are replaced with a fresh default-sized slice on Put.
const (
	compositeKeyScratchDefaultCap = 128        // initial and replacement capacity
	compositeKeyScratchMaxCap     = 256 * 1024 // 256 KB cap guard
)

//nolint:gochecknoglobals
var compositeKeyScratchPool = &sync.Pool{
	New: func() any {
		b := make([]byte, 0, compositeKeyScratchDefaultCap)
		return &b
	},
}

func acquireCompositeKeyScratch() *[]byte {
	//nolint:forcetypeassert
	return compositeKeyScratchPool.Get().(*[]byte)
}

func releaseCompositeKeyScratch(b *[]byte) {
	if cap(*b) > compositeKeyScratchMaxCap {
		// Replace oversized backing array with a fresh small slice.
		// Assign through the pointer so the pool receives the new small slice.
		*b = make([]byte, 0, compositeKeyScratchDefaultCap)
	} else {
		*b = (*b)[:0]
	}
	compositeKeyScratchPool.Put(b)
}

// TraceMetricLabel is a single label key-value pair for a TraceTimeSeries.
// SPEC-ETM-1: Labels are an ordered slice; the same label name will not appear twice.

// TraceTimeSeries is a single time series in a TraceMetricsResult.
// SPEC-ETM-2: Values[i] is NaN when no data exists for bucket i (except COUNT/RATE which use 0).
// SPEC-ETM-3: len(Values) == numBuckets where numBuckets = ceil((EndTime-StartTime)/StepSizeNanos).

// TraceMetricsResult is the output of ExecuteTraceMetrics.
// SPEC-ETM-4: Nil reader returns empty TraceMetricsResult with no error.
// SPEC-ETM-5: Nil querySpec returns an error.

// ExecuteTraceMetrics runs a TraceQL metrics query against a blockpack trace file.
// SPEC-ETM-4 through SPEC-ETM-9: for each matched span, accumulates into time-series
// buckets keyed by (bucketIdx, groupByLabels).
//
// querySpec.TimeBucketing must be enabled with a positive StepSizeNanos.
// Supported functions: COUNT, RATE, SUM, AVG, MIN, MAX, HISTOGRAM, QUANTILE, STDDEV.
func ExecuteTraceMetrics(
	ctx context.Context,
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
	// Normalize nil context to Background so downstream ctx.Err()/ctx.Done() calls
	// (intrinsic fast path, blockGroupPipeline) don't panic. Callers should pass a
	// real context; this guard is a safety net.
	if ctx == nil {
		ctx = context.Background()
	}

	// Normalize short TraceQL intrinsic field names to full column names.
	// The TraceQL parser produces "duration" but all column lookups expect "span:duration".
	// Must happen before outputCols is built so metricsColumnsAreIntrinsic sees the correct name.
	querySpec.Aggregate.Field = normalizeIntrinsicFieldName(querySpec.Aggregate.Field)
	for i, g := range querySpec.Aggregate.GroupBy {
		querySpec.Aggregate.GroupBy[i] = normalizeIntrinsicFieldName(g)
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
		// NOTE-464 (issue #383): all blocks pruned at the block level — no full fetch needed.
		emitPlannerSpan(ctx, plan, &PlannerSpanStats{FullFetchSkipped: true})
		return result, nil
	}

	// predicateCols: only the columns required by ColumnPredicate (first pass).
	// outputCols: metric columns needed for accumulation (always non-nil, even when
	// predicateCols is nil). An explicit non-nil set is required so metricsColumnsAreIntrinsic
	// can correctly evaluate intrinsic eligibility for each individual column; a nil set
	// would make that function return true unconditionally (empty loop), incorrectly routing
	// queries through the intrinsic path when custom attribute columns are needed.
	// SPEC-ETM-9: span:start, the aggregate field, and all GroupBy columns are mandatory for
	// correct metric accumulation; missing any produces silently wrong results.
	// NOTE-018: two-pass decode — predicate columns first, output columns only for blocks with
	// matches — avoids decoding metric columns on blocks that the predicate rejects entirely.
	predicateCols := ProgramWantColumns(program)
	extraCols := []string{colNameSpanStart}
	if querySpec.Aggregate.Field != "" {
		extraCols = append(extraCols, querySpec.Aggregate.Field)
	}
	extraCols = append(extraCols, querySpec.Aggregate.GroupBy...)
	// outputCols = predicateCols ∪ extraCols (always non-nil).
	// When predicateCols is nil (predicate needs all columns), outputCols still explicitly
	// lists the metric-specific columns so the intrinsic eligibility check is precise.
	outputCols := make(map[string]struct{}, len(predicateCols)+len(extraCols))
	for k := range predicateCols { // safe when predicateCols is nil (no-op)
		outputCols[k] = struct{}{}
	}
	for _, c := range extraCols {
		outputCols[c] = struct{}{}
	}

	// NOTE-433: intrinsic fast path removed. IntrinsicTOC no longer exists in v2 files.

	// NOTE-440: this metrics query needs full block payloads (selected blocks were not all
	// pruned above). Report full_fetch_skipped=false.
	emitPlannerSpan(ctx, plan, nil)

	// SPEC-ETM-12 / SPEC-STREAM-11: Blocks are fetched concurrently via blockGroupPipeline
	// (W workers, bounded channel), processed sequentially for parse safety. Peak memory is
	// O(W × group_size) rather than all selected blocks. NOTE-058.
	groups := r.CoalescedGroups(plan.SelectedBlocks)

	// buckets maps a composite key to aggBucketState.
	// For most aggregates: bucketIdxStr + "\x00" + attrGroupKey.
	// For HISTOGRAM:       bucketIdxStr + "\x00" + attrGroupKey + "\x00" + histBoundary.
	// attrGroupKey itself uses "\x00" to separate multiple GroupBy values.
	// See traceAccumulateRow for the authoritative key format.
	buckets := make(map[string]*aggBucketState)

	groupBy := querySpec.Aggregate.GroupBy
	_, _, _, pipelineErr := blockGroupPipeline(
		ctx, r, groups, defaultPipelineWorkers, outputCols,
		func(groupIdx int, groupRaw map[int][]byte) error {
			for _, blockIdx := range groups[groupIdx].BlockIDs {
				raw, ok := groupRaw[blockIdx]
				if !ok {
					continue
				}
				// NOTE: uses actual compressed wire bytes (len(raw)), not BlockMeta.Length
				// (uncompressed size). These differ for compressed blocks; len(raw) reflects
				// actual I/O volume. The pipeline's bytesRead return (BlockMeta.Length-based)
				// is discarded — this accumulation carries the wire-byte semantic.
				result.BytesRead += int64(len(raw)) //nolint:gosec
				// Release raw bytes immediately after use to allow GC before the next block.
				delete(groupRaw, blockIdx)

				meta := r.BlockMeta(blockIdx)
				// NOTE-018: two-pass decode — predicate columns first, output columns only
				// for blocks with matches — avoids decoding metric columns on rejected blocks.
				// When predicateCols is nil (no column predicates, e.g. {} | rate()), program.ColumnPredicate
				// is also nil; calling a nil func panics. Fast path: decode output columns in a single pass
				// and iterate all spans directly, skipping the ColumnPredicate call and allMatchSet allocation.
				if predicateCols == nil {
					bwb, parseErr := r.ParseBlockFromBytes(raw, modules_reader.WantOnly(outputCols), meta)
					if parseErr != nil {
						return fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
					}
					result.BlocksScanned++
					attrVals := make([]string, len(groupBy)) // NOTE-054
					// NOTE-159: one composite-key scratch buffer per block, reused across rows.
					scratch := acquireCompositeKeyScratch()
					// NOTE-160: resolve span:start/GroupBy/field columns once per block.
					cols := resolveBlockMetricsCols(bwb.Block, querySpec)
					for rowIdx := range int(meta.SpanCount) {
						traceAccumulateRow(r, blockIdx, bwb.Block, rowIdx, querySpec, buckets, attrVals, scratch, &cols)
					}
					releaseCompositeKeyScratch(scratch)
					// NOTE-153: block fully scanned (incl. any lazy span:start decode above);
					// return the lazy-column arena to the pool.
					bwb.Block.ReleaseLazyColumnStore()
					// NOTE-208: the assembled read buffer is no longer referenced (all lazy
					// column sub-slices were decoded above); recycle its backing array.
					r.ReleaseRawBuffer(raw)
					continue
				}

				// First pass: decode predicate columns only.
				bwb, parseErr := r.ParseBlockFromBytes(raw, modules_reader.WantOnly(predicateCols), meta)
				if parseErr != nil {
					return fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, parseErr)
				}
				// NOTE-153: the predicate-pass block's lazy-column arena is independent of the
				// output-pass block created below; release it once the predicate is evaluated.
				firstBlock := bwb.Block

				provider := acquireBlockColumnProvider(bwb.Block)
				rowSet, evalErr := program.ColumnPredicate(provider)
				if evalErr != nil {
					releaseBlockColumnProvider(provider)
					firstBlock.ReleaseLazyColumnStore()
					return fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
				}

				if rowSet.Size() == 0 {
					releaseBlockColumnProvider(provider)
					firstBlock.ReleaseLazyColumnStore()
					// NOTE-208: block rejected by the predicate; the buffer is dead (no
					// second pass, no row scan). Recycle its backing array.
					r.ReleaseRawBuffer(raw)
					continue
				}

				// Second pass: decode output columns for matched blocks only.
				bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, modules_reader.WantOnly(outputCols), meta)
				if parseErr != nil {
					releaseBlockColumnProvider(provider)
					firstBlock.ReleaseLazyColumnStore()
					return fmt.Errorf("ParseBlockFromBytes (second pass) block %d: %w", blockIdx, parseErr)
				}

				result.BlocksScanned++

				attrVals := make(
					[]string,
					len(groupBy),
				) // NOTE-054: per-block scratch; cleared at top of traceAccumulateRow
				// NOTE-159: one composite-key scratch buffer per block, reused across rows.
				scratch := acquireCompositeKeyScratch()
				// NOTE-160: resolve span:start/GroupBy/field columns once per block.
				cols := resolveBlockMetricsCols(bwb.Block, querySpec)
				for _, rowIdx := range rowSet.ToSlice() {
					traceAccumulateRow(r, blockIdx, bwb.Block, rowIdx, querySpec, buckets, attrVals, scratch, &cols)
				}
				releaseCompositeKeyScratch(scratch)
				releaseBlockColumnProvider(provider)
				// NOTE-153: both passes fully consumed — return their lazy-column arenas.
				firstBlock.ReleaseLazyColumnStore()
				bwb.Block.ReleaseLazyColumnStore()
				// NOTE-208: both passes parsed from the same `raw` buffer are fully
				// consumed; recycle its backing array.
				r.ReleaseRawBuffer(raw)
			}
			return nil
		},
	)
	if pipelineErr != nil {
		return nil, pipelineErr
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

// blockMetricsCols holds the per-block resolved column pointers for the metrics
// group-by hot path. NOTE-160: GetColumn is a string-keyed map lookup whose result is
// constant for every row in a block. The old traceAccumulateRow resolved span:start, each
// GroupBy attribute column, and the aggregate field column on EVERY span via GetColumn —
// O(rows × (2 + numGroupBy)) map lookups per block. Resolving them once per block and
// passing the pointers in collapses that to O(2 + numGroupBy) lookups per block, removing
// the repeated string hashing from the tight per-row loop (M4/M6/M8).
type blockMetricsCols struct {
	// tsCol is the span:start block column, or nil to fall back to the intrinsic section.
	tsCol *modules_reader.Column
	// fieldCol is the resolved aggregate field column (SUM/AVG/MIN/MAX/QUANTILE/STDDEV/
	// HISTOGRAM), or nil. COUNT/RATE have no field and leave this nil.
	fieldCol *modules_reader.Column
	// groupByCols[i] is the resolved column for querySpec.Aggregate.GroupBy[i], or nil.
	groupByCols []*modules_reader.Column
}

// resolveBlockMetricsCols resolves all per-block column pointers once for the row loop.
// NOTE-160: called once per block by the ExecuteTraceMetrics caller loops, replacing the
// per-row GetColumn lookups inside traceAccumulateRow.
func resolveBlockMetricsCols(block *modules_reader.Block, querySpec *vm.QuerySpec) blockMetricsCols {
	var c blockMetricsCols

	// NOTE-436: span:start is a regular per-row block column.
	c.tsCol = block.GetColumn("span:start")

	groupBy := querySpec.Aggregate.GroupBy
	if len(groupBy) > 0 {
		c.groupByCols = make([]*modules_reader.Column, len(groupBy))
		for i, attr := range groupBy {
			c.groupByCols[i] = block.GetColumn(attr)
		}
	}

	// Aggregate field column (used by traceUpdateBucket/traceHistogramBucket). COUNT/RATE
	// have an empty Field, so this stays nil and the per-row value path is skipped.
	if querySpec.Aggregate.Field != "" {
		c.fieldCol = block.GetColumn(querySpec.Aggregate.Field)
	}

	return c
}

// traceAccumulateRow accumulates one span's contribution into the buckets map.
// attrVals is a scratch slice of len(querySpec.Aggregate.GroupBy) reused across calls.
// NOTE-159: scratch is a per-block composite-key buffer owned by the caller. It is
// acquired once per block from compositeKeyScratchPool (not per row) and reused across
// every row in the block, then released after the block's row loop. This removes one
// sync.Pool Get + one Put + one defer from the per-span hot path (M4/M6/M8). The buffer
// is always truncated to [:0] before each key build below, so no stale bytes leak between
// rows.
// NOTE-160: cols holds the per-block resolved column pointers (span:start, GroupBy, field),
// resolved once by resolveBlockMetricsCols — no GetColumn map lookups occur per row here.
func traceAccumulateRow(
	r *modules_reader.Reader,
	blockIdx int,
	block *modules_reader.Block,
	rowIdx int,
	querySpec *vm.QuerySpec,
	buckets map[string]*aggBucketState,
	attrVals []string,
	scratch *[]byte,
	cols *blockMetricsCols,
) {
	// NOTE-054: clear attrVals at function entry so stale values from prior rows never
	// survive an early return. NOTE-067: scratch buffer appends attrVals in-place; clear
	// is still required to prevent stale values appearing after an early return.
	clear(attrVals)

	tb := querySpec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return
	}

	// Read span:start for time bucketing (SPEC-ETM-7). NOTE-160: column resolved per-block.
	// NOTE-436: span:start is a regular per-row block column.
	if cols.tsCol == nil {
		return
	}
	tsVal, ok := cols.tsCol.Uint64Value(rowIdx)
	if !ok {
		return
	}
	tsNanos := int64(tsVal) //nolint:gosec

	// Skip spans outside the query time window (SPEC-ETM-6).
	// Intervals are right-closed: (StartTime, EndTime] — matches Tempo semantics.
	if tsNanos <= tb.StartTime || tsNanos > tb.EndTime {
		return
	}

	bucketIdx := timeBucketIndex(tsNanos, tb.StartTime, tb.StepSizeNanos)

	// Build group key from GroupBy attributes (SPEC-ETM-8). NOTE-160: columns resolved per-block.
	for i, col := range cols.groupByCols {
		if col != nil {
			attrVals[i] = metricsColumnString(col, rowIdx)
		}
		// Missing column → empty string label (Tempo convention).
	}
	// NOTE-067: scratch buffer eliminates 3 per-span allocs on map-hit (Join + FormatInt + concat).
	// NOTE-159: scratch is now caller-owned (acquired once per block, not per row); reset to [:0].
	*scratch = strconv.AppendInt((*scratch)[:0], bucketIdx, 10)
	*scratch = append(*scratch, '\x00') // always emit separator (matches current " + "\x00" + attrGroupKey")
	for i, v := range attrVals {
		if i > 0 {
			*scratch = append(*scratch, '\x00')
		}
		*scratch = append(*scratch, v...)
	}
	// NOTE-033: HISTOGRAM embeds log2 bucket boundary as a 3rd "\x00" segment.
	// NOTE-160: field column resolved per-block (cols.fieldCol).
	if querySpec.Aggregate.Function == vm.FuncNameHISTOGRAM {
		histBoundary := traceHistogramBucket(cols.fieldCol, rowIdx, querySpec.Aggregate.Field)
		*scratch = append(*scratch, '\x00')
		*scratch = strconv.AppendFloat(*scratch, histBoundary, 'g', -1, 64)
	}

	// Zero-alloc map lookup on hit — Go compiler elides string(*scratch) conversion when key does not escape.
	bucket, exists := buckets[string(*scratch)]
	if !exists {
		key := string(*scratch) // NOTE-073: intentional alloc — key is retained in map
		bucket = &aggBucketState{
			min: math.MaxFloat64,
			max: -math.MaxFloat64,
		}
		buckets[key] = bucket
	}

	// NOTE-160: field column resolved per-block (cols.fieldCol).
	traceUpdateBucket(cols.fieldCol, rowIdx, querySpec.Aggregate, bucket)
}

// traceUpdateBucket accumulates one span's value into a bucket based on the aggregate function.
// NOTE-160: fieldCol is the per-block resolved aggregate field column (nil for COUNT/RATE).
func traceUpdateBucket(
	fieldCol *modules_reader.Column,
	rowIdx int,
	agg vm.AggregateSpec,
	bucket *aggBucketState,
) {
	switch agg.Function {
	case vm.FuncNameCOUNT, vm.FuncNameRATE:
		bucket.count++
	case vm.FuncNameSUM, vm.FuncNameAVG:
		v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
		if ok {
			bucket.count++
			bucket.sum += v
		}
	case vm.FuncNameMIN:
		v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
		if ok {
			// count tracks rows with a valid numeric field (not update count);
			// traceRowValue uses count==0 as the NaN sentinel for "no data in bucket".
			bucket.count++
			if v < bucket.min {
				bucket.min = v
			}
		}
	case vm.FuncNameMAX:
		v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
		if ok {
			// count tracks rows with a valid numeric field (not update count);
			// traceRowValue uses count==0 as the NaN sentinel for "no data in bucket".
			bucket.count++
			if v > bucket.max {
				bucket.max = v
			}
		}
	case vm.FuncNameQUANTILE:
		v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
		if ok {
			bucket.count++
			bucket.values = append(bucket.values, v)
		}
	case vm.FuncNameSTDDEV:
		v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
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
// NOTE-160: fieldCol is the per-block resolved field column; fieldName is retained only
// for the span:duration ns→s special case.
func traceHistogramBucket(fieldCol *modules_reader.Column, rowIdx int, fieldName string) float64 {
	v, ok := traceFieldFloat64Col(fieldCol, rowIdx)
	if !ok || v <= 0 {
		return 0
	}
	if fieldName == colNameSpanDuration {
		vSec := v / 1e9
		if vSec <= 0 {
			return 0
		}
		// NOTE-181: branch-free power-of-2 floor, see pow2Floor.
		return pow2Floor(vSec)
	}
	return pow2Floor(math.Abs(v))
}

// traceFieldFloat64Col reads a numeric field value from a pre-resolved span column.
// Returns (0, false) when the column is absent, not present for this row, or non-numeric.
// SPEC-ETM-8: Non-numeric columns (string, bool, bytes) are silently skipped.
// NOTE-160: takes a resolved *Column (resolved once per block) instead of a block + name,
// removing the per-row GetColumn map lookup from the aggregate value path.
func traceFieldFloat64Col(col *modules_reader.Column, rowIdx int) (float64, bool) {
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

	stepSec := float64(querySpec.TimeBucketing.StepSizeNanos) / 1e9
	groupBy := querySpec.Aggregate.GroupBy
	funcName := querySpec.Aggregate.Function
	quantile := querySpec.Aggregate.Quantile

	// NOTE-246: single-pass scatter. The composite map key is "bucketIdx\x00attrGroupKey",
	// so iterate the populated buckets exactly once and scatter each cell's computed value
	// directly into its series' dense values slice — keyed by attrGroupKey. This replaces the
	// former two-phase build (collectGroupKeys to enumerate distinct attrGroupKeys, then a
	// numSeries×numBuckets loop that rebuilt each composite key and probed the map). For a
	// sparse grid that loop performed numSeries×numBuckets map lookups + key rebuilds, the
	// vast majority returning nil; the scatter touches only len(buckets) populated cells.
	//
	// Unpopulated cells must carry the "empty" value: 0 for COUNT/RATE, NaN for the others
	// (SPEC-ETM-2). traceRowValue(nil, ...) yields exactly that, so each freshly allocated
	// series slice is pre-filled with it and only populated cells overwrite.
	emptyVal := traceRowValue(nil, funcName, stepSec, quantile)
	seriesVals := make(map[string][]float64, len(buckets))
	for compositeKey, bucket := range buckets {
		sep := strings.IndexByte(compositeKey, '\x00')
		if sep < 0 {
			continue
		}
		bucketIdx, err := strconv.ParseInt(compositeKey[:sep], 10, 64)
		if err != nil || bucketIdx < 0 || bucketIdx >= numBuckets {
			continue
		}
		attrGroupKey := compositeKey[sep+1:]
		values, ok := seriesVals[attrGroupKey]
		if !ok {
			values = make([]float64, numBuckets)
			if emptyVal != 0 {
				for i := range values {
					values[i] = emptyVal
				}
			}
			seriesVals[attrGroupKey] = values
		}
		values[bucketIdx] = traceRowValue(bucket, funcName, stepSec, quantile)
	}
	if len(seriesVals) == 0 {
		return nil
	}

	series := make([]TraceTimeSeries, 0, len(seriesVals))
	for attrGroupKey, values := range seriesVals {
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
				labels = append(labels, TraceMetricLabel{Name: intrinsicLabelName(name), Value: val})
			}
		}
		series = append(series, TraceTimeSeries{Labels: labels, Values: values})
	}

	// Sort for deterministic output (SPEC-ETM-11).
	sortSeriesByLabelString(series)

	return series
}

// traceRowValue computes the final metric value for a single bucket.
// SPEC-ETM-2: COUNT/RATE return 0 for nil/empty buckets; others return NaN.
// NOTE-033: QUANTILE uses computeQuantile (nearest-rank); STDDEV uses Welford sample variance.
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
			// Nearest-rank method. NOTE-033.
			return computeQuantile(bucket.values, quantile)
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
// Precondition: ts > startTime (caller must range-check before calling; all callers
// use binary-search lo/hi to guarantee this invariant — see NOTE-118).
// Intervals: (startTime, startTime+step], (startTime+step, startTime+2*step], …
// A span at exactly startTime+N*step belongs to bucket N-1 (previous bucket).
//
// NOTE-118: (offset-1)/stepNanos is algebraically equivalent to the original two-division
// formula (offset/stepNanos with bkt-- when offset%stepNanos==0) for all offset>0.
// Uses one DIVQ instead of two: ~2x fewer division cycles for hot loops (M1/M3 150M calls).
func timeBucketIndex(ts, startTime, stepNanos int64) int64 {
	return (ts - startTime - 1) / stepNanos
}

// intrinsicLabelName maps blockpack's internal column names to their Tempo-compatible
// short label names. For example, "span:kind" → "kind", "span:status" → "status".
// User attribute columns (e.g. "span.http.method") and resource.service.name are
// returned unchanged.
// NOTE-083: label name normalization at emit time — no data migration needed.
func intrinsicLabelName(colName string) string {
	switch colName {
	case colNameSpanKind:
		return "kind"
	case colNameSpanStatus:
		return "status"
	case colNameSpanName:
		return "name"
	case colNameSpanDuration:
		return "duration"
	case colNameSpanStart:
		return "start"
	case colNameStatusMessage:
		return "statusMessage"
	default:
		return colName
	}
}

// traceLabelString builds a deterministic string key from a label slice for sorting.
func traceLabelString(labels []TraceMetricLabel) string {
	if len(labels) == 0 {
		return ""
	}
	// NOTE-281: build the key in a single pre-sized allocation. The former
	// make([]string, n) + per-element "name=value" concat + strings.Join allocated
	// n+2 strings per call (one transient per label plus the parts slice and the
	// joined result). Sizing one byte buffer up front and writing name/value/separators
	// into it directly yields exactly one allocation (the final string), turning the
	// per-series key build from O(n) allocations into O(1).
	total := 0
	for i := range labels {
		total += len(labels[i].Name) + len(labels[i].Value) + 2 // '=' and ',' (extra ',' counted; trimmed below)
	}
	if total > 0 {
		total-- // last label has no trailing ','
	}
	var b strings.Builder
	b.Grow(total)
	for i := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(labels[i].Name)
		b.WriteByte('=')
		b.WriteString(labels[i].Value)
	}
	return b.String()
}

// sortSeriesByLabelString sorts series in place by their deterministic label-string key
// (SPEC-ETM-11). NOTE-281: it computes traceLabelString exactly once per series (a
// decorate-sort) instead of inside the comparator, which the previous SortFunc call did —
// re-building each series' key on every one of the O(n log n) comparisons. For a query with
// many output series (e.g. rate() by a high-cardinality attribute) that was ~2·n·log n key
// builds, each allocating the joined string; the decorate-sort drops it to n by pairing each
// series with its precomputed key, sorting the pairs, and writing the reordered series back.
func sortSeriesByLabelString(series []TraceTimeSeries) {
	if len(series) < 2 {
		return
	}
	// Decorate with the precomputed key in a single backing slice (one allocation), sort the
	// pairs, then write the reordered series back. This computes traceLabelString once per
	// series while adding only one transient slice regardless of series count.
	type keyedSeries struct {
		key string
		s   TraceTimeSeries
	}
	decorated := make([]keyedSeries, len(series))
	for i := range series {
		decorated[i] = keyedSeries{key: traceLabelString(series[i].Labels), s: series[i]}
	}
	slices.SortFunc(decorated, func(a, b keyedSeries) int {
		return cmp.Compare(a.key, b.key)
	})
	for i := range decorated {
		series[i] = decorated[i].s
	}
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

	// NOTE-246: single-pass scatter (companion to traceBuildDenseSeries). The histogram
	// composite key is "bucketIdx\x00attrGroupKey\x00histBoundary"; iterate the populated
	// buckets exactly once and scatter each count directly into its series' dense values
	// slice — keyed by (attrGroupKey, bucketBoundary). This replaces the former two-phase
	// build (enumerate distinct (attrGroupKey, boundary) pairs, then a numSeries×numBuckets
	// loop that rebuilt each composite key and probed the map). For M8 (1987 histogram
	// series × numBuckets steps) that probe loop ran numSeries×numBuckets map lookups + key
	// rebuilds, nearly all returning nil; the scatter touches only len(buckets) populated
	// cells. Histogram values use COUNT semantics: unpopulated cells are 0, which is the
	// zero value of a freshly allocated slice, so no pre-fill is needed.
	//
	// attrGroupKey may itself contain "\x00" separating multiple GroupBy values, so split on
	// the FIRST "\x00" (strips bucketIdx) and the LAST "\x00" (separates histBoundary from
	// attrGroupKey).
	seriesVals := make(map[histSeriesKey][]float64, len(buckets))
	for compositeKey, bucket := range buckets {
		if bucket == nil {
			continue
		}
		firstSep := strings.IndexByte(compositeKey, '\x00')
		if firstSep < 0 {
			continue
		}
		bucketIdx, err := strconv.ParseInt(compositeKey[:firstSep], 10, 64)
		if err != nil || bucketIdx < 0 || bucketIdx >= numBuckets {
			continue
		}
		rest := compositeKey[firstSep+1:]
		lastSep := strings.LastIndexByte(rest, '\x00')
		if lastSep < 0 {
			continue
		}
		sk := histSeriesKey{attrGroupKey: rest[:lastSep], bucketBoundary: rest[lastSep+1:]}
		values, ok := seriesVals[sk]
		if !ok {
			values = make([]float64, numBuckets)
			seriesVals[sk] = values
		}
		values[bucketIdx] = float64(bucket.count)
	}
	if len(seriesVals) == 0 {
		return nil
	}

	groupBy := querySpec.Aggregate.GroupBy
	series := make([]TraceTimeSeries, 0, len(seriesVals))
	for sk, values := range seriesVals {
		var labels []TraceMetricLabel
		if len(groupBy) > 0 {
			attrVals := strings.Split(sk.attrGroupKey, "\x00")
			labels = make([]TraceMetricLabel, 0, len(groupBy)+1)
			for i, name := range groupBy {
				val := ""
				if i < len(attrVals) {
					val = attrVals[i]
				}
				labels = append(labels, TraceMetricLabel{Name: intrinsicLabelName(name), Value: val})
			}
		}
		labels = append(labels, TraceMetricLabel{Name: "__bucket", Value: sk.bucketBoundary})
		series = append(series, TraceTimeSeries{Labels: labels, Values: values})
	}

	sortSeriesByLabelString(series)
	return series
}

// computeQuantile returns the q-quantile of values using the nearest-rank method.
// q is clamped to [0,1]; an empty slice yields 0.
func computeQuantile(values []float64, q float64) float64 {
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

// ValueIndexSource provides value-index data for a specific column query.
// Implementations typically wrap a list of pre-downloaded VI file bytes.
type ValueIndexSource interface {
	// LookupResults returns all matching LookupResults for the given column,
	// predicate, and optional time range. The bool is false when no VI data is
	// available for this column (caller must fall back to a block scan).
	LookupResults(colName string, colType modules_shared.ColumnType) ([]VILookupResult, bool)

	// AllResults returns every indexed span across all columns, deduplicated by
	// (TraceID, SpanID). It backs match-all queries ({} | rate()) where there is
	// no leaf condition to look up. The bool is false when the source cannot
	// enumerate all spans (caller must fall back to a block scan).
	AllResults() ([]VILookupResult, bool)
}

// VILookupResult is a matching entry from a value-index lookup.
// Mirrors valueindex.LookupResult but avoids a cross-package import in the executor.
//
// BlockID and RowIdx locate the span within a blockpack data file for the
// zero-scan search path (NOTE-VI-035, issue #459): BlockID is the block index
// within SourceRef and RowIdx is the row within that block, giving O(1) direct
// access without scanning. They are zero for VI files that predate per-row
// addressing; the search path treats a zero RowIdx as "row 0" and relies on
// the caller to gate on coverage. The metrics path (#460) ignores both.
type VILookupResult struct {
	SourceRef string
	TimeSec   uint64
	BlockID   uint32
	RowIdx    uint16
	TraceID   [16]byte
	SpanID    [8]byte
}

// ExecuteTraceMetricsFromVI runs a count_over_time() or rate() query using only
// value-index data — zero blockpack file reads required (NOTE-VI-032, issue #440).
//
// Returns (result, true, nil) when the query can be fully answered from VI data.
// Returns (nil, false, nil) when VI data is insufficient (caller must fall back to
// full block scan via ExecuteTraceMetrics).
//
// Currently supports: count_over_time() and rate() without group-by clauses.
//
// NOTE-VI-033 (issue #460): the value index carries (TraceID, SpanID, TimeSec) per
// indexed span per column. count_over_time()/rate() without group-by need nothing
// from the block payloads — counting distinct TraceIDs per time bucket is fully
// answerable from index TimeSec alone. This is the zero-block-read metrics path.
func ExecuteTraceMetricsFromVI(
	ctx context.Context,
	source ValueIndexSource,
	prog *vm.Program,
	spec vm.QuerySpec,
) (*TraceMetricsResult, bool, error) {
	if source == nil || prog == nil {
		return nil, false, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	// Gate: only count_over_time() and rate() without group-by are supported.
	switch spec.Aggregate.Function {
	case vm.FuncNameCOUNT, vm.FuncNameRATE:
	default:
		return nil, false, nil
	}
	if len(spec.Aggregate.GroupBy) > 0 {
		return nil, false, nil
	}

	tb := spec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		return nil, false, nil
	}

	// Collect the matching spans for the filter from the value index.
	matches, ok := viMatchSpans(source, prog)
	if !ok {
		return nil, false, nil
	}

	numBuckets := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numBuckets <= 0 {
		return &TraceMetricsResult{}, true, nil
	}

	// Count distinct TraceIDs per bucket. A TraceID may appear in multiple spans;
	// count_over_time()/rate() over the trace-level series counts each distinct
	// TraceID once per bucket (matches the block-scan accumulator, which keys the
	// dense series by bucket only when there is no group-by).
	seenPerBucket := make(map[int64]map[[16]byte]struct{}, numBuckets)
	for _, m := range matches {
		if m.TimeSec == 0 {
			// TimeSec == 0 means the file predates per-span timestamps; its time
			// bucket is unknown, so this span cannot be safely placed. Fall back to
			// the block scan rather than silently dropping spans.
			return nil, false, nil
		}
		tsNanos := int64(m.TimeSec) * 1_000_000_000 //nolint:gosec
		// Right-closed intervals (StartTime, EndTime] — matches the scan path.
		if tsNanos <= tb.StartTime || tsNanos > tb.EndTime {
			continue
		}
		bucketIdx := timeBucketIndex(tsNanos, tb.StartTime, tb.StepSizeNanos)
		if bucketIdx < 0 || bucketIdx >= numBuckets {
			continue
		}
		traces, exists := seenPerBucket[bucketIdx]
		if !exists {
			traces = make(map[[16]byte]struct{})
			seenPerBucket[bucketIdx] = traces
		}
		traces[m.TraceID] = struct{}{}
	}

	values := make([]float64, numBuckets)
	stepSec := float64(tb.StepSizeNanos) / 1e9
	for bucketIdx, traces := range seenPerBucket {
		count := float64(len(traces))
		if spec.Aggregate.Function == vm.FuncNameRATE && stepSec > 0 {
			count /= stepSec
		}
		values[bucketIdx] = count
	}

	result := &TraceMetricsResult{
		Series: []TraceTimeSeries{{Values: values}},
	}
	return result, true, nil
}

// viMatchSpans walks the program's predicate tree and resolves the matching spans
// from the value index. It returns (spans, true) when every leaf has index coverage,
// or (nil, false) when any leaf is unindexed (caller falls back to a block scan).
//
// Match-all queries ({}) have no leaf conditions; they enumerate every indexed span
// via source.AllResults.
func viMatchSpans(source ValueIndexSource, prog *vm.Program) ([]VILookupResult, bool) {
	preds := prog.Predicates
	if preds == nil || (len(preds.Nodes) == 0 && len(preds.Columns) == 0) {
		// Match-all: count every indexed span.
		return source.AllResults()
	}
	if len(preds.Nodes) == 0 {
		// Columns referenced but no evaluable nodes — cannot resolve from the index.
		return nil, false
	}
	return viEvalNodes(source, preds.Nodes, false)
}

// viEvalNodes evaluates a slice of sibling RangeNodes combined by AND, returning the
// span set that satisfies all of them. When isOR is true the siblings are combined by
// OR (union) instead. Returns (nil, false) if any leaf lacks index coverage.
//
// NOTE-VI-040 (#430): the accumulator is kept sorted by the 24-byte span key
// (TraceID++SpanID) so AND intersection and OR union are streaming merge-joins
// over two sorted runs rather than hash-map materialisations. The first set is
// sorted+deduplicated once; every subsequent merge-join output is itself sorted,
// so the accumulator stays sorted across the whole tree walk with no per-node map
// allocation. This bounds intersect/union to O(n+m) time and O(1) extra space
// beyond the output, matching the spec's "merge-join on sorted span ID sets to
// avoid materializing large sets in memory".
func viEvalNodes(source ValueIndexSource, nodes []vm.RangeNode, isOR bool) ([]VILookupResult, bool) {
	var acc []VILookupResult
	first := true
	for i := range nodes {
		set, ok := viEvalNode(source, &nodes[i])
		if !ok {
			return nil, false
		}
		switch {
		case first:
			acc = viSortDedup(set)
			first = false
		case isOR:
			acc = viUnionSorted(acc, viSortDedup(set))
		default:
			acc = viIntersectSorted(acc, viSortDedup(set))
		}
	}
	if first {
		return nil, false
	}
	return acc, true
}

// viEvalNode evaluates a single node. A node with children is an internal AND/OR
// combiner; a leaf node carries a Column and is resolved via the value index.
func viEvalNode(source ValueIndexSource, node *vm.RangeNode) ([]VILookupResult, bool) {
	if len(node.Children) > 0 {
		return viEvalNodes(source, node.Children, node.IsOR)
	}
	if node.Column == "" {
		return nil, false
	}
	// The SliceValueIndexSource already holds predicate-matched results per column
	// (QueryFiles applied the leaf predicate at download time), so the executor only
	// needs the column identity here. colType is resolved by the source from the
	// stored index entries.
	return source.LookupResults(node.Column, modules_shared.ColumnTypeString)
}

// viSortDedup returns set sorted ascending by the 24-byte span key
// (TraceID++SpanID) with consecutive duplicate keys collapsed to their first
// occurrence. It sorts in place (set is owned by the caller's per-column slice,
// which the boolean walk does not reuse afterwards) and returns the deduplicated
// prefix. This is the precondition for the merge-join intersect/union below.
func viSortDedup(set []VILookupResult) []VILookupResult {
	if len(set) < 2 {
		return set
	}
	slices.SortFunc(set, viSpanCmp)
	out := set[:1]
	last := viSpanKey(set[0])
	for _, s := range set[1:] {
		k := viSpanKey(s)
		if k == last {
			continue
		}
		last = k
		out = append(out, s)
	}
	return out
}

// viUnionSorted returns the union of two key-sorted, key-deduplicated span sets
// as a streaming merge-join. Both inputs MUST already be sorted ascending by the
// 24-byte span key (viSortDedup); the output is sorted the same way.
func viUnionSorted(a, b []VILookupResult) []VILookupResult {
	out := make([]VILookupResult, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		ka, kb := viSpanKey(a[i]), viSpanKey(b[j])
		switch bytes.Compare(ka[:], kb[:]) {
		case 0:
			out = append(out, a[i])
			i++
			j++
		case -1:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// viIntersectSorted returns the intersection of two key-sorted, key-deduplicated
// span sets as a streaming merge-join. Both inputs MUST already be sorted
// ascending by the 24-byte span key (viSortDedup); the output is sorted the same
// way. Uses O(1) extra space beyond the output, no hash map.
func viIntersectSorted(a, b []VILookupResult) []VILookupResult {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := make([]VILookupResult, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		ka, kb := viSpanKey(a[i]), viSpanKey(b[j])
		switch bytes.Compare(ka[:], kb[:]) {
		case 0:
			out = append(out, a[i])
			i++
			j++
		case -1:
			i++
		default:
			j++
		}
	}
	return out
}

// viSpanKey is the dedup/intersect key: TraceID (16) ++ SpanID (8).
func viSpanKey(s VILookupResult) [24]byte {
	var k [24]byte
	copy(k[:16], s.TraceID[:])
	copy(k[16:], s.SpanID[:])
	return k
}

// viSpanCmp orders two results ascending by their 24-byte span key.
func viSpanCmp(x, y VILookupResult) int {
	kx, ky := viSpanKey(x), viSpanKey(y)
	return bytes.Compare(kx[:], ky[:])
}

// ValueIndexBuildStats records the I/O the builder performed assembling a
// SliceValueIndexSource: how many value-index files were downloaded, their total
// byte size, and how many span entries were matched. The builder populates these
// so the querier can attach them to its OTel span / log line (tempo issue #465).
// All counters are advisory observability — they never affect query results.
type ValueIndexBuildStats struct {
	// FilesRead is the number of value-index files downloaded from object storage.
	FilesRead int
	// BytesRead is the total byte size of those downloaded files.
	BytesRead int64
	// Hits is the number of span entries the per-column predicates matched (the
	// raw lookup-result count before AND/OR span-ID intersection).
	Hits int
}

// SliceValueIndexSource implements ValueIndexSource from pre-downloaded value-index
// results, grouped by column name and type. Callers populate it from
// valueindex.QueryFiles output (one leaf predicate already applied per column).
type SliceValueIndexSource struct {
	// data maps colName → colType → matched results for that column.
	data map[string]map[modules_shared.ColumnType][]VILookupResult
	// stats records the build-time I/O so the querier can report it (issue #465).
	stats ValueIndexBuildStats
}

// NewSliceValueIndexSource builds an empty source. Use Add to populate per-column
// results.
func NewSliceValueIndexSource() *SliceValueIndexSource {
	return &SliceValueIndexSource{
		data: make(map[string]map[modules_shared.ColumnType][]VILookupResult),
	}
}

// RecordFileIO accumulates the builder's per-column download I/O into the source's
// stats (issue #465). filesRead is the file count for the column, bytesRead their
// total size. Safe to call repeatedly; counters accumulate.
func (s *SliceValueIndexSource) RecordFileIO(filesRead int, bytesRead int64) {
	s.stats.FilesRead += filesRead
	s.stats.BytesRead += bytesRead
}

// Stats returns the build-time I/O counters. Hits is derived from the stored
// results so it stays correct regardless of Add ordering.
func (s *SliceValueIndexSource) Stats() ValueIndexBuildStats {
	out := s.stats
	for _, byType := range s.data {
		for _, results := range byType {
			out.Hits += len(results)
		}
	}
	return out
}

// Add records the matched results for one (colName, colType). Repeated calls for the
// same key append.
func (s *SliceValueIndexSource) Add(colName string, colType modules_shared.ColumnType, results []VILookupResult) {
	byType, ok := s.data[colName]
	if !ok {
		byType = make(map[modules_shared.ColumnType][]VILookupResult)
		s.data[colName] = byType
	}
	byType[colType] = append(byType[colType], results...)
}

// LookupResults returns the matched results for colName. The colType argument is
// advisory: the source returns results across all stored types for the column (the
// leaf predicate that produced them already constrained the type), so an unindexed
// column yields (nil, false) and an indexed one yields (results, true).
func (s *SliceValueIndexSource) LookupResults(colName string, _ modules_shared.ColumnType) ([]VILookupResult, bool) {
	byType, ok := s.data[colName]
	if !ok {
		return nil, false
	}
	var out []VILookupResult
	for _, results := range byType {
		out = append(out, results...)
	}
	return out, true
}

// AllResults returns every stored span across all columns, deduplicated by
// (TraceID, SpanID). It returns (nil, false) when the source holds no data, so a
// match-all query falls back to a block scan rather than reporting an empty result.
func (s *SliceValueIndexSource) AllResults() ([]VILookupResult, bool) {
	if len(s.data) == 0 {
		return nil, false
	}
	var all []VILookupResult
	seen := make(map[[24]byte]struct{})
	for _, byType := range s.data {
		for _, results := range byType {
			for _, r := range results {
				k := viSpanKey(r)
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				all = append(all, r)
			}
		}
	}
	return all, true
}
