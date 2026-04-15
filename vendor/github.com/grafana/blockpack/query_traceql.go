package blockpack

// query_traceql.go — TraceQL streaming and pipeline execution internals.
// Public entry point is QueryTraceQL in api.go; this file holds the
// implementation helpers it delegates to.

import (
	"fmt"
	"log/slog"
	"math"
	"strings"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// streamFilterQuery executes a TraceQL filter query against a modules-format reader.
func streamFilterQuery(
	r *Reader,
	filterExpr *traceqlparser.FilterExpression,
	opts QueryOptions,
	fn spanMatchFn,
) (QueryStats, error) {
	var program *vm.Program
	var compileErr error
	if opts.Embedder != nil {
		compileOpts := vm.CompileOptions{
			Embedder: opts.Embedder,
			Limit:    opts.Limit,
		}
		program, compileErr = vm.CompileTraceQLFilterWithOptions(filterExpr, compileOpts)
	} else {
		program, compileErr = vm.CompileTraceQLFilter(filterExpr)
	}
	if compileErr != nil {
		return QueryStats{}, fmt.Errorf("compile TraceQL filter: %w", compileErr)
	}

	return streamFilterProgram(r, program, opts, fn)
}

// streamFilterProgram executes a compiled filter program against a modules-format reader.
func streamFilterProgram(r *Reader, program *vm.Program, opts QueryOptions, fn spanMatchFn) (QueryStats, error) {
	// SPEC-STREAM-8: MostRecent maps to Backward direction with span:start timestamp sorting.
	// span:start is in searchMetaColumns for V14 files (see NOTE-013); additional I/O may be
	// needed for older formats.
	// When MostRecent+Limit, Collect gives globally top-K results by span:start.
	collectOpts := modules_executor.CollectOptions{
		TimeRange:     normalizeTimeRange(opts.StartNano, opts.EndNano),
		Limit:         opts.Limit,
		StartBlock:    opts.StartBlock,
		BlockCount:    opts.BlockCount,
		SelectColumns: opts.SelectColumns,
		// NOTE-028: AllColumns defaults to false — for Collect this means the second pass decodes only
		// searchMetaColumns ∪ predicate columns.
	}
	if opts.MostRecent {
		collectOpts.Direction = modules_queryplanner.Backward
		collectOpts.TimestampColumn = "span:start"
	}
	rows, stats, err := modules_executor.Collect(r, program, collectOpts)
	if err != nil {
		return stats, err
	}
	// Intrinsic ID maps are built lazily: only constructed on the first block-scan result
	// row where the block columns lack trace:id / span:id. For dual-storage files (PR #174+)
	// these columns are present in block payloads and the maps are never needed.
	var traceIDByRef map[uint32][]byte
	var spanIDByRef map[uint32][]byte
	// Separate "attempted" flags prevent repeated calls to buildIntrinsicBytesMap when
	// it returns nil (e.g. no intrinsic section). Without these flags, every row would
	// re-invoke the expensive O(N) map-build path and still get nil back.
	var traceIDMapAttempted bool
	var spanIDMapAttempted bool
	buildIDMaps := func() {
		if !traceIDMapAttempted {
			traceIDMapAttempted = true
			traceIDByRef = buildIntrinsicBytesMap(r, "trace:id")
		}
		if !spanIDMapAttempted {
			spanIDMapAttempted = true
			spanIDByRef = buildIntrinsicBytesMap(r, "span:id")
		}
	}
	for _, row := range rows {
		var fields SpanFieldsProvider
		var traceIDHex, spanIDHex string
		var rawAdapter SpanFieldsProvider // tracked for pool release
		if row.IntrinsicFields != nil {
			// Intrinsic fast path — fields resolved from intrinsic columns, no block read.
			fields = row.IntrinsicFields
			if v, ok := fields.GetField("trace:id"); ok {
				traceIDHex = hexEncodeField(v)
			}
			if v, ok := fields.GetField("span:id"); ok {
				spanIDHex = hexEncodeField(v)
			}
		} else {
			rawAdapter = modules_blockio.NewSpanFieldsAdapterWithReader(row.Block, r, row.BlockIdx, row.RowIdx)
			fields = rawAdapter
			// For dual-storage blocks, trace:id and span:id are in block columns — extractIDs
			// will find them there without using the fallback maps. Build maps lazily only on
			// the first miss, avoiding the expensive O(N) buildIntrinsicBytesMap for new files.
			if row.Block == nil ||
				row.Block.GetColumn("trace:id") == nil ||
				row.Block.GetColumn("span:id") == nil {
				buildIDMaps()
			}
			traceIDHex, spanIDHex = extractIDs(row.Block, row.RowIdx, row.BlockIdx, traceIDByRef, spanIDByRef)
		}
		if len(opts.SelectColumns) > 0 {
			fields = newFilteredSpanFields(fields, opts.SelectColumns)
		}
		match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
		if !fn(match, true) {
			// NOTE-ALLOC-4: release adapter back to pool after callback returns.
			modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
			break
		}
		// NOTE-ALLOC-4: release adapter back to pool after callback returns.
		modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
	}
	fn(nil, false)
	return stats, nil
}

// emitAllSpans sends every span in allSpans to fn, honoring the given limit
// (0 = unlimited). Calls fn(nil, false) as the terminal signal after the last span.
func emitAllSpans(allSpans []SpanMatch, limit int, fn spanMatchFn) {
	remaining := limit
	for i := range allSpans {
		if !fn(&allSpans[i], true) {
			break
		}
		if remaining > 0 {
			remaining--
			if remaining == 0 {
				break
			}
		}
	}
	fn(nil, false)
}

// SPEC-PA-4, SPEC-PA-5, SPEC-PA-6, SPEC-PA-7: streamPipelineQuery executes a TraceQL pipeline query
// ({filter} | aggregate() > threshold).
// It runs the filter part, groups matching spans into spansets by trace ID, applies the
// aggregate function per spanset, filters by threshold, and emits qualifying spans.
func streamPipelineQuery(r *Reader, mq *traceqlparser.MetricsQuery, opts QueryOptions, fn spanMatchFn) error {
	// Compile and execute the filter part to get all matching spans.
	program, compileErr := vm.CompileTraceQLFilter(mq.Filter)
	if compileErr != nil {
		return fmt.Errorf("compile pipeline filter: %w", compileErr)
	}

	// Run with no limit — we need all matching spans to compute aggregates.
	filterOpts := opts
	filterOpts.Limit = 0

	var allSpans []SpanMatch
	_, streamErr := streamFilterProgram(r, program, filterOpts, func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		allSpans = append(allSpans, match.Clone())
		return true
	})
	if streamErr != nil {
		return streamErr
	}

	const maxPipelineSpans = 1_000_000
	if len(allSpans) > maxPipelineSpans {
		return fmt.Errorf(
			"pipeline query matched %d spans (max %d); narrow your filter",
			len(allSpans),
			maxPipelineSpans,
		)
	}

	pipeline := mq.Pipeline

	if pipeline == nil {
		// No pipeline stages — emit all spans, respecting limit.
		emitAllSpans(allSpans, opts.Limit, fn)
		return nil
	}

	// If pipeline has no aggregate (just by/select), emit all spans, respecting limit.
	if pipeline.Aggregate.Name == "" {
		emitAllSpans(allSpans, opts.Limit, fn)
		return nil
	}

	// Group spans by trace ID into spansets.
	type spanset struct {
		spans []int // indices into allSpans
	}
	traceGroups := make(map[string]*spanset)
	traceOrder := make([]string, 0)
	for i, sp := range allSpans {
		tid := sp.TraceID
		if tid == "" {
			tid = "__no_trace_id__"
		}
		ss, ok := traceGroups[tid]
		if !ok {
			ss = &spanset{}
			traceGroups[tid] = ss
			traceOrder = append(traceOrder, tid)
		}
		ss.spans = append(ss.spans, i)
	}

	// Compute aggregate per spanset and filter by threshold.
	aggName := pipeline.Aggregate.Name
	aggField := pipeline.Aggregate.Field

	// Track limit globally across all traces, not per-trace.
	limit := opts.Limit
	for _, tid := range traceOrder {
		ss := traceGroups[tid]

		aggVal, ok := computeSpansetAggregate(aggName, aggField, ss.spans, allSpans)
		if !ok {
			continue
		}

		if pipeline.HasThreshold {
			if !compareThreshold(aggVal, pipeline.ThresholdOp, pipeline.ThresholdVal) {
				continue
			}
		}

		// Emit all spans from this qualifying spanset.
		for _, idx := range ss.spans {
			if !fn(&allSpans[idx], true) {
				fn(nil, false)
				return nil
			}
			if limit > 0 {
				limit--
				if limit == 0 {
					fn(nil, false)
					return nil
				}
			}
		}
	}

	fn(nil, false)
	return nil
}

// SPEC-PA-1, SPEC-PA-2, SPEC-PA-3: computeSpansetAggregate computes an aggregate value over a spanset.
func computeSpansetAggregate(aggName, aggField string, indices []int, allSpans []SpanMatch) (float64, bool) {
	switch aggName {
	case "count", "count_over_time":
		return float64(len(indices)), true

	case "avg", "min", "max", "sum":
		var values []float64
		for _, idx := range indices {
			v, ok := getSpanFieldNumeric(allSpans[idx], aggField)
			if ok {
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			return 0, false
		}
		switch aggName {
		case "avg":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum / float64(len(values)), true
		case "min":
			minVal := values[0]
			for _, v := range values[1:] {
				if v < minVal {
					minVal = v
				}
			}
			return minVal, true
		case "max":
			maxVal := values[0]
			for _, v := range values[1:] {
				if v > maxVal {
					maxVal = v
				}
			}
			return maxVal, true
		case "sum":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum, true
		}
	default:
		slog.Warn("unsupported aggregate in pipeline query", "aggregate", aggName)
	}
	return 0, false
}

// SPEC-PA-1: getSpanFieldNumeric extracts a numeric value from a span's fields.
// It matches fieldName exactly first, then falls back to unscoped/scoped variants
// (e.g. "duration" matches "span:duration", and "span.duration" matches "duration").
func getSpanFieldNumeric(sp SpanMatch, fieldName string) (float64, bool) {
	if sp.Fields == nil {
		return 0, false
	}
	var result float64
	var found bool
	sp.Fields.IterateFields(func(name string, value any) bool {
		if !fieldNameMatches(name, fieldName) {
			return true
		}
		switch v := value.(type) {
		case int64:
			result = float64(v)
			found = true
		case uint64:
			result = float64(v)
			found = true
		case float64:
			result = v
			found = true
		case int:
			result = float64(v)
			found = true
		}
		return false // stop iteration
	})
	return result, found
}

// fieldNameMatches returns true if emitted matches the requested field name,
// accounting for scope prefix differences (e.g. "span:duration" vs "duration",
// "span.http.method" vs "http.method").
func fieldNameMatches(emitted, requested string) bool {
	if emitted == requested {
		return true
	}
	// Try stripping "span:" or "span." prefix from emitted name.
	for _, prefix := range []string{"span:", "span."} {
		if strings.HasPrefix(emitted, prefix) {
			if emitted[len(prefix):] == requested {
				return true
			}
		}
		if strings.HasPrefix(requested, prefix) {
			if requested[len(prefix):] == emitted {
				return true
			}
		}
	}
	return false
}

// compareThreshold compares an aggregate value against a threshold.
func compareThreshold(aggVal float64, op traceqlparser.BinaryOp, threshold interface{}) bool {
	var thresholdF float64
	switch v := threshold.(type) {
	case int64:
		thresholdF = float64(v)
	case float64:
		thresholdF = v
	default:
		return false
	}

	switch op {
	case traceqlparser.OpGt:
		return aggVal > thresholdF
	case traceqlparser.OpGte:
		return aggVal >= thresholdF
	case traceqlparser.OpLt:
		return aggVal < thresholdF
	case traceqlparser.OpLte:
		return aggVal <= thresholdF
	case traceqlparser.OpEq:
		return math.Abs(aggVal-thresholdF) < 1e-9
	case traceqlparser.OpNeq:
		return math.Abs(aggVal-thresholdF) >= 1e-9
	default:
		return false
	}
}
