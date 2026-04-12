// Package blockpack provides a minimal public API for blockpack.
//
// AGENT: This public API must remain minimal and focused. Before adding any new
// public functions, types, or interfaces, you MUST ask the user for explicit
// permission. The design goal is to keep the API surface as small as possible
// and hide all implementation details in internal/ packages.
//
// The public API intentionally exposes ONLY:
//   - Query execution functions (TraceQL filter queries)
//   - Reader interface and basic types
//   - Provider interfaces for storage abstraction
//
// Everything else is internal implementation detail.
package blockpack

import (
	"encoding/hex"
	"fmt"
	"math"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// AGENT: Query execution - this is the main public API for querying.
// Keep this minimal - just TraceQL filter query function.

// SpanFieldsProvider gives access to all attributes for a single span row.
// Use GetField for known attribute names, IterateFields to enumerate all.
type SpanFieldsProvider = modules_shared.SpanFieldsProvider

// QueryStats contains per-phase execution metrics returned by all query functions.
// It replaces the LogQueryStats type and the OnStats callback pattern.
type QueryStats = modules_executor.QueryStats

// StepStats holds per-phase metrics for a single execution phase within a QueryStats.
type StepStats = modules_executor.StepStats

// QueryOptions configures query execution.
type QueryOptions struct {
	// Embedder enables VECTOR_AI() predicates in TraceQL filter queries. When non-nil,
	// a VECTOR_AI("query text") expression is embedded at compile time and matched against
	// spans by cosine similarity. If nil and the query contains VECTOR_AI(), QueryTraceQL
	// returns an error. Any type implementing vm.TextEmbedder is accepted — this keeps
	// blockpack decoupled from the concrete embedder implementation.
	// VECTOR_ALL() does not require an Embedder.
	Embedder vm.TextEmbedder
	// SelectColumns limits which column names appear in SpanMatch.Fields.
	// When non-empty, only columns whose names are present in this slice are
	// returned by GetField and IterateFields. nil or empty means all columns
	// are returned (no projection applied).
	SelectColumns []string
	// StartNano is the inclusive lower bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range ends before StartNano are skipped entirely.
	// 0 means no lower bound.
	StartNano uint64
	// EndNano is the inclusive upper bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range begins after EndNano are skipped entirely.
	// 0 means no upper bound.
	EndNano uint64
	Limit   int // Maximum number of spans to return (0 = unlimited).
	// StartBlock is the first internal block index to scan (0-based, inclusive).
	// Used by the frontend sharder to partition a single blockpack file into
	// multiple sub-file jobs. 0 means start from the first block.
	StartBlock int
	// BlockCount is the number of internal blocks to scan starting from StartBlock.
	// 0 means scan all blocks (no sub-file sharding).
	BlockCount int
	// MostRecent controls block traversal order. Always use keyed struct literals:
	// QueryOptions{Limit: 10, MostRecent: true}.
	// Uses Backward direction with span:start timestamp sorting. For intrinsic-only queries,
	// top-K selection uses only the intrinsic column blobs (no full block I/O).
	MostRecent bool
}

// validateQueryOptions checks that sharding and time range parameters are valid.
func validateQueryOptions(opts QueryOptions) error {
	if opts.StartBlock < 0 {
		return fmt.Errorf("invalid StartBlock %d: must be >= 0", opts.StartBlock)
	}
	if opts.BlockCount < 0 {
		return fmt.Errorf("invalid BlockCount %d: must be >= 0", opts.BlockCount)
	}
	if opts.BlockCount > 0 {
		end := opts.StartBlock + opts.BlockCount
		if end < opts.StartBlock { // overflow
			return fmt.Errorf("invalid shard range: StartBlock + BlockCount overflows int")
		}
	}
	if opts.StartBlock != 0 && opts.BlockCount == 0 {
		return fmt.Errorf(
			"invalid shard range: StartBlock=%d has no effect when BlockCount=0 (scan all)",
			opts.StartBlock,
		)
	}
	if opts.StartNano > 0 && opts.EndNano > 0 && opts.StartNano > opts.EndNano {
		return fmt.Errorf("invalid time range: StartNano (%d) > EndNano (%d)", opts.StartNano, opts.EndNano)
	}
	return nil
}

// normalizeTimeRange converts StartNano/EndNano into a TimeRange, treating
// EndNano==0 as "no upper bound" (math.MaxUint64) so the TS-index fast path
// does not incorrectly prune all blocks.
func normalizeTimeRange(startNano, endNano uint64) modules_queryplanner.TimeRange {
	if startNano > 0 && endNano == 0 {
		endNano = math.MaxUint64
	}
	return modules_queryplanner.TimeRange{MinNano: startNano, MaxNano: endNano}
}

// ColumnNames returns all column names known to this reader — the union of
// range-indexed and sketch columns. Derived from file header metadata; no
// block I/O is required. Returns a sorted slice of column name strings
// (e.g. "span:status", "resource.service.name", "span.http.method").
func ColumnNames(r *Reader) []string {
	return r.ColumnNames()
}

// QueryTraceQL executes a TraceQL query against a modules-format blockpack file
// and returns all matching spans along with per-phase execution statistics.
// QueryStats is populated for filter queries; structural and pipeline queries
// return an empty QueryStats.
//
// Supported query types:
//   - Filter expressions: `{ span.http.method = "GET" }`
//   - Structural queries: `{ expr } OP { expr }` where OP is >>, >, ~, <<, <, !~
//   - Pipeline queries: `{ filter } | aggregate() > threshold`
//
// For filter queries, each SpanMatch.Fields supports GetField and IterateFields.
// For structural queries, SpanMatch.Fields is nil — only TraceID and SpanID are set.
// Pipeline queries group matching spans into spansets, compute aggregates, and
// filter by threshold before returning qualifying spans.
func QueryTraceQL(
	r *Reader,
	traceqlQuery string,
	opts QueryOptions,
) (results []SpanMatch, stats QueryStats, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQL: reader cannot be nil")
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQL: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, QueryStats{}, fmt.Errorf("parse TraceQL: %w", parseErr)
	}

	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}

	switch q := parsed.(type) {
	case *traceqlparser.FilterExpression:
		stats, err = streamFilterQuery(r, q, opts, collector)
	case *traceqlparser.StructuralQuery:
		execOpts := modules_executor.Options{
			Limit:      opts.Limit,
			TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
			StartBlock: opts.StartBlock,
			BlockCount: opts.BlockCount,
		}
		var execResult *modules_executor.StructuralResult
		execResult, err = modules_executor.ExecuteStructural(r, q, execOpts)
		if err == nil {
			for i := range execResult.Matches {
				m := &execResult.Matches[i]
				match := &SpanMatch{
					TraceID: hex.EncodeToString(m.TraceID[:]),
					SpanID:  hex.EncodeToString(m.SpanID),
				}
				if !collector(match, true) {
					break
				}
			}
			collector(nil, false)
		}
	case *traceqlparser.MetricsQuery:
		err = streamPipelineQuery(r, q, opts, collector)
	default:
		err = fmt.Errorf(
			"QueryTraceQL: query type %T is not supported",
			parsed,
		)
	}
	return results, stats, err
}

// LogQueryOptions configures log query execution.
type LogQueryOptions struct {
	StartNano uint64 // Inclusive start (unix nanos); 0 = no lower bound.
	EndNano   uint64 // Inclusive end (unix nanos); 0 = no upper bound.
	Limit     int    // Max matches; 0 = unlimited.
	Forward   bool   // If true, traverse forward (oldest first). Default is backward (newest first).
}

// QueryLogQL executes a LogQL filter query against a blockpack log file
// and returns all matching log records.
//
// Supported syntax:
//   - Label matchers: {key="val", key=~"regex", key!="val", key!~"regex"}
//   - Line filters: |= "text" != "text" |~ "regex" !~ "regex"
//   - Pipeline stages: | json, | logfmt, | label_format, | line_format, | drop, | keep
//   - Label filter stages: | field = "val", | field > 42, etc.
//   - Unwrap: | unwrap field
//
// When no pipeline stages are present, queries are executed via the low-level
// path. When pipeline stages are present, the native LogQL engine is used.
//
// Direction defaults to backward, traversing blocks from newest to oldest.
//
// Field access via SpanMatch.Fields: for log queries (pipeline path), each
// SpanMatch exposes log:body, log:timestamp, resource.__loki_labels__, and
// log.* ColumnTypeString attributes. It does not expose individual resource.*
// label columns (e.g. GetField("service.name") returns ""). Callers needing
// the full resource label set should parse the resource.__loki_labels__ string,
// or use QueryTraceQL which uses SpanFieldsAdapter with lazy full-column access.
func QueryLogQL(r *Reader, logqlQuery string, opts LogQueryOptions) (results []SpanMatch, qs QueryStats, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryLogQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryLogQL: reader cannot be nil")
	}

	sel, parseErr := logqlparser.Parse(logqlQuery)
	if parseErr != nil {
		return nil, QueryStats{}, fmt.Errorf("parse LogQL: %w", parseErr)
	}

	program, pipeline, compileErr := logqlparser.CompileAll(sel)
	if compileErr != nil {
		return nil, QueryStats{}, fmt.Errorf("compile LogQL: %w", compileErr)
	}

	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}

	if pipeline != nil {
		qs, err = streamLogQLWithPipeline(r, program, pipeline, opts, collector)
	} else {
		qs, err = streamLogProgram(r, program, opts, collector)
	}
	return results, qs, err
}

// LogMetricsResult is the output of ExecuteMetricsLogQL.
type LogMetricsResult = modules_executor.LogMetricsResult

// LogMetricsRow is one row in a LogMetricsResult time-series grid.
type LogMetricsRow = modules_executor.LogMetricsRow

// LogMetricOptions configures a LogQL metrics query.
type LogMetricOptions struct {
	// GroupBy lists label names to group the time series by.
	GroupBy []string
	// StartNano is the inclusive start of the query time window (unix nanoseconds).
	StartNano int64
	// EndNano is the exclusive end of the query time window (unix nanoseconds).
	EndNano int64
	// StepNano is the time bucket step size in nanoseconds (default: 60 seconds).
	StepNano int64
}

// ExecuteMetricsLogQL executes a LogQL metric query against a blockpack log file and
// returns dense time-bucketed results.
//
// Supported metric functions: count_over_time, rate, bytes_over_time, bytes_rate,
// sum_over_time, avg_over_time, min_over_time, max_over_time.
//
// For unwrap-based functions (sum_over_time, avg_over_time, min_over_time,
// max_over_time), include a pipeline with | unwrap <field> to provide numeric values.
//
// Returns a LogMetricsResult with one row per (time-bucket × group-by-key) pair.
// GroupKey[0] is the 0-indexed bucket number; GroupKey[1..] are group-by label values.
func ExecuteMetricsLogQL(r *Reader, logqlQuery string, opts LogMetricOptions) (result *LogMetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsLogQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsLogQL: reader cannot be nil")
	}

	lq, parseErr := logqlparser.ParseQuery(logqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse LogQL metric query: %w", parseErr)
	}

	if lq.VectorAgg != nil {
		return nil, fmt.Errorf(
			"ExecuteMetricsLogQL: vector aggregation operators (topk, sum by, etc.) not yet implemented",
		)
	}

	var metric *logqlparser.MetricExpr
	switch {
	case lq.Metric != nil:
		metric = lq.Metric
	default:
		return nil, fmt.Errorf("ExecuteMetricsLogQL: query is not a metric expression")
	}

	// Reinsert the unwrap stage into the pipeline stages before compiling.
	// parseMetricExpr extracts unwrap into MetricExpr.Unwrap and removes it from
	// Pipeline; we must add it back so the pipeline can populate UnwrapValueKey.
	pipelineStages := metric.Pipeline
	if metric.Unwrap != "" {
		pipelineStages = append(pipelineStages, logqlparser.PipelineStage{
			Type:   logqlparser.StageUnwrap,
			Params: []string{metric.Unwrap},
		})
	}

	// Build a combined selector with the full pipeline for CompileAll pushdown.
	combinedSel := &logqlparser.LogSelector{
		Matchers:    metric.Selector.Matchers,
		LineFilters: metric.Selector.LineFilters,
		Pipeline:    pipelineStages,
	}
	program, pipeline, compileErr := logqlparser.CompileAll(combinedSel)
	if compileErr != nil {
		return nil, fmt.Errorf("compile LogQL: %w", compileErr)
	}

	stepNano := opts.StepNano
	if stepNano <= 0 {
		stepNano = 60 * 1_000_000_000 // default: 1 minute
	}

	querySpec := &vm.QuerySpec{
		TimeBucketing: vm.TimeBucketSpec{
			Enabled:       true,
			StartTime:     opts.StartNano,
			EndTime:       opts.EndNano,
			StepSizeNanos: stepNano,
		},
		Filter: vm.FilterSpec{IsMatchAll: true},
	}

	return modules_executor.ExecuteLogMetrics(r, program, pipeline, querySpec, metric.Function, opts.GroupBy)
}

// TraceMetricsResult is the output of ExecuteMetricsTraceQL.
type TraceMetricsResult = modules_executor.TraceMetricsResult

// TraceTimeSeries is one time series in a TraceMetricsResult.
type TraceTimeSeries = modules_executor.TraceTimeSeries

// TraceMetricLabel is one label key-value pair in a TraceTimeSeries.
type TraceMetricLabel = modules_executor.TraceMetricLabel

// TraceMetricOptions configures a TraceQL metrics query.
type TraceMetricOptions struct {
	// StartNano is the approximate start of the query time window (unix nanoseconds).
	// Internally aligned down to the nearest StepNano boundary before query execution.
	// The effective interval is right-closed: spans at exactly alignedStart are excluded.
	StartNano int64
	// EndNano is the approximate end of the query time window (unix nanoseconds).
	// Internally aligned up to the nearest StepNano boundary before query execution.
	// The effective interval is right-closed: spans at exactly alignedEnd are included.
	EndNano int64
	// StepNano is the time bucket step size in nanoseconds (default: 60 seconds).
	StepNano int64
}

// ExecuteMetricsTraceQL executes a TraceQL metrics query against a blockpack trace file
// and returns dense time-bucketed results.
//
// Supported metric functions: count_over_time(), rate(), sum(field), avg(field), min(field),
// max(field), histogram_over_time(field), quantile_over_time(field, phi), stddev(field).
//
// StartNano and EndNano are aligned to StepNano boundaries before execution (matching Tempo
// IntervalMapperQueryRange semantics). The actual bucket count is
// ceil((alignedEnd-alignedStart)/StepNano), which may exceed ceil((EndNano-StartNano)/StepNano)
// when the inputs are not already step-aligned.
// COUNT/RATE: missing buckets are 0. Other functions: missing buckets are NaN.
func ExecuteMetricsTraceQL(r *Reader, query string, opts TraceMetricOptions) (result *TraceMetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: reader cannot be nil")
	}

	stepNano := opts.StepNano
	if stepNano <= 0 {
		stepNano = 60 * 1_000_000_000 // default: 1 minute
	}

	if opts.StartNano > opts.EndNano {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: StartNano (%d) must not exceed EndNano (%d)",
			opts.StartNano, opts.EndNano)
	}

	// Align start/end to step boundaries — matches Tempo IntervalMapperQueryRange semantics.
	// alignedStart = start rounded down to nearest step (floor division).
	// alignedEnd   = end rounded up to nearest step (ceiling division).
	// Use explicit floor/ceil to handle negative timestamps correctly: Go's % keeps the sign
	// of the dividend, so for negative values plain modulo rounds toward zero, not toward -∞.
	startMod := opts.StartNano % stepNano
	if startMod < 0 {
		startMod += stepNano
	}
	if opts.StartNano < math.MinInt64+startMod {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: StartNano (%d) too close to int64 min for step alignment",
			opts.StartNano)
	}
	alignedStart := opts.StartNano - startMod
	endMod := opts.EndNano % stepNano
	if endMod < 0 {
		endMod += stepNano
	}
	alignedEnd := opts.EndNano
	if endMod != 0 {
		bump := stepNano - endMod
		if opts.EndNano > math.MaxInt64-bump {
			return nil, fmt.Errorf("ExecuteMetricsTraceQL: EndNano (%d) too close to int64 max for step alignment",
				opts.EndNano)
		}
		alignedEnd = opts.EndNano + bump
	}

	prog, spec, compileErr := vm.CompileTraceQLMetrics(query, alignedStart, alignedEnd)
	if compileErr != nil {
		return nil, fmt.Errorf("compile TraceQL metrics query: %w", compileErr)
	}

	// Override the step size with the caller-provided value (compiler uses a fixed default).
	spec.TimeBucketing.StepSizeNanos = stepNano

	return modules_executor.ExecuteTraceMetrics(r, prog, spec)
}
