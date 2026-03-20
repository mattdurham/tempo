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
	"log/slog"
	"math"
	"strings"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
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

// QueryOptions configures query execution.
type QueryOptions struct {
	Limit int // Maximum number of spans to return (0 = unlimited).
	// MostRecent controls block traversal order. Always use keyed struct literals:
	// QueryOptions{Limit: 10, MostRecent: true}.
	// Uses Backward direction with span:start timestamp sorting. For intrinsic-only queries,
	// top-K selection uses only the intrinsic column blobs (no full block I/O).
	MostRecent bool
	// StartNano is the inclusive lower bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range ends before StartNano are skipped entirely.
	// 0 means no lower bound.
	StartNano uint64
	// EndNano is the inclusive upper bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range begins after EndNano are skipped entirely.
	// 0 means no upper bound.
	EndNano uint64
	// StartBlock is the first internal block index to scan (0-based, inclusive).
	// Used by the frontend sharder to partition a single blockpack file into
	// multiple sub-file jobs. 0 means start from the first block.
	StartBlock int
	// BlockCount is the number of internal blocks to scan starting from StartBlock.
	// 0 means scan all blocks (no sub-file sharding).
	BlockCount int
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

// SpanMatch represents a single span that matched the query.
// Fields is safe to retain after QueryTraceQL or QueryLogQL returns.
type SpanMatch struct {
	Fields  SpanFieldsProvider
	TraceID string
	SpanID  string
}

// Clone materializes all fields from the lazy provider and returns a deep copy
// with stable ownership, safe to hold beyond any internal callback or function return.
// If Fields is nil (e.g. structural queries), the clone has nil Fields.
func (m *SpanMatch) Clone() SpanMatch {
	out := SpanMatch{TraceID: m.TraceID, SpanID: m.SpanID}
	if m.Fields == nil {
		return out
	}
	materialized := make(map[string]any, 32) // pre-size to avoid grow/rehash for typical label counts
	m.Fields.IterateFields(func(name string, value any) bool {
		materialized[name] = value
		return true
	})
	out.Fields = &materializedSpanFields{fields: materialized}
	return out
}

// materializedSpanFields is a heap-allocated map-backed SpanFieldsProvider
// returned by SpanMatch.Clone(). Safe to hold beyond the callback lifetime.
type materializedSpanFields struct {
	fields map[string]any
}

func (m *materializedSpanFields) GetField(name string) (any, bool) {
	v, ok := m.fields[name]
	return v, ok
}

func (m *materializedSpanFields) IterateFields(fn func(name string, value any) bool) {
	for k, v := range m.fields {
		if !fn(k, v) {
			return
		}
	}
}

// spanMatchFn is the internal callback type used by streaming query helpers.
// match is valid only for the duration of the call; more=false signals end of results.
type spanMatchFn func(match *SpanMatch, more bool) bool

// QueryTraceQL executes a TraceQL query against a modules-format blockpack file
// and returns all matching spans.
//
// Supported query types:
//   - Filter expressions: `{ span.http.method = "GET" }`
//   - Structural queries: `{ expr } OP { expr }` where OP is >>, >, ~, <<, <, !~
//
// For filter queries, each SpanMatch.Fields supports GetField and IterateFields.
// For structural queries, SpanMatch.Fields is nil — only TraceID and SpanID are set.
// Metrics queries return an error.

// ColumnNames returns all column names known to this reader — the union of
// range-indexed and sketch columns. Derived from file header metadata; no
// block I/O is required. Returns a sorted slice of column name strings
// (e.g. "span:status", "resource.service.name", "span.http.method").
func ColumnNames(r *Reader) []string {
	return r.ColumnNames()
}

// QueryTraceQL executes a TraceQL query against a modules-format blockpack file
// and returns all matching spans.
func QueryTraceQL(r *Reader, traceqlQuery string, opts QueryOptions) (results []SpanMatch, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("QueryTraceQL: reader cannot be nil")
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, fmt.Errorf("QueryTraceQL: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse TraceQL: %w", parseErr)
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
		err = streamFilterQuery(r, q, opts, collector)
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
					TraceID: fmt.Sprintf("%x", m.TraceID[:]),
					SpanID:  fmt.Sprintf("%x", m.SpanID),
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
	return results, err
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
	streamErr := streamFilterProgram(r, program, filterOpts, func(match *SpanMatch, more bool) bool {
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

// streamFilterQuery executes a TraceQL filter query against a modules-format reader.
func streamFilterQuery(r *Reader, filterExpr *traceqlparser.FilterExpression, opts QueryOptions, fn spanMatchFn) error {
	program, compileErr := vm.CompileTraceQLFilter(filterExpr)
	if compileErr != nil {
		return fmt.Errorf("compile TraceQL filter: %w", compileErr)
	}

	return streamFilterProgram(r, program, opts, fn)
}

// streamFilterProgram executes a compiled filter program against a modules-format reader.
func streamFilterProgram(r *Reader, program *vm.Program, opts QueryOptions, fn spanMatchFn) error {
	// SPEC-STREAM-8: MostRecent maps to Backward direction with span:start timestamp sorting.
	// span:start is always present in searchMetaColumns so no extra I/O is needed.
	// When MostRecent+Limit, Collect gives globally top-K results by span:start.
	collectOpts := modules_executor.CollectOptions{
		TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
		Limit:      opts.Limit,
		StartBlock: opts.StartBlock,
		BlockCount: opts.BlockCount,
		// NOTE-028: AllColumns defaults to false — for Collect this means the second pass decodes only
		// searchMetaColumns ∪ predicate columns.
	}
	if opts.MostRecent {
		collectOpts.Direction = modules_queryplanner.Backward
		collectOpts.TimestampColumn = "span:start"
	}
	rows, err := modules_executor.Collect(r, program, collectOpts)
	if err != nil {
		return err
	}
	for _, row := range rows {
		var fields SpanFieldsProvider
		var traceIDHex, spanIDHex string
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
			fields = modules_blockio.NewSpanFieldsAdapter(row.Block, row.RowIdx)
			traceIDHex, spanIDHex = extractIDs(row.Block, row.RowIdx)
		}
		match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
	return nil
}

// LogQueryStats contains block selection and I/O statistics for a log query.
// Reported via LogQueryOptions.OnStats after query execution completes.
type LogQueryStats struct {
	Explain        string // ASCII trace of predicate tree → block set resolution
	TotalBlocks    int    // total blocks in the file
	PrunedByTime   int    // blocks eliminated by time-range comparison
	PrunedByIndex  int    // blocks eliminated by range index lookups
	PrunedByFuse   int    // blocks eliminated by BinaryFuse8 membership checks
	PrunedByCMS    int    // blocks eliminated by Count-Min Sketch zero-estimate checks
	SelectedBlocks int    // blocks selected for reading after all pruning
	FetchedBlocks  int    // blocks actually fetched (≤ SelectedBlocks when limit causes early stop)
}

// LogQueryOptions configures log query execution.
type LogQueryOptions struct {
	// OnStats is an optional callback invoked after query execution with block
	// selection and I/O statistics. FetchedBlocks reflects actual I/O done,
	// which is less than SelectedBlocks when a Limit causes early termination.
	OnStats   func(LogQueryStats)
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
func QueryLogQL(r *Reader, logqlQuery string, opts LogQueryOptions) (results []SpanMatch, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryLogQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("QueryLogQL: reader cannot be nil")
	}

	sel, parseErr := logqlparser.Parse(logqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse LogQL: %w", parseErr)
	}

	program, pipeline, compileErr := logqlparser.CompileAll(sel)
	if compileErr != nil {
		return nil, fmt.Errorf("compile LogQL: %w", compileErr)
	}

	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}

	if pipeline != nil {
		err = streamLogQLWithPipeline(r, program, pipeline, opts, collector)
	} else {
		err = streamLogProgram(r, program, opts, collector)
	}
	return results, err
}

// streamLogQLWithPipeline streams log entries applying a compiled pipeline.
// Delegates to CollectLogs which uses a heap with block-level timestamp pruning
// for limited queries, and a collect-sort-deliver path for unlimited queries.
func streamLogQLWithPipeline(
	r *Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	opts LogQueryOptions,
	fn spanMatchFn,
) error {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}

	entries, err := modules_executor.CollectLogs(
		r,
		program,
		pipeline,
		modules_executor.CollectOptions{
			Limit:     opts.Limit,
			TimeRange: modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction: direction,
			OnStats: func(s modules_executor.CollectStats) {
				if opts.OnStats != nil {
					opts.OnStats(LogQueryStats{
						TotalBlocks:    s.TotalBlocks,
						PrunedByTime:   s.PrunedByTime,
						PrunedByIndex:  s.PrunedByIndex,
						PrunedByFuse:   s.PrunedByFuse,
						PrunedByCMS:    s.PrunedByCMS,
						SelectedBlocks: s.SelectedBlocks,
						FetchedBlocks:  s.FetchedBlocks,
						Explain:        s.Explain,
					})
				}
			},
		},
	)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		match := &SpanMatch{Fields: &logEntryFields{
			lokiLabels: entry.LokiLabels,
			logAttrs:   entry.LogAttrs,
			line:       entry.Line,
			timestamp:  entry.TimestampNanos,
		}}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
	return nil
}

// logEntryFields adapts a log entry to SpanFieldsProvider.
// lokiLabels is the raw resource.__loki_labels__ string (stream selector).
// logAttrs holds log.* ColumnTypeString values as parallel name/value slices
// (e.g. Names=["log.detected_level"]) so extractStructuredMetadata can find them via
// the "log." prefix. IterateFields emits only these three sets — no full
// resource-label map is built per row.
//
// NOTE: This exposes a reduced field set compared to SpanFieldsAdapter (used by the
// trace query path via streamFilterProgram). SpanFieldsAdapter lazily resolves any
// column by full name; logEntryFields only exposes log:body, log:timestamp,
// resource.__loki_labels__, and log.* ColumnTypeString attributes. This is intentional:
// the log pipeline path (streamLogQLWithPipeline) materializes only the fields that
// downstream consumers (the LokiConverter) actually need. Callers requiring full
// column access should use QueryTraceQL or the streamFilterProgram path instead.
type logEntryFields struct {
	lokiLabels string
	line       string
	logAttrs   modules_executor.LogAttrs
	timestamp  uint64
}

func (f *logEntryFields) GetField(name string) (any, bool) {
	switch name {
	case "log:body":
		return f.line, true
	case "log:timestamp":
		return f.timestamp, true
	case "resource.__loki_labels__":
		return f.lokiLabels, true
	}
	for i, n := range f.logAttrs.Names {
		if n == name {
			return f.logAttrs.Values[i], true
		}
	}
	return "", false
}

func (f *logEntryFields) IterateFields(fn func(name string, value any) bool) {
	if !fn("log:body", f.line) {
		return
	}
	if !fn("log:timestamp", f.timestamp) {
		return
	}
	if f.lokiLabels != "" {
		if !fn("resource.__loki_labels__", f.lokiLabels) {
			return
		}
	}
	for i, k := range f.logAttrs.Names {
		if !fn(k, f.logAttrs.Values[i]) {
			return
		}
	}
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
	// StartNano is the inclusive start of the query time window (unix nanoseconds).
	StartNano int64
	// EndNano is the exclusive end of the query time window (unix nanoseconds).
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
// Returns a TraceMetricsResult with one TraceTimeSeries per unique group-by key combination.
// Each series has len(Values) == ceil((EndNano-StartNano)/StepNano) buckets.
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

	prog, spec, compileErr := vm.CompileTraceQLMetrics(query, opts.StartNano, opts.EndNano)
	if compileErr != nil {
		return nil, fmt.Errorf("compile TraceQL metrics query: %w", compileErr)
	}

	// Override the step size with the caller-provided value (compiler uses a fixed default).
	spec.TimeBucketing.StepSizeNanos = stepNano

	return modules_executor.ExecuteTraceMetrics(r, prog, spec)
}

// streamLogProgram executes a compiled log program against a modules-format reader.
// Blocks are fetched lazily in ~8 MB coalesced batches; fetching stops once opts.Limit
// matches have been delivered, so I/O is proportional to results returned.
func streamLogProgram(r *Reader, program *vm.Program, opts LogQueryOptions, fn spanMatchFn) error {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}
	rows, err := modules_executor.Collect(
		r,
		program,
		modules_executor.CollectOptions{
			Limit:           opts.Limit,
			TimeRange:       modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction:       direction,
			TimestampColumn: "log:timestamp",
			OnStats: func(s modules_executor.CollectStats) {
				if opts.OnStats != nil {
					opts.OnStats(LogQueryStats{
						TotalBlocks:    s.TotalBlocks,
						PrunedByTime:   s.PrunedByTime,
						PrunedByIndex:  s.PrunedByIndex,
						PrunedByFuse:   s.PrunedByFuse,
						PrunedByCMS:    s.PrunedByCMS,
						SelectedBlocks: s.SelectedBlocks,
						FetchedBlocks:  s.FetchedBlocks,
						Explain:        s.Explain,
					})
				}
			},
		},
	)
	if err != nil {
		return err
	}
	for _, row := range rows {
		var fields SpanFieldsProvider
		var traceIDHex, spanIDHex string
		if row.IntrinsicFields != nil {
			fields = row.IntrinsicFields
			if v, ok := fields.GetField("trace:id"); ok {
				traceIDHex = hexEncodeField(v)
			}
			if v, ok := fields.GetField("span:id"); ok {
				spanIDHex = hexEncodeField(v)
			}
		} else {
			fields = modules_blockio.NewSpanFieldsAdapter(row.Block, row.RowIdx)
			traceIDHex, spanIDHex = extractIDs(row.Block, row.RowIdx)
		}
		match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
	return nil
}

// extractIDs extracts hex-encoded trace ID and span ID strings from a block row.
// Returns empty strings when block is nil (intrinsic fast-path rows have no block).
func extractIDs(block *modules_reader.Block, rowIdx int) (traceID, spanID string) {
	if block == nil {
		return "", ""
	}
	if col := block.GetColumn("trace:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			traceID = fmt.Sprintf("%x", v)
		}
	}
	if col := block.GetColumn("span:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			spanID = fmt.Sprintf("%x", v)
		}
	}
	return traceID, spanID
}

// hexEncodeField converts a field value (string or []byte) to a hex string.
// Used for trace:id and span:id which are stored as bytes in intrinsic columns.
func hexEncodeField(v any) string {
	switch b := v.(type) {
	case string:
		return b
	case []byte:
		return hex.EncodeToString(b)
	}
	return ""
}

// FileLayoutReport is the top-level result of AnalyzeFileLayout.
