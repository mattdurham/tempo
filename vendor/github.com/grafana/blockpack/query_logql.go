package blockpack

// query_logql.go — LogQL streaming execution internals.
// Public entry point is QueryLogQL in api.go; this file holds the
// implementation helpers it delegates to.

import (
	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// streamLogQLWithPipeline streams log entries applying a compiled pipeline.
// Delegates to CollectLogs which uses a heap with block-level timestamp pruning
// for limited queries, and a collect-sort-deliver path for unlimited queries.
func streamLogQLWithPipeline(
	r *Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	opts LogQueryOptions,
	fn spanMatchFn,
) (QueryStats, error) {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}

	entries, qs, err := modules_executor.CollectLogs(
		r,
		program,
		pipeline,
		modules_executor.CollectOptions{
			Limit:     opts.Limit,
			TimeRange: modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction: direction,
		},
	)
	if err != nil {
		return qs, err
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
	return qs, nil
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

// streamLogProgram executes a compiled log program against a modules-format reader.
// Blocks are fetched lazily in ~8 MB coalesced batches; fetching stops once opts.Limit
// matches have been delivered, so I/O is proportional to results returned.
func streamLogProgram(r *Reader, program *vm.Program, opts LogQueryOptions, fn spanMatchFn) (QueryStats, error) {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}
	rows, qs, err := modules_executor.Collect(
		r,
		program,
		modules_executor.CollectOptions{
			Limit:           opts.Limit,
			TimeRange:       modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction:       direction,
			TimestampColumn: "log:timestamp",
		},
	)
	if err != nil {
		return qs, err
	}
	// Pre-build intrinsic ID lookup maps for the block-scan path. These are used as a
	// fallback when trace:id / span:id are absent from block columns (intrinsic-only storage).
	// GetIntrinsicColumn is cached after first load so this is cheap on subsequent calls.
	logTraceIDByRef := buildIntrinsicBytesMap(r, "trace:id")
	logSpanIDByRef := buildIntrinsicBytesMap(r, "span:id")
	for _, row := range rows {
		var fields SpanFieldsProvider
		var traceIDHex, spanIDHex string
		var rawAdapter SpanFieldsProvider // tracked for pool release
		if row.IntrinsicFields != nil {
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
			traceIDHex, spanIDHex = extractIDs(row.Block, row.RowIdx, row.BlockIdx, logTraceIDByRef, logSpanIDByRef)
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
	return qs, nil
}
