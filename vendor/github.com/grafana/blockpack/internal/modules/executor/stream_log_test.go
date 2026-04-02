package executor_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
)

// buildLogFile writes log records to a buffer and returns the bytes.
// Each record has the given service name, body, and timestamp.
func buildLogFile(t *testing.T, records []struct {
	svc  string
	body string
	ts   uint64
},
) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 0,
	})
	require.NoError(t, err)

	for _, rec := range records {
		record := &logsv1.LogRecord{
			TimeUnixNano: rec.ts,
			Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: rec.body}},
		}
		ld := &logsv1.LogsData{
			ResourceLogs: []*logsv1.ResourceLogs{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: rec.svc}},
					}},
				},
				ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{record}}},
			}},
		}
		require.NoError(t, w.AddLogsData(ld))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestStreamLogs_BasicFilter verifies that StreamLogs returns all matched rows.
func TestStreamLogs_BasicFilter(t *testing.T) {
	data := buildLogFile(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{svc: "svc", body: "hello", ts: 1_000_000_000},
		{svc: "svc", body: "world", ts: 2_000_000_000},
		{svc: "other", body: "skip", ts: 3_000_000_000},
	})

	r := openLogMetricsReader(t, data)
	sel, err := logqlparser.Parse(`{service.name = "svc"}`)
	require.NoError(t, err)
	prog, err := logqlparser.Compile(sel)
	require.NoError(t, err)

	entries, err := modules_executor.StreamLogs(r, prog, nil)
	require.NoError(t, err)
	require.Len(t, entries, 2, "expected 2 entries for svc")

	bodies := make([]string, len(entries))
	for i, e := range entries {
		bodies[i] = e.Line
	}
	assert.Contains(t, bodies, "hello")
	assert.Contains(t, bodies, "world")
}

// TestStreamLogs_WithPipeline verifies that a JSON pipeline stage passes entries through.
// Body-parsed columns (e.g. "level" extracted from JSON at ingest time) are stored as
// ColumnTypeRangeString block columns. With the lazy-decode optimization, only columns
// in wantColumns (predicate columns + log:timestamp + resource.__loki_labels__) appear in
// Materialize() output. Body-parsed non-predicate columns are excluded — callers that
// need them must include a label filter (e.g. | level="info") to pull them into wantColumns.
// This mirrors the Loki chunk model where only stream labels are returned without explicit filtering.
func TestStreamLogs_WithPipeline(t *testing.T) {
	data := buildLogFile(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{svc: "svc", body: `{"level":"info","msg":"started"}`, ts: 1_000_000_000},
		{svc: "svc", body: `{"level":"error","msg":"failed"}`, ts: 2_000_000_000},
	})

	r := openLogMetricsReader(t, data)
	sel, err := logqlparser.Parse(`{service.name = "svc"}`)
	require.NoError(t, err)
	prog, err := logqlparser.Compile(sel)
	require.NoError(t, err)

	stages := []logqlparser.PipelineStage{{Type: logqlparser.StageJSON}}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	raw, collectErr := modules_executor.StreamLogs(r, prog, pipeline)
	require.NoError(t, collectErr)
	require.Len(t, raw, 2)

	// Both rows should be returned with their body lines intact.
	// LokiLabels is empty here because test data uses raw OTLP without __loki_labels__.
	for _, entry := range raw {
		assert.NotEmpty(t, entry.Line)
	}
}

// TestStreamLogs_PipelineDropsRows verifies that label filter stage drops rows.
func TestStreamLogs_PipelineDropsRows(t *testing.T) {
	data := buildLogFile(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{svc: "svc", body: `{"level":"info","msg":"ok"}`, ts: 1_000_000_000},
		{svc: "svc", body: `{"level":"error","msg":"fail"}`, ts: 2_000_000_000},
		{svc: "svc", body: `{"level":"info","msg":"ok2"}`, ts: 3_000_000_000},
	})

	r := openLogMetricsReader(t, data)
	sel, err := logqlparser.Parse(`{service.name = "svc"}`)
	require.NoError(t, err)
	prog, err := logqlparser.Compile(sel)
	require.NoError(t, err)

	stages := []logqlparser.PipelineStage{
		{Type: logqlparser.StageJSON},
		{Type: logqlparser.StageLabelFilter, LabelFilter: &logqlparser.LabelFilter{
			Name:  "level",
			Value: "error",
			Op:    logqlparser.OpEqual,
		}},
	}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	entries, collectErr := modules_executor.StreamLogs(r, prog, pipeline)
	require.NoError(t, collectErr)
	require.Len(t, entries, 1, "only error rows should pass")
	assert.Contains(t, entries[0].Line, "error", "surviving row should be the error entry")
}

// TestStreamLogs_NilReader returns nil without error.
func TestStreamLogs_NilReader(t *testing.T) {
	sel, err := logqlparser.Parse(`{}`)
	require.NoError(t, err)
	prog, err := logqlparser.Compile(sel)
	require.NoError(t, err)

	entries, err := modules_executor.StreamLogs(nil, prog, nil)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

// buildLogFileWithAttrs writes log records with per-record attributes to a buffer.
func buildLogFileWithAttrs(t *testing.T, records []struct {
	svc   string
	body  string
	attrs []*commonv1.KeyValue
	ts    uint64
},
) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 0,
	})
	require.NoError(t, err)
	for _, rec := range records {
		record := &logsv1.LogRecord{
			TimeUnixNano: rec.ts,
			Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: rec.body}},
			Attributes:   rec.attrs,
		}
		ld := &logsv1.LogsData{
			ResourceLogs: []*logsv1.ResourceLogs{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: rec.svc}},
					}},
				},
				ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{record}}},
			}},
		}
		require.NoError(t, w.AddLogsData(ld))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestStreamLogs_NumericPushdown verifies that a numeric label filter (| latency_ms > 100)
// is pushed to the ColumnPredicate and evaluated correctly against a string-typed log.*
// column. rowCompare parses the stored string as float64, matching LabelFilterStage semantics.
func TestStreamLogs_NumericPushdown(t *testing.T) {
	data := buildLogFileWithAttrs(t, []struct {
		svc   string
		body  string
		attrs []*commonv1.KeyValue
		ts    uint64
	}{
		{svc: "svc", body: "slow", ts: 1_000_000_000, attrs: []*commonv1.KeyValue{strAttr("latency_ms", "500")}},
		{svc: "svc", body: "fast", ts: 2_000_000_000, attrs: []*commonv1.KeyValue{strAttr("latency_ms", "90")}},
		{svc: "svc", body: "mid", ts: 3_000_000_000, attrs: []*commonv1.KeyValue{strAttr("latency_ms", "100")}},
	})

	r := openLogMetricsReader(t, data)
	sel, err := logqlparser.Parse(`{service.name = "svc"} | latency_ms > 100`)
	require.NoError(t, err)
	prog, pipeline, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	entries, err := modules_executor.StreamLogs(r, prog, pipeline)
	require.NoError(t, err)
	bodies := make([]string, len(entries))
	for i, e := range entries {
		bodies[i] = e.Line
	}
	assert.Equal(t, []string{"slow"}, bodies,
		"only latency_ms=500 should match > 100; latency_ms=90 and =100 should not")
}

// TestStreamLogs_OrPushdown verifies that a pre-parser OR label filter
// (| level="error" or level="warn") is pushed down to ColumnPredicate and
// returns rows matching either alternative.
func TestStreamLogs_OrPushdown(t *testing.T) {
	data := buildLogFileWithAttrs(t, []struct {
		svc   string
		body  string
		attrs []*commonv1.KeyValue
		ts    uint64
	}{
		{svc: "svc", body: "err-line", ts: 1_000_000_000, attrs: []*commonv1.KeyValue{strAttr("level", "error")}},
		{svc: "svc", body: "warn-line", ts: 2_000_000_000, attrs: []*commonv1.KeyValue{strAttr("level", "warn")}},
		{svc: "svc", body: "info-line", ts: 3_000_000_000, attrs: []*commonv1.KeyValue{strAttr("level", "info")}},
	})

	r := openLogMetricsReader(t, data)
	sel, err := logqlparser.Parse(`{service.name = "svc"} | level="error" or level="warn"`)
	require.NoError(t, err)
	prog, pipeline, err := logqlparser.CompileAll(sel)
	require.NoError(t, err)

	entries, err := modules_executor.StreamLogs(r, prog, pipeline)
	require.NoError(t, err)
	bodies := make([]string, len(entries))
	for i, e := range entries {
		bodies[i] = e.Line
	}
	assert.ElementsMatch(t, []string{"err-line", "warn-line"}, bodies,
		"level=error and level=warn should match; level=info should not")
}
