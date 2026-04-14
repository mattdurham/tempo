package blockpack_test

import (
	"bytes"
	"fmt"
	"testing"

	blockpack "github.com/grafana/blockpack"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestLogBlockpack writes log records with known timestamps and bodies.
// Records are created with timestamps 1_000_000_000 + i*1_000_000_000 (1s apart).
// Two services: "svc-a" (even indices) and "svc-b" (odd indices).
func writeTestLogBlockpack(t *testing.T, numRecords, recordsPerBlock int) []byte {
	t.Helper()
	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, recordsPerBlock)
	require.NoError(t, err)

	for i := range numRecords {
		svc := "svc-a"
		if i%2 == 1 {
			svc = "svc-b"
		}
		tsNano := uint64(1_000_000_000 + i*1_000_000_000) //nolint:gosec
		body := fmt.Sprintf("log message %d", i)

		ld := &logsv1.LogsData{
			ResourceLogs: []*logsv1.ResourceLogs{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svc}},
					}},
				},
				ScopeLogs: []*logsv1.ScopeLogs{{
					LogRecords: []*logsv1.LogRecord{{
						TimeUnixNano: tsNano,
						Body: &commonv1.AnyValue{
							Value: &commonv1.AnyValue_StringValue{StringValue: body},
						},
					}},
				}},
			}},
		}
		require.NoError(t, w.AddLogsData(ld))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return output.Bytes()
}

func openLogReader(t *testing.T, data []byte) *blockpack.Reader {
	t.Helper()
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	return r
}

func TestQueryLogQL_MatchAll(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name=~".+"}`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	assert.Len(t, matches, 6, "all 6 records should match")
}

func TestQueryLogQL_LabelFilter(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name="svc-a"}`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	// Even indices: 0, 2, 4 → "log message 0", "log message 2", "log message 4"
	assert.Len(t, matches, 3)
}

func TestQueryLogQL_LineFilterContains(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name=~".+"} |= "message 3"`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	assert.Len(t, matches, 1)
	if len(matches) > 0 {
		if v, ok := matches[0].Fields.GetField("log:body"); ok {
			assert.Equal(t, "log message 3", v.(string))
		}
	}
}

func TestQueryLogQL_TimeRange(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	// Timestamps: 1s, 2s, 3s, 4s, 5s, 6s (in nanos).
	// Filter to [2s, 4s] inclusive.
	opts := blockpack.LogQueryOptions{
		StartNano: 2_000_000_000,
		EndNano:   4_000_000_000,
	}

	matches, _, err := blockpack.QueryLogQL(r, `{service.name=~".+"}`, opts)
	require.NoError(t, err)
	// Only records at 2s, 3s, 4s should match.
	assert.Len(t, matches, 3, "expected 3 records in [2s, 4s]")
	for _, m := range matches {
		if v, ok := m.Fields.GetField("log:timestamp"); ok {
			if ts, isU64 := v.(uint64); isU64 {
				assert.GreaterOrEqual(t, ts, uint64(2_000_000_000))
				assert.LessOrEqual(t, ts, uint64(4_000_000_000))
			}
		}
	}
}

func TestQueryLogQL_Limit(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name=~".+"}`, blockpack.LogQueryOptions{Limit: 2})
	require.NoError(t, err)
	assert.Len(t, matches, 2, "limit should cap results at 2")
}

func TestQueryLogQL_ForwardDirection(t *testing.T) {
	// Use a single service to get monotonic timestamp ordering.
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name="svc-a"}`, blockpack.LogQueryOptions{Forward: true})
	require.NoError(t, err)
	require.NotEmpty(t, matches)

	var forwardTS []uint64
	for _, m := range matches {
		if v, ok := m.Fields.GetField("log:timestamp"); ok {
			if ts, isU64 := v.(uint64); isU64 {
				forwardTS = append(forwardTS, ts)
			}
		}
	}
	// Verify forward order: each timestamp >= previous.
	for i := 1; i < len(forwardTS); i++ {
		assert.GreaterOrEqual(t, forwardTS[i], forwardTS[i-1], "forward order violated at index %d", i)
	}
}

func TestQueryLogQL_BackwardDirection(t *testing.T) {
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name="svc-a"}`, blockpack.LogQueryOptions{Forward: false})
	require.NoError(t, err)
	require.NotEmpty(t, matches)

	var backwardTS []uint64
	for _, m := range matches {
		if v, ok := m.Fields.GetField("log:timestamp"); ok {
			if ts, isU64 := v.(uint64); isU64 {
				backwardTS = append(backwardTS, ts)
			}
		}
	}
	// Verify backward order: each timestamp <= previous.
	for i := 1; i < len(backwardTS); i++ {
		assert.LessOrEqual(t, backwardTS[i], backwardTS[i-1], "backward order violated at index %d", i)
	}
}

func TestQueryLogQL_EmptyResult(t *testing.T) {
	data := writeTestLogBlockpack(t, 4, 10)
	r := openLogReader(t, data)

	matches, _, err := blockpack.QueryLogQL(r, `{service.name="nonexistent"}`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	assert.Empty(t, matches)
}

func TestQueryLogQL_NilReader(t *testing.T) {
	_, _, err := blockpack.QueryLogQL(nil, `{}`, blockpack.LogQueryOptions{})
	assert.Error(t, err)
}

func TestQueryLogQL_ParseError(t *testing.T) {
	data := writeTestLogBlockpack(t, 1, 10)
	r := openLogReader(t, data)
	_, _, err := blockpack.QueryLogQL(r, `not valid logql`, blockpack.LogQueryOptions{})
	assert.Error(t, err)
}

// TestStreamLogQL_PipelinePathLogAttrs verifies that pipeline-path queries (pipeline != nil)
// expose original LogRecord string attributes via IterateFields with the "log." prefix intact.
// This covers the case where a query like `| logfmt` forces pipeline != nil, and the
// downstream extractStructuredMetadata must find "log.detected_level" (not just "detected_level").
func TestStreamLogQL_PipelinePathLogAttrs(t *testing.T) {
	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, 0)
	require.NoError(t, err)

	ld := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc-a"}},
				}},
			},
			ScopeLogs: []*logsv1.ScopeLogs{{
				LogRecords: []*logsv1.LogRecord{{
					TimeUnixNano: 1_000_000_000,
					Body: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{StringValue: `level=info msg=hello`},
					},
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "detected_level",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "info"}},
						},
						{
							Key:   "instance_id",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "inst-001"}},
						},
					},
				}},
			}},
		}},
	}
	require.NoError(t, w.AddLogsData(ld))
	_, err = w.Flush()
	require.NoError(t, err)

	r := openLogReader(t, output.Bytes())

	// `| logfmt` forces pipeline != nil → dispatches to streamLogQLWithPipeline.
	matches, _, err := blockpack.QueryLogQL(r, `{service.name="svc-a"} | logfmt`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, matches, "expected at least one matched entry")

	fields := make(map[string]any)
	matches[0].Fields.IterateFields(func(name string, value any) bool {
		fields[name] = value
		return true
	})

	// Original LogRecord attributes must appear with "log." prefix intact.
	assert.Equal(t, "info", fields["log.detected_level"], "log.detected_level must be present via IterateFields")
	assert.Equal(t, "inst-001", fields["log.instance_id"], "log.instance_id must be present via IterateFields")
}

// TestExecuteMetricsLogQL_CountOverTime verifies that ExecuteMetricsLogQL
// returns count_over_time rows for a simple metric query.
func TestExecuteMetricsLogQL_CountOverTime(t *testing.T) {
	// Records at 1s, 2s, 3s, 4s, 5s, 6s — 6 total, 2 per service.
	// startNS = 1_000_000_000 (first record timestamp)
	// Use a 10-second bucket covering [0, 10s): should count all 6.
	data := writeTestLogBlockpack(t, 6, 10)
	r := openLogReader(t, data)

	const secNS = int64(1_000_000_000)
	opts := blockpack.LogMetricOptions{
		StartNano: secNS * 0,
		EndNano:   secNS * 10,
		StepNano:  secNS * 10,
	}
	result, err := blockpack.ExecuteMetricsLogQL(r, `count_over_time({service.name=~".+"}[10s])`, opts)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1, "expect 1 time bucket")
	assert.Equal(t, float64(6), result.Rows[0].Values["count_over_time"], "all 6 log records should be counted")
}

// TestExecuteMetricsLogQL_NilReader returns error.
func TestExecuteMetricsLogQL_NilReader(t *testing.T) {
	opts := blockpack.LogMetricOptions{StartNano: 0, EndNano: 10_000_000_000, StepNano: 10_000_000_000}
	_, err := blockpack.ExecuteMetricsLogQL(nil, `count_over_time({}[10s])`, opts)
	assert.Error(t, err)
}

// TestExecuteMetricsLogQL_NotMetricQuery returns error for a filter query.
func TestExecuteMetricsLogQL_NotMetricQuery(t *testing.T) {
	data := writeTestLogBlockpack(t, 1, 10)
	r := openLogReader(t, data)
	opts := blockpack.LogMetricOptions{StartNano: 0, EndNano: 10_000_000_000, StepNano: 10_000_000_000}
	_, err := blockpack.ExecuteMetricsLogQL(r, `{service.name="svc-a"}`, opts)
	assert.Error(t, err)
}

// QS-API-01: QueryLogQL returns QueryStats with a populated ExecutionPath and positive TotalDuration.
func TestQueryLogQL_ReturnsQueryStats(t *testing.T) {
	t.Parallel()

	data := writeTestLogBlockpack(t, 4, 10)
	r := openLogReader(t, data)

	// Simple label filter — no pipeline stages, routes through streamLogProgram.
	matches, qs, err := blockpack.QueryLogQL(r, `{service.name="svc-a"}`, blockpack.LogQueryOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, matches)
	assert.NotEmpty(t, qs.ExecutionPath, "ExecutionPath must be set")
	assert.True(t, qs.TotalDuration > 0, "TotalDuration must be positive")
}

// QS-API-02: QueryLogQL with a pipeline stage returns QueryStats via streamLogQLWithPipeline.
func TestQueryLogQL_PipelinePath_ReturnsQueryStats(t *testing.T) {
	t.Parallel()

	data := writeTestLogBlockpack(t, 4, 10)
	r := openLogReader(t, data)

	// Pipeline stage forces the pipeline path (streamLogQLWithPipeline).
	matches, qs, err := blockpack.QueryLogQL(
		r,
		`{service.name="svc-a"} | line_format "{{.line}}"`,
		blockpack.LogQueryOptions{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, matches)
	assert.NotEmpty(t, qs.ExecutionPath, "ExecutionPath must be set for pipeline path")
	assert.True(t, qs.TotalDuration > 0, "TotalDuration must be positive for pipeline path")
}
