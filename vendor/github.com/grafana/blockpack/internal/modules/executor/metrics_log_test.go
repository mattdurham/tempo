package executor_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/vm"
)

// --- helpers ---

type metricsMemProvider struct{ data []byte }

func (m *metricsMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *metricsMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func mustNewLogMetricsWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *modules_blockio.Writer {
	t.Helper()
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)
	return w
}

func openLogMetricsReader(t *testing.T, data []byte) *modules_reader.Reader {
	t.Helper()
	r, err := modules_reader.NewReaderFromProvider(&metricsMemProvider{data: data})
	require.NoError(t, err)
	return r
}

func makeLogRecord(svcName string, body string, attrs []*commonv1.KeyValue, tsNano uint64) *logsv1.LogsData {
	record := &logsv1.LogRecord{
		TimeUnixNano: tsNano,
		Attributes:   attrs,
		Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}},
	}
	return &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
				}},
			},
			ScopeLogs: []*logsv1.ScopeLogs{{
				LogRecords: []*logsv1.LogRecord{record},
			}},
		}},
	}
}

func compileSelectorProgram(t *testing.T, query string) *vm.Program {
	t.Helper()
	sel, err := logqlparser.Parse(query)
	require.NoError(t, err)
	prog, err := logqlparser.Compile(sel)
	require.NoError(t, err)
	return prog
}

func makeTimeBucketSpec(startNano, stepNano int64, numBuckets int) vm.QuerySpec {
	endNano := startNano + stepNano*int64(numBuckets) //nolint:gosec
	return vm.QuerySpec{
		TimeBucketing: vm.TimeBucketSpec{
			Enabled:       true,
			StartTime:     startNano,
			EndTime:       endNano,
			StepSizeNanos: stepNano,
		},
		Filter:    vm.FilterSpec{IsMatchAll: true},
		Aggregate: vm.AggregateSpec{},
	}
}

// LQL-TEST-025: count_over_time — 2 rows per 1-minute bucket across 3 buckets.
func TestMetrics_CountOverTime(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	// 2 records per bucket, 3 buckets
	for bucket := range 3 {
		for j := range 2 {
			ts := uint64(startNS + int64(bucket)*minuteNS + int64(j)*1_000) //nolint:gosec
			require.NoError(t, w.AddLogsData(makeLogRecord("svc", "hello", nil, ts)))
		}
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 3)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, nil, &qs, "count_over_time", nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// 3 buckets × 1 group key = 3 rows
	require.Len(t, result.Rows, 3)
	for i, row := range result.Rows {
		assert.Equal(t, float64(2), row.Values["count_over_time"], "bucket %d should have count 2", i)
	}
}

// LQL-TEST-026: rate — count/stepSeconds per bucket.
func TestMetrics_Rate(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	for bucket := range 2 {
		for j := range 2 {
			ts := uint64(startNS + int64(bucket)*minuteNS + int64(j)*1_000) //nolint:gosec
			require.NoError(t, w.AddLogsData(makeLogRecord("svc", "hello", nil, ts)))
		}
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 2)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, nil, &qs, "rate", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 2)

	expected := 2.0 / 60.0
	for i, row := range result.Rows {
		assert.InDelta(t, expected, row.Values["rate"], 1e-9, "bucket %d rate mismatch", i)
	}
}

// LQL-TEST-027: bytes_over_time — sum byte lengths per bucket.
func TestMetrics_BytesOverTime(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	bodies := []string{"hello", "world!", "x"}
	wantBytes := 0
	for i, body := range bodies {
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
		wantBytes += len(body)
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, nil, &qs, "bytes_over_time", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	assert.Equal(t, float64(wantBytes), result.Rows[0].Values["bytes_over_time"])
}

// LQL-TEST-028: sum_over_time with unwrap — sum unwrapped values.
func TestMetrics_SumOverTime(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	durations := []int{10, 20, 30}
	for i, d := range durations {
		body := fmt.Sprintf("duration=%d level=info", d)
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	// Build pipeline: logfmt | unwrap duration
	stages := []logqlparser.PipelineStage{
		{Type: logqlparser.StageLogfmt},
		{Type: logqlparser.StageUnwrap, Params: []string{"duration"}},
	}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, pipeline, &qs, "sum_over_time", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	assert.Equal(t, float64(60), result.Rows[0].Values["sum_over_time"])
}

// LQL-TEST-029: count_over_time grouped by level label.
func TestMetrics_GroupBy(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	// 2 error, 3 info
	for i := range 2 {
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		body := fmt.Sprintf("level=error msg=fail%d", i)
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
	}
	for i := range 3 {
		ts := uint64(startNS + int64(i+2)*1_000) //nolint:gosec
		body := fmt.Sprintf("level=info msg=ok%d", i)
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	stages := []logqlparser.PipelineStage{{Type: logqlparser.StageLogfmt}}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, pipeline, &qs, "count_over_time", []string{"level"})
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have 2 groups: error (count=2), info (count=3)
	require.Len(t, result.Rows, 2, "expected 2 group rows")

	counts := make(map[string]float64)
	for _, row := range result.Rows {
		// GroupKey[0] = bucketIdx, GroupKey[1] = level value
		if len(row.GroupKey) >= 2 {
			counts[row.GroupKey[1]] = row.Values["count_over_time"]
		}
	}
	assert.Equal(t, float64(2), counts["error"])
	assert.Equal(t, float64(3), counts["info"])
}

// TestMetrics_NilReader returns empty result without error.
func TestMetrics_NilReader(t *testing.T) {
	qs := makeTimeBucketSpec(1_000_000_000, 60_000_000_000, 1)
	result, err := modules_executor.ExecuteLogMetrics(nil, nil, nil, &qs, "count_over_time", nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Rows)
}

// TestMetrics_NilQuerySpec returns error.
func TestMetrics_NilQuerySpec(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "hello", nil, 1_000_000_000)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{}`)
	_, err = modules_executor.ExecuteLogMetrics(r, prog, nil, nil, "count_over_time", nil)
	assert.Error(t, err)
}

// TestMetrics_BytesRate verifies rate calculation for bytes_rate.
func TestMetrics_BytesRate(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	// 2 rows with 5-byte bodies in 1 bucket: total = 10 bytes
	for i := range 2 {
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", "hello", nil, ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, nil, &qs, "bytes_rate", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	// 10 bytes / 60 seconds
	expected := 10.0 / 60.0
	assert.InDelta(t, expected, result.Rows[0].Values["bytes_rate"], 1e-9)
}

// TestMetrics_AvgOverTime verifies average unwrap aggregation.
func TestMetrics_AvgOverTime(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	// 3 records with duration 10, 20, 30 → avg = 20
	for i, d := range []int{10, 20, 30} {
		body := fmt.Sprintf("duration=%d", d)
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	stages := []logqlparser.PipelineStage{
		{Type: logqlparser.StageLogfmt},
		{Type: logqlparser.StageUnwrap, Params: []string{"duration"}},
	}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, pipeline, &qs, "avg_over_time", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	assert.InDelta(t, 20.0, result.Rows[0].Values["avg_over_time"], 1e-9)
}

// TestMetrics_MinMaxOverTime verifies min/max unwrap aggregation.
func TestMetrics_MinMaxOverTime(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	for i, d := range []int{5, 15, 3, 99, 42} {
		body := fmt.Sprintf("val=%d", d)
		ts := uint64(startNS + int64(i)*1_000) //nolint:gosec
		require.NoError(t, w.AddLogsData(makeLogRecord("svc", body, nil, ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	stages := []logqlparser.PipelineStage{
		{Type: logqlparser.StageLogfmt},
		{Type: logqlparser.StageUnwrap, Params: []string{"val"}},
	}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	resultMin, errMin := modules_executor.ExecuteLogMetrics(r, prog, pipeline, &qs, "min_over_time", nil)
	require.NoError(t, errMin)
	require.Len(t, resultMin.Rows, 1)
	assert.Equal(t, 3.0, resultMin.Rows[0].Values["min_over_time"])

	resultMax, errMax := modules_executor.ExecuteLogMetrics(r, prog, pipeline, &qs, "max_over_time", nil)
	require.NoError(t, errMax)
	require.Len(t, resultMax.Rows, 1)
	assert.Equal(t, 99.0, resultMax.Rows[0].Values["max_over_time"])
}

// TestMetrics_RowsOutsideTimeRange are not counted.
func TestMetrics_RowsOutsideTimeRange(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 0)

	const minuteNS = int64(60_000_000_000)
	startNS := int64(1_000_000_000_000)

	// 1 row inside the range
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "inside", nil, uint64(startNS+1_000)))) //nolint:gosec
	// 1 row before the range
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "before", nil, uint64(startNS-minuteNS)))) //nolint:gosec
	// 1 row exactly at end (exclusive)
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "at-end", nil, uint64(startNS+minuteNS)))) //nolint:gosec

	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)
	qs := makeTimeBucketSpec(startNS, minuteNS, 1)

	result, err := modules_executor.ExecuteLogMetrics(r, prog, nil, &qs, "count_over_time", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, float64(1), result.Rows[0].Values["count_over_time"])
}
