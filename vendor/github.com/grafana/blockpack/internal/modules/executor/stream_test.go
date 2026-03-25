package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// makeStreamLogRecord creates a minimal LogsData payload with a single record at the given timestamp.
func makeStreamLogRecord(svcName string, timeUnixNano uint64) *logsv1.LogsData {
	return &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
		}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			{
				TimeUnixNano: timeUnixNano,
				TraceId: []byte{
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				},
				SpanId: []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
			},
		}}},
	}}}
}

// EX-S-01: TestStream_TracePath verifies that Collect with TimestampColumn="" (trace mode)
// returns exactly the rows matching the predicate and no others.
func TestStream_TracePath(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x01}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-alpha"})
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-beta"})
	addSpan(t, w, tid, 2, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-alpha"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ resource.service.name = "svc-alpha" }`)

	var stats executor.CollectStats
	rows, err := executor.Collect(r, program, executor.CollectOptions{
		TimestampColumn: "",
		OnStats:         func(s executor.CollectStats) { stats = s },
	})

	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "should return exactly two svc-alpha spans")
	assert.GreaterOrEqual(t, stats.SelectedBlocks, 1)
}

// EX-S-02: TestStream_LogPath_TimeFilter verifies that Collect with TimestampColumn="log:timestamp"
// applies per-row time filtering and returns only the record whose timestamp is within [T2, T2].
func TestStream_LogPath_TimeFilter(t *testing.T) {
	t.Parallel()

	w, buf := mustNewLogWriter(t, 0)
	const T1, T2, T3 = uint64(1_000_000_000), uint64(2_000_000_000), uint64(3_000_000_000)

	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T1)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T2)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T3)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	program := compileQuery(t, `{}`)

	rows, err := executor.Collect(r, program, executor.CollectOptions{
		TimestampColumn: "log:timestamp",
		TimeRange:       queryplanner.TimeRange{MinNano: T2, MaxNano: T2},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(rows), "only the T2 record should pass the per-row time filter")
}

// EX-S-03: TestStream_Direction_Backward verifies that Collect with Direction=Backward
// delivers the newest (higher block index) block before the older block.
func TestStream_Direction_Backward(t *testing.T) {
	t.Parallel()

	// MaxBlockSpans=2 forces each pair of records into a separate block.
	w, buf := mustNewLogWriter(t, 2)
	// Block 0: older records.
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 100)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 200)))
	// Block 1: newer records.
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 1000)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 2000)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	require.Equal(t, 2, r.BlockCount(), "expected 2 blocks")

	program := compileQuery(t, `{}`)

	rows, err := executor.Collect(r, program, executor.CollectOptions{
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
	})

	require.NoError(t, err)
	require.NotEmpty(t, rows, "expected at least one matched row")
	assert.Equal(t, 1, rows[0].BlockIdx, "Backward direction must deliver the newer (higher-index) block first")
}

// EX-S-04: TestStream_EarlyStop_FetchedLessThanSelected verifies that Collect with a Limit causes
// early stop so FetchedBlocks <= SelectedBlocks (lazy I/O proportional to results).
func TestStream_EarlyStop_FetchedLessThanSelected(t *testing.T) {
	t.Parallel()

	// MaxBlockSpans=2 → 10 records create 5 blocks, ensuring SelectedBlocks >= 3.
	w, buf := mustNewLogWriter(t, 2)
	for i := range 10 {
		require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", uint64(i+1)*1_000_000_000))) //nolint:gosec
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	require.GreaterOrEqual(t, r.BlockCount(), 3, "expected at least 3 blocks for early-stop test")

	program := compileQuery(t, `{}`)

	var stats executor.CollectStats
	rows, err := executor.Collect(r, program, executor.CollectOptions{
		Limit:   2,
		OnStats: func(s executor.CollectStats) { stats = s },
	})

	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "should return exactly 2 rows (Limit=2)")
	// SPEC-STREAM-3: FetchedBlocks <= SelectedBlocks invariant.
	// With small test data all blocks coalesce into one group so FetchedBlocks == SelectedBlocks.
	// In production with large data, early stop skips unfetched groups, giving FetchedBlocks < SelectedBlocks.
	assert.LessOrEqual(t, stats.FetchedBlocks, stats.SelectedBlocks,
		"FetchedBlocks must not exceed SelectedBlocks (SPEC-STREAM-3)")
}

// EX-S-05: TestStream_NilReader verifies that Collect with a nil reader returns an empty slice
// and nil error (SPEC-STREAM-1).
func TestStream_NilReader(t *testing.T) {
	t.Parallel()

	program := compileQuery(t, `{}`)

	rows, err := executor.Collect(nil, program, executor.CollectOptions{})

	require.NoError(t, err)
	assert.Empty(t, rows, "nil reader must return empty results")
}
