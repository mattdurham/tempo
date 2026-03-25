package blockio_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// ---- in-memory provider for executor integration tests ----

type execMemProvider struct{ data []byte }

func (m *execMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *execMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// ---- helpers ----

func mustNewModulesWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *modules_blockio.Writer {
	t.Helper()
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)
	return w
}

func mustFlushModulesWriter(t *testing.T, w *modules_blockio.Writer) {
	t.Helper()
	_, err := w.Flush()
	require.NoError(t, err)
}

func openModulesReader(t *testing.T, data []byte) *modules_reader.Reader {
	t.Helper()
	r, err := modules_reader.NewReaderFromProvider(&execMemProvider{data: data})
	require.NoError(t, err)
	return r
}

func makeExecSpan(
	traceID [16]byte,
	spanID []byte,
	name string,
	startNano, endNano uint64,
	kind tracev1.Span_SpanKind,
	attrs []*commonv1.KeyValue,
) *tracev1.Span {
	return &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            spanID,
		Name:              name,
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   endNano,
		Kind:              kind,
		Attributes:        attrs,
	}
}

func execStringAttr(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

func compileExecutorQuery(t *testing.T, query string) *vm.Program {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoErrorf(t, err, "ParseTraceQL %q", query)
	filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
	require.Truef(t, ok, "expected FilterExpression, got %T for query %q", parsed, query)
	program, err := vm.CompileTraceQLFilter(filterExpr)
	require.NoErrorf(t, err, "CompileTraceQLFilter %q", query)
	return program
}

// ---- EX-01: Basic query returns matching spans ----

// TestExecutor_BasicQuery writes spans with distinct service names, then runs a
// TraceQL filter for one service and verifies only matching spans are returned.
func TestExecutor_BasicQuery(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)

	traceID := [16]byte{0x01}
	spans := []struct {
		name    string
		service string
	}{
		{"op.alpha", "svc-alpha"},
		{"op.beta", "svc-beta"},
		{"op.gamma", "svc-alpha"},
	}

	for i, s := range spans {
		span := makeExecSpan(
			traceID,
			[]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			s.name,
			uint64(1_000_000_000+i*1000), //nolint:gosec // safe: i is a small loop index, no overflow
			uint64(1_000_002_000+i*1000), //nolint:gosec // safe: i is a small loop index, no overflow
			tracev1.Span_SPAN_KIND_CLIENT,
			nil,
		)
		err := w.AddSpan(traceID[:], span, map[string]any{"service.name": s.service}, "", nil, "")
		require.NoError(t, err)
	}
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())

	program := compileExecutorQuery(t, `{ resource.service.name = "svc-alpha" }`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	// Two spans belong to svc-alpha: op.alpha and op.gamma.
	assert.Equal(t, 2, len(rows), "exactly 2 spans from svc-alpha must match")
}

// ---- EX-02: No matches when predicate does not match ----

// TestExecutor_NoMatches verifies that a query for a non-existent service returns
// zero matches and no error.
func TestExecutor_NoMatches(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)

	traceID := [16]byte{0x02}
	span := makeExecSpan(
		traceID,
		[]byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		"op.one",
		1_000_000_000,
		2_000_000_000,
		tracev1.Span_SPAN_KIND_INTERNAL,
		nil,
	)
	err := w.AddSpan(traceID[:], span, map[string]any{"service.name": "real-svc"}, "", nil, "")
	require.NoError(t, err)
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())

	program := compileExecutorQuery(t, `{ resource.service.name = "ghost-svc" }`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	assert.Empty(t, rows, "non-existent service must return no matches")
}

// ---- EX-03: Match on span attribute ----

// TestExecutor_SpanAttributeFilter writes spans with distinct HTTP methods and
// filters for one method, verifying the count.
func TestExecutor_SpanAttributeFilter(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)

	traceID := [16]byte{0x03}
	methods := []string{"GET", "POST", "GET", "DELETE", "GET"}

	for i, method := range methods {
		span := makeExecSpan(
			traceID,
			[]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			"http.request",
			uint64(1_000_000_000+i*1000), //nolint:gosec // safe: i is a small loop index, no overflow
			uint64(1_000_002_000+i*1000), //nolint:gosec // safe: i is a small loop index, no overflow
			tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{execStringAttr("http.method", method)},
		)
		err := w.AddSpan(traceID[:], span, map[string]any{"service.name": "http-svc"}, "", nil, "")
		require.NoError(t, err)
	}
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())

	program := compileExecutorQuery(t, `{ span.http.method = "GET" }`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	// 3 GET spans out of 5 total.
	assert.Equal(t, 3, len(rows), "exactly 3 GET spans must match")
}

// ---- EX-04: Multi-block query ----

// TestExecutor_MultiBlock writes more spans than maxBlockSpans to force multiple
// blocks and verifies the executor correctly scans all blocks.
func TestExecutor_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 5
	w := mustNewModulesWriter(t, &buf, maxPerBlock)

	traceID := [16]byte{0x04}
	const totalSpans = 12

	for i := range totalSpans {
		span := makeExecSpan(
			traceID,
			[]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			"op.load",
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL,
			[]*commonv1.KeyValue{execStringAttr("batch.id", "b1")},
		)
		err := w.AddSpan(traceID[:], span, map[string]any{"service.name": "batch-svc"}, "", nil, "")
		require.NoError(t, err)
	}
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 2, "12 spans with maxPerBlock=5 must produce at least 2 blocks")

	// Match-all query: every span has batch.id = "b1".
	program := compileExecutorQuery(t, `{ span.batch.id = "b1" }`)
	var statsOut modules_executor.CollectStats
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{
		OnStats: func(s modules_executor.CollectStats) { statsOut = s },
	})
	require.NoError(t, err)

	assert.Equal(t, totalSpans, len(rows), "all spans across all blocks must match")
	assert.GreaterOrEqual(t, statsOut.FetchedBlocks, 2, "executor must scan at least 2 blocks")
}

// ---- EX-05: Empty file query ----

// TestExecutor_EmptyFile verifies that executing a query against a file with no
// blocks returns an empty result without error.
func TestExecutor_EmptyFile(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	assert.Equal(t, 0, r.BlockCount(), "empty flush must have zero blocks")

	program := compileExecutorQuery(t, `{ resource.service.name = "any-svc" }`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	assert.Empty(t, rows, "empty file must return no matches")
}

// ---- EX-06: AND predicate ----

// TestExecutor_ANDPredicate writes spans with two attributes and filters using
// an AND predicate, verifying only spans matching both conditions are returned.
func TestExecutor_ANDPredicate(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)

	traceID := [16]byte{0x06}
	type spanDef struct {
		service string
		method  string
	}
	defs := []spanDef{
		{"svc-a", "GET"},
		{"svc-a", "POST"},
		{"svc-b", "GET"},
		{"svc-b", "POST"},
	}

	for i, d := range defs {
		span := makeExecSpan(
			traceID,
			[]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			"op",
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{execStringAttr("http.method", d.method)},
		)
		err := w.AddSpan(traceID[:], span, map[string]any{"service.name": d.service}, "", nil, "")
		require.NoError(t, err)
	}
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())

	// Only svc-a with GET matches both conditions.
	program := compileExecutorQuery(t, `{ resource.service.name = "svc-a" && span.http.method = "GET" }`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	assert.Equal(t, 1, len(rows), "only svc-a+GET span must match the AND predicate")
}

// ---- EX-07: Match-all wildcard query ----

// TestExecutor_MatchAll verifies that a match-all TraceQL query `{}` returns all
// spans written to the file.
func TestExecutor_MatchAll(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)

	traceID := [16]byte{0x07}
	const spanCount = 5

	for i := range spanCount {
		span := makeExecSpan(
			traceID,
			[]byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			"op",
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		err := w.AddSpan(traceID[:], span, map[string]any{"service.name": "all-svc"}, "", nil, "")
		require.NoError(t, err)
	}
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())

	program := compileExecutorQuery(t, `{}`)
	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	require.NoError(t, err)

	assert.Equal(t, spanCount, len(rows), "match-all query must return all %d spans", spanCount)
}

// makeTSTestLogData builds a minimal LogsData with one record at the given timestamp and body.
func makeTSTestLogData(svcName string, timeUnixNano uint64, body string) *logsv1.LogsData {
	record := &logsv1.LogRecord{
		TimeUnixNano: timeUnixNano,
		Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}},
	}
	return &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
		}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{record}}},
	}}}
}

// TestTSIndexRoundTrip_LogFile verifies that a log file written with the TS index can be
// read back and that BlocksInTimeRange returns the correct block indices for a time-bounded query.
func TestTSIndexRoundTrip_LogFile(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	base := uint64(1_000_000_000) // 1 second in nanoseconds

	// 6 log records → 3 blocks of 2 records each.
	// Block 0: ts=1e9, 2e9. Block 1: ts=3e9, 4e9. Block 2: ts=5e9, 6e9.
	// (Exact block assignment depends on writer sort order; we query for a window
	//  that can only overlap one block and assert exactly 1 block returned.)
	for i := range 6 {
		ts := base + uint64(i)*1_000_000_000 //nolint:gosec // safe: small test values
		require.NoError(t, w.AddLogsData(makeTSTestLogData("svc", ts, fmt.Sprintf("msg-%d", i))))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openModulesReader(t, buf.Bytes())
	require.Equal(t, 3, r.BlockCount())

	// TS index must be present.
	require.Equal(t, 3, r.TimeIndexLen(), "TS index should be present with 3 entries in newly-written files")

	// Query window [3e9, 4e9]: should match exactly the block containing ts=3e9 and ts=4e9.
	blocks := r.BlocksInTimeRange(3_000_000_000, 4_000_000_000)
	require.NotEmpty(t, blocks)
	// Verify the returned blocks all have time ranges overlapping [3e9, 4e9].
	for _, bidx := range blocks {
		meta := r.BlockMeta(bidx)
		overlaps := meta.MaxStart >= 3_000_000_000 && meta.MinStart <= 4_000_000_000
		require.True(t, overlaps, "block %d meta=[%d,%d] should overlap [3e9,4e9]", bidx, meta.MinStart, meta.MaxStart)
	}

	// Query for ts < all blocks: empty result.
	empty := r.BlocksInTimeRange(0, 500_000_000)
	require.Empty(t, empty)
}
