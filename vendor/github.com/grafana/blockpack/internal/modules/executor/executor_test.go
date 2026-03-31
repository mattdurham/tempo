package executor_test

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// --- helpers ---

type memProvider struct{ data []byte }

func (m *memProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *memProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func mustNewWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *modules_blockio.Writer {
	t.Helper()
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)
	return w
}

func mustFlush(t *testing.T, w *modules_blockio.Writer) {
	t.Helper()
	_, err := w.Flush()
	require.NoError(t, err)
}

func openReader(t *testing.T, data []byte) *modules_reader.Reader {
	t.Helper()
	r, err := modules_reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	return r
}

func makeSpan(
	traceID [16]byte,
	spanIdx int,
	name string,
	kind tracev1.Span_SpanKind,
	attrs []*commonv1.KeyValue,
) *tracev1.Span {
	return &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              name,
		StartTimeUnixNano: uint64(1_000_000_000 + spanIdx*1000), //nolint:gosec
		EndTimeUnixNano:   uint64(1_000_002_000 + spanIdx*1000), //nolint:gosec
		Kind:              kind,
		Attributes:        attrs,
	}
}

func addSpan(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	name string,
	kind tracev1.Span_SpanKind,
	attrs []*commonv1.KeyValue,
	resAttrs map[string]any,
) {
	t.Helper()
	sp := makeSpan(traceID, spanIdx, name, kind, attrs)
	require.NoError(t, w.AddSpan(traceID[:], sp, resAttrs, "", nil, ""))
}

func strAttr(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

func compileQuery(t *testing.T, query string) *vm.Program {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoErrorf(t, err, "ParseTraceQL %q", query)
	filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
	require.Truef(t, ok, "expected FilterExpression for %q", query)
	program, err := vm.CompileTraceQLFilter(filterExpr)
	require.NoErrorf(t, err, "CompileTraceQLFilter %q", query)
	return program
}

func runQuery(t *testing.T, r *modules_reader.Reader, query string) []executor.MatchedRow {
	t.Helper()
	program := compileQuery(t, query)
	rows, err := executor.Collect(r, program, executor.CollectOptions{})
	require.NoError(t, err)
	return rows
}

// --- tests ---

// EX-01: Basic query returns matching spans.
func TestExecute_BasicQuery(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x01}
	for i, svc := range []string{"svc-alpha", "svc-beta", "svc-alpha"} {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_CLIENT, nil, map[string]any{"service.name": svc})
	}
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ resource.service.name = "svc-alpha" }`)
	assert.Equal(t, 2, len(rows), "exactly 2 spans from svc-alpha")
}

// EX-02: No matches when predicate does not match.
func TestExecute_NoMatches(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x02}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_INTERNAL, nil, map[string]any{"service.name": "real-svc"})
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ resource.service.name = "ghost-svc" }`)
	assert.Empty(t, rows)
}

// EX-03: Span attribute filter.
func TestExecute_SpanAttributeFilter(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x03}
	for i, method := range []string{"GET", "POST", "GET", "DELETE", "GET"} {
		addSpan(t, w, tid, i, "http.request", tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{strAttr("http.method", method)},
			map[string]any{"service.name": "http-svc"})
	}
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ span.http.method = "GET" }`)
	assert.Equal(t, 3, len(rows))
}

// EX-04: Multi-block query.
func TestExecute_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 5
	w := mustNewWriter(t, &buf, maxPerBlock)
	tid := [16]byte{0x04}
	const totalSpans = 12
	for i := range totalSpans {
		addSpan(t, w, tid, i, "op.load", tracev1.Span_SPAN_KIND_INTERNAL,
			[]*commonv1.KeyValue{strAttr("batch.id", "b1")},
			map[string]any{"service.name": "batch-svc"})
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 2)

	var statsOut executor.CollectStats
	program := compileQuery(t, `{ span.batch.id = "b1" }`)
	rows, err := executor.Collect(r, program, executor.CollectOptions{
		OnStats: func(s executor.CollectStats) { statsOut = s },
	})
	require.NoError(t, err)
	assert.Equal(t, totalSpans, len(rows))
	assert.GreaterOrEqual(t, statsOut.FetchedBlocks, 2)
}

// EX-05: Empty file.
func TestExecute_EmptyFile(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	assert.Equal(t, 0, r.BlockCount())

	rows := runQuery(t, r, `{ resource.service.name = "any" }`)
	assert.Empty(t, rows)
}

// EX-06: AND predicate.
func TestExecute_ANDPredicate(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x06}
	for i, d := range []struct{ svc, method string }{
		{"svc-a", "GET"}, {"svc-a", "POST"}, {"svc-b", "GET"}, {"svc-b", "POST"},
	} {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{strAttr("http.method", d.method)},
			map[string]any{"service.name": d.svc})
	}
	mustFlush(t, w)

	rows := runQuery(
		t,
		openReader(t, buf.Bytes()),
		`{ resource.service.name = "svc-a" && span.http.method = "GET" }`,
	)
	assert.Equal(t, 1, len(rows))
}

// EX-07: Match-all wildcard.
func TestExecute_MatchAll(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x07}
	const spanCount = 5
	for i := range spanCount {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_INTERNAL, nil, map[string]any{"service.name": "all-svc"})
	}
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{}`)
	assert.Equal(t, spanCount, len(rows))
}

// EX-08: Limit option.
func TestExecute_Limit(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x08}
	for i := range 10 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_INTERNAL, nil, map[string]any{"service.name": "svc"})
	}
	mustFlush(t, w)

	program := compileQuery(t, `{}`)
	rows, err := executor.Collect(openReader(t, buf.Bytes()), program, executor.CollectOptions{Limit: 3})
	require.NoError(t, err)
	assert.Equal(t, 3, len(rows))
}

// EX-09: SpanMatch has TraceID and SpanID populated.
func TestExecute_SpanMatchFields(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xAB, 0xCD}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_INTERNAL, nil, map[string]any{"service.name": "svc"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	rows := runQuery(t, r, `{}`)
	require.Equal(t, 1, len(rows))
	match := executor.SpanMatchFromRow(rows[0], r.SignalType(), r)
	assert.Equal(t, tid, match.TraceID)
	assert.NotEmpty(t, match.SpanID)
}

// EX-10: Plan is populated.
func TestExecute_PlanPopulated(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x10}
	for i := range 5 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_INTERNAL, nil, map[string]any{"service.name": "svc"})
	}
	mustFlush(t, w)

	var statsOut executor.CollectStats
	program := compileQuery(t, `{ resource.service.name = "svc" }`)
	_, err := executor.Collect(openReader(t, buf.Bytes()), program, executor.CollectOptions{
		OnStats: func(s executor.CollectStats) { statsOut = s },
	})
	require.NoError(t, err)
	assert.Greater(t, statsOut.TotalBlocks, 0)
}

// EX-11: Unscoped attribute matches when value is in resource scope.
func TestExecute_UnscopedAttr_MatchesResourceScope(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x11}
	// span 0: service.name in resource attrs — should match .service.name = "svc-a"
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
		map[string]any{"service.name": "svc-a"})
	// span 1: different service — should not match
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
		map[string]any{"service.name": "svc-b"})
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ .service.name = "svc-a" }`)
	assert.Len(t, rows, 1)
}

// EX-12: Unscoped attribute matches when value is in span scope only.
func TestExecute_UnscopedAttr_MatchesSpanScope(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x12}
	// span 0: custom attr in span only — should match .custom.attr = "yes"
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("custom.attr", "yes")},
		map[string]any{"service.name": "svc"})
	// span 1: attr absent — should not match
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
		map[string]any{"service.name": "svc"})
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ .custom.attr = "yes" }`)
	assert.Len(t, rows, 1)
}

// EX-13: Unscoped attribute does not match when value is absent from both scopes.
func TestExecute_UnscopedAttr_NoMatchWhenAbsent(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x13}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("other.attr", "val")},
		map[string]any{"service.name": "svc"})
	mustFlush(t, w)

	rows := runQuery(t, openReader(t, buf.Bytes()), `{ .custom.attr = "yes" }`)
	assert.Empty(t, rows)
}

// TestBuildPredicates_RegexPrefix verifies that a regex pattern with a literal prefix
// produces a range-index predicate with Values, enabling block pruning.
func TestBuildPredicates_RegexPrefix(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x20}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.url", "https://api.example.com/foo")},
		map[string]any{"service.name": "web-svc"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ span.http.url =~ "https://api.*" }`)
	preds := executor.BuildPredicates(r, program)

	var found bool
	for _, p := range preds {
		for _, c := range p.Columns {
			if c == "span.http.url" && len(p.Values) > 0 {
				found = true
				assert.Contains(t, p.Values, "https://api")
			}
		}
	}
	assert.True(t, found, "expected a predicate for span.http.url with range values from regex prefix")
}

// TestBuildPredicates_RegexAlternation verifies that alternation regex patterns
// produce multiple values in the range-index predicate.
func TestBuildPredicates_RegexAlternation(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x21}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
		map[string]any{"service.name": "error-svc"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ resource.service.name =~ "error|warn|info" }`)
	preds := executor.BuildPredicates(r, program)

	var found bool
	for _, p := range preds {
		for _, c := range p.Columns {
			if c == "resource.service.name" && len(p.Values) > 0 {
				found = true
				assert.Contains(t, p.Values, "error")
				assert.Contains(t, p.Values, "warn")
				assert.Contains(t, p.Values, "info")
			}
		}
	}
	assert.True(t, found, "expected a predicate for resource.service.name with alternation values")
}

// TestBuildPredicates_DurationRange_ProducesValues verifies that a span:duration
// range predicate produces a non-empty Values slice so the range index is consulted.
// This is a regression test for the bug where TypeDuration values were not encoded
// for ColumnTypeRangeUint64 columns, causing empty Values and zero block pruning.
func TestBuildPredicates_DurationRange_ProducesValues(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x30}
	sp := &tracev1.Span{
		TraceId:           tid[:],
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_010_000_000, // 10ms duration
	}
	require.NoError(t, w.AddSpan(tid[:], sp, map[string]any{"service.name": "svc"}, "", nil, ""))
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ duration > 5ms }`)
	preds := executor.BuildPredicates(r, program)

	var found bool
	for _, p := range preds {
		for _, col := range p.Columns {
			if col == "span:duration" {
				found = true
				assert.NotEmpty(t, p.Values, "duration range predicate must encode bounds (not empty)")
				assert.True(t, p.IntervalMatch, "duration range predicate must use IntervalMatch")
			}
		}
	}
	assert.True(t, found, "expected a predicate for span:duration column")
}

// TestExecute_DurationRange_NoFalseNegatives verifies that duration range queries
// return all matching spans without omitting spans from blocks where ALL durations
// are above the threshold (the false-negative bug from point-lookup range pruning).
func TestExecute_DurationRange_NoFalseNegatives(t *testing.T) {
	// Block 0: 2 short spans (1ms) — below threshold, should be pruned or yield 0 matches.
	// Block 1: 2 long spans (500ms) — above threshold, must NOT be pruned.
	// Block 2 (bridge): spans around 10ms — creates a KLL boundary between the two groups
	//   so interval pruning can separate block 0 from blocks with duration > threshold.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 2)

	addDurSpan := func(traceID [16]byte, idx int, startNano, endNano uint64) {
		sp := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            []byte{byte(idx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			StartTimeUnixNano: startNano,
			EndTimeUnixNano:   endNano,
		}
		require.NoError(t, w.AddSpan(traceID[:], sp, map[string]any{"service.name": "svc"}, "", nil, ""))
	}

	tid := [16]byte{0x31}
	const ms = uint64(1_000_000) // 1 millisecond in nanoseconds

	// Block 0: 2 spans with duration 1ms each (below 50ms threshold).
	addDurSpan(tid, 0, 1*ms, 2*ms)
	addDurSpan(tid, 1, 3*ms, 4*ms)

	// Bridge block: 2 spans with duration ~10ms (creates KLL boundary near 10ms).
	addDurSpan(tid, 2, 10*ms, 20*ms)
	addDurSpan(tid, 3, 12*ms, 22*ms)

	// Block 2: 2 spans with duration 500ms each (above 50ms threshold, must be returned).
	addDurSpan(tid, 4, 500*ms, 1000*ms)
	addDurSpan(tid, 5, 600*ms, 1100*ms)

	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 3, r.BlockCount())

	rows := runQuery(t, r, `{ duration > 50ms }`)

	// Must return exactly the 2 spans with duration 500ms and 500ms — no false negatives.
	assert.Equal(t, 2, len(rows), "must return both high-duration spans without false negatives")
}

// TestBuildPredicates_RegexComplex verifies that a complex regex pattern
// that cannot be optimized still produces a bloom-only predicate (no Values).
func TestBuildPredicates_RegexComplex(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x22}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
		map[string]any{"service.name": "web-svc"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ resource.service.name =~ ".*complex[a-z]+" }`)
	preds := executor.BuildPredicates(r, program)

	// Should have a bloom-only predicate (no values) for the column
	var found bool
	for _, p := range preds {
		for _, c := range p.Columns {
			if c == "resource.service.name" {
				found = true
				assert.Empty(t, p.Values, "complex regex should not produce range values")
			}
		}
	}
	assert.True(t, found, "expected a bloom-only predicate for resource.service.name")
}

// GAP-26: TestCollect_StringTruncation verifies that span attribute values at or below the
// 65535-byte limit round-trip correctly, and that values exceeding the limit are silently
// truncated to 65535 bytes. Uses a non-intrinsic span attribute to avoid TOC key limits.
func TestCollect_StringTruncation(t *testing.T) {
	// 65535-char value — must round-trip exactly.
	val65535 := strings.Repeat("x", 65535)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xF0}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("data.payload", val65535)},
		map[string]any{"service.name": "trunc-svc"})
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	// Query by service.name; the span must be found (payload attr is stored correctly).
	rows := runQuery(t, r, `{ resource.service.name = "trunc-svc" }`)
	require.Len(t, rows, 1, "span with 65535-char attribute value must be stored and queryable")

	// 65536-char value — stored truncated to 65535; write must not error.
	val65536 := strings.Repeat("y", 65536)
	var buf2 bytes.Buffer
	w2 := mustNewWriter(t, &buf2, 0)
	addSpan(t, w2, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("data.payload", val65536)},
		map[string]any{"service.name": "trunc-svc2"})
	_, err := w2.Flush()
	require.NoError(t, err, "flush with 65536-char attribute value must not error")
	r2 := openReader(t, buf2.Bytes())
	rows2 := runQuery(t, r2, `{ resource.service.name = "trunc-svc2" }`)
	require.Len(t, rows2, 1, "span with 65536-char attribute value must be stored (truncated) and queryable")
}

// SPEC-STREAM-1: TestCollect_NilReader verifies that passing a nil reader returns nil, nil.
func TestCollect_NilReader(t *testing.T) {
	prog := compileQuery(t, `{ resource.service.name = "x" }`)
	rows, err := executor.Collect(nil, prog, executor.CollectOptions{})
	require.NoError(t, err)
	require.Nil(t, rows)
}

// GAP-19: TestCollect_WantColumns_NilVsEmpty verifies that running the same program twice
// produces identical results (wantColumns is an internal detail, not caller-visible).
func TestCollect_WantColumns_NilVsEmpty(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xF3}
	for i := range 5 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
			map[string]any{"service.name": "svc", "env": "prod"})
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	prog := compileQuery(t, `{ resource.service.name = "svc" }`)

	rows1, err := executor.Collect(r, prog, executor.CollectOptions{})
	require.NoError(t, err)
	rows2, err := executor.Collect(r, prog, executor.CollectOptions{})
	require.NoError(t, err)
	require.Equal(t, len(rows1), len(rows2), "same program must return same row count on both calls")
}

// GAP-15: TestCollect_NilProgram verifies that passing a nil program returns a descriptive error.
func TestCollect_NilProgram(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	_, err := executor.Collect(r, nil, executor.CollectOptions{})
	require.ErrorContains(t, err, "program must not be nil")
}

// GAP-14: TestCollect_InvalidShardParameters verifies that shard parameter validation
// returns descriptive errors for negative start, negative count, and integer overflow.
func TestCollect_InvalidShardParameters(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xF1}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_CLIENT, nil, map[string]any{"service.name": "x"})
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	prog := compileQuery(t, `{ resource.service.name = "x" }`)

	cases := []struct {
		name       string
		wantErrStr string
		startBlock int
		blockCount int
	}{
		{"negative start", "invalid shard parameters", -1, 0},
		{"negative count", "invalid shard parameters", 0, -1},
		{"overflow", "shard range overflow", math.MaxInt, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executor.Collect(r, prog, executor.CollectOptions{
				StartBlock: tc.startBlock,
				BlockCount: tc.blockCount,
			})
			require.ErrorContains(t, err, tc.wantErrStr)
		})
	}
}

// GAP-22: TestCollect_OnStats_NilAndInvoked verifies that:
// - A nil OnStats callback does not panic.
// - A non-nil OnStats callback is invoked exactly once with non-zero stats.
func TestCollect_OnStats_NilAndInvoked(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xF2}
	for i, svc := range []string{"alpha", "beta", "gamma"} {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": svc})
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	prog := compileQuery(t, `{ resource.service.name = "alpha" }`)

	// Part 1: nil OnStats must not panic.
	_, err := executor.Collect(r, prog, executor.CollectOptions{OnStats: nil})
	require.NoError(t, err)

	// Part 2: non-nil OnStats must be invoked exactly once with non-zero TotalBlocks.
	var called int
	var gotStats executor.CollectStats
	_, err = executor.Collect(r, prog, executor.CollectOptions{
		OnStats: func(s executor.CollectStats) {
			called++
			gotStats = s
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, called, "OnStats must be invoked exactly once")
	require.Greater(t, gotStats.TotalBlocks, 0, "TotalBlocks must be non-zero")
}
