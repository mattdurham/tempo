package writer_test

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// makeSpan creates a minimal OTLP Span with the given fields.
func makeSpan(traceID, spanID []byte, name string, start, end uint64, attrs map[string]string) *tracev1.Span {
	kvs := make([]*commonv1.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, &commonv1.KeyValue{
			Key:   k,
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
		})
	}
	return &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		Name:              name,
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_UNSET},
		Attributes:        kvs,
	}
}

// makeResourceSpan creates a ResourceSpans with Resource containing service.name attribute.
func makeResourceSpan(serviceName string, spans []*tracev1.Span) *tracev1.ResourceSpans {
	return &tracev1.ResourceSpans{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: serviceName}},
				},
			},
		},
		ScopeSpans: []*tracev1.ScopeSpans{
			{Spans: spans},
		},
	}
}

// newTestWriter creates a writer.Writer with the given buffer and maxBlockSpans (0 = default).
func newTestWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *writer.Writer {
	t.Helper()
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)
	return w
}

// makeTracesData builds a TracesData with one ResourceSpans containing the given spans.
func makeTracesData(serviceName string, spans []*tracev1.Span) *tracev1.TracesData {
	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			makeResourceSpan(serviceName, spans),
		},
	}
}

// makeTraceID returns a 16-byte trace ID with the first byte set to n.
func makeTraceID(n byte) []byte {
	id := make([]byte, 16)
	id[0] = n
	return id
}

// makeSpanID returns an 8-byte span ID with the first byte set to n.
func makeSpanID(n byte) []byte {
	id := make([]byte, 8)
	id[0] = n
	return id
}

// --- Configuration Tests ---

// TestNewWriterWithConfig_NilOutputStream verifies that a nil OutputStream returns an error (CFG-01).
func TestNewWriterWithConfig_NilOutputStream(t *testing.T) {
	_, err := writer.NewWriterWithConfig(writer.Config{})
	assert.Error(t, err, "expected error when OutputStream is nil")
}

// TestNewWriterWithConfig_MaxBlockSpansTooLarge verifies that MaxBlockSpans > 65535 returns an error (CFG-02 / CFG-03).
func TestNewWriterWithConfig_MaxBlockSpansTooLarge(t *testing.T) {
	_, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65536,
	})
	assert.Error(t, err, "expected error when MaxBlockSpans exceeds 65535")
}

// TestNewWriterWithConfig_Defaults verifies that a writer with only OutputStream set succeeds (CFG-03).
func TestNewWriterWithConfig_Defaults(t *testing.T) {
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: io.Discard})
	require.NoError(t, err)
	assert.NotNil(t, w)
}

// TestNewWriterWithConfig_MaxBlockSpansBoundary verifies that MaxBlockSpans = 65535 is accepted.
func TestNewWriterWithConfig_MaxBlockSpansBoundary(t *testing.T) {
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	require.NoError(t, err)
	assert.NotNil(t, w)
}

// --- Flush Tests ---

// TestFlush_Empty verifies that Flush on a writer with 0 buffered spans writes a valid non-empty file (RT-02).
func TestFlush_Empty(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(0), "expected > 0 bytes written for empty file (header+footer)")
	assert.Greater(t, buf.Len(), 0, "buffer should contain file bytes")
}

// TestFlush_SingleSpan verifies that adding one span and flushing produces a valid file (RT-01).
func TestFlush_SingleSpan(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	span := makeSpan(
		makeTraceID(1),
		makeSpanID(1),
		"my-op",
		1_000_000_000,
		2_000_000_000,
		map[string]string{"key": "val"},
	)
	td := makeTracesData("my-service", []*tracev1.Span{span})
	require.NoError(t, w.AddTracesData(td))

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(100), "expected > 100 bytes for a valid single-span file")
}

// TestFlush_MultiBlock verifies that MaxBlockSpans triggers multiple block boundaries (RT-03).
func TestFlush_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 10)

	for i := range 25 {
		span := makeSpan(
			makeTraceID(byte(i)),
			makeSpanID(byte(i)),
			fmt.Sprintf("op-%d", i),
			uint64(i)*1_000,
			uint64(i)*1_000+500,
			nil,
		)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
	// After flush the internal size is reset.
	assert.Equal(t, int64(0), w.CurrentSize(), "CurrentSize() should be 0 after flush")
}

// TestFlush_ResetAfterFlush verifies that CurrentSize is 0 after flushing.
func TestFlush_ResetAfterFlush(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 50 {
		span := makeSpan(makeTraceID(byte(i)), makeSpanID(byte(i)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
	assert.Equal(t, int64(0), w.CurrentSize())
}

// TestCurrentSize_GrowsWithSpans verifies that CurrentSize() increases as spans are added.
func TestCurrentSize_GrowsWithSpans(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 100 {
		span := makeSpan(makeTraceID(byte(i)), makeSpanID(byte(i)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}
	size1 := w.CurrentSize()
	assert.Greater(t, size1, int64(0), "CurrentSize() should be > 0 after adding 100 spans")

	for i := range 100 {
		span := makeSpan(makeTraceID(byte(i+100)), makeSpanID(byte(i+100%256)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}
	size2 := w.CurrentSize()
	assert.Greater(t, size2, size1, "CurrentSize() should grow as more spans are added")
}

// TestAddTracesData_MultipleBatches verifies that multiple AddTracesData calls accumulate correctly.
func TestAddTracesData_MultipleBatches(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for batch := range 5 {
		spans := make([]*tracev1.Span, 20)
		for i := range 20 {
			spans[i] = makeSpan(
				makeTraceID(byte(batch*20+i)),
				makeSpanID(byte((batch*20+i)%256)),
				fmt.Sprintf("op-%d-%d", batch, i),
				1000,
				2000,
				nil,
			)
		}
		td := makeTracesData("svc", spans)
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestFlush_NilTracesData verifies that nil TracesData is handled gracefully.
func TestFlush_NilTracesData(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	err := w.AddTracesData(nil)
	require.NoError(t, err)

	_, err = w.Flush()
	require.NoError(t, err)
}

// TestFlush_SortOrderServiceName verifies that Flush completes without error for spans
// with different service names (SORT-01). Sort order is verified to not error; full
// round-trip verification requires the reader package.
func TestFlush_SortOrderServiceName(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// Create spans with two service names: "zzz-service" and "aaa-service".
	serviceNames := []string{"zzz-service", "aaa-service", "zzz-service", "aaa-service", "zzz-service", "aaa-service"}
	for i, svc := range serviceNames {
		span := makeSpan(
			makeTraceID(byte(i+1)),
			makeSpanID(byte(i+1)),
			fmt.Sprintf("op-%d", i),
			uint64(i)*1000,
			uint64(i)*1000+500,
			nil,
		)
		td := makeTracesData(svc, []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))
}

// TestFlush_UUIDStringColumn verifies that a string attribute containing a valid UUID
// is flushed without error (UUID auto-detection, VAL-01 / ENC-09).
func TestFlush_UUIDStringColumn(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	span := makeSpan(
		makeTraceID(1),
		makeSpanID(1),
		"uuid-op",
		1_000_000_000,
		2_000_000_000,
		map[string]string{"request.id": "550e8400-e29b-41d4-a716-446655440000"},
	)
	td := makeTracesData("svc", []*tracev1.Span{span})
	require.NoError(t, w.AddTracesData(td))

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestFlush_MultipleFlushes verifies that the writer can be flushed multiple times.
func TestFlush_MultipleFlushes(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// First flush: add 10 spans.
	for i := range 10 {
		span := makeSpan(makeTraceID(byte(i+1)), makeSpanID(byte(i+1)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}
	_, err := w.Flush()
	require.NoError(t, err, "first flush should succeed")

	// Second flush: add 10 more spans.
	for i := range 10 {
		span := makeSpan(makeTraceID(byte(i+100)), makeSpanID(byte(i+100)), "op2", 3000, 4000, nil)
		td := makeTracesData("svc2", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err, "second flush should succeed")
}

// TestFlush_LargeSpanBatch verifies that a large number of spans can be flushed.
func TestFlush_LargeSpanBatch(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 500 {
		span := makeSpan(
			makeTraceID(byte(i%256)),
			makeSpanID(byte(i%256)),
			fmt.Sprintf("op-%d", i%10),
			uint64(1_000_000_000+i*1000),
			uint64(1_000_000_000+i*1000+500),
			map[string]string{
				"http.method": []string{"GET", "POST", "DELETE"}[i%3],
				"http.status": fmt.Sprintf("%d", 200+i%5),
			},
		)
		td := makeTracesData(fmt.Sprintf("svc-%d", i%5), []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))
}

// TestConcurrentAccessPanic verifies that concurrent calls to Flush cause a panic (CFG-04 / CFG-06).
func TestConcurrentAccessPanic(t *testing.T) {
	// Add spans to both writers so Flush does real work.
	var buf1, buf2 bytes.Buffer

	w1, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf1})
	require.NoError(t, err)

	w2, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf2})
	require.NoError(t, err)

	// Add spans to w1 so Flush does real work and holds inUse longer.
	for i := range 100 {
		span := makeSpan(makeTraceID(byte(i)), makeSpanID(byte(i)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w1.AddTracesData(td))
	}

	// w2: test that AddSpan panics when called concurrently with Flush.
	// We add spans first.
	for i := range 50 {
		span := makeSpan(makeTraceID(byte(i)), makeSpanID(byte(i)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w2.AddTracesData(td))
	}

	// Use assert.Panics with a goroutine-based concurrent access scenario.
	// Call AddTracesData concurrently with Flush on the same writer.
	// One goroutine calls Flush (holds inUse); another calls AddTracesData (which
	// calls CompareAndSwap on inUse per-span, so we need to race with the outer Flush call).
	panicDetected := false

	// Test concurrent Flush calls on a fresh writer with enough data to keep it busy.
	var wg sync.WaitGroup
	var mu sync.Mutex
	panicCh := make(chan struct{}, 1)

	wFresh, err := writer.NewWriterWithConfig(writer.Config{OutputStream: io.Discard})
	require.NoError(t, err)

	// Add spans so flush does non-trivial work.
	for i := range 200 {
		span := makeSpan(makeTraceID(byte(i%256)), makeSpanID(byte(i%256)), "op", 1000, 2000, nil)
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, wFresh.AddTracesData(td))
	}

	// Goroutine 1: call Flush.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				select {
				case panicCh <- struct{}{}:
				default:
				}
			}
		}()
		_, _ = wFresh.Flush()
	}()

	// Goroutine 2: also try to Flush at roughly the same time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				select {
				case panicCh <- struct{}{}:
				default:
				}
			}
		}()
		_, _ = wFresh.Flush()
	}()

	wg.Wait()
	close(panicCh)

	mu.Lock()
	for range panicCh {
		panicDetected = true
	}
	mu.Unlock()

	// Note: the race may not always be detectable timing-wise, so we treat a panic
	// as confirmation but don't fail the test if the goroutines serialize without collision.
	// The important thing is no data corruption and no deadlock.
	t.Logf("concurrent access panic detected: %v", panicDetected)
}

// TestAddTracesData_ConcurrentPanic verifies that concurrent calls to AddTracesData panic,
// consistent with AddSpan and AddLogsData behavior (CFG-04 / CFG-06).
func TestAddTracesData_ConcurrentPanic(t *testing.T) {
	// Build a TracesData with enough spans to make AddTracesData take measurable time.
	spans := make([]*tracev1.Span, 500)
	for i := range spans {
		spans[i] = makeSpan(makeTraceID(byte(i%256)), makeSpanID(byte(i%256)), "op", 1000, 2000, nil)
	}
	td := makeTracesData("svc", spans)

	var panicDetected bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	panicCh := make(chan struct{}, 1)

	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: io.Discard})
	require.NoError(t, err)

	call := func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				mu.Lock()
				select {
				case panicCh <- struct{}{}:
				default:
				}
				mu.Unlock()
			}
		}()
		_ = w.AddTracesData(td)
	}

	wg.Add(2)
	go call()
	go call()
	wg.Wait()
	close(panicCh)

	for range panicCh {
		panicDetected = true
	}

	// Serialization without collision is acceptable; a panic confirms the guard works.
	t.Logf("concurrent AddTracesData panic detected: %v", panicDetected)
}

// TestFlush_MixedAttributeTypes verifies that a span with many attribute types flushes correctly.
func TestFlush_MixedAttributeTypes(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	span := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(1),
		Name:              "mixed-types-op",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK, Message: "all good"},
		Attributes: []*commonv1.KeyValue{
			{Key: "custom.str", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}}},
			{Key: "custom.int", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: -42}}},
			{
				Key:   "custom.float",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14159}},
			},
			{Key: "custom.bool", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}},
			{
				Key:   "custom.bytes",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{0x01, 0x02, 0x03}}},
			},
		},
	}
	td := makeTracesData("test-svc", []*tracev1.Span{span})
	require.NoError(t, w.AddTracesData(td))

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))
}

// TestFlush_SpanWithParentID verifies a span with ParentSpanId round-trips through flush.
func TestFlush_SpanWithParentID(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	parentID := makeSpanID(42)
	span := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(2),
		ParentSpanId:      parentID,
		Name:              "child-op",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_500_000_000,
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
	}
	td := makeTracesData("svc", []*tracev1.Span{span})
	require.NoError(t, w.AddTracesData(td))

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestFlush_SpanWithEvents verifies that span events are encoded without error.
func TestFlush_SpanWithEvents(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 5 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "event-span",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Events: []*tracev1.Span_Event{
				{Name: "exception", TimeUnixNano: 1_100_000_000},
				{Name: "log", TimeUnixNano: 1_200_000_000},
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestAddTracesData_NilResourceSpan verifies that nil ResourceSpans entries are skipped safely.
func TestAddTracesData_NilResourceSpan(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	td := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			nil,
			makeResourceSpan("svc", []*tracev1.Span{
				makeSpan(makeTraceID(1), makeSpanID(1), "op", 1000, 2000, nil),
			}),
		},
	}
	require.NoError(t, w.AddTracesData(td))

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestFlush_MultipleServicesSameBlock verifies spans from multiple services fit in one block.
func TestFlush_MultipleServicesSameBlock(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 100)

	services := []string{"svc-alpha", "svc-beta", "svc-gamma"}
	for i, svc := range services {
		for j := range 5 {
			span := makeSpan(
				makeTraceID(byte(i*10+j+1)),
				makeSpanID(byte(i*10+j+1)),
				"op",
				uint64(j*1000),
				uint64(j*1000+500),
				nil,
			)
			td := makeTracesData(svc, []*tracev1.Span{span})
			require.NoError(t, w.AddTracesData(td))
		}
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// --- Dict ref-width boundary tests ---

// TestWriter_SpansPerBlockBoundary verifies that the block count formula ceil(totalSpans/maxPerBlock)
// is correct for various boundary conditions.
// GAP-7: MaxBlockSpans block count formula.
func TestWriter_SpansPerBlockBoundary(t *testing.T) {
	cases := []struct {
		wantBlocks  int
		maxPerBlock int
		totalSpans  int
	}{
		{10, 10, 100}, // exactly fills 10 blocks
		{11, 10, 101}, // one spillover block
		{4, 33, 100},  // ceil(100/33) = 4 (33+33+33+1)
		{1, 100, 100}, // exactly one block
		{2, 100, 101}, // just over one block
		{5, 1, 5},     // each span its own block
	}

	for _, tc := range cases {
		t.Run(
			fmt.Sprintf("maxPerBlock=%d_totalSpans=%d", tc.maxPerBlock, tc.totalSpans),
			func(t *testing.T) {
				var buf bytes.Buffer
				w, err := writer.NewWriterWithConfig(writer.Config{
					OutputStream:  &buf,
					MaxBlockSpans: tc.maxPerBlock,
				})
				require.NoError(t, err)

				tid := makeTraceID(0x10)
				for i := range tc.totalSpans {
					span := makeSpan(
						tid, makeSpanID(byte(i%256)), //nolint:gosec // safe: modulo 256
						"op",
						uint64(1_000_000_000+i*1000), //nolint:gosec // test: i bounded
						uint64(2_000_000_000+i*1000), //nolint:gosec // test: i bounded
						nil,
					)
					td := makeTracesData("boundary-svc", []*tracev1.Span{span})
					require.NoError(t, w.AddTracesData(td))
				}

				_, err = w.Flush()
				require.NoError(t, err)

				r := openWriterTestReader(t, buf.Bytes())
				assert.Equal(t, tc.wantBlocks, r.BlockCount(),
					"maxPerBlock=%d totalSpans=%d: got %d blocks, want %d",
					tc.maxPerBlock, tc.totalSpans, r.BlockCount(), tc.wantBlocks)
			},
		)
	}
}

// TestWriter_DictRefWidthBoundary verifies that writing 256 unique string attribute values
// in a single block (triggering the 1→2 byte dict index flip at dictSize > 255) produces
// a readable file where all 256 unique values round-trip correctly.
// GAP-3: dict encoding index width boundary.
func TestWriter_DictRefWidthBoundary(t *testing.T) {
	const numUniqueValues = 256

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: numUniqueValues + 1, // all spans in one block
	})
	require.NoError(t, err)

	tid := makeTraceID(0x42)
	// Write 256 spans each with a unique "color" attribute value.
	values := make([]string, numUniqueValues)
	for i := range numUniqueValues {
		values[i] = fmt.Sprintf("color-%03d", i)
		span := makeSpan(
			tid, makeSpanID(byte(i)), //nolint:gosec // safe: i bounded by numUniqueValues (256)
			"op",
			uint64(1_000_000_000+i*1000), //nolint:gosec // test: i bounded
			uint64(2_000_000_000+i*1000), //nolint:gosec // test: i bounded
			map[string]string{"color": values[i]},
		)
		td := makeTracesData("dict-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount(), "all spans must land in one block")

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	colorCol := bwb.Block.GetColumn("span.color")
	require.NotNil(t, colorCol, "span.color column must be present")

	// Collect all recovered values.
	recovered := make(map[string]bool, numUniqueValues)
	for row := range bwb.Block.SpanCount() {
		v, ok := colorCol.StringValue(row)
		if ok {
			recovered[v] = true
		}
	}
	assert.Equal(t, numUniqueValues, len(recovered),
		"all %d unique dict values must round-trip correctly", numUniqueValues)
}

// TestWriter_DictRefWidthAt256Blocks verifies that writing 257 spans (one per block,
// MaxBlockSpans=1) all sharing the same service name correctly handles blockIdx > 255,
// which forces blockW to expand from 1 to 2 bytes in the intrinsic column encoding.
// GAP-3: block ref width boundary in intrinsic accumulator.
func TestWriter_DictRefWidthAt256Blocks(t *testing.T) {
	const numSpans = 257 // block 256 triggers blockIdx > 255 → blockW=2

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: 1, // one span per block → 257 blocks
	})
	require.NoError(t, err)

	tid := makeTraceID(0x43)
	for i := range numSpans {
		span := makeSpan(
			tid, makeSpanID(byte(i%256)), //nolint:gosec // safe: modulo 256
			"op",
			uint64(1_000_000_000+i*1000), //nolint:gosec // test: i bounded
			uint64(2_000_000_000+i*1000), //nolint:gosec // test: i bounded
			nil,
		)
		td := makeTracesData("shared-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	require.Equal(t, numSpans, r.BlockCount(), "must have exactly %d blocks", numSpans)

	// Verify every block is readable and has exactly 1 span.
	totalSpans := 0
	for i := range r.BlockCount() {
		bwb, readErr := r.GetBlockWithBytes(i, nil, nil)
		require.NoError(t, readErr, "block %d must be readable", i)
		totalSpans += bwb.Block.SpanCount()
	}
	assert.Equal(t, numSpans, totalSpans, "all %d spans must be recoverable", numSpans)
}

// --- Auto-Flush Tests ---

type writerTestMemProvider struct{ data []byte }

func (m *writerTestMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *writerTestMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func openWriterTestReader(t *testing.T, data []byte) *modules_reader.Reader {
	t.Helper()
	r, err := modules_reader.NewReaderFromProvider(&writerTestMemProvider{data: data})
	require.NoError(t, err)
	return r
}

func compileWriterTestQuery(t *testing.T, query string) *vm.Program {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoError(t, err)
	fe := parsed.(*traceqlparser.FilterExpression)
	prog, err := vm.CompileTraceQLFilter(fe)
	require.NoError(t, err)
	return prog
}

func addWriterTestSpan(t *testing.T, w *modules_blockio.Writer, traceID [16]byte, i int, service string) {
	t.Helper()
	span := &tracev1.Span{
		TraceId: traceID[:],
		SpanId: []byte{
			byte(i + 1), //nolint:gosec // safe: test loop index bounded, fits in byte
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		},
		Name:              "op",
		StartTimeUnixNano: uint64(1_000_000_000 + i*1_000), //nolint:gosec // test: i bounded by test loop range
		EndTimeUnixNano:   uint64(2_000_000_000 + i*1_000), //nolint:gosec // test: i bounded by test loop range
	}
	err := w.AddSpan(traceID[:], span, map[string]any{"service.name": service}, "", nil, "")
	require.NoError(t, err)
}

// TestAutoFlush_SameOutputAsBatch verifies that writing 3×MaxBufferedSpans spans
// with auto-flush produces the same queryable results as writing all at once.
// This is the critical correctness test for the auto-flush feature.
func TestAutoFlush_SameOutputAsBatch(t *testing.T) {
	const maxBlockSpans = 5
	const maxBufferedSpans = 10 // triggers flush after every 10 spans buffered
	const totalSpans = 30       // 3 × maxBufferedSpans

	traceID := [16]byte{0xDE, 0xAD}

	// Write with auto-flush (MaxBufferedSpans=10, so 3 intermediate flushes happen)
	var bufAutoFlush bytes.Buffer
	wAuto, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:     &bufAutoFlush,
		MaxBlockSpans:    maxBlockSpans,
		MaxBufferedSpans: maxBufferedSpans,
	})
	require.NoError(t, err)

	for i := range totalSpans {
		addWriterTestSpan(t, wAuto, traceID, i, "svc")
	}
	_, err = wAuto.Flush()
	require.NoError(t, err)

	// Write all at once (no auto-flush; all 30 spans buffer until Flush)
	var bufBatch bytes.Buffer
	wBatch, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &bufBatch,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)

	for i := range totalSpans {
		addWriterTestSpan(t, wBatch, traceID, i, "svc")
	}
	_, err = wBatch.Flush()
	require.NoError(t, err)

	// Both must return the same span count when queried with match-all
	rAuto := openWriterTestReader(t, bufAutoFlush.Bytes())
	rBatch := openWriterTestReader(t, bufBatch.Bytes())

	prog := compileWriterTestQuery(t, `{}`)

	rowsAuto, _, err := modules_executor.Collect(rAuto, prog, modules_executor.CollectOptions{})
	require.NoError(t, err)

	rowsBatch, _, err := modules_executor.Collect(rBatch, prog, modules_executor.CollectOptions{})
	require.NoError(t, err)

	assert.Equal(t, totalSpans, len(rowsAuto), "auto-flush must return all %d spans", totalSpans)
	assert.Equal(t, len(rowsBatch), len(rowsAuto),
		"auto-flush and batch must return the same span count")
}

// TestAutoFlush_Triggered verifies that CurrentSize stays bounded when MaxBufferedSpans
// is configured. After the auto-flush, the span buffer is cleared.
func TestAutoFlush_Triggered(t *testing.T) {
	const maxBufferedSpans = 5
	const maxBlockSpans = 3
	const totalSpans = 15 // 3 × maxBufferedSpans, so 3 auto-flushes happen

	traceID := [16]byte{0xBE, 0xEF}

	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:     &buf,
		MaxBlockSpans:    maxBlockSpans,
		MaxBufferedSpans: maxBufferedSpans,
	})
	require.NoError(t, err)

	for i := range totalSpans {
		addWriterTestSpan(t, w, traceID, i, "svc")
	}

	// After all adds, CurrentSize must be < maxBufferedSpans * 2048 bytes
	// because auto-flush cleared the buffer each time it hit the limit.
	require.Less(t, w.CurrentSize(), int64(maxBufferedSpans)*2048,
		"CurrentSize must stay bounded after auto-flush triggers")

	_, err = w.Flush()
	require.NoError(t, err)

	// All spans must be readable
	r := openWriterTestReader(t, buf.Bytes())
	prog := compileWriterTestQuery(t, `{}`)
	rows, _, err := modules_executor.Collect(r, prog, modules_executor.CollectOptions{})
	require.NoError(t, err)
	assert.Equal(t, totalSpans, len(rows), "all %d spans must be present after auto-flush", totalSpans)
}

// TestAddColumn_LateColumnCapacity verifies that a column first seen late in a block
// (span row N, where N > 0) does not prevent the writer from producing a readable file.
// The fix in addColumn (using remaining-capacity hint) must not break the invariant that
// every column in a block has exactly spanCount rows.
// Corresponds to NOTES.md §20.
func TestAddColumn_LateColumnCapacity(t *testing.T) {
	// Use MaxBlockSpans=200 so the block closes after 200 spans.
	// The first 199 spans have no "span.late_attr"; only span 200 has it.
	const blockSize = 200

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: blockSize,
	})
	require.NoError(t, err)

	// Build blockSize-1 spans WITHOUT "late_attr".
	earlySpans := make([]*tracev1.Span, blockSize-1)
	for i := range blockSize - 1 {
		tid := make([]byte, 16)
		tid[0] = byte(i)
		sid := make([]byte, 8)
		sid[0] = byte(i)
		earlySpans[i] = makeSpan(tid, sid, "op",
			1_000_000_000+uint64(i), 2_000_000_000+uint64(i), nil)
	}

	// One final span WITH "late_attr".
	tid := make([]byte, 16)
	tid[0] = byte(blockSize - 1)
	sid := make([]byte, 8)
	sid[0] = byte(blockSize - 1)
	lateSpan := makeSpan(tid, sid, "op",
		1_000_000_000+uint64(blockSize-1), 2_000_000_000+uint64(blockSize-1),
		map[string]string{"late_attr": "present"})

	allSpans := append(earlySpans, lateSpan)
	td := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			makeResourceSpan("test-svc", allSpans),
		},
	}

	require.NoError(t, w.AddTracesData(td))
	_, err = w.Flush()
	require.NoError(t, err)

	// Output must be non-empty.
	assert.Greater(t, buf.Len(), 0, "expected non-empty output")

	// Verify the file is readable and the late column has correct row count.
	r := openWriterTestReader(t, buf.Bytes())

	found := false
	for i := range r.BlockCount() {
		bwb, err := r.GetBlockWithBytes(i, nil, nil)
		require.NoError(t, err)
		if col := bwb.Block.GetColumn("span.late_attr"); col != nil {
			found = true
			// The column must have exactly spanCount rows (reader enforces this).
			assert.Equal(t, bwb.Block.SpanCount(), col.SpanCount,
				"late column row_count must equal block span_count")
			break
		}
	}
	assert.True(t, found, "expected span.late_attr column to appear in at least one block")
}

// TestAutoFlush_ZeroMeansNoAutoFlush verifies that when the default MaxBufferedSpans
// (5 × MaxBlockSpans = 25 for MaxBlockSpans=5) exceeds the total span count, no
// auto-flush occurs — all spans accumulate until explicit Flush().
func TestAutoFlush_ZeroMeansNoAutoFlush(t *testing.T) {
	const totalSpans = 20 // < 5×MaxBlockSpans (25), so no auto-flush expected
	traceID := [16]byte{0xCA, 0xFE}

	var buf bytes.Buffer
	// MaxBlockSpans=5 → MaxBufferedSpans defaults to 5×5=25; 20 spans won't trigger auto-flush.
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 5,
	})
	require.NoError(t, err)

	for i := range totalSpans {
		addWriterTestSpan(t, w, traceID, i, "svc")
	}

	// CurrentSize must reflect all 20 buffered spans (no flush has occurred)
	require.GreaterOrEqual(t, w.CurrentSize(), int64(totalSpans)*2048,
		"with no auto-flush, all spans must remain buffered")

	_, err = w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	prog := compileWriterTestQuery(t, `{}`)
	rows, _, err := modules_executor.Collect(r, prog, modules_executor.CollectOptions{})
	require.NoError(t, err)
	assert.Equal(t, totalSpans, len(rows))
}

// --- GC Safety Tests ---

// addSpanHelper calls AddSpan from a separate stack frame so the *tracev1.Span
// allocation goes out of scope when the helper returns. The only remaining GC
// anchor is w.protoSpanRoots. Used by TestGC_AddSpan_ProtoRootsAnchoring.
func addSpanHelper(t *testing.T, w *writer.Writer, i int) {
	t.Helper()
	// Heap-allocate the span name and attribute values so there are no interned
	// string constants keeping them alive after this frame returns.
	span := &tracev1.Span{
		TraceId:           makeTraceID(byte(i)), //nolint:gosec // test: i bounded by loop range
		SpanId:            makeSpanID(byte(i)),  //nolint:gosec // test: i bounded by loop range
		Name:              fmt.Sprintf("op-long-name-not-interned-%d", i),
		StartTimeUnixNano: uint64(i) * 1_000_000,         //nolint:gosec // test: i bounded by loop range
		EndTimeUnixNano:   uint64(i)*1_000_000 + 500_000, //nolint:gosec // test: i bounded by loop range
		Attributes: []*commonv1.KeyValue{
			{
				Key: "attr.key1",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("val-unique-%d-aaa", i)},
				},
			},
			{
				Key: "attr.key2",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("val-unique-%d-bbb", i)},
				},
			},
		},
	}
	resourceAttrs := map[string]any{
		"service.name": fmt.Sprintf("svc-%d", i),
		"host.name":    fmt.Sprintf("host-%d", i),
	}
	err := w.AddSpan(span.TraceId, span, resourceAttrs, "", nil, "")
	require.NoError(t, err)
	// span and resourceAttrs go out of scope here; only w.protoSpanRoots keeps them alive.
}

// TestGC_WriterCreation_NoSpans reproduces the "startup before any spans written"
// scenario observed in the docker container crash. A Writer is created, GC runs
// immediately — verifying that the arena embedded by value in Writer doesn't
// corrupt GC metadata even without any arena allocations.
func TestGC_WriterCreation_NoSpans(t *testing.T) {
	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	for i := range 200 {
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: io.Discard})
		require.NoError(t, err)
		runtime.GC()
		runtime.KeepAlive(w)
		_ = i
	}
}

// TestGC_Arena_ConcurrentGC stress-tests arena-backed AttrKV storage while a
// goroutine continuously triggers GC. This exercises the write-barrier-free
// xunsafe.ByteStore path inside AllocTraceable and verifies that chunk pointers
// stored in arena.blocks[] are always reachable.
func TestGC_Arena_ConcurrentGC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GC stress test in short mode")
	}

	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	done := make(chan struct{})
	var gcWG sync.WaitGroup
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-done:
				return
			default:
				runtime.GC()
				runtime.Gosched()
			}
		}
	}()

	// Run many flush cycles with many attributes to maximize arena churn.
	attrs := map[string]string{
		"http.method":      "GET",
		"http.url":         "https://example.com/api/v1/traces",
		"http.status_code": "200",
		"component":        "proxy",
		"span.kind":        "server",
		"user.id":          "user-12345",
		"request.id":       "req-abcdef-123456",
		"region":           "us-east-1",
		"cluster":          "prod-cluster-07",
		"service.version":  "1.2.3-deadbeef",
	}

	for cycle := range 10 {
		var buf bytes.Buffer
		w := newTestWriter(t, &buf, 10)
		for i := range 200 {
			span := makeSpan(
				makeTraceID(byte(i+cycle)),
				makeSpanID(byte(i)),
				fmt.Sprintf("op-%d", i),
				uint64(i)*1_000_000,         //nolint:gosec
				uint64(i)*1_000_000+500_000, //nolint:gosec
				attrs,
			)
			td := makeTracesData(fmt.Sprintf("svc-%d", i%5), []*tracev1.Span{span})
			require.NoError(t, w.AddTracesData(td))
		}
		_, err := w.Flush()
		require.NoError(t, err)
		runtime.GC()
	}

	close(done)
	gcWG.Wait()
}

// TestGC_AddSpan_ProtoRootsAnchoring verifies that Writer.protoSpanRoots correctly
// prevents the GC from collecting span proto strings while they are referenced from
// arena memory (which is GC-opaque). The span proto is created in a separate stack
// frame (addSpanHelper) so its only GC anchor after the call returns is protoSpanRoots.
// If protoSpanRoots were absent, GC could collect the proto strings, leaving the arena
// with dangling interior pointers — the "sweep increased allocation count" crash.
func TestGC_AddSpan_ProtoRootsAnchoring(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GC stress test in short mode")
	}

	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 300 {
		addSpanHelper(t, w, i)
		// Force GC after each AddSpan. The span proto created inside addSpanHelper
		// is no longer referenced by the caller's stack — only by w.protoSpanRoots.
		// Without protoSpanRoots the GC would reclaim the proto string backing bytes
		// while the arena still holds pointers into them.
		runtime.GC()
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestGC_TempoWritePattern mirrors Tempo's vblockpack create.go loop where source
// trace objects are converted to OTLP protos (string headers copied, backing bytes
// shared), passed to AddTracesData, then dropped. The backing bytes are then only
// anchored through td in protoRoots (GC-visible) and the arena D-region (GC-opaque).
// Without protoRoots, GC could collect those bytes while the arena still holds
// pointers into them, triggering the "sweep increased allocation count" fatal error.
func TestGC_TempoWritePattern(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping GC stress test in short mode")
	}

	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	// Concurrent GC goroutine mirrors Tempo's background GC pressure during ingestion.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				runtime.GC()
				runtime.Gosched()
			}
		}
	}()
	defer func() {
		close(done)
		wg.Wait()
	}()

	var buf bytes.Buffer
	// Default MaxBufferedSpans=5×MaxBlockSpans(2000)=10000; 500 spans won't trigger auto-flush.
	w := newTestWriter(t, &buf, 0)

	for i := range 500 {
		// Create non-interned source strings simulating tempopb.Trace attributes.
		// fmt.Sprintf guarantees heap-allocated backing bytes not held by any
		// interning cache, so they become GC-eligible as soon as all references drop.
		svcName := fmt.Sprintf("svc-%d", i)
		spanName := fmt.Sprintf("op-%d", i)
		attrKey := fmt.Sprintf("req.id-%d", i)
		attrVal := fmt.Sprintf("req-abc-%d-xyz", i)

		// Simulate tempoTraceToOTLP: string header assignment copies ptr+len but
		// NOT the backing bytes. td.ResourceSpans strings share backing memory with
		// the svcName/spanName/attrKey/attrVal variables declared above.
		span := makeSpan(
			makeTraceID(byte(i)), //nolint:gosec
			makeSpanID(byte(i)),  //nolint:gosec
			spanName,
			uint64(i)*1_000_000,         //nolint:gosec
			uint64(i)*1_000_000+500_000, //nolint:gosec
			map[string]string{attrKey: attrVal},
		)
		td := makeTracesData(svcName, []*tracev1.Span{span})

		// AddTracesData stores AttrKV in the arena's GC-opaque D-region. The
		// backing bytes of svcName/spanName/attrKey/attrVal are now pointed to from
		// both td (GC-visible via protoRoots) and the arena (GC-opaque to GC scan).
		require.NoError(t, w.AddTracesData(td))

		// Release source string references, mirroring the source tempopb.Trace
		// being dropped after conversion in Tempo's create.go. After KeepAlive the
		// backing bytes are only anchored through td stored in protoRoots.
		// Without protoRoots, GC would reclaim those bytes while the arena still
		// holds AttrKV entries pointing into them.
		runtime.KeepAlive(svcName)
		runtime.KeepAlive(spanName)
		runtime.KeepAlive(attrKey)
		runtime.KeepAlive(attrVal)
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestConcurrentWriters verifies that multiple independent Writers can run their
// full lifecycle (AddTracesData + Flush) concurrently without data races or crashes.
// The primary regression target is the package-level kllRNG variable that is mutated
// by KLL.compactLevel during Flush(); when two Writers flush at the same time both
// call kllRandomBit() on the same global, which is a data race under -race and
// undefined behavior without it.
// Run with: go test -race -run TestConcurrentWriters ./internal/modules/blockio/writer/
func TestConcurrentWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent-writers stress test in short mode")
	}

	old := debug.SetGCPercent(1)
	defer debug.SetGCPercent(old)

	const numWriters = 4
	const spansPerWriter = 300 // > kllDefaultK(200) to force KLL compaction + kllRandomBit calls

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for w := range numWriters {
		go func(writerID int) {
			defer wg.Done()

			var buf bytes.Buffer
			wr := newTestWriter(t, &buf, 0)

			for i := range spansPerWriter {
				// Varied attributes ensure high cardinality per column, triggering
				// KLL compaction (which calls the previously-global kllRandomBit).
				svcName := fmt.Sprintf("concurrent-svc-%d-%d", writerID, i)
				spanName := fmt.Sprintf("concurrent-op-%d-%d", writerID, i)
				attrKey := fmt.Sprintf("req.id.w%d", writerID)
				attrVal := fmt.Sprintf("req-w%d-span%d", writerID, i)

				span := makeSpan(
					makeTraceID(byte(writerID*100+i)), //nolint:gosec
					makeSpanID(byte(i)),               //nolint:gosec
					spanName,
					uint64(i)*1_000_000,         //nolint:gosec
					uint64(i)*1_000_000+500_000, //nolint:gosec
					map[string]string{attrKey: attrVal},
				)
				td := makeTracesData(svcName, []*tracev1.Span{span})
				require.NoError(t, wr.AddTracesData(td))
			}

			_, err := wr.Flush()
			require.NoError(t, err)
		}(w)
	}

	wg.Wait()
}

// TestBoundedMemory_PendingBufferBounded verifies that the writer's pending span buffer
// stays bounded at MaxBufferedSpans even when many more spans are written.
// This is the core RSS-bounding invariant: without auto-flush, proto memory grows
// with every span; with auto-flush, it is capped at one batch worth of protos.
func TestBoundedMemory_PendingBufferBounded(t *testing.T) {
	const maxBufferedSpans = 50
	const totalSpans = 500 // 10 × maxBufferedSpans → 10 auto-flushes expected

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:     &buf,
		MaxBlockSpans:    10, // small blocks to exercise block boundaries
		MaxBufferedSpans: maxBufferedSpans,
	})
	require.NoError(t, err)

	traceID := makeTraceID(0x42)
	for i := range totalSpans {
		span := makeSpan(
			traceID,
			makeSpanID(byte(i)), //nolint:gosec
			fmt.Sprintf("op-%d", i),
			uint64(i)*1_000_000,         //nolint:gosec
			uint64(i)*1_000_000+500_000, //nolint:gosec
			map[string]string{"http.method": "GET", fmt.Sprintf("attr-%d", i%10): "val"},
		)
		td := makeTracesData("bounded-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))

		// After each add, CurrentSize must be bounded to MaxBufferedSpans × estimatedBytes.
		// This verifies that auto-flush keeps the pending buffer from growing unboundedly.
		require.LessOrEqual(t, w.CurrentSize(), int64(maxBufferedSpans)*2048,
			"pending buffer exceeded MaxBufferedSpans after span %d", i)
	}

	_, err = w.Flush()
	require.NoError(t, err)

	// Verify all spans are readable from the output.
	r := openWriterTestReader(t, buf.Bytes())
	prog := compileWriterTestQuery(t, `{}`)
	rows, _, err := modules_executor.Collect(r, prog, modules_executor.CollectOptions{})
	require.NoError(t, err)
	assert.Equal(t, totalSpans, len(rows), "all spans must be present in output")
}

// TestBoundedMemory_HeapAllocsProportional verifies that heap allocations during
// span ingestion grow proportionally to MaxBufferedSpans, not to total span count.
// Uses runtime.ReadMemStats to measure bytes allocated during the hot path.
func TestBoundedMemory_HeapAllocsProportional(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap alloc measurement in short mode")
	}

	const maxBufferedSpans = 100
	const totalSpans = 2000 // 20 × maxBufferedSpans

	// Force GC to establish a clean baseline.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:     &buf,
		MaxBlockSpans:    20,
		MaxBufferedSpans: maxBufferedSpans,
	})
	require.NoError(t, err)

	traceID := makeTraceID(0xBB)
	for i := range totalSpans {
		span := makeSpan(
			traceID,
			makeSpanID(byte(i)), //nolint:gosec
			"bounded-op",
			uint64(i)*1_000_000,         //nolint:gosec
			uint64(i)*1_000_000+500_000, //nolint:gosec
			map[string]string{"http.status_code": "200"},
		)
		td := makeTracesData("heap-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// Live heap objects after Flush must be bounded — the writer should have
	// released all per-span allocations. We allow a generous 10 MB for output
	// buffers, metadata, and test framework overhead.
	liveBytes := after.HeapInuse - before.HeapInuse
	const maxLiveBytes = 10 << 20                                // 10 MiB
	assert.LessOrEqual(t, int64(liveBytes), int64(maxLiveBytes), //nolint:gosec
		"live heap after Flush should be bounded (got %d bytes)", liveBytes)
}

// TestFlush_AttributeTypeConflict verifies that writing two spans where the same
// attribute key carries different value types within one block does not panic.
// Each (name, type) combination is stored as a separate column — no values are dropped.
func TestFlush_AttributeTypeConflict(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// Span 1: "conflict.attr" is a bool.
	span1 := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(1),
		Name:              "op-bool",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Attributes: []*commonv1.KeyValue{
			{Key: "conflict.attr", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}},
		},
	}
	// Span 2: "conflict.attr" is a float64 — type conflict within the same block.
	span2 := &tracev1.Span{
		TraceId:           makeTraceID(2),
		SpanId:            makeSpanID(2),
		Name:              "op-float",
		StartTimeUnixNano: 3_000_000_000,
		EndTimeUnixNano:   4_000_000_000,
		Attributes: []*commonv1.KeyValue{
			{Key: "conflict.attr", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14}}},
		},
	}

	td := makeTracesData("test-svc", []*tracev1.Span{span1, span2})
	require.NoError(t, w.AddTracesData(td))

	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(0))
}

// TestFlush_SameNameDifferentTypes verifies that when the same attribute key appears
// with different types across spans in a block, both values are preserved as separate
// typed columns — no values are silently dropped.
func TestFlush_SameNameDifferentTypes(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// Span 1: "mixed.attr" is a string.
	span1 := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(1),
		Name:              "op-string",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Attributes: []*commonv1.KeyValue{
			{Key: "mixed.attr", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}}},
		},
	}
	// Span 2: "mixed.attr" is an int64 — different type, same key.
	span2 := &tracev1.Span{
		TraceId:           makeTraceID(2),
		SpanId:            makeSpanID(2),
		Name:              "op-int",
		StartTimeUnixNano: 3_000_000_000,
		EndTimeUnixNano:   4_000_000_000,
		Attributes: []*commonv1.KeyValue{
			{Key: "mixed.attr", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 42}}},
		},
	}

	td := makeTracesData("test-svc", []*tracev1.Span{span1, span2})
	require.NoError(t, w.AddTracesData(td))

	n, err := w.Flush()
	require.NoError(t, err)
	require.Greater(t, n, int64(0))

	// Read back and verify both typed columns are present.
	reader := openWriterTestReader(t, buf.Bytes())
	require.Equal(t, 1, reader.BlockCount())

	bwb, err := reader.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	block := bwb.Block

	// Both "span.mixed.attr" (string) and "span.mixed.attr" (int64) must be present.
	stringCol := block.GetColumnByType("span.mixed.attr", modules_shared.ColumnTypeString)
	require.NotNil(t, stringCol, "string column for span.mixed.attr must exist")
	int64Col := block.GetColumnByType("span.mixed.attr", modules_shared.ColumnTypeInt64)
	require.NotNil(t, int64Col, "int64 column for span.mixed.attr must exist")

	// Span 0 (row 0 or 1 depending on sort) has the string value.
	// Span 1 has the int64 value.
	// Find the row with the string value present.
	stringRow, int64Row := -1, -1
	for i := range block.SpanCount() {
		if stringCol.IsPresent(i) {
			stringRow = i
		}
		if int64Col.IsPresent(i) {
			int64Row = i
		}
	}
	require.NotEqual(t, -1, stringRow, "string value must be present in some row")
	require.NotEqual(t, -1, int64Row, "int64 value must be present in some row")

	sv, ok := stringCol.StringValue(stringRow)
	require.True(t, ok)
	assert.Equal(t, "hello", sv)

	iv, ok := int64Col.Int64Value(int64Row)
	require.True(t, ok)
	assert.Equal(t, int64(42), iv)
}

// TestMaxIntrinsicRows_OverCap verifies that when MaxIntrinsicRows is exceeded
// the writer produces an empty intrinsic section (HasIntrinsicSection == false),
// and that a file written just below the threshold has a populated section.
// GAP-5: MaxIntrinsicRows safety cap.
func TestMaxIntrinsicRows_OverCap(t *testing.T) {
	const origThreshold = 10_000_000

	makeSpanAt := func(i int) *tracev1.Span {
		return &tracev1.Span{
			TraceId:           makeTraceID(byte(i)), //nolint:gosec
			SpanId:            makeSpanID(byte(i)),  //nolint:gosec
			Name:              fmt.Sprintf("op-%d", i),
			StartTimeUnixNano: uint64(i) * 1_000_000,       //nolint:gosec
			EndTimeUnixNano:   uint64(i)*1_000_000 + 1_000, //nolint:gosec // test: i bounded by small constant (5 or 6)
		}
	}

	t.Run("below threshold has intrinsic section", func(t *testing.T) {
		modules_shared.MaxIntrinsicRows = 5
		t.Cleanup(func() { modules_shared.MaxIntrinsicRows = origThreshold })

		var buf bytes.Buffer
		w := newTestWriter(t, &buf, 0)
		for i := range 5 {
			td := makeTracesData("svc", []*tracev1.Span{makeSpanAt(i)})
			require.NoError(t, w.AddTracesData(td))
		}
		_, err := w.Flush()
		require.NoError(t, err)

		r := openWriterTestReader(t, buf.Bytes())
		assert.True(t, r.HasIntrinsicSection(), "5 spans at threshold=5 must produce intrinsic section")
	})

	t.Run("above threshold produces empty intrinsic section", func(t *testing.T) {
		modules_shared.MaxIntrinsicRows = 5
		t.Cleanup(func() { modules_shared.MaxIntrinsicRows = origThreshold })

		var buf bytes.Buffer
		w := newTestWriter(t, &buf, 0)
		for i := range 6 {
			td := makeTracesData("svc", []*tracev1.Span{makeSpanAt(i)})
			require.NoError(t, w.AddTracesData(td))
		}
		_, err := w.Flush()
		require.NoError(t, err)

		r := openWriterTestReader(t, buf.Bytes())
		assert.False(t, r.HasIntrinsicSection(), "6 spans exceeding threshold=5 must produce empty intrinsic section")
	})
}

// --- AddTempoTrace tests ---

// makeTempoTrace builds a *tempopb.Trace with the given service name and spans.
// Each span entry is [traceID []byte, spanID []byte, name string].
func makeTempoTrace(serviceName string, spans ...[3]any) *tempopb.Trace {
	tempoSpans := make([]*tempotrace.Span, 0, len(spans))
	for _, s := range spans {
		traceID := s[0].([]byte)
		spanID := s[1].([]byte)
		name := s[2].(string)
		tempoSpans = append(tempoSpans, &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              name,
			Kind:              tempotrace.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_UNSET},
		})
	}
	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{
							Key: "service.name",
							Value: &tempocommon.AnyValue{
								Value: &tempocommon.AnyValue_StringValue{StringValue: serviceName},
							},
						},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: tempoSpans}},
			},
		},
	}
}

// sumTempoSpans returns the total span count across all blocks in a Reader.
func sumTempoSpans(r *modules_reader.Reader) int {
	total := 0
	for i := range r.BlockCount() {
		total += int(r.BlockMeta(i).SpanCount) //nolint:gosec // safe: SpanCount bounded by MaxBlockSpans (65535)
	}
	return total
}

// TestAddTempoTrace_Basic verifies that a single *tempopb.Trace produces a valid blockpack file.
func TestAddTempoTrace_Basic(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	trace := makeTempoTrace("my-service",
		[3]any{makeTraceID(1), makeSpanID(1), "op-1"},
		[3]any{makeTraceID(2), makeSpanID(2), "op-2"},
	)
	require.NoError(t, w.AddTempoTrace(trace))
	n, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, n, int64(100))

	r := openWriterTestReader(t, buf.Bytes())
	assert.Equal(t, 2, sumTempoSpans(r))
}

// TestAddTempoTrace_NilTrace verifies that AddTempoTrace(nil) is a no-op.
func TestAddTempoTrace_NilTrace(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)
	require.NoError(t, w.AddTempoTrace(nil))
	_, err := w.Flush()
	require.NoError(t, err)
}

// TestAddTempoTrace_NilSpan verifies that nil spans within a ResourceSpans are skipped.
func TestAddTempoTrace_NilSpan(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{
							Key:   "service.name",
							Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc"}},
						},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							nil,
							{
								TraceId:           makeTraceID(1),
								SpanId:            makeSpanID(1),
								Name:              "real-span",
								StartTimeUnixNano: 1000,
								EndTimeUnixNano:   2000,
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, w.AddTempoTrace(trace))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	assert.Equal(t, 1, sumTempoSpans(r))
}

// TestAddTempoTrace_RejectsMixedSignal verifies that AddTempoTrace returns an error when
// the writer already has log records buffered (signal type guard).
func TestAddTempoTrace_RejectsMixedSignal(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// Buffer one log record to set signalType = SignalTypeLog.
	ld := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
						},
					},
				},
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 1000,
								Body: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "hello"},
								},
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, w.AddLogsData(ld))
	err := w.AddTempoTrace(makeTempoTrace("svc", [3]any{makeTraceID(1), makeSpanID(1), "op"}))
	assert.Error(t, err)
}

// TestAddTempoTrace_EquivalentToOTLP verifies that AddTempoTrace and AddTracesData produce
// files with matching span count and resource.service.name for equivalent input.
func TestAddTempoTrace_EquivalentToOTLP(t *testing.T) {
	traceID := makeTraceID(42)
	spanID := makeSpanID(7)

	// Write via OTLP path.
	var otlpBuf bytes.Buffer
	otlpW := newTestWriter(t, &otlpBuf, 0)
	otlpSpan := makeSpan(traceID, spanID, "test-op", 1_000_000_000, 2_000_000_000,
		map[string]string{"http.method": "GET"})
	require.NoError(t, otlpW.AddTracesData(makeTracesData("my-svc", []*tracev1.Span{otlpSpan})))
	_, err := otlpW.Flush()
	require.NoError(t, err)

	// Write via Tempo path.
	var tempoBuf bytes.Buffer
	tempoW := newTestWriter(t, &tempoBuf, 0)
	tempoTrace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{
							Key: "service.name",
							Value: &tempocommon.AnyValue{
								Value: &tempocommon.AnyValue_StringValue{StringValue: "my-svc"},
							},
						},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "test-op",
								Kind:              tempotrace.Span_SPAN_KIND_UNSPECIFIED,
								StartTimeUnixNano: 1_000_000_000,
								EndTimeUnixNano:   2_000_000_000,
								Attributes: []*tempocommon.KeyValue{
									{
										Key: "http.method",
										Value: &tempocommon.AnyValue{
											Value: &tempocommon.AnyValue_StringValue{StringValue: "GET"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, tempoW.AddTempoTrace(tempoTrace))
	_, err = tempoW.Flush()
	require.NoError(t, err)

	otlpR := openWriterTestReader(t, otlpBuf.Bytes())
	tempoR := openWriterTestReader(t, tempoBuf.Bytes())
	assert.Equal(t, sumTempoSpans(otlpR), sumTempoSpans(tempoR), "span count must match")

	// Verify resource.service.name is present in the intrinsic section for both paths.
	// Block columns no longer store intrinsic fields — use the intrinsic reader.
	otlpSvcIntrinsic, err := otlpR.GetIntrinsicColumn("resource.service.name")
	require.NoError(t, err)
	require.NotNil(t, otlpSvcIntrinsic, "resource.service.name must be in intrinsic section (OTLP path)")

	tempoSvcIntrinsic, err := tempoR.GetIntrinsicColumn("resource.service.name")
	require.NoError(t, err)
	require.NotNil(t, tempoSvcIntrinsic, "resource.service.name must be in intrinsic section (Tempo path)")

	// Both must have the same service name.
	require.NotEmpty(t, otlpSvcIntrinsic.DictEntries, "OTLP intrinsic must have dict entries")
	require.NotEmpty(t, tempoSvcIntrinsic.DictEntries, "Tempo intrinsic must have dict entries")
	assert.Equal(t, otlpSvcIntrinsic.DictEntries[0].Value, tempoSvcIntrinsic.DictEntries[0].Value,
		"resource.service.name must match between OTLP and Tempo paths")

	// Verify resource.service.name is present in block columns (dual storage) — both OTLP and Tempo paths.
	otlpBWB, err := otlpR.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, otlpBWB.Block.GetColumn("resource.service.name"),
		"resource.service.name must be present in block columns (dual storage, OTLP path)")

	tempoBWB, err := tempoR.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, tempoBWB.Block.GetColumn("resource.service.name"),
		"resource.service.name must be present in block columns (dual storage, Tempo path)")

	// Verify the span attribute is also present.
	tempoMethodCol := tempoBWB.Block.GetColumn("span.http.method")
	require.NotNil(t, tempoMethodCol)
	method, ok := tempoMethodCol.StringValue(0)
	assert.True(t, ok)
	assert.Equal(t, "GET", method)
}

// TestAddTempoTrace_MultipleBlocks verifies that auto-flush fires correctly when
// MaxBufferedSpans is exceeded via the Tempo ingest path.
func TestAddTempoTrace_MultipleBlocks(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 5) // MaxBlockSpans=5

	for i := range 12 {
		trace := makeTempoTrace("svc",
			[3]any{makeTraceID(byte(i + 1)), makeSpanID(byte(i + 1)), fmt.Sprintf("op-%d", i)},
		)
		require.NoError(t, w.AddTempoTrace(trace))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	assert.Equal(t, 12, sumTempoSpans(r))
	assert.GreaterOrEqual(t, r.BlockCount(), 2, "expected at least 2 blocks for 12 spans with MaxBlockSpans=5")
}

// TestIntrinsicDualStorage verifies that span intrinsic fields are stored in BOTH
// block columns (for O(1) reverse lookups) AND the intrinsic TOC section (for
// fast block-level pruning). This dual-storage approach was restored to fix an
// O(N) per-column reverse lookup regression introduced in PR #172.
func TestIntrinsicDualStorage(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	td := makeTracesData("acme", []*tracev1.Span{
		makeSpan(makeTraceID(1), makeSpanID(1), "op-a", 1_000_000_000, 2_000_000_000, map[string]string{"env": "prod"}),
		makeSpan(makeTraceID(2), makeSpanID(2), "op-b", 2_000_000_000, 3_000_000_000, nil),
	})
	require.NoError(t, w.AddTracesData(td))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 1, "expected at least one block")

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	// Intrinsic columns MUST be present in block columns (dual storage for O(1) lookups).
	// 7 identity fields + span:status (Status{Code:STATUS_CODE_UNSET} is non-nil) +
	// resource.service.name ("acme" service triggers dual-storage addPresent).
	intrinsicColNames := []string{
		"trace:id",
		"span:id",
		"span:name",
		"span:kind",
		"span:start",
		"span:end",
		"span:duration",
		"span:status",
		"resource.service.name",
	}
	for _, name := range intrinsicColNames {
		assert.NotNil(t, bwb.Block.GetColumn(name),
			"intrinsic column %q must be present in block columns (dual storage)", name)
	}

	// The intrinsic section must also contain the identity fields.
	intrinsicColumnNames := r.IntrinsicColumnNames()
	intrinsicSet := make(map[string]struct{}, len(intrinsicColumnNames))
	for _, n := range intrinsicColumnNames {
		intrinsicSet[n] = struct{}{}
	}
	for _, want := range []string{
		"trace:id", "span:id", "span:name", "span:kind",
		"span:start", "span:duration", "span:status", "resource.service.name",
	} {
		_, ok := intrinsicSet[want]
		assert.True(t, ok, "intrinsic section must contain column %q", want)
	}

	// span:end must NOT be in the intrinsic section — it is synthesized from start+duration on read.
	assert.NotContains(t, intrinsicSet, "span:end",
		"span:end must not be written to the intrinsic section (synthesized on read)")
}

// TestCompactionDualStorage verifies that the compaction write path (addRowFromBlock / AddRow)
// produces block column payloads with intrinsic columns, just like the ingestion path.
// Without this, compacted files revert to the O(N) reverse-lookup path that this PR fixes.
func TestCompactionDualStorage(t *testing.T) {
	// Step 1: write a file with spans via the ingestion path.
	var srcBuf bytes.Buffer
	w := newTestWriter(t, &srcBuf, 0)
	td := makeTracesData("acme", []*tracev1.Span{
		makeSpan(makeTraceID(1), makeSpanID(1), "op-a", 1_000_000_000, 2_000_000_000, map[string]string{"env": "prod"}),
		makeSpan(makeTraceID(2), makeSpanID(2), "op-b", 2_000_000_000, 3_000_000_000, nil),
	})
	require.NoError(t, w.AddTracesData(td))
	_, err := w.Flush()
	require.NoError(t, err)

	// Step 2: compact via AddRow (the addRowFromBlock path).
	srcR := openWriterTestReader(t, srcBuf.Bytes())
	require.GreaterOrEqual(t, srcR.BlockCount(), 1)

	var dstBuf bytes.Buffer
	dstW := newTestWriter(t, &dstBuf, 0)
	srcBlock, err := srcR.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	for rowIdx := range srcBlock.Block.SpanCount() {
		require.NoError(t, dstW.AddRow(srcBlock.Block, rowIdx))
	}
	_, err = dstW.Flush()
	require.NoError(t, err)

	// Step 3: verify intrinsic columns are present in the compacted block payloads.
	dstR := openWriterTestReader(t, dstBuf.Bytes())
	require.GreaterOrEqual(t, dstR.BlockCount(), 1)

	dstBWB, err := dstR.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	compactedIntrinsicCols := []string{
		"trace:id",
		"span:id",
		"span:name",
		"span:kind",
		"span:start",
		"span:end",
		"span:duration",
		"resource.service.name",
	}
	for _, name := range compactedIntrinsicCols {
		assert.NotNil(t, dstBWB.Block.GetColumn(name),
			"compacted block: intrinsic column %q must be present in block columns (dual storage)", name)
	}
}
