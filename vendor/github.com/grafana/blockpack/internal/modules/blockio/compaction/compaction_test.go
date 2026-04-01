package compaction_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/blockio/compaction"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// memProvider is an in-memory ReaderProvider for tests.
type memProvider struct{ data []byte }

func (m *memProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *memProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off+int64(len(p)) > int64(len(m.data)) {
		return 0, fmt.Errorf("memProvider: range [%d,%d) out of bounds (size=%d)",
			off, off+int64(len(p)), len(m.data))
	}
	return copy(p, m.data[off:]), nil
}

// memOutputStorage accumulates Put calls in-memory.
type memOutputStorage struct {
	files map[string][]byte
}

func newMemOutputStorage() *memOutputStorage {
	return &memOutputStorage{files: make(map[string][]byte)}
}

func (s *memOutputStorage) Put(path string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.files[path] = cp
	return nil
}

// makeTraceID returns a 16-byte trace ID with the given byte in the first position.
func makeTraceID(n byte) []byte {
	id := make([]byte, 16)
	id[0] = n
	return id
}

// makeSpanID returns an 8-byte span ID with the given byte in the first position.
func makeSpanID(n byte) []byte {
	id := make([]byte, 8)
	id[0] = n
	return id
}

// writeTestBlock writes N spans to a new blockpack buffer and returns its bytes.
// Each span has distinct traceID, spanID, name, start time, and span/resource attributes.
func writeTestBlock(t *testing.T, spans []*tracev1.TracesData) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 100,
	})
	require.NoError(t, err)
	for _, td := range spans {
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// openBlockReader opens a reader from raw blockpack bytes.
func openBlockReader(t *testing.T, data []byte) *modules_reader.Reader {
	t.Helper()
	r, err := modules_reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	return r
}

// tracesData builds a TracesData with service name and a single span.
func tracesData(svcName string, span *tracev1.Span) *tracev1.TracesData {
	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}},
			},
		},
	}
}

// TestCompactBlocks_NativeColumns writes N blocks with known span data, compacts them
// via CompactBlocks (native columnar path — no OTLP objects created during compaction),
// reads the output back, and verifies all column values are preserved exactly.
func TestCompactBlocks_NativeColumns(t *testing.T) {
	const nBlocks = 3
	const spansPerBlock = 5

	type spanSpec struct {
		name     string
		svcName  string
		spanAttr string // "span.http.method" value
		resAttr  string // "resource.env" value
		traceID  [16]byte
		spanID   [8]byte
		start    uint64
		end      uint64
	}

	// Build spans with fully distinct identifiers across all blocks.
	specs := make([]spanSpec, 0, nBlocks*spansPerBlock)
	for b := range nBlocks {
		for s := range spansPerBlock {
			idx := b*spansPerBlock + s
			var tid [16]byte
			var sid [8]byte
			copy(tid[:], makeTraceID(byte(idx+1)))
			copy(sid[:], makeSpanID(byte(idx+1)))
			specs = append(specs, spanSpec{
				traceID:  tid,
				spanID:   sid,
				name:     fmt.Sprintf("op-%d-%d", b, s),
				start:    uint64(idx)*1_000_000 + 1,
				end:      uint64(idx)*1_000_000 + 500_000,
				svcName:  fmt.Sprintf("svc-%d", b),
				spanAttr: fmt.Sprintf("GET-%d", idx),
				resAttr:  fmt.Sprintf("prod-%d", b),
			})
		}
	}

	// Write each block to a separate blockpack buffer.
	providers := make([]modules_rw.ReaderProvider, nBlocks)
	for b := range nBlocks {
		var tds []*tracev1.TracesData
		for _, sp := range specs[b*spansPerBlock : (b+1)*spansPerBlock] {
			span := &tracev1.Span{
				TraceId:           sp.traceID[:],
				SpanId:            sp.spanID[:],
				Name:              sp.name,
				StartTimeUnixNano: sp.start,
				EndTimeUnixNano:   sp.end,
				Attributes: []*commonv1.KeyValue{
					{
						Key:   "http.method",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: sp.spanAttr}},
					},
				},
			}
			// Also add an extra TracesData with resource attribute.
			td := &tracev1.TracesData{
				ResourceSpans: []*tracev1.ResourceSpans{
					{
						Resource: &resourcev1.Resource{
							Attributes: []*commonv1.KeyValue{
								{
									Key: "service.name",
									Value: &commonv1.AnyValue{
										Value: &commonv1.AnyValue_StringValue{StringValue: sp.svcName},
									},
								},
								{
									Key: "env",
									Value: &commonv1.AnyValue{
										Value: &commonv1.AnyValue_StringValue{StringValue: sp.resAttr},
									},
								},
							},
						},
						ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}},
					},
				},
			}
			tds = append(tds, td)
		}
		providers[b] = &memProvider{data: writeTestBlock(t, tds)}
	}

	// Compact all blocks.
	storage := newMemOutputStorage()
	ctx := context.Background()
	outputPaths, _, err := compaction.CompactBlocks(ctx, providers, compaction.Config{
		MaxSpansPerBlock: 100,
	}, storage)
	require.NoError(t, err)
	require.Len(t, outputPaths, 1, "expected exactly one output file")

	outputData := storage.files[outputPaths[0]]
	require.NotEmpty(t, outputData, "output file must not be empty")

	// Read back and collect all spans.
	r := openBlockReader(t, outputData)

	type gotSpan struct {
		name     string
		svcName  string
		spanAttr string
		resAttr  string
		traceID  [16]byte
		spanID   [8]byte
		start    uint64
		end      uint64
	}
	gotSpans := make(map[[8]byte]gotSpan) // keyed by spanID

	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		require.NoError(t, getErr)
		require.NotNil(t, bwb)

		block := bwb.Block
		for rowIdx := range block.SpanCount() {
			gs := gotSpan{}

			// Intrinsic columns are now in the intrinsic section, not block columns.
			if v, ok := r.IntrinsicBytesAt("trace:id", blockIdx, rowIdx); ok {
				copy(gs.traceID[:], v)
			}
			if v, ok := r.IntrinsicBytesAt("span:id", blockIdx, rowIdx); ok {
				copy(gs.spanID[:], v)
			}
			if v, ok := r.IntrinsicDictStringAt("span:name", blockIdx, rowIdx); ok {
				gs.name = v
			}
			if v, ok := r.IntrinsicUint64At("span:start", blockIdx, rowIdx); ok {
				gs.start = v
			}
			// span:end derived from start+duration
			if start, okS := r.IntrinsicUint64At("span:start", blockIdx, rowIdx); okS {
				if dur, okD := r.IntrinsicUint64At("span:duration", blockIdx, rowIdx); okD {
					gs.end = start + dur
				}
			}
			if v, ok := r.IntrinsicDictStringAt("resource.service.name", blockIdx, rowIdx); ok {
				gs.svcName = v
			}
			if col := block.GetColumn("span.http.method"); col != nil {
				if v, ok := col.StringValue(rowIdx); ok {
					gs.spanAttr = v
				}
			}
			if col := block.GetColumn("resource.env"); col != nil {
				if v, ok := col.StringValue(rowIdx); ok {
					gs.resAttr = v
				}
			}

			gotSpans[gs.spanID] = gs
		}
	}

	// Verify all expected spans are present with correct values.
	assert.Len(t, gotSpans, nBlocks*spansPerBlock, "span count mismatch after compaction")

	for _, sp := range specs {
		gs, found := gotSpans[sp.spanID]
		if !assert.True(t, found, "span %x not found in compacted output", sp.spanID) {
			continue
		}
		assert.Equal(t, sp.traceID, gs.traceID, "trace ID mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.name, gs.name, "span name mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.start, gs.start, "start time mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.end, gs.end, "end time mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.svcName, gs.svcName, "service name mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.spanAttr, gs.spanAttr, "span.http.method mismatch for span %x", sp.spanID)
		assert.Equal(t, sp.resAttr, gs.resAttr, "resource.env mismatch for span %x", sp.spanID)
	}
}

// TestCompactBlocks_NilOutputStorage verifies that passing a nil outputStorage returns an error.
func TestCompactBlocks_NilOutputStorage(t *testing.T) {
	span := &tracev1.Span{
		TraceId: makeTraceID(1),
		SpanId:  makeSpanID(1),
		Name:    "test",
	}
	block := writeTestBlock(t, []*tracev1.TracesData{tracesData("svc", span)})
	providers := []modules_rw.ReaderProvider{&memProvider{data: block}}

	_, _, err := compaction.CompactBlocks(context.Background(), providers, compaction.Config{}, nil)
	assert.Error(t, err, "nil outputStorage must return an error")
}

// TestCompactBlocks_EmptyProviders verifies that zero providers returns no output and no error.
func TestCompactBlocks_EmptyProviders(t *testing.T) {
	paths, dropped, err := compaction.CompactBlocks(
		context.Background(),
		nil,
		compaction.Config{},
		newMemOutputStorage(),
	)
	require.NoError(t, err)
	assert.Empty(t, paths)
	assert.Equal(t, int64(0), dropped)
}

// TestCompactBlocks_DroppedSpans verifies that spans with missing or invalid trace:id / span:id
// are silently dropped and counted in the droppedSpans return value, while valid spans still
// appear in the output.
//
// TEST-COMP-05
func TestCompactBlocks_DroppedSpans(t *testing.T) {
	// validSpan has proper 16-byte traceID and 8-byte spanID.
	validSpan := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(1),
		Name:              "valid-span",
		StartTimeUnixNano: 1_000_000,
		EndTimeUnixNano:   2_000_000,
	}
	// noIDSpan has no TraceId/SpanId set — will be dropped.
	noIDSpan := &tracev1.Span{
		Name:              "no-id-span",
		StartTimeUnixNano: 3_000_000,
		EndTimeUnixNano:   4_000_000,
	}
	// shortTraceIDSpan has a TraceId that is not 16 bytes — will be dropped.
	shortTraceIDSpan := &tracev1.Span{
		TraceId:           []byte{0x01, 0x02}, // only 2 bytes, not 16
		SpanId:            makeSpanID(2),
		Name:              "short-trace-id",
		StartTimeUnixNano: 5_000_000,
		EndTimeUnixNano:   6_000_000,
	}

	block := writeTestBlock(t, []*tracev1.TracesData{
		tracesData("svc-drop", validSpan),
		tracesData("svc-drop", noIDSpan),
		tracesData("svc-drop", shortTraceIDSpan),
	})

	providers := []modules_rw.ReaderProvider{&memProvider{data: block}}
	storage := newMemOutputStorage()

	outputPaths, dropped, err := compaction.CompactBlocks(context.Background(), providers, compaction.Config{}, storage)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, dropped, int64(2), "at least 2 invalid spans must be dropped")

	require.Len(t, outputPaths, 1, "expected one output file")
	r := openBlockReader(t, storage.files[outputPaths[0]])

	totalSpans := 0
	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		require.NoError(t, getErr)
		require.NotNil(t, bwb)
		totalSpans += bwb.Block.SpanCount()
	}
	assert.Equal(t, 1, totalSpans, "only the valid span must appear in output")
}

// TestCompactBlocks_ContextCancellation verifies that a pre-canceled context causes
// CompactBlocks to return a context error before processing any providers.
//
// TEST-COMP-06
func TestCompactBlocks_ContextCancellation(t *testing.T) {
	span := &tracev1.Span{
		TraceId: makeTraceID(1),
		SpanId:  makeSpanID(1),
		Name:    "ctx-span",
	}
	block := writeTestBlock(t, []*tracev1.TracesData{tracesData("svc", span)})
	providers := []modules_rw.ReaderProvider{&memProvider{data: block}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before calling CompactBlocks

	_, _, err := compaction.CompactBlocks(ctx, providers, compaction.Config{}, newMemOutputStorage())
	assert.Error(t, err, "canceled context must return an error")
	assert.ErrorIs(t, err, context.Canceled)
}

// TestCompactBlocks_MaxOutputFileSize verifies that setting MaxOutputFileSize to a small value
// causes multiple output files to be produced when input data exceeds that limit.
//
// TEST-COMP-07
func TestCompactBlocks_MaxOutputFileSize(t *testing.T) {
	// Write enough spans so that after the first one is written the estimated size
	// exceeds MaxOutputFileSize=1, forcing a new output file for each subsequent span.
	const numSpans = 5
	tds := make([]*tracev1.TracesData, 0, numSpans)
	for i := range numSpans {
		tds = append(tds, tracesData("svc-rotate", &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              fmt.Sprintf("span-%d", i),
			StartTimeUnixNano: uint64(i)*1_000_000 + 1,
			EndTimeUnixNano:   uint64(i)*1_000_000 + 500_000,
		}))
	}

	block := writeTestBlock(t, tds)
	providers := []modules_rw.ReaderProvider{&memProvider{data: block}}
	storage := newMemOutputStorage()

	// MaxOutputFileSize=1 causes rotation after every span (estimated size is 2048 bytes/span).
	outputPaths, _, err := compaction.CompactBlocks(context.Background(), providers, compaction.Config{
		MaxOutputFileSize: 1,
	}, storage)
	require.NoError(t, err)
	assert.Greater(
		t,
		len(outputPaths),
		1,
		"multiple output files must be produced when MaxOutputFileSize is very small",
	)

	// Verify all spans are present across the output files.
	totalSpans := 0
	for _, path := range outputPaths {
		r := openBlockReader(t, storage.files[path])
		for blockIdx := range r.BlockCount() {
			bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
			require.NoError(t, getErr)
			require.NotNil(t, bwb)
			totalSpans += bwb.Block.SpanCount()
		}
	}
	assert.Equal(t, numSpans, totalSpans, "all spans must be present across all output files")
}

// TestCompactBlocks_NativeColumns_Dedup verifies that duplicate spans (same trace+span ID
// across multiple input blocks) appear only once in the compacted output.
func TestCompactBlocks_NativeColumns_Dedup(t *testing.T) {
	span := &tracev1.Span{
		TraceId:           makeTraceID(1),
		SpanId:            makeSpanID(1),
		Name:              "dup-op",
		StartTimeUnixNano: 1_000_000,
		EndTimeUnixNano:   2_000_000,
	}
	td := tracesData("svc-dedup", span)

	// Write the same span in two separate blocks.
	block1 := writeTestBlock(t, []*tracev1.TracesData{td})
	block2 := writeTestBlock(t, []*tracev1.TracesData{td})

	providers := []modules_rw.ReaderProvider{
		&memProvider{data: block1},
		&memProvider{data: block2},
	}

	storage := newMemOutputStorage()
	outputPaths, _, err := compaction.CompactBlocks(context.Background(), providers, compaction.Config{}, storage)
	require.NoError(t, err)
	require.Len(t, outputPaths, 1)

	r := openBlockReader(t, storage.files[outputPaths[0]])
	totalSpans := 0
	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		require.NoError(t, getErr)
		require.NotNil(t, bwb)
		totalSpans += bwb.Block.SpanCount()
	}
	assert.Equal(t, 1, totalSpans, "duplicate spans must be deduplicated to exactly 1")
}
