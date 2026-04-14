package writer_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// makeFloat32BytesLE encodes a float32 slice as LE bytes for use as OTLP bytes attribute.
func makeFloat32BytesLE(vec []float32) []byte {
	b := make([]byte, len(vec)*4)
	for i, f := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// makeVecSpan creates an OTLP span with an __embedding__ bytes attribute encoding vec.
func makeVecSpan(traceID, spanID []byte, name string, start, end uint64, vec []float32, text string) *tracev1.Span {
	attrs := []*commonv1.KeyValue{}
	if len(vec) > 0 {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: modules_shared.EmbeddingColumnName,
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_BytesValue{BytesValue: makeFloat32BytesLE(vec)},
			},
		})
	}
	if text != "" {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: modules_shared.EmbeddingTextColumnName,
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: text},
			},
		})
	}
	return &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		Name:              name,
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Attributes:        attrs,
	}
}

// newVectorWriter creates a writer with VectorDimension set.
func newVectorWriter(t *testing.T, buf *bytes.Buffer, dim, maxBlockSpans int) *writer.Writer {
	t.Helper()
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:    buf,
		MaxBlockSpans:   maxBlockSpans,
		VectorDimension: dim,
	})
	require.NoError(t, err)
	return w
}

// addVecSpanToWriter adds one span with an embedding vector to the writer.
func addVecSpanToWriter(t *testing.T, w *writer.Writer, traceID, spanID []byte, vec []float32) {
	t.Helper()
	span := makeVecSpan(traceID, spanID, "op", 1000, 2000, vec, "test text")
	td := makeTracesData("svc", []*tracev1.Span{span})
	err := w.AddTracesData(td)
	require.NoError(t, err)
}

// vectorFlushWriter flushes the writer to the buffer.
func vectorFlushWriter(t *testing.T, w *writer.Writer) {
	t.Helper()
	_, err := w.Flush()
	require.NoError(t, err)
}

func TestWriter_noVectorDimension(t *testing.T) {
	// VectorDimension=0: no vector section written. V14 writer always produces V7 footer.
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// Standard span without embedding.
	span := makeVecSpan(makeTraceID(1), makeSpanID(1), "op", 1000, 2000, nil, "")
	td := makeTracesData("svc", []*tracev1.Span{span})
	require.NoError(t, w.AddTracesData(td))
	vectorFlushWriter(t, w)

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	vi, err := r.VectorIndex()
	require.NoError(t, err)
	assert.Nil(t, vi, "no vector index expected (V14 uses intrinsic __embedding__ column, not VectorIndex section)")
	assert.Equal(t, modules_shared.FooterV7Version, r.FooterVersion(), "V14 writer always produces V7 footer")
}

func TestWriter_vectorRoundTrip(t *testing.T) {
	// V14 writer stores embeddings as the __embedding__ intrinsic column (not VectorIndex section).
	const dim = 8
	var buf bytes.Buffer
	w := newVectorWriter(t, &buf, dim, 50)

	// Add 100 spans with unique embedding vectors across multiple blocks.
	for i := range 100 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		vec := make([]float32, dim)
		for d := range dim {
			vec[d] = float32(i) + float32(d)*0.01
		}
		addVecSpanToWriter(t, w, traceID, spanID, vec)
	}
	vectorFlushWriter(t, w)

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	// V14 does not write a VectorIndex section — embeddings are stored as __embedding__ column.
	vi, err := r.VectorIndex()
	require.NoError(t, err)
	assert.Nil(t, vi, "V14 writer does not produce a VectorIndex section")
	assert.Equal(t, modules_shared.FooterV7Version, r.FooterVersion(), "V14 writer produces V7 footer")

	// The __embedding__ column must be present in every block.
	wantCols := map[string]struct{}{modules_shared.EmbeddingColumnName: {}}
	blockCount := r.BlockCount()
	assert.Greater(t, blockCount, 0, "must have at least one block")
	for i := range blockCount {
		raw, readErr := r.ReadBlockRaw(i)
		require.NoError(t, readErr, "block %d ReadBlockRaw", i)
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantCols, r.BlockMeta(i))
		require.NoError(t, parseErr, "block %d ParseBlockFromBytes", i)
		col := bwb.Block.GetColumn(modules_shared.EmbeddingColumnName)
		assert.NotNil(t, col, "block %d: __embedding__ column must be present", i)
	}
}

func TestWriter_vectorV5FooterDetected(t *testing.T) {
	// V14 writer always produces FooterV7, regardless of VectorDimension setting.
	const dim = 4
	var buf bytes.Buffer
	w := newVectorWriter(t, &buf, dim, 0)

	for i := range 50 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		vec := make([]float32, dim)
		for d := range dim {
			vec[d] = float32(i) + float32(d)*0.1
		}
		addVecSpanToWriter(t, w, traceID, spanID, vec)
	}
	vectorFlushWriter(t, w)

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	assert.Equal(t, modules_shared.FooterV7Version, r.FooterVersion(), "V14 writer always produces V7 footer")
}

func TestWriter_fewVectors(t *testing.T) {
	// Fewer than pqM*pqK vectors: no panic, valid file produced.
	const dim = 16
	var buf bytes.Buffer
	w := newVectorWriter(t, &buf, dim, 0)

	// Add only 5 spans — fewer than pqM*pqK (96*256) required for full PQ.
	for i := range 5 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		vec := make([]float32, dim)
		for d := range dim {
			vec[d] = float32(i+1) + float32(d)
		}
		addVecSpanToWriter(t, w, traceID, spanID, vec)
	}
	vectorFlushWriter(t, w)

	// File must be readable.
	prov := &writerTestMemProvider{data: buf.Bytes()}
	_, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err, "file with few vectors must be readable")
}

func TestWriter_vectorParseEmbeddingColumn(t *testing.T) {
	// Verify that ParseBlockFromBytes can decode the __embedding__ column.
	const dim = 4
	var buf bytes.Buffer
	w := newVectorWriter(t, &buf, dim, 5)

	for i := range 12 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		vec := []float32{float32(i), float32(i) + 1, float32(i) + 2, float32(i) + 3}
		addVecSpanToWriter(t, w, traceID, spanID, vec)
	}
	vectorFlushWriter(t, w)

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	wantCols := map[string]struct{}{modules_shared.EmbeddingColumnName: {}}
	for i := range r.BlockCount() {
		raw, err := r.ReadBlockRaw(i)
		require.NoError(t, err, "block %d ReadBlockRaw", i)
		blk, err := r.ParseBlockFromBytes(raw, wantCols, r.BlockMeta(i))
		require.NoError(t, err, "block %d ParseBlockFromBytes", i)
		col := blk.Block.GetColumn(modules_shared.EmbeddingColumnName)
		require.NotNil(t, col, "block %d: embedding column missing", i)
	}
}
