package writer_test

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// testEmbedder is a deterministic TextEmbedder for testing.
// It returns a fixed-dimension vector whose first element encodes the text length,
// making each distinct text produce a unique (but predictable) embedding.
type testEmbedder struct {
	dim int
}

func (e *testEmbedder) Embed(text string) ([]float32, error) {
	vecs, err := e.EmbedBatch([]string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

func (e *testEmbedder) EmbedBatch(texts []string) ([][]float32, error) {
	vecs := make([][]float32, len(texts))
	for i, text := range texts {
		vec := make([]float32, e.dim)
		if len(text) > 0 {
			vec[0] = float32(len(text))
		}
		for j := 1; j < e.dim; j++ {
			vec[j] = float32(j) * 0.01
		}
		vecs[i] = vec
	}
	return vecs, nil
}

// TestWriter_autoEmbed verifies V14 behavior when Config.Embedder is set:
//  1. NewWriterWithConfig probes the embedder and sets VectorDimension automatically.
//  2. V14 auto-embeds spans — each span gets an __embedding__ column entry.
//  3. The __embedding__ column is present in all blocks after Flush.
func TestWriter_autoEmbed(t *testing.T) {
	const dim = 8
	emb := &testEmbedder{dim: dim}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: 5, // force multiple blocks with 12 spans
		Embedder:      emb,
		// VectorDimension intentionally left 0 — must be auto-detected via probe
	})
	require.NoError(t, err, "NewWriterWithConfig must succeed when Embedder is set")

	// Add 12 spans across multiple blocks (MaxBlockSpans=5 → 3 blocks).
	for i := range 12 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "auto-embed-span",
			StartTimeUnixNano: 1_000_000_000 + uint64(i)*1000, //nolint:gosec // test values, no overflow risk
			EndTimeUnixNano:   2_000_000_000 + uint64(i)*1000, //nolint:gosec // test values, no overflow risk
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
		}
		td := makeTracesData("embed-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td), "AddTracesData span %d", i)
	}

	_, err = w.Flush()
	require.NoError(t, err, "Flush must succeed")

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err, "must read back the written file")

	// V14 auto-embeds spans — the __embedding__ column must be present in every block.
	wantCols := map[string]struct{}{modules_shared.EmbeddingColumnName: {}}
	blockCount := r.BlockCount()
	assert.Greater(t, blockCount, 1, "expect multiple blocks for 12 spans with MaxBlockSpans=5")

	for i := range blockCount {
		raw, err := r.ReadBlockRaw(i)
		require.NoError(t, err, "block %d ReadBlockRaw", i)

		bwb, err := r.ParseBlockFromBytes(raw, wantCols, r.BlockMeta(i))
		require.NoError(t, err, "block %d ParseBlockFromBytes", i)

		col := bwb.Block.GetColumn(modules_shared.EmbeddingColumnName)
		assert.NotNil(t, col, "block %d: __embedding__ column must be present when Embedder is set", i)
	}
}

// TestWriter_autoEmbed_nilEmbedder verifies that when Config.Embedder is nil,
// the writer does not build an __embedding__ column (existing behavior preserved).
func TestWriter_autoEmbed_nilEmbedder(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: 10,
		Embedder:      nil,
	})
	require.NoError(t, err)

	for i := range 5 {
		traceID := makeTraceID(byte(i + 1))
		spanID := makeSpanID(byte(i + 1))
		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "no-embed-span",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		}
		td := makeTracesData("no-embed-svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)

	prov := &writerTestMemProvider{data: buf.Bytes()}
	r, err := modules_reader.NewReaderFromProvider(prov)
	require.NoError(t, err)

	// VectorIndex must be nil — no embedder, no VectorDimension.
	vi, err := r.VectorIndex()
	require.NoError(t, err)
	assert.Nil(t, vi, "VectorIndex must be nil when Embedder is nil")

	// No __embedding__ column should appear in any block.
	wantCols := map[string]struct{}{modules_shared.EmbeddingColumnName: {}}
	for i := range r.BlockCount() {
		raw, err := r.ReadBlockRaw(i)
		require.NoError(t, err)
		bwb, err := r.ParseBlockFromBytes(raw, wantCols, r.BlockMeta(i))
		require.NoError(t, err)
		col := bwb.Block.GetColumn(modules_shared.EmbeddingColumnName)
		assert.Nil(t, col, "block %d: __embedding__ column must be absent when Embedder is nil", i)
	}
}
