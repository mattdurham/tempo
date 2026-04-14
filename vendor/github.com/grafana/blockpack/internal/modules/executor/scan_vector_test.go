package executor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// float32ToLE encodes a float32 slice as little-endian bytes.
func float32ToLE(vec []float32) []byte {
	b := make([]byte, len(vec)*4)
	for i, f := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// makeEmbedAttr creates an OTLP KeyValue with the __embedding__ bytes attribute.
func makeEmbedAttr(vec []float32) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key: modules_shared.EmbeddingColumnName,
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_BytesValue{BytesValue: float32ToLE(vec)},
		},
	}
}

// buildVectorBlock writes spans (some with embeddings) and returns the first Block.
type vecSpanDef struct {
	svcName string
	name    string
	vec     []float32 // nil = no embedding
}

func buildVectorBlock(t *testing.T, dim int, spans []vecSpanDef) *modules_reader.Block { //nolint:unparam
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:    &buf,
		MaxBlockSpans:   0,
		VectorDimension: dim,
	})
	require.NoError(t, err)

	traceID := [16]byte{0xAA}
	for i, s := range spans {
		attrs := []*commonv1.KeyValue{}
		if s.vec != nil {
			attrs = append(attrs, makeEmbedAttr(s.vec))
		}
		sp := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              s.name,
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Attributes:        attrs,
		}
		svcName := s.svcName
		if svcName == "" {
			svcName = "svc"
		}
		require.NoError(t, w.AddSpan(traceID[:], sp, map[string]any{"service.name": svcName}, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&memProv{data: buf.Bytes()})
	require.NoError(t, err)
	require.Greater(t, r.BlockCount(), 0)
	wantCols := map[string]struct{}{modules_shared.EmbeddingColumnName: {}}
	bwb, err := r.GetBlockWithBytes(0, wantCols, nil)
	require.NoError(t, err)
	return bwb.Block
}

// l2Normalize normalizes a vector for testing.
func l2NormalizeVec(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	norm := float32(1.0 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * norm
	}
	return out
}

// buildProgramWithVec builds a test Program with VectorScorer for __embedding__.
func buildProgramWithVec(queryVec []float32, threshold float32) *vm.Program {
	return &vm.Program{
		HasVector:    true,
		QueryVector:  queryVec,
		VectorColumn: modules_shared.EmbeddingColumnName,
		VectorLimit:  10,
		VectorScorer: func(getVec func(int) ([]float32, bool), candidates vm.RowSet) []vm.ScoredRow {
			var scored []vm.ScoredRow
			for _, idx := range candidates.ToSlice() {
				vec, ok := getVec(idx)
				if !ok {
					continue
				}
				var dot, normA, normB float32
				for i := range queryVec {
					dot += queryVec[i] * vec[i]
					normA += queryVec[i] * queryVec[i]
					normB += vec[i] * vec[i]
				}
				var sim float32
				if normA > 0 && normB > 0 {
					sim = dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
				}
				if sim >= threshold {
					scored = append(scored, vm.ScoredRow{RowIdx: idx, Score: sim})
				}
			}
			return scored
		},
		ColumnPredicate: func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.FullScan(), nil
		},
	}
}

// fullRowSet returns a RowSet containing all rows [0, n).
func fullRowSet(n int) vm.RowSet {
	rs := newRowSet()
	for i := range n {
		rs.Add(i)
	}
	return rs
}

// --- applyVectorScorerToBlock tests ---

func TestApplyVectorScorerToBlock_NoEmbeddingColumn(t *testing.T) {
	// Block with no embedding column — applyVectorScorerToBlock should return nil.
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	queryVec := l2NormalizeVec([]float32{1, 0, 0})
	program := buildProgramWithVec(queryVec, 0.0)
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	// No embedding column → nil result (VectorScorer gets nil getVec → returns empty from col check).
	assert.Empty(t, result, "expected no scored rows when block has no embedding column")
}

func TestApplyVectorScorerToBlock_AboveThreshold(t *testing.T) {
	// Two spans, both above threshold (threshold=0), both match.
	dim := 3
	v1 := l2NormalizeVec([]float32{1, 0, 0})
	v2 := l2NormalizeVec([]float32{0.9, 0.1, 0})
	queryVec := l2NormalizeVec([]float32{1, 0, 0})

	block := buildVectorBlock(t, dim, []vecSpanDef{
		{name: "s1", vec: v1},
		{name: "s2", vec: v2},
	})
	program := buildProgramWithVec(queryVec, 0.0)
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	assert.Equal(t, 2, len(result), "both spans should be scored with threshold 0")
}

func TestApplyVectorScorerToBlock_BelowThresholdExcluded(t *testing.T) {
	// v1 identical to query (sim ~1.0), v2 orthogonal (sim ~0); threshold=0.5.
	dim := 3
	v1 := l2NormalizeVec([]float32{1, 0, 0})
	v2 := l2NormalizeVec([]float32{0, 1, 0}) // orthogonal
	queryVec := l2NormalizeVec([]float32{1, 0, 0})

	block := buildVectorBlock(t, dim, []vecSpanDef{
		{name: "s1", vec: v1},
		{name: "s2", vec: v2},
	})
	program := buildProgramWithVec(queryVec, 0.5)
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	require.Equal(t, 1, len(result), "only high-similarity span should match")
	assert.Equal(t, 0, result[0].RowIdx, "span 0 (identical vector) should match")
	assert.InDelta(t, 1.0, float64(result[0].Score), 0.001, "score should be ~1.0")
}

func TestApplyVectorScorerToBlock_MixedPresence(t *testing.T) {
	// Three spans: s1 has embedding, s2 does not, s3 has embedding — 2 should score.
	dim := 3
	v1 := l2NormalizeVec([]float32{1, 0, 0})
	v3 := l2NormalizeVec([]float32{1, 0, 0})
	queryVec := l2NormalizeVec([]float32{1, 0, 0})

	block := buildVectorBlock(t, dim, []vecSpanDef{
		{name: "s1", vec: v1},
		{name: "s2", vec: nil}, // no embedding
		{name: "s3", vec: v3},
	})
	program := buildProgramWithVec(queryVec, 0.0)
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	assert.Equal(t, 2, len(result), "2 spans have embeddings, both should score")
}

func TestApplyVectorScorerToBlock_OnlyCandidatesScored(t *testing.T) {
	// 3 spans with embeddings; candidates excludes row 1. Only rows 0 and 2 scored.
	dim := 3
	vecs := [][]float32{
		l2NormalizeVec([]float32{1, 0, 0}),
		l2NormalizeVec([]float32{1, 0, 0}),
		l2NormalizeVec([]float32{1, 0, 0}),
	}
	queryVec := l2NormalizeVec([]float32{1, 0, 0})

	block := buildVectorBlock(t, dim, []vecSpanDef{
		{name: "s1", vec: vecs[0]},
		{name: "s2", vec: vecs[1]},
		{name: "s3", vec: vecs[2]},
	})
	// Candidates: only rows 0 and 2.
	candidates := newRowSet()
	candidates.Add(0)
	candidates.Add(2)

	program := buildProgramWithVec(queryVec, 0.0)
	result := applyVectorScorerToBlock(block, program, candidates)
	rowIdxs := make([]int, len(result))
	for i, sr := range result {
		rowIdxs[i] = sr.RowIdx
	}
	assert.NotContains(t, rowIdxs, 1, "row 1 was not a candidate and must not be scored")
	assert.Equal(t, 2, len(result), "exactly the 2 candidate rows should be scored")
}

func TestApplyVectorScorerToBlock_NilScorer(t *testing.T) {
	// program.VectorScorer == nil → must return nil without panicking.
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	program := &vm.Program{HasVector: false}
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	assert.Nil(t, result)
}

// --- vectorTopKFromScoredRows tests ---

func TestVectorTopKFromScoredRows_ReturnsTopK(t *testing.T) {
	// 20 rows with varying scores; top-10 should be returned.
	rows := make([]MatchedRow, 20)
	for i := range 20 {
		rows[i] = MatchedRow{RowIdx: i, Score: float32(20-i) / 20.0}
	}

	result := vectorTopKFromScoredRows(rows, 10)
	assert.Equal(t, 10, len(result), "top-10 should be returned from 20 candidates")
	for i := 1; i < len(result); i++ {
		assert.GreaterOrEqual(t, result[i-1].Score, result[i].Score,
			"scores must be non-increasing: result[%d].Score=%f, result[%d].Score=%f",
			i-1, result[i-1].Score, i, result[i].Score)
	}
}

func TestVectorTopKFromScoredRows_SortedDescending(t *testing.T) {
	rows := []MatchedRow{
		{RowIdx: 0, Score: 0.5},
		{RowIdx: 1, Score: 1.0},
		{RowIdx: 2, Score: 0.7},
	}
	result := vectorTopKFromScoredRows(rows, 10)
	require.Equal(t, 3, len(result))
	assert.Equal(t, float32(1.0), result[0].Score)
	assert.Equal(t, float32(0.7), result[1].Score)
	assert.Equal(t, float32(0.5), result[2].Score)
}

func TestVectorTopKFromScoredRows_UsesDefaultLimitWhenZero(t *testing.T) {
	rows := make([]MatchedRow, 15)
	for i := range 15 {
		rows[i] = MatchedRow{RowIdx: i, Score: float32(i) / 15.0}
	}

	result := vectorTopKFromScoredRows(rows, 0) // 0 → DefaultVectorLimit (10)
	assert.Equal(t, vm.DefaultVectorLimit, len(result),
		"limit=0 should use DefaultVectorLimit=%d", vm.DefaultVectorLimit)
}

func TestVectorTopKFromScoredRows_LimitLargerThanRows(t *testing.T) {
	rows := []MatchedRow{
		{RowIdx: 0, Score: 0.9},
		{RowIdx: 1, Score: 0.8},
	}
	result := vectorTopKFromScoredRows(rows, 10)
	assert.Equal(t, 2, len(result), "all rows returned when limit > count")
}

// --- applyVectorScorerToBlock using the actual vectormath/CosineDistance path ---

func TestApplyVectorScorerToBlock_ScoresMatch(t *testing.T) {
	// Verify Score on returned ScoredRow is approximately cosine similarity to query.
	dim := 3
	v1 := l2NormalizeVec([]float32{1, 0, 0})
	queryVec := l2NormalizeVec([]float32{1, 0, 0})

	block := buildVectorBlock(t, dim, []vecSpanDef{
		{name: "s1", vec: v1},
	})
	program := buildProgramWithVec(queryVec, 0.0)
	candidates := fullRowSet(block.SpanCount())
	result := applyVectorScorerToBlock(block, program, candidates)
	require.Equal(t, 1, len(result))
	assert.InDelta(t, 1.0, float64(result[0].Score), 0.001, "identical vectors → similarity ~1.0")
}

// Compile-time check: blockColumnProvider satisfies vm.ColumnDataProvider.
var _ vm.ColumnDataProvider = (*blockColumnProvider)(nil)

// Verify that the embedding format round-trips through float32ToLE.
func TestFloat32ToLE_RoundTrip(t *testing.T) {
	vec := []float32{1.0, 2.5, -0.5, 0.0}
	encoded := float32ToLE(vec)
	require.Equal(t, len(vec)*4, len(encoded))
	for i, f := range vec {
		bits := binary.LittleEndian.Uint32(encoded[i*4:])
		got := math.Float32frombits(bits)
		assert.Equal(t, f, got)
	}
}

func TestVectorTopKFromScoredRows_EmptyInput(t *testing.T) {
	result := vectorTopKFromScoredRows(nil, 10)
	assert.Empty(t, result)
}

// --- compile-time helper: ensure fmt is used ---
var _ = fmt.Sprintf
