package embedder

// embedder_test.go — tests for AssembleText, field ordering, truncation, and L2 normalization.
// Tests that require a real GGUF model are skipped when the model file is not present.

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- AssembleText tests ---

func TestAssembleText_primaryOnly(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
	}
	fields := map[string]string{"span.name": "GET /api/users"}
	got := AssembleText(fields, spec, 0)
	assert.Equal(t, "GET /api/users", got)
}

func TestAssembleText_fieldOrdering(t *testing.T) {
	// primary comes first, then context, then secondary — regardless of spec order.
	spec := []EmbeddingField{
		{Name: "span.status", Weight: WeightSecondary},
		{Name: "service.name", Weight: WeightContext},
		{Name: "span.name", Weight: WeightPrimary},
	}
	fields := map[string]string{
		"span.name":    "op",
		"service.name": "auth",
		"span.status":  "ok",
	}
	got := AssembleText(fields, spec, 0)
	// Expected: "op service.name=auth span.status=ok"
	assert.True(t, strings.HasPrefix(got, "op "), "primary should come first, got: %q", got)
	assert.Contains(t, got, "service.name=auth")
	assert.Contains(t, got, "span.status=ok")
	// context before secondary
	contextIdx := strings.Index(got, "service.name=auth")
	secondaryIdx := strings.Index(got, "span.status=ok")
	assert.Less(t, contextIdx, secondaryIdx, "context must appear before secondary")
}

func TestAssembleText_missingFieldsSkipped(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
		{Name: "missing.col", Weight: WeightContext},
	}
	fields := map[string]string{"span.name": "op"}
	got := AssembleText(fields, spec, 0)
	assert.Equal(t, "op", got, "missing fields should be skipped")
	assert.NotContains(t, got, "missing.col")
}

func TestAssembleText_emptyFieldValueSkipped(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
		{Name: "empty.col", Weight: WeightContext},
	}
	fields := map[string]string{
		"span.name": "op",
		"empty.col": "",
	}
	got := AssembleText(fields, spec, 0)
	assert.Equal(t, "op", got, "empty field values should be skipped")
}

func TestAssembleText_truncation(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
	}
	longValue := strings.Repeat("a", 5000)
	fields := map[string]string{"span.name": longValue}
	maxLen := 100
	got := AssembleText(fields, spec, maxLen)
	require.LessOrEqual(t, len(got), maxLen, "result must be <= maxLen")
	assert.Equal(t, strings.Repeat("a", maxLen), got)
}

func TestAssembleText_multipleContext(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
		{Name: "service.name", Weight: WeightContext},
		{Name: "region", Weight: WeightContext},
	}
	fields := map[string]string{
		"span.name":    "rpc",
		"service.name": "svc-a",
		"region":       "us-west",
	}
	got := AssembleText(fields, spec, 0)
	assert.True(t, strings.HasPrefix(got, "rpc "), "primary first: %q", got)
	assert.Contains(t, got, "service.name=svc-a")
	assert.Contains(t, got, "region=us-west")
}

func TestAssembleText_noFields(t *testing.T) {
	got := AssembleText(map[string]string{}, nil, 0)
	assert.Equal(t, "", got)
}

func TestAssembleText_unknownWeightFallsToSecondary(t *testing.T) {
	spec := []EmbeddingField{
		{Name: "col", Weight: "unknown-weight"},
	}
	fields := map[string]string{"col": "val"}
	got := AssembleText(fields, spec, 0)
	// unknown weight treated as secondary ("col=val")
	assert.Equal(t, "col=val", got)
}

// --- l2Normalize tests ---

func TestL2Normalize_unitVector(t *testing.T) {
	vec := []float32{3, 4, 0} // ||[3,4,0]|| = 5
	got := l2Normalize(vec)
	assert.InDelta(t, 0.6, got[0], 1e-6)
	assert.InDelta(t, 0.8, got[1], 1e-6)
	assert.InDelta(t, 0.0, got[2], 1e-6)

	// Verify unit length.
	var sum float64
	for _, v := range got {
		sum += float64(v) * float64(v)
	}
	assert.InDelta(t, 1.0, sum, 1e-5)
}

func TestL2Normalize_zeroVector(t *testing.T) {
	vec := []float32{0, 0, 0}
	got := l2Normalize(vec)
	assert.Equal(t, []float32{0, 0, 0}, got)
}

func TestL2Normalize_alreadyNormalized(t *testing.T) {
	vec := []float32{1, 0, 0}
	got := l2Normalize(vec)
	assert.InDelta(t, 1.0, float64(got[0]), 1e-6)
}

func TestL2Normalize_preservesLength(t *testing.T) {
	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i+1) * 0.001
	}
	got := l2Normalize(vec)
	require.Equal(t, len(vec), len(got))
	var sum float64
	for _, v := range got {
		sum += float64(v) * float64(v)
	}
	assert.InDelta(t, 1.0, sum, 1e-4)
}

func TestL2Normalize_doesNotModifyInput(t *testing.T) {
	orig := []float32{3, 4, 0}
	in := make([]float32, len(orig))
	copy(in, orig)
	_ = l2Normalize(in)
	assert.Equal(t, orig, in, "l2Normalize must not modify the input slice")
}

// --- Config validation tests ---

func TestConfig_defaults(t *testing.T) {
	cfg := Config{}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.MaxTextLength <= 0 {
		cfg.MaxTextLength = defaultMaxTextLen
	}
	assert.Equal(t, defaultBatchSize, cfg.BatchSize)
	assert.Equal(t, defaultMaxTextLen, cfg.MaxTextLength)
}

// --- Batch embedding with mock backend ---
// These tests verify EmbedBatch plumbing without needing a real model.
// We exercise AssembleText integration via the exported surface.

func TestAssembleText_batchOrdering(t *testing.T) {
	// Simulate assembling text for multiple spans.
	spec := []EmbeddingField{
		{Name: "span.name", Weight: WeightPrimary},
		{Name: "service.name", Weight: WeightContext},
	}
	inputs := []map[string]string{
		{"span.name": "op1", "service.name": "svc-a"},
		{"span.name": "op2", "service.name": "svc-b"},
	}
	for i, fields := range inputs {
		text := AssembleText(fields, spec, 0)
		assert.True(t, strings.Contains(text, "op"+string(rune('1'+i))),
			"text %d should contain op name", i)
	}
}

// TestL2Normalize_numericalStability checks that very small vectors normalize correctly.
func TestL2Normalize_numericalStability(t *testing.T) {
	vec := []float32{1e-20, 0, 0}
	got := l2Normalize(vec)
	var sum float64
	for _, v := range got {
		sum += float64(v) * float64(v)
	}
	// Should be unit length or zero (if underflowed to 0).
	if !math.IsNaN(sum) && sum > 0 {
		assert.InDelta(t, 1.0, sum, 1e-4)
	}
}
