// Package embedder wraps embedding backends for text-to-vector inference.
// It converts text fields from spans into float32 embedding vectors.
//
// Usage pattern (yzma/llama.cpp backend):
//
//	e, err := embedder.New(embedder.Config{
//	    LibPath:   "/usr/local/lib/libllama.so",
//	    ModelPath: "/models/nomic-embed-text-v1.5.Q8_0.gguf",
//	    BatchSize: 512,
//	    Fields: []embedder.EmbeddingField{
//	        {Name: "span.name",    Weight: "primary"},
//	        {Name: "service.name", Weight: "context"},
//	        {Name: "span.status",  Weight: "secondary"},
//	    },
//	})
//	vecs, err := e.EmbedBatch([]string{"GET /api/users", "POST /api/orders"})
//
// Usage pattern (HTTP backend):
//
//	e, err := embedder.NewHTTP(embedder.HTTPConfig{
//	    ServerURL: "http://localhost:8765",
//	    Fields:    []embedder.EmbeddingField{{Name: "span.name", Weight: "primary"}},
//	})
package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

// FieldWeight controls how a field contributes to the assembled text.
const (
	// WeightPrimary — field value is placed first as plain text.
	WeightPrimary = "primary"
	// WeightContext — field value is placed after primary as "key=value".
	WeightContext = "context"
	// WeightSecondary — field value is placed last as "key=value".
	WeightSecondary = "secondary"
)

// EmbeddingField describes one span field to include in the embedding text.
type EmbeddingField struct {
	// Name is the column name to read (e.g. "span.name", "resource.service.name").
	Name string
	// Weight controls inclusion order: "primary", "context", "secondary".
	Weight string
}

// Config configures an Embedder instance backed by yzma (llama.cpp).
type Config struct {
	// LibPath is the absolute path to the llama.cpp shared library (e.g. "/usr/lib/libllama.so").
	LibPath string
	// ModelPath is the absolute path to the GGUF model file.
	// If empty, DefaultModelPath() is used (~/.blockpack/models/nomic-embed-text-v1.5.Q8_0.gguf).
	ModelPath string
	// ModelURL is the URL to download the model from when AutoDownload is true.
	// Default: DefaultModelURL (HuggingFace nomic-embed-text-v1.5 Q8_0 GGUF).
	ModelURL string
	// Fields defines which span fields to include and their ordering weight.
	Fields []EmbeddingField
	// BatchSize is the number of tokens per inference batch (default: 512).
	BatchSize int
	// MaxTextLength is the maximum character length of assembled text (default: 2048).
	MaxTextLength int
	// AutoDownload enables automatic model download on first use.
	// When true and ModelPath doesn't exist, the model is downloaded from ModelURL.
	// Default: true when ModelPath is empty.
	AutoDownload bool
}

// Embedder provides text-to-vector embedding via a pluggable Backend.
// Create with New (yzma/llama.cpp) or NewWithBackend (custom backend).
// Call Close when done to free backend resources.
type Embedder struct {
	backend Backend
	cfg     Config
}

const (
	defaultBatchSize   = 512
	defaultMaxTextLen  = 24000 // ~24KB — fits within nomic-embed-text-v1.5's 8192 token window for JSON content
	defaultContextSize = 2048
)

// New loads the llama.cpp shared library and the GGUF model, then initializes
// an embedding context backed by yzma. Returns error if the library or model cannot be loaded.
//
// If cfg.ModelPath is empty, DefaultModelPath() is used and AutoDownload is set to true
// automatically. If cfg.AutoDownload is true and the model file does not exist,
// EnsureModel is called to download it before loading.
func New(cfg Config) (*Embedder, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.MaxTextLength <= 0 {
		cfg.MaxTextLength = defaultMaxTextLen
	}
	if cfg.ModelPath == "" {
		cfg.ModelPath = DefaultModelPath()
		cfg.AutoDownload = true
	}
	if cfg.AutoDownload {
		if err := EnsureModel(cfg); err != nil {
			return nil, fmt.Errorf("embedder: ensure model: %w", err)
		}
	}

	b, err := newYzmaBackend(cfg)
	if err != nil {
		return nil, err
	}

	return &Embedder{cfg: cfg, backend: b}, nil
}

// NewWithBackend creates an Embedder using a caller-supplied Backend implementation.
// fields and maxTextLength configure AssembleText; maxTextLength defaults to 2048 if <= 0.
func NewWithBackend(b Backend, fields []EmbeddingField, maxTextLength int) *Embedder {
	if maxTextLength <= 0 {
		maxTextLength = defaultMaxTextLen
	}
	return &Embedder{
		cfg:     Config{Fields: fields, MaxTextLength: maxTextLength},
		backend: b,
	}
}

// Close frees backend resources. Call when the Embedder is no longer needed.
func (e *Embedder) Close() {
	e.backend.Close()
}

// Dim returns the embedding dimension of the loaded model.
func (e *Embedder) Dim() int { return e.backend.Dim() }

// Embed encodes a single text string into a normalized float32 vector.
// Returns error if inference fails.
func (e *Embedder) Embed(text string) ([]float32, error) {
	vecs, err := e.EmbedBatch([]string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("embedder: Embed returned no vectors")
	}
	return vecs[0], nil
}

// EmbedBatch encodes each text and returns one L2-normalized float32 vector per input.
// Delegates to the backend; behavior depends on the backend implementation.
// Returns error if any inference fails.
func (e *Embedder) EmbedBatch(texts []string) ([][]float32, error) {
	return e.backend.EmbedBatch(texts)
}

// AssembleText builds the embedding text for a span from a map of field values.
// Fields are ordered: primary first (plain), then context ("key=value"), then secondary ("key=value").
// The result is truncated to cfg.MaxTextLength characters.
func (e *Embedder) AssembleText(fields map[string]string) string {
	return AssembleText(fields, e.cfg.Fields, e.cfg.MaxTextLength)
}

// AssembleText builds an embedding text string from a field value map, given a field spec and max length.
// This is exported for use in tests and in the writer embedding path.
// Fields are ordered: primary (plain), context ("key=value"), secondary ("key=value").
func AssembleText(fieldValues map[string]string, spec []EmbeddingField, maxLen int) string {
	if maxLen <= 0 {
		maxLen = defaultMaxTextLen
	}
	var primary, context, secondary []string
	for _, f := range spec {
		v, ok := fieldValues[f.Name]
		if !ok || v == "" {
			continue
		}
		switch f.Weight {
		case WeightPrimary:
			primary = append(primary, v)
		case WeightContext:
			context = append(context, f.Name+"="+v)
		default: // WeightSecondary and anything else
			secondary = append(secondary, f.Name+"="+v)
		}
	}

	parts := make([]string, 0, len(primary)+len(context)+len(secondary))
	parts = append(parts, primary...)
	parts = append(parts, context...)
	parts = append(parts, secondary...)

	text := strings.Join(parts, " ")
	if len(text) > maxLen {
		text = text[:maxLen]
		// Walk back to the last valid rune boundary to avoid splitting a multi-byte rune.
		for !utf8.Valid([]byte(text)) {
			text = text[:len(text)-1]
		}
	}
	return text
}

// l2Normalize returns a copy of vec scaled to unit length (L2 norm).
// If the norm is zero, returns the zero vector.
func l2Normalize(vec []float32) []float32 {
	var sum float64
	for _, v := range vec {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		out := make([]float32, len(vec))
		return out
	}
	norm := float32(1.0 / math.Sqrt(sum))
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[i] = v * norm
	}
	return out
}
