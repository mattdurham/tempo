package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultHTTPTimeout = 30 * time.Second

// HTTPConfig configures the HTTP embedding backend.
type HTTPConfig struct {
	// ServerURL is the base URL of the embed-server (e.g. "http://localhost:8765").
	// Supports both TEI (HuggingFace Text Embeddings Inference) and custom servers.
	ServerURL string
	// Fields defines which span fields to include and their ordering weight.
	Fields []EmbeddingField
	// MaxTextLength is the maximum character length of assembled text (default: 24000).
	MaxTextLength int
	// Timeout is the HTTP request timeout. Defaults to 30s if zero.
	Timeout time.Duration
}

// httpBackend implements Backend by forwarding requests to an embedding server over HTTP.
// Supports the TEI protocol: POST /embed with {"inputs": [...], "normalize": true}
// Response: [[float, ...], ...] — flat array of vectors.
type httpBackend struct {
	client    *http.Client
	serverURL string
	dim       int
}

// teiRequest is the JSON request body for TEI's POST /embed.
type teiRequest struct {
	Inputs    []string `json:"inputs"`
	Normalize bool     `json:"normalize"`
}

// NewHTTP creates an Embedder backed by an HTTP embedding server (TEI or compatible).
// It sends a probe request on construction to determine Dim().
// Returns error if the server is unreachable or returns an unexpected response.
func NewHTTP(cfg HTTPConfig) (*Embedder, error) {
	if cfg.MaxTextLength <= 0 {
		cfg.MaxTextLength = defaultMaxTextLen
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	b := &httpBackend{
		serverURL: cfg.ServerURL,
		client:    &http.Client{Timeout: timeout},
	}

	// Probe with a single short text to determine dimension.
	vecs, err := b.sendBatch([]string{"probe"})
	if err != nil {
		return nil, fmt.Errorf("embedder http: probe server %q: %w", cfg.ServerURL, err)
	}
	if len(vecs) == 0 || len(vecs[0]) == 0 {
		return nil, fmt.Errorf("embedder http: probe returned empty vectors")
	}
	b.dim = len(vecs[0])

	return &Embedder{
		cfg:     Config{Fields: cfg.Fields, MaxTextLength: cfg.MaxTextLength},
		backend: b,
	}, nil
}

// Dim returns the embedding dimension determined during initialization.
func (b *httpBackend) Dim() int { return b.dim }

// Embed encodes a single text string.
func (b *httpBackend) Embed(text string) ([]float32, error) {
	vecs, err := b.EmbedBatch([]string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("embedder http: Embed returned no vectors")
	}
	return vecs[0], nil
}

// EmbedBatch sends all texts to the server in a single POST /embed request.
func (b *httpBackend) EmbedBatch(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	vecs, err := b.sendBatch(texts)
	if err != nil {
		return nil, fmt.Errorf("embedder http: embed batch: %w", err)
	}

	if len(vecs) != len(texts) {
		return nil, fmt.Errorf("embedder http: server returned %d vectors for %d texts", len(vecs), len(texts))
	}

	if b.dim > 0 {
		for i, v := range vecs {
			if len(v) != b.dim {
				return nil, fmt.Errorf("embedder http: vector[%d] has length %d, expected %d", i, len(v), b.dim)
			}
		}
	}

	return vecs, nil
}

// Close is a no-op for the HTTP backend.
func (b *httpBackend) Close() {}

// sendBatch posts texts to POST /embed using the TEI protocol and returns vectors.
func (b *httpBackend) sendBatch(texts []string) ([][]float32, error) {
	reqBody, err := json.Marshal(teiRequest{Inputs: texts, Normalize: true})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := b.serverURL + "/embed"
	httpResp, err := b.client.Post(url, "application/json", bytes.NewReader(reqBody)) //nolint:noctx
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() { _ = httpResp.Body.Close() }()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1024)) //nolint:gomnd
		return nil, fmt.Errorf("POST %s: %s — %s", url, httpResp.Status, string(body))
	}

	// TEI returns a flat JSON array of arrays: [[float, ...], ...]
	const maxResponseBytes = 64 << 20 // 64 MB
	limited := io.LimitReader(httpResp.Body, maxResponseBytes)
	var vecs [][]float32
	if err := json.NewDecoder(limited).Decode(&vecs); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return vecs, nil
}
