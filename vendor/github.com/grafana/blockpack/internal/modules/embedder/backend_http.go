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
	// MaxBatchSize is the maximum number of texts to send in a single POST /embed request.
	// 0 means no chunking (use the server's limit — may return 422 for large batches).
	// Defaults to 32 when set to 0 via NewHTTP, matching TEI's typical default limit.
	MaxBatchSize int
}

const defaultMaxBatchSize = 32 // TEI default max_batch_tokens / typical per-request limit

// httpBackend implements Backend by forwarding requests to an embedding server over HTTP.
// Supports the TEI protocol: POST /embed with {"inputs": [...], "normalize": true}
// Response: [[float, ...], ...] — flat array of vectors.
type httpBackend struct {
	client       *http.Client
	serverURL    string
	dim          int
	maxBatchSize int // max texts per request; 0 means no chunking
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
	maxBatch := cfg.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = defaultMaxBatchSize
	}
	b := &httpBackend{
		serverURL:    cfg.ServerURL,
		client:       &http.Client{Timeout: timeout},
		maxBatchSize: maxBatch,
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

// EmbedBatch sends texts to the server, chunking into maxBatchSize requests when needed.
// Returns one vector per input text in the same order.
func (b *httpBackend) EmbedBatch(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	chunkSize := b.maxBatchSize
	if chunkSize <= 0 {
		chunkSize = len(texts) // no chunking
	}

	all := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += chunkSize {
		end := start + chunkSize
		if end > len(texts) {
			end = len(texts)
		}
		chunk := texts[start:end]

		vecs, err := b.sendBatch(chunk)
		if err != nil {
			return nil, fmt.Errorf("embedder http: embed batch: %w", err)
		}
		if len(vecs) != len(chunk) {
			return nil, fmt.Errorf("embedder http: server returned %d vectors for %d texts", len(vecs), len(chunk))
		}
		if b.dim > 0 {
			for i, v := range vecs {
				if len(v) != b.dim {
					return nil, fmt.Errorf("embedder http: vector[%d] has length %d, expected %d", start+i, len(v), b.dim)
				}
			}
		}
		all = append(all, vecs...)
	}

	return all, nil
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
