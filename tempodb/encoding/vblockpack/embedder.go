package vblockpack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// textEmbedder is the interface accepted by blockpack.WriterConfig.Embedder and
// blockpack.QueryOptions.Embedder. Both internal interfaces (writer.TextEmbedder and
// vm.TextEmbedder) require exactly this method, so Go structural typing satisfies both.
type textEmbedder interface {
	Embed(text string) ([]float32, error)
}

var (
	processEmbedder     textEmbedder
	processEmbedderOnce sync.Once
	configuredEmbedURL  string // set by ConfigureEmbedding
)

// ConfigureEmbedding stores the embedding server URL for process-level use.
// Called from block creation and compaction init paths.
func ConfigureEmbedding(url string) {
	configuredEmbedURL = url
}

// getProcessEmbedder returns a process-level embedder for the configured URL.
// Returns nil if no URL is configured (embedding disabled). Thread-safe;
// initializes the embedder exactly once.
func getProcessEmbedder(url string) textEmbedder {
	if url == "" {
		return nil
	}
	processEmbedderOnce.Do(func() {
		emb, err := newHTTPEmbedder(url)
		if err != nil {
			slog.Error("vblockpack: failed to initialize embedder", "url", url, "err", err)
			return
		}
		slog.Info("vblockpack: embedder initialized", "url", url, "dim", emb.dim)
		processEmbedder = emb
	})
	return processEmbedder
}

// httpEmbedder implements textEmbedder via a TEI-compatible HTTP embedding server.
type httpEmbedder struct {
	client    *http.Client
	serverURL string
	dim       int
}

type teiRequest struct {
	Inputs    []string `json:"inputs"`
	Normalize bool     `json:"normalize"`
}

const defaultHTTPEmbedTimeout = 30 * time.Second

// newHTTPEmbedder creates an httpEmbedder by probing the server to determine vector dimension.
func newHTTPEmbedder(serverURL string) (*httpEmbedder, error) {
	e := &httpEmbedder{
		serverURL: serverURL,
		client:    &http.Client{Timeout: defaultHTTPEmbedTimeout},
	}
	// Probe with a single short text to determine dimension.
	vecs, err := e.embedBatch([]string{"probe"})
	if err != nil {
		return nil, fmt.Errorf("embedder http: probe server %q: %w", serverURL, err)
	}
	if len(vecs) == 0 || len(vecs[0]) == 0 {
		return nil, fmt.Errorf("embedder http: probe returned empty vectors")
	}
	e.dim = len(vecs[0])
	return e, nil
}

// Embed encodes a single text string, satisfying both vm.TextEmbedder and writer.TextEmbedder.
func (e *httpEmbedder) Embed(text string) ([]float32, error) {
	vecs, err := e.embedBatch([]string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("embedder http: Embed returned no vectors")
	}
	return vecs[0], nil
}

// embedBatch posts texts to POST /embed using the TEI protocol and returns vectors.
func (e *httpEmbedder) embedBatch(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	reqBody, err := json.Marshal(teiRequest{Inputs: texts, Normalize: true})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	url := e.serverURL + "/embed"
	httpResp, err := e.client.Post(url, "application/json", bytes.NewReader(reqBody)) //nolint:noctx
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() { _ = httpResp.Body.Close() }()
	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1024)) //nolint:gomnd
		return nil, fmt.Errorf("POST %s: %s — %s", url, httpResp.Status, string(body))
	}
	const maxResponseBytes = 64 << 20 // 64 MB
	limited := io.LimitReader(httpResp.Body, maxResponseBytes)
	var vecs [][]float32
	if err := json.NewDecoder(limited).Decode(&vecs); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return vecs, nil
}
