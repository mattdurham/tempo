package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const defaultHTTPTimeout = 120 * time.Second

// sendBatchMaxRetries is the number of times sendBatch retries on transient errors
// (timeout, connection reset, EOF, 5xx, 429) before propagating the error.
const sendBatchMaxRetries = 8

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
	// MaxConcurrentBatches is the maximum number of batch requests to issue concurrently.
	// When > 1, EmbedBatch sends multiple chunks in parallel, distributing load across
	// multiple embed-server pods. Defaults to 8 when set to 0 via NewHTTP.
	MaxConcurrentBatches int
}

const defaultMaxBatchSize = 32         // TEI default max_batch_tokens / typical per-request limit
const defaultMaxConcurrentBatches = 4  // concurrently hit up to 4 embed-server pods

// httpBackend implements Backend by forwarding requests to an embedding server over HTTP.
// Supports the TEI protocol: POST /embed with {"inputs": [...], "normalize": true}
// Response: [[float, ...], ...] — flat array of vectors.
type httpBackend struct {
	client               *http.Client
	serverURL            string
	dim                  int
	maxBatchSize         int // max texts per request; 0 means no chunking
	maxConcurrentBatches int // max parallel requests; 1 means sequential
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
	maxConc := cfg.MaxConcurrentBatches
	if maxConc <= 0 {
		maxConc = defaultMaxConcurrentBatches
	}
	b := &httpBackend{
		serverURL: cfg.ServerURL,
		client: &http.Client{
			Timeout: timeout,
			// Disable keep-alives so each request gets a fresh connection.
			// kube-proxy (iptables) load-balances per connection, not per request;
			// with keep-alives all batches would go to the same pod.
			Transport: &http.Transport{DisableKeepAlives: true},
		},
		maxBatchSize:         maxBatch,
		maxConcurrentBatches: maxConc,
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
// Up to maxConcurrentBatches chunks are sent in parallel to distribute load across pods.
// Returns one vector per input text in the same order.
func (b *httpBackend) EmbedBatch(texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	chunkSize := b.maxBatchSize
	if chunkSize <= 0 {
		chunkSize = len(texts) // no chunking
	}

	// Build chunk index: each entry is the start offset into texts.
	type chunk struct {
		start int
		end   int
	}
	var chunks []chunk
	for start := 0; start < len(texts); start += chunkSize {
		end := start + chunkSize
		if end > len(texts) {
			end = len(texts)
		}
		chunks = append(chunks, chunk{start, end})
	}

	// Pre-allocate result slice so goroutines can write by index without locking.
	results := make([]*[][]float32, len(chunks))
	errs := make([]error, len(chunks))

	// Semaphore to cap in-flight requests at maxConcurrentBatches.
	sem := make(chan struct{}, b.maxConcurrentBatches)
	var wg sync.WaitGroup
	for i, c := range chunks {
		i, c := i, c
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			vecs, err := b.sendBatch(texts[c.start:c.end])
			if err != nil {
				errs[i] = fmt.Errorf("chunk %d: %w", i, err)
				return
			}
			results[i] = &vecs
		}()
	}
	wg.Wait()

	// Check for errors; report the first one found.
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("embedder http: embed batch: %w", err)
		}
		vecs := *results[i]
		c := chunks[i]
		if len(vecs) != c.end-c.start {
			return nil, fmt.Errorf("embedder http: server returned %d vectors for %d texts", len(vecs), c.end-c.start)
		}
		if b.dim > 0 {
			for j, v := range vecs {
				if len(v) != b.dim {
					return nil, fmt.Errorf("embedder http: vector[%d] has length %d, expected %d", c.start+j, len(v), b.dim)
				}
			}
		}
	}

	// Flatten results in order.
	all := make([][]float32, 0, len(texts))
	for i := range chunks {
		all = append(all, *results[i]...)
	}
	return all, nil
}

// Close is a no-op for the HTTP backend.
func (b *httpBackend) Close() {}

// sendBatch posts texts to POST /embed using the TEI protocol and returns vectors.
// Retries up to sendBatchMaxRetries times on transient network errors or 5xx responses,
// with a short backoff between attempts.
func (b *httpBackend) sendBatch(texts []string) ([][]float32, error) {
	reqBody, err := json.Marshal(teiRequest{Inputs: texts, Normalize: true})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := b.serverURL + "/embed"

	var lastErr error
	var lastWas429 bool
	for attempt := range sendBatchMaxRetries {
		if attempt > 0 {
			if lastWas429 {
				// Short jittered backoff for TEI overload.
				// Base: 150ms×attempt (150ms, 300ms, 450ms...).
				// Jitter: ±50% to prevent thundering-herd when many batches
				// retry simultaneously after the same burst of 429 responses.
				base := time.Duration(150*attempt) * time.Millisecond
				jitter := time.Duration(rand.Int63n(int64(base/2+1))) //nolint:gosec
				time.Sleep(base + jitter - jitter/2)
			} else {
				// Backoff: 2s, 5s, 10s, 17s — gives crashing pods time to restart and
				// become Ready before kube-proxy routes new connections to them.
				time.Sleep(time.Duration(attempt*attempt+1) * time.Second)
			}
		}

		vecs, err := b.doPost(url, reqBody)
		if err == nil {
			return vecs, nil
		}
		lastErr = err
		lastWas429 = false

		// Retry on network errors, 5xx, and 429 (Too Many Requests / service overloaded).
		// Stop immediately on other 4xx (bad request, auth failure, etc.).
		var httpErr *httpStatusError
		if errors.As(err, &httpErr) {
			if httpErr.status == http.StatusTooManyRequests {
				lastWas429 = true
				continue // retry with short backoff
			}
			if httpErr.status < 500 {
				break // other 4xx: not retryable
			}
		}
	}
	return nil, fmt.Errorf("POST %s: %w", url, lastErr)
}

// httpStatusError is returned by doPost when the server returns a non-200 HTTP status.
type httpStatusError struct {
	status int
	body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d — %s", e.status, e.body)
}

// doPost performs a single HTTP POST to url with reqBody and decodes the vector response.
func (b *httpBackend) doPost(url string, reqBody []byte) ([][]float32, error) {
	httpResp, err := b.client.Post(url, "application/json", bytes.NewReader(reqBody)) //nolint:noctx
	if err != nil {
		return nil, err
	}
	defer func() { _ = httpResp.Body.Close() }()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1024)) //nolint:gomnd
		return nil, &httpStatusError{status: httpResp.StatusCode, body: string(body)}
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
