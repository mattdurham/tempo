package vblockpack

import (
	"log/slog"
	"sync"

	"github.com/grafana/blockpack"
)

var (
	processEmbedder            *blockpack.Embedder
	processEmbedderOnce        sync.Once
	configuredEmbedURL         string // set by ConfigureEmbedding
	configuredEmbedConcurrency int    // set by ConfigureEmbedding; 0 = use blockpack default
	configuredEmbedBatchSize   int    // set by ConfigureEmbedding; 0 = use blockpack default (32)
	configuredEmbedMaxTextLen  int    // set by ConfigureEmbedding; 0 = use blockpack default (24000)
)

// ConfigureEmbedding stores the embedding server URL, concurrency, batch size, and max text length
// for process-level use. Called from block creation and compaction init paths.
func ConfigureEmbedding(url string, concurrentBatches int, batchSize int, maxTextLength int) {
	configuredEmbedURL = url
	if concurrentBatches > 0 {
		configuredEmbedConcurrency = concurrentBatches
	}
	if batchSize > 0 {
		configuredEmbedBatchSize = batchSize
	}
	if maxTextLength > 0 {
		configuredEmbedMaxTextLen = maxTextLength
	}
}

// getProcessEmbedder returns a process-level embedder for the configured URL.
// Returns nil if no URL is configured (embedding disabled). Thread-safe;
// initializes the embedder exactly once.
func getProcessEmbedder(url string) *blockpack.Embedder {
	if url == "" {
		return nil
	}
	processEmbedderOnce.Do(func() {
		cfg := blockpack.EmbedderHTTPConfig{
			ServerURL:            url,
			MaxConcurrentBatches: configuredEmbedConcurrency,
			MaxBatchSize:         configuredEmbedBatchSize,
			MaxTextLength:        configuredEmbedMaxTextLen,
		}
		emb, err := blockpack.NewHTTPEmbedder(cfg)
		if err != nil {
			slog.Error("vblockpack: failed to initialize embedder", "url", url, "err", err)
			return
		}
		slog.Info("vblockpack: embedder initialized", "url", url, "dim", emb.Dim(), "concurrent_batches", configuredEmbedConcurrency, "batch_size", configuredEmbedBatchSize)
		processEmbedder = emb
	})
	return processEmbedder
}
