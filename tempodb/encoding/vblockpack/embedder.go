package vblockpack

import (
	"log/slog"
	"sync"

	"github.com/grafana/blockpack"
)

var (
	processEmbedder     *blockpack.Embedder
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
func getProcessEmbedder(url string) *blockpack.Embedder {
	if url == "" {
		return nil
	}
	processEmbedderOnce.Do(func() {
		emb, err := blockpack.NewHTTPEmbedder(blockpack.EmbedderHTTPConfig{
			ServerURL: url,
		})
		if err != nil {
			slog.Error("vblockpack: failed to initialize embedder", "url", url, "err", err)
			return
		}
		slog.Info("vblockpack: embedder initialized", "url", url, "dim", emb.Dim())
		processEmbedder = emb
	})
	return processEmbedder
}
