// Package embedder is the importable write-path surface for blockpack's
// text-to-vector embedding. It exists as a dedicated subpackage rather than
// part of the top-level blockpack package so that the embedder is no longer
// entangled in blockpack's general query/storage API surface (issue #346).
//
// The actual implementation lives in internal/modules/embedder. Go's internal
// package rule prevents external modules (tempo) from importing that package
// directly, so this thin subpackage re-exports the minimal write-path
// constructor that tempo's block-creation path needs. NOTE-370.
package embedder

import (
	modules_embedder "github.com/grafana/blockpack/internal/modules/embedder"
	vm "github.com/grafana/blockpack/internal/vm"
)

// Embedder provides text-to-vector embedding via a pluggable Backend.
// It satisfies the TextEmbedder interface consumed by the write path and by
// blockpack.QueryOptions.Embedder for VECTOR_AI() query-time embedding.
type Embedder = modules_embedder.Embedder

// HTTPConfig configures the HTTP embedding backend (TEI or compatible).
type HTTPConfig = modules_embedder.HTTPConfig

// NewHTTPEmbedder creates an Embedder backed by an HTTP embedding server
// (TEI or compatible). It sends a probe request on construction to determine
// the embedding dimension; it returns an error if the server is unreachable
// or returns an unexpected response.
func NewHTTPEmbedder(cfg HTTPConfig) (*Embedder, error) {
	return modules_embedder.NewHTTP(cfg)
}

// Compile-time check: *Embedder satisfies vm.TextEmbedder, the interface
// consumed by the writer (Config.Embedder) and the query path
// (QueryOptions.Embedder).
var _ vm.TextEmbedder = (*Embedder)(nil)
