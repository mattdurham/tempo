package shared

// TextEmbedder converts text to float32 embedding vectors.
// This is the canonical interface definition shared between the writer and other packages
// that need embedding support, placed here to avoid import cycles.
// The public *blockpack.Embedder (HTTP and yzma backends) satisfies this interface.
type TextEmbedder interface {
	Embed(text string) ([]float32, error)
	// EmbedBatch encodes a batch of texts in a single call. Must return exactly
	// len(texts) vectors in the same order. Implementations may send a single
	// HTTP request for all texts (HTTP backend) or process locally (yzma backend).
	EmbedBatch(texts []string) ([][]float32, error)
}
