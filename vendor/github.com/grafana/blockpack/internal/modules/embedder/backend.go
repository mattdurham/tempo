package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Backend is the interface for embedding inference engines.
// Implementations must be safe for concurrent use after construction.
// Specifically: any shared mutable state (inference contexts, connection pools,
// etc.) must be protected by a sync.Mutex or equivalent. Immutable post-construction
// state (like httpBackend) is inherently safe. Stateful backends that hold shared
// resources (e.g. an llama.cpp context) must serialize access.
// httpBackend is stateless after NewHTTP returns; all fields are immutable.
// MockBackend is likewise thread-safe.
type Backend interface {
	// Embed encodes a single text string into an L2-normalized float32 vector.
	// Returns error if inference fails.
	Embed(text string) ([]float32, error)

	// EmbedBatch encodes each text and returns one L2-normalized float32 vector per text,
	// in input order. Returns nil, err on any failure (no partial results).
	EmbedBatch(texts []string) ([][]float32, error)

	// Dim returns the embedding dimension (e.g. 768).
	Dim() int

	// Close releases any resources held by the backend.
	Close()
}
