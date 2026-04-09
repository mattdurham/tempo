package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "fmt"

// yzmaBackend is a stub implementation of Backend for builds where the yzma/llama.cpp
// shared library is not available. All methods return errors or zero values.
type yzmaBackend struct{}

// newYzmaBackend always returns an error because yzma/llama.cpp is not linked in this build.
func newYzmaBackend(_ Config) (*yzmaBackend, error) {
	return nil, fmt.Errorf("embedder: yzma backend not available in this build")
}

// Dim returns 0 for the stub backend.
func (b *yzmaBackend) Dim() int { return 0 }

// Embed always returns an error for the stub backend.
func (b *yzmaBackend) Embed(_ string) ([]float32, error) {
	return nil, fmt.Errorf("embedder: yzma backend not available in this build")
}

// EmbedBatch always returns an error for the stub backend.
func (b *yzmaBackend) EmbedBatch(_ []string) ([][]float32, error) {
	return nil, fmt.Errorf("embedder: yzma backend not available in this build")
}

// Close is a no-op for the stub backend.
func (b *yzmaBackend) Close() {}
