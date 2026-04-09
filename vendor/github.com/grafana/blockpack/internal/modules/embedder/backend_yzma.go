package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "fmt"

// yzmaBackend is stubbed out — Tempo uses only the HTTP backend.
// The full yzma/llama.cpp implementation is available in the blockpack source;
// it is excluded here to avoid pulling in purego's dynamic-link dependency on libdl.so.2,
// which is incompatible with the distroless/static runtime image.
type yzmaBackend struct{}

func newYzmaBackend(_ Config) (*yzmaBackend, error) {
	return nil, fmt.Errorf("yzma backend is not available in this build; use the HTTP backend")
}

func (b *yzmaBackend) Dim() int                              { return 0 }
func (b *yzmaBackend) Embed(_ string) ([]float32, error)    { return nil, fmt.Errorf("yzma backend is not available in this build; use the HTTP backend") }
func (b *yzmaBackend) EmbedBatch(_ []string) ([][]float32, error) { return nil, fmt.Errorf("yzma backend is not available in this build; use the HTTP backend") }
func (b *yzmaBackend) Close()                               {}
