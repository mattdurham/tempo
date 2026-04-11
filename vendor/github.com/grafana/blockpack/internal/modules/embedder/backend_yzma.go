package embedder

import "fmt"

type yzmaBackend struct{}

func newYzmaBackend(_ Config) (Backend, error) {
	return nil, fmt.Errorf("yzma backend not available in this build")
}

func (y *yzmaBackend) Embed(_ string) ([]float32, error) {
	return nil, fmt.Errorf("yzma backend not available in this build")
}

func (y *yzmaBackend) EmbedBatch(_ []string) ([][]float32, error) {
	return nil, fmt.Errorf("yzma backend not available in this build")
}

func (y *yzmaBackend) Dim() int { return 0 }

func (y *yzmaBackend) Close() {}
