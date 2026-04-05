package embedder

import "fmt"

// yzmaBackend is stubbed out — Tempo uses only the HTTP backend.
type yzmaBackend struct{}

func newYzmaBackend(_ Config) (Backend, error) {
	return nil, fmt.Errorf("yzma backend not available: use HTTP backend instead")
}
