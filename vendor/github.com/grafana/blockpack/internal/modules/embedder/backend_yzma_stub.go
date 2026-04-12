//go:build !yzma

package embedder

import "fmt"

// newYzmaBackend is a stub for builds without the yzma/CGo backend.
// Build with -tags yzma and CGO_ENABLED=1 to enable the real backend.
func newYzmaBackend(_ Config) (Backend, error) {
	return nil, fmt.Errorf("embedder: yzma backend not available; build with -tags yzma and CGO_ENABLED=1")
}
