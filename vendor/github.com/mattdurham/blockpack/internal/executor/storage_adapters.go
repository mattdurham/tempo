package executor

import (
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
)

// FileStorage provides access to on-disk encoded files by path.
type FileStorage interface {
	Get(path string) ([]byte, error)
}

// ProviderStorage returns a reader provider for ranged access.
type ProviderStorage interface {
	GetProvider(path string) (blockpackio.ReaderProvider, error)
}
