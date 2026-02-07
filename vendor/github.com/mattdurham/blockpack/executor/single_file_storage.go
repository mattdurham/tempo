package executor

import (
	"fmt"

	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
)

// SingleFileStorage provides FileStorage for a single file's bytes.
// Useful for benchmarks or when you have the file data already loaded.
type SingleFileStorage struct {
	data []byte
}

// NewSingleFileStorage creates a FileStorage for a single file.
func NewSingleFileStorage(data []byte) *SingleFileStorage {
	return &SingleFileStorage{data: data}
}

// Get returns the file data. The path parameter is ignored.
func (s *SingleFileStorage) Get(path string) ([]byte, error) {
	if len(s.data) == 0 {
		return nil, fmt.Errorf("no data available")
	}
	return s.data, nil
}

// GetProvider returns a reader provider for the single file.
func (s *SingleFileStorage) GetProvider(path string) (blockpackio.ReaderProvider, error) {
	if len(s.data) == 0 {
		return nil, fmt.Errorf("no data available")
	}
	return &bytesReaderProvider{data: s.data}, nil
}
