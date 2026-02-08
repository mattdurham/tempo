package executor

import (
	"io"
	"os"
)

type bytesReaderProvider struct {
	data []byte
}

func (p *bytesReaderProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

// ReadAt simulates object storage behavior by always copying data.
// Does NOT implement Slice to force realistic IO operations for benchmarks.
func (p *bytesReaderProvider) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

type fileReaderProvider struct {
	file *os.File
	size int64
}

func (p *fileReaderProvider) Size() (int64, error) {
	return p.size, nil
}

// ReadAt reads from the underlying file.
// Does NOT implement Slice to force realistic IO operations for benchmarks.
func (p *fileReaderProvider) ReadAt(b []byte, off int64) (int, error) {
	return p.file.ReadAt(b, off)
}

// Close closes the underlying file.
// This method should be called when the provider is no longer needed to prevent file descriptor leaks.
func (p *fileReaderProvider) Close() error {
	if p.file == nil {
		return nil
	}
	return p.file.Close()
}
