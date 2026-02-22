package reader

import (
	"fmt"
	"io"
)

func newRangeReaderProvider(base ReaderProvider, offset int64, length int64) *rangeReaderProvider {
	return &rangeReaderProvider{
		base:   base,
		offset: offset,
		length: length,
	}
}

func (p *rangeReaderProvider) Size() (int64, error) {
	return p.length, nil
}

func (p *rangeReaderProvider) ReadAt(b []byte, off int64, dataType DataType) (int, error) {
	if off < 0 || off >= p.length {
		return 0, io.EOF
	}
	// Allow partial reads at the end of the range
	if off+int64(len(b)) > p.length {
		b = b[:p.length-off]
	}
	return p.base.ReadAt(b, p.offset+off, dataType)
}

func (p *byteSliceProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

func (p *byteSliceProvider) ReadAt(b []byte, off int64, dataType DataType) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// readProviderRangeUncached reads a byte range from the provider without caching.
// Used during Reader construction before the Reader is fully initialized.
func readProviderRangeUncached(provider ReaderProvider, off int64, length int, dataType DataType) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}
	buf := make([]byte, length)
	n, err := provider.ReadAt(buf, off, dataType)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("short read: %d != %d", n, length)
	}
	return buf, nil
}

// readProviderRange reads a byte range from the provider, using cache if available.
func (r *Reader) readProviderRange(off int64, length int, dataType DataType) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}

	// Check cache first (if present)
	if r.cache != nil && r.name != "" {
		key := fmt.Sprintf("%s:%d:%d", r.name, off, length)
		if data, found := r.cache.Get(key); found {
			return data, nil // Cache hit - no I/O
		}
	}

	// Cache miss or no cache - read from provider
	var data []byte

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, off, dataType)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("short read: %d != %d", n, length)
	}
	data = buf

	// Store in cache for future reads
	if r.cache != nil && r.name != "" {
		key := fmt.Sprintf("%s:%d:%d", r.name, off, length)
		r.cache.Put(key, data)
	}

	return data, nil
}

func (r *Reader) readRange(off int64, length int, dataType DataType) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid read length %d", length)
	}
	end := off + int64(length)
	if off < 0 || end < off || end > r.dataSize {
		return nil, fmt.Errorf("read out of bounds (offset %d length %d size %d)", off, length, r.dataSize)
	}
	if r.data != nil {
		start := int(off)
		stop := int(end)
		return r.data[start:stop], nil
	}
	return r.readProviderRange(off, length, dataType)
}

// Blocks returns block metadata (including bloom filters) for all blocks.
// Blocks are not parsed until GetBlock() is called.
