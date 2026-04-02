package rw

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"io"
	"sync"
)

// cachedRange is one cached contiguous byte range.
type cachedRange struct {
	data   []byte
	offset int64
}

// RangeCachingProvider wraps a ReaderProvider with sub-range caching.
// If a requested range is fully contained within a cached range, the bytes are
// served from memory without a new I/O call.
// All methods are safe for concurrent use.
//
// WARNING: The cache grows without bound for the lifetime of this instance.
// There is no eviction policy. This type is intended for short-lived, per-file
// use: create one instance per open file, read, and discard. Do NOT reuse a
// RangeCachingProvider across multiple files or after the underlying storage
// has changed. See rw/NOTES.md §5.
type RangeCachingProvider struct {
	underlying ReaderProvider
	cache      []cachedRange
	mu         sync.RWMutex
}

// NewRangeCachingProvider wraps underlying with range caching.
func NewRangeCachingProvider(underlying ReaderProvider) *RangeCachingProvider {
	return &RangeCachingProvider{underlying: underlying}
}

// Size delegates to the underlying provider.
func (c *RangeCachingProvider) Size() (int64, error) {
	return c.underlying.Size()
}

// ReadAt checks whether [off, off+len(p)) is covered by a cached range and
// serves from cache if so. Otherwise it issues a real read and stores the
// result in the cache.
func (c *RangeCachingProvider) ReadAt(p []byte, off int64, dt DataType) (int, error) {
	end := off + int64(len(p))

	c.mu.RLock()
	for _, cr := range c.cache {
		crEnd := cr.offset + int64(len(cr.data))
		if off >= cr.offset && end <= crEnd {
			n := copy(p, cr.data[off-cr.offset:])
			c.mu.RUnlock()
			return n, nil
		}
	}
	c.mu.RUnlock()

	buf := make([]byte, len(p))
	n, err := c.underlying.ReadAt(buf, off, dt)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}

	// n == len(p) guaranteed by the short-read guard above.
	copy(p, buf[:n])

	c.mu.Lock()
	c.cache = append(c.cache, cachedRange{offset: off, data: buf[:n]})
	c.mu.Unlock()

	return n, nil
}
