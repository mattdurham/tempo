package shared

import (
	"fmt"
	"sort"
	"sync"
)

// rangeCachingEntry stores a single cached byte range.
//
//nolint:govet,betteralign
type rangeCachingEntry struct {
	data   []byte
	offset int64
}

// RangeCachingProvider wraps a ReaderProvider with in-memory range caching.
// Unlike CachingReaderProvider (which requires exact offset:length matches),
// this provider supports sub-range lookups: if the requested range [off, off+len(p))
// is fully contained within any previously cached range, it is served from memory
// without issuing a new I/O operation.
//
// Intended for single-query lifetimes: the provider accumulates all reads in memory
// so that subsequent re-reads of overlapping regions (e.g. AddColumnsToBlock re-reading
// a block that was already fetched by a coalesced read) hit the cache instead of storage.
//
// Thread-safe via mutex.
type RangeCachingProvider struct {
	underlying ReaderProvider
	ranges     []rangeCachingEntry // sorted by offset, ascending
	mu         sync.Mutex
	size       int64
}

// NewRangeCachingProvider creates a new in-memory range-caching provider.
// The cache is empty initially and fills lazily as data is read.
func NewRangeCachingProvider(underlying ReaderProvider) (*RangeCachingProvider, error) {
	if underlying == nil {
		return nil, fmt.Errorf("underlying provider cannot be nil")
	}
	size, err := underlying.Size()
	if err != nil {
		return nil, fmt.Errorf("get provider size: %w", err)
	}
	return &RangeCachingProvider{
		underlying: underlying,
		size:       size,
	}, nil
}

// Size returns the total size of the underlying file.
func (r *RangeCachingProvider) Size() (int64, error) {
	return r.size, nil
}

// ReadAt reads bytes from the provider, serving from in-memory cache when possible.
// If the requested range [off, off+len(p)) is fully within any previously cached range,
// the data is returned without I/O. Otherwise the underlying provider is called and
// the result is added to the cache for future sub-range requests.
func (r *RangeCachingProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	end := off + int64(len(p)) //nolint:gosec

	r.mu.Lock()
	// Binary search: find the last entry with offset <= off
	i := sort.Search(len(r.ranges), func(i int) bool {
		return r.ranges[i].offset > off
	}) - 1
	if i >= 0 {
		entry := r.ranges[i]
		entryEnd := entry.offset + int64(len(entry.data)) //nolint:gosec
		if entryEnd >= end {
			// Cache hit: requested range is fully contained within a cached range
			start := off - entry.offset
			copy(p, entry.data[start:start+int64(len(p))]) //nolint:gosec
			r.mu.Unlock()
			return len(p), nil
		}
	}
	r.mu.Unlock()

	// Cache miss: read from underlying provider
	n, err := r.underlying.ReadAt(p, off, dataType)
	if err != nil || n != len(p) {
		return n, err
	}

	// Cache the result; make a copy since p may be reused by the caller
	cached := make([]byte, n)
	copy(cached, p[:n])

	r.mu.Lock()
	r.insertSorted(off, cached)
	r.mu.Unlock()

	return n, nil
}

// insertSorted inserts a new range into r.ranges maintaining ascending offset order.
// If an entry at the same offset already exists (e.g. inserted by a concurrent goroutine
// between the cache-miss check and this lock acquisition), the insert is skipped.
// Assumes mu is held by the caller.
func (r *RangeCachingProvider) insertSorted(offset int64, data []byte) {
	i := sort.Search(len(r.ranges), func(i int) bool {
		return r.ranges[i].offset >= offset
	})
	if i < len(r.ranges) && r.ranges[i].offset == offset {
		return // already present, skip duplicate
	}
	r.ranges = append(r.ranges, rangeCachingEntry{})
	copy(r.ranges[i+1:], r.ranges[i:])
	r.ranges[i] = rangeCachingEntry{offset: offset, data: data}
}

// Close releases the in-memory cache and closes the underlying provider if it supports it.
func (r *RangeCachingProvider) Close() error {
	r.mu.Lock()
	r.ranges = nil
	r.mu.Unlock()
	if closeable, ok := r.underlying.(interface{ Close() error }); ok {
		return closeable.Close()
	}
	return nil
}
