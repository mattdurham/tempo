package rw

// NOTE: SharedLRUCache invariant — total cached bytes never exceeds maxBytes.
// Eviction removes least-recently-used entries starting from the lowest-priority
// tier (DataTypeBlock) and working up, so high-priority data (footers, bloom filters)
// survives cache pressure from block reads.

import (
	"container/list"
	"io"
	"sync"
)

const numCacheTiers = 4

// dataTypeTier maps a DataType to its eviction tier (0 = hardest to evict, 3 = easiest).
func dataTypeTier(dt DataType) int {
	switch dt {
	case DataTypeFooter, DataTypeHeader:
		return 0
	case DataTypeMetadata, DataTypeTraceBloomFilter:
		return 1
	case DataTypeTimestampIndex:
		return 2
	default: // DataTypeBlock, DataTypeUnknown, and any future types
		return 3
	}
}

// cacheKey uniquely identifies a cached byte range within a named reader.
type cacheKey struct {
	readerID string
	offset   int64
	length   int32
}

// lruEntry holds one cached range and its tier.
type lruEntry struct {
	data []byte
	key  cacheKey
	tier int
}

// SharedLRUCache is a byte-bounded, priority-tiered LRU cache safe for concurrent use.
// It is designed to be shared across multiple SharedLRUProvider instances (one per file).
//
// Eviction policy: when adding a new entry would exceed maxBytes, entries are removed
// starting from the lowest-priority tier (tier 3, DataTypeBlock) and working up to
// higher-priority tiers. Within a tier, the least-recently-used entry is removed first.
type SharedLRUCache struct {
	// lists[0] = highest priority (Footer/Header), lists[3] = lowest (Block).
	lists    [numCacheTiers]*list.List
	index    map[cacheKey]*list.Element
	maxBytes int64
	curBytes int64
	mu       sync.Mutex
}

// NewSharedLRUCache creates a SharedLRUCache with the given total byte capacity.
func NewSharedLRUCache(maxBytes int64) *SharedLRUCache {
	c := &SharedLRUCache{
		maxBytes: maxBytes,
		index:    make(map[cacheKey]*list.Element),
	}
	for i := range c.lists {
		c.lists[i] = list.New()
	}
	return c
}

// Get looks up a cached range. On hit the entry is promoted to MRU within its tier.
// Returns a copy of the cached bytes to prevent callers from mutating the cache.
func (c *SharedLRUCache) Get(readerID string, off int64, length int) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{
		readerID: readerID,
		offset:   off,
		length:   int32(length), //nolint:gosec // length is bounded by read-buffer size
	}
	elem, ok := c.index[key]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*lruEntry)
	c.lists[entry.tier].MoveToBack(elem)

	out := make([]byte, len(entry.data))
	copy(out, entry.data)
	return out, true
}

// Put stores data in the cache keyed by (readerID, off, len(data)) with priority
// derived from dt. No-ops if the key already exists or if data is larger than maxBytes.
func (c *SharedLRUCache) Put(readerID string, off int64, data []byte, dt DataType) {
	needed := int64(len(data))
	if needed > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{readerID: readerID, offset: off, length: int32(len(data))} //nolint:gosec
	if _, exists := c.index[key]; exists {
		return
	}

	tier := dataTypeTier(dt)
	c.evictLocked(needed)

	if c.curBytes+needed > c.maxBytes {
		return // cache still full after eviction (shouldn't happen, but guard anyway)
	}

	buf := make([]byte, len(data))
	copy(buf, data)
	entry := &lruEntry{key: key, data: buf, tier: tier}
	elem := c.lists[tier].PushBack(entry)
	c.index[key] = elem
	c.curBytes += needed
}

// evictLocked removes LRU entries from lowest-priority tiers first until
// curBytes+needed <= maxBytes. Must be called with c.mu held.
func (c *SharedLRUCache) evictLocked(needed int64) {
	for tier := numCacheTiers - 1; tier >= 0 && c.curBytes+needed > c.maxBytes; tier-- {
		for c.curBytes+needed > c.maxBytes && c.lists[tier].Len() > 0 {
			elem := c.lists[tier].Front()
			entry := elem.Value.(*lruEntry)
			c.lists[tier].Remove(elem)
			delete(c.index, entry.key)
			c.curBytes -= int64(len(entry.data))
		}
	}
}

// SharedLRUProvider wraps a ReaderProvider and routes all reads through a SharedLRUCache.
// Multiple SharedLRUProvider instances can share the same *SharedLRUCache.
// All methods are safe for concurrent use.
type SharedLRUProvider struct {
	underlying ReaderProvider
	cache      *SharedLRUCache
	readerID   string
}

// NewSharedLRUProvider creates a SharedLRUProvider that caches reads in cache.
// readerID must uniquely identify the underlying reader within the cache namespace
// (e.g. a file path or object storage key).
func NewSharedLRUProvider(underlying ReaderProvider, readerID string, cache *SharedLRUCache) *SharedLRUProvider {
	return &SharedLRUProvider{underlying: underlying, readerID: readerID, cache: cache}
}

// Size delegates to the underlying provider.
func (s *SharedLRUProvider) Size() (int64, error) {
	return s.underlying.Size()
}

// ReadAt checks the shared LRU cache first. On a miss it reads from the underlying
// provider, stores the result in the cache keyed by (readerID, off, len(p)), and
// returns the data. A short read from the underlying provider is escalated to
// io.ErrUnexpectedEOF.
func (s *SharedLRUProvider) ReadAt(p []byte, off int64, dt DataType) (int, error) {
	if cached, ok := s.cache.Get(s.readerID, off, len(p)); ok {
		return copy(p, cached), nil
	}

	buf := make([]byte, len(p))
	n, err := s.underlying.ReadAt(buf, off, dt)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}

	copy(p, buf[:n])
	s.cache.Put(s.readerID, off, buf[:n], dt)
	return n, nil
}
