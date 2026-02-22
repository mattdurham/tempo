package shared

import (
	"fmt"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
)

// LRUByteCache implements ByteCache using a size-aware LRU cache.
type LRUByteCache struct {
	cache       *lru.Cache[string, []byte]
	maxSize     int64
	currentSize atomic.Int64
	hits        atomic.Int64
	misses      atomic.Int64
	evictions   atomic.Int64
	mu          sync.Mutex // Protects eviction logic
}

// NewLRUByteCache creates a new LRU byte cache with the given maximum size in bytes.
func NewLRUByteCache(maxBytes int64) (*LRUByteCache, error) {
	c := &LRUByteCache{
		maxSize: maxBytes,
	}

	// Create LRU with eviction callback
	cache, err := lru.NewWithEvict(10000, func(key string, value []byte) {
		// Track evicted bytes
		size := int64(len(value))
		c.currentSize.Add(-size)
		c.evictions.Add(size)
	})
	if err != nil {
		return nil, fmt.Errorf("create LRU cache: %w", err)
	}

	c.cache = cache
	return c, nil
}

// Get retrieves cached data for the given key.
func (c *LRUByteCache) Get(key string) ([]byte, bool) {
	val, found := c.cache.Get(key)
	if found {
		c.hits.Add(1)
		return val, true
	}
	c.misses.Add(1)
	return nil, false
}

// Put stores data in the cache with the given key.
// Evicts LRU entries if necessary to stay within size limit.
func (c *LRUByteCache) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dataSize := int64(len(data))

	// Don't cache items larger than max size
	if dataSize > c.maxSize {
		return
	}

	// Check if key already exists (to handle size accounting correctly)
	if oldData, exists := c.cache.Peek(key); exists {
		// Updating existing key - manually adjust size difference
		oldSize := int64(len(oldData))
		sizeDiff := dataSize - oldSize
		c.currentSize.Add(sizeDiff)
		c.cache.Add(key, data)
		return
	}

	// New key - evict until we have room for new entry
	for c.currentSize.Load()+dataSize > c.maxSize {
		// Remove oldest entry
		_, _, ok := c.cache.RemoveOldest()
		if !ok {
			// Cache is empty but we still don't have room
			break
		}
	}

	// Add new entry
	c.cache.Add(key, data)
	c.currentSize.Add(dataSize)
}

// Stats returns current cache statistics.
func (c *LRUByteCache) Stats() CacheStats {
	return CacheStats{
		Hits:        c.hits.Load(),
		Misses:      c.misses.Load(),
		Evictions:   c.evictions.Load(),
		CurrentSize: c.currentSize.Load(),
		MaxSize:     c.maxSize,
	}
}

// Close closes the cache and frees resources.
func (c *LRUByteCache) Close() {
	c.cache.Purge()
}
