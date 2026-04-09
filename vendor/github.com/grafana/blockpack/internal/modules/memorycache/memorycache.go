// Package memorycache provides an in-memory, byte-bounded LRU cache that
// implements filecache.Cache. It is intended as the fastest tier in a
// multi-level cache chain (memorycache → filecache → memcache).
//
// Eviction is LRU: when total cached bytes would exceed MaxBytes the
// least-recently-used entry is removed first.
// Concurrent fetch deduplication uses singleflight.
package memorycache

import (
	"container/list"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"
)

// Config configures a MemoryCache.
type Config struct {
	// MaxBytes is the maximum total bytes the cache may hold.
	// Required and must be positive.
	MaxBytes int64
}

// entry is one record in the LRU list.
type entry struct {
	key  string
	data []byte
}

// MemoryCache is an in-memory, byte-bounded LRU cache safe for concurrent use.
// It implements filecache.Cache.
//
// A nil *MemoryCache is safe to use: all operations become pass-throughs.
type MemoryCache struct {
	group    singleflight.Group
	index    map[string]*list.Element
	lru      *list.List // back = MRU, front = LRU
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
}

// New creates a MemoryCache with the given byte capacity.
func New(cfg Config) (*MemoryCache, error) {
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("memorycache: MaxBytes must be positive, got %d", cfg.MaxBytes)
	}
	return &MemoryCache{
		maxBytes: cfg.MaxBytes,
		index:    make(map[string]*list.Element),
		lru:      list.New(),
	}, nil
}

// Get returns the cached bytes for key, or (nil, false, nil) on a miss.
// The returned slice is an independent copy safe for the caller to modify.
func (c *MemoryCache) Get(key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	c.mu.Lock()
	elem, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		return nil, false, nil
	}
	c.lru.MoveToBack(elem)
	src := elem.Value.(*entry).data
	out := make([]byte, len(src))
	copy(out, src)
	c.mu.Unlock()
	return out, true, nil
}

// Put stores key→value in the cache.
// No-op if the key already exists or if len(value) > MaxBytes.
func (c *MemoryCache) Put(key string, value []byte) error {
	if c == nil {
		return nil
	}
	needed := int64(len(value))
	if needed > c.maxBytes {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[key]; exists {
		return nil
	}

	c.evictLocked(needed)

	if c.curBytes+needed > c.maxBytes {
		return nil // still over budget after eviction (shouldn't happen)
	}

	cp := make([]byte, len(value))
	copy(cp, value)
	e := &entry{key: key, data: cp}
	elem := c.lru.PushBack(e)
	c.index[key] = elem
	c.curBytes += needed
	return nil
}

// evictLocked removes LRU entries until curBytes+needed ≤ maxBytes.
// Must be called with c.mu held.
func (c *MemoryCache) evictLocked(needed int64) {
	for c.curBytes+needed > c.maxBytes && c.lru.Len() > 0 {
		elem := c.lru.Front()
		e := elem.Value.(*entry)
		c.lru.Remove(elem)
		delete(c.index, e.key)
		c.curBytes -= int64(len(e.data))
	}
}

// GetOrFetch returns the cached value for key; on a miss it calls fetch(),
// stores the result, and returns it. Concurrent calls for the same uncached
// key share a single fetch invocation.
func (c *MemoryCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if c == nil {
		return fetch()
	}

	if val, ok, err := c.Get(key); err != nil {
		return nil, err
	} else if ok {
		return val, nil
	}

	result, err, _ := c.group.Do(key, func() (any, error) {
		if val, ok, getErr := c.Get(key); getErr != nil {
			return nil, getErr
		} else if ok {
			return val, nil
		}

		fetched, fetchErr := fetch()
		if fetchErr != nil {
			return nil, fetchErr
		}

		if putErr := c.Put(key, fetched); putErr != nil {
			return nil, putErr
		}
		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("memorycache: unexpected singleflight result type %T", result)
	}
	// singleflight shares one fetched slice; callers may get back the same underlying
	// array. Return a copy so callers cannot corrupt each other.
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}

// Close is a no-op for an in-memory cache.
// Safe to call on a nil *MemoryCache.
func (c *MemoryCache) Close() error { return nil }
