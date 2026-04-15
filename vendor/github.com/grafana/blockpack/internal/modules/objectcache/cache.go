// Package objectcache provides a generic, concurrent in-memory cache with strong references
// and LRU eviction bounded to a configurable byte budget (default: 20% of GOMEMLIMIT).
//
// Entries are retained until evicted by LRU pressure or explicitly cleared.
// GOMEMLIMIT controls GC pacing but is NOT a hard memory cap — this cache
// enforces its own byte budget independently.
package objectcache

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"errors"
	"math"
	"runtime/debug"
	"sync"
)

// defaultBudgetFraction is the fraction of GOMEMLIMIT used as the default
// per-Cache byte budget when GOMEMLIMIT is set.
const defaultBudgetFraction = 0.20

// defaultMaxBytesNoGOMEMLIMIT is the hard byte budget used when GOMEMLIMIT is
// not set. This prevents unbounded cache growth in deployments that omit the
// env var. Callers can override via SetMaxBytes.
const defaultMaxBytesNoGOMEMLIMIT = 256 << 20 // 256 MiB

// Sizer is implemented by cached values that can report their in-memory size.
// If a value does not implement Sizer, a default of 1 MB per entry is used.
type Sizer interface {
	SizeBytes() int64
}

const defaultEntrySize int64 = 1 << 20 // 1 MB fallback

// entry is a doubly-linked list node for LRU tracking.
type entry[V any] struct {
	val        *V
	prev, next *entry[V]
	key        string
	sizeBytes  int64
}

// Cache is a generic in-memory cache with strong references and LRU eviction.
// The byte budget defaults to 20% of GOMEMLIMIT when the env var is set, or
// 256 MiB when it is not. Callers may override via SetMaxBytes.
//
// Cache is safe for concurrent use. The zero value is ready to use.
// A Cache must not be copied after first use.
//
// SPEC-OC-001: Get returns nil on miss.
// SPEC-OC-002: Put stores a strong reference; entries are evicted by LRU, not GC.
// SPEC-OC-005: Clear removes all entries.
// SPEC-OC-006: All methods are safe for concurrent use.
// SPEC-OC-007: Byte budget defaults to 20% of GOMEMLIMIT (or 256 MiB fallback); configurable via SetMaxBytes.
type Cache[V any] struct {
	items    map[string]*entry[V]
	head     *entry[V] // most recently used
	tail     *entry[V] // least recently used
	curBytes int64
	maxBytes int64 // 0 = compute from GOMEMLIMIT on first Put
	mu       sync.Mutex
	inited   bool
}

// SetMaxBytes overrides the default byte budget. Must be called before first Put.
// Pass 0 to revert to the default (20% of GOMEMLIMIT).
func (c *Cache[V]) SetMaxBytes(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = n
	c.inited = n > 0
}

// Get returns the cached *V for key, or nil if the entry is absent.
// Promotes the entry to the front of the LRU list on hit.
func (c *Cache[V]) Get(key string) *V {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return nil
	}
	c.moveToFront(e)
	return e.val
}

// Put stores a strong reference to v under key.
// If v implements Sizer, its SizeBytes() is used for LRU budget tracking.
// Otherwise a default of 1 MB per entry is assumed.
// Evicts least-recently-used entries if the byte budget is exceeded.
// Returns an error if v is nil.
func (c *Cache[V]) Put(key string, v *V) error {
	if v == nil {
		return errors.New("objectcache: Put called with nil pointer")
	}

	size := defaultEntrySize
	if s, ok := any(v).(Sizer); ok {
		size = s.SizeBytes()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInit()

	if e, ok := c.items[key]; ok {
		c.curBytes -= e.sizeBytes
		e.val = v
		e.sizeBytes = size
		c.curBytes += size
		c.moveToFront(e)
	} else {
		e = &entry[V]{key: key, val: v, sizeBytes: size}
		if c.items == nil {
			c.items = make(map[string]*entry[V])
		}
		c.items[key] = e
		c.pushFront(e)
		c.curBytes += size
	}

	// Evict LRU entries until under budget (maxBytes=0 means unbounded).
	for c.maxBytes > 0 && c.curBytes > c.maxBytes && c.tail != nil {
		c.evictTail()
	}
	return nil
}

// Delete removes the entry for key if present. No-op if the key is absent.
func (c *Cache[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return
	}
	c.unlink(e)
	delete(c.items, key)
	c.curBytes -= e.sizeBytes
}

// Clear removes all entries from the cache.
func (c *Cache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = nil
	c.head = nil
	c.tail = nil
	c.curBytes = 0
	c.inited = false
}

// Len returns the number of entries in the cache.
func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// ensureInit lazily computes the default byte budget.
// Priority: (1) SetMaxBytes override, (2) 20% of GOMEMLIMIT, (3) 256 MiB hard fallback.
// The fallback prevents unbounded cache growth in deployments that omit GOMEMLIMIT.
func (c *Cache[V]) ensureInit() {
	if c.inited {
		return
	}
	c.inited = true
	if c.maxBytes <= 0 {
		// math.MaxInt64 is the sentinel Go uses for "no limit" (GOMEMLIMIT not set).
		// Treat it the same as 0 — use the hard fallback rather than computing an
		// astronomically large budget from float64(math.MaxInt64) * 0.20.
		if limit := debug.SetMemoryLimit(-1); limit > 0 && limit != math.MaxInt64 {
			c.maxBytes = int64(float64(limit) * defaultBudgetFraction) //nolint:gosec
		} else {
			c.maxBytes = defaultMaxBytesNoGOMEMLIMIT
		}
	}
}

func (c *Cache[V]) moveToFront(e *entry[V]) {
	if c.head == e {
		return
	}
	c.unlink(e)
	c.pushFront(e)
}

func (c *Cache[V]) pushFront(e *entry[V]) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *Cache[V]) unlink(e *entry[V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (c *Cache[V]) evictTail() {
	if c.tail == nil {
		return
	}
	e := c.tail
	c.unlink(e)
	delete(c.items, e.key)
	c.curBytes -= e.sizeBytes
}
