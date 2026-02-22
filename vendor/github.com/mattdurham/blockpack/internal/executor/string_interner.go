package executor

import (
	"container/list"
	"strings"
	"sync"
)

const (
	// DefaultMaxCacheSize is the default maximum number of entries in the string cache
	// This prevents unbounded memory growth in high-cardinality aggregations
	DefaultMaxCacheSize = 10000
)

// StringInterner caches strings to reduce allocations with LRU eviction
type StringInterner struct {
	cache     map[string]*list.Element
	lruList   *list.List
	maxSize   int
	hits      int64 // Cache hit count (for metrics)
	misses    int64 // Cache miss count (for metrics)
	evictions int64 // Eviction count (for metrics)
	mu        sync.RWMutex
}

type cacheEntry struct {
	key   string
	value string
}

// NewStringInterner creates a new string interner with the default cache size.
func NewStringInterner() *StringInterner {
	return NewStringInternerWithSize(DefaultMaxCacheSize)
}

// NewStringInternerWithSize creates a new string interner with a specified maximum cache size.
func NewStringInternerWithSize(maxSize int) *StringInterner {
	return &StringInterner{
		cache:   make(map[string]*list.Element, maxSize),
		lruList: list.New(),
		maxSize: maxSize,
	}
}

// Intern returns a cached copy of the string if it exists, otherwise caches and returns the input
// Uses LRU eviction when cache size exceeds maxSize to prevent unbounded growth
func (si *StringInterner) Intern(s string) string {
	// Check if already cached (read lock)
	si.mu.RLock()
	elem, ok := si.cache[s]
	si.mu.RUnlock()

	if ok {
		// Cache hit - update LRU and stats (write lock)
		si.mu.Lock()
		si.hits++
		si.lruList.MoveToFront(elem)
		value := elem.Value.(*cacheEntry).value
		si.mu.Unlock()
		return value
	}

	// Cache miss - need to insert (write lock)
	si.mu.Lock()
	defer si.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have inserted)
	if elem, ok := si.cache[s]; ok { //nolint:govet
		si.hits++
		si.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value
	}

	si.misses++

	// Evict LRU entry if at capacity
	if si.lruList.Len() >= si.maxSize {
		oldest := si.lruList.Back()
		if oldest != nil {
			si.lruList.Remove(oldest)
			oldEntry := oldest.Value.(*cacheEntry)
			delete(si.cache, oldEntry.key)
			si.evictions++
		}
	}

	// Add new entry at front (most recently used)
	entry := &cacheEntry{key: s, value: s}
	elem = si.lruList.PushFront(entry)
	si.cache[s] = elem

	return s
}

// InternConcat concatenates strings and interns the result
func (si *StringInterner) InternConcat(parts ...string) string {
	// Build key inline to avoid allocation
	totalLen := 0
	for _, p := range parts {
		totalLen += len(p)
	}

	var b strings.Builder
	b.Grow(totalLen)
	for _, p := range parts {
		b.WriteString(p)
	}

	key := b.String()
	return si.Intern(key)
}
