package vblockpack

// content_cache.go — process-level content cache + singleflight dedup for
// trace-by-ID index file fetches (blockpack issue #475).
//
// Production incident 2026-07-06: tempo's tempodb.readerWriter.Find fans
// FindTraceByID out across every block whose time range overlaps the query, all
// sharing one request context. With no time-range hint on a plain
// /api/traces/{id} request that can be hundreds of blocks. Trace-by-ID index
// files are large (19MB-205MB in production) and, because L0 files are merged
// during compaction, many different blocks' discovery windows resolve to the
// SAME compacted index file key. blockpack's findTraceGroupInCandidates calls
// LookupStore.Get(ctx, key) — a whole-object fetch — for every candidate, so N
// overlapping blocks trigger N separate full downloads of the same file
// concurrently with zero sharing. Confirmed reproducible: one query fanned out to
// 24 blocks referencing only 5 unique index file keys, all timing out with
// context deadline exceeded.
//
// cachingStore wraps the raw minioVIStore so its Get (used exclusively by the
// trace-by-ID index lookup path) is served from a size-bounded LRU keyed by
// object key, with a singleflight so concurrent misses on the same key collapse
// to one download. Index files are immutable once written (a compaction produces
// a NEW key, never mutates an existing one), so the key alone is a complete cache
// identity — no TTL, no invalidation. List/Size/ReadAt (the search/metrics path)
// pass straight through: those are already ranged, small, and covered by
// blockpack's own SectionCache/IndexFileCache.
//
// This mirrors the SectionCache/TypedTieredCache pattern getCache() already uses
// for data-block reads. It is scoped narrowly per issue #475: tempo-side only, no
// blockpack API or wire-format change.
//
// Update (blockpack issue #476 / NOTE-VI-075): the trace-by-id index wire format
// has since been rebuilt into a batched min/max+bloom+footer format that resolves
// a lookup with targeted partial reads (Size + ranged ReadAt of only the footer +
// block directory + surviving block), never a whole-object Get. So for v2 index
// files this Get cache is no longer on the hot path — those go through the ReadAt
// passthrough below. Get is still exercised for legacy pre-#476 flat-blob files
// during the rollover window (blockpack's findTraceGroupInCandidates falls back to
// Get + full decode for a non-v2 file), so the cache is retained: it keeps the
// oversized legacy files from being re-downloaded whole per candidate until
// retention ages them out.

import (
	"context"
	"sync"

	blockpack "github.com/grafana/blockpack"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/sync/singleflight"
)

// defaultContentCacheBytes is the trace-by-ID index content-cache budget used when
// value_index_query.content_cache_bytes is left at zero. Production index files run
// 19MB-205MB (blockpack issue #475); 2 GiB holds a working set of the largest
// compacted files while staying well under the querier's memory envelope. A negative
// config value disables the cache entirely.
const defaultContentCacheBytes = 2 << 30 // 2 GiB

// contentCache is a byte-bounded LRU of immutable object bytes keyed by object
// key. simplelru is count-bounded, so we track total bytes ourselves and evict
// the oldest entries until the new entry fits. It holds caller-owned copies of
// the fetched bytes; callers must not mutate returned slices in place (the
// trace-by-ID path only decodes, never mutates).
type contentCache struct {
	mu       sync.Mutex
	lru      *simplelru.LRU[string, []byte]
	curBytes int64
	maxBytes int64
}

func newContentCache(maxBytes int64) *contentCache {
	c := &contentCache{maxBytes: maxBytes}
	// A large count cap that we never actually hit — eviction is driven by the
	// byte budget in add(), not by count. simplelru requires a positive size.
	lru, _ := simplelru.NewLRU[string, []byte](1<<30, func(_ string, v []byte) {
		c.curBytes -= int64(len(v))
	})
	c.lru = lru
	return c
}

// get returns the cached bytes for key, or (nil, false) on a miss.
func (c *contentCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Get(key)
}

// add inserts data under key, evicting oldest entries until it fits the byte
// budget. An entry larger than the whole budget is not cached (it would evict
// everything and still not fit) but is still returned to the caller by the
// fetch path — caching is best-effort, correctness never depends on it.
func (c *contentCache) add(key string, data []byte) {
	sz := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	if sz > c.maxBytes {
		return
	}
	// Adding may replace an existing entry; the evict callback keeps curBytes in
	// step for both replacement and eviction.
	if old, ok := c.lru.Peek(key); ok {
		c.curBytes -= int64(len(old))
	}
	c.curBytes += sz
	c.lru.Add(key, data)
	for c.curBytes > c.maxBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			break
		}
	}
}

// cachingStore decorates a valueIndexStore so trace-by-ID index file fetches
// (Get) hit a process-level content cache with singleflight dedup. All other
// methods pass through unchanged.
type cachingStore struct {
	inner valueIndexStore
	cache *contentCache
	group singleflight.Group
}

// newCachingStore wraps inner with a byte-bounded content cache for Get. A
// maxBytes <= 0 disables caching and returns inner unwrapped, so the trace-by-ID
// path is byte-identical to before (raw store, no dedup).
func newCachingStore(inner valueIndexStore, maxBytes int64) valueIndexStore {
	if maxBytes <= 0 {
		return inner
	}
	return &cachingStore{inner: inner, cache: newContentCache(maxBytes)}
}

// Get serves index file bytes from the content cache, deduplicating concurrent
// misses on the same key via singleflight so the N-way redundant download
// collapses to one. Index files are immutable per key, so a cache hit is always
// valid. On any inner fetch error nothing is cached and the error propagates so
// the caller still sees a correct failure (blockpack's GetTraceByID treats a
// fetch error as a hard error, not a silent miss).
func (s *cachingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if data, ok := s.cache.get(key); ok {
		return data, nil
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		// Re-check under the flight: an earlier flight for the same key may have
		// already populated the cache while we waited to become the leader.
		if data, ok := s.cache.get(key); ok {
			return data, nil
		}
		data, gerr := s.inner.Get(ctx, key)
		if gerr != nil {
			return nil, gerr
		}
		s.cache.add(key, data)
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// List passes through — DiscoverIndexFiles listing is cheap and already covered
// by blockpack's IndexFileCache; there is nothing large to dedup here.
func (s *cachingStore) List(ctx context.Context, prefix string) ([]string, error) {
	return s.inner.List(ctx, prefix)
}

// Size passes through — search/metrics path, not the trace-by-ID fetch this cache targets.
func (s *cachingStore) Size(key string) (int64, error) {
	return s.inner.Size(key)
}

// ReadAt passes through — search/metrics path uses ranged reads that are already
// small and covered by blockpack's SectionCache.
func (s *cachingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	return s.inner.ReadAt(key, p, off)
}

// Compile-time assertions that cachingStore still satisfies every interface the
// trace-by-ID and search/metrics paths require of the store.
var (
	_ valueIndexStore               = (*cachingStore)(nil)
	_ blockpack.LookupStore         = (*cachingStore)(nil)
	_ blockpack.ValueIndexFileStore = (*cachingStore)(nil)
)
