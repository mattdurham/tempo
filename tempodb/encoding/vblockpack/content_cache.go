package vblockpack

// content_cache.go — process-level read caches + singleflight dedup for
// value-index / trace-by-ID index file reads: whole-object Get (blockpack issue
// #475) plus Size and ranged ReadAt (blockpack issue #477).
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
// cachingStore wraps the raw minioVIStore so its reads are served from
// process-level caches with singleflight dedup, mirroring the
// SectionCache/TypedTieredCache pattern getCache() already uses for data-block
// reads. It is scoped narrowly: tempo-side only, no blockpack API or wire-format
// change. Value-index / trace-by-ID index files are immutable once written (a
// compaction produces a NEW key, never mutates an existing one), so the key —
// plus, for a ranged read, the (offset, length) — is a complete cache identity:
// no TTL, no invalidation.
//
// Three read surfaces are cached (all with singleflight so concurrent misses on
// the same cache identity collapse to a single object-store round-trip):
//
//   - Get (whole object): the legacy pre-#476 trace-by-ID flat-blob path. A v1
//     file has no footer/TOC, so it must be fetched whole and DecodeTraceGroups'd.
//     Retained so oversized legacy files (19MB-205MB, blockpack issue #475) are
//     not re-downloaded whole per candidate until retention ages them out.
//   - Size (object length): both the search/metrics index path and the v2
//     trace-by-ID partial-read path stat the object before ranged reads. Cheap to
//     cache (one int64 per key), and immutability makes it exact forever.
//   - ReadAt (ranged read): the v2 batched min/max+bloom+footer format (issue #476,
//     now the PRIMARY trace-by-id path) and the search/metrics BuildValueIndexSource
//     path both resolve a query with targeted partial reads of the footer + block
//     directory + surviving blocks. Those ranges are small and recur across queries
//     for hot/popular data, so they are cached keyed by (key, offset, length).
//
// This closes blockpack issue #477: before it, Size/ReadAt passed straight through
// to a fresh minio round-trip on EVERY call of EVERY query, with zero reuse — the
// value-index/trace-by-id partial-read path was strictly worse than the main
// data-block path (which gets real disk+memcache reuse via getCache()). The
// SectionCache/IndexFileCache the earlier comment claimed covered this path do
// not: SectionCache (getCache()) is wired only to the main data-block reader,
// never to minioVIStore; IndexFileCache caches directory LISTINGS only, never
// downloaded bytes.

import (
	"context"
	"strconv"
	"sync"

	blockpack "github.com/grafana/blockpack"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/sync/singleflight"
)

// defaultContentCacheBytes is the value-index / trace-by-ID index read-cache
// budget used when value_index_query.content_cache_bytes is left at zero. It bounds
// the whole-object Get cache (issue #475) and the ranged ReadAt cache (issue #477)
// — each holds up to this many bytes. Production index files run 19MB-205MB
// (blockpack issue #475); 2 GiB holds a working set of the largest compacted files
// (Get path) or many small footer/block-directory/block ranges (ReadAt path) while
// staying well under the querier's memory envelope. A negative config value
// disables all read caching entirely (raw store, byte-identical to before).
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

// rangeKey identifies a single ranged read of an immutable object: the object
// key plus the (offset, length) of the range. Because files are immutable per
// key, this triple is a complete, permanent cache identity. Used as a map key so
// it must stay comparable (all fixed-size fields, no slices).
type rangeKey struct {
	key string
	off int64
	len int
}

// rangeCache is a byte-bounded LRU of immutable object ranges keyed by
// (key, offset, length), mirroring contentCache but for ReadAt's partial reads
// (blockpack issue #477). The v2 trace-by-id format and the search/metrics path
// both issue a small, recurring set of ranged reads (footer + block directory +
// surviving blocks) per file; caching those ranges lets popular/repeated queries
// and hot recent data reuse them instead of a fresh minio round-trip per call.
type rangeCache struct {
	mu       sync.Mutex
	lru      *simplelru.LRU[rangeKey, []byte]
	curBytes int64
	maxBytes int64
}

func newRangeCache(maxBytes int64) *rangeCache {
	c := &rangeCache{maxBytes: maxBytes}
	lru, _ := simplelru.NewLRU[rangeKey, []byte](1<<30, func(_ rangeKey, v []byte) {
		c.curBytes -= int64(len(v))
	})
	c.lru = lru
	return c
}

// get returns a copy of the cached bytes for rk, or (nil, false) on a miss. It
// copies because the cache owns the stored slice and the caller (ReadAt) writes
// the result into a caller-supplied buffer — an alias would let a later mutation
// of that buffer corrupt the cache.
func (c *rangeCache) get(rk rangeKey) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.lru.Get(rk)
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// add stores a copy of data under rk, evicting oldest entries until it fits the
// byte budget. A range larger than the whole budget is not cached (best-effort;
// correctness never depends on the cache). It copies so the cache does not alias
// the caller's buffer.
func (c *rangeCache) add(rk rangeKey, data []byte) {
	sz := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	if sz > c.maxBytes {
		return
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	if old, ok := c.lru.Peek(rk); ok {
		c.curBytes -= int64(len(old))
	}
	c.curBytes += sz
	c.lru.Add(rk, stored)
	for c.curBytes > c.maxBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			break
		}
	}
}

// sizeCache memoises Size(key) results. Object lengths are immutable per key
// (blockpack issue #477), so a cached size is exact forever; the entries are one
// int64 each, so the map is unbounded (a querier sees a bounded set of live index
// keys, and the whole entry-set is far smaller than a single cached range).
type sizeCache struct {
	mu    sync.Mutex
	sizes map[string]int64
}

func newSizeCache() *sizeCache {
	return &sizeCache{sizes: make(map[string]int64)}
}

func (c *sizeCache) get(key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sz, ok := c.sizes[key]
	return sz, ok
}

func (c *sizeCache) add(key string, sz int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sizes[key] = sz
}

// cachingStore decorates a valueIndexStore so value-index / trace-by-ID index
// file reads hit process-level caches with singleflight dedup: whole-object Get
// (blockpack issue #475), object Size, and ranged ReadAt (blockpack issue #477).
// List passes through — its result is already covered by blockpack's
// IndexFileCache. The Get and ReadAt caches share a single byte budget (maxBytes)
// so the two paths compete for one bounded pool rather than each holding a
// separate multi-GiB reservation.
type cachingStore struct {
	inner  valueIndexStore
	cache  *contentCache
	ranges *rangeCache
	sizes  *sizeCache
	// group dedups whole-object Get flights (keyed by object key); rangeGroup dedups
	// ranged ReadAt flights (keyed by "key|off|len"). Separate groups so a Get and a
	// ReadAt of the same key never collide on one flight key.
	group      singleflight.Group
	rangeGroup singleflight.Group
}

// newCachingStore wraps inner with byte-bounded caches for Get and ReadAt (sharing
// maxBytes) plus a Size memo. A maxBytes <= 0 disables all caching and returns
// inner unwrapped, so every read path is byte-identical to before (raw store, no
// dedup).
func newCachingStore(inner valueIndexStore, maxBytes int64) valueIndexStore {
	if maxBytes <= 0 {
		return inner
	}
	return &cachingStore{
		inner:  inner,
		cache:  newContentCache(maxBytes),
		ranges: newRangeCache(maxBytes),
		sizes:  newSizeCache(),
	}
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

// Size serves the object length from the size memo. Object lengths are immutable
// per key (blockpack issue #477), so a cached size is always valid. An error is
// never cached — a transient stat failure must not poison the key.
func (s *cachingStore) Size(key string) (int64, error) {
	if sz, ok := s.sizes.get(key); ok {
		return sz, nil
	}
	sz, err := s.inner.Size(key)
	if err != nil {
		return 0, err
	}
	s.sizes.add(key, sz)
	return sz, nil
}

// ReadAt serves a ranged read from the range cache, deduplicating concurrent
// misses on the same (key, off, len) via singleflight so redundant round-trips
// for the same footer/block-directory/block range collapse to one (blockpack
// issue #477). Ranges are immutable per (key, off, len), so a hit is always
// valid. On any inner read error nothing is cached and the error propagates, so
// the caller still sees a correct failure. Only a full read (n == len(p), err ==
// nil) is cached — a short read is an error condition (io.ReaderAt contract) that
// must not be memoised as if it were the whole range.
func (s *cachingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	rk := rangeKey{key: key, off: off, len: len(p)}
	if data, ok := s.ranges.get(rk); ok {
		return copy(p, data), nil
	}
	flightKey := key + "|" + strconv.FormatInt(off, 10) + "|" + strconv.Itoa(len(p))
	v, err, _ := s.rangeGroup.Do(flightKey, func() (interface{}, error) {
		// Re-check under the flight: an earlier flight for the same range may have
		// populated the cache while we waited to become the leader.
		if data, ok := s.ranges.get(rk); ok {
			return data, nil
		}
		buf := make([]byte, len(p))
		n, rerr := s.inner.ReadAt(key, buf, off)
		if rerr != nil {
			return nil, rerr
		}
		if n != len(buf) {
			// A non-error short read: cannot memoise a partial fill as the whole
			// range. Return what we read so the caller sees the exact same bytes
			// the raw store would have produced, without caching.
			return buf[:n], nil
		}
		s.ranges.add(rk, buf)
		return buf, nil
	})
	if err != nil {
		return 0, err
	}
	return copy(p, v.([]byte)), nil
}

// Compile-time assertions that cachingStore still satisfies every interface the
// trace-by-ID and search/metrics paths require of the store.
var (
	_ valueIndexStore               = (*cachingStore)(nil)
	_ blockpack.LookupStore         = (*cachingStore)(nil)
	_ blockpack.ValueIndexFileStore = (*cachingStore)(nil)
)
