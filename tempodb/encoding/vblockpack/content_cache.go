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
//
// NOTE-VI-107: this package's caches (contentCache/listCache) are in-process-only
// by design (immutable-key LRU + singleflight, no memcache tier) — considered and
// deliberately deferred during issue #515 (querier blockpack cache backing audit,
// 2026-07-17). #515 wired a memcache tier for the MAIN data-block path
// (backend_block.go's getCache()/TypedTieredCache) but explicitly left this
// package's value-index/trace-by-ID index-file cache out of scope: adding a
// memcache tier here would change cachingStore's public constructor shape
// (newCachingStoreWithListTTL etc.), a larger surface change than #515's
// config-only fix, for a lower-priority benefit (this cache's cold-after-restart
// exposure is real but smaller-blast-radius than the bulk Block-content path that
// actually triggered #515's production retry storm). Revisit if telemetry shows
// this path's cold-start cost is independently significant.

import (
	"context"
	"strconv"
	"sync"
	"time"

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

// listCache memoises List(prefix) results for a bounded time (blockpack issue
// #478, the cube-backfill/cardinality-gate VCNT read path). Unlike object bytes,
// sizes, and ranges — all immutable per key — a directory LISTING is NOT
// immutable: a new VI/VCNT file written under the prefix changes the correct
// result. So a listing entry is only valid for ttl, after which it is refetched.
// The backfill/cardinality-gate path re-lists the SAME (tenant, column, type)
// prefix repeatedly within a single pass (per span, per repeated TryCreate); a
// short TTL collapses those bursts to one LIST while still observing newly-written
// files within ttl. The entry count is bounded by the live prefix set (one per
// tenant/column/type), so no byte budget is needed — each value is a slice of
// keys, far smaller than a cached object.
type listCache struct {
	mu      sync.Mutex
	entries map[string]listCacheEntry
	ttl     time.Duration
	now     func() time.Time // injectable for tests; defaults to time.Now
}

type listCacheEntry struct {
	keys    []string
	fetched time.Time
}

func newListCache(ttl time.Duration) *listCache {
	return &listCache{
		entries: make(map[string]listCacheEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

// get returns the cached keys for prefix if a fresh (within ttl) entry exists.
// It returns a copy so a caller mutating (e.g. sorting/appending to) the returned
// slice cannot corrupt the cached entry.
func (c *listCache) get(prefix string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[prefix]
	if !ok || c.now().Sub(e.fetched) >= c.ttl {
		return nil, false
	}
	out := make([]string, len(e.keys))
	copy(out, e.keys)
	return out, true
}

// add stores a copy of keys under prefix with the current fetch time.
func (c *listCache) add(prefix string, keys []string) {
	stored := make([]string, len(keys))
	copy(stored, keys)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[prefix] = listCacheEntry{keys: stored, fetched: c.now()}
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
	// lists, when non-nil, memoises List(prefix) results for a bounded TTL
	// (blockpack issue #478). It is nil on the query path (List passes straight
	// through, already covered by blockpack's IndexFileCache) and set only for the
	// cube-backfill/cardinality-gate path, which has no IndexFileCache in front and
	// re-lists the same prefix repeatedly.
	lists *listCache
	// group dedups whole-object Get flights (keyed by object key); rangeGroup dedups
	// ranged ReadAt flights (keyed by "key|off|len"); listGroup dedups List flights
	// (keyed by prefix). Separate groups so a Get, a ReadAt, and a List of the same
	// key/prefix never collide on one flight key.
	group      singleflight.Group
	rangeGroup singleflight.Group
	listGroup  singleflight.Group
}

// newCachingStore wraps inner with byte-bounded caches for Get and ReadAt (sharing
// maxBytes) plus a Size memo. List passes straight through (already covered by
// blockpack's IndexFileCache on the query path). A maxBytes <= 0 disables all
// caching and returns inner unwrapped, so every read path is byte-identical to
// before (raw store, no dedup).
func newCachingStore(inner valueIndexStore, maxBytes int64) valueIndexStore {
	return newCachingStoreWithListTTL(inner, maxBytes, 0)
}

// newCachingStoreWithListTTL is newCachingStore plus an optional listing cache
// (blockpack issue #478). A listTTL > 0 memoises List(prefix) results for that
// duration; listTTL <= 0 leaves List a straight pass-through (the query-path
// default, where IndexFileCache already covers listings). Used by the
// cube-backfill/cardinality-gate VCNT path, which has no IndexFileCache and
// re-lists the same (tenant, column, type) prefix repeatedly. When both maxBytes
// <= 0 and listTTL <= 0 there is nothing to cache, so inner is returned unwrapped
// (byte-identical to before, no dedup).
func newCachingStoreWithListTTL(inner valueIndexStore, maxBytes int64, listTTL time.Duration) valueIndexStore {
	if maxBytes <= 0 && listTTL <= 0 {
		return inner
	}
	cs := &cachingStore{inner: inner, sizes: newSizeCache()}
	if maxBytes > 0 {
		cs.cache = newContentCache(maxBytes)
		cs.ranges = newRangeCache(maxBytes)
	}
	if listTTL > 0 {
		cs.lists = newListCache(listTTL)
	}
	return cs
}

// Get serves index file bytes from the content cache, deduplicating concurrent
// misses on the same key via singleflight so the N-way redundant download
// collapses to one. Index files are immutable per key, so a cache hit is always
// valid. On any inner fetch error nothing is cached and the error propagates so
// the caller still sees a correct failure (blockpack's GetTraceByID treats a
// fetch error as a hard error, not a silent miss).
func (s *cachingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.cache == nil {
		// No content cache configured (list-only wrapping): pass straight through.
		return s.inner.Get(ctx, key)
	}
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

// List serves object listings from the TTL'd listing cache when one is
// configured (blockpack issue #478, the cube-backfill/cardinality-gate VCNT
// path), deduplicating concurrent misses on the same prefix via singleflight so
// repeated re-lists of the same (tenant, column, type) collapse to one LIST. When
// no listing cache is configured (the query path, where blockpack's
// IndexFileCache already covers listings) List passes straight through. Listings
// are NOT immutable — a newly-written file changes the correct result — so an
// entry is only valid within its TTL, unlike the immutable Get/Size/ReadAt caches.
// On any inner error nothing is cached and the error propagates.
func (s *cachingStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.lists == nil {
		return s.inner.List(ctx, prefix)
	}
	if keys, ok := s.lists.get(prefix); ok {
		return keys, nil
	}
	v, err, _ := s.listGroup.Do(prefix, func() (interface{}, error) {
		// Re-check under the flight: an earlier flight for the same prefix may have
		// populated the cache while we waited to become the leader.
		if keys, ok := s.lists.get(prefix); ok {
			return keys, nil
		}
		keys, lerr := s.inner.List(ctx, prefix)
		if lerr != nil {
			return nil, lerr
		}
		s.lists.add(prefix, keys)
		return keys, nil
	})
	if err != nil {
		return nil, err
	}
	// Copy: the singleflight result is shared across all waiters, and get() already
	// returns copies, so callers must never observe a shared slice they might mutate.
	keys := v.([]string)
	out := make([]string, len(keys))
	copy(out, keys)
	return out, nil
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
	if s.ranges == nil {
		// No range cache configured (list-only wrapping): pass straight through.
		return s.inner.ReadAt(key, p, off)
	}
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

// sizeCtx serves the object length exactly like Size, threading ctx down to the inner
// store's own ctx-aware path (task #199, NOTE-VI-106 follow-up) on a cache miss so a
// real per-query deadline can actually cancel the in-flight StatObject call. A cache
// hit needs no ctx at all (no network call), so it is identical to Size's hit path.
func (s *cachingStore) sizeCtx(ctx context.Context, key string) (int64, error) {
	if sz, ok := s.sizes.get(key); ok {
		return sz, nil
	}
	sz, err := sizeWithCtx(ctx, s.inner, key)
	if err != nil {
		return 0, err
	}
	s.sizes.add(key, sz)
	return sz, nil
}

// readAtCtx serves a ranged read exactly like ReadAt, threading ctx down to the inner
// store's own ctx-aware path (task #199) on a cache miss. A cache hit needs no ctx
// (no network call).
//
// Task #200: the flightKey below is shared with plain ReadAt's s.rangeGroup.Do call above,
// and — more importantly — with every OTHER concurrent readAtCtx caller that happens to
// request the exact same (key, off, len), which is the documented common case for
// candidate index files (NOTE-VI-076). singleflight.Group.Do makes the first caller to
// arrive for a given key the "leader": only the leader's function body actually runs: every
// other caller just blocks until it completes and observes the SAME result/error. If the
// leader's own ctx were used for the actual I/O (as a naive threading of ctx into the
// flight body would do), one caller's own cancellation/deadline would abort the shared
// operation and hand every unrelated concurrent waiter — including callers whose OWN ctx is
// nowhere near expiring — that same spurious cancellation error. So the flight body below
// deliberately uses context.Background() (via readAtWithCtx), detaching the shared,
// singleflight-deduplicated I/O from any single waiter's cancellation, while this specific
// call's own ctx is still honored: the select below lets THIS goroutine return ctx.Err()
// immediately on its own cancellation without cancelling the in-flight work for any other
// waiter sharing the same flight (DoChan, unlike Do, returns a channel immediately instead
// of blocking, which is what makes the select possible).
func (s *cachingStore) readAtCtx(ctx context.Context, key string, p []byte, off int64) (int, error) {
	if s.ranges == nil {
		return readAtWithCtx(ctx, s.inner, key, p, off)
	}
	rk := rangeKey{key: key, off: off, len: len(p)}
	if data, ok := s.ranges.get(rk); ok {
		return copy(p, data), nil
	}
	flightKey := key + "|" + strconv.FormatInt(off, 10) + "|" + strconv.Itoa(len(p))
	ch := s.rangeGroup.DoChan(flightKey, func() (interface{}, error) {
		if data, ok := s.ranges.get(rk); ok {
			return data, nil
		}
		buf := make([]byte, len(p))
		// Deliberately context.Background(), not ctx: this flight body may be run on
		// behalf of an unrelated concurrent caller sharing the same flightKey (see the
		// doc comment above) — it must not be tied to any single waiter's cancellation.
		n, rerr := readAtWithCtx(context.Background(), s.inner, key, buf, off)
		if rerr != nil {
			return nil, rerr
		}
		if n != len(buf) {
			return buf[:n], nil
		}
		s.ranges.add(rk, buf)
		return buf, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return 0, res.Err
		}
		return copy(p, res.Val.([]byte)), nil
	case <-ctx.Done():
		// Only THIS caller gives up early; the flight itself (ch) keeps running to
		// completion in the background for every other waiter (including a later
		// s.ranges cache population that benefits everyone, us included, next time).
		return 0, ctx.Err()
	}
}

// Compile-time assertions that cachingStore still satisfies every interface the
// trace-by-ID and search/metrics paths require of the store.
var (
	_ valueIndexStore               = (*cachingStore)(nil)
	_ blockpack.LookupStore         = (*cachingStore)(nil)
	_ blockpack.ValueIndexFileStore = (*cachingStore)(nil)
	_ ctxAwareStore                 = (*cachingStore)(nil)
)

// ctxAwareStore is an OPTIONAL, package-private capability (task #199, NOTE-VI-106
// follow-up): a store that can serve Size/ReadAt with a caller-supplied ctx instead of
// always using context.Background() internally. blockpack.ValueIndexFileStore's fixed
// Size(key)/ReadAt(key,p,off) signatures (shared across both the search/metrics and
// trace-by-id paths, and implemented by every FileStore/LookupStore in both repos) are
// deliberately left UNCHANGED — widening them to accept a context would be a breaking
// public-API change requiring explicit cross-repo sign-off, which is out of scope here.
// Every concrete store this package can construct (*minioVIStore, *cachingStore,
// *rawFileStore) implements this so ctxBoundLookupStore below can always reach it; a
// hypothetical store that does NOT (e.g. a test fake) is handled by sizeWithCtx/
// readAtWithCtx falling back to the plain, ctx-less Size/ReadAt.
type ctxAwareStore interface {
	sizeCtx(ctx context.Context, key string) (int64, error)
	readAtCtx(ctx context.Context, key string, p []byte, off int64) (int, error)
}

// sizeWithCtx calls store's ctx-aware sizeCtx when available, falling back to the plain
// Size(key) (context.Background() semantics, byte-identical to before task #199) for a
// store that does not implement ctxAwareStore — e.g. a test double.
func sizeWithCtx(ctx context.Context, store valueIndexStore, key string) (int64, error) {
	if cs, ok := store.(ctxAwareStore); ok {
		return cs.sizeCtx(ctx, key)
	}
	return store.Size(key)
}

// readAtWithCtx calls store's ctx-aware readAtCtx when available, falling back to the
// plain ReadAt(key,p,off) for a store that does not implement ctxAwareStore.
func readAtWithCtx(ctx context.Context, store valueIndexStore, key string, p []byte, off int64) (int, error) {
	if cs, ok := store.(ctxAwareStore); ok {
		return cs.readAtCtx(ctx, key, p, off)
	}
	return store.ReadAt(key, p, off)
}

// ctxBoundLookupStore binds a single query's ctx to a valueIndexStore's Size/ReadAt
// calls (task #199) without changing blockpack.LookupStore's signature at all: List and
// Get already take ctx as their own parameter and pass straight through unchanged; only
// Size/ReadAt — which the interface fixes at ctx-less — are rebound here to the query's
// real ctx via the ctxAwareStore capability above, so an in-flight StatObject/GetObject
// call can actually be cancelled by the caller's own deadline instead of running to
// completion against context.Background() regardless of the query having already timed
// out. Constructed FRESH per query (see bindQueryCtx) — never shared across queries — so
// holding ctx as a plain field is race-free even under FindTraceGroupInCandidates'
// concurrent candidate fan-out (NOTE-VI-106): every goroutine in that fan-out reads the
// same immutable ctx field, never mutates it.
type ctxBoundLookupStore struct {
	inner valueIndexStore
	ctx   context.Context
}

// bindQueryCtx returns store wrapped so its Size/ReadAt calls use ctx instead of
// context.Background() (task #199). Call once per query with that query's own ctx —
// never reuse across queries or hold beyond the query's lifetime.
//
// ctx is deliberately the first parameter (task #200 cleanup): this mirrors its own
// sibling functions (sizeWithCtx/readAtWithCtx, both ctx-first) and satisfies this repo's
// enforced revive context-as-argument lint rule, which the previous ctx-second signature
// violated.
func bindQueryCtx(ctx context.Context, store valueIndexStore) blockpack.LookupStore {
	return &ctxBoundLookupStore{inner: store, ctx: ctx}
}

func (c *ctxBoundLookupStore) List(ctx context.Context, prefix string) ([]string, error) {
	return c.inner.List(ctx, prefix)
}

func (c *ctxBoundLookupStore) Get(ctx context.Context, key string) ([]byte, error) {
	return c.inner.Get(ctx, key)
}

func (c *ctxBoundLookupStore) Size(key string) (int64, error) {
	return sizeWithCtx(c.ctx, c.inner, key)
}

func (c *ctxBoundLookupStore) ReadAt(key string, p []byte, off int64) (int, error) {
	return readAtWithCtx(c.ctx, c.inner, key, p, off)
}

var _ blockpack.LookupStore = (*ctxBoundLookupStore)(nil)
