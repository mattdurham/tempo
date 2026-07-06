package vblockpack

// content_cache_list_test.go — tests for the TTL'd listing cache and the
// list-only wrapping mode added for the cube-backfill/cardinality-gate VCNT read
// path (blockpack issue #478). These exercise the parts newCachingStoreWithListTTL
// adds on top of the immutable Get/Size/ReadAt caches already covered by
// content_cache_range_test.go.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listCountingStore is an in-memory valueIndexStore that records how many times
// List and Get are invoked per prefix/key, so tests can assert the listing cache
// (blockpack issue #478) and the shared content cache actually collapse the
// repeated re-lists / re-fetches the cube-backfill path issues.
type listCountingStore struct {
	mu        sync.Mutex
	listings  map[string][]string
	objects   map[string][]byte
	listHits  map[string]*int64
	getHits   map[string]*int64
	listErr   error
	getErr    error
	block     chan struct{}
	blockList bool
}

func newListCountingStore() *listCountingStore {
	return &listCountingStore{
		listings: map[string][]string{},
		objects:  map[string][]byte{},
		listHits: map[string]*int64{},
		getHits:  map[string]*int64{},
	}
}

func (s *listCountingStore) putListing(prefix string, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listings[prefix] = keys
	// Preserve any existing call counter so a test can rewrite the listing (e.g.
	// simulate a newly-written file) without resetting the count it is asserting.
	if _, ok := s.listHits[prefix]; !ok {
		var z int64
		s.listHits[prefix] = &z
	}
}

func (s *listCountingStore) putObject(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = data
	var z int64
	s.getHits[key] = &z
}

func (s *listCountingStore) listCalls(prefix string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.listHits[prefix]; ok {
		return atomic.LoadInt64(c)
	}
	return 0
}

func (s *listCountingStore) getCalls(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.getHits[key]; ok {
		return atomic.LoadInt64(c)
	}
	return 0
}

func (s *listCountingStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	err := s.listErr
	keys, ok := s.listings[prefix]
	c := s.listHits[prefix]
	blk := s.block
	shouldBlock := s.blockList
	s.mu.Unlock()
	if c != nil {
		atomic.AddInt64(c, 1)
	}
	if shouldBlock && blk != nil {
		<-blk
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	out := make([]string, len(keys))
	copy(out, keys)
	return out, nil
}

func (s *listCountingStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	err := s.getErr
	data, ok := s.objects[key]
	c := s.getHits[key]
	s.mu.Unlock()
	if c != nil {
		atomic.AddInt64(c, 1)
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no such key")
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (s *listCountingStore) Size(string) (int64, error)                { return 0, nil }
func (s *listCountingStore) ReadAt(string, []byte, int64) (int, error) { return 0, nil }

// TestListCacheCollapsesRepeatedLists: within the TTL, re-listing the same prefix
// hits the cache and does not touch the inner store again — the core win for the
// cube-backfill/cardinality-gate path that re-lists the same (tenant, column, type)
// prefix repeatedly (blockpack issue #478).
func TestListCacheCollapsesRepeatedLists(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("t/indexes/col/string/", []string{"a", "b", "c"})
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute)

	for i := 0; i < 5; i++ {
		keys, err := cs.List(context.Background(), "t/indexes/col/string/")
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, keys)
	}
	assert.Equal(t, int64(1), inner.listCalls("t/indexes/col/string/"),
		"only the first List should reach the inner store within the TTL")
}

// TestListCacheExpiresAfterTTL: once the TTL elapses a re-list refetches, so a
// newly-written file becomes visible (listings are not immutable, unlike object
// bytes). Uses an injected clock so the test is deterministic and fast.
func TestListCacheExpiresAfterTTL(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("p/", []string{"a"})
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute).(*cachingStore)

	now := time.Unix(0, 0)
	cs.lists.now = func() time.Time { return now }

	_, err := cs.List(context.Background(), "p/")
	require.NoError(t, err)
	assert.Equal(t, int64(1), inner.listCalls("p/"))

	// Still within the TTL: cache hit, no new inner call.
	now = now.Add(30 * time.Second)
	_, err = cs.List(context.Background(), "p/")
	require.NoError(t, err)
	assert.Equal(t, int64(1), inner.listCalls("p/"))

	// A new file appears and the TTL elapses: the next List must refetch and see it.
	inner.putListing("p/", []string{"a", "b"})
	now = now.Add(31 * time.Second) // total 61s > 60s TTL
	keys, err := cs.List(context.Background(), "p/")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, keys)
	assert.Equal(t, int64(2), inner.listCalls("p/"),
		"an expired entry must trigger a refetch that observes the new file")
}

// TestListCacheReturnsCopy: a caller mutating (sorting/appending to) the returned
// slice must not corrupt the cached listing — the next reader still sees the
// original keys.
func TestListCacheReturnsCopy(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("p/", []string{"a", "b", "c"})
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute)

	keys1, err := cs.List(context.Background(), "p/")
	require.NoError(t, err)
	keys1[0] = "MUTATED"

	keys2, err := cs.List(context.Background(), "p/")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, keys2,
		"mutating a returned listing must not corrupt the cached entry")
}

// TestListCacheError: a List error is never cached — a transient failure must not
// poison the prefix, so the next call retries the inner store.
func TestListCacheError(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("p/", []string{"a"})
	inner.listErr = errors.New("transient")
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute)

	_, err := cs.List(context.Background(), "p/")
	require.Error(t, err)

	inner.mu.Lock()
	inner.listErr = nil
	inner.mu.Unlock()

	keys, err := cs.List(context.Background(), "p/")
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, keys,
		"a cached error must not poison the prefix; the next call retries")
	assert.Equal(t, int64(2), inner.listCalls("p/"))
}

// TestListCacheSingleflight: N concurrent misses on the same prefix collapse to a
// single inner List via singleflight — the dedup the backfill fan-out relies on
// (blockpack issue #478).
func TestListCacheSingleflight(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("p/", []string{"a", "b"})
	inner.block = make(chan struct{})
	inner.blockList = true
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute)

	const n = 8
	var wg sync.WaitGroup
	results := make([][]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			keys, err := cs.List(context.Background(), "p/")
			require.NoError(t, err)
			results[i] = keys
		}(i)
	}
	// Give the goroutines time to all enter the (blocked) inner List / flight.
	time.Sleep(50 * time.Millisecond)
	close(inner.block)
	wg.Wait()

	for i := 0; i < n; i++ {
		assert.Equal(t, []string{"a", "b"}, results[i])
	}
	assert.LessOrEqual(t, inner.listCalls("p/"), int64(2),
		"concurrent misses on the same prefix must collapse to (near) one inner List")
}

// TestListOnlyWrappingPassesGetThrough: with maxBytes <= 0 but a positive listTTL,
// the listing cache is active but Get/ReadAt pass straight through (no content
// cache). Verifies the nil-cache guards on Get/ReadAt.
func TestListOnlyWrappingPassesGetThrough(t *testing.T) {
	inner := newListCountingStore()
	inner.putListing("p/", []string{"a"})
	inner.putObject("k", []byte("hello"))
	cs := newCachingStoreWithListTTL(inner, 0, time.Minute)

	// List is cached.
	for i := 0; i < 3; i++ {
		_, err := cs.List(context.Background(), "p/")
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), inner.listCalls("p/"))

	// Get passes through every time (no content cache configured).
	for i := 0; i < 3; i++ {
		data, err := cs.Get(context.Background(), "k")
		require.NoError(t, err)
		assert.Equal(t, "hello", string(data))
	}
	assert.Equal(t, int64(3), inner.getCalls("k"),
		"with no content cache, every Get must reach the inner store")
}

// TestBothCachesDisabledReturnsRawStore: maxBytes <= 0 AND listTTL <= 0 returns the
// inner store unwrapped — byte-identical to before, no dedup on any path.
func TestBothCachesDisabledReturnsRawStore(t *testing.T) {
	inner := newListCountingStore()
	got := newCachingStoreWithListTTL(inner, 0, 0)
	require.Same(t, inner, got)
}

// TestBackfillContentCacheCollapsesRefetch: with a positive content budget, the
// cube-backfill path's repeated Get of the same immutable VI/VCNT file hits the
// content cache — re-scanning overlapping windows no longer re-downloads the file
// (blockpack issue #478).
func TestBackfillContentCacheCollapsesRefetch(t *testing.T) {
	inner := newListCountingStore()
	inner.putObject("t/indexes/col/string/f.vcnt", []byte("payload-bytes"))
	cs := newCachingStoreWithListTTL(inner, 1<<20, time.Minute)

	for i := 0; i < 4; i++ {
		data, err := cs.Get(context.Background(), "t/indexes/col/string/f.vcnt")
		require.NoError(t, err)
		assert.Equal(t, "payload-bytes", string(data))
	}
	assert.Equal(t, int64(1), inner.getCalls("t/indexes/col/string/f.vcnt"),
		"an immutable VI/VCNT file must be downloaded once and served from cache after")
}
