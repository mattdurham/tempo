package vblockpack

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingStore is an in-memory valueIndexStore that records how many times Get is
// invoked per key, so tests can assert the content cache + singleflight dedup
// actually collapse redundant fetches. getDelay optionally blocks each fetch so
// concurrent callers overlap inside singleflight.
type countingStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	getHits map[string]*int64
	// block, when non-nil, is closed by the test to release all in-flight Gets at
	// once — used to force N concurrent callers into the same singleflight window.
	block  chan struct{}
	getErr error
}

func newCountingStore() *countingStore {
	return &countingStore{
		objects: map[string][]byte{},
		getHits: map[string]*int64{},
	}
}

func (s *countingStore) put(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = data
	var z int64
	s.getHits[key] = &z
}

func (s *countingStore) hits(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.getHits[key]; ok {
		return atomic.LoadInt64(c)
	}
	return 0
}

func (s *countingStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	blk := s.block
	err := s.getErr
	data, ok := s.objects[key]
	c := s.getHits[key]
	s.mu.Unlock()
	if c != nil {
		atomic.AddInt64(c, 1)
	}
	if blk != nil {
		<-blk
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no such key %q", key)
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (s *countingStore) List(context.Context, string) ([]string, error) { return nil, nil }
func (s *countingStore) Size(string) (int64, error)                     { return 0, nil }
func (s *countingStore) ReadAt(string, []byte, int64) (int, error)      { return 0, nil }

// TestCachingStoreDisabled: a non-positive budget returns the raw store unwrapped,
// so behaviour is byte-identical to before (no caching, no dedup).
func TestCachingStoreDisabled(t *testing.T) {
	inner := newCountingStore()
	inner.put("k", []byte("hello"))

	for _, budget := range []int64{0, -1} {
		got := newCachingStore(inner, budget)
		require.Same(t, inner, got, "budget %d should return raw store", budget)
	}
}

// TestCachingStoreCacheHit: a repeated Get for the same immutable key hits the
// cache on the second call and does not touch the inner store again.
func TestCachingStoreCacheHit(t *testing.T) {
	inner := newCountingStore()
	inner.put("idx-A", []byte("payload-A"))
	cs := newCachingStore(inner, 1<<20)

	for i := 0; i < 5; i++ {
		data, err := cs.Get(context.Background(), "idx-A")
		require.NoError(t, err)
		assert.Equal(t, "payload-A", string(data))
	}
	assert.Equal(t, int64(1), inner.hits("idx-A"), "only the first Get should reach the inner store")
}

// TestCachingStoreSingleflightDedup is the core of blockpack issue #475: N
// concurrent Gets for the SAME key while the fetch is in flight must collapse to a
// single inner download, not N.
func TestCachingStoreSingleflightDedup(t *testing.T) {
	inner := newCountingStore()
	inner.put("big-index", make([]byte, 4096))
	inner.block = make(chan struct{})
	cs := newCachingStore(inner, 1<<20)

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	started := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started <- struct{}{}
			_, errs[i] = cs.Get(context.Background(), "big-index")
		}(i)
	}
	// Wait for all goroutines to have entered Get before releasing the fetch, so
	// they all coalesce into the same singleflight window.
	for i := 0; i < n; i++ {
		<-started
	}
	close(inner.block)
	wg.Wait()

	for i := range errs {
		require.NoError(t, errs[i])
	}
	assert.Equal(t, int64(1), inner.hits("big-index"),
		"all %d concurrent Gets of the same key must collapse to one inner fetch", n)
}

// TestCachingStoreErrorNotCached: a fetch error propagates and is NOT cached — a
// later Get after the object becomes available must succeed. blockpack's
// GetTraceByID treats a fetch error as a hard error, so caching an error would
// wrongly poison the key.
func TestCachingStoreErrorNotCached(t *testing.T) {
	inner := newCountingStore()
	inner.getErr = errors.New("transient object store failure")
	cs := newCachingStore(inner, 1<<20)

	_, err := cs.Get(context.Background(), "k")
	require.Error(t, err)

	// Recover the store and make the object available.
	inner.mu.Lock()
	inner.getErr = nil
	inner.mu.Unlock()
	inner.put("k", []byte("now-here"))

	data, err := cs.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, "now-here", string(data))
}

// TestCachingStoreByteBoundedEviction: the LRU evicts by byte budget, not count.
// A budget that holds two entries evicts the oldest when a third is added.
func TestCachingStoreByteBoundedEviction(t *testing.T) {
	inner := newCountingStore()
	inner.put("a", make([]byte, 100))
	inner.put("b", make([]byte, 100))
	inner.put("c", make([]byte, 100))
	cs := newCachingStore(inner, 250).(*cachingStore)

	// Fill: a, then b — both fit under 250 bytes.
	_, _ = cs.Get(context.Background(), "a")
	_, _ = cs.Get(context.Background(), "b")
	// Touch a so it is more-recently-used than b.
	_, _ = cs.Get(context.Background(), "a")
	assert.Equal(t, int64(1), inner.hits("a"))
	assert.Equal(t, int64(1), inner.hits("b"))

	// Add c: total would be 300 > 250, so the oldest (b) is evicted.
	_, _ = cs.Get(context.Background(), "c")

	// a is still cached (no new inner hit); b was evicted (a re-fetch reaches inner).
	_, _ = cs.Get(context.Background(), "a")
	assert.Equal(t, int64(1), inner.hits("a"), "a should still be cached")
	_, _ = cs.Get(context.Background(), "b")
	assert.Equal(t, int64(2), inner.hits("b"), "b should have been evicted and re-fetched")
}

// TestCachingStoreOversizeNotCached: an entry larger than the whole budget is
// returned to the caller but never cached (it would evict everything and still not
// fit), so a repeat Get re-fetches from the inner store.
func TestCachingStoreOversizeNotCached(t *testing.T) {
	inner := newCountingStore()
	inner.put("huge", make([]byte, 1000))
	cs := newCachingStore(inner, 500)

	d1, err := cs.Get(context.Background(), "huge")
	require.NoError(t, err)
	assert.Len(t, d1, 1000)
	d2, err := cs.Get(context.Background(), "huge")
	require.NoError(t, err)
	assert.Len(t, d2, 1000)
	assert.Equal(t, int64(2), inner.hits("huge"),
		"an entry larger than the budget must not be cached")
}

// TestContentCacheBudgetTracking: curBytes stays in step across replace + evict so
// the byte budget is honoured over many overwrites of the same key.
func TestContentCacheBudgetTracking(t *testing.T) {
	c := newContentCache(300)
	c.add("k", make([]byte, 100))
	c.add("k", make([]byte, 200)) // replace: 100 -> 200, curBytes must be 200 not 300
	assert.Equal(t, int64(200), c.curBytes)

	c.add("j", make([]byte, 200)) // total would be 400 > 300, oldest (k) evicted
	_, ok := c.get("k")
	assert.False(t, ok, "k should have been evicted")
	got, ok := c.get("j")
	require.True(t, ok)
	assert.Len(t, got, 200)
	assert.Equal(t, int64(200), c.curBytes)
}
