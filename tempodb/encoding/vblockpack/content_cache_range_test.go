package vblockpack

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rangeCountingStore is an in-memory valueIndexStore that records how many times
// Size and ReadAt are invoked per key, so tests can assert the size memo and range
// cache (blockpack issue #477) actually collapse redundant reads. block, when
// non-nil, is closed by the test to release all in-flight ReadAts at once — used to
// force N concurrent callers into the same singleflight window.
type rangeCountingStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	sizeHits   map[string]*int64
	readAtHits map[string]*int64
	block      chan struct{}
	sizeErr    error
	readAtErr  error
}

func newRangeCountingStore() *rangeCountingStore {
	return &rangeCountingStore{
		objects:    map[string][]byte{},
		sizeHits:   map[string]*int64{},
		readAtHits: map[string]*int64{},
	}
}

func (s *rangeCountingStore) put(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = data
	var z1, z2 int64
	s.sizeHits[key] = &z1
	s.readAtHits[key] = &z2
}

func (s *rangeCountingStore) sizeCalls(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.sizeHits[key]; ok {
		return atomic.LoadInt64(c)
	}
	return 0
}

func (s *rangeCountingStore) readAtCalls(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.readAtHits[key]; ok {
		return atomic.LoadInt64(c)
	}
	return 0
}

func (s *rangeCountingStore) Get(context.Context, string) ([]byte, error) { return nil, nil }
func (s *rangeCountingStore) List(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *rangeCountingStore) Size(key string) (int64, error) {
	s.mu.Lock()
	err := s.sizeErr
	data, ok := s.objects[key]
	c := s.sizeHits[key]
	s.mu.Unlock()
	if c != nil {
		atomic.AddInt64(c, 1)
	}
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("no such key")
	}
	return int64(len(data)), nil
}

func (s *rangeCountingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	s.mu.Lock()
	blk := s.block
	err := s.readAtErr
	data, ok := s.objects[key]
	c := s.readAtHits[key]
	s.mu.Unlock()
	if c != nil {
		atomic.AddInt64(c, 1)
	}
	if blk != nil {
		<-blk
	}
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("no such key")
	}
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

// TestCachingStoreSizeMemo: a repeated Size for the same immutable key hits the
// memo on the second call and does not touch the inner store again.
func TestCachingStoreSizeMemo(t *testing.T) {
	inner := newRangeCountingStore()
	inner.put("idx-A", make([]byte, 4242))
	cs := newCachingStore(inner, 1<<20)

	for i := 0; i < 5; i++ {
		sz, err := cs.Size("idx-A")
		require.NoError(t, err)
		assert.Equal(t, int64(4242), sz)
	}
	assert.Equal(t, int64(1), inner.sizeCalls("idx-A"),
		"only the first Size should reach the inner store")
}

// TestCachingStoreSizeErrorNotCached: a Size error propagates and is NOT memoised,
// so a later Size after the object appears succeeds.
func TestCachingStoreSizeErrorNotCached(t *testing.T) {
	inner := newRangeCountingStore()
	// Register the key's call counter up front (put installs the counter) but
	// force Size to fail via sizeErr regardless of the object's presence.
	inner.put("k", make([]byte, 7))
	inner.mu.Lock()
	inner.sizeErr = errors.New("transient stat failure")
	inner.mu.Unlock()
	cs := newCachingStore(inner, 1<<20)

	_, err := cs.Size("k")
	require.Error(t, err)

	// Clear the induced error; the object (and its call counter) already exist, so
	// the count reflects both the failed and the recovered call.
	inner.mu.Lock()
	inner.sizeErr = nil
	inner.mu.Unlock()

	sz, err := cs.Size("k")
	require.NoError(t, err)
	assert.Equal(t, int64(7), sz)
	assert.Equal(t, int64(2), inner.sizeCalls("k"),
		"the failed Size must not have been memoised (both calls reached inner)")
}

// TestCachingStoreReadAtCacheHit: a repeated ReadAt of the same (key, off, len)
// hits the range cache on the second call, returns the correct bytes, and does not
// touch the inner store again.
func TestCachingStoreReadAtCacheHit(t *testing.T) {
	inner := newRangeCountingStore()
	payload := []byte("0123456789abcdef")
	inner.put("idx-A", payload)
	cs := newCachingStore(inner, 1<<20)

	for i := 0; i < 5; i++ {
		buf := make([]byte, 4)
		n, err := cs.ReadAt("idx-A", buf, 6)
		require.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "6789", string(buf))
	}
	assert.Equal(t, int64(1), inner.readAtCalls("idx-A"),
		"only the first ReadAt of a given range should reach the inner store")
}

// TestCachingStoreReadAtDistinctRanges: distinct (off, len) of the same key are
// cached independently — each range misses once, then hits.
func TestCachingStoreReadAtDistinctRanges(t *testing.T) {
	inner := newRangeCountingStore()
	inner.put("idx-A", []byte("0123456789abcdef"))
	cs := newCachingStore(inner, 1<<20)

	read := func(off int64, n int) string {
		buf := make([]byte, n)
		_, err := cs.ReadAt("idx-A", buf, off)
		require.NoError(t, err)
		return string(buf)
	}

	assert.Equal(t, "012", read(0, 3))
	assert.Equal(t, "abc", read(10, 3))
	// Repeat both — should be served from cache.
	assert.Equal(t, "012", read(0, 3))
	assert.Equal(t, "abc", read(10, 3))
	// Same offset, different length is a distinct range.
	assert.Equal(t, "0123", read(0, 4))

	assert.Equal(t, int64(3), inner.readAtCalls("idx-A"),
		"three distinct ranges => three inner ReadAts, repeats served from cache")
}

// TestCachingStoreReadAtSingleflightDedup: N concurrent ReadAts of the SAME range
// while the fetch is in flight must collapse to a single inner read.
func TestCachingStoreReadAtSingleflightDedup(t *testing.T) {
	inner := newRangeCountingStore()
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
			buf := make([]byte, 256)
			_, errs[i] = cs.ReadAt("big-index", buf, 128)
		}(i)
	}
	for i := 0; i < n; i++ {
		<-started
	}
	close(inner.block)
	wg.Wait()

	for i := range errs {
		require.NoError(t, errs[i])
	}
	assert.Equal(t, int64(1), inner.readAtCalls("big-index"),
		"all %d concurrent ReadAts of the same range must collapse to one inner read", n)
}

// TestCachingStoreReadAtErrorNotCached: a ReadAt error propagates and is NOT
// cached, so a later ReadAt after the object becomes available succeeds.
func TestCachingStoreReadAtErrorNotCached(t *testing.T) {
	inner := newRangeCountingStore()
	inner.readAtErr = errors.New("transient object store failure")
	cs := newCachingStore(inner, 1<<20)

	buf := make([]byte, 4)
	_, err := cs.ReadAt("k", buf, 0)
	require.Error(t, err)

	inner.mu.Lock()
	inner.readAtErr = nil
	inner.mu.Unlock()
	inner.put("k", []byte("abcd"))

	n, err := cs.ReadAt("k", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "abcd", string(buf))
}

// TestCachingStoreReadAtDoesNotAliasCallerBuffer: a cache hit must copy into the
// caller's buffer, and mutating that buffer afterwards must not corrupt the cached
// range for the next reader (the classic aliasing bug for ranged caches).
func TestCachingStoreReadAtDoesNotAliasCallerBuffer(t *testing.T) {
	inner := newRangeCountingStore()
	inner.put("idx-A", []byte("ABCDEFGH"))
	cs := newCachingStore(inner, 1<<20)

	buf1 := make([]byte, 4)
	_, err := cs.ReadAt("idx-A", buf1, 0)
	require.NoError(t, err)
	assert.Equal(t, "ABCD", string(buf1))

	// Mutate the caller's buffer in place after the read.
	for i := range buf1 {
		buf1[i] = 'x'
	}

	// A second reader of the same range must still get the original bytes.
	buf2 := make([]byte, 4)
	_, err = cs.ReadAt("idx-A", buf2, 0)
	require.NoError(t, err)
	assert.Equal(t, "ABCD", string(buf2),
		"mutating an earlier caller buffer must not corrupt the cached range")
	assert.Equal(t, int64(1), inner.readAtCalls("idx-A"),
		"the second read was still a cache hit")
}

// TestCachingStoreReadAtOversizeNotCached: a range larger than the whole budget is
// returned to the caller but never cached, so a repeat re-reads from the inner store.
func TestCachingStoreReadAtOversizeNotCached(t *testing.T) {
	inner := newRangeCountingStore()
	inner.put("huge", make([]byte, 1000))
	cs := newCachingStore(inner, 500)

	buf := make([]byte, 1000)
	_, err := cs.ReadAt("huge", buf, 0)
	require.NoError(t, err)
	_, err = cs.ReadAt("huge", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inner.readAtCalls("huge"),
		"a range larger than the budget must not be cached")
}

// TestCachingStoreDisabledPassesRangeReads: a non-positive budget returns the raw
// store unwrapped, so Size/ReadAt are byte-identical to before (no caching).
func TestCachingStoreDisabledPassesRangeReads(t *testing.T) {
	inner := newRangeCountingStore()
	inner.put("k", []byte("hello"))

	got := newCachingStore(inner, 0)
	require.Same(t, inner, got)
}

// TestRangeCacheByteBoundedEviction: the range LRU evicts by byte budget, not by
// count — adding a third 100-byte range under a 250-byte budget evicts the oldest.
func TestRangeCacheByteBoundedEviction(t *testing.T) {
	c := newRangeCache(250)
	ka := rangeKey{key: "a", off: 0, len: 100}
	kb := rangeKey{key: "b", off: 0, len: 100}
	kc := rangeKey{key: "c", off: 0, len: 100}
	c.add(ka, make([]byte, 100))
	c.add(kb, make([]byte, 100))
	// Touch a so it is more-recently-used than b.
	_, ok := c.get(ka)
	require.True(t, ok)
	c.add(kc, make([]byte, 100)) // 300 > 250 => evict oldest (b)

	_, ok = c.get(ka)
	assert.True(t, ok, "a should still be cached")
	_, ok = c.get(kb)
	assert.False(t, ok, "b should have been evicted")
	_, ok = c.get(kc)
	assert.True(t, ok, "c should be cached")
	assert.Equal(t, int64(200), c.curBytes)
}
