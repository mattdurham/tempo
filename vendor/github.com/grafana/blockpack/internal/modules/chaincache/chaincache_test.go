package chaincache

import (
	"errors"
	"testing"

	"github.com/grafana/blockpack/internal/modules/filecache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapCache is a simple in-memory cache for testing.
type mapCache struct {
	data map[string][]byte
	gets []string // keys requested via Get
	puts []string // keys stored via Put
}

func newMapCache() *mapCache {
	return &mapCache{data: make(map[string][]byte)}
}

func (m *mapCache) Get(key string) ([]byte, bool, error) {
	m.gets = append(m.gets, key)
	v, ok := m.data[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (m *mapCache) Put(key string, value []byte) error {
	m.puts = append(m.puts, key)
	cp := make([]byte, len(value))
	copy(cp, value)
	m.data[key] = cp
	return nil
}

func (m *mapCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if v, ok, err := m.Get(key); err != nil {
		return nil, err
	} else if ok {
		return v, nil
	}
	v, err := fetch()
	if err != nil {
		return nil, err
	}
	_ = m.Put(key, v)
	return v, nil
}

func (m *mapCache) Close() error { return nil }

var _ filecache.Cache = (*mapCache)(nil)

// errCache always returns the configured error from Get.
type errCache struct{ err error }

func (e *errCache) Get(_ string) ([]byte, bool, error)                            { return nil, false, e.err }
func (e *errCache) Put(_ string, _ []byte) error                                  { return e.err }
func (e *errCache) GetOrFetch(_ string, _ func() ([]byte, error)) ([]byte, error) { return nil, e.err }
func (e *errCache) Close() error                                                  { return nil }

func TestGet_HitInFirstTier(t *testing.T) {
	t1 := newMapCache()
	t2 := newMapCache()
	t1.data["k"] = []byte("v")

	c := New(t1, t2)
	val, ok, err := c.Get("k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v"), val)
	// t2 should never be queried when t1 hits
	assert.Empty(t, t2.gets, "second tier should not be queried when first tier hits")
}

func TestGet_HitInSecondTier_WritesBackToFirst(t *testing.T) {
	t1 := newMapCache()
	t2 := newMapCache()
	t2.data["k"] = []byte("v")

	c := New(t1, t2)
	val, ok, err := c.Get("k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v"), val)

	// t1 should now have the value written back
	_, ok1, _ := t1.Get("k")
	assert.True(t, ok1, "write-back to tier 1 on tier 2 hit")
}

func TestGet_MissAllTiers(t *testing.T) {
	t1 := newMapCache()
	t2 := newMapCache()

	c := New(t1, t2)
	val, ok, err := c.Get("k")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestGetOrFetch_FetchesOnMiss(t *testing.T) {
	t1 := newMapCache()
	t2 := newMapCache()

	c := New(t1, t2)
	calls := 0
	val, err := c.GetOrFetch("k", func() ([]byte, error) {
		calls++
		return []byte("fetched"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("fetched"), val)
	assert.Equal(t, 1, calls)

	// both tiers should now hold the value
	_, ok1, _ := t1.Get("k")
	_, ok2, _ := t2.Get("k")
	assert.True(t, ok1, "tier 1 should hold fetched value")
	assert.True(t, ok2, "tier 2 should hold fetched value")
}

func TestGetOrFetch_NoFetchOnHit(t *testing.T) {
	t1 := newMapCache()
	t1.data["k"] = []byte("cached")

	c := New(t1)
	calls := 0
	val, err := c.GetOrFetch("k", func() ([]byte, error) {
		calls++
		return nil, nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("cached"), val)
	assert.Equal(t, 0, calls)
}

func TestPut_WritesToAllTiers(t *testing.T) {
	t1 := newMapCache()
	t2 := newMapCache()
	t3 := newMapCache()

	c := New(t1, t2, t3)
	require.NoError(t, c.Put("k", []byte("v")))

	for i, tier := range []*mapCache{t1, t2, t3} {
		_, ok, _ := tier.Get("k")
		assert.True(t, ok, "tier %d should hold the value", i+1)
	}
}

func TestClose_ClosesAllTiers(t *testing.T) {
	closed := make([]bool, 3)
	tiers := make([]filecache.Cache, 3)
	for i := range tiers {
		idx := i
		tiers[i] = &closingCache{onClose: func() { closed[idx] = true }}
	}

	c := New(tiers...)
	require.NoError(t, c.Close())

	for i, ok := range closed {
		assert.True(t, ok, "tier %d Close was not called", i)
	}
}

type closingCache struct {
	onClose func()
}

func (cc *closingCache) Get(_ string) ([]byte, bool, error)                            { return nil, false, nil }
func (cc *closingCache) Put(_ string, _ []byte) error                                  { return nil }
func (cc *closingCache) GetOrFetch(_ string, f func() ([]byte, error)) ([]byte, error) { return f() }
func (cc *closingCache) Close() error                                                  { cc.onClose(); return nil }

func TestGet_PropagatesError(t *testing.T) {
	boom := &errCache{err: errors.New("boom")}
	c := New(boom)

	_, _, err := c.Get("k")
	assert.Error(t, err)
}

func TestEmptyChain(t *testing.T) {
	c := New()
	val, ok, err := c.Get("k")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val)

	require.NoError(t, c.Put("k", []byte("v")))
	require.NoError(t, c.Close())
}

func TestNilTiersFilteredInNew(t *testing.T) {
	t1 := newMapCache()
	// Pass two nils alongside a real tier — they should be silently dropped.
	c := New(nil, t1, nil)
	require.NoError(t, c.Put("k", []byte("v")))
	_, ok, _ := t1.Get("k")
	assert.True(t, ok, "real tier should receive Put despite nil siblings")
}

func TestNilReceiverClose(t *testing.T) {
	var c *ChainedCache
	assert.NoError(t, c.Close(), "Close on nil *ChainedCache must not panic")
}
