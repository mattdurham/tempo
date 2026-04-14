package memcache

import (
	"errors"
	"testing"

	gomemcache "github.com/grafana/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClient is an in-memory mock of the memcache client for testing.
type fakeClient struct {
	store map[string]*gomemcache.Item
	err   error // if non-nil, all calls return this error
}

func newFakeClient() *fakeClient {
	return &fakeClient{store: make(map[string]*gomemcache.Item)}
}

func (f *fakeClient) Get(key string, _ ...gomemcache.Option) (*gomemcache.Item, error) {
	if f.err != nil {
		return nil, f.err
	}
	item, ok := f.store[key]
	if !ok {
		return nil, gomemcache.ErrCacheMiss
	}
	return item, nil
}

func (f *fakeClient) Set(item *gomemcache.Item) error {
	if f.err != nil {
		return f.err
	}
	cp := make([]byte, len(item.Value))
	copy(cp, item.Value)
	f.store[item.Key] = &gomemcache.Item{Key: item.Key, Value: cp, Expiration: item.Expiration}
	return nil
}

func (f *fakeClient) Close() {}

func newTestMemCache(c client) *MemCache {
	return &MemCache{c: c, expiration: 0}
}

func TestGetMiss(t *testing.T) {
	m := newTestMemCache(newFakeClient())

	val, ok, err := m.Get("missing")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestPutGet(t *testing.T) {
	m := newTestMemCache(newFakeClient())

	require.NoError(t, m.Put("k", []byte("hello")))

	val, ok, err := m.Get("k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("hello"), val)
}

func TestGetReturnsCopy(t *testing.T) {
	m := newTestMemCache(newFakeClient())
	require.NoError(t, m.Put("k", []byte("hello")))

	val, _, _ := m.Get("k")
	val[0] = 'X'

	val2, _, _ := m.Get("k")
	assert.Equal(t, []byte("hello"), val2, "cache data must not be modified by caller")
}

func TestGetOrFetch_Hit(t *testing.T) {
	m := newTestMemCache(newFakeClient())
	require.NoError(t, m.Put("k", []byte("cached")))

	calls := 0
	val, err := m.GetOrFetch("k", func() ([]byte, error) {
		calls++
		return []byte("fetched"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("cached"), val)
	assert.Equal(t, 0, calls)
}

func TestGetOrFetch_Miss(t *testing.T) {
	m := newTestMemCache(newFakeClient())

	val, err := m.GetOrFetch("k", func() ([]byte, error) {
		return []byte("fetched"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("fetched"), val)

	// should now be cached
	val2, ok, _ := m.Get("k")
	assert.True(t, ok)
	assert.Equal(t, []byte("fetched"), val2)
}

func TestTransientErrorTreatedAsMiss(t *testing.T) {
	fc := newFakeClient()
	fc.err = errors.New("connection refused")
	m := newTestMemCache(fc)

	val, ok, err := m.Get("k")
	require.NoError(t, err, "transient errors must be treated as cache misses")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestNilReceiver(t *testing.T) {
	var m *MemCache

	_, ok, err := m.Get("k")
	assert.NoError(t, err)
	assert.False(t, ok)

	assert.NoError(t, m.Put("k", []byte("v")))

	val, err := m.GetOrFetch("k", func() ([]byte, error) { return []byte("v"), nil })
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), val)

	assert.NoError(t, m.Close())
}

func TestOpen_Disabled(t *testing.T) {
	m, err := Open(Config{Enabled: false})
	require.NoError(t, err)
	assert.Nil(t, m)
}

func TestOpen_MissingServers(t *testing.T) {
	_, err := Open(Config{Enabled: true, Servers: nil})
	require.Error(t, err)
}

func TestMemcacheKey_AlwaysValid(t *testing.T) {
	cases := []string{
		"",
		"short",
		"/very/long/s3/path/that/exceeds/250/chars/" + string(make([]byte, 300)),
		"key with spaces",
		"key\twith\ttabs",
	}
	for _, raw := range cases {
		k := memcacheKey(raw)
		assert.Len(t, k, 64, "sha256 hex must always be 64 chars")
		for _, b := range []byte(k) {
			assert.True(t, b > ' ' && b != 0x7f, "key must contain only printable non-space ASCII")
		}
	}
}
