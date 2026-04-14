package memorycache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_InvalidMaxBytes(t *testing.T) {
	_, err := New(Config{MaxBytes: 0})
	require.Error(t, err)

	_, err = New(Config{MaxBytes: -1})
	require.Error(t, err)
}

func TestGetMiss(t *testing.T) {
	c, err := New(Config{MaxBytes: 1024})
	require.NoError(t, err)

	val, ok, err := c.Get("missing")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestPutGet(t *testing.T) {
	c, err := New(Config{MaxBytes: 1024})
	require.NoError(t, err)

	require.NoError(t, c.Put("k", []byte("hello")))

	val, ok, err := c.Get("k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("hello"), val)
}

func TestGetReturnsCopy(t *testing.T) {
	c, err := New(Config{MaxBytes: 1024})
	require.NoError(t, err)

	require.NoError(t, c.Put("k", []byte("hello")))

	val, _, _ := c.Get("k")
	val[0] = 'X' // modify the returned slice

	val2, _, _ := c.Get("k")
	assert.Equal(t, []byte("hello"), val2, "cache data must not be modified by caller")
}

func TestEviction(t *testing.T) {
	// capacity for exactly two 5-byte entries
	c, err := New(Config{MaxBytes: 10})
	require.NoError(t, err)

	require.NoError(t, c.Put("a", []byte("aaaaa")))
	require.NoError(t, c.Put("b", []byte("bbbbb")))

	// "a" should be evicted when "c" is inserted (LRU = "a")
	require.NoError(t, c.Put("c", []byte("ccccc")))

	_, okA, _ := c.Get("a")
	_, okB, _ := c.Get("b")
	_, okC, _ := c.Get("c")

	assert.False(t, okA, "oldest entry 'a' should have been evicted")
	assert.True(t, okB)
	assert.True(t, okC)
}

func TestLRUPromotion(t *testing.T) {
	// capacity for exactly two 5-byte entries
	c, err := New(Config{MaxBytes: 10})
	require.NoError(t, err)

	require.NoError(t, c.Put("a", []byte("aaaaa")))
	require.NoError(t, c.Put("b", []byte("bbbbb")))

	// access "a" to promote it to MRU
	_, _, _ = c.Get("a")

	// "b" is now LRU; inserting "c" should evict "b"
	require.NoError(t, c.Put("c", []byte("ccccc")))

	_, okA, _ := c.Get("a")
	_, okB, _ := c.Get("b")
	assert.True(t, okA, "'a' should survive (was promoted)")
	assert.False(t, okB, "'b' should be evicted (LRU)")
}

func TestGetOrFetch_Hit(t *testing.T) {
	c, err := New(Config{MaxBytes: 1024})
	require.NoError(t, err)

	require.NoError(t, c.Put("k", []byte("cached")))

	calls := 0
	val, err := c.GetOrFetch("k", func() ([]byte, error) {
		calls++
		return []byte("fetched"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("cached"), val)
	assert.Equal(t, 0, calls, "fetch must not be called on hit")
}

func TestGetOrFetch_Miss(t *testing.T) {
	c, err := New(Config{MaxBytes: 1024})
	require.NoError(t, err)

	val, err := c.GetOrFetch("k", func() ([]byte, error) {
		return []byte("fetched"), nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("fetched"), val)

	// result is now cached
	val2, ok, _ := c.Get("k")
	assert.True(t, ok)
	assert.Equal(t, []byte("fetched"), val2)
}

func TestGetOrFetch_Concurrent(t *testing.T) {
	c, err := New(Config{MaxBytes: 1 << 20})
	require.NoError(t, err)

	var calls int
	var mu sync.Mutex

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ferr := c.GetOrFetch("k", func() ([]byte, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				return []byte("value"), nil
			})
			assert.NoError(t, ferr)
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, calls, 2, "singleflight should deduplicate concurrent fetches")
}

func TestNilReceiver(t *testing.T) {
	var c *MemoryCache

	_, ok, err := c.Get("k")
	assert.NoError(t, err)
	assert.False(t, ok)

	assert.NoError(t, c.Put("k", []byte("v")))

	val, err := c.GetOrFetch("k", func() ([]byte, error) { return []byte("v"), nil })
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), val)

	assert.NoError(t, c.Close())
}

func TestOversizedEntryDropped(t *testing.T) {
	c, err := New(Config{MaxBytes: 4})
	require.NoError(t, err)

	// 5 bytes > 4-byte capacity: should be silently dropped
	require.NoError(t, c.Put("big", []byte("hello")))

	_, ok, _ := c.Get("big")
	assert.False(t, ok, "oversized entry should be dropped")
}
