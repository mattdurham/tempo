package objectcache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type internalTestValue struct{ n int }

// TestCache_StrongRefInternalMap verifies that entries are stored and retrievable.
func TestCache_StrongRefInternalMap(t *testing.T) {
	var c Cache[internalTestValue]

	require.NoError(t, c.Put("key", &internalTestValue{n: 1}))

	val := c.Get("key")
	assert.NotNil(t, val)
	assert.Equal(t, 1, val.n)
}

// TestCache_LRUEviction verifies entries are evicted when byte budget is exceeded.
func TestCache_LRUEviction(t *testing.T) {
	var c Cache[internalTestValue]
	c.SetMaxBytes(3 << 20) // 3 MB budget, each entry defaults to 1 MB

	require.NoError(t, c.Put("a", &internalTestValue{n: 1}))
	require.NoError(t, c.Put("b", &internalTestValue{n: 2}))
	require.NoError(t, c.Put("c", &internalTestValue{n: 3}))
	assert.Equal(t, 3, c.Len())

	// Adding a 4th should evict the LRU entry ("a").
	require.NoError(t, c.Put("d", &internalTestValue{n: 4}))
	assert.Equal(t, 3, c.Len())
	assert.Nil(t, c.Get("a"), "LRU entry 'a' should be evicted")
	assert.NotNil(t, c.Get("d"))
}

// TestCache_DefaultBudgetNeverZero verifies that a zero-value Cache always has a
// finite byte budget after ensureInit — never unbounded (maxBytes==0).
// NOTE-OC-004: fallback to defaultMaxBytesNoGOMEMLIMIT prevents OOM when GOMEMLIMIT unset.
func TestCache_DefaultBudgetNeverZero(t *testing.T) {
	var c Cache[internalTestValue]
	c.ensureInit() // trigger budget computation
	assert.Greater(t, c.maxBytes, int64(0), "budget must always be finite after init")
}

// TestCache_GetPromotesLRU verifies that Get promotes an entry so it isn't evicted.
func TestCache_GetPromotesLRU(t *testing.T) {
	var c Cache[internalTestValue]
	c.SetMaxBytes(3 << 20) // 3 MB

	require.NoError(t, c.Put("a", &internalTestValue{n: 1}))
	require.NoError(t, c.Put("b", &internalTestValue{n: 2}))
	require.NoError(t, c.Put("c", &internalTestValue{n: 3}))

	// Touch "a" to promote it.
	c.Get("a")

	// Adding "d" should evict "b" (now the LRU), not "a".
	require.NoError(t, c.Put("d", &internalTestValue{n: 4}))
	assert.NotNil(t, c.Get("a"), "'a' was promoted and should survive")
	assert.Nil(t, c.Get("b"), "'b' should be evicted as LRU")
}
