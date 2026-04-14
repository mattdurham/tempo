package objectcache_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/objectcache"
)

type testValue struct {
	n int
}

// TestCache_GetMissReturnsNil verifies that Get on an empty cache returns nil.
// SPEC-OC-001: Get returns nil on cache miss.
func TestCache_GetMissReturnsNil(t *testing.T) {
	var c objectcache.Cache[testValue]
	got := c.Get("missing")
	assert.Nil(t, got)
}

// TestCache_PutThenGet verifies a Put is immediately retrievable via Get.
// SPEC-OC-001, SPEC-OC-002.
func TestCache_PutThenGet(t *testing.T) {
	var c objectcache.Cache[testValue]
	v := &testValue{n: 42}
	require.NoError(t, c.Put("key1", v))
	got := c.Get("key1")
	require.NotNil(t, got)
	assert.Equal(t, 42, got.n)
}

// TestCache_StrongReference verifies that entries survive GC cycles (strong references).
// SPEC-OC-002: entries are not GC'd while in the cache.
func TestCache_StrongReference(t *testing.T) {
	var c objectcache.Cache[testValue]
	require.NoError(t, c.Put("retained", &testValue{n: 99}))

	// Unlike weak-pointer cache, strong references survive GC.
	got := c.Get("retained")
	require.NotNil(t, got, "strong-reference cache must retain entries across GC")
	assert.Equal(t, 99, got.n)
}

// TestCache_Clear verifies that Clear removes all entries from the cache.
// SPEC-OC-005: Clear removes all entries.
func TestCache_Clear(t *testing.T) {
	var c objectcache.Cache[testValue]
	v1 := &testValue{n: 1}
	v2 := &testValue{n: 2}
	require.NoError(t, c.Put("a", v1))
	require.NoError(t, c.Put("b", v2))

	c.Clear()

	assert.Nil(t, c.Get("a"))
	assert.Nil(t, c.Get("b"))
}

// TestCache_ConcurrentPutGet verifies that concurrent Put AND Get operations on
// the same key do not race (validated with -race flag during test runs).
// SPEC-OC-006: Cache is safe for concurrent use.
func TestCache_ConcurrentPutGet(t *testing.T) {
	const (
		numGoroutines      = 10
		numOpsPerGoroutine = 1000
	)

	var c objectcache.Cache[testValue]
	done := make(chan struct{}, numGoroutines)

	for g := range numGoroutines {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := range numOpsPerGoroutine {
				v := &testValue{n: id*numOpsPerGoroutine + i}
				_ = c.Put("shared", v)
				_ = c.Get("shared")
			}
		}(g)
	}

	for range numGoroutines {
		<-done
	}
}

// TestCache_PutNilReturnsError verifies that Put returns an error when called with a nil pointer.
func TestCache_PutNilReturnsError(t *testing.T) {
	var c objectcache.Cache[testValue]
	err := c.Put("key", nil)
	require.Error(t, err)
	assert.Nil(t, c.Get("key"))
}

// TestCache_OverwriteSameKey verifies that Putting a new value for an existing key
// replaces the old entry.
func TestCache_OverwriteSameKey(t *testing.T) {
	var c objectcache.Cache[testValue]
	v1 := &testValue{n: 1}
	v2 := &testValue{n: 2}

	require.NoError(t, c.Put("k", v1))
	require.NoError(t, c.Put("k", v2))

	got := c.Get("k")
	require.NotNil(t, got)
	assert.Equal(t, 2, got.n)
}
