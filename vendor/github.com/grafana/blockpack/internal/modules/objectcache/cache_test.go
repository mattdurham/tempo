package objectcache_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/objectcache"
)

type testValue struct {
	n int
}

// putValue is a //go:noinline helper used by GC eviction tests.
// Extracting the Put call into a noinline function guarantees the compiler
// cannot keep the strong reference live past the call site, so GC can
// collect the value in the caller after putValue returns.
//
//go:noinline
func putValue(t *testing.T, c *objectcache.Cache[testValue], key string, val *testValue) {
	t.Helper()
	require.NoError(t, c.Put(key, val))
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
	runtime.KeepAlive(v) // prevent GC from collecting v before Get returns
}

// TestCache_GCEviction verifies that after all strong references to a value are dropped
// and GC runs, Get returns nil (the weak pointer was reclaimed).
// SPEC-OC-003: GC-cooperative — entries are reclaimed when no strong refs remain.
func TestCache_GCEviction(t *testing.T) {
	var c objectcache.Cache[testValue]

	// Use a //go:noinline helper so the compiler cannot keep the strong reference
	// live past the call site. The only strong ref is gone when putValue returns.
	putValue(t, &c, "evictable", &testValue{n: 99})

	// Force GC cycles; the weak pointer should now report nil.
	runtime.GC()
	runtime.GC() // second GC to reduce nondeterminism in when weak pointers are cleared

	got := c.Get("evictable")
	assert.Nil(t, got, "value should be GC'd when no strong refs remain")
}

// TestCache_StaleKeyCleanedOnGet verifies that after GC evicts an entry,
// a subsequent Get deletes the stale map key (prevents unbounded key growth).
// SPEC-OC-004: stale keys are deleted lazily on Get.
func TestCache_StaleKeyCleanedOnGet(t *testing.T) {
	var c objectcache.Cache[testValue]

	// Use //go:noinline helper so the strong ref is guaranteed off the stack.
	putValue(t, &c, "stale", &testValue{n: 7})
	runtime.GC()
	runtime.GC()

	// Get should return nil AND delete the stale key.
	got := c.Get("stale")
	assert.Nil(t, got)

	// A second Put + Get cycle on the same key should work correctly.
	v2 := &testValue{n: 8}
	require.NoError(t, c.Put("stale", v2))
	got2 := c.Get("stale")
	require.NotNil(t, got2)
	assert.Equal(t, 8, got2.n)
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

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < numOpsPerGoroutine; i++ {
				v := &testValue{n: id*numOpsPerGoroutine + i}
				_ = c.Put("shared", v) // error only on nil — never nil here
				got := c.Get("shared")
				// got may be nil if GC ran — acceptable per SPEC-OC-001/SPEC-OC-003
				_ = got
			}
		}(g)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// TestCache_PutNilReturnsError verifies that Put returns an error when called with a nil pointer.
// SPEC-OC-002: nil v returns an error.
func TestCache_PutNilReturnsError(t *testing.T) {
	var c objectcache.Cache[testValue]
	err := c.Put("key", nil)
	require.Error(t, err)
	assert.Nil(t, c.Get("key"))
}

// TestCache_OverwriteSameKey verifies that Putting a new value for an existing key
// replaces the old entry.
// SPEC-OC-002: Put is idempotent for immutable data — second Put wins.
func TestCache_OverwriteSameKey(t *testing.T) {
	var c objectcache.Cache[testValue]
	v1 := &testValue{n: 1}
	v2 := &testValue{n: 2}

	require.NoError(t, c.Put("k", v1))
	require.NoError(t, c.Put("k", v2)) // overwrite

	got := c.Get("k")
	require.NotNil(t, got)
	assert.Equal(t, 2, got.n)
	runtime.KeepAlive(v2) // prevent GC from collecting v2 before Get returns
}
