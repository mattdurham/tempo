package objectcache

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type internalTestValue struct{ n int }

//go:noinline
func putInternal(c *Cache[internalTestValue], key string, v *internalTestValue) error {
	return c.Put(key, v)
}

// TestCache_StaleKeyActuallyDeleted verifies that a GC-evicted entry's map key is
// removed from the underlying sync.Map by Get, not merely skipped.
// SPEC-OC-004: stale keys are deleted lazily on Get.
func TestCache_StaleKeyActuallyDeleted(t *testing.T) {
	var c Cache[internalTestValue]

	require.NoError(t, putInternal(&c, "gone", &internalTestValue{n: 1}))

	runtime.GC()
	runtime.GC()

	// Before Get: key should still be in the map (not yet cleaned).
	_, existsBefore := c.m.Load("gone")
	assert.True(t, existsBefore, "stale key should exist in map before Get is called")

	// Get triggers lazy deletion.
	got := c.Get("gone")
	assert.Nil(t, got)

	// After Get: key must be absent from the map.
	_, existsAfter := c.m.Load("gone")
	assert.False(t, existsAfter, "stale key must be deleted from map after Get returns nil")
}
