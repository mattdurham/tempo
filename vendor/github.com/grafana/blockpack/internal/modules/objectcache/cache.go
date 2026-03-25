// Package objectcache provides a generic, GC-cooperative in-memory cache
// backed by weak.Pointer, allowing entries to be reclaimed by the GC when
// no strong references remain.
package objectcache

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"errors"
	"sync"
	"weak"
)

// Cache is a generic, GC-cooperative in-memory cache backed by weak.Pointer[V].
// Entries are held by weak references: the GC may reclaim a value when no other
// strong references exist. Get returns nil for absent or reclaimed entries.
// Stale map keys (whose values have been GC'd) are deleted lazily on Get to
// prevent unbounded key accumulation in long-running processes.
//
// Cache is safe for concurrent use. The zero value is ready to use.
// A Cache must not be copied after first use (it contains a sync.Map).
//
// SPEC-OC-001: Get returns nil on miss or GC eviction.
// SPEC-OC-002: Put stores a weak reference; the caller must retain a strong ref.
// SPEC-OC-003: GC may reclaim entries when no strong references remain.
// SPEC-OC-004: Stale keys are deleted lazily on Get.
// SPEC-OC-005: Clear removes all entries.
// SPEC-OC-006: All methods are safe for concurrent use.
type Cache[V any] struct {
	m sync.Map // key: string → weak.Pointer[V]
}

// Get returns the cached *V for key, or nil if the entry is absent or has been
// GC'd. If the entry was GC'd, its stale map key is deleted.
func (c *Cache[V]) Get(key string) *V {
	val, ok := c.m.Load(key)
	if !ok {
		return nil
	}
	wp := val.(weak.Pointer[V])
	v := wp.Value()
	if v == nil {
		c.m.Delete(key) // SPEC-OC-004: clean stale key
	}
	return v
}

// Put stores a weak reference to v under key.
// Returns an error if v is nil. If a previous entry exists under key, it is overwritten.
//
// SPEC-OC-002: nil v returns an error — callers must check.
func (c *Cache[V]) Put(key string, v *V) error {
	if v == nil {
		return errors.New("objectcache: Put called with nil pointer")
	}
	c.m.Store(key, weak.Make(v))
	return nil
}

// Clear removes all entries from the cache. Intended for testing.
// Uses Range+Delete to avoid unsafe replacement of a sync.Map that may be
// in concurrent use.
func (c *Cache[V]) Clear() {
	c.m.Range(func(k, _ any) bool {
		c.m.Delete(k)
		return true
	})
}
