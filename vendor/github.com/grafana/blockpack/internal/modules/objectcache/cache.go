// Package objectcache provides a generic, concurrent in-memory cache with strong references.
// Entries are retained until explicitly cleared — the GC will not reclaim cached values.
// Callers must call Clear or manage cache lifetime to prevent unbounded growth.
// GOMEMLIMIT controls GC pacing but is NOT a hard memory cap.
package objectcache

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"errors"
	"sync"
)

// Cache is a generic in-memory cache backed by strong references.
// Entries persist across queries until explicitly cleared or the process exits.
// Memory is bounded by GOMEMLIMIT at the process level.
//
// Cache is safe for concurrent use. The zero value is ready to use.
// A Cache must not be copied after first use (it contains a sync.Map).
//
// SPEC-OC-001: Get returns nil on miss.
// SPEC-OC-002: Put stores a strong reference; entries are not GC'd while in the cache.
// SPEC-OC-005: Clear removes all entries.
// SPEC-OC-006: All methods are safe for concurrent use.
type Cache[V any] struct {
	m sync.Map // key: string → *V
}

// Get returns the cached *V for key, or nil if the entry is absent.
func (c *Cache[V]) Get(key string) *V {
	val, ok := c.m.Load(key)
	if !ok {
		return nil
	}
	return val.(*V)
}

// Put stores a strong reference to v under key.
// Returns an error if v is nil.
func (c *Cache[V]) Put(key string, v *V) error {
	if v == nil {
		return errors.New("objectcache: Put called with nil pointer")
	}
	c.m.Store(key, v)
	return nil
}

// Clear removes all entries from the cache. Intended for testing.
func (c *Cache[V]) Clear() {
	c.m.Range(func(k, _ any) bool {
		c.m.Delete(k)
		return true
	})
}
