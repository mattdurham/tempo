# objectcache Module — Specifications

## SPEC-OC-001: Get — nil on miss or GC eviction
*Added: 2026-03-23*

`Get(key string) *V` returns nil in two cases:
1. No entry has ever been stored under `key`.
2. An entry was stored but the GC reclaimed it (no strong references remained).

Callers must treat nil as a cache miss and recompute/re-fetch the value.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Get`

---

## SPEC-OC-002: Put — stores weak reference; caller retains ownership
*Added: 2026-03-23*

`Put(key string, v *V)` stores `weak.Make(v)`. The entry is only guaranteed
retrievable as long as at least one strong pointer to `*V` is held elsewhere.
For immutable data (parsed metadata, sketches), a second Put for the same key
is benign — it overwrites with an equivalent value.

If `v` is nil, `Put` returns an error and no entry is stored.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Put`

---

## SPEC-OC-003: GC-cooperative eviction
*Added: 2026-03-23*

The GC may reclaim any value in the cache when no strong references remain.
This is the primary design goal: the cache does not prevent GC of stale file
metadata in long-running processes.

---

## SPEC-OC-004: Lazy stale-key deletion
*Added: 2026-03-23*

When `Get` detects that the weak pointer's `.Value()` is nil (GC'd), it calls
`c.m.Delete(key)` before returning nil. This prevents unbounded map key
accumulation in processes that open and close many distinct files over time.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Get`

---

## SPEC-OC-005: Clear — removes all entries
*Added: 2026-03-23*

`Clear()` removes all entries via Range+Delete. Intended for test teardown
(mirrors the contract of `reader.ClearCaches()`).

Back-ref: `internal/modules/objectcache/cache.go:Cache.Clear`

---

## SPEC-OC-006: Thread safety
*Added: 2026-03-23*

All methods (`Get`, `Put`, `Clear`) are safe for concurrent use. The underlying
`sync.Map` provides the necessary synchronization.
