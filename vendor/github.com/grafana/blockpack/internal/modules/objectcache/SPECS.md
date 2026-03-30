# objectcache Module — Specifications

## SPEC-OC-001: Get — nil on miss
*Added: 2026-03-23*
*Updated: 2026-03-29*

`Get(key string) *V` returns nil when no entry has ever been stored under `key`,
or when a previous entry was removed via `Clear`. There is no GC-cooperative
eviction — entries are held by strong reference until explicitly removed.

Callers must treat nil as a cache miss and recompute/re-fetch the value.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Get`

---

## SPEC-OC-002: Put — stores strong reference
*Added: 2026-03-23*
*Updated: 2026-03-29*

`Put(key string, v *V)` stores a strong `*V` reference in the underlying
`sync.Map`. Entries are retained for the lifetime of the process (or until
`Clear` is called) — the GC will not reclaim cached values. For immutable data
(parsed metadata, sketches), a second Put for the same key is benign — it
overwrites with an equivalent value.

If `v` is nil, `Put` returns an error and no entry is stored.

Memory is bounded at the process level by `GOMEMLIMIT`.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Put`

---

## SPEC-OC-003: Memory bound via GOMEMLIMIT
*Added: 2026-03-23*
*Updated: 2026-03-29*

Entries are NOT GC'd while in the cache. The cache grows without explicit
eviction. Memory is bounded at the process level by `GOMEMLIMIT` — the Go
runtime soft-memory limit triggers GC pressure before OOM. Operators must set
`GOMEMLIMIT` appropriately for their deployment.

No per-entry eviction policy is implemented. Cached values (parsed file
metadata, sketch indexes, decoded intrinsic columns) are immutable and
process-scoped; they remain valid and useful for the entire process lifetime.

---

## SPEC-OC-004: No stale-key cleanup required
*Added: 2026-03-23*
*Updated: 2026-03-29*

Because entries are held by strong reference, there are no GC'd (nil) weak
pointers to clean up. The map never accumulates dead entries. No lazy deletion
on `Get` is performed.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Get`

---

## SPEC-OC-005: Clear removes all entries
*Added: 2026-03-23*

`Clear()` removes all entries via Range+Delete. Intended for test teardown
(mirrors the contract of `reader.ClearCaches()`).

Back-ref: `internal/modules/objectcache/cache.go:Cache.Clear`

---

## SPEC-OC-006: Thread safety
*Added: 2026-03-23*

All methods (`Get`, `Put`, `Clear`) are safe for concurrent use. The underlying
`sync.Map` provides the necessary synchronization.
