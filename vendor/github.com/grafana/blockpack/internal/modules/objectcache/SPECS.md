# objectcache Module — Specifications

## SPEC-OC-001: Get — nil on miss
*Added: 2026-03-23*
*Updated: 2026-04-14*

`Get(key string) *V` returns nil when no entry has been stored under `key`,
or after an entry was removed by LRU eviction or `Clear`.

Callers must treat nil as a cache miss and recompute/re-fetch the value.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Get`

---

## SPEC-OC-002: Put — LRU eviction, strong references
*Added: 2026-03-23*
*Updated: 2026-04-14*

`Put(key string, v *V)` stores a strong `*V` reference. Entries are held by
strong reference and are NOT reclaimed by the GC; they persist until evicted
by LRU pressure or removed by `Clear`.

After storing the entry, the eviction loop runs until `curBytes <= maxBytes`,
removing the least-recently-used entry on each iteration.

If `v` is nil, `Put` returns an error and no entry is stored.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Put`

---

## SPEC-OC-003: Byte budget — always bounded
*Added: 2026-03-23*
*Updated: 2026-04-14*

Every `Cache` has a finite byte budget. Priority order:

1. **`SetMaxBytes` override** — if called before first `Put`, that value is used.
2. **GOMEMLIMIT fraction** — if GOMEMLIMIT env var is set, budget = 20% of GOMEMLIMIT.
3. **Hard fallback** — if GOMEMLIMIT is not set, budget = 256 MiB.

The cache is never unbounded. Deployers may set GOMEMLIMIT to tune the budget
for their environment; the 256 MiB fallback prevents OOM in deployments that
omit GOMEMLIMIT.

Back-ref: `internal/modules/objectcache/cache.go:Cache.ensureInit`

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

`Clear()` removes all entries and resets all state, including the `inited` flag
so the byte budget is re-read from GOMEMLIMIT on next use.

Back-ref: `internal/modules/objectcache/cache.go:Cache.Clear`

---

## SPEC-OC-006: Thread safety
*Added: 2026-03-23*

All methods (`Get`, `Put`, `Clear`, `Len`, `SetMaxBytes`) are safe for concurrent
use via a single `sync.Mutex`.

Back-ref: `internal/modules/objectcache/cache.go:Cache`

---

## SPEC-OC-007: Byte budget default and override
*Added: 2026-04-14*

The default byte budget is 20% of GOMEMLIMIT when set, or 256 MiB otherwise.
`SetMaxBytes(n)` overrides the budget; it must be called before the first `Put`.
Passing 0 to `SetMaxBytes` reverts to the default computation on next `Put`.

Back-ref: `internal/modules/objectcache/cache.go:Cache.SetMaxBytes`
