# memorycache — Specifications

This document defines the public contracts and invariants for the `memorycache` package.

---

## SPEC-MEM-001: MemoryCache Interface Compliance

`MemoryCache` implements `filecache.Cache` and is intended as the fastest (in-process)
tier in a multi-level cache chain (memorycache → filecache → memcache).

Back-ref: `internal/modules/memorycache/memorycache.go:MemoryCache`

---

## SPEC-MEM-002: LRU Eviction

Eviction is LRU: when total cached bytes would exceed `MaxBytes`, the least-recently-used
entry is removed first. Entries are moved to the MRU end of the list on each `Get`.

Back-ref: `internal/modules/memorycache/memorycache.go:MemoryCache`

---

## SPEC-MEM-003: Byte Budget Enforcement

`Config.MaxBytes` must be positive. `New` returns an error if `MaxBytes <= 0`.
The total cached bytes never exceeds `MaxBytes`; entries are evicted before a new Put
would breach this limit.

Back-ref: `internal/modules/memorycache/memorycache.go:New`

---

## SPEC-MEM-004: Nil Receiver Safety

A nil `*MemoryCache` is safe to use: all operations become pass-throughs.
`Get` returns `(nil, false, nil)`, `Put` is a no-op, `GetOrFetch` delegates to the
fetch function, `Close` is a no-op.

Back-ref: `internal/modules/memorycache/memorycache.go:MemoryCache`

---

## SPEC-MEM-005: Concurrent Fetch Deduplication

Concurrent calls to `GetOrFetch` for the same uncached key share a single fetch
invocation via `singleflight.Group`. Only one fetch is issued per key regardless of
concurrent callers.

Back-ref: `internal/modules/memorycache/memorycache.go:GetOrFetch`
