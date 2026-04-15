# chaincache — Specifications

This document defines the public contracts and invariants for the `chaincache` package.

---

## SPEC-CC-001: ChainedCache Multi-Tier Delegation

**ChainedCache** delegates cache operations through an ordered list of `filecache.Cache` tiers
(fastest-first). On a cache hit at tier N, the value is written back to all tiers 0..N-1.
On a cache miss across all tiers, the fetch function is called and the result is stored in
every tier.

Back-ref: `internal/modules/chaincache/chaincache.go:ChainedCache`

---

## SPEC-CC-002: Nil Tier Filtering

`New(tiers ...filecache.Cache)` silently drops nil tiers. This allows disabled caches
(e.g. `memcache.Open` with `Enabled:false` returns nil) to be passed directly without
wrapping. A ChainedCache with zero non-nil tiers behaves identically to `filecache.NopCache`.

Back-ref: `internal/modules/chaincache/chaincache.go:New`

---

## SPEC-CC-003: Concurrent Fetch Deduplication

Concurrent calls to `GetOrFetch` for the same uncached key share a single fetch invocation
via `singleflight.Group` at the chain level. Only one fetch is issued regardless of how many
goroutines request the same key simultaneously. The returned value is an independent copy
for each caller.

Back-ref: `internal/modules/chaincache/chaincache.go:GetOrFetch`

---

## SPEC-CC-004: Thread Safety

`ChainedCache` is safe for concurrent use. All Get/Put/GetOrFetch/Close operations may
be called from multiple goroutines simultaneously.

Back-ref: `internal/modules/chaincache/chaincache.go:ChainedCache`

---

## SPEC-CC-005: Close

`Close()` closes every tier and returns all errors joined together via `errors.Join`.
It is safe to call `Close()` on a nil `*ChainedCache`.

Back-ref: `internal/modules/chaincache/chaincache.go:Close`
