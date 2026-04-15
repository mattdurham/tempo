# memcache — Specifications

This document defines the public contracts and invariants for the `memcache` package.

---

## SPEC-MC-001: MemCache Interface Compliance

`MemCache` implements `filecache.Cache` and is intended as the outermost (slowest but
largest) tier in a multi-level cache chain.

Back-ref: `internal/modules/memcache/memcache.go:MemCache`

---

## SPEC-MC-002: Key Hashing

All cache keys are SHA-256 hashed before being sent to memcache. This maps
arbitrary-length blockpack cache keys (e.g. long S3 paths) to valid memcache keys
(≤ 250 chars, ASCII printable). The hash is hex-encoded (64 chars).

Back-ref: `internal/modules/memcache/memcache.go:memcacheKey`

---

## SPEC-MC-003: Transient Error Handling

Connection errors and other transient failures are treated as cache misses (not errors).
The `Get` method returns `(nil, false, nil)` on any non-`ErrCacheMiss` error.
Memcache is a best-effort tier; an unavailable server must never break the read path.

Back-ref: `internal/modules/memcache/memcache.go:Get`

---

## SPEC-MC-004: Enabled Flag

When `Config.Enabled` is false, `Open` returns `(nil, nil)`. A nil `*MemCache` is safe
to use: all operations become no-ops. `Get` returns `(nil, false, nil)`, `Put` is a
no-op, `GetOrFetch` delegates directly to the fetch function, `Close` is a no-op.

Back-ref: `internal/modules/memcache/memcache.go:Open`

---

## SPEC-MC-005: Concurrent Fetch Deduplication

Concurrent calls to `GetOrFetch` for the same uncached key share a single fetch
invocation via `singleflight.Group`. Only one fetch is issued per key regardless of
concurrent callers.

Back-ref: `internal/modules/memcache/memcache.go:GetOrFetch`
