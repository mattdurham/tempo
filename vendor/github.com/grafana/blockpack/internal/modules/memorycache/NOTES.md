# memorycache — Design Notes

---

## NOTE-MEM-001: In-Memory LRU as Fastest Cache Tier
*Added: 2026-04-14*

**Decision:** `MemoryCache` uses a doubly-linked list (`container/list`) with a map
index for O(1) LRU Get/Put operations.

**Why:** In a multi-level cache chain, the in-memory tier must be as fast as possible.
The `list + map` pattern gives O(1) promotion on Get (move to MRU end) and O(1) eviction
(remove from LRU end), avoiding any scan cost proportional to cache size.

**How to apply:** If additional metadata per entry is needed (e.g. TTL, type tag), add
it to the `entry` struct rather than changing the underlying data structure.

Back-ref: `internal/modules/memorycache/memorycache.go:MemoryCache`

---

## NOTE-MEM-002: Prometheus Metrics with Register-or-Reuse
*Added: 2026-04-14*

**Decision:** Prometheus metrics use a register-or-reuse pattern to avoid panic on
duplicate registration when multiple MemoryCache instances share the same registerer.

**Why:** Like memcache, multiple MemoryCache instances may exist in the same process.
Panicking on re-registration would break multi-reader setups. The register-or-reuse
pattern recovers the existing collector on `AlreadyRegisteredError`.

**How to apply:** All new Prometheus metric registrations in this package should follow
the same pattern.

Back-ref: `internal/modules/memorycache/memorycache.go:New`
