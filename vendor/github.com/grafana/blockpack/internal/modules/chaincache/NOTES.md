# chaincache — Design Notes

---

## NOTE-CC-001: Recommended Cache Chain Order
*Added: 2026-04-14*

**Decision:** The recommended tier order is `memorycache → filecache → memcache`.

**Why:** This orders tiers fastest-first: in-memory reads are sub-microsecond, disk reads
are microseconds to milliseconds, and remote memcache reads are milliseconds. On a hit,
write-back promotes the value to all faster tiers so subsequent reads are served from
the fastest available tier.

**How to apply:** Callers constructing a ChainedCache should follow this order. Nil
tiers (disabled caches) are filtered automatically by `New()`.

Back-ref: `internal/modules/chaincache/chaincache.go:New`

---

## NOTE-CC-002: Singleflight at Chain Level
*Added: 2026-04-14*

**Decision:** Singleflight deduplication is applied at the ChainedCache level, not
within individual tier implementations that also implement it.

**Why:** Individual tiers (memcache, memorycache) each have their own singleflight. The
chain-level singleflight prevents redundant tier checks when multiple goroutines race
on the same uncached key. Without chain-level deduplication, N goroutines would each
traverse all tier misses before one wins and fetches.

**How to apply:** Do not remove the `c.group.Do` wrapper in `GetOrFetch` — it is
not redundant even if the fetch function is slow.

Back-ref: `internal/modules/chaincache/chaincache.go:GetOrFetch`
