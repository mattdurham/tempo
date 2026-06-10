# memcache — Design Notes

---

## NOTE-162: Deep idle-connection pool to stop the dial/handshake storm
*Added: 2026-06-10*

**Decision:** Build the gomemcache client via `newPooledClient` with
`MaxIdleConns = 512` (per server) and `MinIdleConnsHeadroomPercentage = -1`
instead of the bare `gomemcache.New(...)` which leaves `MaxIdleConns` at its
default of 2.

**Why:** A querier CPU profile (2026-06-10, `gcx process_cpu`) showed ~66% of
querier CPU in `memcache.(*Client).dial` → `net.Dialer.DialContext` →
`dialSerial`, bottoming out in kernel `__inet_hash_connect`,
`__inet_check_established`, `tcp_twsk_unique` (ephemeral-port allocation +
TIME_WAIT socket reuse) plus TLS re-handshake crypto (`bigmod.addMulVVW2048`,
edwards25519). The blockpack executor scan/decode path was <2% of CPU. This is a
connection-churn signature: a single heavy metrics query (M1/M4 `{} | rate()`)
fans out into hundreds of concurrent block-page / TOC / intrinsic cache lookups,
each calling `Client.Get`. With only 2 idle connections per server the pool is
instantly exhausted and every extra concurrent Get dials a fresh connection and
discards it. Sizing the pool above peak parallel requests lets connections be
reused. `MinIdleConnsHeadroomPercentage = -1` prevents the background reaper from
closing idle conns down to ~2 between query bursts (which would re-trigger the
storm on the next query).

**How to apply:** Construct the client through `newPooledClient`; do not call
`gomemcache.New` directly. If memcache server connection limits become a concern
(memcached default is 1024 conns/instance), tune `memcacheMaxIdleConns` down, but
keep it well above the per-query concurrent-lookup count.

Back-ref: `internal/modules/memcache/memcache.go:newPooledClient`

---

## NOTE-MC-001: Transient Errors as Misses
*Added: 2026-04-14*

**Decision:** Transient memcache errors (connection loss, server unavailable) are
treated as cache misses, not propagated as errors.

**Why:** Memcache is a best-effort cache layer. If the memcache server is unavailable,
the read path should fall back to the underlying data source (filecache or reader)
rather than failing the entire request. Treating transient errors as misses ensures
memcache outages are transparent to callers.

**How to apply:** Do not promote transient errors to caller-visible errors. Only
`ErrCacheMiss` (explicit key absence) and structural errors (corrupt response) warrant
differentiated handling.

Back-ref: `internal/modules/memcache/memcache.go:Get`

---

## NOTE-MC-002: Prometheus Metrics Registration
*Added: 2026-04-14*

**Decision:** Use `prometheus.AlreadyRegisteredError` to reuse existing metrics rather
than panicking on duplicate registration.

**Why:** Multiple MemCache instances (e.g. one per reader in a process) may share the
same Prometheus Registerer. The `memcacheRegisterOrReuse` helper recovers the existing
metric on `AlreadyRegisteredError` so that instantiating multiple caches does not panic.

**How to apply:** All new Prometheus metric registrations in this package should use
the same `registerOrReuse` pattern.

Back-ref: `internal/modules/memcache/memcache.go:memcacheRegisterOrReuse`
