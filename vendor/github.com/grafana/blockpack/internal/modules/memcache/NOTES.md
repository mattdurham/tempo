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

## NOTE-196: Socket timeout high enough to keep slow-but-healthy conns pooled
*Added: 2026-06-11*

**Decision:** Set `Timeout = ConnectTimeout = 5s` on the pooled gomemcache client
(`newPooledClient`) instead of leaving them at the library default of 100ms.

**Why:** This is the second half of the dial/handshake storm NOTE-162 only
partially fixed. A querier CPU profile (2026-06-11, `gcx process_cpu`) STILL
showed ~43% of querier CPU in the connection-establishment path —
`internal/runtime/syscall.Syscall6` (~30%) plus kernel `__inet_hash_connect`,
`__inet_check_established`, `tcp_twsk_unique` (ephemeral-port allocation +
TIME_WAIT reuse churn) and TLS GCM re-handshake crypto (`gcmAesDec`,
`addMulVVW2048`) — even though the 512-conn idle pool from NOTE-162 was deployed.

The mechanism: under a heavy metrics fan-out (M4/M9 issue hundreds of concurrent
`GetMulti`) memcache servers intermittently respond in >100ms. gomemcache's read
deadline (`netTimeout()` = `Timeout`, default 100ms) then fires and the read
returns a `net.Error` timeout. `condRelease` treats anything that is not a
`resumableError` (cache miss / CAS conflict / not-stored / malformed key) as a
broken connection and calls `cn.nc.Close()` — so every transiently-slow response
*permanently destroys* a pooled connection. The pool drains faster than it refills
and the next request must dial a fresh TCP connection + redo the TLS handshake,
re-triggering the exact storm NOTE-162's deep pool was meant to stop.

Raising the socket timeout to 5s keeps transiently-slow-but-healthy connections in
the pool (they are reused, not re-dialed) while staying far below the query-level
timeout (bench uses 30s), so a genuinely dead connection is still reaped promptly
relative to the query budget. This is a structural fix to the connection lifecycle,
not a per-query knob; it benefits every cache-backed read path equally.

**How to apply:** Set `Timeout` and `ConnectTimeout` on the client built by
`newPooledClient`. `ConnectTimeout` falls back to `Timeout` when zero, but is set
explicitly for clarity. Pair with NOTE-162's deep idle pool — the deep pool is
necessary but not sufficient; without the timeout fix the pool keeps draining.

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

## NOTE-179: GetMulti batched fetch

`MemCache.GetMulti(keys)` wraps `gomemcache.GetMulti`, which groups the hashed keys per server
and pipelines them over a SINGLE connection per server. The per-column block-read path (NOTE-179
in blockio/reader) uses it to collapse the N-per-block per-column Gets — each of which forced a
fresh `(*Client).dial` (~28% of querier CPU) under the concurrent fan-out — into one request.
Transient errors and the nil receiver are treated as a total miss, identical to `Get`.

Back-ref: `internal/modules/memcache/memcache.go:GetMulti`
