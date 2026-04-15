# memcache — Design Notes

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
