# objectcache Module — Design Notes

## NOTE-OC-004: Hard fallback budget when GOMEMLIMIT is not set
*Added: 2026-04-14*

The original `ensureInit` only set a byte budget when `GOMEMLIMIT` was configured;
if the env var was absent, `maxBytes` stayed 0 and the eviction loop
(`for c.maxBytes > 0 && ...`) never fired. This caused a production OOM: querier
pods without `GOMEMLIMIT` filled `parsedIntrinsicCache` to ~7 GiB after a few
`histogram_over_time` queries and were killed by the kernel.

Fix: `ensureInit` now falls back to `defaultMaxBytesNoGOMEMLIMIT` (256 MiB) when
`debug.SetMemoryLimit(-1)` returns 0 (i.e., GOMEMLIMIT not set). This ensures the
cache is always bounded regardless of operator configuration.

The 256 MiB fallback is conservative — enough for ~50-100 average-sized intrinsic
columns but well below typical pod limits. Deployments with known memory budgets
should set `GOMEMLIMIT` to get the 20%-fraction behaviour, or call `SetMaxBytes`
directly.

Back-ref: `internal/modules/objectcache/cache.go:ensureInit`

---

## NOTE-OC-001: Why strong references instead of weak.Pointer
*Added: 2026-03-23*
*Updated: 2026-03-29*

The reader package originally used three `sync.Map` globals that cached parsed
file metadata, sketch indexes, and decoded intrinsic columns using strong `*T`
pointers. These caches grew without bound. An intermediate design used
`weak.Pointer[V]` (Go 1.24+) to allow GC-cooperative eviction when no `*Reader`
held a strong reference.

`weak.Pointer` was subsequently replaced with plain strong references after
profiling revealed a 50x performance regression: weak pointer entries were
reclaimed between block scans, forcing constant re-decode from the file cache.
Even with `filecache.GetOrFetch` deduplicating raw I/O, the snappy decompression
and struct parse cost (up to ~45 MB per file) dominated query latency when the
parsed objects were repeatedly evicted and rebuilt.

Strong references mean cached entries persist for the process lifetime.
Memory is bounded by `GOMEMLIMIT` at the process level — the Go runtime soft-
memory limit provides the necessary budget control without per-entry eviction.
Operators set `GOMEMLIMIT` to their deployment's available memory; the runtime
triggers GC before OOM.

## NOTE-OC-002: No background goroutine for cleanup
*Added: 2026-03-23*

A background goroutine scanning for stale keys was considered and rejected.
With strong references there are no dead entries to clean up — every stored key
maps to a live `*V`. For typical workloads (hundreds of distinct files), the key
count is small enough that no cleanup mechanism is needed.

## NOTE-OC-003: No GetOrPut / singleflight
*Added: 2026-03-23*

A `GetOrPut(key, factory)` with singleflight deduplication was considered to
prevent concurrent cache misses from triggering parallel re-parses of the same
file. This was deferred because `filecache.GetOrFetch` already deduplicates the
underlying I/O at the raw-bytes level. A double-parse (two goroutines both get a
cache miss, both parse, second Put overwrites first) is rare and benign for
immutable data.
