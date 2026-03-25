# objectcache Module — Design Notes

## NOTE-OC-001: Why weak.Pointer instead of sync.Map with strong references
*Added: 2026-03-23*

The reader package had three `sync.Map` globals that cached parsed file metadata,
sketch indexes, and decoded intrinsic columns using strong `*T` pointers. These
caches grew without bound — entries were never evicted except during test teardown
via `ClearCaches()`. In a Tempo deployment scanning hundreds of blockpack files,
parsed metadata (~45 MB per file after snappy decompression) accumulated until OOM
or process restart.

`weak.Pointer[V]` (Go 1.24+, available here — go.mod declares `go 1.26.0`) solves
this: entries remain alive as long as a `*Reader` holds a strong reference to the
parsed data, and become GC-eligible when all readers for a file are gone.

The stale-key problem (dead weak.Pointer entries accumulating in the map) is
addressed by lazy deletion in `Get`: when `.Value()` returns nil, the key is
immediately deleted before returning nil to the caller.

## NOTE-OC-002: No background goroutine for cleanup
*Added: 2026-03-23*

A background goroutine scanning for stale keys was considered and rejected.
Dead entries waste only map key memory (a few dozen bytes per string), not value
memory (the GC reclaims values). For typical workloads (hundreds of distinct files),
the key count is small enough that lazy cleanup on Get is sufficient.

## NOTE-OC-003: No GetOrPut / singleflight
*Added: 2026-03-23*

A `GetOrPut(key, factory)` with singleflight deduplication was considered to
prevent concurrent cache misses from triggering parallel re-parses of the same
file. This was deferred because `filecache.GetOrFetch` already deduplicates the
underlying I/O at the raw-bytes level. A double-parse (two goroutines both get a
cache miss, both parse, second Put overwrites first) is rare and benign for
immutable data.
