# filecache — Specifications

This document defines the public contracts and invariants for the `filecache` package.

---

## SPEC-FC-001: Cache Interface

The `Cache` interface is the common contract implemented by all blockpack cache tiers:
`FileCache` (disk), `memorycache.MemoryCache` (in-process), `memcache.MemCache` (remote),
and `chaincache.ChainedCache` (multi-tier).

Methods:
- `Get(key string) ([]byte, bool, error)` — returns an independent copy on hit
- `Put(key string, value []byte) error` — may silently drop oversized entries
- `GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error)` — miss calls fetch
- `Close() error` — releases resources; must be safe on nil receiver

A nil `Cache` value is NOT safe to use. Callers that may receive nil should use
`filecache.NopCache` as a safe no-op stand-in.

Back-ref: `internal/modules/filecache/cache.go:Cache`

---

## SPEC-FC-002: NopCache

`NopCache` is a `Cache` that never stores anything and always calls the fetch function.
It is safe to use as a drop-in when no caching is desired.

Back-ref: `internal/modules/filecache/cache.go:NopCache`

---

## SPEC-FC-003: FileCache Disk Layout

`FileCache` stores one file per entry under `<dir>/<2-char-hex-prefix>/<sha256hex>.bin`.
File content format: `[4B magic "BPC1"][4B keyLen LE][key bytes][value bytes]`.
In-memory state is rebuilt on `Open` by walking the directory. Eviction is FIFO
(insertion order): when total bytes exceed `MaxBytes`, the oldest entries are removed.

Back-ref: `internal/modules/filecache/filecache.go:FileCache`

---

## SPEC-FC-004: FileCache Enabled Flag

When `Config.Enabled` is false, `Open` returns `(nil, nil)`. Callers must treat a nil
`*FileCache` as equivalent to `NopCache`. Nil `*FileCache` methods are safe no-ops.

Back-ref: `internal/modules/filecache/filecache.go:Open`

---

## SPEC-FC-005: FileCache Concurrent Safety

`FileCache` is safe for concurrent use. Concurrent writes to different entries succeed
independently (OS filesystem handles concurrency). Concurrent fetches for the same
uncached key are deduplicated via `singleflight`.

Back-ref: `internal/modules/filecache/filecache.go:FileCache`
