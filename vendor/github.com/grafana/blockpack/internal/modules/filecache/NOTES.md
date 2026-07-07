# filecache — Design Notes

---

## NOTE-FC-001: File-Per-Entry vs. bbolt
*Added: 2026-04-14*

**Decision:** Use one file per cache entry instead of a bbolt key-value store.

**Why:** bbolt uses a single write lock, serializing all concurrent writes. File-per-entry
allows concurrent writes to different entries simultaneously — the OS filesystem handles
concurrency natively. This is critical for workloads where many goroutines write cache
entries concurrently (e.g. multiple block reads completing at the same time).

**How to apply:** Do not revert to a single-file store. If atomic multi-key operations
are needed in the future, investigate a sharded approach rather than a single lock.

Back-ref: `internal/modules/filecache/filecache.go`

---

## NOTE-FC-002: SHA-256 Key Hashing for Directory Layout
*Added: 2026-04-14*

**Decision:** Cache entries are stored at `<dir>/<first-2-chars-of-sha256hex>/<sha256hex>.bin`.
The first 2 hex chars form a 256-bucket prefix directory to limit directory entry count.

**Why:** Filesystems with large directories (thousands of files in one dir) suffer
performance degradation on stat/readdir. The 2-char prefix limits each subdirectory to
~1/256 of total entries. SHA-256 is used to map arbitrary-length cache keys (e.g. long
S3 paths) to fixed-length, filesystem-safe filenames.

**How to apply:** This layout is format-stable. Any change to the hashing or prefix
scheme requires migrating existing cache files.

Back-ref: `internal/modules/filecache/filecache.go`

## NOTE-FC-003: No fsync on Cache Writes
*Added: 2026-07-05*

**Decision:** `writeFile` does NOT call `f.Sync()` (fsync) before renaming the temp
file into place. Writes are made visible via `os.Rename` only.

**Why:** The FileCache is a cache, not primary storage — every entry is
re-derivable from the source block. A synchronous per-entry `fsync()` serializes
concurrent writers against the disk. Live diagnosis on the dev test cluster (issue #470)
showed a single full-scan trace-by-id burst parked ~30% of all querier goroutines
(229 of 775) blocked in `syscall.Fsync` via this exact path, hanging an interactive
`GET /api/traces/{id}` for 90+ seconds. Removing fsync eliminates that serialization
for every write path (search/metrics block reads too), not just trace-by-id.

**Why it is safe:**
- `os.Rename` gives atomic visibility *within the process*: a concurrent `Get`
  observes either no file or the fully-written file, never a torn one.
- On restart, `load()` re-reads and validates every `.bin` via `readFileHeader`
  and removes any corrupt/partial entry, so an OS crash that loses an un-synced
  entry is self-healing — the entry is simply re-fetched from the source block.
- Durability buys nothing for re-derivable cache data; the only cost of a lost
  entry is one cache miss.

**How to apply:** Do not re-add `f.Sync()` / `fdatasync` here. If a future cache
tier stores non-re-derivable data, that is a different durability decision and must
not be conflated with this re-fetchable block-column cache.

Back-ref: `internal/modules/filecache/filecache.go` (`writeFile`)
