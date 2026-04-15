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
