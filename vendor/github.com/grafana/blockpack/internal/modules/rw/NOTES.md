# rw — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants
for the `rw` package. These notes complement SPECS.md and are intended to
prevent re-introducing decisions that were deliberately reversed.

---

## 1. Why Caching Wraps Tracking (Not the Reverse)
*Added: 2026-02-27*

**Decision:** `DefaultProvider` composition is `RangeCachingProvider(TrackingReaderProvider(storage))`,
not `TrackingReaderProvider(RangeCachingProvider(storage))`.

**Rationale:** The `IOOps` and `BytesRead` metrics are used to monitor object-storage
efficiency (see BENCHMARKS.md). If the tracker were the outer layer, cache hits would
increment `IOOps` even though no actual storage I/O occurred. This would inflate the
metrics and make them useless for detecting regressions. Wrapping the tracker with the
cache means the tracker only sees reads that reach actual storage.

**Consequence:** `IOOps()` reflects real backend I/O operations, not logical read calls.
Cache hits are invisible to the metrics layer by design.

---

## 2. Why `latencyProvider` Is Unexported
*Added: 2026-02-27*

**Decision:** `latencyProvider` is unexported; test callers use `NewDefaultProviderWithLatency`.

**Rationale:** Artificial latency injection is a test concern only. Exporting
`latencyProvider` would invite misuse in production code and pollute the public API.
The constructor `NewDefaultProviderWithLatency` provides a safe, intentional entry point
that makes the test-only nature explicit.

**Consequence:** The `latencyProvider` type is not visible outside the `rw` package.
Tests that need latency injection must use `NewDefaultProviderWithLatency`.

---

## 3. Why Providers Live in Their Own Module
*Added: 2026-02-27*

**Decision:** Extracted `ReaderProvider`, `DataType`, `TrackingReaderProvider`,
`RangeCachingProvider`, and `DefaultProvider` from `blockio/shared/provider.go` and
`blockio/reader/provider.go` into a new `internal/modules/rw/` package.

**Rationale:** The storage abstraction layer (`rw`) is logically independent of the
blockpack file format. Keeping provider types in `blockio/shared/` or `blockio/reader/`
created an artificial coupling between byte-moving concerns and format-parsing concerns.
By extracting them into their own module:
- The package can be tested without any blockpack format knowledge.
- Future alternative storage backends can import `rw` without pulling in the full
  blockio dependency graph.
- Cyclic import risks between shared/, reader/, and future modules are eliminated.

**Consequence:** `internal/modules/blockio/shared/provider.go` retains only `SpanFieldsProvider`
(field access, not byte-moving). All callers of `ReaderProvider` and `DataType` now import
`github.com/grafana/blockpack/internal/modules/rw` instead of `blockio/shared`.

---

## 4. TOCTOU Window in RangeCachingProvider (Duplicate Cache Entries)
*Added: 2026-02-27*

**Observation:** There is a TOCTOU window in `RangeCachingProvider.ReadAt` between
releasing the read lock (after a cache miss) and acquiring the write lock to append
the new entry. Two goroutines that simultaneously observe a miss for the same range
can both call `underlying.ReadAt` and both append a `cachedRange` for the same
offset/length, resulting in duplicate entries in `c.cache`.

**Why this is acceptable:** Duplicate entries do not cause incorrect data to be
returned — the first matching entry wins on subsequent scans. The only cost is extra
memory proportional to the number of concurrent goroutines that raced on a cold range.
In blockpack usage, providers are lifetime-bounded per file (NOTES.md §5), so each
provider is opened, read in a small number of operations, and discarded. The number
of concurrent goroutines that can race on the same range for the same provider is low
in practice, making the memory impact negligible.

**Pre-existing behavior:** This pattern was inherited from
`internal/modules/blockio/reader/provider.go` and was not introduced by this package.

**If this ever becomes a concern:** A check-after-lock pattern (re-scan cache under
write lock before appending) would eliminate duplicates at the cost of a second scan.

---

## 5. No Per-Range Cache Eviction
*Added: 2026-02-27*

**Decision:** `RangeCachingProvider` has no eviction policy. Cached ranges accumulate
for the lifetime of the provider instance.

**Rationale:** The primary use case is blockpack file reading, where each file is opened,
read in a small number of I/O operations (footer, header, metadata, blocks), and then
closed. The provider instance is discarded with the reader. Eviction complexity would
add overhead with no benefit in this lifetime-bounded use case.

**Consequence:** Callers must not reuse a `RangeCachingProvider` across logically
different files or after the underlying storage has changed. Cache invalidation is not
supported; create a new provider instance instead.

**Known Limitation — Unbounded Memory Growth:** The `cache` slice grows without bound
as new ranges are read. There is no size limit, LRU eviction, or TTL. In long-lived
or reused provider instances, this will exhaust memory proportional to total bytes
read. This is intentional for the expected per-file lifecycle, but operators must
ensure `RangeCachingProvider` instances are discarded promptly after the file is
no longer needed. The godoc on `RangeCachingProvider` carries a warning to this effect.

Back-ref: `internal/modules/rw/caching.go:RangeCachingProvider`

---

## 6. Short-Read Guard in RangeCachingProvider
*Added: 2026-02-27*

**Decision:** `RangeCachingProvider.ReadAt` now returns `(n, io.ErrUnexpectedEOF)`
when the underlying provider returns `n < len(p)` with `err == nil`.

**Rationale:** The `ReaderProvider` interface does not formally inherit `io.ReaderAt`,
but callers throughout the blockpack read path assume a nil error means the full buffer
was filled. A provider that returns a short read without an error (e.g., a test double
using `copy` without EOF handling) causes callers to silently operate on a truncated
buffer. Making the caching layer enforce the full-read contract converts a silent data
hazard into an explicit, catchable error.

**Placement:** The guard is in `caching.go` rather than in `tracking.go` or the
interface definition. This is the earliest point in the default provider stack where
a short read would be *cached* and silently served as complete data to future callers.
`TrackingReaderProvider` propagates short reads transparently without caching them.

**Cache interaction:** The guard returns before the `cache = append(...)` line, so
partial reads are never cached. A subsequent full read for the same range will miss
the cache and issue a fresh underlying call.

**Consequence:** Any `ReaderProvider` that returns `n < len(p)` with `err == nil` will
surface as `io.ErrUnexpectedEOF` at the caching layer. All production providers
(`os.File` via `storageReaderProvider`) already satisfy the full-read contract.
Test doubles that deliberately return short reads must account for this; see
`shortReadProvider` and RW-T-05 in `provider_test.go`.

---

## 7. Why DataType Changed from string to uint8
*Added: 2026-03-04*

**Decision:** `DataType` was changed from `type DataType string` to `type DataType uint8`
with iota-assigned constants. This is a **breaking change** — callers that used string
literals (e.g. `DataType("footer")`) must migrate to the typed constants.

**Rationale:** The string representation was used for human-readable debugging, but it
provided no correctness benefit. The `uint8` representation:
- Enables exhaustive switch statements and compile-time checks for unknown values.
- Allows `dataTypeTier()` to be a simple switch with a zero-allocation fast path.
- Makes it impossible to construct an arbitrary string and accidentally pass it as a
  valid DataType — the type system now enforces use of declared constants.
- Is more compact in memory when stored in structs or passed on the stack.

**Removed constants:**
- `DataTypeIndex` — was defined but never used in any `ReadAt` call.
- `DataTypeCompact` — renamed to `DataTypeTraceBloomFilter` for clarity; the compact
  trace index section contains both the trace-ID bloom filter and the hash map.

**New constants:**
- `DataTypeUnknown` (iota 0) — the zero value; treated as lowest priority.
- `DataTypeTraceBloomFilter` — replaces `DataTypeCompact`.
- `DataTypeTimestampIndex` — for per-file timestamp index reads.

Back-ref: `internal/modules/rw/provider.go:DataType`

---

## 8. SharedLRUCache — Priority-Tiered Eviction Design
*Added: 2026-03-04*

**Decision:** `SharedLRUCache` uses four independent `container/list` LRU queues
(one per priority tier) rather than a single LRU or a size-weighted heap.

**Rationale:** The blockpack read path issues reads with very different re-use
profiles: footer/header reads happen once per file open and are tiny (< 1 KB);
bloom filter and timestamp index reads happen once per query and are small (< 100 KB);
block reads happen many times per query and are large (10 KB – 10 MB). A single
unified LRU would evict footers under block read pressure even though footers are
the cheapest possible cache hits (saves the most I/O per byte cached). Tiered
eviction ensures that the most cost-effective data (footers, bloom filters) survives
cache pressure from the cheapest-to-re-read data (blocks).

**Why four tiers not two:** Three meaningful priority gaps exist:
1. Footer/Header — critical for opening any file, always tiny.
2. Metadata/TraceBloomFilter — critical for query pruning, small.
3. TimestampIndex — useful for time-range pruning, medium.
4. Block — large, re-readable from storage with predictable cost.

**Why `container/list` not a custom heap:** The eviction pattern is always
"evict LRU from tier N before tier N-1". A doubly-linked list per tier gives
O(1) append (MRU push to back), O(1) eviction (pop from front), and O(1)
promotion (MoveToBack). A heap would add O(log n) cost for no benefit.

**Cache key design:** Keys are `(readerID string, offset int64, length int)`.
Length is included because the same offset can be read with different lengths
(e.g. footer is 22 bytes; a block at the same offset would be larger).
Sub-range serving (as in `RangeCachingProvider`) is intentionally not implemented:
`SharedLRUCache` targets exact-match reuse across readers, not sub-range reuse
within a single reader's read sequence.

**TOCTOU note:** `SharedLRUCache.Put` checks for key existence under the mutex,
so duplicate entries cannot accumulate (unlike `RangeCachingProvider`).

Back-ref: `internal/modules/rw/lru.go:SharedLRUCache`

---

## NOTE-009: Disk-Backed Caches Removed (ObjectCache and DiskCache)
*Added: 2026-03-05*

**Decision:** `ObjectCache` (bbolt-backed, string-keyed, async writes) and `DiskCache` /
`DiskLRUProvider` (bbolt-backed, offset-keyed, synchronous writes) were removed entirely.

**Rationale:** Benchmarking showed both implementations hurt rather than helped performance
due to excessive allocations:
- `ObjectCache.Put` made 2 full-block copies (a `dataCopy` slice + the encode buffer);
  `Get` made 1 additional copy from the bbolt mmap — 3× the allocations of the no-cache path.
- `DiskCache.Put` was synchronous (blocking `db.Update`), adding ~1–10 ms latency per write
  on the hot query path and negating the supposed latency benefit for subsequent reads.

The in-memory `RangeCachingProvider` and `SharedLRUProvider` remain. They avoid disk I/O
entirely and do not suffer the allocation overhead.

**Consequence:** All `readThroughCache` callsites in `blockio/reader` were replaced with
direct `readRange` / `provider.ReadAt` calls. The `WithObjectCache` and `WithPath` reader
options were removed. The `ReaderOption` type still exists for future extensibility.

---

## NOTE-010: BUG-15 Fix — cacheKey.length Changed from int32 to int (2026-04-01)
*Added: 2026-04-01*

**Decision:** `cacheKey.length` field changed from `int32` to `int`. The `//nolint:gosec`
conversion annotations on both `Get` and `Put` were removed (no conversion needed).

**Rationale:** `int32` silently truncates lengths exceeding 2^31-1 (≈2 GB), producing a
negative or wrapped cache key. A cache `Get` with a >2 GB buffer would not match a `Put`
of the same data because the stored key has a different `length` value. The result is a
silent cache miss on every subsequent read of that range — correct behavior but degraded
performance. In practice, `MaxBlockSize` (1 GB) prevents any single read buffer from
reaching this limit, but the type mismatch is a latent trap for future callers or platform
changes. Using plain `int` (native word size: 32-bit on 32-bit, 64-bit on 64-bit platforms)
eliminates the truncation on 64-bit builds and the nolint annotations that suppressed it.

Back-ref: `internal/modules/rw/lru.go:cacheKey`
