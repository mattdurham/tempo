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
