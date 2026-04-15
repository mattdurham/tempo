# rw — Benchmark Targets and I/O Metrics

This document defines the I/O performance targets for blockpack storage providers.
These targets apply to the full provider stack (DefaultProvider over real object storage).

---

## I/O Metric Targets

| Metric | Good | Warning | Critical |
|--------|------|---------|----------|
| `io_ops` | < 500 | 500 – 1000 | > 1000 |
| `bytes/io` | > 100 KB | 10 – 100 KB | < 10 KB |

These thresholds are inherited from the blockio design invariants (see
`internal/modules/blockio/NOTES.md §1`). The rationale: object storage request
latency (50–100 ms first-byte) dominates cost, not bytes transferred. Each unnecessary
I/O operation has a measurable latency cost. Keeping `io_ops` low and `bytes/io` high
ensures the provider stack stays efficient on S3/GCS/Azure.

---

## BENCH-RW-01: BenchmarkDefaultProviderSingleRead

**Target:** Read a 1 MB payload in a single I/O; simulate 20 ms per-request latency.

```
BenchmarkDefaultProviderSingleRead — cache miss: 1 I/O, ~20 ms, 1 MB/io
```

Use `NewDefaultProviderWithLatency(underlying, 20*time.Millisecond)` for benchmarks
that simulate real-world object-storage first-byte latency.

**Implementation:** `BenchmarkDefaultProviderSingleRead` in `rw/provider_bench_test.go`.

---

## BENCH-RW-02: BenchmarkDefaultProviderCacheHit

**Target:** Cache-hit read through `DefaultProvider` must produce 0 I/Os in < 1 µs.

```
BenchmarkDefaultProviderCacheHit — cache hit: 0 I/O, < 1 µs
```

**Setup:** 1 MB payload; prime cache with one read; call `dp.Reset()` to zero counters;
then measure repeated hits.

**Implementation:** `BenchmarkDefaultProviderCacheHit` in `rw/provider_bench_test.go`.

---

## BENCH-RW-03: SharedLRUProvider — Cross-Reader Cache Hit

**Target:** Cache hit through `SharedLRUProvider` must add < 5 µs overhead vs. a direct
memory copy — the mutex acquisition and map lookup must not dominate.

```
BenchmarkSharedLRUProviderCacheHit — cache hit: 0 I/O, < 5 µs/op
```

**Setup:** 1 MB payload; `SharedLRUCache(10 MB)`; prime cache with one read; then
measure repeated hits. A miss would cause an underlying read (defeating the benchmark).

**What to watch:** If hit latency climbs above 10 µs under concurrent load, the mutex
in `SharedLRUCache` may be a bottleneck — consider sharding the index.

**Implementation:** `BenchmarkSharedLRUProviderCacheHit` in `rw/provider_bench_test.go`.

---

## Notes

- `TrackingReaderProvider.IOOps()` is the source of truth for `io_ops` metrics.
- Cache hits suppress both `io_ops` and `BytesRead` from growing — the tracker only
  sees reads that reach actual storage, so both metrics reflect real backend I/O
  (see NOTES.md §1 for the composition rationale).
- Benchmarks should use `NewDefaultProvider` (not raw providers) to accurately reflect
  the production code path.
- `SharedLRUProvider` does not wrap a `TrackingReaderProvider` — callers that need
  I/O metrics should compose: `SharedLRUProvider → TrackingReaderProvider → storage`.
