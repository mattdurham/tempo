# filecache — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/filecache` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-FILE-001: FileCache Get Latency (Warm)

**Target:** Disk-backed Get for a cached key (warm OS page cache) completes in < 500 µs.

**Setup:** Open FileCache; Put a value; measure repeated Get calls.

**Variants:**
| Sub-benchmark | Value size |
|---------------|-----------|
| `_1KB`        | 1 KB      |
| `_64KB`       | 64 KB     |
| `_1MB`        | 1 MB      |

**Implementation:** Not yet implemented — `BenchmarkFileCacheGet` in `filecache/filecache_bench_test.go`.

---

## BENCH-FILE-002: FileCache Put Throughput

**Target:** Put throughput >= 50 MB/s for 64 KB values on a local SSD.

**Setup:** Open FileCache with generous MaxBytes; measure Put throughput across N distinct keys.

**Implementation:** Not yet implemented — `BenchmarkFileCachePut` in `filecache/filecache_bench_test.go`.

---

## BENCH-FILE-003: NopCache GetOrFetch Overhead

**Target:** NopCache GetOrFetch adds < 200 ns overhead vs. calling fetch directly.

**Setup:** nil Cache; GetOrFetch with a no-op fetch returning a fixed slice.

**Implementation:** Not yet implemented — `BenchmarkNopCacheGetOrFetch` in `filecache/filecache_bench_test.go`.

---

## BENCH-FILE-004: FIFO Eviction Under Byte Budget

**Target:** Eviction (triggered at MaxBytes) adds < 10 ms latency per Put when eviction fires.

**Setup:** Fill cache to 95% of MaxBytes; measure Put latency as eviction fires.

**Implementation:** Not yet implemented — `BenchmarkFileCacheEviction` in `filecache/filecache_bench_test.go`.

---

## BENCH-FILE-005: Concurrent GetOrFetch Singleflight

**Target:** Under N concurrent goroutines requesting the same absent key, fetch is called
exactly once; total throughput degradation < 20% vs. non-concurrent baseline at N=16.

**Setup:** N goroutines call GetOrFetch for the same absent key; fetch has a 1 ms latency.

**Implementation:** Not yet implemented — `BenchmarkFileCacheSingleflight` in `filecache/filecache_bench_test.go`.
