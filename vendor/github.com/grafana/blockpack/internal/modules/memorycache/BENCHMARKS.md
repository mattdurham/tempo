# memorycache — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/memorycache` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-MEMO-001: Get/Put Throughput (Hot Cache)

**Target:** Get from a warm in-memory LRU >= 5 M ops/sec for 1 KB values.

**Setup:** MemoryCache with generous MaxBytes; prime with N entries; measure Get throughput.

**Variants:**
| Sub-benchmark | Value size |
|---------------|-----------|
| `_1KB`        | 1 KB      |
| `_64KB`       | 64 KB     |
| `_1MB`        | 1 MB      |

**Implementation:** Not yet implemented — `BenchmarkMemoryCacheGet` in `memorycache/memorycache_bench_test.go`.

---

## BENCH-MEMO-002: LRU Eviction Overhead

**Target:** Eviction overhead (triggered at MaxBytes boundary) < 2 µs per Put when eviction fires.

**Setup:** Fill cache to 95% of MaxBytes; measure Put latency as eviction fires repeatedly.

**Implementation:** Not yet implemented — `BenchmarkMemoryCacheEviction` in `memorycache/memorycache_bench_test.go`.

---

## BENCH-MEMO-003: Concurrent GetOrFetch Singleflight

**Target:** Under N concurrent goroutines requesting the same absent key, fetch is called
exactly once; latency overhead per goroutine < 5 µs at N=64.

**Setup:** N goroutines call GetOrFetch for the same key; fetch returns a fixed 1 KB value.

**Variants:**
| Sub-benchmark | Concurrency |
|---------------|------------|
| `_4`          | 4          |
| `_16`         | 16         |
| `_64`         | 64         |

**Implementation:** Not yet implemented — `BenchmarkMemoryCacheSingleflight` in `memorycache/memorycache_bench_test.go`.
