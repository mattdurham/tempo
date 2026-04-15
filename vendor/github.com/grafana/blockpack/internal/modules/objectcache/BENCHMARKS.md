# objectcache — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/objectcache` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-OC-001: Get/Put Throughput (Hot Cache)

**Target:** Get from a warm LRU in-process cache >= 2 M ops/sec for 1 KB values under
concurrent access.

**Setup:** ObjectCache with generous MaxBytes and N shards; prime with N entries; measure
Get throughput from M goroutines.

**Variants:**
| Sub-benchmark | Value size | Goroutines |
|---------------|-----------|-----------|
| `_1KB_4G`     | 1 KB      | 4         |
| `_1KB_16G`    | 1 KB      | 16        |
| `_64KB_4G`    | 64 KB     | 4         |

**Implementation:** Not yet implemented — `BenchmarkObjectCacheGet` in `objectcache/objectcache_bench_test.go`.

---

## BENCH-OC-002: LRU Eviction Overhead

**Target:** Eviction overhead at byte budget boundary < 5 µs per Put (SPEC-OC-003).

**Setup:** Fill cache to 95% of MaxBytes; measure Put latency as eviction fires.

**Implementation:** Not yet implemented — `BenchmarkObjectCacheEviction` in `objectcache/objectcache_bench_test.go`.

---

## BENCH-OC-003: Clear() Performance

**Target:** Clear() for a cache holding N entries completes in < 1 ms for N <= 100 K (SPEC-OC-005).

**Setup:** Fill cache with N entries; measure Clear() latency.

**Variants:**
| Sub-benchmark | Entry count |
|---------------|------------|
| `_1K`         | 1 000      |
| `_10K`        | 10 000     |
| `_100K`       | 100 000    |

**Implementation:** Not yet implemented — `BenchmarkObjectCacheClear` in `objectcache/objectcache_bench_test.go`.
