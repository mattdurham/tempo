# memcache — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/memcache` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-MEM-001: GetOrFetch Round-Trip Latency (Warm Server)

**Target:** GetOrFetch hit latency (round-trip to local memcache) < 1 ms at p99.

**Setup:** Local memcache server; pre-populate key; measure repeated GetOrFetch hits.

**Variants:**
| Sub-benchmark | Value size |
|---------------|-----------|
| `_1KB`        | 1 KB      |
| `_64KB`       | 64 KB     |

**Implementation:** Not yet implemented — `BenchmarkMemcacheGetOrFetchHit` in `memcache/memcache_bench_test.go`.

---

## BENCH-MEM-002: Key Hashing Throughput

**Target:** hashKey() processes >= 500 K keys/sec for typical 64-byte keys.

**Setup:** Call hashKey() N times with a fixed 64-byte key; measure throughput.

**Implementation:** Not yet implemented — `BenchmarkMemcacheKeyHash` in `memcache/memcache_bench_test.go`.

---

## BENCH-MEM-003: Concurrent GetOrFetch Singleflight

**Target:** Under N concurrent goroutines requesting the same absent key, server is queried
exactly once; latency overhead per goroutine < 2 ms at N=32.

**Setup:** N goroutines call GetOrFetch for the same key; local memcache with 1 ms simulated
round-trip.

**Variants:**
| Sub-benchmark | Concurrency |
|---------------|------------|
| `_8`          | 8          |
| `_32`         | 32         |

**Implementation:** Not yet implemented — `BenchmarkMemcacheSingleflight` in `memcache/memcache_bench_test.go`.
