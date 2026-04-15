# chaincache — Benchmark Targets

This document defines planned benchmarks for the `internal/modules/chaincache` package.

> **Implementation status:** No benchmarks are implemented yet. All entries below are planned
> targets — they define the performance contract for future work. Do not remove them.

---

## BENCH-CHAIN-001: GetOrFetch Hit Latency

**Target:** Cache hit from tier 0 must add < 1 µs vs. direct map lookup.

**Setup:** Single-tier chain with MemoryCache; prime cache; measure repeated GetOrFetch hits.

**Variants:**
| Sub-benchmark | Value size |
|---------------|-----------|
| `_1KB`        | 1 KB      |
| `_64KB`       | 64 KB     |
| `_1MB`        | 1 MB      |

**Implementation:** Not yet implemented — `BenchmarkChainCacheHit` in `chaincache/chaincache_bench_test.go`.

---

## BENCH-CHAIN-002: GetOrFetch Miss + Fetch Latency

**Target:** Full miss path (all tiers miss, fetch called, result stored) adds < 5 µs overhead
vs. calling fetch directly.

**Setup:** Single-tier chain; cache cold; fetch returns a fixed-size value.

**Implementation:** Not yet implemented — `BenchmarkChainCacheMiss` in `chaincache/chaincache_bench_test.go`.

---

## BENCH-CHAIN-003: Multi-Tier Hit Write-Back Throughput

**Target:** Write-back from tier N to tiers 0..N-1 adds < 2 µs per tier vs. direct tier Put.

**Setup:** 3-tier chain; key present only in tier 2; measure GetOrFetch throughput.

**Implementation:** Not yet implemented — `BenchmarkChainWriteBack` in `chaincache/chaincache_bench_test.go`.

---

## BENCH-CHAIN-004: Concurrent Singleflight Deduplication

**Target:** Under N concurrent goroutines requesting the same absent key, fetch is called exactly
once; latency overhead per goroutine < 10 µs at N=64.

**Setup:** N goroutines call GetOrFetch for the same key; fetch has a 1 ms simulated latency.

**Variants:**
| Sub-benchmark | Concurrency |
|---------------|------------|
| `_4`          | 4          |
| `_16`         | 16         |
| `_64`         | 64         |

**Implementation:** Not yet implemented — `BenchmarkChainSingleflight` in `chaincache/chaincache_bench_test.go`.
