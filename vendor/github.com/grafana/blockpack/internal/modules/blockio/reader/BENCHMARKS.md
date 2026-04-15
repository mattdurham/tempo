# blockio/reader — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

Key benchmarks should measure:
- Reader open latency (see `BENCH-R-08: BenchmarkReaderOpen_V13` in `blockio/BENCHMARKS.md`)
- ParseBlockFromBytes throughput
- ReadGroup I/O coalescing efficiency
- Bloom filter reject rate vs. full scan

When benchmarks are written, add BENCH-READER-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.

---

## BENCH-READER-001: Lazy Decompression — Narrow vs Wide Column Access
**Function:** `BenchmarkLazyDecompression_NarrowVsWide`
**File:** `internal/modules/blockio/reader/alloc_bench_test.go`
**Setup:** 200-span block with 1 target column (`span.target.col`) + 18 extra columns.
- **Wide sub-bench:** `GetBlockWithBytes(0, nil)` — all 19 columns eagerly decompressed.
- **Narrow sub-bench:** `GetBlockWithBytes(0, {"span.target.col"})` — 1 column decompressed; 18 stay compressed.

**Expected relationship:** Narrow allocs/op and ns/op must be meaningfully lower than
Wide. A meaningful improvement confirms SPEC-V14-002 defers snappy cost for non-wanted
columns. If Narrow ≈ Wide, deferred decompression is not working for the non-wanted path.

**Note:** This benchmark measures the lazy-registration overhead (storing compressedEncoding
slice, allocating the lazyStore arena). It does NOT assert that non-wanted columns remain
compressed — use READER-TEST-014 for that structural check. The allocs/op gap is the
regression signal.

Back-ref: `internal/modules/blockio/reader/alloc_bench_test.go:BenchmarkLazyDecompression_NarrowVsWide`
