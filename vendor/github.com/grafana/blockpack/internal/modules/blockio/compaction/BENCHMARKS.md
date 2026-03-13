# compaction — Benchmark Specifications

This document describes performance baselines and benchmark scenarios for the
`internal/modules/blockio/compaction` package.

---

## Overview

Compaction is a background operation in Blockpack and is not on the critical query path.
The primary performance concern is throughput: spans processed per second and memory
allocated per span during the merge.

Key metrics:
- **spans/s**: throughput during compaction
- **bytes/span**: memory allocated per span (target: < 512 bytes/span with native path)
- **allocs/op**: allocations per span (native path avoids OTLP object reconstruction)

---

## BENCH-01: BenchmarkCompactBlocks_NativeColumns

**Scenario:** Compact N blocks of M spans each using the native columnar path.

**Parameters:**
- `N = 10` blocks, `M = 1000` spans each → 10,000 total spans
- `MaxSpansPerBlock = 2000`

**Expected baseline (not yet measured):**
- Throughput: > 500,000 spans/s on commodity hardware
- Memory: < 512 bytes/span allocated
- No OTLP object allocations per span (native path)

**Implementation note:** No benchmarks are currently implemented for this package.
When implementing, compare `AddRow` (native) vs `AddTracesData` (OTLP reconstruction)
to validate the design decision in NOTES.md §1.

---

## BENCH-02: BenchmarkCompactBlocks_Dedup

**Scenario:** Compact 2 identical input files (100% duplicate spans) to measure deduplication
overhead.

**Parameters:**
- 2 providers, each with 5,000 spans (all identical trace:id + span:id)

**Expected baseline (not yet measured):**
- Map lookup overhead: O(N) with constant-time hash for [24]byte keys
- Memory: `seenSpans` map grows to N entries then stays constant (duplicates skipped)
