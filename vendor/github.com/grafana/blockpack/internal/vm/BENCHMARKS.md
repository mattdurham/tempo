# vm — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

The `vm` package benchmarks should measure:
- TraceQL compilation time for representative filter queries
- ColumnPredicate evaluation throughput (spans/second)
- VectorScorer throughput for VECTOR() queries

When benchmarks are written, add BENCH-VM-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.
