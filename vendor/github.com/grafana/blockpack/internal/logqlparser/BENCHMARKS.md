# logqlparser — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

The `logqlparser` package benchmarks should measure:
- LogQL parse time for representative filter and pipeline expressions
- Label selector matching throughput
- Pipeline stage evaluation throughput

When benchmarks are written, add BENCH-LQL-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.
