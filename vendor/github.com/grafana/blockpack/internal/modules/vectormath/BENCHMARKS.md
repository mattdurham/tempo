# vectormath — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

Key benchmarks should measure:
- Cosine similarity throughput at representative dimensions (384, 768, 1536)
- SIMD vs. scalar path comparison
- Batch scoring throughput (N candidates × D dimensions)

When benchmarks are written, add BENCH-VMATH-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.
