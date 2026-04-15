# blockio/writer — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

Key benchmarks should measure:
- Block encoding throughput (spans/second by column type)
- Flush latency for representative block sizes (1k, 10k, 100k spans)
- Auto-embedding throughput with mock embedder
- Range index construction time

When benchmarks are written, add BENCH-WRITER-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.
