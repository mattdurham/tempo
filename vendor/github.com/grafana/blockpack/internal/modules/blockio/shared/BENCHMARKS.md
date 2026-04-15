# blockio/shared — Benchmark Targets

This file is a stub — entries to be added as benchmarks are written.

---

## Overview

The `blockio/shared` package provides shared types, constants, codecs, and utilities
used by both reader and writer. Key areas for benchmarking:

- Intrinsic column encode/decode throughput (DecodeTOC, DecodeIntrinsicColumnBlob)
- Snappy decompression throughput for representative column blob sizes
- ScanDictColumnRefs and ScanFlatColumnRefs scan throughput
- RLE index operations

When benchmarks are written, add BENCH-SHARED-* entries here following the format
established in `internal/modules/blockio/BENCHMARKS.md`.
