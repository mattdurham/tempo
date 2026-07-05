# valueindexconsumer — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valueindexconsumer`
package. Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, and distinct from
`valueindex/BENCHMARKS.md`'s and `valueindexcompactor/BENCHMARKS.md`'s own independent
`BENCH-VI-N` sequences despite the shared prefix). IDs are assigned in ascending order and
never reused or renumbered.

**This file did not exist before 2026-07-04** — created as part of task #85 (wiring
`internal/modules/valueindex/traceindex.go` into the trace-by-id path), per CLAUDE.md's
standing permission to create spec files under `internal/modules/`.

This file is a stub — no benchmark exists yet for the new trace-group buffer/flush path
introduced by Stage 2 of that work (confirmed via direct code review: no `Benchmark*` function
was added alongside `traceflush.go`). Entries should be added here if a future benchmark is
written, following the format used by `valueindex/BENCHMARKS.md`'s `BENCH-VI-1`.

Next free ID: **BENCH-VI-1**.

---

## Overview

Candidate future benchmarks (not yet written, listed for whoever picks this up):

- Trace-row spill/read-back throughput (`writeTraceRow`/`readTraceRow`,
  `traceflush.go`) at realistic per-block span-row volumes.
- `flushTraceGroups`'s grouping pass (`readTraceGroups`) memory footprint as a function of
  distinct-trace-count within one flush window, analogous to
  `valueindex/BENCHMARKS.md`'s `BENCH-VI-1` (`BenchmarkCompactBucketFiles_OldVsStreaming`),
  which benchmarks the equivalent grouping/merge step for the standard `BucketGroup` path.
