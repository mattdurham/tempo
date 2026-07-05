# valueindex — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valueindex` package.
Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`). IDs are assigned in
ascending order and never reused or renumbered.

Next free ID: **BENCH-VI-2**.

---

## BENCH-VI-1: BenchmarkCompactBucketFiles_OldVsStreaming

**Function / file:** `BenchmarkCompactBucketFiles_OldVsStreaming` —
`internal/modules/valueindex/stream_compaction_bench_test.go`.

**Purpose:** Peak-allocation regression baseline comparing the non-streaming
`CompactBucketFiles` path (`bucketmerge.go`) against the streaming
`StreamCompactBucketFiles` path (`stream_compaction.go`), on the same input: K=8 input
BucketFiles x 2000 groups/file, with every 5th key shared across files to force cross-file
merge work (exercises the heap merge's dedup/coalesce path, not just disjoint pass-through).

**Baseline (authoritative re-measurement, 2026-07-02, independent test-phase agent,
`go test -bench BenchmarkCompactBucketFiles_OldVsStreaming -benchmem
./internal/modules/valueindex/`, default 1s/op benchtime, Go 1.26.0, 13th Gen Intel
i5-13500). This supersedes an earlier self-reported figure from initial implementation that
used the same setup but fewer/less-precise iterations:**

| Path | Iterations | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| `_old` (`CompactBucketFiles`) | 48 | 24,990,000 (24.99 ms) | 19,260,000 (19.26 MB) | 364,245 |
| `_streaming` (`StreamCompactBucketFiles`) | 64 | 32,790,000 (32.79 ms) | 16,970,000 (16.97 MB) | 284,190 |

**Delta (streaming vs old):** +31.2% ns/op (slower), −11.9% B/op (smaller), −22.0% allocs/op
(fewer).

**This is an explicit, accepted tradeoff, not an unintended regression.** The streaming path
is serial-only by design (no parallel input downloads — see `NOTES.md` NOTE-VI-046) and
trades throughput for a bounded peak-memory profile, mirroring the `blockio/compaction`
NOTE-459 precedent this design follows: memory safety over throughput, because an OOM aborts
the whole compaction job while a slower serial merge only lengthens it. A future reader
should not treat the +31.2% ns/op figure as a bug — it is the expected cost of eliminating
`MergeBucketFiles`'s map-of-maps full-materialization (SPEC-VI-2), verified here by the
−11.9%/−22.0% memory-side improvement this benchmark exists to protect.

**Regression threshold:** Flag if `_streaming`'s `allocs/op` or `B/op` regresses back toward
or past `_old`'s baseline (364,245 allocs/op, 19.26 MB/op) — that would indicate the k-way
merge has silently reintroduced full-materialization behavior, defeating the reason this path
exists. Flag if `_streaming`'s `ns/op` regresses more than 20% *beyond this baseline's own
32.79 ms* (i.e. > ~39.3 ms) — the accepted throughput cost is already priced into this
baseline; further slowdown is not.

**Spec invariants tested:** SPEC-VI-2 (`StreamCompactBucketFiles` merge-semantics equivalence
+ peak-memory bound contract, `internal/modules/valueindex/SPECS.md`).

Back-ref: `internal/modules/valueindex/stream_compaction_bench_test.go:BenchmarkCompactBucketFiles_OldVsStreaming`.
