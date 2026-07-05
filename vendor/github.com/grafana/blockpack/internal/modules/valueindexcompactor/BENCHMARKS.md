# valueindexcompactor — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valueindexcompactor`
package. Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, and independent of
`internal/modules/valueindex/BENCHMARKS.md`'s own `BENCH-VI-N` sequence — each module's
BENCHMARKS.md numbers from 1). IDs are assigned in ascending order and never reused or
renumbered.

Next free ID: **BENCH-VI-2**.

---

## BENCH-VI-1: mergeLevel peak-memory bound (pre- vs post-disk-streaming-redesign)

**Function / file:** `TestMergeLevel_MemoryBoundedRegardlessOfInputCount`
(`internal/modules/valueindexcompactor/mergelevel_scaling_test.go`) is the current, live test.
It is the Phase 5 refactor of an earlier Phase 0 red test, originally named
`TestMergeLevel_MemoryScalesWithInputCount_PreRedesign` — that name no longer exists as a
distinct test in the codebase (per plan.md Step 5.1, it was renamed/refactored in place into
today's test once the redesign landed, sharing the same measurement harness); its baseline
numbers below are preserved only as a code comment on
`TestMergeLevel_MemoryBoundedRegardlessOfInputCount` (`mergelevel_scaling_test.go:211-219`)
and in this entry, not as a separately-runnable test. Not a `go test -bench` microbenchmark;
this measures peak/cumulative memory around one `mergeLevel` call, since the property under
test (peak resident memory bounded regardless of input file count) is about memory shape, not
throughput.

**Purpose:** Prove `mergeLevel`'s peak decoded memory no longer scales linearly with the
number of input files (K) at a merge level, per the corrected SPEC-VI-1 / NOTE-VI-052
disk-streaming redesign — this benchmark exists specifically because of this module's OOM
history (NOTE-VI-046).

**Measured numbers (real, 2026-07-03, both from the same K=5/K=50 comparison):**

| Version | Metric | groupsPerFile | groupsPerBlock | K=5 | K=50 | Ratio |
|---|---|---|---|---|---|---|
| BEFORE (pre-redesign, historical baseline) | `TotalAlloc` delta (cumulative) | 200 | single block (default) | 1,863,048 B | 14,914,816 B | ~8.0x |
| AFTER (post-redesign, current test) | Peak `HeapAlloc` (resident, sampled) | 2000 | 20 | 2,347,992 B | 4,029,664 B | ~1.72x |

**Critical methodology caveat — the two rows are NOT directly comparable to each other:**

`TotalAlloc` delta is a *cumulative* metric — it counts every byte ever allocated during the
call, regardless of how much is resident at any one instant. It scales with total bytes
processed (K-proportional either way: reading K files and writing K temp files is
K-proportional whether the decode itself is eager or lazy) and therefore **cannot actually
prove the bounded-peak-memory property this redesign delivers**. This was confirmed
empirically while building the Phase 5 test: a `TotalAlloc`-delta version of the *same*
fixture still showed ~9.5x scaling from K=5 to K=50 even when run against the **fully
implemented, disk-streaming** code — because `TotalAlloc` counts the (unavoidably
K-proportional) bytes read from the store and written to local temp files just as much as the
(now block-bounded) decoded representation.

Peak `HeapAlloc` (sampled by a background goroutine that forces a GC every 200µs and tracks
the maximum observed live heap) is the metric that actually distinguishes "K fully-decoded
files resident simultaneously" (old design) from "K iterators each holding at most one small
block at a time" (new design), because it reflects genuinely-resident memory, not cumulative
work performed. The AFTER row uses this metric; the BEFORE row (captured before this
distinction was understood, at Phase 0) used the cumulative metric and a different, smaller
fixture shape (larger groupsPerFile with a single default-sized block, since block granularity
has zero effect on the old whole-file decoder's behavior).

**Do not read this table as "peak memory dropped from ~14.9 MB to ~4.0 MB" — that comparison
mixes two different metrics on two different fixtures and is not a valid before/after
percentage.** The correct before/after comparison is the **ratio** each metric shows for its
own K=5→K=50 scaling: ~8.0x (linear, the problem) vs. ~1.72x (bounded, the fix) — both
measuring "does memory scale with K," just via different (necessarily different, per the
caveat above) instruments. This is the actual property SPEC-VI-1's correction and
`TestMergeLevel_MemoryBoundedRegardlessOfInputCount`'s assertion (`peakLarge <
peakSmall*2`) claim and test.

**Regression threshold:** Flag if a future change to `TestMergeLevel_MemoryBoundedRegardlessOfInputCount`'s
K=50/K=5 peak-`HeapAlloc` ratio regresses toward or past 2x (its own asserted threshold) — that
would indicate the disk-backed iterator has regressed toward holding more than one block
resident per open iterator, reintroducing the scaling behavior this redesign eliminates.

**Spec invariants tested:** SPEC-VI-1 (valueindexcompactor, corrected 2026-07-03).

Back-ref: `internal/modules/valueindexcompactor/mergelevel_scaling_test.go:TestMergeLevel_MemoryBoundedRegardlessOfInputCount`
(current test, including the preserved historical pre-redesign numbers in its doc comment).
