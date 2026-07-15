# valueindex — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valueindex` package.
Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`). IDs are assigned in
ascending order and never reused or renumbered.

Next free ID: **BENCH-VI-3**.

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

**Updated (2026-07-07, issue #490, task A-4/#98):** the `_old` (`CompactBucketFiles`) comparison
arm no longer exists — that function is deleted (zero remaining production callers, see
`NOTES.md` NOTE-VI-053 addendum). The benchmark is renamed `BenchmarkStreamCompactBucketFiles`
(top-level, no longer a `b.Run` subtest) and now stands alone as a baseline for the streaming
path only. **The historical `_old`/`_streaming` comparison numbers above are RETAINED, not
deleted, per this file's regression-threshold convention** — they remain the last known
old-path baseline for historical reference, but are no longer an active regression comparison
target since there is no old path left to compare against.

**Regression threshold, updated:** flag if `BenchmarkStreamCompactBucketFiles`'s `allocs/op` or
`B/op` regresses past the `_streaming` baseline above (284,190 allocs/op, 16.97 MB/op) — the
old-path comparison columns are historical context only, not a live threshold. The `ns/op`
threshold (> ~39.3 ms) from the original entry still applies unchanged.

---

## BENCH-VI-2: BenchmarkMergeGroupsAtKey_SmallKey_NeverSpills (issue #503)

**Function / file:** `BenchmarkMergeGroupsAtKey_SmallKey_NeverSpills` —
`internal/modules/valueindex/stream_compaction_bench_test.go`.

**Purpose:** Common-case allocation regression guard for #503's uniform flat-accumulation
rewrite of `mergeGroupsAtKey` (SPEC-VI-18's Design decision, plan.md): real S3 evidence (issue
#503 brainstorm, tenant 11638's `string` column) found the TYPICAL key's combined cross-file
span-ref fan-in has a median of ~9 spans — small enough that the per-key disk-spill threshold
(`ValueIndexMergeBufferSpillBytes`, 4 MiB) is never crossed. This fixture reproduces that shape
(200 keys per call, K=3 contributing groups each, 3 spans/group — 9 total per key across all
contributors) so the rewrite's allocation profile on exactly this never-spills path can be
checked against the pre-rewrite map-based implementation's own baseline.

**Baseline (captured 2026-07-15, BEFORE Phase 5's rewrite touched `mergeGroupsAtKey` —
specifically so this comparison has a real captured before/after pair, not a reconstructed one;
`go test -bench BenchmarkMergeGroupsAtKey_SmallKey_NeverSpills -benchmem -count=5
./internal/modules/valueindex/`, `benchstat` both sides):**

| Metric | before (map-based) | after (flat-accumulation) | delta |
|---|---|---|---|
| `sec/op` | 948.5µs | 1150.5µs | ~ (p=0.548, not significant) |
| `B/op` | 733.0Ki | 978.7Ki | +33.52% (p=0.008, significant) |
| `allocs/op` | 16.60k | 13.40k | −19.27% (p=0.008, significant) |

**Honest finding, not silently absorbed:** `B/op` increased significantly (fewer, larger flat-
record allocations replacing the old map-of-maps' many small per-entry allocations), while
`allocs/op` improved and `ns/op` showed no significant change. Per plan.md's own stated fallback
trigger ("if `allocs/op` or `ns/op` regresses meaningfully"), NEITHER of those two metrics
regressed, so the hybrid-cutover fallback (keeping the old map-based code as a below-threshold
fast path) is not warranted. The `B/op` increase is real and should not be hidden, but in
absolute terms it is small (~1.3 KB/key extra on a ~9-span key, versus the multi-MB-to-GB
pathological case this issue exists to fix) — accepted as a reasonable tradeoff, not a
regression requiring redesign.

**Regression threshold:** Flag if `allocs/op` or `ns/op` regresses meaningfully past the "after"
baseline above (13.40k allocs/op, no significant ns/op change from 1150.5µs) — per the accepted
tradeoff above, a FURTHER `B/op` increase beyond 978.7Ki/op is also worth flagging for review,
though it is not itself an automatic fail threshold given the already-accepted increase.

**Spec invariants tested:** SPEC-VI-18 (per-key disk-spill mechanism, common-case no-op path).

Back-ref: `internal/modules/valueindex/stream_compaction_bench_test.go:
BenchmarkMergeGroupsAtKey_SmallKey_NeverSpills, buildMergeGroupsAtKeyContributionForBench`. See
`SPECS.md` SPEC-VI-18. Issue #503.

Back-ref (updated): `internal/modules/valueindex/stream_compaction_bench_test.go:BenchmarkStreamCompactBucketFiles`.
