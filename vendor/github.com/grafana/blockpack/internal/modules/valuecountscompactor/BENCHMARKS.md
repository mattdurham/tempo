# valuecountscompactor — Benchmark Specifications

This document defines the benchmark suite for the `internal/modules/valuecountscompactor`
package. Baseline numbers serve as regression thresholds, per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `BENCH-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecounts/BENCHMARKS.md`'s own separate `BENCH-VC-N` sequence). IDs are
assigned in ascending order and never reused or renumbered.

Next free ID: **BENCH-VC-4**.

---

## BENCH-VC-1: BenchmarkMergeLevel
*Added: 2026-07-02*

**Measures:** `mergeLevel`'s full get→decode→Compact→encode→put→delete cost for one merge
job, including `Compact`'s group-sum merge path (not a pure concatenation).

**Setup:** 20 input L0 files, 300 records each (6,000 total decoded records), 50 overlapping
value-keys per column so multiple files share merge keys.

**Baseline (fresh — no prior numbers exist, this service is new; confirmed stable within normal
benchmark variance across repeated runs — B/op and allocs/op observed to vary by up to ~0.2%
run-to-run on identical hardware/software, likely from `valuecounts.NewID()`-generated
filename/key lengths perturbing string/slice allocation sizes downstream; ns/op varies more with
machine load):** 3,328,451 ns/op (~3.3 ms/op), 3,113,800 B/op, 12,287 allocs/op — **~519 B/decoded-record**
(3,113,800 ÷ 6,000 records). Same order of magnitude as team-brainstormer's ~300 B/record
back-of-envelope estimate (brainstorm.md Addendum 2) behind `DefaultMaxRecordsPerMerge`'s
sizing; moderately higher due to this workload's per-record allocation pattern (separate
`ColumnName`/`Value` backing byte slices per decoded record, as anticipated in that estimate).
See BENCH-VC-2 for the same per-record cost re-confirmed under the `MaxRecordsPerMerge` gate.

**Environment:** Go 1.26.1, linux/amd64, 13th Gen Intel Core i5-13500, `go test -bench <name>
-benchmem -run '^$'`, single machine, un-pinned CPU count — first baseline pending a proper
multi-run/pinned-CPU measurement.

**Regression threshold:** flag if `B/op` or `allocs/op` increases > 15% (these are the
peak-memory-relevant metrics for this benchmark, per `MaxRecordsPerMerge`'s OOM-guard purpose —
see BENCH-VC-2). `ns/op` is secondary; flag only if it roughly doubles, given single-run noise
on this baseline.

Back-ref: `internal/modules/valuecountscompactor/service_bench_test.go:BenchmarkMergeLevel`.

---

## BENCH-VC-2: BenchmarkMergeLevel_AtMaxRecordsPerMerge
*Added: 2026-07-02*

**Measures:** `mergeLevel`'s cost when the `MaxRecordsPerMerge` admission gate (SPEC-VC-2)
actually triggers partway through a batch, confirming the gate's peak-memory bound scales
linearly with `MaxRecordsPerMerge` as designed.

**Setup:** Same 20×300-record corpus as BENCH-VC-1, but `MaxRecordsPerMerge=3000` and
`CompactThresholdFiles=2` — the gate triggers exactly after 10 files (3,000 records = the
10th file's cumulative total), deferring the remaining 10 files.

**Baseline (confirmed stable within normal benchmark variance across repeated runs; B/op and
allocs/op observed to vary by up to ~0.2% run-to-run on identical hardware/software — see
BENCH-VC-1's note on `NewID()`-length-dependent allocation sizes — ns/op varies more with machine
load as expected):** 1,175,997 ns/op (~1.2 ms/op), 1,559,591 B/op, 6,155 allocs/op.

**Per-record memory cost — validation for `DefaultMaxRecordsPerMerge`'s sizing estimate
(plan.md Step 16.1):** dividing `B/op` by the number of records actually **processed** (3,000 —
10 files × 300 records/file, not `allocs/op`) gives **~520 B/processed-record**
(1,559,591 ÷ 3,000 ≈ 519.9), matching BENCH-VC-1's ~519 B/decoded-record (3,113,800 ÷ 6,000)
almost exactly. This is the correct per-record denominator; `B/op ÷ allocs/op` (1,559,591 ÷
6,155 ≈ 253) is bytes-per-*allocation*, not bytes-per-*record* (this benchmark's allocs/op is
roughly 2× the processed-record count, not 1:1), and is not the comparable quantity for
validating `DefaultMaxRecordsPerMerge`'s ~300 B/record RSS estimate (brainstorm.md Addendum 2).
**Conclusion for Step 16.1:** the measured ~520 B/record is the same order of magnitude as the
~300 B/record back-of-envelope estimate — same conclusion team-brainstormer's estimate was
used for (validates `DefaultMaxRecordsPerMerge=3_000_000` as a reasonable peak-RSS bound), the
actual per-record cost running somewhat above the estimate rather than below it.

**Environment:** Same as BENCH-VC-1 (Go 1.26.1, linux/amd64, i5-13500, `go test -bench <name>
-benchmem -run '^$'`, single machine, un-pinned CPU count — first baseline pending a proper
multi-run/pinned-CPU measurement).

**Regression threshold:** flag if `B/op` or `allocs/op` increases > 15%, or if the
per-processed-record cost (`B/op` ÷ records actually processed) diverges from BENCH-VC-1's
per-record cost by more than ~20% — a growing gap between the two would indicate the gate
itself is adding overhead disproportionate to the records it lets through.

Back-ref: `internal/modules/valuecountscompactor/service_bench_test.go:BenchmarkMergeLevel_AtMaxRecordsPerMerge`.

---

## BENCH-VC-3: BenchmarkClusterByTimeRange — recommended, not yet implemented (issue #494)
*Added: 2026-07-10*

**Recommendation:** `clusterByTimeRange` now runs on every `compactColumn` call (once per level
per column per compaction pass) — a strict behavioral upgrade from the prior lexicographic
key-sort it replaced, since it also does a linear greedy-walk over the sorted slice. Given how
much more frequently this executes relative to `mergeLevel` itself (every `compactColumn` call
vs. only calls that actually admit a winning cluster), a dedicated benchmark at a few realistic
per-level file counts (e.g. 10, 100, 1000 files) was recommended (plan.md Step A6.8) to confirm
the sort+walk cost stays negligible relative to `mergeLevel`'s own per-merge cost (BENCH-VC-1/
BENCH-VC-2) at realistic scale.

**Status: not yet implemented.** No `BenchmarkClusterByTimeRange` exists in this package as of
this spec-doc pass (verified via `grep -rn "^func Benchmark"` across this module — only
`BenchmarkMergeLevel`/`BenchmarkMergeLevel_AtMaxRecordsPerMerge` exist). This is flagged as an
open follow-up rather than silently marked done — `clusterByTimeRange`'s own algorithmic
complexity is `O(n log n)` (the sort) `+ O(n)` (the greedy walk) per call, which is unlikely to
dominate `mergeLevel`'s decode/`Compact`/encode cost at the file counts `CompactThresholdFiles`
realistically triggers on, but this has not been empirically confirmed via benchmark.

Back-ref: `internal/modules/valuecountscompactor/cluster.go:clusterByTimeRange`. `SPECS.md`
SPEC-VC-3.
