# valueindexcompactor — Test Specifications

This document defines the required tests for the `internal/modules/valueindexcompactor`
package. Each test is described with its scenario, setup, and expected assertions, per root
`SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, and independent of
`internal/modules/valueindex/TESTS.md`'s own `TEST-VI-N` sequence — each module's TESTS.md
numbers from 1). IDs are assigned in ascending order and never reused or renumbered.

Next free ID: **TEST-VI-20**.

---

## TEST-VI-1: mergeLevel merges overlapping keys and applies retention filtering
*Added: 2026-07-02*

**Scenario:** `mergeLevel`'s merge/dedup/retention-filter behavior (via
`StreamCompactBucketFiles`, SPEC-VI-1) produces correct merged, deduped, and
retention-filtered output, deletes all processed input keys, and reports accurate
Prometheus counters — driven end-to-end via `RunOnce`/`compactColumn` since `mergeLevel`
itself is unexported and not reachable from the external `_test` package. (Note: this test
was originally named `TestMergeLevel_StreamsInsteadOfDownloadingAllUpfront`; it was renamed
because it does not assert any streaming/bounded-memory property — that property is
regression-tested separately by `BenchmarkCompactBucketFiles_OldVsStreaming` in
`internal/modules/valueindex/stream_compaction_bench_test.go`.)

**Setup:** `TestMergeLevel_OverlappingKeysMergeAndRetentionFilters` seeds 3 level-0 input
files: two share an overlapping `(time, value)` key across different source files (must
merge into one group, unioning both live sources' `BucketBlockRef`s), and a third carries a
key from a source a `fakeExister` reports as dead (must be dropped by retention filtering).

**Assertions:**
- Exactly one output file is written at `level+1` with the expected merged/deduped/
  retention-filtered content.
- All 3 input keys are deleted.
- Prometheus counters `blockpack_value_index_compactor_entries_retained_total` /
  `entries_dropped_total` reflect accurate counts (2 retained, 1 dropped), gathered from a
  real `prometheus.Registry` via a local `gatherCounterValue` helper.

**Companion test:** `TestMergeLevel_PutFailureKeepsInputs_OverlappingKeys` re-verifies the
existing write-then-delete crash-safety property (NOTE-VI-017) isn't regressed by the
streaming rewrite — reuses the existing `putFailStore` pattern from
`TestRunOnce_PutFailureKeepsInputs` but exercises it against the new overlapping-key,
multi-source input shape introduced by this change.

**Spec invariants tested:** SPEC-VI-1.

Back-ref: `internal/modules/valueindexcompactor/service_test.go:TestMergeLevel_OverlappingKeysMergeAndRetentionFilters`,
`TestMergeLevel_PutFailureKeepsInputs_OverlappingKeys`.

---

## TEST-VI-2: mergeLevel writes zero output files when every source at a level is retention-dead
*Added: 2026-07-02*

**Scenario:** When every input file at a level has its sole source reported dead by the
`SourceExister`, the entire level collapses to zero live entries. Per NOTE-VI-046, the new
streaming path (`StreamCompactBucketFiles`) writes **no** output file in this case, unlike
the old `CompactBucketFiles` path which wrote a spurious empty V2 file and counted it as
written. This is a documented behavior change that must be regression-tested at the layer
that actually consumes the contract (`mergeLevel`), not only at the `valueindex` package
level (`TestStreamCompactBucketFiles_EdgeCases/"all refs dead"` in `stream_compaction_test.go`,
which drives `DecodeFilteredBucketFile`+`StreamCompactBucketFiles` directly).

**Setup:** `TestMergeLevel_AllSourcesDead_NoOutputWritten` seeds 3 level-0 input files
sharing the same `(time, value)` key from 3 distinct sources, all reported dead by a
`fakeExister`, then runs `RunOnce`.

**Assertions:**
- Zero output files exist anywhere in the store after the run (no file at `level+1`).
- All 3 input keys are deleted.
- `blockpack_value_index_compactor_files_written_total` is 0 while
  `blockpack_value_index_compactor_entries_dropped_total` is 3.

**Spec invariants tested:** SPEC-VI-2 (output semantics: `output` is not called when
nothing survives retention filtering).

Back-ref: `internal/modules/valueindexcompactor/service_test.go:TestMergeLevel_AllSourcesDead_NoOutputWritten`.

---

## TEST-VI-3: mergeLevel's peak decoded memory is bounded regardless of input file count
*Added: 2026-07-03*

**Scenario:** The disk-backed streaming redesign (SPEC-VI-1 corrected, NOTE-VI-052) bounds
`mergeLevel`'s peak decoded memory to roughly (concurrently-open iterators × one block),
independent of the number of input files (K) or their total size — replacing the pre-redesign
behavior where peak memory scaled linearly with K (every input's fully-decoded representation
stayed resident simultaneously).

**Setup:** `TestMergeLevel_MemoryBoundedRegardlessOfInputCount`
(`mergelevel_scaling_test.go`) builds K synthetic input files (2000 groups/file, split into
~100 blocks/file via `groupsPerBlock=20` — block-count matters here because the old whole-file
decoder ignores block granularity entirely, so a multi-block fixture is required to actually
exercise the new lazy per-block decode path) in a fake `IndexStore`, then measures peak live
heap (`runtime.MemStats.HeapAlloc`, sampled by a background goroutine forcing GC every 200µs)
during one `mergeLevel` call, for K=5 and K=50. A cumulative metric (`TotalAlloc` delta,
`testing.AllocsPerRun`) cannot demonstrate this property — see the BENCHMARKS.md BENCH-VI-1
methodology note — so peak-live-heap sampling is required instead.

**Assertions:** K=50's peak heap delta is less than 2x K=5's (measured: ~1.72x) — proving
peak memory does not scale linearly with input file count, unlike the pre-redesign baseline
(`TestMergeLevel_MemoryScalesWithInputCount_PreRedesign`, Phase 0, measured ~8.0x for the same
K=5→K=50 comparison under the old cumulative metric — see BENCHMARKS.md BENCH-VI-1 for why the
two numbers use different metrics and are not directly comparable to each other).

**Spec invariants tested:** SPEC-VI-1 (valueindexcompactor, corrected).

Back-ref: `internal/modules/valueindexcompactor/mergelevel_scaling_test.go:TestMergeLevel_MemoryBoundedRegardlessOfInputCount`.

---

## TEST-VI-4: A local-disk write failure while staging an input aborts the merge and leaves S3 untouched
*Added: 2026-07-03*

**Scenario:** The disk-backed redesign introduces `mergeLevel`'s first local-disk dependency
(`writeLocalTempInput`, staging each fetched input's bytes to a local temp file before
constructing a `valueindex.GroupIterator` against it). A local disk failure at this staging
step (e.g. an unwritable/nonexistent temp directory) is a new failure mode that must behave
like any other pre-`Put` failure: abort the merge cleanly, touch neither `Put` nor `Delete`,
and leave every S3 input in place for the next compaction pass to retry.

**Setup:** `TestMergeLevel_LocalDiskWriteFailureLeavesS3Untouched` (`diskstage_test.go`)
points `TMPDIR` at a nonexistent directory, seeds one input file in a call-counting fake
`IndexStore`, and calls `mergeLevel` directly.

**Assertions:** `mergeLevel` returns an error; the fake store's `Put`/`Delete` call counts are
both 0; the input key is still retrievable via `Get` afterward (untouched, safe for retry).

**Companion test:** `TestWriteLocalTempInput_WriteFailureReturnsError` (same file) unit-tests
`writeLocalTempInput` in isolation: a bad target directory returns a clean error and an empty
path, no panic.

**Spec invariants tested:** SPEC-VI-1 (valueindexcompactor, corrected) — the crash-safety
property preserved by the correction's own text.

Back-ref: `internal/modules/valueindexcompactor/diskstage_test.go:TestMergeLevel_LocalDiskWriteFailureLeavesS3Untouched`,
`TestWriteLocalTempInput_WriteFailureReturnsError`.

---

## TEST-VI-5: A genuinely corrupt (non-legacy) input aborts the whole merge and leaks no local temp files
*Added: 2026-07-03*

**Scenario:** Per NOTE-VI-052's documented corruption-handling behavior change: a decode
failure other than a header-magic mismatch (e.g. a corrupted compressed block payload) must
abort the entire merge rather than being silently skipped as a legacy file would be — this is
a deliberate divergence from the pre-redesign behavior, where `DecodeFilteredBucketFile`
treated every decode failure identically to a legacy-format skip.

**Setup:** `TestMergeLevel_ErrorMidMergeLeavesInputsUntouched` (`mergelevel_crash_test.go`)
seeds 2 good input files plus a 3rd whose compressed block payload has been corrupted (one
byte flipped inside the block body, header/footer magic untouched), in a call-counting fake
`IndexStore`, then calls `mergeLevel` directly.

**Assertions:** `mergeLevel` returns an error; the fake store's `Put`/`Delete` call counts are
both 0; all 3 input keys (including the 2 good ones) are still retrievable via `Get`
afterward; a `vi-merge-*.tmp` glob of `os.TempDir()` taken before and after the call is
identical (no local temp file — input or output — leaks on the error path, proving `defer`
cleanup fired).

**Spec invariants tested:** SPEC-VI-1 (valueindexcompactor, corrected).

Back-ref: `internal/modules/valueindexcompactor/mergelevel_crash_test.go:TestMergeLevel_ErrorMidMergeLeavesInputsUntouched`.

---

## TEST-VI-6: CompactMaxInputFiles caps a merge's input file count, independent of the byte cap
*Added: 2026-07-03*

**Scenario:** `compactColumn`'s new file-count cap (SPEC-VI-2) must limit a single merge to at
most `CompactMaxInputFiles` inputs regardless of `CompactBatchBytes`, and must floor to a
minimum of 2 rather than stall on a pathologically small configured cap.

**Setup (primary):** `TestRunOnce_MaxInputFilesCapsIndependentOfBatchBytes` seeds 500 tiny L0
files for one column with `CompactBatchBytes: 0` (no byte cap, isolating the file-count cap's
effect) and `CompactMaxInputFiles: 50`, using a *distinct* value per input file (`value-<i>`)
so the cap is directly observable in the merged output's decoded group count — an
identical-value fixture would hide an off-by-N cap bug behind dedup.

**Assertions (primary):** after one `RunOnce`, exactly 450 L0 inputs remain untouched, exactly
one new L1 output exists, and its decoded group count is exactly 50 (proving the merge
consumed exactly `CompactMaxInputFiles` distinct-value inputs, no more, no less).

**Companion test (edge case):** `TestRunOnce_MaxInputFilesFloorsAtTwo` sets the pathological
`CompactMaxInputFiles: 1` against 8 seeded L0 files (`CompactThresholdFiles: 8`) and asserts
the merge still proceeds against a floored batch of 2 (6 inputs remain + 1 new L1 output),
not a stuck 1-file no-op.

**Spec invariants tested:** SPEC-VI-2.

Back-ref: `internal/modules/valueindexcompactor/run_concurrency_test.go:TestRunOnce_MaxInputFilesCapsIndependentOfBatchBytes`,
`TestRunOnce_MaxInputFilesFloorsAtTwo`.

---

## TEST-VI-7: effectiveBatchBytes scales CompactBatchBytes down by CompactConcurrency, with guards
*Added: 2026-07-03*

**Scenario:** `effectiveBatchBytes()` (SPEC-VI-3) must scale proportionally with
`CompactConcurrency`, while never manufacturing a cap out of an explicit "no cap" (`0`), never
truncating a real cap to `0` via integer-division floor, and never dividing by zero if called
before `withDefaults()` normalization.

**Setup:** `TestEffectiveBatchBytes_Table` is a table test constructing a bare `Service{cfg:
...}` (no store/registry needed — this is a pure function test) over five cases:
`{1000, 1} → 1000` (exact identity at concurrency 1), `{1000, 4} → 250` (proportional scaling),
`{1000, 0} → 1000` (defensive clamp when concurrency isn't yet defaulted), `{0, 8} → 0`
(explicit no-cap survives scaling), `{3, 10} → 1` (floor-division-to-zero guard).

**Assertions:** each case's computed `effectiveBatchBytes()` matches the table's expected
value exactly.

**Spec invariants tested:** SPEC-VI-3.

Back-ref: `internal/modules/valueindexcompactor/concurrency_test.go:TestEffectiveBatchBytes_Table`.

---

## TEST-VI-8: Run() at default concurrency never overlaps merges; CompactConcurrency: 4 actually overlaps within its bound
*Added: 2026-07-03*

**Scenario:** This is the core regression/proof pair for `Run()`'s `errgroup`-based
restructuring. `TestRun_ConcurrencyDefaultOne_NeverOverlaps` is a regression proof designed to
pass both *before and after* the restructuring landed (proving "default concurrency behaves
exactly like the old fully-sequential loop" is genuinely true, not just assumed) — it was
verified to already pass against the pre-restructuring code, then re-verified to still pass
after. `TestRun_ConcurrencyFour_ActuallyOverlaps` proves the opposite direction: that raising
`CompactConcurrency` really does allow concurrent dispatch, bounded by `SetLimit`.

**Setup:** both tests use a shared `peakConcurrencyStore` harness (wraps `fakeStore`,
instruments `List` — the first call each dispatched closure makes — to track the peak number
of concurrently-overlapping calls via an atomic compare-and-swap loop, with a fixed sleep to
widen the observation window) and `seedIndependentColumns` (≥6 independently-hashed columns,
each above `CompactThresholdFiles`). Both start `Run(ctx)` in a goroutine with a short
`CompactInterval`, poll via `require.Eventually` until every seeded column has merged to L1,
then cancel and assert `context.Canceled`.

**Assertions:** `TestRun_ConcurrencyDefaultOne_NeverOverlaps` (`CompactConcurrency` unset/0) —
peak observed overlap `== 1`. `TestRun_ConcurrencyFour_ActuallyOverlaps` (`CompactConcurrency:
4`, 8 seeded columns, longer per-call sleep to widen the overlap window) — peak `> 1` (proves
real concurrency) **and** peak `<= 4` (proves `SetLimit` actually bounds it).

**Spec invariants tested:** SPEC-VI-2 (`CompactConcurrency` semantics), SPEC-VI-4 (colDir
exclusivity is not violated by either configuration).

Back-ref: `internal/modules/valueindexcompactor/run_concurrency_test.go:TestRun_ConcurrencyDefaultOne_NeverOverlaps`,
`TestRun_ConcurrencyFour_ActuallyOverlaps`.

---

## TEST-VI-9: A panic in one column's merge does not crash the process or block other concurrent merges
*Added: 2026-07-03*

**Scenario:** The most safety-critical property in this task (SPEC-VI-5). A panic inside one
column's dispatched merge must not crash the process, must not prevent any other
concurrently-dispatched column from completing, must leave the poisoned column's inputs
untouched for retry, and must not alter `Run`'s own control-flow return value.

**Setup:** `TestRun_PanicInOneMergeDoesNotCrashOthersOrProcess` uses a `panicOnKeyStore`
(wraps `fakeStore`, panics inside `List` whenever the prefix contains one specific "poison"
column's hash) alongside ≥3 other independently-seeded, non-poisoned columns, all above
threshold, `CompactConcurrency: 4`, a real `prometheus.Registry`. Starts `Run(ctx)` in a
goroutine, polls until every non-poison column reaches L1, then cancels.

**Assertions:** (a) the poll loop actually observes every non-poison column completing — this
would time out if the panic had genuinely crashed the process/goroutine group, so success here
is itself proof of isolation; (b) the poison column's L0 inputs are still present afterward
(panic aborted only that merge, leaving inputs for the next pass's retry, mirroring
`TestMergeLevel_ErrorMidMergeLeavesInputsUntouched`'s ordinary-error case); (c) `Run` still
returns `context.Canceled` after cancellation, not any panic-derived error; (d)
`errors_total{op="panic"}` (gathered via `gatherCounterValue`) is `>= 1`.

**Spec invariants tested:** SPEC-VI-5, including the specific "recover() branch returns nil,
not a synthesized error" mechanism SPEC-VI-5 documents — this test is what proves that
mechanism is necessary: without it, the poison column's panic would cancel `gctx` and this
test's non-poison columns would never reach L1, timing the `require.Eventually` out.

Back-ref: `internal/modules/valueindexcompactor/run_concurrency_test.go:TestRun_PanicInOneMergeDoesNotCrashOthersOrProcess`.

---

## TEST-VI-10: No colDir is ever processed concurrently with itself, including across a lap boundary
*Added: 2026-07-03*

**Scenario:** A construction-correctness test for SPEC-VI-4's `g.Wait()`-before-rebuild
design. It is specifically constructed so it would fail under the brainstorm's explicitly-
rejected alternative (a plain unbounded worker-pool-with-continuous-dispatch that never waits
for a slow prior-lap goroutine before starting the next lap's work) — this was sanity-checked
during design rather than assumed.

**Setup:** `TestRun_NeverProcessesSameColDirConcurrentlyAcrossLapBoundary` uses an
`exclusivityStore` (wraps `fakeStore`; tracks, per colDir prefix, whether `List` is currently
"active" via a mutex-guarded map, recording a violation if `List` is called for a prefix
already marked active; sleeps a staggered, deterministically-hashed duration per prefix so
different columns finish at different points within a lap, exercising the actual race window).
Seeds 6 independent columns with enough files to stay above threshold across many laps,
`CompactConcurrency: 4`, short `CompactInterval`. Runs `Run(ctx)` until total `List` call count
exceeds twice the column count (proving at least 2 full laps occurred), then cancels.

**Assertions:** the violations slice recorded by `exclusivityStore` is empty — no colDir was
ever `List`-ed while another call for that same colDir was still active, across any lap
boundary.

**Spec invariants tested:** SPEC-VI-4.

Back-ref: `internal/modules/valueindexcompactor/run_concurrency_test.go:TestRun_NeverProcessesSameColDirConcurrentlyAcrossLapBoundary`.

---

## TEST-VI-11: backlogL0Files and configuredConcurrency gauges are populated correctly
*Added: 2026-07-03*

**Scenario:** SPEC-VI-6's `backlogL0Files` contract (populated from data `compactColumn`
already has in hand, labeled by tenant) and the new `configuredConcurrency` gauge (reflects
the post-defaulting `CompactConcurrency` a `Service` was constructed with) must both report
correct values through a real `prometheus.Registry`.

**Setup (backlog):** `TestCompactColumn_PopulatesBacklogL0Files` seeds 3 L0 files for one
column, deliberately below `CompactThresholdFiles: 8` so the backlog is observable and stable
(not compacted away mid-assertion), with a real `Registerer`. Runs `RunOnce` once.

**Assertions (backlog):** `blockpack_value_index_compactor_backlog_l0_files{tenant="t1"} == 3`
(via `testutil.ToFloat64` against `svc.metrics.backlogL0Files.WithLabelValues("t1")`).

**Setup (configured concurrency):** `TestNewService_SetsConfiguredConcurrencyGauge`
constructs a `Service` with `CompactConcurrency: 4` and a real `Registerer` — no `Run`/
`RunOnce` call needed.

**Assertions (configured concurrency):**
`blockpack_value_index_compactor_configured_concurrency == 4` immediately after `NewService`
returns.

**Spec invariants tested:** SPEC-VI-6 (backlog), SPEC-VI-2 (configured concurrency reflects
the post-defaulting `CompactConcurrency` value).

Back-ref: `internal/modules/valueindexcompactor/metrics_test.go:TestCompactColumn_PopulatesBacklogL0Files`,
`TestNewService_SetsConfiguredConcurrencyGauge`.

---

## TEST-VI-12: merges_in_flight gauge tracks genuine peak concurrency and returns to zero after drain
*Added: 2026-07-03*

**Scenario:** The `merges_in_flight` gauge (incremented/decremented around each dispatched
merge in `Run()`'s `g.Go` closure) must actually reflect concurrent merges while they are in
flight, not just be wired up without observable effect, and must return to exactly `0` once
`Run` is canceled and every in-flight goroutine has drained.

**Setup:** `TestRun_MergesInFlightGaugeTracksPeakConcurrency` reuses the `peakConcurrencyStore`
harness and `seedIndependentColumns` shape from TEST-VI-8 (8 columns, `CompactConcurrency: 4`,
a real `prometheus.Registry`). While polling for all seeded columns to reach L1, it also
samples the gauge (via a `gatherGaugeValue` helper added alongside the existing
`gatherCounterValue`) on every poll iteration and tracks the maximum observed value.

**Assertions:** the maximum sampled value is `> 1` (proves the gauge was observed reflecting
genuine overlap, not just incremented-then-immediately-decremented with no observable window);
after cancellation and full drain, the gauge reads exactly `0`.

**Spec invariants tested:** SPEC-VI-4 (concurrency actually happens, bounded correctly) as
observed through this specific metric.

Back-ref: `internal/modules/valueindexcompactor/run_concurrency_test.go:TestRun_MergesInFlightGaugeTracksPeakConcurrency`.

---

## TEST-VI-13: `compactColumn` dispatches trace-index colDirs to `mergeTraceLevel` without misdetecting them as legacy junk
*Added: 2026-07-04*

**Scenario:** A trace-index colDir's files (no `vbg2Magic` framing, by design) must never be
deleted by the `vbg2Magic` purge loop, must be routed to `mergeTraceLevel`, and the merged
output must be a valid, one-level-higher `TraceGroup` file with correctly merged spans.

**Setup:** `TestCompactColumn_TraceIndexColDirDispatchesToTraceMerge` puts two hand-built
`TraceGroup` files (same `TraceID`, disjoint spans) under a trace-index colDir via an
`orderGuardStore` wrapper that fails the test if `Delete` is ever called before any `Get` (the
Finding-2 regression guard), runs `RunOnce`, and decodes the result.

**Assertions:** `orderGuardStore.deletedBeforeGet` is false; exactly one output file exists at
level 1; neither input key survives; the decoded output has one `TraceGroup` with both spans
and `TimeSec` equal to the minimum across inputs.

**Spec invariants tested:** SPEC-VI-7 (dispatch condition, magic-purge skip).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestCompactColumn_TraceIndexColDirDispatchesToTraceMerge`.

---

## TEST-VI-14: The real `SourceExister` → `cachingRefChecker` → `RefChecker` chain drops stale spans in a trace merge
*Added: 2026-07-04*

**Scenario:** proves the widened `MergeTraceGroups` signature (SPEC-VI-6, `valueindex/SPECS.md`)
works end-to-end through the compactor's real retention-check chain, not just a bespoke test
closure calling `MergeTraceGroups` directly.

**Setup:** `TestMergeTraceLevel_StaleSourceDroppedViaRealRefChecker` builds two input files for
the same trace, one span with a live `SourceRef`, one with a dead one (`newFakeExister`), runs
`RunOnce`.

**Assertions:** the merged output retains exactly the live span; the dead span is gone.

**Spec invariants tested:** SPEC-VI-6, SPEC-VI-7.

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestMergeTraceLevel_StaleSourceDroppedViaRealRefChecker`.

---

## TEST-VI-15: A `Put` failure during a trace-index merge leaves every input untouched
*Added: 2026-07-04*

**Scenario:** direct structural mirror of `TestMergeLevel_PutFailureKeepsInputs_OverlappingKeys`
for the trace-index path — write-then-delete ordering must hold even on failure.

**Setup:** `TestMergeTraceLevel_PutFailureKeepsInputs` wraps the store's `Put` to always fail,
runs `RunOnce` against two valid input files.

**Assertions:** `RunOnce` returns an error; both input keys are still present in the store
afterward.

**Spec invariants tested:** SPEC-VI-7 (write-then-delete crash safety).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestMergeTraceLevel_PutFailureKeepsInputs`.

---

## TEST-VI-16: A corrupt trace-index input is skipped, not deleted, and does not abort the merge
*Added: 2026-07-04*

**Scenario:** one unreadable input file among otherwise-valid inputs must not abort the whole
column's compaction, and must not be deleted.

**Setup:** `TestMergeTraceLevel_CorruptInputSkippedNotAborted` puts one valid `TraceGroup` file
and one file containing garbage bytes under the same colDir, runs `RunOnce`.

**Assertions:** the corrupt key is still present afterward; the valid key is gone; a merged
output exists containing exactly the valid file's trace.

**Spec invariants tested:** SPEC-VI-7 (decode-failure-skips-one-file contract),
NOTE-VI-066 (corrupt-input-left-in-place rationale).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestMergeTraceLevel_CorruptInputSkippedNotAborted`.

---

## TEST-VI-17: Zero output files are written when every span across every trace-index input is retention-dead
*Added: 2026-07-04*

**Scenario:** mirrors the `BucketGroup` path's `TestMergeLevel_AllSourcesDead_NoOutputWritten`
for the trace-index path — no spurious empty output file, but fully-stale (successfully
decoded) inputs are still deleted.

**Setup:** `TestMergeTraceLevel_AllSourcesDead_NoOutputWritten` puts two input files whose only
spans reference a `SourceRef` reported dead by a fake exister, runs `RunOnce`.

**Assertions:** the store ends up empty (no output written, both stale inputs deleted).

**Spec invariants tested:** SPEC-VI-7 point 4 (zero-output guard).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestMergeTraceLevel_AllSourcesDead_NoOutputWritten`.

---

## TEST-VI-18: `CompactMaxInputFiles` caps a trace-index colDir's batch identically to a standard colDir
*Added: 2026-07-04*

**Scenario:** the existing file-count cap (applied upstream of the format-dispatch branch) must
apply identically regardless of which merge function the dispatch routes to.

**Setup:** `TestCompactColumn_TraceIndexColDir_RespectsCompactMaxInputFiles` seeds 5 trace-index
files with `CompactMaxInputFiles: 3`, runs `RunOnce`.

**Assertions:** exactly `1 + (5 - 3)` keys remain — one merged output plus the leftover
unconsumed inputs from the capped batch.

**Spec invariants tested:** SPEC-VI-7 ("the same rules as any other column's" capping claim).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestCompactColumn_TraceIndexColDir_RespectsCompactMaxInputFiles`.

---

## TEST-VI-19: A hard `Get` failure during a trace-index merge surfaces as an error, unlike a decode failure
*Added: 2026-07-04*

**Scenario:** distinguishes "the store itself failed" (must abort, surfaced as an error) from
"one file's payload is corrupt" (TEST-VI-16, must NOT abort) — these are deliberately different
failure postures for the same merge function.

**Setup:** `TestMergeTraceLevel_GetErrorAbortsMerge` makes the store's `Get` fail for one of two
input keys, runs `RunOnce`.

**Assertions:** `RunOnce` returns an error.

**Spec invariants tested:** SPEC-VI-7 point 2 (hard Get failure vs. decode failure distinction).

Back-ref: `internal/modules/valueindexcompactor/traceindex_dispatch_test.go:TestMergeTraceLevel_GetErrorAbortsMerge`.
