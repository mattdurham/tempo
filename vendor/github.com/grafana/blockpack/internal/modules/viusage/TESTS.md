# viusage — Test Specifications

This document defines the required tests for `internal/modules/viusage` AND, per this
module's role as #496's overall test-plan record, every other test file #496 (blockpack/#496:
VI dedicated columns + query-usage-driven backfill) added outside this package's own
directory — root `blockpack` (`valueindex_policy_test.go`, `valueindex_watermark_test.go`) and
`internal/modules/vibuilder` (`watermark_test.go`, `builder_watermark_test.go`) — because
#496's feature boundary spans multiple packages for import-cycle reasons documented in
`NOTES.md` NOTE-VIUSAGE-7, and a reader wanting the full #496 test plan should not need to
hunt across four different TESTS.md files to find it.

## ID convention

Entries use the module-local, sequential prefix `TEST-VIUSAGE-N`, independent of the shared
`TEST-VI-N` space maintained by `valueindex`/`valueindexcompactor`/`valueindexconsumer`'s own
TESTS.md files (see `SPECS.md`'s ID-convention section). IDs are assigned in ascending order
and never reused or renumbered.


## TEST-VIUSAGE-1 through 4: `BackfillState.CoversRange` / `ColumnWatermark.CoversRange` boundary conditions, plus their cross-implementation parity test
*Added: 2026-07-10. Updated: 2026-07-10 (task #117 landed the parity test flagged below as a recommended follow-up; the gap is now resolved.)*

**Scenario:** the R7 coverage-check primitive's 4 branches (SPEC-VIUSAGE-2 /
`vibuilder/SPECS.md` SPEC-VB-4): `Done` always covers; never-`Triggered` never covers;
in-progress covers only from the watermark forward; the exact `minSec == WatermarkSec`
boundary covers (not a strict `>`). Because `viusage.BackfillState.CoversRange` and
`vibuilder.ColumnWatermark.CoversRange` are two independently-maintained copies of the
identical logic (NOTE-VIUSAGE-7), the real risk is not merely "does each copy pass its own
tests" but "can the two copies silently diverge from each other" — task #117 added a
dedicated parity test for exactly this risk, resolving the gap this entry originally flagged.

**Setup/Assertions:**
- `TestColumnWatermark_CoversRange_DoneAlwaysTrue`, `_NeverTriggeredAlwaysFalse`,
  `_InProgressCoversOnlyFromWatermarkForward`, `_InProgressExactlyAtWatermarkBoundaryCovers` —
  direct-call tests against `vibuilder.ColumnWatermark.CoversRange` covering all 4 branches,
  including the explicit boundary-inclusive case.
- `TestCoversRange_Parity_BackfillStateAndColumnWatermark` (`viusage/coversrange_parity_test.go`,
  task #117) — a 9-case table test that constructs BOTH `viusage.BackfillState` and
  `vibuilder.ColumnWatermark` from the SAME `{watermarkSec, minSec, maxSec, done, triggered}`
  input for each case, asserts BOTH match the case's expected `want` value, AND asserts the two
  implementations' own results equal each OTHER (`assert.Equal(t, gotBS, gotWM, ...)`) —
  catching a divergence even in the hypothetical case where both copies agreed with each other
  but disagreed with the intended contract in the same wrong way (an ordinary equality-to-`want`
  check alone would not catch that). Cases cover all 4 SPEC-VIUSAGE-2/SPEC-VB-4 branches plus
  additional boundary variations (one second before/after the watermark, a degenerate
  zero-width range, a wide `Done` range) beyond the 4 `vibuilder`-only tests above.

**Resolution of the originally-flagged coverage gap:** this entry originally noted
`viusage.BackfillState.CoversRange` had no DIRECT dedicated boundary test of its own (only
`vibuilder.ColumnWatermark.CoversRange` did) and recommended a follow-up parity/mirror test.
Task #117's `TestCoversRange_Parity_BackfillStateAndColumnWatermark` satisfies that
recommendation directly — it exercises `viusage.BackfillState.CoversRange` on every one of its
9 cases, not merely as an indirect side effect of `trigger_test.go`/`backfill_test.go`'s
scenarios as before. The asymmetry is resolved: both copies now have direct, dedicated
boundary-condition coverage, plus a cross-check that they agree with each other.

**Spec invariants tested:** SPEC-VIUSAGE-2, `vibuilder/SPECS.md` SPEC-VB-4 (both copies, plus
their mutual agreement).

Back-refs: `internal/modules/vibuilder/watermark_test.go:TestColumnWatermark_CoversRange_
DoneAlwaysTrue,_NeverTriggeredAlwaysFalse,_InProgressCoversOnlyFromWatermarkForward,
_InProgressExactlyAtWatermarkBoundaryCovers`; `internal/modules/viusage/
coversrange_parity_test.go:TestCoversRange_Parity_BackfillStateAndColumnWatermark`.

---

## TEST-VIUSAGE-5 through 8: `RecordUseAndMaybeTrigger` — unconditional first-use trigger, R5/R8 lifecycle
*Added: 2026-07-10. Updated: 2026-07-11 (task #154 removed the repeated-use threshold in favor of
an unconditional first-use trigger — see SPEC-VIUSAGE-8; the 3 threshold/window-specific tests this
range originally described (`_BelowThresholdNoTrigger`, `_ExactlyAtThresholdTriggers`,
`_UsesOutsideWindowNotCounted`) no longer exist, since there is no threshold or window left to test.
This range now covers 4 tests, not the original 6.)*

**Scenario:** the current decision table SPEC-VIUSAGE-8 documents: unconditional trigger on the
first-ever recorded use, R5's never-re-trigger-once-triggered rule, R8's expired-lease re-acquire
(crash self-heal), and R8's concurrent-caller mutual exclusion (exactly one winner).

**Setup/Assertions:**
- `TestRecordUseAndMaybeTrigger_FiresOnFirstUse_NoThresholdField` — the first-ever recorded use of a
  never-seen column always fires (`ShouldBackfill=true`, `Entry.Backfill.Triggered=true`), with no
  `Threshold`/`WindowSeconds` field on `TriggerConfig` to gate it.
- `TestRecordUseAndMaybeTrigger_AlreadyTriggeredNeverReTriggers` — R5: once `Triggered=true` with an
  active, unexpired lease, further uses are a no-op (`ShouldBackfill=false`); `LeaseExpiresAt` does
  not change across 5 subsequent calls.
- `TestRecordUseAndMaybeTrigger_ExpiredLeaseAllowsReAcquire` — R8's crash-self-heal: a lease with
  `LeaseExpiresAt` in the past (simulating a crashed backfill worker) allows a fresh call to
  re-acquire the lease and return `ShouldBackfill=true` again for an already-`Triggered`,
  not-yet-`Done` column.
- `TestRecordUseAndMaybeTrigger_ConcurrentCallersOnlyOneWinsLease` — 5 goroutines each individually
  cross the (now unconditional) first-use trigger on their own first call, against a shared store
  with real conditional-PUT-conflict semantics; asserts exactly 1 of the 5 gets
  `ShouldBackfill=true` and the registry ends up with exactly 1 entry, `Triggered=true` — proving
  the lease genuinely serializes concurrent triggering rather than each caller independently
  believing it won.

**Spec invariants tested:** SPEC-VIUSAGE-8 (supersedes SPEC-VIUSAGE-3 for the trigger-condition
portion; the lease mechanics both spec entries describe are unchanged).

Back-ref: `internal/modules/viusage/trigger_test.go` (all 4 tests above).

---

## TEST-VIUSAGE-12 through 18: `Registry` — load/retry/lease-renewal contract
*Added: 2026-07-10. Updated: 2026-07-11 (task #154: `TestRegistry_RecordUse_AppendsTimestamp` and
`_BoundedTimestampWindow` were deleted along with `Registry.recordUse` itself, which no longer
exists once `Entry.UseTimestamps` was removed — see SPEC-VIUSAGE-8. The distinct-column-types test
was renamed from `TestRegistry_RecordUse_DistinctColumnTypesTrackedIndependently` to
`TestRegistry_DistinctColumnTypesTrackedIndependently`, now exercised via a plain trigger call
instead of the deleted `recordUse`. This range now covers 6 tests, not the original 7.)*

**Scenario:** the conditional-PUT-with-retry discipline (SPEC-VIUSAGE-4): empty-index load,
conflict-retry success and exhaustion, lease renewal, and independent per-`(name, colType)`
tracking.

**Setup/Assertions:**
- `TestRegistry_Load_EmptyWhenNotFound` — a missing index object (`ObjectStore.Get` returns
  `ErrNotFound`) returns an empty, non-error result: `(nil entries, "" etag, nil error)`.
  **Updated 2026-07-10 (go-presubmit.md CRITICAL Fix C):** this test's fake store
  (`registryMemStore`) now signals a miss via the typed `ErrNotFound` sentinel, not a bare
  nil-error-with-empty-shape return — see TEST-VIUSAGE-38/39 below for the regression pair
  that pins the distinction between this legitimate case and a real error sharing the same
  `(nil, "")` shape.
- `TestRegistry_ConditionalPutConflictRetries` — a store that returns `ErrConflict` N times
  (N < 5) then succeeds → the retry loop succeeds on the eventual non-conflicting attempt.
- `TestRegistry_ConditionalPutExhaustsRetries` — a store that always conflicts → a typed
  "exceeded retries" error after exactly 5 attempts.
- `TestRegistry_RenewLease_UpdatesExpiry` — `RenewLease` pushes `LeaseExpiresAt` forward on an
  existing entry via the same retry discipline.
- `TestRegistry_RenewLease_NotFoundReturnsError` — renewing a lease for a nonexistent entry
  errors (mirrors cube's `UpdateWatermarks` "not found" behavior — `createIfMissing=nil`).
- `TestRegistry_DistinctColumnTypesTrackedIndependently` — the same column name observed under
  two distinct `ColumnType`s tracks as two independent `Entry` records, never collapsed into one.

**Spec invariants tested:** SPEC-VIUSAGE-4.

Back-ref: `internal/modules/viusage/registry_test.go` (all 6 tests above; uses
`registryMemStore`, an in-memory `ObjectStore` mirroring `internal/modules/cube`'s own test
store shape, independently written per R1).

---

## TEST-VIUSAGE-19 through 30: `BackfillEngine`/`Run` — ordering, allowlist scoping, file-layout parity, watermark progression across blocks, progress/cancellation, real-write-path round trip
*Added: 2026-07-10. Updated: 2026-07-10 (test now lives in root `blockpack` alongside `BackfillEngine` itself, per SPEC-VIUSAGE-5's package-placement note; task #116 added `TestBackfillEngine_WatermarkValuesAcrossMultiBlockRun`, extending this range from 19-29 to 19-30.)*

**Scenario:** SPEC-VIUSAGE-5's full contract: newest-first processing, column-scoped
extraction (proving `ExtractValueIndexEntriesForColumns` is genuinely used, not a full
extraction), the load-bearing exact-file-key-format claim, per-block progress reporting AND
its exact `WatermarkSec` values across a real multi-block run, context cancellation, the
empty-range terminal case, column-TYPE filtering, real-write-path round-trip (the mandatory
TEST-VI-22/EX-36-class test for this module), and error propagation from both `BlockFetcher`
methods.

**Setup/Assertions:**
- `TestBackfillEngine_ProcessesBlocksNewestFirst` — a fake `BlockFetcher` returning blocks in
  a known order; asserts `Run` processes them in that exact order (the fetcher owns the
  ordering guarantee per SPEC-VIUSAGE-5; this test proves `Run` does not itself re-sort or
  reverse it).
- `TestBackfillEngine_OnlyExtractsAllowedColumn` — a fake block exposing 3 columns; asserts
  only the target column's entries reach the written output, proving
  `ExtractValueIndexEntriesForColumns`'s allowlist is genuinely applied, not a full
  unfiltered extraction.
- `TestBackfillEngine_WritesViaExistingL0FileLayout` — **load-bearing**: asserts the written
  object key matches EXACTLY the `<tenant>/<indexPrefix>/<colHash>/<typeName>/L0-...`
  convention `flushAndPutL0`/`WriteValueIndexL0` use. If this key shape drifts even slightly,
  `valueindexcompactor`'s own file-discovery parsing would silently miss these files — this
  assertion is the entire proof behind R6's "zero special-casing" claim, not decorative.
- `TestBackfillEngine_ProgressFnCalledPerCompletedUnit` — `progressFn` is called once per
  fetched block (never batched).
- `TestBackfillEngine_WatermarkValuesAcrossMultiBlockRun` (task #116) — the EXACT-VALUE
  companion to the test above: 3 real blocks (T3/T2/T1, each 1h/2h/3h old respectively, each
  with exactly one span so `blockCoverageRangeSec` collapses to that span's own second),
  fetched newest-first. Asserts the running-minimum watermark advances correctly block-by-block
  (`progress[0].WatermarkSec == T3's own second`, `progress[1].WatermarkSec == T2's own
  second` — the OLDER of the two, proving the running-minimum, not running-maximum or
  last-seen, semantics) AND that the FINAL block's `Done=true` call forces `WatermarkSec` to
  the full window's start (`nowSec - 48h`), NOT merely T1's own second — proving the
  window-closing behavior SPEC-VIUSAGE-5 describes ("closing any rounding gap between the last
  block's own MinStart and the window's configured edge") with real numbers, not just a
  boolean `Done` check. Also asserts every progress call's `WindowStartSec`/`WindowEndSec`
  stay constant across all 3 calls (the window itself does not drift mid-run).
- `TestBackfillEngine_StopsOnCtxCancel` — a canceled `ctx` stops processing before the next
  block fetch; `Run` returns `ctx.Err()`.
- `TestBackfillEngine_EmptyBlockRangeWritesNothingReturnsNil` — zero blocks in range →
  `progressFn` called exactly once with `Done=true`, `Run` returns `nil`, nothing written.
- `TestBackfillEngine_FiltersToEntryColumnType` — a column observed under a DIFFERENT
  `ColumnType` than `entry.ColumnType` in a fetched block is excluded from output — proves the
  type-filter half of SPEC-VIUSAGE-5's "belongs to a distinct Entry/key" rule, distinct from
  the name-allowlist filter `TestBackfillEngine_OnlyExtractsAllowedColumn` proves.
- `TestBackfillEngine_RealWriteThenQuery_RoundTrips` — the mandatory real-write-path test
  (TEST-VI-22/EX-36 convention, inherited by this new sibling module per that policy's own
  stated scope): writes a REAL block, runs the backfill engine against it (no hand-built
  `ValueIndexEntry`/`LookupResult` fixtures), then runs a real VI query against the written
  output and asserts the expected span is found.
- `TestBackfillEngine_ListBlocksErrorPropagates` / `_FetchBlockErrorPropagates` — a
  `BlockFetcher` error from either method surfaces as `Run`'s own returned error, wrapped with
  context (not swallowed or silently skipped).
- `TestNewBackfillEngine_DefaultsApplied` — `WindowSeconds`/`Workers`/`Now` all receive their
  documented defaults (48h, 4, `time.Now`) when the caller supplies a zero-valued
  `BackfillConfig` field.

**Spec invariants tested:** SPEC-VIUSAGE-5.

Back-ref: `valueindex_backfill_test.go` (root package `blockpack_test` — moved here from
`internal/modules/viusage/backfill_test.go` alongside `BackfillEngine` itself; all 12 tests
above).

---

## TEST-VIUSAGE-31 through 33: `Config`/`MaybeRecordUseAndMaybeTrigger` — R12 safety valve
*Added: 2026-07-10*

**Scenario:** SPEC-VIUSAGE-6's binding requirement: the safety valve disables usage-tracking
registry I/O ENTIRELY when off, defaults to enabled, and threads correctly alongside A1's
write-path `ColumnPolicy`.

**Setup/Assertions:**
- `TestConfig_DedicatedColumnsEnabled_DefaultsToTrue` — `DefaultConfig()` returns
  `{DedicatedColumnsEnabled: true}`.
- `TestConfig_DedicatedColumnsDisabled_NoUsageTrackingEngaged` — with
  `DedicatedColumnsEnabled: false`, `MaybeRecordUseAndMaybeTrigger` performs ZERO registry
  I/O (asserted via a call-counting fake `ObjectStore` — no `Get`, no `ConditionalPut`) even
  when a non-dedicated column is "queried" via the test harness, and returns a zero
  `TriggerResult`.
- `TestConfig_DedicatedColumnsEnabled_UsageTrackingProceeds` — the enabled-path control: the
  same harness with `DedicatedColumnsEnabled: true` DOES perform registry I/O and can trigger
  normally — proves the disabled case above is caused by the gate specifically, not some
  other harness bug that would trivially "pass" the disabled test for the wrong reason.
- `TestConfig_DedicatedColumnsEnabled_ThreadsIntoColumnPolicy` (`config_policy_threading_
  test.go`) — confirms the SAME boolean that gates `MaybeRecordUseAndMaybeTrigger` is also
  what a caller threads into `blockpack.BuildColumnPolicy`'s `enabled` parameter (A1's
  write-path policy, SPEC-VI-11) — proving the two halves of R12's "one boolean gates both
  sides" claim are actually wireable from the same config value, not merely documented as
  intending to be.

**Spec invariants tested:** SPEC-VIUSAGE-6.

Back-ref: `internal/modules/viusage/config_test.go`, `config_policy_threading_test.go`.

---

## TEST-VIUSAGE-34: `DefaultDedicatedColumns` — the A0/R2 bootstrap-list finding, self-consistency
*Added: 2026-07-10*

**Scenario:** the bootstrap list (SPEC-VIUSAGE-7) actually contains all 4 legacy HTTP aliases
per the A0 finding, and no column in the list also appears in `blockpack.HardExcludedColumns`
(the two lists are meant to be orthogonal — a column landing in both would be silently
excluded anyway per `ColumnPolicy.Allowed`'s `AlwaysExclude`-wins rule, but the list itself
should not accidentally contain one, since that would signal a construction mistake).

**Setup/Assertions:**
- `TestDefaultDedicatedColumns_ContainsAllFourLegacyAliases` — asserts
  `span.http.method`/`span.http.url`/`span.http.route`/`span.http.status_code` are all
  present in `DefaultDedicatedColumns`.
- `TestDefaultDedicatedColumns_NoHardExcludedColumnLeaksIn` — asserts no entry in
  `DefaultDedicatedColumns` also appears in `blockpack.HardExcludedColumns`.

**Spec invariants tested:** SPEC-VIUSAGE-7.

Back-ref: `internal/modules/viusage/policy_test.go`.

---

## Cross-package tests (root `blockpack`), documented here per this file's stated cross-package scope

### `ColumnPolicy`/`BuildColumnPolicy` — A1's write-path policy (SPEC-VI-11, valueindex's spec domain)

`TestBuildColumnPolicy_DedicatedColumnIndexed`, `_TriggeredColumnIndexed`,
`_NonDedicatedNonTriggeredExcluded`, `_HardExcludedNeverIndexedEvenIfDedicated` (a column
placed in BOTH a dedicated list AND `HardExcludedColumns` is still excluded — AlwaysExclude
wins), `_DisabledFeatureIndexesEverything` (R12: `Enabled=false` → every column passes,
matching pre-#496 nil-policy behavior byte-for-byte); plus `TestColumnPolicy_
ZeroValueIndexesEverything`, `TestColumnPolicy_AlwaysExcludeWinsOverAllow`,
`TestHardExcludedColumns_ContainsExactlyTheFourPermanentColumns`.

Back-ref: `valueindex_policy_test.go` (root package). Full contract: `valueindex/SPECS.md`
SPEC-VI-11.

### `TestQueryDeclinesOnPartialBackfillCoverage_NotFalseComplete` — the R7 mandatory adversarial test (plan.md Section 4.8)

**Scenario:** a column's backfill has confirmed coverage for only the newest 6h of a 48h
window (`WatermarkSec = now-6h`); a REAL VI file exists on disk (written via the real
`WriteValueIndexL0` path, no hand-built fixtures) for that 6h sub-range only. A query spanning
the full 48h window must decline (`BuildValueIndexSource` returns `ok=false`), NOT assemble a
false-complete answer from the partial, genuinely-discoverable file.

**Setup:** writes one real block (a span inside the covered 6h sub-range only) via a real
`blockpack.Writer`, extracts+PUTs it via the real `WriteValueIndexL0`, then queries via the
real `BuildValueIndexSource`/`IndexFileCache` path — subtests:
1. **Full 48h window declines** — `BuildValueIndexSource(ctx, cache, store, prog, windowStart,
   nowSec, watermarks)` returns `ok=false, src=nil` despite the on-disk file being genuinely
   discoverable, proving partial coverage is correctly detected rather than silently trusted.
2. **Control: narrower query within the covered range succeeds** — the SAME watermark map,
   queried over `[watermarkSec, nowSec]` only, returns `ok=true` AND a real matching span via
   `QueryTraceQLFromIndex` — proving subtest 1's decline is caused by partial coverage
   specifically, not a blanket "any watermarked column always declines" bug that would
   trivially pass subtest 1 for the wrong reason.

**Mutation-check (plan.md 4.8 step 3, a required MANUAL verification, not an automated test
step) — CONFIRMED PERFORMED (coder-1, task #108/A5):** the `!wm.CoversRange(...)` gate was
temporarily removed from `vibuilder.BuildSource`'s leaf-loop (marked
`MUTATION-CHECK-TEMP-REMOVED`); subtest 1 ("full 48h window declines") was re-run and FAILED
exactly as expected — `Should be false` / a real, non-nil `*executor.SliceValueIndexSource`
returned, reproducing the literal false-complete bug the gate exists to prevent. The gate was
restored and both subtest 1 and subtest 2 (the control) passed again.

**The same mutation-check exercise also caught a SECOND, independent bug**, not merely
confirmed by the gate's removal: `BuildSource`'s `added`/`ok` return value was tracking
"at least one leaf had a buildable predicate" (`len(work) > 0`) rather than "a leaf was
ACTUALLY `Add`ed" — a bug that, left unfixed, would have reintroduced the same false-complete
failure mode one layer further out (`ok=true` with zero actually-covered columns) even with
the `CoversRange` gate correctly in place. Fixed via an `atomic.Bool anyLeafAdded`, set only
inside the real `src.Add` call. See `vibuilder/NOTES.md` NOTE-VI-105 for the full writeup of
this second finding and `SPECS.md` (this file) SPEC-VB-4's own binding paragraph on the fix.

**Spec invariants tested:** R7 (the single most important correctness gate in #496); `SPECS.md`
SPEC-VIUSAGE-2's `CoversRange` contract, exercised end-to-end through the real query path
rather than as a unit test of the primitive alone.

Back-ref: `valueindex_watermark_test.go:TestQueryDeclinesOnPartialBackfillCoverage_
NotFalseComplete` (root package `blockpack_test`).

### `vibuilder.BuildSource`'s watermark gate — both `src.Add` call sites

`TestBuildSource_WatermarkGateSkipsUncoveredColumn`, `_WatermarkAbsentColumnUnaffected`
(no watermark entry for a column → behaves exactly as pre-#496, the common case for dedicated
columns), `_WatermarkCoveredColumnResolves`, `_MixedLeaves_OneGatedOneNot_EdgeCase1` (plan.md's
own "Edge Case 1: query references BOTH a dedicated and a non-dedicated column in one predicate
tree" — one leaf gated, one not, in the SAME query), `_MatchAll_WatermarkGateSkipsUncoveredColumn`,
`_MatchAll_WatermarkCoveredColumnResolves` (the SECOND `src.Add` call site — match-all/Columns-
list queries have the identical partial-coverage risk as leaf predicates and must be gated too,
per plan.md 4.7's explicit callout that there are TWO call sites, not one).

Back-ref: `internal/modules/vibuilder/builder_watermark_test.go` (all 7 tests above). Full
contract: `vibuilder/SPECS.md` SPEC-VB-4.

---

## TEST-VIUSAGE-35 through 37: `Registry.UpdateWatermark` — the R7/R9 persistence primitive
*Added: 2026-07-10*

**Scenario:** `Registry.UpdateWatermark` (SPEC-VIUSAGE-4) — the concrete method
`BackfillEngine.Run`'s `progressFn` contract (SPEC-VIUSAGE-5) expects its caller to invoke on
every progress update — must persist `WatermarkSec`/window bounds on an in-progress call,
release the lease in the SAME conditional-PUT that sets `Done=true`, and error on an
unregistered entry (mirroring `RenewLease`'s own "not found" contract).

**Setup/Assertions:**
- `TestRegistry_UpdateWatermark_PersistsWatermarkSec` — an in-progress (`done=false`) call
  persists `WatermarkSec` and the window bounds without marking the entry `Done`.
- `TestRegistry_UpdateWatermark_DoneReleasesLeaseInSamePut` — an entry with an actively-held
  lease (`RenewLease`d to a far-future expiry first, to prove the release is not merely "the
  lease happened to already be expired"); a `done=true` call asserts BOTH `Backfill.Done ==
  true` AND `Backfill.BackfillInProgress == false` afterward — proving the release happens in
  the SAME `UpdateWatermark` call, not a separate step a caller could forget.
- `TestRegistry_UpdateWatermark_NotFoundReturnsError` — calling `UpdateWatermark` for an entry
  that was never `recordUse`'d errors, rather than silently creating one or no-oping (mirrors
  `TestRegistry_RenewLease_NotFoundReturnsError`'s own contract for the same reason: a backfill
  run's `progressFn` should only ever be invoked after `RecordUseAndMaybeTrigger` already
  created the entry, so a "not found" here signals a genuine caller-ordering bug, not a
  legitimate first-write case).

**Spec invariants tested:** SPEC-VIUSAGE-4 (the `UpdateWatermark` addition), SPEC-VIUSAGE-3
(Release step 3, now backed by this concrete method), SPEC-VIUSAGE-5 (the `progressFn`
contract this method exists to satisfy), and `NOTES.md` NOTE-VIUSAGE-5's addendum (the R9 gap
this method closes).

Back-ref: `internal/modules/viusage/registry_test.go:TestRegistry_UpdateWatermark_
PersistsWatermarkSec,_DoneReleasesLeaseInSamePut,_NotFoundReturnsError`.

---

## TEST-VIUSAGE-38 and 39: `Registry.Load` — real errors never conflated with not-found, regardless of value shape (go-presubmit.md CRITICAL Fix C)
*Added: 2026-07-10*

**Scenario:** the CRITICAL data-loss regression this pair exists to pin: `Registry.Load` must
distinguish a genuine miss from a real `ObjectStore.Get` failure by the returned ERROR's
identity (`errors.Is(err, ErrNotFound)`), never by the accompanying `(data, etag)` value
SHAPE — because a real failure's `(data, etag)` is, in practice, indistinguishable in shape
from a genuine 404's (`(nil, "")` either way). Before this fix, `Load` inferred "not found"
from the shape alone, so every real `Get` error was silently treated as "empty index," which
then caused `updateEntryWithRetry` to persist a fresh single-entry index via an unconditional
`ConditionalPut` (`etag=""`), destroying every other tracked column's usage/trigger/lease/
watermark state for that tenant on a single transient failure.

**Setup/Assertions:**
- `TestRegistry_Load_RealErrorNotConflatedWithNotFound` — a fake `ObjectStore` (`realErrStore`)
  whose `Get` always returns a real, non-`ErrNotFound` error (`"503 slow down: transient S3
  throttling"`) alongside the EXACT SAME empty-shaped `(nil, "")` a genuine not-found response
  has. Asserts `Load` returns a non-nil error that wraps the real error
  (`errors.Is(err, wantErr)`) — proving the real error propagates rather than being silently
  swallowed as an empty index, regardless of its shape matching not-found's shape exactly.
- `TestRegistry_Load_ErrNotFoundTreatedAsEmpty` — the same `realErrStore` fake, but returning
  `ErrNotFound` specifically. Asserts `Load` returns `(nil, "", nil)` — the ONLY error identity
  that produces the empty-index result, proving the fix didn't overcorrect into treating every
  error as a real failure (a `errors.Is(err, ErrNotFound)` check that always returned `false`
  would also make `TestRegistry_Load_RealErrorNotConflatedWithNotFound` pass, but would break
  this test, which is why both are required together as a pair).

**Spec invariants tested:** SPEC-VIUSAGE-4 (the `ErrNotFound` sentinel and `Load`'s
error-identity-not-value-shape contract).

Back-ref: `internal/modules/viusage/registry_test.go:TestRegistry_Load_
RealErrorNotConflatedWithNotFound,_ErrNotFoundTreatedAsEmpty,realErrStore`. Root re-export
parity: `valueindex_usage_test.go:TestErrNotFound_ForwardsSameSentinel`.

---

## TEST-VIUSAGE-40 and 41: `Registry.UpdateCatalogCursor` — monotonic file-catalog cursor (task #154)
*Added: 2026-07-11*

**Scenario:** SPEC-VIUSAGE-9's binding contract: the persisted cursor advances on a higher
`rowID`, silently no-ops on a lower-or-equal `rowID` (never regresses), and errors on an
unregistered entry (mirrors `RenewLease`/`UpdateWatermark`'s own not-found contract).

**Setup/Assertions:**
- `TestRegistry_UpdateCatalogCursor_MonotonicOnly` — seeds an entry, then calls
  `UpdateCatalogCursor` with rowID 50 (`LastCatalogRowID` becomes 50), then rowID 10 (no-op,
  stays 50 — proving the cursor never regresses), then rowID 75 (`LastCatalogRowID` becomes
  75 — proving a genuinely higher rowID still advances it after a no-op attempt).
- `TestRegistry_UpdateCatalogCursor_NotFoundReturnsError` — calling `UpdateCatalogCursor` for
  an entry that was never `RecordUseAndMaybeTrigger`'d errors, rather than silently creating
  one or no-oping.

**Spec invariants tested:** SPEC-VIUSAGE-9.

Back-ref: `internal/modules/viusage/registry_test.go:TestRegistry_UpdateCatalogCursor_
MonotonicOnly,_NotFoundReturnsError`.

## TEST-VIUSAGE-42..46 — Native Postgres EntryStore (issue #506)

| ID | Test | File | What it pins |
|---|---|---|---|
| TEST-VIUSAGE-42 | `TestPgEntryStore_UpsertEntry_CreateThenMutate` | pg_entry_store_test.go | UpsertEntry creates a missing entry via createIfMissing, then a subsequent call mutates the existing row; both persist correctly |
| TEST-VIUSAGE-43 | `TestPgEntryStore_UpsertEntry_MissingNoCreateIfMissing_Errors` | pg_entry_store_test.go | UpsertEntry against a missing entry with nil createIfMissing returns an error rather than silently no-oping |
| TEST-VIUSAGE-44 | `TestPgEntryStore_Load_ReturnsAllRowsForTenant_NotOtherTenants` | pg_entry_store_test.go | Load returns exactly one tenant's rows, never leaking another tenant's entries |
| TEST-VIUSAGE-45 | `TestPgEntryStore_UpsertEntry_ConcurrentTriggersConvergeOnOneWinner` | pg_entry_store_test.go | 20 goroutines racing UpsertEntry on a pre-seeded, already-existing row converge on exactly one distinct LeaseOwnerID -- the SELECT...FOR UPDATE-guarded read-modify-write race. Mutation-verified (see NOTE-VIUSAGE-14): the row must be PRE-SEEDED for this test to actually exercise FOR UPDATE's protection -- a fresh-key race is already correctly serialized by INSERT...ON CONFLICT DO NOTHING regardless of FOR UPDATE, and does not reproduce the bug when the guard is removed |
| TEST-VIUSAGE-46 | `TestViUsageRegistry_BlobAndPgBackends_IdenticalBehavior` | pg_blob_differential_test.go | Runs the identical seed/RenewLease/UpdateWatermark/UpdateCatalogCursor(advancing-then-regressing) sequence against both a blob-backed and Postgres-backed Registry; asserts resulting entries are field-equal, with explicit attention to SPEC-VIUSAGE-9's monotonic-cursor no-op on both backends -- proves SPEC-VIUSAGE-10's behavioral-identity invariant end-to-end |

**Next free ID: TEST-VIUSAGE-47.**
