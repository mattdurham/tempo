# queryplan — Test Specifications

This document defines the required tests for the `internal/modules/queryplan` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

**Backfill note (2026-07-08):** this file did not exist prior to Phase F (issue #481) —
`queryplan` shipped with only `SPECS.md`/`NOTES.md` since the package's creation for issues
#485/#486/#487, a pre-existing gap against SPEC-ROOT-009's four-file requirement. Created now per
the Phase F spec-oracle's standing ruling: create it the moment the first Phase F queryplan test
entry needs a home. Entries below start from Phase F's own test additions; no attempt is made to
retroactively backfill test descriptions for #485/#486/#487's pre-existing test suite
(`cost_test.go`, `plan_test.go`, `vcnt_cost_test.go`, `selectivity_test.go`, `lead_test.go`,
`slices_test.go`, `indexable_test.go`, `queryplan_test.go`'s pre-Phase-F tests) — that would be a
much larger, separate documentation-debt project, out of scope for this phase's trigger.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-QP-N` (file-scoped per
SPEC-ROOT-009 — distinct from `SPEC-QP-N`/`NOTE-QP-N` in this package's other two files). IDs are
assigned in ascending order and never reused or renumbered; superseded entries are marked
`[SUPERSEDED by TEST-QP-N]` rather than deleted.

Next free ID: **TEST-QP-4**.

---

## TEST-QP-1: SelectSearchStrategy five-row core decision table
*Added: 2026-07-08 (issue #481 part 2, F-5)*

**Scenario:** `SelectSearchStrategy` returns the correct `(DispatchStrategy, planTimeDecline)`
pair for every `(Selectivity, hasLimit)` input combination per the R13 ruling table: Selective
always dispatches index-only regardless of `hasLimit`; LowSelectivity+limit and
UnknownSelectivity+limit both dispatch `DispatchBoundedRecentFirst`; LowSelectivity+no-limit sets
`planTimeDecline=true`; UnknownSelectivity+no-limit dispatches index-only.

**Setup:** `TestSelectSearchStrategy_FiveRowCore` (table-driven) covers all 6 concrete
`(Selectivity, hasLimit)` input combinations (Selective collapses both `hasLimit` values to the
same outcome, hence 5 distinct rows over 6 inputs).

**Assertions:** Each input combination produces the exact `(strategy, planTimeDecline)` pair the
ruling table specifies — no row falls through to an unhandled default.

**Spec invariants tested:** SPEC-QP-6.

Back-ref: `internal/modules/queryplan/queryplan_test.go:TestSelectSearchStrategy_FiveRowCore`.

---

## TEST-QP-2: SelectSearchStrategy never returns DispatchTimeSliced or an invalid strategy value
*Added: 2026-07-08 (issue #481 part 2, F-5)*

**Scenario:** Regression guard confirming `SelectSearchStrategy` never returns `DispatchTimeSliced`
(the unrelated #487 opt-in strategy value owned by `BuildQueryPlan`, not a search-dispatch concept
at all) or any other value outside its own two legitimate outputs
(`DispatchBlockSharded`/`DispatchBoundedRecentFirst`) — and does not invent a fake "decline"
`DispatchStrategy` enum value to signal a plan-time decline, since `planTimeDecline` is the sole,
out-of-band decline signal. Protects a caller doing an exhaustive switch over `DispatchStrategy`'s
three real values from ever being surprised by a value `SelectSearchStrategy` has no business
returning.

**Setup:** `TestSelectSearchStrategy_NeverReturnsTimeSlicedOrFakeDeclineStrategy` iterates all 6
`(Selectivity, hasLimit)` input combinations and asserts the returned `strategy` is always exactly
`DispatchBlockSharded` or `DispatchBoundedRecentFirst` — `DispatchTimeSliced` never appears.

**Assertions:** No input combination returns `DispatchTimeSliced` or an undocumented enum value;
decline is always communicated via `planTimeDecline`, never via `strategy`.

**Spec invariants tested:** SPEC-QP-6.

Back-ref: `internal/modules/queryplan/queryplan_test.go:TestSelectSearchStrategy_NeverReturnsTimeSlicedOrFakeDeclineStrategy`.

---

## TEST-QP-3: VCNTDurationCostFunc / CombineCostFuncs / histogram-fallback ColumnTotalFunc (issue #205)
*Added: 2026-07-13*

**Scenario:** `VCNTDurationCostFunc` (`vcnt_duration_cost.go`) must recognize single-bound and
between duration-range leaves on the histogram-eligible column and answer via
`valuecounts.DurationHistogram`'s estimator methods as a thin adapter (not a second
implementation); must reject every other leaf shape as `UnknownCost()`; `CombineCostFuncs` must
implement pure try-first-then-second semantics; and composing both into the existing
`ClassifyProgramVCNT*` family (`vcnt_cost.go`) must not change any pre-existing equality-leaf
classification result, byte for byte.

**Setup / Assertions (in `vcnt_duration_cost_test.go` unless noted):**
- `TestVCNTDurationCostFunc_RangeLeafOnHistogramColumn_ReturnsKnownCost`: a hand-built histogram
  spread across 4 buckets; `GT`/`GTE`/`LT`/`LTE` subtests each build a single-bound leaf and
  assert `VCNTDurationCostFunc`'s `LeafCost.Count` equals calling
  `DurationHistogram.EstimateThreshold` directly with the same op/threshold — proving the adapter
  contributes no independent logic.
- `TestVCNTDurationCostFunc_BetweenLeaf_ReturnsKnownCost`: same proof for a Min-AND-Max leaf
  against `EstimateBetween`.
- `TestVCNTDurationCostFunc_EqualityLeaf_ReturnsUnknownCost`: a `Values`-only leaf on the
  histogram column returns `UnknownCost()`.
- `TestVCNTDurationCostFunc_NonHistogramColumn_ReturnsUnknownCost`: a range leaf shaped exactly
  like a duration leaf, on a column that isn't the eligible histogram column, returns
  `UnknownCost()` — proves the column gate is real, not "every range leaf now gets a cost."
- `TestVCNTDurationCostFunc_EqualityShapeLeaf_LeavesToVCNTCostFunc`: an equality leaf on the
  histogram column itself still returns `UnknownCost()` from `VCNTDurationCostFunc` — mutual
  exclusivity with `VCNTCostFunc` by construction.
- `TestCombineCostFuncs_TriesFirstThenSecond`: both-Known returns first's answer; first-Unknown
  falls through to second's answer.
- `TestCombineCostFuncs_BothUnknown_ReturnsUnknown`.
- `TestCombineCostFuncs_NeverDoubleCounts`: two stub funcs deliberately recognizing the same leaf
  (an overlap the real production functions never have) — asserts the combined result is exactly
  first's answer, never a sum or blend.
- **`TestVCNTCostFunc_ExistingEqualityBehavior_ByteIdenticalAfterHistogramWiring`** (the CRITICAL
  regression guard): every leaf shape from `vcnt_cost_test.go`'s own fixtures (equality hits,
  unseen value, missing column, net-zero, and all five non-equality shapes —
  range-min/regex/present-only/multi-value/columnless) is run through `VCNTCostFunc` alone (the
  pre-#205 behavior) and through `CombineCostFuncs(VCNTCostFunc(...), VCNTDurationCostFunc(...))`
  (the exact composition `ClassifyProgramVCNTWithThreshold`/`WithDetail` now use) and asserted
  byte-identical.
- `TestVCNTColumnTotalFunc_HistogramFallback_UsedOnlyWhenBareColumnUncovered`: a section with
  ONLY histogram rows for the eligible column (no bare discrete-value records — the real
  production shape) still yields a summed `ColumnTotal`, `ok=true`.
- `TestVCNTColumnTotalFunc_PrefersBareColumnWhenBothExist`: when both a bare discrete-value
  record and histogram rows exist for the same column, the bare value wins and the histogram
  fallback is not consulted (defensive; no real production column has both today).

**Mutation-test verification (performed manually, not committed as a test — see
`NOTES.md` `NOTE-QP-013` for the full writeup):** (1) `CombineCostFuncs`'s try-order was flipped;
confirmed this does NOT break the byte-identical regression guard above (proving the two
production `CostFunc`s are mutually exclusive by leaf shape in practice, not just by inspection)
but DOES break `TestCombineCostFuncs_TriesFirstThenSecond`/`_NeverDoubleCounts`. (2)
`VCNTDurationCostFunc` was temporarily mutated to return `KnownCost(999)` unconditionally;
confirmed this DOES break the byte-identical regression guard for every leaf shape
`VCNTCostFunc` alone reads as Unknown. Both mutations were reverted after confirming the
respective failures.

**Spec invariants tested:** SPEC-QP-9.

Back-ref: `internal/modules/queryplan/vcnt_duration_cost_test.go`,
`internal/modules/queryplan/vcnt_cost_test.go` (regression fixtures reused, unmodified).
