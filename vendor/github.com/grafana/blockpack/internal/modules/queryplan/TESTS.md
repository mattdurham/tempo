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

Next free ID: **TEST-QP-3**.

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
