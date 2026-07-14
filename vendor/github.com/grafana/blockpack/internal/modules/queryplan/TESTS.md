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

Next free ID: **TEST-QP-7**.

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

---

## TEST-QP-4: collectANDConjoinedLeaves — OR-ancestry-aware leaf enumeration (issue #499, Phase 2)
*Added: 2026-07-14*

**Scenario:** `collectANDConjoinedLeaves` (`and_conjoined.go`) must enumerate every leaf that is
AND-conjoined all the way to the root of the given `Group` — excluding any leaf nested under a
`GroupOR` at any depth — while never changing `leadLeaf`/`Group.Lead()`'s own existing behavior
for either of their two existing production callers (`classifyDetailed`, `selectivity.go`;
`BuildQueryPlan`'s lead-leaf resolution, `queryplan.go`).

**Setup / Assertions (in `and_conjoined_test.go` unless noted):**
- `TestCollectANDConjoinedLeaves_ExcludesLeavesNestedUnderOR`: reuses `lead_test.go`'s exact
  fixture, `AND(kind, OR(region, AND(user, env)))` (built via the same `costByColumn`/`Plan`
  calls that test already uses). Asserts `collectANDConjoinedLeaves(root)` returns exactly
  `{"kind"}` — `"region"`/`"user"`/`"env"` all excluded because they sit under the `OR`.
  Adversarially rigs `"user"` to ALSO win `Lead()` by cost (lowest `KnownCost`), and asserts
  `Lead()` does pick `"user"` — proving the exclusion from `collectANDConjoinedLeaves` holds even
  when the excluded leaf is simultaneously the plan's own lead, not just when the two happen to
  differ.
- `TestCollectANDConjoinedLeaves_ReturnsAllLeavesForPureANDTree`: a pure-AND tree (`planAND`
  helper, `selectivity_test.go`) `AND(a, b, c)`; asserts all three leaves are returned,
  order-independent (compared as a sorted column-name set, not by slice index).
- `TestCollectANDConjoinedLeaves_NestedPureANDFlattenedByPlanAlreadyReturnsAllLeaves`: exercises
  `planAndNodes`'s own maximal-AND-subtree flattening rule (a nested pure-AND composite under a
  top-level AND is flattened into the same `GroupAND`, confirmed via `len(g.SubGroup) == 0`);
  since `Plan()` already flattens this before `collectANDConjoinedLeaves` ever sees it, this
  documents (rather than newly discovers) that no leaf is missed due to nesting depth alone —
  distinguishing "nested AND" (fully collected) from "nested under an OR" (excluded, per the test
  above) as the only two shapes that matter to this function.
- `TestCollectANDConjoinedLeaves_EmptyGroupReturnsNil`: a zero-value `Group{}` (`Kind == GroupAND`
  since that's the zero value) with no `Leaves`/`SubGroup` returns `nil`, not a panic or an
  empty-but-non-nil slice.

**Proof by absence of change (review-time checklist, not a new test):**
`TestGroupLead_FindsLowestKnownCostAcrossSubGroups` (`lead_test.go`),
`TestClassifyLeadLeafNestedInORBranch`, `TestClassifyUsesMostSelectiveLead`
(`selectivity_test.go`) all continue to pass with zero edits to their own assertions — the diff
introducing `collectANDConjoinedLeaves` touches no existing file (`plan.go`, `lead.go`,
`selectivity.go`) at all. Verified via `go test ./internal/modules/queryplan/... -run
'TestCollectANDConjoinedLeaves|TestGroupLead|TestClassifyLeadLeafNestedInORBranch|TestClassifyUsesMostSelectiveLead'`.

**Mutation-test verification (performed manually, not committed as a test — not required by the
Phase 2 plan, since Phase 3's later OR-safety integration test re-proves the same property, but
performed anyway per this project's mutation-test convention, memory
`feedback_mutation_test_review.md`):** the `if g.Kind != GroupAND { return nil }` guard was
temporarily removed; confirmed `TestCollectANDConjoinedLeaves_ExcludesLeavesNestedUnderOR` then
fails (incorrectly includes `region`/`user`/`env`); the guard was restored and the full package
test suite reconfirmed green.

**Spec invariants tested:** SPEC-QP-10.

Back-ref: `internal/modules/queryplan/and_conjoined_test.go`.
---

## TEST-QP-5: VCNTDurationPerMinuteFunc / CombinePerMinuteFuncs / TimeSliceOracle's composed pair (issue #499, Phase 1)
*Added: 2026-07-14*

**Scenario:** `VCNTDurationPerMinuteFunc` (`vcnt_duration_perminute.go`) must recognize the same
single-bound/between duration-range leaf shapes `VCNTDurationCostFunc` recognizes and answer via
`valuecounts.DurationHistogramPerMinuteInRange`'s per-minute histograms as a thin adapter, must
exclude every zero/unknown-estimate minute, and must reject every other leaf shape as `nil`;
`CombinePerMinuteFuncs` must implement pure try-first-then-second semantics; and composing both
`VCNTDurationCostFunc`/`VCNTDurationPerMinuteFunc` into root `TimeSliceOracle` together (never
independently) must let a genuinely-more-selective duration-range leaf win `Lead()` AND carry real
per-minute signal, without changing any pre-existing equality-leaf fixture's result.

**Setup / Assertions (in `vcnt_duration_perminute_test.go` unless noted):**
- `TestVCNTDurationPerMinuteFunc_RangeLeafOnHistogramColumn_ReturnsPerMinuteSignal`: a
  two-minute histogram fixture; asserts the returned `[]valuecounts.MinuteCount` matches applying
  `EstimateThreshold` to each minute's histogram independently — the same "thin adapter, not a
  second implementation" proof `TestVCNTDurationCostFunc_RangeLeafOnHistogramColumn_ReturnsKnownCost`
  established for the cost side.
- `TestVCNTDurationPerMinuteFunc_BetweenLeaf_ReturnsPerMinuteSignal`: same proof for
  `EstimateBetween`.
- `TestVCNTDurationPerMinuteFunc_EqualityLeaf_ReturnsNil`: an equality-shaped leaf on
  `span:duration` returns `nil` — mutual exclusivity with `VCNTPerMinuteFunc`, mirroring
  `VCNTDurationCostFunc`'s own equality-leaf mutual-exclusivity test.
- `TestVCNTDurationPerMinuteFunc_NonHistogramColumn_ReturnsNil`: a range leaf shaped exactly like
  a duration leaf, on a column with no histogram, returns `nil`.
- `TestVCNTDurationPerMinuteFunc_ZeroOrUnknownMinutesExcludedFromOutput`: a minute whose estimate
  is `<= 0` for the leaf's own threshold is excluded from the returned slice entirely — proves the
  `if !known || count <= 0 { continue }` filtering is real, not assumed.
- `TestCombinePerMinuteFuncs_TriesFirstThenSecond`: both non-empty returns first's answer;
  first-empty falls through to second's answer.
- `TestCombinePerMinuteFuncs_BothEmpty_ReturnsNilOrEmpty`.
- `TestCombinePerMinuteFuncs_NeverDoubleCounts`: two stub funcs deliberately recognizing the same
  leaf (an overlap the real production functions never have) — asserts the combined result is
  exactly first's answer, never merged/concatenated.
- **`TestTimeSliceOracle_ComposedFuncs_ByteIdenticalForPreExistingEqualityFixtures`** (root
  package, `timeslice_composition_test.go`; the CRITICAL regression guard): every pure-equality
  leaf shape is run through both the pre-#499 bare pair (`queryplan.VCNTCostFunc`/
  `queryplan.VCNTPerMinuteFunc` directly) and the post-#499 composed pair (`TimeSliceOracle`'s new
  body) and asserted byte-identical (cost) / length-and-value-equivalent (per-minute, treating a
  `nil` and an empty-non-nil result as equivalent per this spec's own liveness convention).
- **`TestBuildQueryPlan_EqualityLeafRemainsLeadWhenGenuinelyMoreSelectiveThanDurationLeaf`** (root
  package): a compound AND of an equality leaf and a duration-range leaf where the equality leaf's
  real VCNT count is genuinely lower; asserts `Lead()` still picks the equality leaf.
- **`TestBuildQueryPlan_DurationLeafBecomesLeadWhenGenuinelyMoreSelective_CarriesRealPerMinuteSignal`**
  (root package): the mirror case (duration leaf genuinely more selective); asserts (a) `Lead()`
  picks the duration leaf, AND (b) the COMPOSED `perMinuteForLead` callback returns real,
  non-empty per-minute signal for it — the CI-enforced guard against reintroducing the naive
  cost-only mistake (NOTE-QP-015's "Approach 1 (rejected)").
- **`TestBuildQueryPlan_PublicAPIDrivesPerMinuteSlicingForDurationLeadLeaf`**
  (`timeslice_publicapi_test.go`, root `blockpack_test` package): the real, through-`TimeSliceOracle`,
  through-public-`BuildQueryPlan`, through-real-`CompileTraceQL` end-to-end proof for a compound
  equality+duration query, using the exact real root API a tempo caller would use.

**Mutation-test verification performed (per this project's standing convention — see `NOTES.md`
`NOTE-QP-015` for the full writeup):** `TimeSliceOracle`'s per-minute composition was temporarily
reverted to the bare `VCNTPerMinuteFunc` (cost composition left intact); confirmed this DOES break
`TestBuildQueryPlan_DurationLeafBecomesLeadWhenGenuinelyMoreSelective_CarriesRealPerMinuteSignal`'s
assertion (b) (returned empty instead of real signal), then reverted the mutation and re-confirmed
the test passes.

**Spec invariants tested:** SPEC-QP-11.

Back-ref: `internal/modules/queryplan/vcnt_duration_perminute_test.go`; root
`timeslice_composition_test.go`, `timeslice_publicapi_test.go`.

---

## TEST-QP-6: TimeSlice.SkipDispatch — multi-leaf veto, freshness margin, and parity with existing callers (issue #499, Phase 3)
*Added: 2026-07-14*

**Scenario:** `TimeSlice.SkipDispatch` (`slices.go`) must be set from every leaf
`collectANDConjoinedLeaves` returns, independent of `Lead()`'s own selection, gated by the
`skipDispatchFreshnessMarginSeconds` freshness margin anchored to `maxTS`; must never be
influenced by a leaf sitting under an `OR`; must remain `false` for every pre-#499 caller shape
(`andConjoinedSignals == nil`); and must correctly disagree with `EstMatches` on the same slice
when a non-lead AND-conjoined leaf vetoes a lead leaf's own positive count.

**Setup / Assertions (in `skip_dispatch_test.go` unless noted):**
- **`TestBuildQueryPlan_NeverSkipsSliceWhenNonANDConjoinedLeafIsConfidentZero`** (THE most
  important test in the whole combined #499 task): reuses `lead_test.go`'s exact fixture,
  `AND(kind, OR(region, AND(user, env)))`. `"user"` (lowest cost, wins `Lead()`) has a non-empty
  per-minute signal that is absent — confidently zero — at the tested minute; `"region"` has real
  matches at that same minute. Asserts the resulting slice has `SkipDispatch == false` — proving
  `collectANDConjoinedLeaves` correctly excludes `"region"`/`"user"`/`"env"` from the checked set
  even when the excluded leaf (`"user"`) is simultaneously the plan's own lead. The zero signal is
  deliberately non-empty-but-absent-at-the-tested-minute (not `nil`) — a `nil`/empty signal is
  excluded from the veto set by `BuildQueryPlan`'s own `len(sig) > 0` gate regardless of
  `collectANDConjoinedLeaves`'s correctness, which would make this test pass even against a
  broken, guardless implementation (see `NOTES.md` NOTE-QP-016 for the mid-implementation
  rewrite this caused).
- **`TestBuildQueryPlan_SkipsSliceWhenPureANDEqualityLeadIsConfidentZero`**: a pure-AND query (two
  equality leaves, no `OR`), real per-minute signal via the real `VCNTPerMinuteFunc` oracle
  end-to-end (not a stub). The lead leaf has no record at one minute (confidently zero, outside
  the freshness margin) and both leaves have real matches at another minute. Asserts
  `SkipDispatch == true` for the zero minute and `SkipDispatch == false` for the live minute.
- **`TestBuildQueryPlan_SkipsSliceWhenPureANDDurationLeadIsConfidentZero`**: the same shape and
  assertion structure as the equality test above, but with a duration-range lead leaf using
  Phase 1's real `VCNTDurationPerMinuteFunc`/`VCNTDurationCostFunc` — the concrete
  leaf-type-agnostic proof that `SkipDispatch` requires zero extra per-leaf-type code.
- **`TestBuildQueryPlan_SkipsSliceWhenNonLeadANDConjoinedLeafIsConfidentZero_EvenWhenLeadLeafHasMatches`**
  (the veto test — direct proof of the multi-leaf expansion's core new capability): `AND(c, d)`
  where `c` wins `Lead()` with real matches at a minute (`EstMatches > 0` on the resulting slice)
  but `d` (not lead, still AND-conjoined) is confidently zero for that same minute. Asserts
  `SkipDispatch == true` on the SAME slice where `EstMatches > 0` — the literal proof the two
  fields are independent and can disagree, and that a naive lead-leaf-only gate would fail this
  exact test (mutation-tested, see below).
- **`TestBuildTimeSlices_NilAndConjoinedSignals_SkipDispatchAlwaysFalse`** (`slices_test.go`): a
  nil `andConjoinedSignals` — every pre-#499 call site's shape — makes `SkipDispatch` false for
  every slice regardless of `perMinute`'s own content, made explicit rather than relying on
  existing tests' own passing as implicit proof.
- **`TestBuildTimeSlices_NeverSkipsDispatchWithinFreshnessMarginOfMaxTS`** (`slices_test.go`, the
  locked freshness-margin test): a genuine confident-zero entry at two minutes — one within
  `skipDispatchFreshnessMarginSeconds` of `maxTS`, one clearly outside it. Asserts the near-`maxTS`
  minute's slice has `SkipDispatch == false` (the margin's protective effect) and the
  far-from-`maxTS` minute's slice has `SkipDispatch == true` (proving the margin does not disable
  the whole mechanism).
- **Proof by absence of change** (review-time checklist, not a new test, restated from `TEST-QP-4`
  since Phase 3 is where the temptation to "simplify" `Lead()` would most likely creep in):
  `TestGroupLead_FindsLowestKnownCostAcrossSubGroups`, `TestClassifyLeadLeafNestedInORBranch`,
  `TestClassifyUsesMostSelectiveLead` all continue to pass with zero edits to their own bodies.
- **Known, intentional fallout to a pre-existing test:**
  `TestBuildQueryPlan_QualifiesWithUniformSlicesWhenNoLeafIsVCNTEstimable`'s old
  `perMinuteForLeadCalled` assertion was removed (not weakened silently — see `NOTES.md`
  NOTE-QP-016 for the full rationale): `perMinuteForLead` may now legitimately be called for an
  AND-conjoined leaf even when `Lead()` itself returns `ok=false`. The test's other assertions
  (`Strategy == DispatchTimeSliced`, every slice `EstKnown == false`) are unchanged, plus a new
  `SkipDispatch == false` assertion (this scenario's `perMinuteForLead` always returns `nil`, so no
  leaf can ever veto).

**Mutation-test verification performed (all three, per this project's mutation-test convention —
see `NOTES.md` NOTE-QP-016 for the full writeup):**
1. **OR-safety** (the single most important guard in the whole plan):
   `collectANDConjoinedLeaves`'s `if g.Kind != GroupAND { return nil }` guard was temporarily
   removed; confirmed `TestBuildQueryPlan_NeverSkipsSliceWhenNonANDConjoinedLeafIsConfidentZero`
   fails (a data-loss-shaped bug — `SkipDispatch` incorrectly becomes `true` — becomes
   reproducible); guard restored, full package test suite reconfirmed green.
2. **Veto (lead-leaf-only gate):** `BuildQueryPlan`'s real multi-leaf `andConjoinedSignals`
   resolution was temporarily replaced with a deliberately-wrong lead-leaf-only gate; confirmed
   `TestBuildQueryPlan_SkipsSliceWhenNonLeadANDConjoinedLeafIsConfidentZero_EvenWhenLeadLeafHasMatches`
   fails; reverted to the real multi-leaf loop, test reconfirmed passing.
3. **Freshness margin:** `BuildTimeSlices`'s `if m < freshnessCutoff` guard was temporarily
   removed; confirmed `TestBuildTimeSlices_NeverSkipsDispatchWithinFreshnessMarginOfMaxTS`'s
   near-`maxTS` assertion fails; guard restored, test reconfirmed passing.

**Spec invariants tested:** SPEC-QP-12 (and its amendments to SPEC-QP-2/SPEC-QP-3).

Back-ref: `internal/modules/queryplan/skip_dispatch_test.go`, `internal/modules/queryplan/slices_test.go`.
