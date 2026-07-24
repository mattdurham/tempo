# queryplan — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/queryplan` package. It complements `NOTES.md` (design rationale), per root
`SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

This file is created for the first time as part of issue #487 (time-slice job sharding) —
prior to this, `queryplan` had only a `NOTES.md`.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-QP-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/queryplanner/SPECS.md`'s own numbering — **`queryplan` and `queryplanner` are
two different packages**; do not conflate them. `queryplanner`'s own NOTES.md predates this
convention and uses a plain `NOTE-NNN` numbering scheme, not `NOTE-QP-N`). IDs are assigned in
ascending order and never reused or renumbered; superseded entries are marked `[SUPERSEDED by
SPEC-QP-N]` rather than deleted. `queryplan/NOTES.md`'s own entries use the separate `NOTE-QP-00N`
counter (module-local to this package only, not shared with any other module).

Next free ID: **SPEC-QP-13**. (Corrected 2026-07-13: this line had drifted stale at
`SPEC-QP-7` even though `SPEC-QP-8` already existed below — issue #217 added it without updating
this line. `SPEC-QP-9` below is confirmed correct against the file's actual last header, not
against this line, per this project's standing ID-drift-checking convention.)

---

## SPEC-QP-1: `Group.Lead()` — exported lead-leaf accessor
*Added: 2026-07-07*

**Contract:** `func (g Group) Lead() (PlannedLeaf, bool)` returns the plan tree's
most-selective leaf — the leaf with the lowest `Known` `LeafCost.Count`, searching `g.Leaves`
and recursing into every `g.SubGroup` entry. Implemented as a direct delegation to
`selectivity.go`'s unexported `leadLeaf` (`return leadLeaf(g)`) — see `NOTES.md` NOTE-QP-004.

**Rules:**
- A leaf with `Cost.Known == false` (Unknown) is never returned as the lead, regardless of any
  `Count` value it may carry — mirrors the Known/Unknown asymmetry established in NOTE-QP-001.
- `ok == false` only when no leaf anywhere in the tree (including nested `SubGroup`s) carries a
  Known cost. Otherwise `ok == true` and `PlannedLeaf` is the selected lead.
- A `KnownCost(0)` (net-zero / maximally selective) leaf is a valid, selectable lead — it is not
  treated specially or excluded here (contrast with `Classify`'s own short-circuit behavior on a
  net-zero lead, NOTE-QP-002, which is a separate consumer-level decision, not part of this
  accessor's contract).
- Ties between two leaves with equal Known cost resolve to whichever is encountered first in
  the tree walk (direct `Leaves` in declaration order, then `SubGroup`s in order) — a
  deterministic, order-of-discovery tie-break, not magnitude- or alphabetically-based.
- Because `Lead()` delegates directly to `leadLeaf`, the two can never diverge.

Back-ref: `internal/modules/queryplan/lead.go:Group.Lead`,
`internal/modules/queryplan/selectivity.go:leadLeaf`. See `NOTES.md` NOTE-QP-004.
Test: `lead_test.go:TestGroupLead_FindsLowestKnownCostAcrossSubGroups`. Issue #487.

---

## SPEC-QP-2: `TimeSlice` / `BuildTimeSlices` — time-slice partition contract
*Added: 2026-07-07. Amended 2026-07-07 (holistic-review Fixes 3/4/5/6): overflow-saturation
bullet added, "narrower than 60s" exception removed (proven unreachable), negative-Count
clamp bullet added, slice-count cap bullet added (Fix 6, the boundary story now has three
parts: minute floor, MaxUint64 saturation, slice-count cap). Amended again 2026-07-07
(team-lead ruling, T-phase design question): DefaultK=1 ruling bullet added, implementation
pending. Amended 2026-07-14 (issue #499, Phase 3): `andConjoinedSignals` parameter and
`SkipDispatch` field added — see `SPEC-QP-12` for `SkipDispatch`'s own full contract. This is an
ADDITIVE change, not a removal: the full-partition guarantee below (every minute still gets
exactly one `TimeSlice`) is unchanged — a caller opts into treating a `SkipDispatch`-marked slice
as unfetchable, the partition itself never shrinks.*

**Contract:** `BuildTimeSlices(perMinute []valuecounts.MinuteCount, andConjoinedSignals
[][]valuecounts.MinuteCount, minTS, maxTS uint64, concurrentRequests, k int) []TimeSlice`
partitions `[minTS, maxTS]` into a fully-covering, minute-aligned, non-overlapping sequence of
`TimeSlice{Start, End uint64; EstMatches int64; EstKnown bool; VCNTEmpty bool; SkipDispatch bool}`
values, `[Start, End)` half-open. `andConjoinedSignals` feeds `SkipDispatch` only — see
`SPEC-QP-12` for its own contract; every other rule below is unchanged by its addition.

**Rules:**
- `minTS > maxTS` returns `nil`.
- `Start`/`End` on every returned slice are 60-second-aligned (`(sec/60)*60`).
- **Full coverage, no gaps (binding):** the returned slices always exactly and contiguously
  cover the minute-floored `[minTS, maxTS]` window with zero holes.
- Every slice's width is in `[60, 3600]` seconds inclusive; the post-floor window is always
  `>= 60s` when `minTS <= maxTS`, so no narrower slice is ever produced. **Superseded 2026-07-14
  (issue #499 holistic review, pre-existing #217 drift caught in passing):** the adaptive-width
  half of this range no longer exists — `SPEC-QP-8`/`NOTE-QP-012` (issue #217) forced every
  slice to exactly `minSliceWidthSeconds` (60s); `BuildTimeSlices` itself documents that "the #487
  adaptive/uniform width algorithms this doc comment used to describe are removed, not merely
  disabled." The `>= 60s` lower bound above still holds; the `<= 3600s` upper bound is moot since
  width is now always exactly 60s. Left in place rather than deleted per this file's own
  mark-superseded-don't-silently-delete convention.
- **Overflow saturation (binding):** if `maxTS` exceeds `maxSafeMaxTS` (=
  `math.MaxUint64 - maxSliceWidthSeconds - minSliceWidthSeconds`; unreachable with real
  unix-second timestamps — this ceiling sits near 2^64), `maxTS` is silently saturated down to
  `maxSafeMaxTS` before any window arithmetic runs, guaranteeing `Start`/`End` computation can
  never wrap `uint64`. If `minTS` exceeds the saturated `maxTS`, `BuildTimeSlices` returns
  `nil`, same as any other inverted range.
- **Slice-count cap (binding, holistic-review Fix 6):** if the (post-saturation) window would
  require more than `maxSlicesPerPlan` (2000) slices even at the widest allowed width
  (`maxSliceWidthSeconds`), `BuildTimeSlices` returns `nil` rather than attempting the
  allocation — SPEC-ROOT-001 forbids a public-API-reachable panic regardless of input
  absurdity. This does not widen the `[60, 3600]` width clamp; an oversized window simply does
  not qualify for slice mode, and the caller (`BuildQueryPlan`) falls back to
  `DispatchBlockSharded` via the existing zero-`Slices` rule (SPEC-QP-3).
- **`EstKnown`/`VCNTEmpty`/`EstMatches` three-way contract (binding — see NOTES.md NOTE-QP-005
  for full rationale):**

  | `EstKnown` | `VCNTEmpty` | `EstMatches` | Meaning |
  |---|---|---|---|
  | `true` | `false` | `>0` | real VCNT signal found for this slice |
  | `true` | `true` | `0` | real VCNT signal fetched, this slice's minutes summed to zero |
  | `false` | `false` | `0` | no VCNT signal was ever fetched for the plan (uniform fallback) |

  The `(false, true, *)` combination never occurs.
- When `len(perMinute) > 0`: width is adaptive per NOTE-QP-005's target/clamp formula; every
  resulting slice has `EstKnown: true`.
- When `len(perMinute) == 0`: width is uniform, `window / desiredSliceCount` (clamped). Every
  slice has `EstKnown: false`, `VCNTEmpty: false`, `EstMatches: 0`.
- **Default `k` — binding ruling, `DefaultK = 1` (issue #487's design doc left this unpinned):**
  `desiredSliceCount = concurrentRequests * k` (clamped to a minimum of 1) governs both the
  uniform-width fallback's slice count and the adaptive path's per-slice matches target. The
  design doc states the formula's shape (`window / (ConcurrentRequests × k)`) but explicitly
  leaves `k` for post-deployment empirical tuning ("Slice-width tuning" open question) without
  pinning a value. `DefaultK = 1` is the ruled first-guess default: it sizes the uniform-width
  fallback so one dispatch round at `concurrentRequests` slices already covers the whole window
  — the least-aggressive default when no VCNT signal exists to justify finer slicing. `k` itself
  remains caller-supplied (not `BuildTimeSlices`-internal); `DefaultK` is the config-visible
  constant callers should reference instead of hardcoding a literal, mirroring the
  config-visibility convention already established for `minSliceWidthSeconds`/
  `maxSliceWidthSeconds`/`maxSlicesPerPlan`. A caller MAY pass `k > 1` for narrower slices (more
  jobs, finer `ShouldQuit` cancellation granularity, more per-slice dispatch/fetch overhead) —
  this is a tuning knob, not a correctness constraint. `queryplan.DefaultK = 1` (root re-export
  `blockpack.DefaultK`) is the landed constant — see `NOTES.md` NOTE-QP-008 for the full
  rationale.
- **Negative `Count` inputs are clamped to zero (defensive, binding):** `perMinute` is
  caller-supplied (ultimately `BuildQueryPlan`'s `perMinuteForLead`), not guaranteed to route
  through `valuecounts.SelectivityPerMinute`'s own non-negative liveness-rule contract. A
  `MinuteCount.Count < 0` is clamped to `0` before accumulation, so it can never drive
  `total`/`target` negative (which would silently disable the target-reached stop condition) or
  masquerade as a genuine negative signal.
- **Ordering (binding):** returned slices are chronologically ordered, most-recent-first.
  `BuildTimeSlices` does NOT sort by `EstMatches` or `EstKnown`/`VCNTEmpty` — any priority-based
  reordering is the caller's responsibility. See `NOTES.md` NOTE-QP-005.
- **Degenerate all-`EstKnown=false` case (binding):** when every returned slice has
  `EstKnown=false`, the returned chronological (most-recent-first) order is the complete
  dispatch order — no further est-matches-based reordering is meaningful or permitted, since no
  slice carries a signal to rank by. See `NOTES.md` NOTE-QP-005.

Back-ref: `internal/modules/queryplan/slices.go:TimeSlice,BuildTimeSlices,maxSafeMaxTS,maxSlicesPerPlan,DefaultK`.
See `NOTES.md` NOTE-QP-005, NOTE-QP-008 (`DefaultK`). Tests: `slices_test.go` (including
`TestBuildTimeSlices_NearMaxUint64DoesNotOverflow`,
`TestBuildTimeSlices_NegativeCountFromNonConformingOracleClampedToZero`,
`TestBuildTimeSlices_HugeWindowIsCappedNotPanicking`,
`TestBuildTimeSlices_RealisticLongWindowStillSlicesNormally`, `TestDefaultK_IsOne`). Issue #487.
`SkipDispatch`/`andConjoinedSignals` (issue #499, Phase 3) tests: `slices_test.go`
(`TestBuildTimeSlices_NilAndConjoinedSignals_SkipDispatchAlwaysFalse`,
`TestBuildTimeSlices_NeverSkipsDispatchWithinFreshnessMarginOfMaxTS`) — see `SPEC-QP-12`.

---

## SPEC-QP-3: `QueryPlan` / `DispatchStrategy` / `BuildQueryPlan` — top-level dispatch qualification contract
*Added: 2026-07-07. Amended 2026-07-07 (holistic-review Fix 2): added the third
non-empty-`Slices` qualification condition. Amended again 2026-07-07 (issue #487 T5b): added
the binding `allLeavesResolvable`-computation bullet cross-referencing SPEC-QP-5.*

**Contract:** `BuildQueryPlan(prog *vm.Program, cost CostFunc, allLeavesResolvable bool,
perMinuteForLead func(*vm.RangeNode) []valuecounts.MinuteCount, minTS, maxTS uint64,
concurrentRequests, k int) QueryPlan` returns `QueryPlan{Root Group, Slices []TimeSlice,
Strategy DispatchStrategy}`.

**Rules:**
- `DispatchStrategy` is an enum: `DispatchBlockSharded` (zero value) or `DispatchTimeSliced`.
- `Strategy == DispatchTimeSliced` if and only if `Plan(prog, cost)` succeeds (`ok=true`),
  `allLeavesResolvable == true`, AND `BuildTimeSlices` returns a non-empty `Slices` for the
  resolved `perMinute`/window. The third condition exists so an otherwise-qualified plan whose
  window degenerates to zero slices (e.g. an inverted `minTS > maxTS`) still falls back to
  `DispatchBlockSharded` rather than reporting `DispatchTimeSliced` with an empty `Slices` —
  which a caller could misread as "dispatch zero jobs" (holistic-review Fix 2). Lead-leaf VCNT
  estimability still does not affect `Strategy` on its own — only whether `BuildTimeSlices` gets
  real per-minute signal vs. the uniform-width fallback, which for any non-degenerate window
  always yields a non-empty `Slices`. See `NOTES.md` NOTE-QP-006 for the binding ruling
  rejecting a stricter, estimability-gated alternative that was separately considered.
- **How a caller should correctly COMPUTE `allLeavesResolvable` (binding, issue #487 T5b):** it
  is NOT simply "some index coverage exists." A caller MUST require
  `AllLeavesIndexable(prog)` (SPEC-QP-5) — an ALL-leaves shape verdict — in addition to whatever
  data-availability check it already performs; the availability check alone (e.g.
  `vibuilder.BuildValueIndexSource`'s own `ok`) is satisfied by ANY ONE resolvable leaf, which
  would wrongly qualify a mixed-shape query (one indexable leaf plus one leaf the index cannot
  represent at all) for `DispatchTimeSliced`. See `SPEC-QP-5`/`NOTES.md` NOTE-QP-009 for the full
  contract and the mixed-shape regression proof.
- When `Strategy == DispatchBlockSharded`: `Root` is the zero `Group{}` and `Slices` is `nil`
  — no plan/slice work is attempted or returned.
- When `Strategy == DispatchTimeSliced`: `Root` is `Plan`'s own output (unchanged), and `Slices`
  is `BuildTimeSlices`'s output (SPEC-QP-2) called with `perMinute` resolved as follows: if
  `g.Lead()` (SPEC-QP-1) returns `ok=true` and `perMinuteForLead != nil`, `perMinute =
  perMinuteForLead(lead.Node)`; otherwise `perMinute` is `nil`, which drives `BuildTimeSlices`'s
  uniform-width fallback per SPEC-QP-2. **Amended 2026-07-14 (issue #499, Phase 3):**
  `BuildTimeSlices`'s second parameter, `andConjoinedSignals`, is resolved separately from
  `collectANDConjoinedLeaves(g)` (SPEC-QP-10), reusing the already-resolved lead-leaf signal
  above when the lead leaf is itself AND-conjoined to root (the dedup optimization, SPEC-QP-12)
  and calling `perMinuteForLead` once more for every other AND-conjoined leaf. This drives
  `SkipDispatch` per SPEC-QP-12 only — it has no effect on `EstMatches`/`EstKnown`/`VCNTEmpty`,
  which are governed exclusively by `perMinute` as described above.
- `k` is forwarded unchanged to `BuildTimeSlices` — `BuildQueryPlan` does not special-case it.
  See `SPEC-QP-2`'s `DefaultK` ruling for the recommended default when a caller has no better
  value to supply.
- `perMinuteForLead` is called at most once per `BuildQueryPlan` call FOR THE LEAD LEAF
  specifically — never before `Lead()` has run, and never at all (for any leaf) when
  `allLeavesResolvable` is false. **Amended 2026-07-14 (issue #499, Phase 3, SPEC-QP-12):** this
  bullet's older, stricter form ("never for any other leaf... never... when `Lead()` returns
  `ok=false`") no longer holds — `perMinuteForLead` MAY now additionally be called once for every
  leaf `collectANDConjoinedLeaves(g)` returns that is NOT the lead leaf (SkipDispatch's own
  resolution, independent of whether `Lead()` succeeds at all), including when `Lead()` returns
  `ok=false` (a plan with no Known-cost leaf can still have AND-conjoined leaves needing their own
  per-minute check). The lead leaf itself is still resolved at most once (the dedup optimization,
  `SPEC-QP-12`) — this amendment widens the SET of leaves `perMinuteForLead` may be called for, it
  does not remove the at-most-once-per-leaf guarantee.
- The zero-value `QueryPlan{}` has `Strategy == DispatchBlockSharded` (Go's int zero value for
  `DispatchStrategy`) — the safe default requires no explicit construction.
- When `Strategy == DispatchTimeSliced` and every `TimeSlice.EstKnown` in `Slices` is `false`
  (the resolvable-but-VCNT-blind case), the caller's dispatch order is `Slices`' own
  chronological order, unmodified — see `SPEC-QP-2`'s degenerate-case rule and `NOTES.md`
  NOTE-QP-006.

Back-ref: `internal/modules/queryplan/queryplan.go:DispatchStrategy,QueryPlan,BuildQueryPlan`.
See `NOTES.md` NOTE-QP-006. Tests: `queryplan_test.go` (including
`TestBuildQueryPlan_InvertedTimeRangeFallsBackToBlockSharded`,
`TestBuildQueryPlan_HugeWindowFallsBackToBlockShardedNoPanic`). Issue #487.

**Addendum 2026-07-08 (issue #481 parts 2-3, F-5) — `DispatchStrategy` gains a third value,
`DispatchBoundedRecentFirst`.** The "`DispatchStrategy` is an enum: `DispatchBlockSharded` (zero
value) or `DispatchTimeSliced`" bullet above is now incomplete, not wrong — both of those values
and their `BuildQueryPlan`-computed selection rule are UNCHANGED by this addendum.
`DispatchBoundedRecentFirst` is a THIRD value, selected by a completely different function
(`SelectSearchStrategy`, `SPEC-QP-6`) for a completely different caller context (tempo's
search-query frontend, not `BuildQueryPlan`'s own #487 time-slice qualification) — `BuildQueryPlan`
itself never returns `DispatchBoundedRecentFirst`, and `SelectSearchStrategy` never returns
`DispatchTimeSliced`. The two dispatch-selection functions are siblings sharing one enum type, not
one function with an expanded contract; see `SPEC-QP-6` for the full contract this value belongs
to.

---

## SPEC-QP-4: `VCNTPerMinuteFunc` — per-minute-signal oracle construction, `VCNTCostFunc`'s sibling
*Added: 2026-07-07 (holistic-review Fix 1).*

**Contract:** `VCNTPerMinuteFunc(data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS
uint64) func(leaf *vm.RangeNode) []valuecounts.MinuteCount` closes over one decoded VCNT
section and window, returning a callback suitable for `BuildQueryPlan`'s `perMinuteForLead`
parameter.

**Rules:**
- Uses the exact same `leafEqualityValue`/canonical-encoding extraction `VCNTCostFunc`
  (`vcnt_cost.go`, NOTE-QP-001/003) uses — a same-package call, not a duplicate implementation —
  so the two oracles can never disagree on which leaves are point lookups or how their values
  are canonically encoded.
- Returns `nil` for any leaf shape `VCNTCostFunc` would itself score `UnknownCost()` for
  (range/regex/multi-value/present-only/non-canonically-encodable), or when the underlying
  `valuecounts.SelectivityPerMinute` call errors.
- A leaf that IS a valid equality lookup but has no live VCNT coverage returns an empty
  (possibly non-nil) `[]MinuteCount` via `SelectivityPerMinute`'s own contract — not an error.
  Both `nil` and an empty slice are equivalent "no signal" outcomes for `BuildTimeSlices`
  (`len(perMinute) == 0` either way).
- Performs no object-storage I/O — `data`/`dir` are the caller's already-decoded VCNT section
  bytes, identical to `VCNTCostFunc`'s own inputs.

Back-ref: `internal/modules/queryplan/perminutefrom.go:VCNTPerMinuteFunc`. Tests:
`perminutefrom_test.go`. Issue #487 (holistic-review Fix 1).

---

## SPEC-QP-5: `AllLeavesIndexable` — the correct ALL-leaves input for `BuildQueryPlan`'s `allLeavesResolvable` gate
*Added: 2026-07-07 (issue #487, task T5b)*

**Contract:** `AllLeavesIndexable(prog *vm.Program) bool` reports whether EVERY leaf in `prog`'s
predicate tree has a shape `vibuilder.LeafIndexable` (`vibuilder/SPECS.md` SPEC-VB-3) accepts —
reusing that exact per-leaf decision verbatim, never re-derived.

**Rules:**
- `prog == nil` or `prog.Predicates == nil` returns `false`.
- A program with no `Nodes` and no `Columns` (nothing referenced at all) returns `false`.
- A match-all query (`Nodes` empty, `Columns` populated — e.g. `{} | rate()`) returns `true`:
  there is no per-leaf value predicate to reject in this shape.
- Otherwise, `prog.Predicates.Nodes` is flattened to its leaf `RangeNode`s via a walk that
  descends into every composite child regardless of AND/OR structure — mirroring
  `vibuilder.collectLeaves`' own flat walk (NOT `Plan()`'s AND/OR-structured walk), since this
  function must inspect exactly the same leaf set `BuildSource` itself would touch, independent
  of the query's boolean structure. If flattening yields zero leaves, returns `false`.
- Returns `true` only if `vibuilder.LeafIndexable` is `true` for every flattened leaf; returns
  `false` on the first leaf it isn't.

**This is the ALL-not-ANY correction to a naive `allLeavesResolvable` computation (binding):** a
caller must NOT derive `BuildQueryPlan`'s `allLeavesResolvable` parameter (SPEC-QP-3) from a
data-availability check alone (e.g. whether `vibuilder.BuildValueIndexSource`'s own `ok` return
found ANY coverage) — that is a strictly weaker "at least one leaf resolved" condition
(`vibuilder/NOTES.md` NOTE-VI-036). A query mixing one indexable leaf with one leaf the index
architecturally cannot represent (multi-value OR, negation, a bare `RequirePresent` leaf) would
wrongly qualify for `DispatchTimeSliced` under an ANY-based check. The correct computation is
`allLeavesResolvable := <availability check> && AllLeavesIndexable(prog)`. See `NOTES.md`
NOTE-QP-009 for the full rationale and the mixed-shape regression proof.

Back-ref: `internal/modules/queryplan/indexable.go:AllLeavesIndexable,collectLeafNodes`. See
`NOTES.md` NOTE-QP-009, `SPEC-QP-3` (`BuildQueryPlan`'s gate this function correctly feeds), and
`vibuilder/SPECS.md` SPEC-VB-3 (`LeafIndexable`, the per-leaf decision this reuses). Tests:
`indexable_test.go` (including `TestAllLeavesIndexable_MixedIndexableAndNegatedLeafIsNotIndexable`),
root-level parity test `timeslice_test.go:TestAllLeavesIndexable_RootReexportMatchesQueryplanPackage`.
Issue #487.


---

## SPEC-QP-6: `SelectSearchStrategy` — Search-Query Dispatch Strategy Decision Table
*Added: 2026-07-08 (issue #481 parts 2-3, team-lead ruling R13)*

**Contract:**
```go
func SelectSearchStrategy(sel Selectivity, hasLimit bool) (strategy DispatchStrategy, planTimeDecline bool)
```
A PURE function (no I/O, no tempo context) implementing R13's ruling table for choosing a
search-query dispatch strategy from a VCNT selectivity classification and whether the query
carries a limit. It is the single source of truth for this decision so tempo's frontend
(`buildQueryPlanFromProgram`) consumes it directly instead of hand-rolling an
independently-maintained copy that could drift.

**The five-row core** (Selective's outcome does not depend on `hasLimit`, collapsing what would
otherwise be six selectivity×limit combinations into five distinct outcomes):

| `Selectivity` | `hasLimit` | `strategy` | `planTimeDecline` |
|---|---|---|---|
| `Selective` | any | `DispatchBlockSharded` | `false` |
| `LowSelectivity` | `true` | `DispatchBoundedRecentFirst` | `false` |
| `LowSelectivity` | `false` | `DispatchBlockSharded` | `true` |
| `UnknownSelectivity` | `true` | `DispatchBoundedRecentFirst` | `false` |
| `UnknownSelectivity` | `false` | `DispatchBlockSharded` | `false` |

**`planTimeDecline=true` means the query has NO safe answer at plan time** (`LowSelectivity` with
no limit present: every per-block job would decline identically, so dispatching at all wastes N
block round-trips on a certain failure, per team-lead ruling R6). When `planTimeDecline` is `true`,
the returned `strategy` value is MEANINGLESS and MUST NOT be dispatched — the caller (tempo) must
fail the query at plan time instead, mapping to the typed hard-error family. **This is deliberately
NOT represented as a fake `DispatchStrategy` enum value** (e.g. an invented `DispatchDecline`
constant) — decline is a distinct, non-dispatchable outcome, not a dispatch strategy, so it is
returned out-of-band via this second bool. See `NOTE-QP-010` for the design rationale.

**Deliberately EXCLUDED from this function:** `boundedEligible` (search-vs-metrics) and
resolvability (index-coverage). Both are tempo-context concerns the caller must gate on FIRST —
metrics callers never reach this function at all (team-lead ruling R2: metrics is never
bounded-served, see `SPEC-VIS-2`/`NOTE-VI-096`), and an unresolvable plan is `DispatchBlockSharded`
before selectivity is even considered. This keeps the five-row core here untestable-drift-proof in
one place.

**`DispatchStrategy` gains a third value, `DispatchBoundedRecentFirst`** (see `SPEC-QP-3`'s
2026-07-08 addendum) — the #481 bounded-newest-first strategy, selected by tempo's frontend when a
search query's index coverage is low/unknown but a limit is present. Unlike `DispatchTimeSliced`,
`QueryPlan` carries ONLY this `Strategy` value for the bounded path — no budget fields
(`MaxBlocks`/`MaxBytes`/`MaxDuration`): the querier (tempo's `backend_block.go`) owns and applies
its own budget policy when it sees this signal via blockpack's `QueryOptions.RecentFirstBudget`
(root `SPEC.md` `SPEC-ROOT-023`), mirroring how `IndexOnly`/`MostRecent` are tempo-set booleans on
blockpack's `QueryOptions` rather than blockpack-computed values (plan-f.md Task 5's Option B,
ratified).

Back-ref: `internal/modules/queryplan/queryplan.go:SelectSearchStrategy,DispatchStrategy,DispatchBoundedRecentFirst`.
Tests: `queryplan_test.go` (`TestSelectSearchStrategy_FiveRowCore`,
`TestSelectSearchStrategy_NeverReturnsTimeSlicedOrFakeDeclineStrategy` — see `queryplan/TESTS.md`
`TEST-QP-1`/`TEST-QP-2`). See `NOTES.md` NOTE-QP-010. Issue #481.

**Update (2026-07-12, plan-scan-fallback.md Phase 7, task #190): the five-row table above is
STALE — `DispatchBoundedRecentFirst` has been deleted from `DispatchStrategy` entirely, and the
decision table has collapsed.** Confirmed by direct read of the current implementation:

```go
func SelectSearchStrategy(sel Selectivity, hasLimit bool) (strategy DispatchStrategy, planTimeDecline bool) {
	if sel == LowSelectivity && !hasLimit {
		return DispatchBlockSharded, true
	}
	return DispatchBlockSharded, false
}
```

**The current table (every row, no exceptions):**

| `Selectivity` | `hasLimit` | `strategy` | `planTimeDecline` |
|---|---|---|---|
| `Selective` | any | `DispatchBlockSharded` | `false` |
| `LowSelectivity` | `true` | `DispatchBlockSharded` | `false` |
| `LowSelectivity` | `false` | `DispatchBlockSharded` | `true` |
| `UnknownSelectivity` | any | `DispatchBlockSharded` | `false` |

`strategy` is now `DispatchBlockSharded` in every row — `hasLimit` affects ONLY `planTimeDecline`
(via the single `LowSelectivity && !hasLimit` exceptional case), never the strategy value itself,
since there is no longer a second non-`DispatchBlockSharded` strategy to select between.
`DispatchStrategy` itself is now a genuine 2-value enum (`DispatchBlockSharded`,
`DispatchTimeSliced` — the latter from `SPEC-QP-3`, unrelated to and unaffected by this change).
`LowSelectivity`/`UnknownSelectivity`-with-a-limit queries, which previously activated
`DispatchBoundedRecentFirst`, now dispatch as ordinary `DispatchBlockSharded` — the case is served
correctly and boundedly instead by `SPEC-VI-12`'s early-stopping index resolution (root
`SPEC.md` `SPEC-ROOT-023`'s own retirement note has the full removal rationale), not by a distinct
dispatch strategy at the queryplan layer.

**Why this is an update to SPEC-QP-6, not a separate retirement entry:** `SelectSearchStrategy`
itself is still live, still called the same way, still has the same signature and the same
`(strategy, planTimeDecline)` two-return-value shape and the same "decline is out-of-band, not a
fake enum value" design this entry's own body already argues for. Only ONE table row's outcome
and the enum's cardinality changed — this is a narrowing correction to an existing contract, not
a removal of the contract itself (contrast with `SPEC-ROOT-023`/`SPEC-STREAM-13`/`SPEC-STRUCT-13`/
`14`, which describe mechanisms that no longer exist at all).
Pinned by `queryplan_test.go:TestSelectSearchStrategy_NoLongerReturnsDispatchBoundedRecentFirst`
(Phase 7's own regression test, confirmed present by direct read). **Correction:** the original
`TestSelectSearchStrategy_FiveRowCore` name referenced in this entry's own pre-Phase-7 Tests:
line was NOT left stale — it was renamed to `TestSelectSearchStrategy_FourRowCore`, confirmed by
direct read of `queryplan_test.go`, matching the table's new row count exactly.

Back-ref (current): `internal/modules/queryplan/queryplan.go:SelectSearchStrategy,DispatchStrategy`
(now 2-valued). Tests: `queryplan_test.go:TestSelectSearchStrategy_FourRowCore,TestSelectSearchStrategy_NoLongerReturnsDispatchBoundedRecentFirst`.

**Update (2026-07-24, issue #535, team-lead ruling): the `LowSelectivity`+`hasLimit=false` ->
`planTimeDecline=true` row above is REVERSED — this is a deliberate reversal of team-lead ruling
R6 (issue #481), not a silent behavior change.** The team-lead ruling behind this reversal: "We
cannot decline a valid query merely because it is expensive. A query may only decline when the
index/cube genuinely isn't built yet for the queried window (a real coverage gap, or cube
warming) — never as a cost/selectivity heuristic for a query the system CAN answer correctly."
R6's premise (every per-block job would decline identically for a `LowSelectivity`/no-limit
query, so plan-time decline avoids N certain-to-fail block round-trips) was checked against the
actual execution code and found FALSE: `executor.ExecuteTraceMetricsFromVI`
(`internal/modules/executor/metrics_trace.go`) computes the exact bucketed count over EVERY
matched value-index entry unconditionally — there is no selectivity-based bailout anywhere in
that function — and the unbounded value-index read path used for a no-limit search query
(`vibuilder.BuildSource`) has the identical property: it enumerates and returns everything the
index finds, with no selectivity-based limitation of its own. Neither execution path needed R6's
plan-time safety net; it was declining answerable queries.

**The current table (every row, no exceptions):**

| `Selectivity` | `hasLimit` | `strategy` | `planTimeDecline` |
|---|---|---|---|
| `Selective` | any | `DispatchBlockSharded` | `false` |
| `LowSelectivity` | any | `DispatchBlockSharded` | `false` |
| `UnknownSelectivity` | any | `DispatchBlockSharded` | `false` |

`planTimeDecline` is now `false` in every row — `SelectSearchStrategy` can no longer produce a
decline outcome at all. `hasLimit` no longer affects the outcome in any way. It is still accepted
as a parameter purely for this function's own signature/API stability across ITS OTHER callers
(root `timeslice.go`'s re-export, `cmd/deadcode/main.go`) — tempo's own `buildQueryPlanFromProgram`
no longer calls `SelectSearchStrategy` or branches on either return value at all (that dead call
site, and the sibling metrics-side `sel == blockpack.LowSelectivity` decline branch, were BOTH
removed in the same change; see tempo's own `modules/frontend/vcnt_fetch.go` doc comments). This
does NOT relax anything about genuine coverage gaps — `ErrSearchNoCoverage`/`ErrCubeWarming`/
`ErrMetricsNoCoverage` and friends are untouched by this change; only the cost/selectivity
heuristic (`ErrPlanTimeLowSelectivityNoLimit`, tempo's `modules/frontend/vcnt_fetch.go`) is
removed, since it now has zero remaining production callers.

Back-ref (current): `internal/modules/queryplan/queryplan.go:SelectSearchStrategy` (function body
now unconditionally `return DispatchBlockSharded, false`). Tests:
`queryplan_test.go:TestSelectSearchStrategy_FourRowCore` (retained name, rewritten cases — no
case still expects `planTimeDecline=true`),
`TestSelectSearchStrategy_NeverReturnsTimeSlicedOrFakeDeclineStrategy`,
`TestSelectSearchStrategy_NoLongerReturnsDispatchBoundedRecentFirst`. Real end-to-end regression
coverage (both search and metrics actually dispatching and returning correct results for a
`LowSelectivity`+no-limit query) lives in tempo:
`modules/frontend/vcnt_fetch_test.go`, `tempodb/encoding/vblockpack/vcnt_duration_histogram_integration_test.go`.
Issue #535.

---

## SPEC-QP-7: `LeadDetail` / `ClassifyProgramVCNTWithDetail` — Both-Sides'-Costs Contract

*Added: 2026-07-09 (issue #493, Task 4b, team-lead ruling R4)*

`LeadDetail` (`vcnt_cost.go`) carries the "both sides' costs" a selectivity classification
computes internally and discards: the lead leaf's own estimated matching count (`IndexCost`,
the index/VI/VCNT side numerator) and its column's total live population over the window
(`ColumnTotal`, the full-scan side denominator).

**Fields:** `LeadColumn string`, `IndexCost int64`, `ColumnTotal int64`, `HasLead bool`,
`IndexCostKnown bool`, `ColumnTotalKnown bool`.

**Known-flag semantics:** `HasLead` is `false` when the plan had no lead leaf at all
(`UnknownSelectivity`, no signal) — every other field is then zero-value. `IndexCostKnown`/
`ColumnTotalKnown` are independently `false` whenever their respective oracle had no coverage
for the lead leaf, mirroring `LeafCost`'s own Known/Unknown split (`NOTE-QP-001`).
`IndexCostKnown` is always equal to `HasLead` in practice (`leadLeaf` only ever returns a
Known-cost leaf) — kept as a distinct field for the same defensive symmetry `LeafCost` itself
uses.

**Parity guarantee:** `ClassifyWithThreshold(g, total, fraction)` is implemented as
`sel, _ := classifyDetailed(g, total, fraction); return sel` — `classifyDetailed` IS
`ClassifyWithThreshold`'s implementation, not a parallel reimplementation, so the two can never
diverge. Mirrors `SPEC-QP-1`'s `Group.Lead()`/`leadLeaf` wrapper-can-never-diverge pattern.

`ClassifyProgramVCNTWithDetail(prog, data, dir, minTS, maxTS) (Selectivity, LeadDetail)`
composes `VCNTCostFunc` → `Plan` → `VCNTColumnTotalFunc` → `classifyDetailed`, same default
threshold as `ClassifyProgramVCNT`, zero additional object-storage I/O (same decoded VCNT bytes
the caller already has in hand).

**Root re-export:** `blockpack.LeadDetail` (type alias), `blockpack.ClassifyProgramVCNTWithDetail`
(thin wrapper), `timeslice.go` — same pattern as `ClassifyProgramVCNT`'s own root re-export.

**Deadcode anchoring:** `cmd/deadcode/main.go` anchors both symbols permanently (tempo's
frontend, a separate repo, is the only real caller — blockpack's own deadcode analysis has no
cross-repo visibility, same situation as `ClassifyProgramVCNT`/`SelectSearchStrategy`'s existing
anchors).

Back-ref: `internal/modules/queryplan/vcnt_cost.go:LeadDetail,ClassifyProgramVCNTWithDetail`;
`internal/modules/queryplan/selectivity.go:leadDetail,classifyDetailed`;
`timeslice.go:LeadDetail,ClassifyProgramVCNTWithDetail`; `cmd/deadcode/main.go` (anchor). See
`NOTES.md` `NOTE-QP-011`. Issue #493.

## SPEC-QP-8: `BuildTimeSlices` forces exactly one-minute-wide slices (issue #217)

*Added: 2026-07-13*

Every `TimeSlice` `BuildTimeSlices` returns has `End - Start == 60` (unconditionally, regardless
of `perMinute`, `concurrentRequests`, or `k`). There is no adaptive-width or uniform-width MODE
anymore — those algorithms were removed, not gated. The full-partition invariant (slices
gaplessly cover `[floor(minTS), floor(maxTS)+60)`) and the `EstKnown`/`EstMatches`/`VCNTEmpty`
three-state semantics (`SPEC-QP-2`) are unchanged; only slice WIDTH changed.

`maxSlicesPerPlan` is `50000` (raised from `2000` — see `NOTE-QP-012` for the full before/after
reasoning). A window requiring more than `50000` one-minute slices (>34.7 days) does not qualify
for `DispatchTimeSliced` and `BuildTimeSlices` returns `nil`, which `BuildQueryPlan`'s existing
zero-slices fallback (`SPEC-QP-3`) converts to `DispatchBlockSharded` — no new fallback
mechanism.

Back-ref: `internal/modules/queryplan/slices.go:BuildTimeSlices,maxSlicesPerPlan`. See
`NOTE-QP-012`, `BENCHMARKS.md` BENCH-QP-010. Issue #217.

---

## SPEC-QP-9: `VCNTDurationCostFunc` / `CombineCostFuncs` / histogram-fallback `ColumnTotalFunc` (issue #205)

*Added: 2026-07-13*

**`VCNTDurationCostFunc(data, dir, minTS, maxTS) CostFunc`** (`vcnt_duration_cost.go`) is
`VCNTCostFunc`'s range-predicate sibling for the histogram-eligible column
(`durationHistogramColumn = "span:duration"`, Phase 1's sole eligible column — a single hardcoded
string comparison, not a map). It recognizes exactly two leaf shapes on that column, both mutually
exclusive with `leafEqualityValue`'s equality shape by construction (equality requires
`len(Values)==1`; both shapes below require `len(Values)==0`):

- **Single-bound** (`Min` XOR `Max` set, no `Values`/`Pattern`/`RequirePresent`): maps to
  `valuecounts.OpGT`/`OpGTE`/`OpLT`/`OpLTE` per `MinInclusive`/`MaxInclusive`, converts the raw
  nanosecond `Value.Data` to milliseconds via plain integer division (`nanos/1_000_000`,
  negative-clamped to 0) — deliberately NOT `vibuilder`'s
  `intOrDedicatedColType`/`decidableTimeBucketThreshold` decidability logic, which solves a
  different, unrelated exact-lookup problem and has no bearing on this histogram's
  intentionally-approximate, over-estimating semantics — and answers via
  `valuecounts.DurationHistogram.EstimateThreshold`.
- **Between** (both `Min` and `Max` set, same other constraints): answers via
  `DurationHistogram.EstimateBetween`.

Any other leaf shape (equality, no Min/Max, a column other than `durationHistogramColumn`, or a
column with no histogram coverage at all — `DurationHistogramInRange`'s `Covered=false`) returns
`UnknownCost()`.

**`CombineCostFuncs(first, second CostFunc) CostFunc`** (`vcnt_duration_cost.go`) tries `first`,
and only falls through to `second` when `first` returns `Unknown` — never both, and never sums or
blends the two. Generic: usable with any two `CostFunc`s, not specific to VCNT/duration.

**Composition (`vcnt_cost.go`):** `ClassifyProgramVCNTWithThreshold` and
`ClassifyProgramVCNTWithDetail` both build their `cost` oracle as
`CombineCostFuncs(VCNTCostFunc(data, dir, minTS, maxTS), VCNTDurationCostFunc(data, dir, minTS, maxTS))`
instead of `VCNTCostFunc` alone. No signature change to either function — tempo's existing,
unmodified `ClassifyProgramVCNTWithDetail` call site starts recognizing duration-range leaves with
zero tempo-side code change (see `NOTE-QP-013`).

**Histogram fallback (`VCNTColumnTotalFunc`, `vcnt_cost.go`):** tries the bare discrete-value
`valuecounts.ColumnTotalInRange(data, dir, leaf.Column, ...)` lookup first; only when that has no
coverage (`Covered=false` or an error) does it fall back to summing
`valuecounts.DurationHistogramInRange(data, dir, leaf.Column, ...).Counts` into a single total via
`durationColumnTotalFallback`. A column with bare coverage is never affected by this fallback
(zero regression risk for every pre-existing production column, `TEST-QP-3`'s
`..._PrefersBareColumnWhenBothExist`); a column with ONLY histogram coverage (the real
`span:duration` production shape — no bare-value VCNT records for it exist or are planned) now
gets a real `ColumnTotal` instead of always reading `ok=false` (`TEST-QP-3`'s
`..._HistogramFallback_UsedOnlyWhenBareColumnUncovered`).

Back-ref: `internal/modules/queryplan/vcnt_duration_cost.go:VCNTDurationCostFunc,CombineCostFuncs,durationRangeLeafShape,durationBetweenLeafShape,durationValueMillis`;
`internal/modules/queryplan/vcnt_cost.go:ClassifyProgramVCNTWithThreshold,ClassifyProgramVCNTWithDetail,VCNTColumnTotalFunc,durationColumnTotalFallback`.
See `NOTE-QP-013`, `TESTS.md` `TEST-QP-3`. Issue #205.

---

## SPEC-QP-10: `collectANDConjoinedLeaves` — OR-ancestry-aware leaf enumeration (issue #499, Phase 2)

*Added: 2026-07-14*

**`collectANDConjoinedLeaves(g Group) []PlannedLeaf`** (`and_conjoined.go`) returns every leaf in
`g`'s tree whose path from `g` down to that leaf passes through zero `GroupOR` groups — i.e. every
leaf that is AND-conjoined all the way to `g`'s own root. It explicitly checks `sub.Kind` at every
recursion level (`if g.Kind != GroupAND { return nil }`, applied at each recursive call, not just
the top) rather than relying on `Plan()`'s current "an AND group's sub-groups are always OR
groups" construction invariant (`planAndNodes`/`planOrNode`, `plan.go`) — so it stays correct even
if a future refactor of that construction changes the incidental structure.

**Contract:**
- A pure-AND tree with no `GroupOR` anywhere returns every leaf in the tree, order-independent.
- Any leaf sitting under a `GroupOR` — at any nesting depth — is excluded, along with every other
  leaf nested beneath that same `GroupOR`, regardless of whether an inner sub-group further below
  the `GroupOR` is itself `GroupAND` (recursion into the `GroupOR` itself returns `nil`
  immediately, so nothing beneath it is ever visited).
- `collectANDConjoinedLeaves(Group{})` (the zero value, `Kind == GroupAND`) returns `nil`, not a
  panic or an empty-but-non-nil slice.
- A nested pure-AND composite that `Plan()`'s own `planAndNodes` flattening rule already merges
  into a single `GroupAND` (never its own `SubGroup` entry) contributes every one of its leaves —
  "nested AND" (fully collected) and "nested under an OR" (excluded) are the only two shapes that
  matter to this function.

**Never changes `leadLeaf`/`Group.Lead()`'s own behavior.** This is a wholly separate, additive
tree walk with no shared state or control flow with `leadLeaf` (`selectivity.go`) or `Lead()`
(`lead.go`) — confirmed by a zero-diff to either file. It is consulted only by the new
skip-computation path issue #499 Phase 3 adds to `BuildQueryPlan` (`queryplan.go`), never by
`Classify`/`classifyDetailed` (`selectivity.go`'s production caller of `leadLeaf`, line ~131) or by
`BuildQueryPlan`'s existing lead-leaf dispatch-priority resolution (`queryplan.go`'s
`perMinuteForLead(lead.Node)` call, line ~115). Each AND-conjoined leaf this function returns is
independently sufficient to prove a minute's whole-plan match count is zero when its own
per-minute signal is confidently zero for that minute, since an AND is false the moment any one
conjunct is false — regardless of any sibling OR branch or any other AND-conjoined leaf. This is
distinct from, and does not narrow or widen, `leadLeaf`'s own "most-selective leaf across the
whole tree" contract (`SPEC-QP-1`), which recurses into sub-groups unconditionally for a different
(priority/bound) purpose and correctly considers a leaf nested inside an OR branch's AND sub-group
as a legitimate lead candidate — `collectANDConjoinedLeaves` would exclude that exact same leaf
from its own returned set, and both facts are simultaneously true and non-contradictory (see
`NOTE-QP-014`).

Back-ref: `internal/modules/queryplan/and_conjoined.go:collectANDConjoinedLeaves`. See
`NOTE-QP-014`, `TESTS.md` `TEST-QP-4`. Issue #499.
---

## SPEC-QP-11: `VCNTDurationPerMinuteFunc` / `CombinePerMinuteFuncs` — per-minute duration signal, and `TimeSliceOracle`'s composed pair (issue #499, Phase 1)
*Added: 2026-07-14*

**`VCNTDurationPerMinuteFunc(data, dir, minTS, maxTS) func(*vm.RangeNode) []valuecounts.MinuteCount`**
(`vcnt_duration_perminute.go`) is `VCNTPerMinuteFunc`'s (SPEC-QP-4) range-predicate sibling for the
histogram-eligible column, mirroring `VCNTDurationCostFunc`'s (SPEC-QP-9) relationship to
`VCNTCostFunc`. It recognizes exactly the same two leaf shapes `VCNTDurationCostFunc` recognizes
(reusing `durationRangeLeafShape`/`durationBetweenLeafShape` directly, same package, unexported —
never duplicated) and answers via `valuecounts.DurationHistogramPerMinuteInRange` plus the SAME
`EstimateThreshold`/`EstimateBetween` estimators `VCNTDurationCostFunc` uses — a thin adapter, not
a second implementation.

**Contract:** returns `nil` for any leaf shape those extractors reject (equality, non-histogram
column, or any other non-range shape) — mutually exclusive with `VCNTPerMinuteFunc`'s equality
shape by construction, the same guarantee `VCNTDurationCostFunc`/`VCNTCostFunc` rely on. A minute
whose estimate is unknown or `<= 0` is excluded from the returned slice entirely — never a
`Count: 0` entry — mirroring `VCNTPerMinuteFunc`'s own "absent minute vs. zero-count minute is not
a meaningful distinction to this contract" liveness convention, applied per-minute. Performs no
object-storage I/O: `data`/`dir` are already-decoded in-memory bytes, identical to
`VCNTDurationCostFunc`'s own inputs.

**`CombinePerMinuteFuncs(first, second func(*vm.RangeNode) []valuecounts.MinuteCount) func(*vm.RangeNode) []valuecounts.MinuteCount`**
(`vcnt_duration_perminute.go`) mirrors `CombineCostFuncs`'s (SPEC-QP-9) try-first-then-second
semantics for the per-minute callback shape: tries `first`, and only falls through to `second`
when `first` returns a `nil` or empty (`len() == 0`) result — never both, never merges/concatenates
the two. Generic: usable with any two per-minute callbacks, not specific to VCNT/duration.

**`TimeSliceOracle`'s composed pair (root `timeslice.go`):** since issue #499 Phase 1,
`TimeSliceOracle` returns `queryplan.CombineCostFuncs(VCNTCostFunc(...), VCNTDurationCostFunc(...))`
and `queryplan.CombinePerMinuteFuncs(VCNTPerMinuteFunc(...), VCNTDurationPerMinuteFunc(...))`
instead of the bare pre-#499 `VCNTCostFunc`/`VCNTPerMinuteFunc` pair — **both compositions are
wired together in the same change, never independently.** Composing cost alone (without the
per-minute composition) would let a duration-range leaf legitimately win `Lead()` by cost while
still returning zero per-minute signal (the un-composed `VCNTPerMinuteFunc` only recognizes
equality leaves, so it returns `nil` for a Min/Max-bearing leaf) — a strict regression versus
`TimeSliceOracle`'s own pre-#499 behavior (every slice degrading to `EstKnown=false`) for a query
that used to get real per-slice signal from its equality-cost-Unknown duration leaf's fallback path.
`TimeSliceOracle`'s own exported signature is unchanged by this — only its internal composition.

Back-ref: `internal/modules/queryplan/vcnt_duration_perminute.go:VCNTDurationPerMinuteFunc,CombinePerMinuteFuncs`;
`timeslice.go:TimeSliceOracle` (root package). See `NOTE-QP-015`, `TESTS.md` `TEST-QP-5`. Issue #499, Phase 1.

---

## SPEC-QP-12: `TimeSlice.SkipDispatch` — multi-leaf, freshness-margin-gated dispatch-skip contract (issue #499, Phase 3)

*Added: 2026-07-14*

**Contract:** `TimeSlice.SkipDispatch bool` (`slices.go`) is `true` for a given slice if and only
if BOTH of the following hold:

1. At least one leaf in `collectANDConjoinedLeaves(g)` (`SPEC-QP-10`) — independent of which leaf
   is `Lead()` — has a confidently-zero per-minute signal for that slice's covered minute. A
   signal is "confidently zero" for a minute when the leaf's own `[]valuecounts.MinuteCount`
   (resolved via `perMinuteForLead`, `BuildQueryPlan`) is either absent for that minute or present
   with `Count <= 0` — the two are equivalent for this question (see the implementation note
   below; this is a DELIBERATE departure from `EstKnown`/`VCNTEmpty`'s own absent-vs-zero
   distinction, which exists for a different reason).
2. That minute's `Start` is NOT within `skipDispatchFreshnessMarginSeconds` (900s / 15 minutes,
   `slices.go`) of the query's own `maxTS` — anchored to `maxTS`, never wall-clock `time.Now()`,
   so `BuildTimeSlices`/`BuildQueryPlan` remain pure functions of their inputs. A `maxTS` smaller
   than the margin makes every minute ineligible (the safe direction), never every minute
   eligible.

**A leaf whose per-minute signal is entirely empty/nil (no coverage at all, e.g. a
VCNT-inestimable shape) contributes NOTHING to the veto** — `BuildQueryPlan`'s own
`len(sig) > 0` gate (`queryplan.go`) excludes it from the checked set entirely, so a
coverage-blind leaf can never accidentally veto every minute in the window just because its
oracle never returns anything. Only a leaf that DOES have per-minute coverage somewhere, but is
absent/zero for THIS specific minute, counts as a confident zero for that minute.

**Independence from `EstMatches`/`EstKnown`/`VCNTEmpty` (binding):** `SkipDispatch` is computed
from an entirely different leaf set (every AND-conjoined leaf) than `EstMatches`/`EstKnown`/
`VCNTEmpty` (the lead leaf only) and the two CAN legitimately disagree on the same slice —
`SkipDispatch=true` can co-occur with `EstMatches>0` when a non-lead AND-conjoined leaf vetoes a
lead leaf's own positive count (an AND is false the moment any one conjunct is false, regardless
of what any other conjunct — including the lead — reports). A caller MUST check `SkipDispatch`
unconditionally, never gated on or inferred from `EstMatches`/`EstKnown`/`VCNTEmpty`.

**Additive, not a removal (amends `SPEC-QP-2`):** the full-partition guarantee is unchanged —
every minute still gets exactly one `TimeSlice`; `SkipDispatch` only marks a slice as
caller-skippable, it never removes it from the returned partition.

**`BuildQueryPlan`'s resolution (`queryplan.go`), including the oracle-call dedup:** for every
leaf `collectANDConjoinedLeaves(g)` returns, if that leaf IS the lead leaf (`g.Lead()`), its
already-resolved `perMinute` value (the same one driving `EstMatches`) is reused rather than
calling `perMinuteForLead` a second time for the same leaf; every other AND-conjoined leaf is
resolved via a fresh `perMinuteForLead` call. This widens `perMinuteForLead`'s own call-cardinality
contract — see `SPEC-QP-3`'s 2026-07-14 amendment.

**`BuildTimeSlices`'s mechanism (`slices.go`):** `andConjoinedSignals [][]valuecounts.MinuteCount`
(one entry per AND-conjoined leaf's own signal) is precomputed into one zero-lookup map per leaf,
outside the per-minute loop; the freshness cutoff (`maxTS - skipDispatchFreshnessMarginSeconds`,
saturating at 0) is computed once; the per-minute loop then does only map lookups, never re-scans.
`andConjoinedSignals == nil` (every pre-#499 caller's shape) means the veto loop never executes —
`SkipDispatch` is `false` for every slice, byte-identical to pre-#499 behavior.

Back-ref: `internal/modules/queryplan/slices.go:TimeSlice.SkipDispatch,BuildTimeSlices,skipDispatchFreshnessMarginSeconds`;
`internal/modules/queryplan/queryplan.go:BuildQueryPlan`. See `NOTE-QP-016`, `TESTS.md` `TEST-QP-6`,
`SPEC-QP-2` (amended), `SPEC-QP-3` (amended), `SPEC-QP-10` (`collectANDConjoinedLeaves`). Issue #499, Phase 3.
