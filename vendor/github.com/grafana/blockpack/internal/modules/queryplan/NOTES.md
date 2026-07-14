# queryplan — NOTES

Design decisions for the cost-based predicate query-planning primitive. Keep in sync
with `doc.go` and the `.go` sources.

## NOTE-QP-001: general cost-based leaf scoring + AND/OR-aware combination (issue #485)

*Added: 2026-07-06*

**What this is.** The general query-planning primitive underneath #484. #484 built a
concrete VCNT selectivity oracle (`valuecounts.SelectivityInRange`) but wired no
consumer (`cmd/deadcode/main.go` explicitly notes the consumer is "not yet wired").
This package is that consumer's *foundation*: a generic `LeafCost` + `CostFunc`
abstraction and an AND/OR-aware tree walk (`Plan`) that scores leaves and orders them
correctly per boolean structure. It is grounded in prior art (System R's cost-based
access-path selection, 1979; Lucene's `ConjunctionDISI` lead-iterator model) rather
than reinvented ad hoc.

**Scope boundary (deliberate).** This issue is the primitive ONLY — the tree walk,
the cost abstraction, and the two group behaviors. It produces a *plan* (an ordered
`Group` tree) and performs no I/O. Actually walking the plan to resolve leaves in
order and apply the empty-AND short-circuit is #484 Phase 2-3's `vibuilder.BuildSource`
wiring, explicitly out of scope here per the issue's stated sequencing (#483 → #484
Phase 1 → THIS → #484 Phases 2-3). Keeping the plan pure is what makes both AND and OR
behaviors independently unit-testable without object storage.

### The three-state cost, and why Unknown ≠ zero

`LeafCost` carries a `Known` flag distinct from `Count`. The correctness crux (also
the lesson in the VCNT `SelectivityEstimate.Covered` contract, NOTE-VC-013): "no cost
signal" and "the oracle affirmatively says zero matches" are **directionally
opposite** for ordering.

- `KnownCost(0)` (`.IsEmpty() == true`): maximally selective — sorts FIRST, and inside
  an AND legitimately short-circuits the whole group to empty.
- `UnknownCost()` (`.IsEmpty() == false`): no signal — sorts LAST, never triggers a
  short-circuit.

Collapsing Unknown into `Count == 0` would make an unmeasured predicate masquerade as
a cheap zero and wrongly empty an AND that actually has matches. `LeafCost.less`
enforces this: Known sorts before Unknown regardless of count; `orderAscending` uses a
stable `origin` tie-break so equal-cost / both-Unknown leaves keep tree order.

### AND vs OR — the flattening trap

`vibuilder.collectLeaves` flattens the whole tree, discarding AND/OR structure. That
is correct *there* (every leaf is resolved independently and the compiled program tree
re-combines them), but any cost-based *ordering/short-circuiting* must NOT flatten,
or it would short-circuit across an OR boundary — wrong. So `Plan` walks
`vm.RangeNode`'s real structure (`IsOR` distinguishes composite kind; leaves have
`Column != ""` and no `Children`):

- **AND group** (`planAndNodes`): a maximal AND-only subtree. Top-level `Nodes` are
  implicitly AND-combined, so they seed the root `GroupAND`. A nested *non-OR*
  composite is **flattened up** into its parent AND (the whole AND chain is one
  orderable pool). Leaves sort cost-ascending; `CanShortCircuit() == true`.
- **OR group** (`planOrNode`): every branch is independently required for
  correctness, so there is **no free skip**. The only optimization is evaluation
  *order* (cheapest branch first — a hint for a limit-bounded caller that can stop
  once it has enough matches). Nested OR-under-OR is flattened (OR of ORs is one OR);
  an AND-subtree branch of an OR becomes its own nested `GroupAND` sub-group,
  cost-ordered recursively (its internal short-circuit is valid *within* the branch).
  `CanShortCircuit() == false`.

The two behaviors share one `Group` struct but are documented and consumed
differently; `CanShortCircuit()` is the single switch a plan-walker keys on.

### Prior art not reused

`internal/modules/queryplanner`'s NOTE-019 `intersectBySelectivity` was the closest
in-codebase precedent, but §8 of that package's NOTES records it (and all of
`selection.go`) was **deleted outright** when the on-disk range-index section was
retired — it could only ever operate on empty inputs. So there was nothing live to
reuse or mirror; this walk is written fresh over the value-index leaf layer.

### VCNT as the first concrete CostFunc

`vcnt_cost.go`'s `VCNTCostFunc` is the FIRST (initially only) concrete `CostFunc`,
kept in a separate file so the core (`cost.go`/`plan.go`) has zero VCNT dependency and
other oracles (per-block min/max/bloom, a no-signal default) can plug in identically.
It returns `Known` only for single-value equality leaves it can canonically encode
(same scope as #484's `SelectivityInRange`); range/regex/multi-value/present-only
leaves and uncovered lookups return `UnknownCost()`.

**Back-ref:** `internal/modules/queryplan/{cost,plan,vcnt_cost}.go`;
`internal/modules/valuecounts/query.go:SelectivityInRange` (NOTE-VC-013, the cost
oracle this plugs in); `internal/modules/vibuilder/builder.go:collectLeaves` (the
flatten-is-fine-there path this walk deliberately does NOT mimic).
**Tests:** `cost_test.go` (three-state cost + ordering), `plan_test.go` (AND ordering,
OR order-hint-not-short-circuit, nested AND-under-OR recursion, AND/OR flattening,
Unknown-sorts-last, known-zero-vs-unknown distinction), `vcnt_cost_test.go` (concrete
oracle known/uncovered/net-zero/time-window/non-equality + an end-to-end
VCNT-drives-`Plan` ordering proof).

## NOTE-QP-002: selectivity classification — recognize low-selectivity predicates (issue #486)

*Added: 2026-07-06*

**What this is.** The recognition primitive #481 part 2 needs before its remaining
routine-decline scan fallback can be removed. #481 warns that hard-erroring the fallback
would turn broad, low-selectivity predicates (`kind=server`, which matches most spans)
from a slow success into a hard failure. The design answer (#486): for a `limit`-bounded
search against a low-selectivity predicate, prefer a pointed, recent-block-first,
limit-bounded execution — but the executor first has to RECOGNIZE that a predicate is
low-selectivity. `Classify` is that recognition, built on the same VCNT signal and the
same `Plan` walk as NOTE-QP-001.

**The signal.** A `column = value` leaf's selectivity fraction is its net live matching
count (NOTE-VC-013's `SelectivityInRange`, the numerator, already surfaced as `LeafCost`)
divided by the column's total live-span population (NOTE-VC-014's `ColumnTotalInRange`, the
denominator, surfaced here as `ColumnTotalFunc`). A leaf whose value accounts for >= a
threshold fraction of its column is low-selectivity: index pruning on it skips
< (1 - fraction) of the work, so a pointed limit-bounded scan wins.

### Classify the LEAD leaf, not the whole plan

`Classify` keys the decision on the plan's LEAD leaf — the lowest-Known-cost leaf across
the whole tree (`leadLeaf` recurses into sub-groups). Rationale: the executor leads an AND
with its cheapest leaf and narrows from there, so the lead leaf UPPER-BOUNDS how much
pruning can possibly skip. If even the most-selective leaf matches most of its column, no
leaf prunes well and pointed execution is right. For an OR the lead leaf is the cheapest
disjunct, and an OR can be at most as selective as its most-selective branch (every branch's
matches are unioned in) — so the same lead-leaf bound is the correct, conservative signal for
both group kinds. This is why classification lives HERE (over the cost-scored `Plan`) rather
than as a naive per-leaf loop: it must respect the AND-lead / OR-union structure the plan
already encodes.

### Three-state, mirroring LeafCost (Known/Unknown ≠ zero)

`Selectivity` is `{UnknownSelectivity, Selective, LowSelectivity}`, mirroring the
Known/Unknown split in NOTE-QP-001. "No signal" (no leaf had a Known cost, or the lead
leaf's column has no population coverage) is `UnknownSelectivity` — NOT `Selective` and NOT
`LowSelectivity` — so the caller applies its own no-signal policy (keep the existing
index-source-over-window default) rather than the planner silently picking a strategy on no
evidence. A net-zero (`IsEmpty`) lead leaf is the opposite extreme: it prunes everything, so
it always classifies `Selective` and short-circuits before any denominator lookup.

### Threshold is a policy, not a benchmark constant

`DefaultLowSelectivityFraction = 0.5` is the natural "the majority of spans match, pruning is
a net loss" boundary, deliberately conservative and NOT tuned to any specific benchmark
predicate (per the repo's no-benchmark-specific-constants rule). `ClassifyWithThreshold` lets
a caller pass its own policy; an out-of-range fraction (<= 0 or > 1) falls back to the default
so a caller cannot accidentally classify everything (or nothing) as low-selectivity. `>=`
threshold comparison means exactly-at-threshold classifies low (matching half the column is
already a pruning loss).

### Scope boundary (deliberate, mirrors NOTE-QP-001)

This is the RECOGNITION primitive only — it produces a `Selectivity` verdict and performs no
I/O (the concrete `VCNTColumnTotalFunc`, like `VCNTCostFunc`, closes over already-decoded
in-memory VCNT bytes). Wiring the verdict into the executor's actual strategy choice
(pointed recent-block-first vs index-source-over-window) and then removing #481's routine-
decline scan fallback is the tempo-side follow-on, exactly as #484/#485 shipped their
primitives before wiring a consumer. Keeping the verdict pure makes it independently
unit-testable without object storage.

**Back-refs:** `internal/modules/queryplan/{selectivity,vcnt_cost}.go`;
`internal/modules/valuecounts/query.go:ColumnTotalInRange` (NOTE-VC-014, the denominator
oracle). **Tests:** `selectivity_test.go` (three-state, lead-leaf selection incl. nested-in-OR,
empty-leaf, threshold boundary/custom/invalid, no-signal cases), `vcnt_cost_test.go`
(`TestVCNTColumnTotalFunc`, `TestVCNTDrivesClassify` end-to-end). Issue #486.

---

## NOTE-QP-003: single VCNT composition entry point for consumers (issue #481 part 2 / #486 consumer wiring)

*Added: 2026-07-06*

**What this is.** `ClassifyProgramVCNT` (and its `...WithThreshold` variant): the single
call a consumer makes to get a `Selectivity` verdict for a program's predicate over one
decoded VCNT section and window. #486 shipped the recognition primitive as four disjoint
pieces — `Plan`, `VCNTCostFunc`, `Classify`, `VCNTColumnTotalFunc` — and left "wiring the
verdict into a consumer" as the follow-on. This is the smallest safe slice of that wiring:
it collapses the boilerplate a consumer would otherwise repeat (build both oracles over the
same inputs, `Plan`, then `Classify`, in that order) into one function, WITHOUT yet touching
any live fallback code path (#481 parts 3–5 remain out of scope — deleting the routine-
decline scan is still blocked on a bounded pointed-execution path existing on the consumer
side).

### Why a composition function and not "let the caller wire it"

The numerator (a leaf's own live count, via `VCNTCostFunc`) and the denominator (its
column's total population, via `VCNTColumnTotalFunc`) are only comparable when measured over
the SAME window. A hand-wired consumer that built the cost func over one `[minTS, maxTS]` and
the column-total func over another would compute a fraction whose numerator and denominator
describe different slices of time — a silently-wrong selectivity that could mislabel a
selective predicate as low-selectivity (or vice versa) and route to the wrong strategy.
Threading a single `(data, dir, minTS, maxTS)` through one call makes that mismatch
unrepresentable. `TestClassifyProgramVCNTSingleWindow` is the proof: the same `server`
predicate classifies LowSelectivity over an early window where it dominates and Selective
over a late window where it is rare — a per-window answer only a single-window composition
can give.

### Contract

`UnknownSelectivity` ("no signal", caller keeps its index-source-over-window default) on: a
nil program, an unplannable predicate (`Plan` ok=false), no leaf with a Known VCNT cost, or a
lead leaf whose column has no VCNT population coverage. Otherwise `Selective` /
`LowSelectivity` per `ClassifyWithThreshold`. No object-storage I/O — inputs are the caller's
already-decoded VCNT bytes, identical to what the two oracles take.

**Back-refs:** `internal/modules/queryplan/vcnt_cost.go`. **Tests:** `vcnt_cost_test.go`
(`TestClassifyProgramVCNT`, `TestClassifyProgramVCNTNoSignal`,
`TestClassifyProgramVCNTSingleWindow`, `TestClassifyProgramVCNTWithThreshold`). Issue #481.

---

## NOTE-QP-004: Lead() — exported accessor for the plan's lead leaf, a thin delegation to leadLeaf (issue #487)

*Added: 2026-07-07*

**What this is.** `Group.Lead() (PlannedLeaf, bool)` exposes the same "most-selective leaf"
concept `selectivity.go`'s unexported `leadLeaf` already computes internally for
`ClassifyWithThreshold` (NOTE-QP-002) — as its own public accessor, for callers (time-slice
construction, #487) that need the lead leaf itself, not a derived `Selectivity` verdict.

**Implementation: a thin delegation, not a parallel implementation.** `Lead()` is a one-line
wrapper — `return leadLeaf(g)` — not an independently maintained re-implementation. An earlier
version of this change (task #142) duplicated `leadLeaf`'s walk into `lead.go` to avoid touching
`selectivity.go` (a conflict-minimization decision, since `selectivity.go` was not on that
task's concurrent-edit list). Reviewer-2-c flagged the duplication as an unnecessary drift risk
(task #147, MEDIUM finding): `lead.go` and `selectivity.go` are both `package queryplan`, so
calling the unexported `leadLeaf` directly requires zero edits to `selectivity.go` and carries
none of the collision risk the original duplication was trying to avoid. Fixed to delegate.

**Contract summary (see SPEC-QP-1 for the formal version, unchanged by this fix):** returns the
leaf with the lowest `Known` `LeafCost`, recursing into `Group.SubGroup`. Unknown-cost leaves are
never chosen. `ok=false` when no leaf in the tree carries a Known cost. Ties between equal Known
costs resolve to whichever leaf is encountered first in the tree walk (direct `Leaves` in order,
then `SubGroup` recursion in order) — this is `leadLeaf`'s own tie-break rule, inherited
unchanged since `Lead()` now calls it directly.

Back-refs: `internal/modules/queryplan/lead.go:Group.Lead`,
`internal/modules/queryplan/selectivity.go:leadLeaf` (the function this delegates to). Tests:
`lead_test.go:TestGroupLead_FindsLowestKnownCostAcrossSubGroups`. Issue #487.

---

## NOTE-QP-005: BuildTimeSlices — full-partition guarantee, adaptive-width clamp, EstKnown/VCNTEmpty/EstMatches three-way semantics, and chronological (not priority) ordering (issue #487)

*Added: 2026-07-07*

> **SUPERSEDED by `NOTE-QP-012` (issue #217, 2026-07-13).** This note describes the pre-#217
> adaptive/uniform-width algorithm (`adaptiveWidthSlices`/`uniformWidthSlices`/`clampWidth`/
> `desiredSliceCount`), which was DELETED, not merely disabled or bypassed — see `NOTE-QP-012`
> for what actually ships today (every slice forced to exactly `minSliceWidthSeconds`, 60s, wide).
> The three-way `EstKnown`/`VCNTEmpty`/`EstMatches` semantics and the full-partition/no-gap
> guarantee documented below are still accurate and unchanged by #217 — only the WIDTH-sizing
> algorithm (adaptive-vs-uniform, the clamp, `desiredSliceCount`) is obsolete. Kept for historical
> context (this note's rationale for the three-way semantics and the full-partition invariant is
> still the authoritative explanation for those parts); do not use the width-sizing sections below
> as a description of current behavior.

**What this is.** `BuildTimeSlices(perMinute, minTS, maxTS, concurrentRequests, k)
[]TimeSlice` partitions a query window into minute-aligned `TimeSlice`s for #487's time-slice
job sharding, sized adaptively from `valuecounts.SelectivityPerMinute`'s per-minute VCNT
signal (NOTE-VC-016) when available, falling back to uniform width when it isn't.

**Invariant: always fully partitions, never drops a gap (safety-critical).** A minute with no
entry in `perMinute` is NOT skipped or omitted — it still advances the current slice's width
and the resulting slice is marked `VCNTEmpty: true` rather than dropped. `[minTS, maxTS]`
(floored/widened to whole minutes) is therefore ALWAYS covered by contiguous, abutting slices
with zero holes. Pinned by `TestBuildTimeSlices_VCNTGapMarkedEmptyNeverSkipped`.

**Three-way `EstKnown`/`VCNTEmpty`/`EstMatches` semantics — mirrors `LeafCost`'s Known/Unknown
split (NOTE-QP-001), do not collapse the two booleans into one.**
- `EstKnown=true, VCNTEmpty=false, EstMatches>0` — real per-minute VCNT signal was found for
  this slice's covered minutes (adaptive path).
- `EstKnown=true, VCNTEmpty=true, EstMatches=0` — real VCNT signal was fetched (`perMinute` was
  non-empty for the whole plan) but this specific slice's covered minutes summed to zero — an
  affirmative "checked, found empty" (which may mean genuinely zero matches OR VCNT lag for a
  recent window, brainstorm-c.md risk 2 — callers deprioritize, never skip).
- `EstKnown=false, VCNTEmpty=false, EstMatches=0` — the uniform-width no-signal fallback: no
  VCNT signal was EVER fetched for the plan (empty `perMinute`), so there is nothing to report
  as "checked and empty." `VCNTEmpty` stays `false` here specifically because it would otherwise
  falsely claim an affirmative empty-check that never happened.
A caller must never conflate `EstKnown=false` (no signal, no basis to prioritize/deprioritize)
with `EstKnown=true, VCNTEmpty=true` (signal fetched, affirmatively found empty) — the two
`EstMatches=0` cases mean opposite things for dispatch-priority purposes.

**Adaptive-width clamp: [60s, 3600s], per #487's design doc.** Every slice's width — both the
adaptive path's accumulated width and the no-signal uniform-width fallback's computed width —
is clamped to `[minSliceWidthSeconds=60, maxSliceWidthSeconds=3600]` (1 minute to 1 hour).
`clampWidth` floors to a minute boundary first, then clamps. The adaptive path additionally
stops accumulating and emits a slice as soon as ANY of three conditions is met: the window's
last minute is reached, `maxSliceWidthSeconds` is hit, or the accumulated matches reach
`target = total matches / desiredSliceCount` (and at least one minute has been accumulated) —
whichever comes first. `desiredSliceCount = concurrentRequests * k`, clamped to a minimum of 1.
The target-reached and last-minute stop conditions are pinned by
`TestBuildTimeSlices_AdaptiveWidthFromMinuteCounts`; the adaptive path's own `reachedMax` stop
condition (hitting the 3600s ceiling while still short of `target`) is pinned independently by
`TestBuildTimeSlices_AdaptiveWidthRespectsMaxWidthClamp` (mutation-verified — weakening
`reachedMax` fails only this test); the uniform-fallback clamp (a separate code path,
`clampWidth` applied to `uniformWidthSlices`' computed width) is pinned by the three
`TestBuildTimeSlices_EmptyMinuteCounts_UniformWidthFallback` subtests.

**No-signal fallback formula.** When `perMinute` is empty, `uniformWidthSlices` splits
`[start, end)` into equal-width slices of `window / desiredSliceCount`, clamped the same way,
with every slice `EstKnown: false`, `VCNTEmpty: false`, `EstMatches: 0` (see the three-way
semantics above for why `VCNTEmpty` is `false`, not `true`, here — corrected from an earlier,
incorrect `VCNTEmpty: true` in this same task before this note's binding correction landed).

**Chronological ordering, NOT priority ordering — a deliberate decoupling (brainstorm-c.md
risk 1).** `BuildTimeSlices` returns slices in most-recent-first chronological order — it does
NOT sort by `EstMatches` descending, even though #487's design doc describes dispatch priority
in est-matches-DESC/VCNT-empty-last terms. `TestBuildTimeSlices_OrderingEstMatchesDescThenMostRecentFirstThenVCNTEmptyLast`
names the CALLER's eventual dispatch-priority ordering (tempo's sharder, Section 2 T4) but the
test itself proves the array is NOT re-sorted by `EstMatches` — a recent slice with LOW
`EstMatches` still sorts before an older slice with HIGH `EstMatches`. **Why decouple:**
`EstKnown`/`EstMatches`/`VCNTEmpty` are dispatch-priority HINTS attached to a
chronologically-stable array; re-sorting here would conflate "the canonical slice ordering"
with "a particular caller's dispatch order" — both a chronological-replay caller and a
priority-first caller (tempo's sharder) can consume the same `[]TimeSlice` without either
accommodating the other.

**Degenerate case: all-`EstKnown=false` means dispatch order is ALREADY the complete answer
(team-lead final settlement, plan-c.md Correction log).** When `perMinute` is empty for the
whole plan, every returned slice has `EstKnown=false` (the uniform-width fallback) — there is
no est-matches signal anywhere in the result to prioritize by. In this case the chronological,
most-recent-first order `BuildTimeSlices` already returns IS the complete dispatch order; a
caller must not invent a separate est-matches-based sort over an all-`EstKnown=false` result
(there is nothing to sort by — every slice ties at "unknown"). The only remaining optimization
lever in this case is recency-based tail-cancellation (e.g. a `ShouldQuit`-style early-stop once
enough matches have been found scanning most-recent-first), not re-prioritization. This is the
exact invariant tempo-side task T5 (Section 2, dispatch-priority-ordering construction) needs in
order to build its ordering logic correctly for the VCNT-blind case — flagging the
cross-reference explicitly since T5 is a downstream, cross-repo consumer of this contract, not
yet implemented as of this writing.

**Addendum (2026-07-07, holistic-review Fixes 3/4/5).** Three follow-up fixes, none changing the
invariants documented above:

- **Overflow saturation (Fix 3/4).** `BuildTimeSlices` now guards against a `maxTS` large enough
  that `floorToMinute(maxTS) + minSliceWidthSeconds` (the `end` computation) would wrap `uint64`.
  A new `maxSafeMaxTS` constant (`math.MaxUint64 - maxSliceWidthSeconds - minSliceWidthSeconds`)
  bounds `maxTS`: any caller-supplied `maxTS` beyond it — unreachable with real unix-second
  timestamps, this ceiling sits near 2^64 — is silently saturated down to `maxSafeMaxTS` before
  any window arithmetic runs, guaranteeing every downstream `uint64` addition (each bounded by
  `maxSliceWidthSeconds`) stays below the ceiling. If saturating `maxTS` makes `minTS > maxTS`,
  `BuildTimeSlices` returns `nil`, identical to any other inverted range. Pinned by
  `TestBuildTimeSlices_NearMaxUint64DoesNotOverflow`. See `SPECS.md` SPEC-QP-2 for the formal
  bound.
- **Negative-`Count` defensive clamp (Fix 5, LOW).** `perMinute` is caller-supplied (via
  `BuildQueryPlan`'s `perMinuteForLead`), not guaranteed to honor `SelectivityPerMinute`'s own
  non-negative liveness-rule contract. `adaptiveWidthSlices` now clamps any `MinuteCount.Count <
  0` to `0` before accumulating, so a non-conforming oracle cannot drive `total`/`target`
  negative (which would silently disable the target-reached stop condition) or masquerade as a
  genuine negative signal. Pinned by
  `TestBuildTimeSlices_NegativeCountFromNonConformingOracleClampedToZero`.
- **Redundant conjunct removed (Fix 5, LOW, no behavior change).** `adaptiveWidthSlices`'
  `reachedTarget` computation dropped an always-true `accumWidth >= minSliceWidthSeconds`
  conjunct: `accumWidth` is incremented by `minSliceWidthSeconds` at the top of every loop
  iteration before this check runs, so the conjunct could never be false. Removed as dead logic,
  not a behavior change — no new test needed.

**Slice-count cap — FIXED (holistic-review Fix 6, resolves the latent panic flagged during Fix
3's design).** A full-range window (`BuildTimeSlices(nil, 0, math.MaxUint64, 1, 1)`, as opposed
to a narrow window near the `maxSafeMaxTS` ceiling) previously attempted an allocation on the
order of 10^15 `TimeSlice` structs and panicked (`makeslice: cap out of range`) in
`uniformWidthSlices` — the overflow-saturation fix above prevents `uint64` wraparound but does
not bound how many `maxSliceWidthSeconds`-wide slices a multi-`~2^64`-second window would
otherwise produce; those are two distinct boundary concerns. **Resolution:** a new
`maxSlicesPerPlan = 2000` constant (`2000 * maxSliceWidthSeconds` = 7,200,000s ≈ 83.3 days —
comfortably beyond any realistic query window, while 2000 `TimeSlice` values is a trivial
allocation even in the worst case). Immediately after computing `start`/`end` (post-saturation),
`BuildTimeSlices` checks `(end-start)/maxSliceWidthSeconds > maxSlicesPerPlan`: if even the
WIDEST allowed per-slice width (`maxSliceWidthSeconds`) can't fit the window under the cap, no
valid slicing exists within `[minSliceWidthSeconds, maxSliceWidthSeconds]` at all, and
`BuildTimeSlices` returns `nil` rather than attempting the allocation — per SPEC-ROOT-001, a
public-API-reachable panic is forbidden regardless of how absurd the input is. **No new
invariant needed at the `BuildQueryPlan` layer:** the existing Fix 2 zero-`Slices` fallback
(NOTE-QP-006's own addendum) already converts this `nil` into `DispatchBlockSharded` — an
oversized window simply does not qualify for slice mode, exactly like an inverted range does
not. The boundary story for `BuildTimeSlices`' window handling now has three parts: the
60-second minute floor, the `maxSafeMaxTS` overflow-saturation ceiling, and this
`maxSlicesPerPlan` slice-count cap. Pinned by `TestBuildTimeSlices_HugeWindowIsCappedNotPanicking`
(a `math.MaxUint64`-wide window returns `nil`, no panic) and
`TestBuildTimeSlices_RealisticLongWindowStillSlicesNormally` (a regression guard proving a
realistic 30-day window still slices normally — the cap is not too tight for real traffic); a
root-level mirror, `TestBuildQueryPlan_HugeWindowFallsBackToBlockShardedNoPanic`, proves the same
through the public `BuildQueryPlan`/`timeslice.go` surface. See `SPECS.md` SPEC-QP-2's
slice-count-cap bullet for the formal contract.

Back-refs (CORRECTED 2026-07-13 — `adaptiveWidthSlices`/`uniformWidthSlices`/`clampWidth`/
`desiredSliceCount` were deleted by #217/`NOTE-QP-012` and no longer exist; see that note for the
current algorithm): `internal/modules/queryplan/slices.go:TimeSlice,BuildTimeSlices,maxSafeMaxTS,maxSlicesPerPlan`.
Tests (CORRECTED 2026-07-13 — `_AdaptiveWidthFromMinuteCounts`/`_AdaptiveWidthRespectsMaxWidthClamp`
no longer exist; see `NOTE-QP-012`'s own back-refs for the current width-forcing tests):
`slices_test.go` (`TestBuildTimeSlices_MinuteAligned`,
`_OrderingEstMatchesDescThenMostRecentFirstThenVCNTEmptyLast`,
`_VCNTGapMarkedEmptyNeverSkipped`, `_EmptyMinuteCounts_UniformWidthFallback`,
`_NearMaxUint64DoesNotOverflow`, `_NegativeCountFromNonConformingOracleClampedToZero`,
`_HugeWindowIsCappedNotPanicking`, `_RealisticLongWindowStillSlicesNormally`). See `NOTES.md`
(`valuecounts`) NOTE-VC-016 (the per-minute signal source this consumes) and this file's
NOTE-QP-006 (the Strategy-vs-estimability gate distinction this field feeds). Issue #487.

---

## NOTE-QP-006: BuildQueryPlan — single-gate qualification contract; "option (ii)" considered and rejected (issue #487, binding ruling)

*Added: 2026-07-07*

**What this is.** `BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS,
maxTS, concurrentRequests, k) QueryPlan` composes `Plan()` (leaf-cost tree), `Group.Lead()`
(NOTE-QP-004), and `BuildTimeSlices` (NOTE-QP-005) into the single top-level call #487's
consumer (tempo's frontend) makes to decide between today's per-block job dispatch
(`DispatchBlockSharded`) and the new time-sliced dispatch (`DispatchTimeSliced`).

**Single-gate contract (binding): `allLeavesResolvable` is the ONLY gate on `Strategy`.**
`allLeavesResolvable` is the caller-supplied #481 index-coverage verdict — blockpack's `Plan`/
`Group` has no notion of "index coverage" itself (that's a tempo/vibuilder-side concept), so it
is threaded in rather than re-derived. `Strategy` is `DispatchTimeSliced` if and only if
`Plan()` itself succeeds (`ok=true`, there is something to plan) AND `allLeavesResolvable` is
true. Nothing about lead-leaf VCNT-estimability enters this gate.

**"Option (ii)" — decline slice mode when the lead leaf is VCNT-inestimable — was considered
and explicitly rejected by a binding team-lead ruling.** The task's original description gated
`Strategy` on BOTH index-coverage resolvability AND the lead leaf carrying a Known VCNT cost
(i.e., decline `DispatchTimeSliced` and fall back to `DispatchBlockSharded` whenever
`Group.Lead()` returns `ok=false`, or `perMinuteForLead` returns empty for the lead leaf that
is found). This was rejected: a resolvable-but-VCNT-blind plan (a regex/range/NEQ/numeric-between
lead leaf, or simply no leaf ever getting a Known VCNT cost) is still perfectly safe to dispatch
in time-sliced mode — `BuildTimeSlices`' own uniform-width fallback (NOTE-QP-005) already
handles "no signal" correctly and safely (full-partition guarantee still holds, `EstKnown=false`
slices carry no false claim). Declining slice mode here would throw away a real, independent
benefit of time-sliced dispatch (finer-grained, more parallelizable jobs) purely because the
WIDTH-SIZING heuristic has no signal to work with — conflating "can I size slices intelligently"
with "can I safely use slices at all," which are different questions with different answers.
**Estimability governs slice WIDTH MODE only (adaptive vs. uniform), never `Strategy`.** Pinned
by the binding-ruling-3 regression guard,
`TestBuildQueryPlan_QualifiesWithUniformSlicesWhenNoLeafIsVCNTEstimable`.

**`perMinuteForLead` is resolved lazily, after `Lead()`, not before.** `BuildQueryPlan` calls
`g.Lead()` first to identify which leaf is lead, then calls `perMinuteForLead(lead.Node)` only
for that one leaf — the caller never has to guess or pre-fetch per-minute VCNT data for every
leaf in the tree ahead of time, only for whichever one turns out to be lead.
`perMinuteForLead` is never called at all when `Lead()` returns `ok=false` (no Known-cost leaf
exists to fetch signal for) or when `allLeavesResolvable` is false (short-circuited before
`Lead()` is even called) — both pinned by test assertions
(`perMinuteForLeadCalled`/`t.Fatal`-in-callback guards) in `_QualifiesWithUniformSlicesWhenNoLeafIsVCNTEstimable`
and `_FallsBackToBlockShardedWhenAnyLeafUnresolvable` respectively.

**`DispatchBlockSharded` is the zero value — safe by construction, not by convention.**
`QueryPlan{}` (the Go zero value, no fields set) has `Strategy == DispatchBlockSharded`
automatically, matching "the always-safe default, unchanged today's path" without needing any
explicit assignment — a caller that forgets to check an error/ok return and ends up with a
zero-value `QueryPlan` gets the safe fallback, not an arbitrary enum value. Pinned by
`TestBuildQueryPlan_DefaultZeroValueIsBlockSharded`, which checks both the literal zero value
and a `BuildQueryPlan(nil program, ...)` call that fails to plan.

**Consequence for the resolvable-but-VCNT-blind case: dispatch order degrades to pure
chronological, not a mixed sort.** When `allLeavesResolvable` is true but no leaf in the plan
is VCNT-estimable (`Lead()` returns `ok=false`, or `perMinuteForLead` returns empty for the
found lead leaf), `Strategy` is still `DispatchTimeSliced` (per this note's single-gate
contract above) but every resulting `TimeSlice.EstKnown` is `false` (NOTE-QP-005's degenerate
case). A caller's dispatch-priority ordering in that case degrades to PURE most-recent-first —
`BuildTimeSlices`' own chronological return order, unmodified — with no separate est-matches-
based sort layered on top, since there is no signal anywhere in the result to sort by. This is
the exact invariant tempo-side task T5 (Section 2, dispatch-priority-ordering construction)
needs in order to build its ordering logic correctly for the VCNT-blind case — flagging the
cross-reference explicitly since T5 is a downstream, cross-repo consumer of this contract, not
yet implemented as of this writing.

**Addendum (2026-07-07, holistic-review Fix 2; extended same day for Fix 6).** `BuildQueryPlan`
now checks `len(slices) == 0` after calling `BuildTimeSlices` (NOTE-QP-005) and falls back to
`QueryPlan{Strategy: DispatchBlockSharded}` when the call returns zero slices — a third
necessary condition alongside `Plan() ok=true` and `allLeavesResolvable=true`, added because an
otherwise-qualified plan whose window degenerates to zero slices (the concrete failure cases: an
inverted `minTS > maxTS`, or, since Fix 6, a window requiring more than `maxSlicesPerPlan`
slices even at the widest allowed width) previously would have reported `DispatchTimeSliced`
with an empty `Slices`, which a caller could misread as "dispatch zero jobs" rather than the
safe no-signal fallback. This is additive to the single-gate framing above, not a reversal of
it: lead-leaf VCNT estimability still never influences `Strategy` directly — it only ever
affects whether `BuildTimeSlices` produces adaptive or uniform-width slices, both of which are
non-empty for any non-degenerate, non-oversized window. Pinned by
`TestBuildQueryPlan_InvertedTimeRangeFallsBackToBlockSharded` (present in both this file's
`queryplan_test.go` and its root-level mirror in `timeslice_test.go`) for the Fix 2 case, and by
`TestBuildQueryPlan_HugeWindowFallsBackToBlockShardedNoPanic` (root `timeslice_test.go` only —
Fix 6's public-API-level regression guard) for the Fix 6 case. See `SPECS.md` SPEC-QP-3 for the
formal three-condition gate.

Back-refs: `internal/modules/queryplan/queryplan.go:DispatchStrategy,QueryPlan,BuildQueryPlan`.
Tests: `queryplan_test.go` (all 4). See `NOTES.md` NOTE-QP-004 (`Lead`), NOTE-QP-005
(`BuildTimeSlices`, the `EstKnown` semantics this note's estimability distinction depends on).
Issue #487.

---

## NOTE-QP-007: VCNTPerMinuteFunc — a new file beside vcnt_cost.go, delegating to its unexported leafEqualityValue (issue #487, holistic-review Fix 1)

*Added: 2026-07-07*

**What this is.** The holistic review found a composition gap: #487's root public API
(`timeslice.go`) could hand a caller a `CostFunc` (via `queryplan.VCNTCostFunc`, NOTE-QP-001)
but had no equivalent VCNT-backed way to build the `perMinuteForLead` callback `BuildQueryPlan`
(NOTE-QP-006) needs — a caller wanting the full VCNT-driven pipeline through public API alone
had no path to it. `VCNTPerMinuteFunc` closes that gap: `VCNTCostFunc`'s sibling, closing over
the same `(data, dir, minTS, maxTS)` inputs and returning a `func(*vm.RangeNode)
[]valuecounts.MinuteCount` suitable for `perMinuteForLead`.

**Why a new file, not an edit to vcnt_cost.go.** Per the task's own scope ruling (all-new-files
for this fix pass), `perminutefrom.go` is added alongside `vcnt_cost.go` rather than editing it
— but it deliberately CALLS `vcnt_cost.go`'s unexported `leafEqualityValue` directly (same
package, `queryplan`), rather than duplicating that extraction logic. This is the same reasoning
NOTE-QP-004 already established for `Lead()`/`leadLeaf` (task #147's fix): once two pieces of
logic live in the same package, calling the existing unexported helper directly is strictly
better than a parallel re-implementation that could silently drift — `VCNTCostFunc` and
`VCNTPerMinuteFunc` can never disagree on which leaves are point lookups or how their values are
canonically encoded, because they share the one extraction path.

**nil and empty are equivalent "no signal" — never an error.** `VCNTPerMinuteFunc`'s returned
callback returns `nil` for any leaf `VCNTCostFunc` itself would score `UnknownCost()` for
(range/regex/multi-value/present-only/non-canonically-encodable), or when
`valuecounts.SelectivityPerMinute` itself errors. A leaf that IS a valid equality lookup but has
no live VCNT coverage instead returns an empty (possibly non-nil) slice via
`SelectivityPerMinute`'s own contract (NOTE-VC-016) — not an error. `BuildTimeSlices`
(NOTE-QP-005) treats both identically (`len(perMinute) == 0`), so this distinction is
intentionally erased at this layer rather than requiring `BuildQueryPlan` to special-case two
different "no signal" shapes.

**Root-level composition: TimeSliceOracle.** Root `timeslice.go` exports
`TimeSliceOracle(data []byte, dir []VCNTChunkDirEntry, minTS, maxTS uint64) (CostFunc,
func(*RangeNode) []MinuteCount)`, a thin wrapper returning `(queryplan.VCNTCostFunc(...),
queryplan.VCNTPerMinuteFunc(...))` — mirroring `ClassifyProgramVCNT`'s own "compose oracles over
one decoded section" pattern (NOTE-QP-003) for the #487 time-slice case. This is the intended
way for a caller outside this module (tempo's frontend) to get both `BuildQueryPlan` inputs from
one decoded VCNT section without hand-rolling leaf-value canonicalization itself. Per the
established root convention (no VCNT-prefixed naming on new root symbols per the binding naming
ruling; root re-export files carry no SPECS/NOTES.md of their own, per the `vcnt.go`
precedent — see `.bob/state/spec-updates-log.md`'s C5 disposition entry), `TimeSliceOracle`
itself needs no separate spec entry.

**End-to-end proof.** `timeslice_publicapi_test.go` (package `blockpack_test`, zero internal
imports) builds a real VCNT section via `SortVCNTRecords`/`EncodeVCNTFile`/
`VCNTBuildSectionFromObjects`, composes via `TimeSliceOracle`, compiles a query via
`CompileTraceQL`, and asserts `BuildQueryPlan` produces genuinely adaptive (non-uniform-width)
slices — proving the whole #487 pipeline (VCNT section -> oracle construction -> plan -> time
slices) works through public API only, not just through internal-package unit tests.

Back-refs: `internal/modules/queryplan/perminutefrom.go:VCNTPerMinuteFunc`,
`internal/modules/queryplan/vcnt_cost.go:leafEqualityValue,VCNTCostFunc` (NOTE-QP-001),
`timeslice.go:TimeSliceOracle` (root package, no separate spec entry). Tests:
`perminutefrom_test.go`, `timeslice_publicapi_test.go`. See `SPECS.md` SPEC-QP-4. Issue #487.

---

## NOTE-QP-008: DefaultK=1 — ruling the previously-unpinned `k` multiplier in the uniform-width fallback formula (issue #487, team-lead ruling)

*Added: 2026-07-07*

> **PARTIALLY SUPERSEDED by `NOTE-QP-012` (issue #217, 2026-07-13).** `desiredSliceCount` and the
> uniform-width fallback FORMULA this note rules `k`'s default for were deleted by #217 —
> `concurrentRequests`/`k` are now unused for width/count purposes (kept in the signature only for
> call-site stability). `DefaultK = 1` itself is UNCHANGED and still lives in `slices.go`/
> `timeslice.go` exactly as described below (still referenced by tempo's call sites), so this
> note's ruling and its "LANDED" implementation-status section remain accurate for the constant's
> existence and value — only the formula that USED to consume it (`desiredSliceCount =
> concurrentRequests * k`) is gone. See `NOTE-QP-012` for the current algorithm.

**The gap.** `BuildTimeSlices`' uniform-width no-signal fallback (NOTE-QP-005) and
`desiredSliceCount = concurrentRequests * k` both depend on a caller-supplied `k`. Issue #487's
design doc states the formula's *shape* — "default when no signal: window / (ConcurrentRequests
× k)" — but never pins a numeric value for `k`; its own "Open questions" section explicitly
lists "Slice-width tuning: equal-estimated-work sizing needs empirical calibration once #484's
oracle exists" as unresolved. Neither `brainstorm-c.md` nor `plan-c.md` pinned one either — both
carry the formula's shape forward from the issue without adding a number. Coder-1-c's tempo call
site used `k=1` as a practical choice, but nothing in blockpack documented that as an intended
default, and nothing prevented a second call site (or a future tempo change) from silently
picking a different, undocumented value.

**Ruling: `DefaultK = 1`.** Confirmed by direct read of the issue body (`gh issue view 487`) that
no numeric default is specified anywhere upstream — this is a genuine gap, not a missed
cross-reference, so the team lead ruled a default rather than searching further. Rationale:
`k=1` makes `desiredSliceCount = concurrentRequests` — the uniform-width fallback partitions the
window into exactly as many slices as one round of dispatch at `concurrentRequests` can cover,
the least-aggressive default available. This matters specifically for the *no-signal* fallback:
when there's no VCNT data to justify finer adaptive slicing, over-slicing would add per-slice
dispatch/fetch overhead (index-file discovery, footer/TOC reads) for no matching benefit. A
caller that wants finer cancellation granularity (more, narrower slices, so `ShouldQuit` can cut
off a low-selectivity scan sooner) can pass `k > 1` deliberately — this remains a tuning knob,
not a value this package can compute or default away, since it depends on cluster-specific
concurrency/latency tradeoffs the design doc itself defers to "post-deployment empirical
calibration."

**Why a named constant, not just documentation.** Per the binding rule established at Phase C's
brainstorm stage ("Slice-width constants config-visible for post-deploy tuning" —
`brainstorm-prompt-c.md`), `minSliceWidthSeconds`/`maxSliceWidthSeconds`/`maxSlicesPerPlan` are
all named, documented constants rather than magic numbers scattered across call sites. `k` is
architecturally different — it's caller-supplied, not `BuildTimeSlices`-internal — but the same
principle applies: a config-visible `DefaultK` constant lets tempo's frontend (and any other
future caller) reference one canonical "we have no better idea" value instead of each call site
independently guessing or hardcoding a literal `1` that could silently drift from blockpack's
own intended default.

**Implementation status: LANDED.** `const DefaultK = 1` lives in
`internal/modules/queryplan/slices.go` (alongside `minSliceWidthSeconds`/`maxSliceWidthSeconds`/
`maxSlicesPerPlan`, exported separately since it is a caller-facing default rather than an
internal algorithm bound), re-exported at root as `blockpack.DefaultK` in `timeslice.go`
(mirroring `DispatchBlockSharded`/`DispatchTimeSliced`'s existing re-export pattern). Tempo's
call sites (`modules/frontend/vcnt_fetch.go`'s `buildQueryPlan`, plus the three `BuildQueryPlan`
calls in `tempodb/encoding/vblockpack/coverage_decline_test.go`) reference `blockpack.DefaultK`
instead of a bare literal `1`, closing the drift risk this note originally flagged. Pinned by
`TestDefaultK_IsOne` (`queryplan` package — also asserts `desiredSliceCount(4, DefaultK) == 4`)
and `TestDefaultK_RootReexportMatchesQueryplanPackage` (root, cross-checks against
`queryplan.DefaultK` directly).

Back-refs: `internal/modules/queryplan/slices.go:DefaultK,desiredSliceCount`,
`timeslice.go:DefaultK` (root re-export). See `SPECS.md` SPEC-QP-2's `DefaultK` ruling bullet.
Tests: `slices_test.go:TestDefaultK_IsOne`, `timeslice_test.go:TestDefaultK_RootReexportMatchesQueryplanPackage`.
Issue #487.

---

## NOTE-QP-009: AllLeavesIndexable — closing the ANY-vs-ALL gap in `allLeavesResolvable`'s computation (issue #487, task T5b)

*Added: 2026-07-07*

**The gap.** `BuildQueryPlan`'s `allLeavesResolvable` parameter (SPEC-QP-3, NOTE-QP-006) is
documented as "the caller-supplied #481 index-coverage verdict," but nothing in this package
ever specified HOW a caller should correctly compute it. The obvious candidate —
`vibuilder.BuildValueIndexSource`'s own `ok` return, or `CheckIndexCoverage`'s (#481/T5) reuse of
it — answers a strictly weaker question: "did AT LEAST ONE leaf resolve against the index," not
"did EVERY leaf." `vibuilder.BuildSource`'s own doc comment (NOTE-VI-036) is explicit about this:
a column the builder could not express a predicate for is simply never `Add`ed, and the QUERY
still proceeds with partial index coverage for a normal (block-sharded) scan — that's the correct
behavior for `BuildSource`'s own purpose, but wrong to reuse verbatim as `allLeavesResolvable`.

**Concrete failure mode without this fix.** Real TraceQL negation (`{ span.foo != "bar" }`)
compiles, via NOTE-453's presence+range-OR rewrite, to a mixed shape: one leaf the index CAN
represent plus one leaf (multi-value OR, or a bare `RequirePresent`) it architecturally cannot.
Under an ANY-based `allLeavesResolvable`, this mixed-shape query would wrongly qualify for
`DispatchTimeSliced` — the unindexable leaf's constraint would simply never be applied by the
per-slice job, silently returning more results than the query asked for. This is exactly the
class of correctness bug SPEC-QP-3's single-gate contract exists to prevent, but the contract
itself never said how to correctly *compute* the boolean it gates on until this fix.

**Fix: `AllLeavesIndexable(prog *vm.Program) bool` (SPEC-QP-5).** Flattens `prog`'s predicate
tree to its leaf `RangeNode`s (mirroring `vibuilder.collectLeaves`' own flat walk — deliberately
NOT `Plan()`'s AND/OR-structured walk, since this must inspect the exact same leaf set
`BuildSource` itself touches, independent of boolean structure) and requires
`vibuilder.LeafIndexable` (`vibuilder/SPECS.md` SPEC-VB-3, `vibuilder/NOTES.md` NOTE-VI-085) to
hold for EVERY leaf, not just one. A caller now computes `allLeavesResolvable :=
<availability check> && AllLeavesIndexable(prog)` — combining the existing data-presence check
with this shape-only check, neither of which alone is sufficient.

**Match-all is a special case, not an oversight.** A match-all-with-column-list query (`{} |
rate()` — `Nodes` empty, `Columns` populated) returns `true`: `BuildSource`'s own
`lookupColumnAll` path never rejects a column's shape (NOTE-VI-036), so there is no per-leaf
predicate to reject here either. A program referencing nothing at all (`Nodes` and `Columns`
both empty) returns `false`.

**Cross-package design, mirroring NOTE-VI-085's own framing.** `vibuilder.LeafIndexable` is the
per-leaf primitive (owned by `vibuilder`, since it's a thin wrapper over `buildPredicate`, the
same package that owns the real index-source-construction decision); `queryplan.AllLeavesIndexable`
is the ALL-leaves aggregation (owned by `queryplan`, since it needs to walk `vm.Program` the same
way `Plan`/`Lead` already do and feed `BuildQueryPlan`'s own gate). Neither package duplicates
the other's logic — `queryplan` calls `vibuilder.LeafIndexable` per leaf rather than re-deriving
`buildPredicate`'s shape rules, the same delegate-don't-duplicate principle NOTE-QP-004 already
established for `Lead()`/`leadLeaf`, now applied across a package boundary.

**Root-level re-export, no separate spec entry needed.** Root `timeslice.go` exports a thin
`AllLeavesIndexable(prog *Program) bool` wrapper (`return queryplan.AllLeavesIndexable(prog)`) —
per the established root-re-export convention (no separate SPECS/NOTES.md for root files, per
the `vcnt.go` precedent; see this file's own C5 and Fix-1/`TimeSliceOracle` dispositions),
`AllLeavesIndexable`'s root re-export needs no entry of its own here.

**Verification.** `indexable_test.go`'s
`TestAllLeavesIndexable_MixedIndexableAndNegatedLeafIsNotIndexable` is the direct regression
proof for the concrete failure mode described above. `timeslice_test.go`'s
`TestAllLeavesIndexable_RootReexportMatchesQueryplanPackage` pins the root wrapper against
`queryplan.AllLeavesIndexable` directly for both a fully-indexable and a mixed-shape program.

Back-refs: `internal/modules/queryplan/indexable.go:AllLeavesIndexable,collectLeafNodes`,
`internal/modules/vibuilder/builder.go:LeafIndexable` (NOTE-VI-085). See `SPECS.md` SPEC-QP-5,
SPEC-QP-3 (the gate this function correctly feeds). Tests: `indexable_test.go`,
`timeslice_test.go`. Issue #487.


---

## NOTE-QP-010: SelectSearchStrategy — reusing DispatchBlockSharded (not a fake enum value) to signal a non-dispatchable plan-time decline (issue #481 parts 2-3, F-5, team-lead ruling R13)

*Added: 2026-07-08*

**The design question:** `SelectSearchStrategy`'s `LowSelectivity`+no-limit row has no safe
`DispatchStrategy` to return — every per-block job dispatched under this row would decline
identically (no limit to bound the work, and pruning doesn't help), so dispatching AT ALL wastes N
block round-trips reaching a certain failure (team-lead ruling R6). Two designs were available for
signaling this:

1. **Invent a new `DispatchStrategy` enum value** (e.g. `DispatchDecline`) meaning "there is no
   valid strategy" (REJECTED). This conflates two different kinds of thing under one type: a
   `DispatchStrategy` is supposed to mean "how to execute this query," and "there is no way to
   execute this query" is not a way to execute it — it is the ABSENCE of one. A caller doing an
   exhaustive switch over `DispatchStrategy` (a natural, idiomatic Go pattern for a small enum)
   would need a permanent, unremovable case for a value that means "ignore the other return value
   in this struct, check the OTHER return value instead" — an awkward, easy-to-forget contract to
   express through a single return value alone.
2. **Return the real `DispatchBlockSharded` value alongside a second, explicit `bool`, and
   document the value as MEANINGLESS in that branch** (ADOPTED). `planTimeDecline bool` is the
   sole, out-of-band decline signal — a caller MUST check it before ever consulting `strategy`, but
   when it checks in the right order, there is no possibility of dispatching a decline as if it
   were a real, safe-to-run strategy. `DispatchBlockSharded` is chosen as the paired,
   never-meant-to-be-used value specifically because it is `DispatchStrategy`'s own Go zero value —
   a caller that (incorrectly) ignores `planTimeDecline` and dispatches anyway gets the SAFEST
   possible wrong behavior (a full, always-correct block-sharded scan) rather than an
   uninitialized or nonsensical strategy value. This is a deliberate defense-in-depth choice, not
   a claim that ignoring `planTimeDecline` is an intended or supported usage.

**Why this shape, not a `(strategy DispatchStrategy, err error)` return instead:** an `error`
return would suggest something went WRONG (a bug, a malformed input) — `planTimeDecline` is not an
error condition, it is a correct, expected outcome of a correctly-functioning classifier looking at
a genuinely unfavorable query shape. Keeping it a plain `bool` alongside the strategy value (rather
than an `error`) matches this project's existing "decline vs. error" distinction used throughout
the value-index/structural-query engines (e.g. `NOTE-QP-006`'s own three-state cost handling,
`ErrStructuralIndexCoverageGap`'s own careful separation of routine decline from genuine error).

Back-refs: `internal/modules/queryplan/queryplan.go:SelectSearchStrategy`. See `SPECS.md`
SPEC-QP-6 (the full decision-table contract), `SPEC-QP-3`'s addendum (the third `DispatchStrategy`
value this note's decline-signaling design coexists with). Tests: `queryplan_test.go`
(`TestSelectSearchStrategy_FiveRowCore`,
`TestSelectSearchStrategy_NeverReturnsTimeSlicedOrFakeDeclineStrategy`). Issue #481.

## NOTE-QP-011: `LeadDetail`'s R4 pre-authorization + parity-first refactor rationale (issue #493, Task 4b)

*Added: 2026-07-09*

**Why new public API was warranted.** The "both sides' costs" data (lead leaf's Known cost,
its column's total) already existed as local variables inside `ClassifyWithThreshold`'s own
classification logic (`lead.Cost.Count`, `colTotal`) — it was computed, compared, and discarded
before returning, never surfaced to any caller. Tempo's frontend
(`buildQueryPlanFromProgram`) needs exactly this detail to report WHY a query's plan qualified
or declined, not just the 3-state `Selectivity` verdict — team-lead ruling R4 pre-authorized
this as new blockpack public API for observability data, matching the project's "minimal
necessary API growth" pattern.

**Why `classifyDetailed` as a wrapped-by-`ClassifyWithThreshold` split, not a duplicate
implementation.** Reimplementing the classification logic a second time (even if kept in sync
by discipline) risks silent drift between the two — a bugfix or edge-case correction landed in
one copy but not the other. Instead, `classifyDetailed` IS the implementation;
`ClassifyWithThreshold` becomes `sel, _ := classifyDetailed(...); return sel`, a one-line
wrapper that can never diverge from what `classifyDetailed` computes. This is the SAME
parity-first pattern `SPEC-QP-1`'s `Group.Lead()`/`leadLeaf` pair already establishes in this
package — precedent, not a new convention.

**Regression guard.** All of `ClassifyWithThreshold`'s existing tests pass completely
unmodified after the refactor (confirmed via `go test ./internal/modules/queryplan/... -run
'Classify|Selectivity'`), proving the parity requirement holds in practice, not just by
inspection.

Back-ref: same files as `SPEC-QP-7`. Issue #493.

## NOTE-QP-012: forced literal one-minute slice width, replacing #487's adaptive algorithm (issue #217)

*Added: 2026-07-13*

**What changed.** `BuildTimeSlices` no longer sizes slices adaptively (denser minutes narrower,
up to a 1h ceiling) or falls back to a uniform width derived from `concurrentRequests*k`. Every
slice returned is now EXACTLY `minSliceWidthSeconds` (60s) wide, unconditionally, for every call.
The `adaptiveWidthSlices`/`uniformWidthSlices`/`clampWidth`/`desiredSliceCount` helper functions
were deleted, not merely bypassed — there is no remaining code path that produces a
non-60s-wide slice. `concurrentRequests`/`k` remain in `BuildTimeSlices`/`BuildQueryPlan`'s
signatures for call-site stability (tempo's frontend still passes them through) but no longer
influence width or count in any way.

**Why (explicit user override, not re-derived from this package alone).** This task's own
planning pass (`.bob/state/217-per-minute-dispatch-plan.md` §2.3) originally recommended
KEEPING the adaptive/uniform defaults unchanged, reasoning that the correctness property #217
needed ("a coverage boundary loses at most one slice's width, never the whole query") was
already satisfied at 1h granularity, and that forcing 1-minute slicing for every window was an
unmeasured fanout-cost increase not worth taking without a benchmark first. The user explicitly
REJECTED that recommendation: the actual goal is bounded, PREDICTABLE query latency independent
of the requested window's width, via MAXIMUM PARALLELISM (every minute dispatched as its own
concurrent job) — not minimizing data loss at a coverage boundary, which was this package's own
(now superseded) framing. The user stated they are willing to accept the resulting higher
job-fanout/infrastructure cost to get that latency guarantee. BENCH-QP-010 (`BENCHMARKS.md`) was
still added, decoupled from gating this decision, to get a real number for per-job overhead.

**maxSlicesPerPlan raised 2000 → 50000.** Before this change, the widest a single slice could
ever be was 3600s, so a 2000-slice cap always implied ≥83.3 days of headroom regardless of
individual slice width. Forcing every slice to 60s means the SAME 2000 cap would instead mean at
most ~33.3 hours before a query stops qualifying for `DispatchTimeSliced` and silently falls back
to `DispatchBlockSharded` (unbounded latency — the opposite of #217's goal). Raised to 50,000
(≈34.7 days) so common week-to-month-wide dashboard/investigation queries still qualify. See
`slices.go`'s `maxSlicesPerPlan` doc comment and `BENCHMARKS.md` BENCH-QP-010 for the numbers
supporting this ceiling (frontend-side combine overhead at 50,000 slices is ≈47ms, negligible
next to real per-job querier round-trip cost).

**What did NOT change.** The per-minute `EstMatches`/`EstKnown`/`VCNTEmpty` signal plumbing is
untouched — a minute with real VCNT signal still gets `EstKnown=true` with its own count; a
minute absent from `perMinute` still gets `EstKnown=true, VCNTEmpty=true` (a checked gap, not a
hole); a plan with no VCNT signal at all still gets `EstKnown=false` uniformly. Only the WIDTH
these facts get attached to changed — that width is now always exactly one minute rather than a
function of the signal.

Back-ref: `internal/modules/queryplan/slices.go:BuildTimeSlices,maxSlicesPerPlan`. See
`BENCHMARKS.md` BENCH-QP-010. Issue #217.

## NOTE-QP-013: composed VCNTDurationCostFunc into the existing ClassifyProgramVCNT* family rather than new sibling entry points (issue #205)

*Added: 2026-07-13*

**Decision.** `VCNTDurationCostFunc` (a new `CostFunc` recognizing duration-range leaves via
`internal/modules/valuecounts`'s new fixed-bucket histogram, Phase A) is wired into
`ClassifyProgramVCNTWithThreshold`/`ClassifyProgramVCNTWithDetail`'s existing `cost` construction
via a new generic combinator, `CombineCostFuncs(VCNTCostFunc(...), VCNTDurationCostFunc(...))` —
not exposed as a new parallel `ClassifyProgramVCNTDuration*` entry point, and not merged into
`VCNTCostFunc` itself.

**Rationale.** Tracing every real call site of `ClassifyProgramVCNT*` in tempo (agentic-tempo
branch) found exactly one: `vcnt_fetch.go:288`'s `ClassifyProgramVCNTWithDetail` call, which feeds
the plan-time decline gate (`SelectSearchStrategy`, `SPEC-QP-6`) that rejects a query before any
block-scan I/O happens. That call site's signature is fixed by tempo's own code, which blockpack
cannot and does not need to change — composing the new cost func INTO the existing functions means
tempo's already-shipped, unmodified call gets duration-range recognition automatically on the next
ordinary blockpack revendor, with zero tempo-side code change. A new sibling entry point
(`ClassifyProgramVCNTDurationWithDetail`, say) would have required tempo to add a second call
site and reconcile two verdicts per query for no benefit — nothing in tempo's real code ever
wants "duration signal only" or "equality signal only" in isolation, only the combined verdict.

> **Correction (2026-07-13, Phase D):** "zero tempo-side code change" above is accurate only for
> the *classify* call site (`ClassifyProgramVCNTWithDetail` itself, as described). It is NOT
> accurate for the feature as a whole: the real end-to-end pipeline test (Phase D) found that
> `vcnt_fetch.go`'s `dims` construction in `buildQueryPlanFromProgram` — a separate function, the
> *fetch* step that runs before classify — only ever requested `prog.WantColumns`'s bare column
> names (e.g. `"span:duration"`), never the histogram's own colHash-distinct column name
> (`VCNTDurationHistogramColumnName("span:duration")` == `"span:duration#hist"`). Since VCNT
> objects are stored one-directory-per-colHash, the histogram object was never fetched at all —
> `ClassifyProgramVCNTWithDetail` always received empty histogram data, making this feature a
> complete no-op through the real dispatch path despite passing every phase's unit tests. Fixed
> by widening `dims` to also include each `WantColumns` entry's histogram column name (additive,
> harmless for every non-histogram column — mirrors `fetchVCNTSection`'s own existing
> no-coverage-tolerant convention). See `.bob/state/205-vcnt-histogram-plan.md` §2 for the same
> correction and `vcnt_fetch_test.go`'s `TestBuildQueryPlanFromProgram_DimsWideningReachesDurationHistogramClassification`
> for the regression guard.

**Why a new file (`vcnt_duration_cost.go`) rather than editing `VCNTCostFunc` in place.** Mirrors
this package's own established "additive file, don't touch code whose behavior must stay
byte-identical" pattern (`perminutefrom.go`'s precedent, `NOTE-QP-007`). `VCNTCostFunc` itself is
untouched by this change — only its two callers' `cost :=` line changed, from `VCNTCostFunc(...)`
to `CombineCostFuncs(VCNTCostFunc(...), VCNTDurationCostFunc(...))`.

**Why `CombineCostFuncs` is a separate, generic combinator rather than baking the two together
inline.** `VCNTCostFunc` and `VCNTDurationCostFunc` are mutually exclusive by leaf shape
by construction (equality requires `len(Values)==1`; both of `VCNTDurationCostFunc`'s recognized
shapes require `len(Values)==0`) — there is no leaf either single-value equality OR a
Min/Max-bearing range predicate that both functions could ever claim simultaneously. This makes
`CombineCostFuncs`'s try-order provably inert for every REAL leaf shape today (confirmed by
`TEST-QP-3`'s `TestVCNTCostFunc_ExistingEqualityBehavior_ByteIdenticalAfterHistogramWiring`,
which passes regardless of which of the two functions is tried first) — but the combinator is
still kept generic and separately tested (`TestCombineCostFuncs_TriesFirstThenSecond` et al.,
using deliberately-overlapping stub funcs) rather than hardcoded into the two call sites, since a
future third `CostFunc` (a per-block min/max/bloom estimator, per `CostFunc`'s own doc comment) is
exactly the kind of extension this package's pluggable-oracle design already anticipates.

**Mutation-test verification performed (per this project's standing convention, memory
`feedback_mutation_test_review.md`).** Two separate mutations were applied and reverted:
(1) `CombineCostFuncs`'s try-order was flipped (`second` tried before `first`) — this does NOT
break `TestVCNTCostFunc_ExistingEqualityBehavior_ByteIdenticalAfterHistogramWiring` (confirming
the mutual-exclusivity property above holds in practice, not just by shape-gate inspection), but
DOES break `TestCombineCostFuncs_TriesFirstThenSecond`/`_NeverDoubleCounts` (the pure combinator
tests using deliberately-overlapping stubs), proving `CombineCostFuncs`'s try-order semantics are
real and tested. (2) `VCNTDurationCostFunc` was temporarily mutated to return
`KnownCost(999)` unconditionally, bypassing all of its shape/coverage gates — this DOES break
`TestVCNTCostFunc_ExistingEqualityBehavior_ByteIdenticalAfterHistogramWiring` for every leaf shape
`VCNTCostFunc` alone reads as Unknown (range-min, regex, present-only, multi-value, columnless,
unseen, missing-column), proving that test is a real, non-vacuous regression guard against a
second cost func contributing an incorrect answer once composed in — not just against a
try-order bug specifically.

Back-ref: same files as `SPEC-QP-9`. Issue #205.
