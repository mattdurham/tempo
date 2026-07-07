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

Back-refs: `internal/modules/queryplan/slices.go:TimeSlice,BuildTimeSlices,adaptiveWidthSlices,uniformWidthSlices,clampWidth,desiredSliceCount,maxSafeMaxTS,maxSlicesPerPlan`.
Tests: `slices_test.go` (`TestBuildTimeSlices_MinuteAligned`,
`_AdaptiveWidthFromMinuteCounts`, `_AdaptiveWidthRespectsMaxWidthClamp`,
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
