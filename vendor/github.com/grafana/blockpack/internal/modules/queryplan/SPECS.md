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

Next free ID: **SPEC-QP-5**.

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
parts: minute floor, MaxUint64 saturation, slice-count cap).*

**Contract:** `BuildTimeSlices(perMinute []valuecounts.MinuteCount, minTS, maxTS uint64,
concurrentRequests, k int) []TimeSlice` partitions `[minTS, maxTS]` into a fully-covering,
minute-aligned, non-overlapping sequence of `TimeSlice{Start, End uint64; EstMatches int64;
EstKnown bool; VCNTEmpty bool}` values, `[Start, End)` half-open.

**Rules:**
- `minTS > maxTS` returns `nil`.
- `Start`/`End` on every returned slice are 60-second-aligned (`(sec/60)*60`).
- **Full coverage, no gaps (binding):** the returned slices always exactly and contiguously
  cover the minute-floored `[minTS, maxTS]` window with zero holes.
- Every slice's width is in `[60, 3600]` seconds inclusive; the post-floor window is always
  `>= 60s` when `minTS <= maxTS`, so no narrower slice is ever produced.
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

Back-ref: `internal/modules/queryplan/slices.go:TimeSlice,BuildTimeSlices,maxSafeMaxTS,maxSlicesPerPlan`.
See `NOTES.md` NOTE-QP-005. Tests: `slices_test.go` (including
`TestBuildTimeSlices_NearMaxUint64DoesNotOverflow`,
`TestBuildTimeSlices_NegativeCountFromNonConformingOracleClampedToZero`,
`TestBuildTimeSlices_HugeWindowIsCappedNotPanicking`,
`TestBuildTimeSlices_RealisticLongWindowStillSlicesNormally`). Issue #487.

---

## SPEC-QP-3: `QueryPlan` / `DispatchStrategy` / `BuildQueryPlan` — top-level dispatch qualification contract
*Added: 2026-07-07. Amended 2026-07-07 (holistic-review Fix 2): added the third
non-empty-`Slices` qualification condition.*

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
- When `Strategy == DispatchBlockSharded`: `Root` is the zero `Group{}` and `Slices` is `nil`
  — no plan/slice work is attempted or returned.
- When `Strategy == DispatchTimeSliced`: `Root` is `Plan`'s own output (unchanged), and `Slices`
  is `BuildTimeSlices`'s output (SPEC-QP-2) called with `perMinute` resolved as follows: if
  `g.Lead()` (SPEC-QP-1) returns `ok=true` and `perMinuteForLead != nil`, `perMinute =
  perMinuteForLead(lead.Node)`; otherwise `perMinute` is `nil`, which drives `BuildTimeSlices`'s
  uniform-width fallback per SPEC-QP-2.
- `perMinuteForLead` is called at most once per `BuildQueryPlan` call, and only for the
  identified lead leaf — never for any other leaf, never before `Lead()` has run, and never at
  all when `allLeavesResolvable` is false or `Lead()` returns `ok=false`.
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
