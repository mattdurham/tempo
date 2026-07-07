// Package queryplan is the general cost-based predicate query-planning primitive
// (issue #485, NOTE-QP-001). It is the abstraction that #484's VCNT selectivity
// oracle plugs into, kept deliberately separate from any concrete cost source so
// other oracles (per-block min/max/bloom selectivity, a no-signal default) can plug
// in later without redesigning the combination logic.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// # Prior art
//
// This is the well-established, decades-old approach to predicate evaluation, not a
// novel idea. It is grounded in:
//
//   - System R (Selinger et al., 1979): assign selectivity estimates to predicates
//     and order/choose access paths to minimize total evaluation cost.
//   - Lucene's ConjunctionDISI: for AND-combined leaf predicates, pick the cheapest
//     ("sparsest") iterator as the conjunction lead and verify candidates against
//     the rest — verify-against-narrowed-candidates rather than resolving each
//     independently.
//
// # What this package provides
//
//  1. LeafCost — a generic, comparable cost estimate for a single leaf predicate,
//     with a distinct Unknown sentinel state that is NOT collapsed into "zero".
//     "Uncovered / no signal" and "the oracle affirmatively says zero matches" are
//     directionally opposite for ordering and must never be conflated (NOTE-QP-001).
//  2. CostFunc — a pluggable per-leaf cost estimator. #484's VCNT oracle is the
//     first (and initially only) concrete implementation wired in by the caller.
//  3. A correct AND/OR-tree walk over vm.RangeNode's real tree structure that
//     identifies maximal AND-only subtrees and produces per-leaf ordering plans.
//  4. Selectivity — a three-state classification (Classify, NOTE-QP-002, issue
//     #486) over a scored Plan that recognizes a LOW-SELECTIVITY predicate: one
//     whose lead (most-selective) leaf still matches most of its column's
//     population, so value-index pruning would skip almost nothing. It is the
//     recognition primitive #481 part 2 needs to choose a pointed, limit-bounded,
//     recent-block-first execution over an index-source build across the whole
//     window. Like the cost side it is pluggable (ColumnTotalFunc is the population
//     denominator; VCNTColumnTotalFunc is the concrete VCNT implementation) and
//     performs no I/O.
//  5. ClassifyProgramVCNT — the single consumer entry point (NOTE-QP-003, issue
//     #481 part 2) that composes VCNTCostFunc, Plan, VCNTColumnTotalFunc, and
//     Classify over ONE decoded VCNT section and window into a verdict. It exists so
//     a consumer cannot build the cost and column-total oracles over mismatched
//     windows — which would make the selectivity fraction's numerator and
//     denominator describe different slices of time and silently misclassify.
//  6. Group.Lead() (NOTE-QP-004/SPEC-QP-1, issue #487) — the plan's most-selective
//     leaf as its own exported accessor, for callers (time-slice construction) that
//     need the lead leaf itself rather than a selectivity verdict derived from it.
//  7. TimeSlice / BuildTimeSlices (NOTE-QP-005/SPEC-QP-2, issue #487) — partitions a
//     query window into minute-aligned, chronologically-ordered sub-windows, sized
//     adaptively from a lead leaf's per-minute VCNT signal (denser minutes get
//     narrower slices) with a uniform-width fallback when no signal exists. This is
//     the #487 time-slice job-sharding primitive: shard by the index's own time
//     partition key instead of by block.
//  8. QueryPlan / BuildQueryPlan / DispatchStrategy (NOTE-QP-006/SPEC-QP-3, issue
//     #487) — composes Plan(), Lead(), and BuildTimeSlices into the top-level output
//     blockpack hands a caller: a Strategy verdict (DispatchBlockSharded, the
//     always-safe zero value, or DispatchTimeSliced) gated SOLELY on whether every
//     leaf is resolvable by the index; lead-leaf VCNT-estimability governs only
//     whether the resulting TimeSlices are adaptive or uniform-width, never Strategy
//     itself.
//
// # AND vs OR semantics (the crux)
//
// vibuilder.collectLeaves flattens the whole predicate tree, discarding AND/OR
// structure. That is correct for its purpose (every leaf is resolved independently
// and the real compiled program tree re-combines them), but any cost-based ordering
// or short-circuiting MUST NOT make the same flattening mistake. This package walks
// the tree preserving structure:
//
//   - An AND group (a maximal subtree whose parent chain up to the nearest OR or the
//     root is entirely AND) may be cost-ordered ascending and short-circuited: a
//     cheap-but-empty leaf lets the group skip resolving its remaining siblings,
//     because an empty conjunct makes the whole AND empty.
//   - An OR group provides no free skip: every branch must still be evaluated for
//     correctness. The only available optimization is evaluation ORDER (cheapest
//     first), a hint for early-exit-eligible callers (e.g. limit-bounded search that
//     can stop once it has enough matches) — never skipping branches outright.
//
// Each OR branch's own internal AND-subtree is cost-ordered recursively.
//
// # Scope boundary
//
// This package produces an ordering/short-circuit PLAN only. It performs no I/O and
// resolves no leaves itself; the caller (vibuilder.BuildSource, in #484 Phase 2-3)
// walks the plan, resolves leaves in the planned order, and applies the empty-AND
// short-circuit. Keeping the plan pure makes both group behaviors independently
// testable without object storage.
package queryplan
