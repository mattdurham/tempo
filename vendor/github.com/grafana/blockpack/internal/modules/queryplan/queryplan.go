package queryplan

// NOTE: QueryPlan/BuildQueryPlan are new, additive code for issue #487 — no existing file
// (plan.go, cost.go, selectivity.go, vcnt_cost.go) is touched. See SPEC-QP-3 and NOTE-QP-006
// for the full contract.

import (
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/vm"
)

// DispatchStrategy tells the caller (tempo's frontend) which job-construction path to use for
// a query. DispatchBlockSharded (the zero value) is today's existing per-block job path,
// unchanged — the always-safe default. DispatchTimeSliced is the #487 opt-in narrowing, selected
// whenever every leaf in the plan is resolvable by the index (the ONLY Strategy gate — lead-leaf
// estimability governs slice width mode, not Strategy; see BuildQueryPlan). DispatchBoundedRecentFirst
// is the #481 part 2/3 bounded-newest-first strategy: selected by tempo's frontend
// (buildQueryPlanFromProgram) when a search query's index coverage is low/unknown but a limit is
// present, per R3's ruling — NEVER selected for metrics (a truncated aggregate is a wrong
// answer, not a partial one). Unlike DispatchTimeSliced, blockpack's QueryPlan carries ONLY this
// Strategy value for the bounded path — no budget fields (MaxBlocks/MaxBytes/MaxDuration): the
// querier (tempo's backend_block.go) owns and applies its own budget policy when it sees this
// signal, mirroring how IndexOnly/MostRecent are tempo-set booleans on blockpack's QueryOptions
// rather than blockpack-computed values (plan-f.md Task 5's Option B, ratified).
type DispatchStrategy int

const (
	// DispatchBlockSharded is the zero value and today's existing per-block job path.
	DispatchBlockSharded DispatchStrategy = iota
	// DispatchTimeSliced is the #487 opt-in narrowing; see DispatchStrategy's doc comment.
	DispatchTimeSliced
	// DispatchBoundedRecentFirst is the #481 bounded-newest-first strategy; see
	// DispatchStrategy's doc comment for the full contract, and SPEC-QP-6 (SelectSearchStrategy)
	// for the full selection decision table.
	DispatchBoundedRecentFirst
)

// SelectSearchStrategy implements R3's ruling table for choosing a search-query dispatch
// strategy from a VCNT selectivity classification and whether the query carries a limit
// (issue #481 parts 2/3, team-lead ruling R13). It is a PURE function — no I/O, no tempo
// context — and is the single source of truth for this decision table so tempo's frontend
// (buildQueryPlanFromProgram) consumes it directly instead of hand-rolling an
// independently-maintained copy that could drift.
//
// SPEC-QP-6: the full decision-table contract lives here. NOTE-QP-010 records the design
// rationale.
//
// Deliberately EXCLUDED from this function: boundedEligible (search-vs-metrics) and
// resolvability (index-coverage). Both are tempo-context concerns the caller must gate on
// FIRST — metrics callers never reach this function at all (R2: metrics is never
// bounded-served), and an unresolvable plan is DispatchBlockSharded before selectivity is
// even considered. This keeps the five-row core here untestable-drift-proof in one place.
//
// planTimeDecline=true means the query has NO safe answer at plan time (LowSelectivity with
// no limit present: every per-block job would decline identically, so dispatching at all
// wastes N block round-trips on a certain failure, per R6). When planTimeDecline is true, the
// returned strategy value is MEANINGLESS and must NOT be dispatched — the caller (tempo) must
// fail the query at plan time instead, mapping to the typed hard-error family. This is
// deliberately NOT represented as a fake DispatchStrategy enum value (e.g. a
// "DispatchDecline" constant) — decline is a distinct, non-dispatchable outcome, not a
// dispatch strategy, so it is returned out-of-band via this second bool.
//
// The five-row core (Selective's outcome does not depend on hasLimit, collapsing what would
// otherwise be six selectivity×limit combinations into five distinct outcomes):
//
//	Selective         + any limit -> DispatchBlockSharded  (index-only; hard-errors on decline downstream)
//	LowSelectivity     + limit    -> DispatchBoundedRecentFirst
//	LowSelectivity     + no limit -> planTimeDecline=true
//	UnknownSelectivity + limit    -> DispatchBoundedRecentFirst
//	UnknownSelectivity + no limit -> DispatchBlockSharded  (index-only; hard-errors on decline downstream)
func SelectSearchStrategy(sel Selectivity, hasLimit bool) (strategy DispatchStrategy, planTimeDecline bool) {
	switch sel {
	case Selective:
		return DispatchBlockSharded, false
	case LowSelectivity:
		if hasLimit {
			return DispatchBoundedRecentFirst, false
		}
		return DispatchBlockSharded, true
	default: // UnknownSelectivity
		if hasLimit {
			return DispatchBoundedRecentFirst, false
		}
		return DispatchBlockSharded, false
	}
}

// QueryPlan is the top-level output blockpack hands tempo's frontend: the existing leaf-cost
// Group tree (Root, unchanged from Plan()'s own output) plus, when qualified, a set of
// chronologically-ordered TimeSlices and the Strategy verdict telling the caller which to use.
type QueryPlan struct {
	Slices   []TimeSlice
	Root     Group
	Strategy DispatchStrategy
}

// BuildQueryPlan composes Plan() with time-slice construction. allLeavesResolvable is the
// caller-supplied #481 index-coverage verdict (blockpack's Plan/Group has no notion of "index
// coverage" itself — that's a tempo/vibuilder-side concept — so it's threaded in, not re-derived
// here); it is the ONLY gate on Strategy. perMinuteForLead is a pluggable per-leaf oracle
// (mirroring CostFunc's own shape) that, given the lead leaf's RangeNode, returns its per-minute
// VCNT breakdown (valuecounts.SelectivityPerMinute's output) or nil/empty if unavailable — the
// caller resolves this AFTER Lead() identifies which leaf needs it, so it never has to guess
// which leaf will be lead ahead of time. Its MinuteCount.Count values are expected to be
// non-negative, mirroring SelectivityPerMinute's own liveness-rule contract; BuildTimeSlices
// defensively clamps a non-conforming negative Count to zero rather than letting it corrupt
// the adaptive-width target calculation.
//
// Binding contract (issue #487): a resolvable-but-VCNT-blind plan (Lead() returns ok=false, or
// perMinuteForLead returns empty) STILL qualifies for DispatchTimeSliced — BuildTimeSlices' own
// empty-perMinute uniform-width fallback handles that case (every resulting TimeSlice has
// EstKnown=false). allLeavesResolvable=false, Plan() itself failing (ok=false, nothing to plan),
// or BuildTimeSlices producing zero Slices for an otherwise-qualified plan (e.g. an inverted
// minTS > maxTS) all fall back to DispatchBlockSharded — DispatchTimeSliced is never reported
// with an empty Slices, which a caller could otherwise misread as "dispatch zero jobs" instead
// of falling back (holistic-review Fix 2).
//
// In that VCNT-blind case, the resulting Slices carry no est-matches signal at all (EstKnown
// false on every slice) — a caller's dispatch-priority ordering degrades to PURE
// most-recent-first, i.e. exactly BuildTimeSlices' own chronological return order, with no
// separate est-matches-based sort to apply on top (team-lead final settlement, plan-c.md
// Correction log; see TimeSlice.EstKnown's doc comment for the full contract).
//
// SPEC-QP-3: the single-gate qualification contract (resolvability is the only Strategy gate;
// lead-leaf estimability governs slice width mode only), including the option-(ii)-rejected
// record.
// NOTE-QP-006: the lazy perMinuteForLead resolution sequencing and the zero-value-is-safe-
// default property.
func BuildQueryPlan(
	prog *vm.Program, cost CostFunc, allLeavesResolvable bool,
	perMinuteForLead func(leaf *vm.RangeNode) []valuecounts.MinuteCount,
	minTS, maxTS uint64, concurrentRequests, k int,
) QueryPlan {
	g, ok := Plan(prog, cost)
	if !ok || !allLeavesResolvable {
		return QueryPlan{Strategy: DispatchBlockSharded}
	}
	var perMinute []valuecounts.MinuteCount
	if lead, leadOK := g.Lead(); leadOK && perMinuteForLead != nil {
		perMinute = perMinuteForLead(lead.Node)
	}
	slices := BuildTimeSlices(perMinute, minTS, maxTS, concurrentRequests, k)
	if len(slices) == 0 {
		// An otherwise-qualified plan (Plan() ok, allLeavesResolvable) that still yields zero
		// Slices (e.g. an inverted minTS > maxTS) must not report DispatchTimeSliced — that
		// would silently dispatch zero jobs instead of falling back to the safe default.
		return QueryPlan{Strategy: DispatchBlockSharded}
	}
	return QueryPlan{Root: g, Slices: slices, Strategy: DispatchTimeSliced}
}
