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
// estimability governs slice width mode, not Strategy; see BuildQueryPlan).
type DispatchStrategy int

const (
	// DispatchBlockSharded is the zero value and today's existing per-block job path.
	DispatchBlockSharded DispatchStrategy = iota
	// DispatchTimeSliced is the #487 opt-in narrowing; see DispatchStrategy's doc comment.
	DispatchTimeSliced
)

// SelectSearchStrategy implements the plan-time ruling table for choosing a search-query
// dispatch strategy from a VCNT selectivity classification and whether the query carries a
// limit. It is a PURE function — no I/O, no tempo context — and is the single source of truth
// for this decision table so tempo's frontend (buildQueryPlanFromProgram) consumes it directly
// instead of hand-rolling an independently-maintained copy that could drift.
//
// SPEC-QP-6: the full decision-table contract lives here.
//
// Deliberately EXCLUDED from this function: boundedEligible (search-vs-metrics) and
// resolvability (index-coverage). Both are tempo-context concerns the caller must gate on
// FIRST — an unresolvable plan is DispatchBlockSharded before selectivity is even considered.
//
// ISSUE #535 (team-lead ruling, an explicit REVERSAL of issue #481's R6 ruling, not a silent
// behavior change): a query may only decline when the index/cube genuinely isn't built yet for
// the queried window (a real coverage gap, or cube warming) — never as a cost/selectivity
// heuristic for a query the system CAN answer correctly. R6 previously set
// planTimeDecline=true for LowSelectivity with no limit present, on the theory that every
// per-block job would decline identically and dispatching at all would waste N block
// round-trips on a certain failure. That premise was checked against the actual execution code
// and found false: executor.ExecuteTraceMetricsFromVI (metrics) and the unbounded value-index
// read path (vibuilder.BuildSource, search with no limit) both compute the exact answer over
// EVERY matched entry unconditionally — neither has a selectivity-based bailout. There is no
// execution-side reason to decline a LowSelectivity/no-limit query, so this function no longer
// does. planTimeDecline is now ALWAYS false; it is kept as a return value (rather than dropping
// it and shrinking this to a single-value-returning function) purely for signature/API
// stability across this function's other callers (root timeslice.go's re-export,
// cmd/deadcode/main.go) — tempo's own buildQueryPlanFromProgram no longer branches on it at all
// (that dead call site was removed in the same change), so no production caller anywhere still
// inspects this value; a future cleanup may drop it once nothing still calls this function
// expecting the two-value shape.
//
//	Selective          -> DispatchBlockSharded  (index-only; hard-errors on decline downstream)
//	LowSelectivity     -> DispatchBlockSharded  (index-only; hard-errors on decline downstream;
//	                       regardless of limit presence — see issue #535 above)
//	UnknownSelectivity -> DispatchBlockSharded  (index-only; hard-errors on decline downstream)
func SelectSearchStrategy(sel Selectivity, hasLimit bool) (strategy DispatchStrategy, planTimeDecline bool) {
	return DispatchBlockSharded, false
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
// the per-minute EstMatches signal (#217/NOTE-QP-012: slice width itself is no longer
// target-derived — every slice is forced to exactly one minute).
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
// SPEC-QP-12/NOTE-QP-016 (issue #499, Phase 3): the every-AND-conjoined-leaf SkipDispatch
// resolution below, including the oracle-call dedup when the lead leaf is itself AND-conjoined
// to root. Worked example of SkipDispatch/EstMatches disagreeing on the same slice: AND(L1, L2)
// where L1 wins Lead() with a real, positive per-minute count for minute M, but L2 (not lead,
// still AND-conjoined) confidently reports zero for M — the resulting slice for M has
// EstMatches>0 (from L1, via perMinute below) AND SkipDispatch=true (from L2's veto, via
// andConjoinedSignals below): the AND is false the moment L2 is false, regardless of what L1's
// own optimistic count says.
func BuildQueryPlan(
	prog *vm.Program, cost CostFunc, allLeavesResolvable bool,
	perMinuteForLead func(leaf *vm.RangeNode) []valuecounts.MinuteCount,
	minTS, maxTS uint64, concurrentRequests, k int,
) QueryPlan {
	g, ok := Plan(prog, cost)
	if !ok || !allLeavesResolvable {
		return QueryPlan{Strategy: DispatchBlockSharded}
	}
	// UNCHANGED: lead-leaf per-minute signal drives EstMatches/EstKnown/VCNTEmpty only.
	var perMinute []valuecounts.MinuteCount
	lead, leadOK := g.Lead()
	if leadOK && perMinuteForLead != nil {
		perMinute = perMinuteForLead(lead.Node)
	}

	// NEW (issue #499, Phase 3): every AND-conjoined-to-root leaf independently gates
	// SkipDispatch — an OR-combined-across-leaves signal, fully independent of which leaf is
	// lead. De-duplication: if the lead leaf is ALSO AND-conjoined to root (the common case for a
	// pure-AND query), reuse perMinute above rather than calling perMinuteForLead(lead.Node) a
	// second time for the same leaf — avoids one redundant oracle scan per plan for the most
	// common query shape.
	var andConjoinedSignals [][]valuecounts.MinuteCount
	if perMinuteForLead != nil {
		for _, l := range collectANDConjoinedLeaves(g) {
			if leadOK && l.Node == lead.Node {
				if len(perMinute) > 0 {
					andConjoinedSignals = append(andConjoinedSignals, perMinute)
				}
				continue
			}
			if sig := perMinuteForLead(l.Node); len(sig) > 0 {
				andConjoinedSignals = append(andConjoinedSignals, sig)
			}
		}
	}

	slices := BuildTimeSlices(perMinute, andConjoinedSignals, minTS, maxTS, concurrentRequests, k)
	if len(slices) == 0 {
		// An otherwise-qualified plan (Plan() ok, allLeavesResolvable) that still yields zero
		// Slices (e.g. an inverted minTS > maxTS) must not report DispatchTimeSliced — that
		// would silently dispatch zero jobs instead of falling back to the safe default.
		return QueryPlan{Strategy: DispatchBlockSharded}
	}
	return QueryPlan{Root: g, Slices: slices, Strategy: DispatchTimeSliced}
}
