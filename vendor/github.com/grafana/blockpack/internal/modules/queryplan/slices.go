package queryplan

// NOTE: TimeSlice/BuildTimeSlices are new, additive code for issue #487 — no existing file
// (plan.go, cost.go, selectivity.go, vcnt_cost.go) is touched. See SPEC-QP-2 and NOTE-QP-005
// for the full contract.
//
// #217/NOTE-QP-012 (explicit user override of this task's own written recommendation): every
// slice is now forced to exactly minSliceWidthSeconds (60s/1 minute) wide, unconditionally, for
// every DispatchTimeSliced query — the #487 adaptive-width algorithm (denser minutes get
// narrower slices up to a 1h ceiling) is REMOVED, not merely disabled. Rationale (ruled by the
// user, not re-derivable from this file alone): the point of per-minute dispatch is BOUNDED,
// PREDICTABLE query latency independent of the requested window's width, via maximum
// parallelism — every minute is its own job, dispatched concurrently — not minimizing data loss
// at a coverage boundary (which was this task's own original, now-superseded framing). The
// tradeoff is a larger job fanout for wide windows; the user explicitly accepted that
// infrastructure cost to get the latency guarantee. See maxSlicesPerPlan's own doc comment for
// the resulting fanout-ceiling change this forces.
import (
	"math"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
)

const (
	// minSliceWidthSeconds is the fixed, forced width of every TimeSlice (#217/NOTE-QP-012) —
	// no longer merely the FLOOR of an adaptive [1min,1h] range (that range no longer exists;
	// see this file's top-of-file NOTE-QP-012 comment).
	minSliceWidthSeconds = 60

	// maxSafeMaxTS is the largest maxTS BuildTimeSlices computes end/window arithmetic
	// against directly. It leaves math.MaxUint64-maxSafeMaxTS >= 2*minSliceWidthSeconds of
	// headroom below math.MaxUint64 — enough that every uint64 addition performed downstream
	// of the initial start/end computation (end's own +minSliceWidthSeconds; each loop's +=
	// minSliceWidthSeconds) is guaranteed not to overflow, since every such addend is
	// <= minSliceWidthSeconds and every base value it's added to stays
	// < end <= maxSafeMaxTS + minSliceWidthSeconds. A caller passing a maxTS beyond this
	// ceiling (unreachable with real unix-second timestamps: ~1.8e9 today vs. this ceiling
	// near 2^64) has its window silently saturated to the ceiling (holistic-review Fix 3)
	// rather than producing a wrapped, corrupted End < Start.
	maxSafeMaxTS = math.MaxUint64 - 2*minSliceWidthSeconds

	// maxSlicesPerPlan caps how many TimeSlices a single BuildTimeSlices call will ever
	// attempt to allocate.
	//
	// #217/NOTE-QP-012 RAISED this from its original #487 value of 2000. Before #217, the
	// widest a single slice could ever be was 3600s (the adaptive algorithm's own ceiling), so
	// 2000 slices always meant AT LEAST 2000*3600s ≈ 83.3 days of headroom regardless of how
	// wide any individual slice happened to be. #217 forces every slice to exactly 60s, so the
	// SAME 2000 cap would instead mean at most 2000*60s ≈ 33.3 HOURS before a query stops
	// qualifying for DispatchTimeSliced and falls back to DispatchBlockSharded (unbounded
	// latency, the opposite of this task's whole goal) — silently regressing every dashboard
	// query wider than about a day and a half.
	//
	// Raised to 50,000 (50,000*60s ≈ 34.7 days) so the literal-1-minute default does not
	// regress the common case of week-to-month-wide dashboard/investigation queries, which are
	// exactly the queries #217's "any range of data can return in a fixed amount of time" goal
	// is aimed at. 50,000 TimeSlice values (Start/End uint64 + EstMatches int64 + 2 bool ≈ 32
	// bytes each after alignment) is ~1.6MB per qualifying plan — still trivial memory-wise,
	// matching the original comment's own reasoning for why a generous cap is safe. A window
	// needing more than 50,000 one-minute slices (>34.7 days) does not qualify for slice mode at
	// all and falls back to DispatchBlockSharded via BuildQueryPlan's existing zero-slices
	// fallback — no new invariant, just a new numeric ceiling. See BENCHMARKS.md BENCH-QP-010
	// for the actual measured per-job dispatch overhead this ceiling was chosen alongside.
	maxSlicesPerPlan = 50000
)

// DefaultK is the recommended default for BuildTimeSlices/BuildQueryPlan's k parameter (issue
// #487, SPEC-QP-2/NOTE-QP-008). #217/NOTE-QP-012: since BuildTimeSlices now forces every slice
// to exactly 60s regardless of concurrentRequests/k, this constant no longer affects slice width
// or count at all — kept, and callers still pass it, purely for signature/call-site stability.
const DefaultK = 1

// TimeSlice is one time-bounded sub-window of a query, carrying enough information for a
// dispatch-priority-ordering caller to reorder without re-deriving signal blockpack already
// computed. Start/End are minute-aligned unix seconds, [Start, End) half-open, matching the
// same (sec/60)*60 floor VI/VCNT write paths already use.
type TimeSlice struct {
	Start, End uint64
	// EstMatches is the estimated match count for this slice's window, from the lead leaf's
	// per-minute VCNT signal (summed across the slice's covered minutes) — a dispatch-priority
	// hint, NOT the order slices are returned in (see BuildTimeSlices doc). Only meaningful when
	// EstKnown is true.
	EstMatches int64
	// EstKnown reports whether EstMatches carries a real per-minute VCNT signal for this slice,
	// mirroring LeafCost's Known/Unknown split (NOTE-QP-001): a resolvable-but-VCNT-blind lead
	// leaf (regex, range, NEQ, numeric-between), or a plan with no VCNT-estimable leaf at all,
	// still qualifies the WHOLE PLAN for slice-mode dispatch (issue #487 binding ruling —
	// resolvable-for-every-leaf is the only Strategy gate; estimability only controls slice WIDTH
	// MODE, never Strategy — see queryplan.go's BuildQueryPlan). When EstKnown is false (the
	// uniform-width fallback), EstMatches is always 0 and carries NO signal — a caller must never
	// treat that 0 as "affirmatively empty" (that's VCNTEmpty's job) or as low dispatch priority
	// relative to an EstKnown=true, EstMatches=0 slice; the two 0s mean opposite things. When
	// EVERY slice returned by BuildTimeSlices has EstKnown=false, there is no est-matches signal
	// anywhere in the result to sort by — the chronological, most-recent-first order the slices
	// already arrive in IS the complete dispatch order in that case; a caller must not attempt a
	// separate est-matches-based reordering pass over an all-EstKnown=false result (team-lead
	// final settlement, plan-c.md Correction log).
	EstKnown bool
	// VCNTEmpty is true only when EstKnown is true AND the summed per-minute signal for this
	// slice's covered minutes is zero — an affirmative "no live matches, or VCNT hasn't been
	// written yet for this recent window" (brainstorm risk 2), distinct from "no signal was ever
	// fetched" (EstKnown=false). Callers must never skip a VCNTEmpty slice, only deprioritize it;
	// an EstKnown=false slice carries no basis for deprioritizing at all.
	VCNTEmpty bool
}

// BuildTimeSlices partitions [minTS, maxTS] into minute-aligned TimeSlices, each EXACTLY
// minSliceWidthSeconds (60s) wide — forced, unconditional, for every call (#217/NOTE-QP-012;
// the #487 adaptive/uniform width algorithms this doc comment used to describe are removed, not
// merely disabled — see this file's top-of-file NOTE-QP-012 comment for why). concurrentRequests
// and k are still accepted for call-site/signature stability (BuildQueryPlan threads them
// through unchanged) but no longer influence slice width or count in any way.
//
// When perMinute (the lead leaf's per-minute VCNT signal, valuecounts.SelectivityPerMinute's
// output) is non-empty, every slice gets EstKnown=true, with EstMatches set to that minute's
// count (VCNTEmpty=true when the count is zero, including a minute genuinely absent from
// perMinute — a gap is signal, not a hole in the partition). When perMinute is empty (no VCNT
// signal at all for the whole plan — issue #487 binding ruling: this does NOT mean "decline
// slice mode," see queryplan.go), every slice instead gets EstKnown=false, EstMatches=0,
// VCNTEmpty=false — no signal was ever fetched, which is distinct from VCNTEmpty's own
// "checked and found zero" meaning.
//
// Returned slices are chronologically ordered, most-recent-first (mirroring today's block
// ordering convention) and always fully partition [minTS, maxTS] with no gaps. Dispatch-priority
// ordering (est-matches DESC) is intentionally NOT applied here — see EstMatches' doc comment;
// that reordering is the caller's (tempo's) responsibility, kept separate from this chronological
// list per the shard-array-order/dispatch-order decoupling this design requires (brainstorm-c.md
// risk 1).
//
// SPEC-QP-2: full-partition invariant, EstKnown/VCNTEmpty/EstMatches three-state semantics.
// NOTE-QP-005: ordering rationale (chronological-not-priority) and the EstKnown=false
// degenerate-case dispatch-order note.
// NOTE-QP-012: the #217 forced-1-minute-width override and the resulting maxSlicesPerPlan raise.
func BuildTimeSlices(
	perMinute []valuecounts.MinuteCount, minTS, maxTS uint64, concurrentRequests, k int,
) []TimeSlice {
	_, _ = concurrentRequests, k // #217/NOTE-QP-012: retained for signature stability only.
	if minTS > maxTS {
		return nil
	}
	if maxTS > maxSafeMaxTS {
		// Saturate rather than let floorToMinute(maxTS)+minSliceWidthSeconds wrap uint64.
		// If minTS itself is also beyond the saturated ceiling, no safe window remains once
		// maxTS is capped — bail out exactly like any other invalid range (unreachable with
		// real unix-second timestamps).
		maxTS = maxSafeMaxTS
		if minTS > maxTS {
			return nil
		}
	}
	// floorToMinute floors sec down to the nearest 60-second boundary — the same alignment
	// VI/VCNT write paths use (tempo vcntwriter.go's minuteBucket). No exported floor helper
	// exists elsewhere in this repo to reuse (checked internal/modules/valueindex and
	// internal/modules/blockio/shared) — that floor lives in tempo's write path, out of this
	// repo's scope — so it is reimplemented here as a two-line local helper.
	start := floorToMinute(minTS)
	end := floorToMinute(maxTS) + minSliceWidthSeconds

	if (end-start)/minSliceWidthSeconds > maxSlicesPerPlan {
		// Every slice is now exactly minSliceWidthSeconds wide, so this IS the actual slice
		// count this window would produce — no valid slicing exists under the cap. Bail out
		// before attempting an allocation sized off the raw window (holistic-review Fix 6).
		return nil
	}

	haveSignal := len(perMinute) > 0
	counts := make(map[uint64]int64, len(perMinute))
	for _, c := range perMinute {
		if c.Minute < start || c.Minute >= end {
			continue
		}
		count := c.Count
		if count < 0 {
			// Defensive: perMinute is filled by a caller-supplied perMinuteForLead callback
			// (BuildQueryPlan), not required to route through
			// valuecounts.SelectivityPerMinute's own non-negative liveness-rule guarantee. A
			// non-conforming oracle returning a negative Count is clamped to zero here so it
			// can never masquerade as a genuine negative signal (holistic-review LOW finding).
			count = 0
		}
		counts[c.Minute] = count
	}

	n := (end - start) / minSliceWidthSeconds
	slices := make([]TimeSlice, 0, n)
	for m := start; m < end; m += minSliceWidthSeconds {
		if !haveSignal {
			slices = append(slices, TimeSlice{Start: m, End: m + minSliceWidthSeconds})
			continue
		}
		count := counts[m] // zero value for a minute genuinely absent from perMinute — a gap.
		slices = append(slices, TimeSlice{
			Start:      m,
			End:        m + minSliceWidthSeconds,
			EstMatches: count,
			EstKnown:   true,
			VCNTEmpty:  count == 0,
		})
	}
	reverseSlices(slices)
	return slices
}

// floorToMinute floors sec down to the nearest 60-second boundary.
func floorToMinute(sec uint64) uint64 {
	return (sec / minSliceWidthSeconds) * minSliceWidthSeconds
}

// reverseSlices reverses s in place, turning the oldest-first construction order into the
// chronological most-recent-first order BuildTimeSlices returns.
func reverseSlices(s []TimeSlice) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
