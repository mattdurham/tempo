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

	// skipDispatchFreshnessMarginSeconds (issue #499, locked default — a best-reasoned default,
	// not a measured-optimal one, per this project's own convention for exactly this kind of
	// tunable, mirroring #181's retry-backoff numbers) is how close to the query's own maxTS a
	// minute must be before SkipDispatch is unconditionally forced false, regardless of what any
	// AND-conjoined leaf's oracle reports.
	//
	// Reasoning: a minute's VCNT signal is only queryable once the block containing it is
	// completed and flushed, which happens once per consume cycle — blockbuilder's
	// ConsumeCycleDuration, default 5 minutes (modules/blockbuilder/config.go:85, tempo repo) — a
	// span landing near the START of a cycle can wait nearly the full cycle before its block (and
	// therefore its VCNT record) exists at all. Once flushed, a querier must also notice the new
	// block exists — governed by tempodb.DefaultBlocklistPoll, also 5 minutes
	// (tempodb/config.go:27, tempo repo). Worst-case default lag from "span occurs" to "its VCNT
	// signal is visible to a plan-building query" is therefore approximately
	// ConsumeCycleDuration + BlocklistPoll ~= 10 minutes, plus encode/upload/propagation slack not
	// captured by either constant alone.
	//
	// 15 minutes (900s) comfortably exceeds that ~10-minute worst case (1.5x) and matches this
	// project's own already-established safety margin for a directly comparable block-lifecycle
	// timing concern (compaction_window must be >= 15m for 3-min block cycles in this project's
	// real dev-03 deployment) rather than inventing an unrelated number. Anchored to maxTS, not
	// wall-clock time.Now(), so BuildTimeSlices/BuildQueryPlan remain pure functions of their
	// inputs (deterministic, no new `now` parameter, no new signature-level API surface) — for a
	// live/recent query (maxTS close to real "now"), this correctly protects the last 15 minutes
	// of VCNT signal from being trusted for a skip decision; for a purely historical query (maxTS
	// far in the past), the ONLY cost is that the last 15 minutes of that OLD window are also
	// never skip-eligible — a small, bounded, per-query inefficiency (a handful of extra
	// dispatched jobs at the tail of the window, never a correctness issue, never unbounded),
	// which is the safe direction to err in.
	skipDispatchFreshnessMarginSeconds = 15 * 60

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
	//
	// Narrow, additive exception (issue #499, Phase 3): SkipDispatch (below) IS a sanctioned skip
	// signal, computed from a wholly different leaf set (every AND-conjoined-to-root leaf, never
	// just the lead leaf VCNTEmpty is derived from) and gated by its own freshness margin. This
	// does not reverse or narrow the "never skip on VCNTEmpty alone" rule above — a caller must
	// still never treat VCNTEmpty as a skip signal by itself; SkipDispatch is the only field whose
	// contract permits skipping a slice.
	VCNTEmpty bool
	// SkipDispatch is true when at least one leaf that is AND-conjoined all the way to the plan's
	// root (collectANDConjoinedLeaves, and_conjoined.go) has a confidently-zero per-minute signal
	// for this slice's covered minute, AND that minute is outside the freshness margin
	// (skipDispatchFreshnessMarginSeconds, anchored to maxTS) — proving the WHOLE conjunction is
	// false for that minute, independent of what any other leaf (including the LEAD leaf) reports.
	// SkipDispatch and EstMatches/EstKnown/VCNTEmpty are fully independent signals computed from
	// different leaf sets (every AND-conjoined leaf, vs. only the lead leaf) and CAN legitimately
	// disagree on the same slice: SkipDispatch=true can co-occur with EstMatches>0 when a non-lead
	// conjunct vetoes a lead leaf's own optimistic count (see BuildQueryPlan's doc comment for the
	// worked example). Callers (tempo's dispatch functions) must check SkipDispatch
	// unconditionally, never gated on EstMatches/EstKnown/VCNTEmpty.
	//
	// SPEC-QP-12.
	SkipDispatch bool
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
// andConjoinedSignals (issue #499, Phase 3) is one per-minute signal per leaf that is
// AND-conjoined all the way to the plan's root (collectANDConjoinedLeaves), independent of
// perMinute/the lead leaf. A minute is SkipDispatch-eligible when at least one of these signals
// is confidently zero for it (absent or a present Count<=0 — both mean the same thing, see the
// implementation note below) AND the minute is outside skipDispatchFreshnessMarginSeconds of
// maxTS. nil/empty means no AND-conjoined leaf ever offered a checkable signal — SkipDispatch is
// false for every slice in that case (byte-identical to every pre-#499 caller).
//
// SPEC-QP-2: full-partition invariant, EstKnown/VCNTEmpty/EstMatches three-state semantics.
// SPEC-QP-12: SkipDispatch's own contract, independence from EstMatches/EstKnown/VCNTEmpty, and
// the freshness-margin mechanism.
// NOTE-QP-005: ordering rationale (chronological-not-priority) and the EstKnown=false
// degenerate-case dispatch-order note.
// NOTE-QP-012: the #217 forced-1-minute-width override and the resulting maxSlicesPerPlan raise.
// NOTE-QP-016: the every-AND-conjoined-leaf veto design, the freshness margin's reasoning, and
// the absent-vs-present-zero conflation being deliberate for this field only.
func BuildTimeSlices(
	perMinute []valuecounts.MinuteCount, andConjoinedSignals [][]valuecounts.MinuteCount,
	minTS, maxTS uint64, concurrentRequests, k int,
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

	// One zero-lookup map per AND-conjoined leaf's own signal, precomputed once outside the
	// per-minute loop below (issue #499, Phase 3) — mirrors counts above exactly (same
	// clamp-negative-to-zero defensiveness, same non-negative contract), so the per-minute loop
	// only ever does map lookups, never re-scans.
	andCounts := make([]map[uint64]int64, len(andConjoinedSignals))
	for i, sig := range andConjoinedSignals {
		mp := make(map[uint64]int64, len(sig))
		for _, c := range sig {
			count := c.Count
			if count < 0 {
				count = 0
			}
			mp[c.Minute] = count
		}
		andCounts[i] = mp
	}

	// skipDispatchFreshnessMarginSeconds (§5.1b): any minute whose Start is within the margin of
	// maxTS is never SkipDispatch-eligible, regardless of what any AND-conjoined leaf's oracle
	// reports. Saturating subtraction: a maxTS smaller than the margin (tiny/test windows near
	// epoch 0) makes freshnessCutoff 0, so the `m < freshnessCutoff` guard below is never true for
	// any minute — every minute is treated as within the margin (ineligible), the safe direction,
	// rather than wrongly treating a tiny maxTS as "far enough in the past" to trust.
	var freshnessCutoff uint64
	if maxTS > skipDispatchFreshnessMarginSeconds {
		freshnessCutoff = maxTS - skipDispatchFreshnessMarginSeconds
	}

	n := (end - start) / minSliceWidthSeconds
	slices := make([]TimeSlice, 0, n)
	for m := start; m < end; m += minSliceWidthSeconds {
		skipDispatch := false
		if m < freshnessCutoff {
			for _, cm := range andCounts {
				// cm[m] == 0 covers BOTH a minute absent from this leaf's own per-minute signal
				// (the leaf's oracle never emits a live entry for it, per every oracle's own
				// liveness-drop contract) and a minute present with Count 0 — both mean the same
				// "confidently zero" fact for THIS question (unlike EstKnown/VCNTEmpty, which
				// must distinguish "no oracle ever looked" from "oracle looked and found zero" for
				// dispatch-PRIORITY ordering; SkipDispatch needs only a yes/no CORRECTNESS
				// verdict, so there is no third state to distinguish here).
				if cm[m] == 0 {
					skipDispatch = true
					break // per-minute early exit: sufficient once any one leaf confirms zero
				}
			}
		}

		if !haveSignal {
			slices = append(slices, TimeSlice{Start: m, End: m + minSliceWidthSeconds, SkipDispatch: skipDispatch})
			continue
		}
		count := counts[m] // zero value for a minute genuinely absent from perMinute — a gap.
		slices = append(slices, TimeSlice{
			Start:        m,
			End:          m + minSliceWidthSeconds,
			EstMatches:   count,
			EstKnown:     true,
			VCNTEmpty:    count == 0,
			SkipDispatch: skipDispatch,
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
