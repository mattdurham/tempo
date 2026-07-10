package queryplan

// NOTE: TimeSlice/BuildTimeSlices are new, additive code for issue #487 — no existing file
// (plan.go, cost.go, selectivity.go, vcnt_cost.go) is touched. See SPEC-QP-2 and NOTE-QP-005
// for the full contract.

import (
	"math"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
)

const (
	// minSliceWidthSeconds and maxSliceWidthSeconds are #487's stated adaptive-width clamp
	// bounds (design doc, "Slice construction": "clamp to [1 min, 1 h]").
	minSliceWidthSeconds = 60
	maxSliceWidthSeconds = 3600

	// maxSafeMaxTS is the largest maxTS BuildTimeSlices computes end/window arithmetic
	// against directly. It leaves math.MaxUint64-maxSafeMaxTS >= maxSliceWidthSeconds +
	// minSliceWidthSeconds of headroom below math.MaxUint64 — enough that every uint64
	// addition performed downstream of the initial start/end computation (end's own
	// +minSliceWidthSeconds; each loop's += width / += minSliceWidthSeconds, bounded by
	// maxSliceWidthSeconds; sliceStart's += accumWidth) is guaranteed not to overflow, since
	// every such addend is <= maxSliceWidthSeconds and every base value it's added to stays
	// < end <= maxSafeMaxTS + minSliceWidthSeconds. A caller passing a maxTS beyond this
	// ceiling (unreachable with real unix-second timestamps: ~1.8e9 today vs. this ceiling
	// near 2^64) has its window silently saturated to the ceiling (holistic-review Fix 3)
	// rather than producing a wrapped, corrupted End < Start.
	maxSafeMaxTS = math.MaxUint64 - maxSliceWidthSeconds - minSliceWidthSeconds

	// maxSlicesPerPlan caps how many TimeSlices a single BuildTimeSlices call will ever
	// attempt to allocate. 2000 * maxSliceWidthSeconds (3600s) = 7,200,000 seconds ≈ 83.3
	// days — comfortably beyond any realistic query window (block-sharded queries today
	// rarely exceed a handful of days) while remaining small enough that even the maximum
	// possible allocation (2000 TimeSlice values) is trivial. A window that would need more
	// than maxSlicesPerPlan slices EVEN AT THE WIDEST allowed per-slice width
	// (maxSliceWidthSeconds) cannot be sliced within [minSliceWidthSeconds,
	// maxSliceWidthSeconds] at all — it simply does not qualify for slice mode
	// (holistic-review Fix 6: SPEC-ROOT-001 forbids a public-API-reachable panic regardless
	// of how absurd the input is; BuildTimeSlices returns nil for such a window, which
	// BuildQueryPlan's Fix 2 zero-slices fallback already converts to DispatchBlockSharded —
	// no new invariant, no widening of the [1min,1h] width clamp).
	maxSlicesPerPlan = 2000
)

// DefaultK is the recommended default for BuildTimeSlices/BuildQueryPlan's k parameter (issue
// #487, SPEC-QP-2/NOTE-QP-008; the design doc's desiredSliceCount = concurrentRequests*k formula
// never pinned a numeric default for k — ruled here). k=1 makes desiredSliceCount exactly
// concurrentRequests, so the no-VCNT-signal uniform-width fallback partitions the window into
// precisely what one dispatch round at concurrentRequests can cover — the least-aggressive
// choice for the case where there is no signal to justify finer slicing. A caller wanting finer
// ShouldQuit cancellation granularity may still pass k>1 explicitly; DefaultK only fixes the
// literal a caller would otherwise have to invent on their own.
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

// BuildTimeSlices partitions [minTS, maxTS] into minute-aligned TimeSlices. When perMinute (the
// lead leaf's per-minute VCNT signal, valuecounts.SelectivityPerMinute's output) is non-empty,
// slices are sized adaptively — denser minutes get narrower slices, sparser/absent-signal minutes
// within the window get wider ones — each clamped to [minSliceWidthSeconds, maxSliceWidthSeconds]
// (1 minute .. 1 hour per #487's stated bounds), and every slice gets EstKnown=true (VCNTEmpty=
// true for a slice whose covered minutes are all absent from perMinute). When perMinute is empty
// (no VCNT signal at all for the whole plan — issue #487 binding ruling: this does NOT mean
// "decline slice mode," see queryplan.go), falls back to uniform-width slices of
// window/(concurrentRequests*k) (#487's stated no-signal default), each with EstKnown=false,
// VCNTEmpty=false, EstMatches=0.
//
// Returned slices are chronologically ordered, most-recent-first (mirroring today's block
// ordering convention) and always fully partition [minTS, maxTS] with no gaps. Dispatch-priority
// ordering (est-matches DESC) is intentionally NOT applied here — see EstMatches' doc comment;
// that reordering is the caller's (tempo's) responsibility, kept separate from this chronological
// list per the shard-array-order/dispatch-order decoupling this design requires (brainstorm-c.md
// risk 1).
//
// Degenerate case: when perMinute is empty, every returned slice has EstKnown=false (see
// TimeSlice.EstKnown's doc comment) — there is no est-matches signal to prioritize by, so
// dispatch order degrades to PURE most-recent-first, i.e. exactly the order already returned
// here. A caller must not invent an est-matches-based sort over an all-EstKnown=false result;
// ShouldQuit-style recency-based tail-cancellation is the only remaining optimization lever in
// that case (team-lead final settlement, plan-c.md Correction log).
//
// SPEC-QP-2: full-partition invariant, EstKnown/VCNTEmpty/EstMatches three-state semantics,
// adaptive-width clamp bounds.
// NOTE-QP-005: ordering rationale (chronological-not-priority) and the EstKnown=false
// degenerate-case dispatch-order note.
func BuildTimeSlices(
	perMinute []valuecounts.MinuteCount, minTS, maxTS uint64, concurrentRequests, k int,
) []TimeSlice {
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

	if (end-start)/maxSliceWidthSeconds > maxSlicesPerPlan {
		// Even the widest allowed slice width can't fit this window under maxSlicesPerPlan —
		// no valid slicing exists. Bail out before either construction function attempts an
		// allocation sized off the raw window (holistic-review Fix 6).
		return nil
	}

	var slices []TimeSlice
	if len(perMinute) == 0 {
		slices = uniformWidthSlices(start, end, concurrentRequests, k)
	} else {
		slices = adaptiveWidthSlices(perMinute, start, end, concurrentRequests, k)
	}
	reverseSlices(slices)
	return slices
}

// floorToMinute floors sec down to the nearest 60-second boundary.
func floorToMinute(sec uint64) uint64 {
	return (sec / minSliceWidthSeconds) * minSliceWidthSeconds
}

// desiredSliceCount derives the target slice count from concurrentRequests*k, the same knobs
// the no-signal uniform-width fallback uses, so the adaptive algorithm's per-slice matches
// target is on a comparable scale. A non-positive product falls back to 1 (a single slice)
// rather than dividing by zero.
func desiredSliceCount(concurrentRequests, k int) uint64 {
	n := concurrentRequests * k
	if n <= 0 {
		return 1
	}
	return uint64(n)
}

// clampWidth floors width to a minute boundary, then clamps it to
// [minSliceWidthSeconds, maxSliceWidthSeconds].
func clampWidth(width uint64) uint64 {
	width = floorToMinute(width)
	if width < minSliceWidthSeconds {
		return minSliceWidthSeconds
	}
	if width > maxSliceWidthSeconds {
		return maxSliceWidthSeconds
	}
	return width
}

// uniformWidthSlices is the no-VCNT-signal fallback: [start, end) split into equal-width
// slices of window/(concurrentRequests*k), clamped to the same [1min, 1h] bounds as the
// adaptive path. Every slice has EstKnown=false — no signal was ever fetched for it, which is
// distinct from VCNTEmpty's "checked and found zero" meaning, so VCNTEmpty stays false too.
func uniformWidthSlices(start, end uint64, concurrentRequests, k int) []TimeSlice {
	window := end - start
	width := clampWidth(window / desiredSliceCount(concurrentRequests, k))

	slices := make([]TimeSlice, 0, window/width+1)
	for s := start; s < end; s += width {
		e := s + width
		if e > end {
			e = end
		}
		slices = append(slices, TimeSlice{Start: s, End: e})
	}
	return slices
}

// adaptiveWidthSlices partitions [start, end) minute-by-minute, accumulating perMinute's
// signal until either the target-matches-per-slice (total matches / desiredSliceCount) is
// reached, the maximum slice width is hit, or the window's final minute is reached — whichever
// comes first. A minute with no entry in perMinute contributes zero matches but still advances
// the slice's width, so gaps become wider (never skipped, EstKnown=true/VCNTEmpty=true) slices
// rather than holes.
func adaptiveWidthSlices(
	perMinute []valuecounts.MinuteCount, start, end uint64, concurrentRequests, k int,
) []TimeSlice {
	counts := make(map[uint64]int64, len(perMinute))
	var total int64
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
			// can never push total/target negative (which would silently disable the
			// target-reached stop condition) or masquerade as a genuine negative signal
			// (holistic-review LOW finding).
			count = 0
		}
		counts[c.Minute] = count
		total += count
	}
	divisor := int64(desiredSliceCount(concurrentRequests, k)) //nolint:gosec // small positive divisor
	target := total / divisor

	var slices []TimeSlice
	sliceStart := start
	var accumWidth uint64
	var accumMatches int64
	for m := start; m < end; m += minSliceWidthSeconds {
		accumWidth += minSliceWidthSeconds
		accumMatches += counts[m]

		last := m+minSliceWidthSeconds >= end
		reachedMax := accumWidth >= maxSliceWidthSeconds
		// reachedTarget omits an "accumWidth >= minSliceWidthSeconds" conjunct deliberately:
		// accumWidth is incremented by minSliceWidthSeconds at the top of every iteration
		// before this line runs, so it is always >= minSliceWidthSeconds here by construction
		// — a redundant check, not a real guard (holistic-review LOW finding).
		reachedTarget := target > 0 && accumMatches >= target

		if !last && !reachedMax && !reachedTarget {
			continue
		}
		slices = append(slices, TimeSlice{
			Start:      sliceStart,
			End:        sliceStart + accumWidth,
			EstMatches: accumMatches,
			EstKnown:   true,
			VCNTEmpty:  accumMatches == 0,
		})
		sliceStart += accumWidth
		accumWidth = 0
		accumMatches = 0
	}
	return slices
}

// reverseSlices reverses s in place, turning the oldest-first construction order into the
// chronological most-recent-first order BuildTimeSlices returns.
func reverseSlices(s []TimeSlice) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
