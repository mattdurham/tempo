package frontend

// dispatch_events.go — issue #493, Task 5: dispatch/overlap-filter/completion-progression
// observability for the #487 time-sliced sharders (search_sharder.go, structural_sharder.go,
// metrics_query_range_sharder.go). No NOTES.md exists in this package (confirmed by search
// during planning) — inline comments are the full extent of documentation, matching this
// package's own house style for design decisions.

import (
	"context"
	"sort"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const maxAdvancementSpanEvents = 20

// attachDispatchSpanInfo attaches R2's scalar dispatch-count attributes (INCLUDING
// dispatch.jobs_skipped_overlap) and the capped, evenly-sampled completedThroughSeconds
// progression events to whatever span is already active on ctx. Use this ONLY for dispatch
// models with a genuine 1-job-per-surviving-candidate overlap filter: timeSlicedJobsFunc
// ((block, slice) pairs) and structuralTimeSlicedJobsFunc (slices only — one job per slice,
// never per (block, slice), see that function's own doc comment). totalCandidatePairs is the
// full candidate count BEFORE that filter; jobsSkippedByOverlap = totalCandidatePairs -
// jobsDispatched (clamped to 0) is only a MEANINGFUL skip count when each surviving candidate
// produces EXACTLY one job — see attachDispatchSpanInfoNoOverlapFilter's own doc comment for why
// backendJobsFunc (which can produce MANY jobs per block via page-splitting) must use that
// sibling function instead, never this one (reviewer-2 finding: the subtraction formula silently
// clamps to a misleading 0 whenever jobsDispatched > totalCandidatePairs, which is the COMMON
// case for backendJobsFunc, not a rare edge case, once any block is large enough to page-split).
//
// See attachDispatchSpanInfoCommon's own doc comment for the shared jobs_total/
// jobs_cancelled_undispatched/position/event behavior both entry points share.
func attachDispatchSpanInfo(ctx context.Context, jobsDispatched, totalCandidatePairs int, points []advancementPoint) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	jobsSkippedByOverlap := totalCandidatePairs - jobsDispatched
	if jobsSkippedByOverlap < 0 {
		jobsSkippedByOverlap = 0
	}
	span.SetAttributes(attribute.Int("dispatch.jobs_skipped_overlap", jobsSkippedByOverlap))
	attachDispatchSpanInfoCommon(ctx, span, jobsDispatched, points)
}

// attachDispatchSpanInfoNoOverlapFilter is attachDispatchSpanInfo's sibling for dispatch models
// with NO overlap-filtering concept at all — today, exactly backendJobsFunc (the
// DispatchBlockSharded/DispatchBoundedRecentFirst/nil-plan fallback, the most common dispatch
// path in production). backendJobsFunc's only "skip" is a block whose own pagesPerRequest
// returns 0 (contributing zero jobs), and — critically — a block that DOES produce jobs can
// produce MANY of them via page-splitting (jobsInBlock = ceil(TotalRecords/pages)), so there is
// no fixed 1-job-per-candidate relationship a subtraction against len(blocks) could validly
// recover: jobsDispatched (a JOB count) and totalCandidatePairs (a BLOCK count) are not
// comparable quantities on this path the way they are for timeSlicedJobsFunc's (block, slice)
// pairs.
//
// Reviewer-2 finding (issue #493, mutation-testing-style review pass): the FIRST version of this
// instrumentation reused attachDispatchSpanInfo's subtraction formula unchanged here, passing
// totalCandidatePairs=len(blocks) — for the common production case of blocks larger than
// TargetBytesPerRequest (multiple jobs per block), jobsDispatched routinely EXCEEDS len(blocks),
// so the subtraction went negative and clamped to 0 even when some blocks WERE genuinely fully
// skipped (pages==0) — silently hiding real skips behind an always-zero-looking attribute. Per
// reviewer-2's own preferred fix (option a over reintroducing a per-block-tracking closure,
// option b): dispatch.jobs_skipped_overlap is simply NOT EMITTED on this path at all — omitting
// an inapplicable attribute is more honest than a structurally-biased-toward-zero number.
func attachDispatchSpanInfoNoOverlapFilter(ctx context.Context, jobsDispatched int, points []advancementPoint) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attachDispatchSpanInfoCommon(ctx, span, jobsDispatched, points)
}

// attachDispatchSpanInfoCommon attaches the attributes/events every dispatch model shares,
// regardless of whether it has an overlap-filtering concept: dispatch.jobs_total,
// dispatch.jobs_cancelled_undispatched, dispatch.position_min/max/median, and the capped,
// evenly-sampled dispatch.progress events. Callers (attachDispatchSpanInfo/
// attachDispatchSpanInfoNoOverlapFilter) have already checked span.IsRecording() before calling
// this — it does no recording check of its own.
//
// dispatch.position_min/max/median (R2(b)): "position" is each advancement point's index within
// points — i.e. the SHARD index in dispatch-priority order (shards are rolled in the same order
// blockMetasForSearch already sorts blocks, newest-first), not a per-job or per-block index.
// Every caller buffers exactly one advancementPoint per shardIterCallback invocation (see each
// call site's own wrapping closure), so a point's slice index and its shard index are the same
// number by construction.
//
// jobsCancelledUndispatched (R2(a)) is a NECESSARILY narrower signal than its name might suggest:
// it reports whether ctx was ALREADY Done at counting time (a real, if rare, race — e.g. the
// client already disconnected before this shard-counting pass ran), in which case every counted
// job here is later dropped by buildTimeSlicedBackendRequests'/buildStructuralTimeSlicedBackendRequests'/
// buildBackendRequests' own `case <-ctx.Done(): return` branch. A job that is counted here (ctx
// still live) but individually cancelled LATER, mid-dispatch, is NOT observable from this call
// site: that happens inside a `go func() { ... }()` goroutine RoundTrip does not wait for, and by
// the time it runs, RoundTrip has typically already returned and called span.End() on this exact
// span — the same "span already ended by the time the fact is knowable" situation R2's own
// ShouldQuit design question describes (see advancementPoint's doc comment below), so per that
// same ruling no new cross-component/cross-goroutine plumbing was added to force it.
func attachDispatchSpanInfoCommon(ctx context.Context, span trace.Span, jobsDispatched int, points []advancementPoint) {
	jobsCancelledUndispatched := 0
	if ctx.Err() != nil {
		jobsCancelledUndispatched = jobsDispatched
	}

	var positions []int
	for i, p := range points {
		if p.jobs > 0 {
			positions = append(positions, i)
		}
	}
	posMin, posMax, posMedian := positionStats(positions)

	span.SetAttributes(
		attribute.Int("dispatch.jobs_total", jobsDispatched),
		attribute.Int("dispatch.jobs_cancelled_undispatched", jobsCancelledUndispatched),
	)
	if len(positions) > 0 {
		span.SetAttributes(
			attribute.Int("dispatch.position_min", posMin),
			attribute.Int("dispatch.position_max", posMax),
			attribute.Int("dispatch.position_median", posMedian),
		)
	}

	for _, p := range sampleAdvancementEvents(points, maxAdvancementSpanEvents) {
		span.AddEvent("dispatch.progress", trace.WithAttributes(
			attribute.Int("dispatch.jobs", p.jobs),
			attribute.Int64("dispatch.bytes", int64(p.bytes)), //nolint:gosec // byte sizes are well within int64 range
			attribute.Int64("dispatch.completed_through_seconds", int64(p.completedThroughSeconds)),
		))
	}
}

// positionStats returns positions' min, max, and median (lower-of-two-middles for an even
// count, matching the plan's own "min/max/median position" wording with no fractional-index
// ambiguity). Returns (0, 0, 0) for an empty input — callers must check len(positions) > 0
// before trusting these, mirroring every other *Known-guarded field in this phase.
func positionStats(positions []int) (minP, maxP, medianP int) {
	if len(positions) == 0 {
		return 0, 0, 0
	}
	minP, maxP = positions[0], positions[0]
	for _, p := range positions[1:] {
		if p < minP {
			minP = p
		}
		if p > maxP {
			maxP = p
		}
	}
	sorted := make([]int, len(positions))
	copy(sorted, positions)
	// positions are already produced in ascending order by every caller (advancement points are
	// buffered in shard order), but sort defensively rather than document-and-trust that invariant
	// across every future caller.
	sort.Ints(sorted)
	medianP = sorted[len(sorted)/2]
	return minP, maxP, medianP
}

// advancementPoint is one genuine dispatch-progression event: a shard boundary crossed during
// job counting, carrying the same (jobs, bytes, completedThroughSeconds) triple shardIterCallback
// already reports to its caller. isShouldQuitTrigger marks the point (if any) at which the
// combiner's ShouldQuit() fired.
//
// No real search/metrics/structural dispatch call site sets isShouldQuitTrigger today: R2's
// design question (does frontend.ShardSearch's span stay open long enough to receive a
// ShouldQuit event?) resolves to NO for every sharder in this package — ShouldQuit
// (combiner/common.go, combiner/trace_by_id.go) is only ever evaluated inside
// consumeAndCombineResponses (pipeline/collector_http.go), which runs AFTER
// asyncSearchSharder.RoundTrip (and every other sharder's RoundTrip) has already returned and
// therefore already called span.End() on frontend.ShardSearch/frontend.QueryRangeSharder.* —
// confirmed by reading the call chain: RoundTrip builds a plan, spawns the real per-job dispatch
// in `go func() { buildTimeSlicedBackendRequests(...) }()`, and returns an async response
// iterator immediately (deferred span.End() fires right there); httpCollector.RoundTrip only
// starts consuming that iterator (and therefore only starts checking ShouldQuit) once RoundTrip
// has already returned. Setting attributes on an already-ended span is a documented OTel SDK
// no-op, so there is no live span to attach a ShouldQuit event to at the point ShouldQuit
// actually fires. Per R2's own explicit fallback for exactly this situation: no new
// cross-component plumbing was added to force it — the "ShouldQuit fired at job N" fact is
// instead recorded as a best-effort SUMMARY ATTRIBUTE on whatever span genuinely IS active at
// that point (httpCollector.RoundTrip's own span) — see consumeAndCombineResponses,
// pipeline/collector_http.go.
//
// isShouldQuitTrigger and sampleAdvancementEvents' handling of it are kept fully implemented and
// unit-tested anyway (not deleted as dead code) because: (a) the sampling algorithm's own
// correctness under a ShouldQuit-marked point is independently valuable to pin regardless of
// whether any current caller supplies one, and (b) a FUTURE sharder or a refactor that keeps
// frontend.ShardSearch's span open for the full response lifetime (not attempted here — out of
// scope for #493, a bigger architectural change) could wire a real trigger through this same
// mechanism without any change to sampleAdvancementEvents itself.
type advancementPoint struct {
	jobs                    int
	bytes                   uint64
	completedThroughSeconds uint32
	isShouldQuitTrigger     bool
}

// sampleAdvancementEvents implements R2(c)'s exact sampling algorithm (issue #493), transcribed
// verbatim from the team-lead ruling:
//
// "completedThroughSeconds progression is OTel SPAN EVENTS (not attributes, not child spans),
// CAPPED AT 20 events per span — if more than 20 genuine advancement points occur, keep the
// first event, the ShouldQuit-trigger event if one fires (always keep this one, it's the most
// diagnostically valuable), and evenly-sampled events from the remainder to fill the cap; do not
// silently truncate to "first 20" (that would bias toward early-query behavior and hide
// late-query problems)."
//
// A capN <= 0, or len(points) <= capN, is a no-op (returns points unchanged) — there is nothing
// to sample. Kept points are always returned in their original chronological order.
func sampleAdvancementEvents(points []advancementPoint, capN int) []advancementPoint {
	if capN <= 0 || len(points) <= capN {
		return points
	}

	keep := make(map[int]bool, capN)
	keep[0] = true

	shouldQuitIdx := -1
	for i, p := range points {
		if p.isShouldQuitTrigger {
			shouldQuitIdx = i
			break
		}
	}
	if shouldQuitIdx >= 0 {
		keep[shouldQuitIdx] = true
	}

	sampleRemainder(points, keep, capN-len(keep))

	out := make([]advancementPoint, 0, capN)
	for i, p := range points {
		if keep[i] {
			out = append(out, p)
		}
	}
	return out
}

// sampleRemainder evenly stride-samples `remaining` more indices from points' not-yet-kept
// entries into keep, in-place. Split out of sampleAdvancementEvents to keep that function's own
// cyclomatic complexity down.
func sampleRemainder(points []advancementPoint, keep map[int]bool, remaining int) {
	if remaining <= 0 {
		return
	}
	candidates := make([]int, 0, len(points)-len(keep))
	for i := range points {
		if !keep[i] {
			candidates = append(candidates, i)
		}
	}
	if remaining >= len(candidates) {
		for _, i := range candidates {
			keep[i] = true
		}
		return
	}
	if remaining == 1 {
		// Inclusive-endpoint stride (below) is undefined for remaining==1 (division by zero) --
		// and R2(c)'s whole point is reaching the TAIL, so prefer the last candidate over the
		// first here rather than falling back to some other arbitrary choice.
		keep[candidates[len(candidates)-1]] = true
		return
	}
	// go-presubmit MEDIUM finding: the OLD `step := len(candidates)/remaining` formula's max k is
	// remaining-1, so the highest index it ever reached was int((remaining-1)*step) -- strictly
	// less than len(candidates)-1 whenever remaining>1, a SYSTEMATIC, growing-with-shard-count
	// blind spot at the tail of every capped dispatch. This directly contradicts R2(c)'s own
	// stated reason for choosing even sampling over "first N" in the first place ("do not
	// silently truncate ... that would bias toward early-query behavior and hide late-query
	// problems") -- the old formula still hid late-query problems, just less obviously than a
	// literal "first N" would have. This inclusive-endpoint stride instead spans the FULL
	// [0, len(candidates)-1] range: k=0 always maps to index 0, k=remaining-1 always maps to
	// EXACTLY index len(candidates)-1, with the remainder evenly spread between.
	step := float64(len(candidates)-1) / float64(remaining-1)
	for k := 0; k < remaining; k++ {
		idx := int(float64(k)*step + 0.5) // round to nearest for a more even spread than truncation
		if idx >= len(candidates) {
			idx = len(candidates) - 1
		}
		keep[candidates[idx]] = true
	}
	// Guarantee the true tail is kept regardless of any floating-point rounding error above --
	// this is the one index the whole fix exists to make reachable.
	keep[candidates[len(candidates)-1]] = true
}
