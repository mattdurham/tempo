package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// SPEC-OBS-006: StructuralFunnelStats -- the index-driven structural funnel's per-stage
// counters, emitted on the blockpack.query span for ExecuteStructuralFromIndex/
// ExecuteNegatedStructuralFromIndex (issue #493). NOTE-479 records the design decisions
// (nil-means-skip, TraceGroupPartial bool-not-counter, VerifiedSurvivors' semantic divergence
// between the two engines, why this struct is separate from StructuralResult).

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// StructuralFunnelStats carries per-stage candidate counts for the index-driven structural
// funnel (ExecuteStructuralFromIndex / ExecuteNegatedStructuralFromIndex). Threaded ALONGSIDE
// *StructuralResult, never added to it -- StructuralResult is the scan-path's own result type,
// and growing it here would muddy that contract.
//
// nil means "span not recording, skip all counting work" -- mirrors PlannerSpanStats' nil-means-
// skip convention (otel_spans.go). Every increment site in both engines must be guarded by a
// `stats != nil` check so an unsampled query pays zero extra cost for this instrumentation.
//
// Field semantics differ slightly between the two engines because their underlying algorithms
// differ (see each file's own package doc comment):
//   - LeadMatches/LeadTraces are keyed off the walk anchor for the positive engine (L, the exact
//     VI resolution) and off the discovery side for the negated engine (R, its only VI-resolved
//     side) -- see each engine's own doc comment for why the walk-anchor/discovery-seed split
//     does not exist symmetrically on both sides.
//   - VerifiedSurvivors is the positive engine's D3B confirmation count of R's real filter against
//     the tree-walk's survivors (a narrowing step, produces final matches directly). For the
//     negated engine, D3B's verifyCandidateSpans runs UNCONDITIONALLY on every span in the
//     resolved tree BEFORE the structural walk (ruling 3 -- it is not a narrowing-after-walk step
//     there), so VerifiedSurvivors instead reports how many of those spans confirmed the
//     left/negated filter; TreeWalkSurvivors is the count that then actually passes the negated
//     structural operator and becomes eligible for a final match. Do not compare the two engines'
//     VerifiedSurvivors values as if they measured the same funnel stage.
//   - TraceGroupPartial can only ever be 0-or-1 occurrence in practice: the candidate loop
//     (ExecuteStructuralFromIndex / ExecuteNegatedStructuralFromIndex) returns an error and
//     unwinds entirely the first time a candidate's assembled TraceGroup is Partial, so no later
//     candidate is ever evaluated. It is a bool, not a counter, to avoid implying otherwise.
type StructuralFunnelStats struct {
	LeadMatches       int
	LeadTraces        int
	CandidateTraces   int
	TraceGroupHits    int
	TraceGroupMisses  int
	TreeWalkSurvivors int
	VerifiedSurvivors int
	TraceGroupPartial bool
}

// newStructuralFunnelStatsIfRecording allocates a fresh *StructuralFunnelStats when recording is
// true, or returns nil otherwise -- extracted from ExecuteStructuralFromIndex/
// ExecuteNegatedStructuralFromIndex's own inline `if querySpan.IsRecording() { stats =
// &StructuralFunnelStats{} }` (issue #493) specifically so R5's nil-means-skip allocation claim
// is unit-testable in complete ISOLATION from attachStructuralFunnelStats' own SetAttributes
// calls below. A mutation-testing pass (reviewer-2) found that comparing
// ExecuteStructuralFromIndex's TOTAL allocs recording-vs-not doesn't isolate this specific claim:
// attachStructuralFunnelStats' own SetAttributes calls dominate that delta (~8 of ~9 allocs) and
// stay correctly gated on span.IsRecording() regardless of whether `stats` itself was gated, so a
// mutation that made ONLY the stats allocation unconditional barely moved the total and the
// broader test still passed for the wrong reason. This function's own dedicated allocation test
// (structural_funnel_stats_test.go) has no SetAttributes call anywhere in its call graph, so it
// fails immediately and specifically if this gating regresses.
func newStructuralFunnelStatsIfRecording(recording bool) *StructuralFunnelStats {
	if !recording {
		return nil
	}
	return &StructuralFunnelStats{}
}

// startStructuralIndexQuerySpan starts the shared "blockpack.query" span for BOTH structural
// index-driven engines (ExecuteStructuralFromIndex, ExecuteNegatedStructuralFromIndex) and wires
// up the recording-gated StructuralFunnelStats + blockpack.query.engine attribute in exactly ONE
// place.
//
// This exists specifically because go-presubmit's HIGH finding (issue #493): before this
// extraction, each engine independently duplicated the identical 4-line wiring pattern (`stats :=
// newStructuralFunnelStatsIfRecording(querySpan.IsRecording()); if querySpan.IsRecording() {
// querySpan.SetAttributes(...) }`), and NOTHING caught a mutation that hardcoded `true` at the
// negated engine's own copy -- verified two ways: (a) the reviewer's own mutation probe, and (b)
// an INDEPENDENT mutation probe run while fixing this, which additionally found that even an
// EXACT allocs/op comparison on the full ExecuteNegatedStructuralFromIndex call (not just the
// `<` comparison the reviewer's own suggested fix used) could not reliably detect the mutation --
// the measured not-recording allocs/op was IDENTICAL (280 vs 280) between the mutated and
// unmutated negated engine across a cache-busted rebuild, despite the isolated
// newStructuralFunnelStatsIfRecording test proving the underlying 1-allocation difference is
// real in ISOLATION. Whatever escape-analysis/inlining interaction causes that at this specific,
// larger call site, it means allocation-profiling is NOT a reliable instrument for catching this
// class of copy-paste regression at the ExecuteNegatedStructuralFromIndex level specifically --
// so instead of writing a MORE ELABORATE allocation test, the duplication itself is removed:
// with only ONE call site left (this function), TestStartStructuralIndexQuerySpan_
// StatsGatedByRecording (structural_funnel_stats_test.go) asserts directly on the returned
// *StructuralFunnelStats POINTER's nilness -- a 100%-reliable, non-probabilistic check -- and
// protects both engines simultaneously, since both now call this and only this.
func startStructuralIndexQuerySpan(ctx context.Context) (context.Context, trace.Span, *StructuralFunnelStats) {
	ctx, querySpan := tracer.Start(ctx, "blockpack.query")
	stats := newStructuralFunnelStatsIfRecording(querySpan.IsRecording())
	if querySpan.IsRecording() {
		querySpan.SetAttributes(attribute.String("blockpack.query.engine", "structural_index"))
	}
	return ctx, querySpan, stats
}

// attachStructuralFunnelStats sets stats' fields as attributes on span. A no-op unless
// span.IsRecording() and stats != nil (nil-means-skip, per this file's own doc comment).
// blockpack.query.engine is NOT set here -- callers set it unconditionally, once, right after
// starting the span (see ExecuteStructuralFromIndex/ExecuteNegatedStructuralFromIndex), so it is
// present on every blockpack.query span this engine produces, including the error-returning
// paths that never reach a stats-attach call (R3's own success criterion: the engine attribute
// must be present on every span, not only the ones with a complete funnel to report).
func attachStructuralFunnelStats(span trace.Span, stats *StructuralFunnelStats) {
	if !span.IsRecording() || stats == nil {
		return
	}
	span.SetAttributes(
		attribute.Int("blockpack.structural.lead_matches", stats.LeadMatches),
		attribute.Int("blockpack.structural.lead_traces", stats.LeadTraces),
		attribute.Int("blockpack.structural.candidate_traces", stats.CandidateTraces),
		attribute.Int("blockpack.structural.tracegroup_hits", stats.TraceGroupHits),
		attribute.Int("blockpack.structural.tracegroup_misses", stats.TraceGroupMisses),
		attribute.Bool("blockpack.structural.tracegroup_partial", stats.TraceGroupPartial),
		attribute.Int("blockpack.structural.tree_walk_survivors", stats.TreeWalkSurvivors),
		attribute.Int("blockpack.structural.verified_survivors", stats.VerifiedSurvivors),
	)
}
