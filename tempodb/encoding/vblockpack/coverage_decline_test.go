package vblockpack

// coverage_decline_test.go — T5/#155: pins that CheckIndexCoverage's reuse of
// tryIndexFetch's own first decline gate (BuildValueIndexSource's ok return) actually
// forces blockpack.BuildQueryPlan's Strategy the way #487's frontend call site (T3/#153)
// depends on: a routine decline (no VI coverage) keeps Strategy at DispatchBlockSharded end to
// end, while genuine VI coverage lets Strategy reach DispatchTimeSliced.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"
)

const svcAlphaQuery = `{ resource.service.name = "svc-alpha" }`

// TestQualification_IndexCoverageDeclineForcesBlockSharded pins the #155 contract: when the
// configured value index has no coverage for a query's leaf (a routine decline — the same
// condition that makes tryIndexFetch's own `if !ok { ...decline... }` gate fire), reusing that
// verdict as blockpack.BuildQueryPlan's allLeavesResolvable keeps the resulting Strategy at
// DispatchBlockSharded, exactly as if the frontend had no plan at all — through to the
// sharder's branch (asyncSearchSharder.backendRequests / queryRangeSharder.backendRequests
// both key their dispatch entirely off Strategy, per T2/#152).
func TestQualification_IndexCoverageDeclineForcesBlockSharded(t *testing.T) {
	// The index-driven query path disabled (no reader configured) is tryIndexFetch's own
	// first, unconditional decline trigger (`vr := getValueIndexQueryReader(); if vr == nil {
	// ...decline... }`, value_index_query.go). NOTE: an installed-but-empty store is NOT a
	// decline in this system's design — NOTE-VI-033 treats a covered-but-empty column lookup
	// as a legitimate, authoritative "zero matches" answer, not a routine decline; declines
	// are reserved for query SHAPES the index cannot represent at all (multi-value OR,
	// negation, RequirePresent-only leaves) or, as here, the index path being disabled.
	withVIQueryReader(t, nil, "")

	prog, err := blockpack.CompileTraceQL(svcAlphaQuery, blockpack.QueryOptions{})
	require.NoError(t, err)

	minTS := uint64(time.Now().Add(-10 * time.Minute).Unix())
	maxTS := uint64(time.Now().Add(10 * time.Minute).Unix())

	allLeavesResolvable := CheckIndexCoverage(prog)
	require.False(t, allLeavesResolvable, "an empty index store must decline, not report coverage")

	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, minTS, maxTS)
	plan := blockpack.BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, 1000, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchBlockSharded, plan.Strategy)
}

// TestQualification_IndexCoverageMatchAllowsTimeSliced is the positive-path companion: with
// genuine VI coverage for the query's leaf (a real block's value-index files, written by
// writeSvcBlock exactly as the querier's own write path produces them), CheckIndexCoverage
// reports true and BuildQueryPlan can reach DispatchTimeSliced — confirming the #155 wiring
// does not just always decline.
func TestQualification_IndexCoverageMatchAllowsTimeSliced(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	_, _ = writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	prog, err := blockpack.CompileTraceQL(svcAlphaQuery, blockpack.QueryOptions{})
	require.NoError(t, err)

	minTS := uint64(time.Now().Add(-10 * time.Minute).Unix())
	maxTS := uint64(time.Now().Add(10 * time.Minute).Unix())

	allLeavesResolvable := CheckIndexCoverage(prog)
	require.True(t, allLeavesResolvable, "a real block's VI coverage must be reported, not declined")

	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, minTS, maxTS)
	plan := blockpack.BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, 1000, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
}

// TestCheckIndexCoverage_IgnoresWatermarks_EvenWithHalfBackfill (#217, Phase 2.1) pins the fix
// for the reported bug: a query with full backfill for HALF a window used to report
// allLeavesResolvable=false (decline the WHOLE window) because CheckIndexCoverage consulted
// watermarksForOrNil/BuildValueIndexSource's whole-window coverage gate. After the fix,
// CheckIndexCoverage is a pure shape+deployment check — it no longer consults watermarks at
// all — so it must report true even though the SAME watermark state would make
// BuildValueIndexSource itself (still called per-slice by tryIndexFetch) report incomplete
// coverage for the whole window. Per-slice coverage is decided locally and authoritatively by
// each slice job's own tryIndexFetch call (Phase 1/2's dispatch redesign), not by this
// frontend-side, block-independent proxy.
func TestCheckIndexCoverage_IgnoresWatermarks_EvenWithHalfBackfill(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	_, _ = writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	// The query window is a full 2h; the column's backfill watermark only confirms coverage for
	// the NEWEST half (WatermarkSec set to the window's midpoint) — the classic "full backfill
	// for half a window" shape this whole task fixes.
	windowStart := uint64(time.Now().Add(-2 * time.Hour).Unix())
	windowEnd := uint64(time.Now().Unix())
	midpoint := windowStart + (windowEnd-windowStart)/2

	fakeCache := &viWatermarkCache{
		ttl: time.Hour,
		now: time.Now,
		entries: map[string]viWatermarkCacheEntry{
			tenant: {
				fetched: time.Now(),
				// Issue #536: keyed by blockpack.ColumnWatermarkKey(colName, colType), not
				// colName alone -- resource.service.name resolves as a "string" leaf.
				watermarks: map[string]blockpack.ColumnWatermark{
					blockpack.ColumnWatermarkKey("resource.service.name", "string"): {
						Triggered: true, Done: false, WatermarkSec: midpoint,
						GapRanges: []blockpack.ColumnWatermarkGapRange{{StartSec: 0, EndSec: midpoint}},
					},
				},
			},
		},
	}
	prevCache := getViWatermarkCache()
	setViWatermarkCache(fakeCache)
	t.Cleanup(func() { setViWatermarkCache(prevCache) })

	prog, err := blockpack.CompileTraceQL(svcAlphaQuery, blockpack.QueryOptions{})
	require.NoError(t, err)

	// Sanity check: prove the watermark state really would have declined the whole window under
	// the OLD (pre-#217) BuildValueIndexSource-based gate, so this test is anchored to a genuine
	// coverage-gap scenario rather than a vacuous one.
	cache := viStore // fakeVISink also satisfies the reader's cacheFor-style lookup in this test file's helpers
	_ = cache
	vr := getValueIndexQueryReader()
	require.NotNil(t, vr, "test setup: reader must be configured")
	src, ok, buildErr := blockpack.BuildValueIndexSource(
		context.Background(), vr.cacheFor(tenant), vr.store, prog, windowStart, windowEnd,
		watermarksForOrNil(context.Background(), tenant),
	)
	require.NoError(t, buildErr)
	require.False(t, ok, "test setup: half-backfill watermark must still make BuildValueIndexSource decline the whole window")
	require.Nil(t, src)

	// The actual assertion: CheckIndexCoverage must NOT decline, because it no longer consults
	// watermarks at all — only shape (AllLeavesIndexable) and deployment (reader configured).
	allLeavesResolvable := CheckIndexCoverage(prog)
	require.True(t, allLeavesResolvable,
		"CheckIndexCoverage must ignore watermark state entirely; per-slice coverage is decided by each slice's own tryIndexFetch")
}

// TestQualification_MixedIndexableAndNegatedLeafForcesBlockSharded is the T5b/#162 regression
// test: `{ span.foo != "bar" }` compiles to a RequirePresent-only leaf (NOTE-453's presence
// requirement, not indexable) ANDed with an OR-of-two-range leaves (each individually
// indexable). Before T5b, CheckIndexCoverage relied solely on BuildValueIndexSource's own ok,
// which only requires ONE leaf in the whole program to have an indexable shape — the two range
// leaves alone would satisfy it, even against a completely empty index (BuildValueIndexSource's
// ok is a query-SHAPE check, not a data-presence one; see CheckIndexCoverage's doc comment) —
// so this exact query would have wrongly qualified for DispatchTimeSliced and then, at
// dispatch time, an IndexOnly=true job for the RequirePresent leaf's coverage gap would FAIL
// with ErrSliceIndexCoverageGap where today it works block-sharded. T5b's
// blockpack.AllLeavesIndexable AND closes that gap: this test pins that the mixed-shape query
// now declines and Strategy stays DispatchBlockSharded, with only an empty-but-enabled index
// reader (no VI files at all) — proving the decline is driven by leaf SHAPE, not data absence.
func TestQualification_MixedIndexableAndNegatedLeafForcesBlockSharded(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes")

	prog, err := blockpack.CompileTraceQL(`{ span.foo != "bar" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minTS := uint64(time.Now().Add(-10 * time.Minute).Unix())
	maxTS := uint64(time.Now().Add(10 * time.Minute).Unix())

	allLeavesResolvable := CheckIndexCoverage(prog)
	require.False(t, allLeavesResolvable, "a query mixing an indexable leaf with a RequirePresent-only leaf must not report full coverage")

	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, minTS, maxTS)
	plan := blockpack.BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, 1000, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchBlockSharded, plan.Strategy)
}

// TestCheckIndexCoverage_DurationStrictGT_NowCoverageEligibleViaResidualPushdown
// (#217 Phase 4.2's original red-herring regression guard, UPDATED by issue #534) used to pin
// Phase 0's grounding-pass finding that `{duration > 1ms}` is architecturally UNDECIDABLE
// (vibuilder.decidableTimeBucketThreshold's own derivation — a strict `>` at an exact
// millisecond-aligned threshold cannot be resolved against the millisecond-bucketed dedicated
// duration column, task #204) and must therefore decline outright. Issue #534 deliberately
// changed this: the ambiguity is now resolved via residual pushdown (vibuilder widens the
// value-index query to also capture the one genuinely ambiguous boundary bucket, and attaches
// the ORIGINAL exact predicate as a residual re-checked against the real value once the
// candidate's block is fetched for materialization — see blockpack's
// executor.ResidualColumnPredicate/NOTE-VI-123) — so this shape is now genuinely
// coverage-eligible, exactly like `{duration >= 1ms}` always was. This test's ORIGINAL purpose
// (a change to CheckIndexCoverage's own simplification logic must not silently alter which
// query shapes are answerable from the index) still applies; only the expected answer for THIS
// specific shape flipped, deliberately, with #534's own fix.
func TestCheckIndexCoverage_DurationStrictGT_NowCoverageEligibleViaResidualPushdown(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes")

	gtProg, err := blockpack.CompileTraceQL(`{ duration > 1ms }`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.True(t, CheckIndexCoverage(gtProg),
		"{duration > 1ms} is now coverage-eligible (issue #534's residual pushdown resolves the "+
			"one ambiguous boundary bucket against the real value once the block is fetched)")

	geProg, err := blockpack.CompileTraceQL(`{ duration >= 1ms }`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.True(t, CheckIndexCoverage(geProg),
		"{duration >= 1ms} was already decidable at the SAME threshold (only the operator differs) and must remain eligible")
}
