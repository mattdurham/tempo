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

	allLeavesResolvable := CheckIndexCoverage(context.Background(), "test-tenant", prog, minTS, maxTS)
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

	allLeavesResolvable := CheckIndexCoverage(context.Background(), tenant, prog, minTS, maxTS)
	require.True(t, allLeavesResolvable, "a real block's VI coverage must be reported, not declined")

	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, minTS, maxTS)
	plan := blockpack.BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, 1000, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
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

	allLeavesResolvable := CheckIndexCoverage(context.Background(), "test-tenant", prog, minTS, maxTS)
	require.False(t, allLeavesResolvable, "a query mixing an indexable leaf with a RequirePresent-only leaf must not report full coverage")

	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, minTS, maxTS)
	plan := blockpack.BuildQueryPlan(prog, cost, allLeavesResolvable, perMinuteForLead, minTS, maxTS, 1000, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchBlockSharded, plan.Strategy)
}
