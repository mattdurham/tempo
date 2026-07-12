package vblockpack

// tryindexfetch_bounded_test.go — F-7 (issue #481 part 3, team-lead rulings R7/R17): regression
// guard for tryIndexFetch's boundedAuthorized-gated decline contract. Uses a REAL block
// (createFetchTestBlock, fetch_test.go) and a REAL, test-configured value-index reader
// (withVIQueryReader + fakeVISink, matching coverage_decline_test.go's established pattern) —
// never a hand-built RowSet or Selectivity value.
//
// Query shape note (verified empirically via a debug probe before writing these tests): an
// installed-but-EMPTY store answers a plain equality/range/regex/multi-value-OR leaf with an
// AUTHORITATIVE ZERO-MATCHES result (NOTE-VI-033), NOT a routine decline — BuildValueIndexSource's
// own ok=true means "at least one column resolved against the index" (a SHAPE question), which
// covers all of those. Only a RequirePresent-only leaf (`{ span.foo }`, no comparison at all)
// has NO representable column at all and genuinely declines (ok=false) — this is the shape all
// tests below use to reach tryIndexFetch's routine-decline path.

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"
)

// TestTryIndexFetch_RoutineDecline_WithoutBoundedAuthorization_HardErrors is a MUST per #63/R7:
// absent an explicit boundedAuthorized signal, a routine decline converts DIRECTLY to
// ErrSearchNoCoverage — never an implicit scan (the caller must observe an error,
// not (nil, false, nil) inviting a silent fallback).
func TestTryIndexFetch_RoutineDecline_WithoutBoundedAuthorization_HardErrors(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes") // installed but EMPTY — a genuine routine decline

	b, _ := createFetchTestBlock(t)
	r, err := b.newReader()
	require.NoError(t, err)
	prog, err := blockpack.CompileTraceQL(`{ span.foo }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	matches, ok, _, idxErr := b.tryIndexFetch(
		context.Background(), r, prog, `{ span.foo }`,
		blockpack.QueryOptions{}, false /* indexOnly */, false, /* boundedAuthorized */
	)
	require.False(t, ok)
	require.Nil(t, matches)
	require.Error(t, idxErr, "a routine decline without bounded authorization must hard-error, never relay (nil,false,nil)")
	require.True(t, errors.Is(idxErr, ErrSearchNoCoverage))
}

// TestTryIndexFetch_RoutineDecline_WithBoundedAuthorization_HardErrors is Phase 6's
// (plan-scan-fallback.md) companion to the WithoutBoundedAuthorization test above, superseding
// this test's own former "_Relays" name/behavior: once Phases 2-4 landed real early-stopping
// index resolution, boundedAuthorized=true no longer means "relay this decline so the caller
// can route to a SEPARATE bounded raw-block-scan path" — tryIndexFetch's own call site (above
// this function in value_index_query.go) already tried BuildValueIndexSourceBounded FIRST when
// boundedAuthorized is true, BEFORE ever reaching declineOutcomeBounded. A decline reaching here
// means the bounded, early-stopping attempt already ran and genuinely found nothing -- there is
// no separate path left to route to, so this now hard-errors exactly like the unauthorized case,
// with the SAME ErrSearchNoCoverage sentinel.
func TestTryIndexFetch_RoutineDecline_WithBoundedAuthorization_HardErrors(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes")

	b, _ := createFetchTestBlock(t)
	r, err := b.newReader()
	require.NoError(t, err)
	prog, err := blockpack.CompileTraceQL(`{ span.foo }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	matches, ok, _, idxErr := b.tryIndexFetch(
		context.Background(), r, prog, `{ span.foo }`,
		blockpack.QueryOptions{}, false /* indexOnly */, true, /* boundedAuthorized */
	)
	require.False(t, ok)
	require.Nil(t, matches)
	require.Error(t, idxErr, "Phase 6: a routine decline WITH bounded authorization must now hard-error too -- there is no separate bounded-scan path left to relay to")
	require.True(t, errors.Is(idxErr, ErrSearchNoCoverage))
}

// TestTryIndexFetch_RoutineDecline_IndexOnlySliceJob_ErrSliceIndexCoverageGap_Unchanged is #63's
// third required test: a #487 time-slice job's (indexOnly=true) routine decline stays
// ErrSliceIndexCoverageGap — completely unaffected by boundedAuthorized either way, since a
// slice job's narrowed window has no safe scan (or bounded-path) fallback regardless (R11).
func TestTryIndexFetch_RoutineDecline_IndexOnlySliceJob_ErrSliceIndexCoverageGap_Unchanged(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes")

	b, _ := createFetchTestBlock(t)
	r, err := b.newReader()
	require.NoError(t, err)
	prog, err := blockpack.CompileTraceQL(`{ span.foo }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	for _, boundedAuthorized := range []bool{false, true} {
		matches, ok, _, idxErr := b.tryIndexFetch(
			context.Background(), r, prog, `{ span.foo }`,
			blockpack.QueryOptions{}, true /* indexOnly */, boundedAuthorized,
		)
		require.False(t, ok)
		require.Nil(t, matches)
		require.Error(t, idxErr)
		require.True(t, errors.Is(idxErr, ErrSliceIndexCoverageGap),
			"indexOnly must ALWAYS decline with ErrSliceIndexCoverageGap regardless of boundedAuthorized=%v", boundedAuthorized)
	}
}

// TestTryIndexFetch_VRNil_UnchangedByBoundedAuthorization pins R8: the vr==nil zeroth category
// (index-driven path disabled entirely) is UNCHANGED and untouched by boundedAuthorized — it
// keeps relaying (nil, false, nil) for the non-indexOnly case either way (F-8 keeps the
// unconditional scan for this category, per the Open Fork ruling).
func TestTryIndexFetch_VRNil_UnchangedByBoundedAuthorization(t *testing.T) {
	withVIQueryReader(t, nil, "") // vr == nil: the index-driven path is disabled entirely

	b, _ := createFetchTestBlock(t)
	r, err := b.newReader()
	require.NoError(t, err)
	prog, err := blockpack.CompileTraceQL(`{ span.foo }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	for _, boundedAuthorized := range []bool{false, true} {
		matches, ok, _, idxErr := b.tryIndexFetch(
			context.Background(), r, prog, `{ span.foo }`,
			blockpack.QueryOptions{}, false /* indexOnly */, boundedAuthorized,
		)
		require.False(t, ok)
		require.Nil(t, matches)
		require.NoError(t, idxErr, "vr==nil must relay (nil,false,nil) regardless of boundedAuthorized=%v (R8, unchanged)", boundedAuthorized)
	}
}
