package vblockpack

// matchall_fetch_test.go — Phase 6b (plan-scan-fallback.md) test-plan items for the bounded
// newest-first match-all materializer, through the real blockpackBlock.Fetch call chain: the
// MaxTraces=0 ("unlimited", never a hard error, per #193) contract, proof the value index is
// never even consulted for this query shape (limited or not), and proof a #487 slice job's
// narrowed window is the actual safety boundary (not just an assumption) since the materializer
// never touches the value index at all and therefore cannot rely on VI-side time-partitioning
// the way the filter path does.

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetch_MatchAll_UnlimitedMaxTraces_ReturnsAllMatches_NeverTruncates is #193's required
// regression guard: common.SearchOptions.MaxTraces's own contract defines 0 as "unlimited" (its
// doc comment in tempodb/encoding/common/interfaces.go says so explicitly), a convention every
// non-search caller of Fetch relies on -- a match-all query with MaxTraces<=0 must therefore
// return the COMPLETE, untruncated match set, never hard-error and never apply some other
// silent default cap (a truncated read here is exactly the class of wrong-answer bug #481's
// whole scan-fallback-removal initiative exists to eliminate, since a metrics aggregator
// consuming Fetch's results needs every matching span to compute a correct answer). The fixture
// intentionally has more traces than any plausible accidental "default limit" (e.g. the
// frontend's own DefaultLimit=20) would allow through, so a regression that reintroduces ANY
// cap -- hard error or silent truncation -- turns this test red.
func TestFetch_MatchAll_UnlimitedMaxTraces_ReturnsAllMatches_NeverTruncates(t *testing.T) {
	dir := t.TempDir()
	const n = 30 // deliberately > any plausible accidental default cap (e.g. 20)
	meta := writeMultiLeafANDFixture(t, dir, n)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := context.Background()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 0})
	require.NoError(t, err, "MaxTraces=0 (unlimited) must never hard-error for a match-all query")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, n, "an unlimited match-all query must return every trace, not a truncated subset")
}

// TestFetch_MatchAll_DefaultSearchOptions_ProductionShape_ReturnsAllMatches (#193) exercises the
// ACTUAL reachable production shape that surfaced this bug: modules/livestore/instance_search.go's
// queryRangeCompleteBlock evaluates a local, unfiltered metrics-range query (e.g. plain
// `rate()`/`count_over_time()`, or any `{} | <agg>()`) by calling Fetch with an empty
// traceql.FetchSpansRequest (no Conditions -- conditionsToTraceQL compiles that to "{}", a
// match-all program) and common.DefaultSearchOptions() (MaxTraces left at its zero value) --
// never a synthetic MaxTraces:0 literal. This must succeed and return every match, exactly
// mirroring the general unlimited-match-all guarantee above through the real caller convention.
func TestFetch_MatchAll_DefaultSearchOptions_ProductionShape_ReturnsAllMatches(t *testing.T) {
	dir := t.TempDir()
	const n = 30
	meta := writeMultiLeafANDFixture(t, dir, n)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := context.Background()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.DefaultSearchOptions())
	require.NoError(t, err, "the real DefaultSearchOptions() production shape must never hard-error")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, n, "the real production shape must return every match, not a truncated subset")
}

// TestFetch_MatchAll_Unlimited_SkipsValueIndexEntirely extends
// TestFetch_MatchAll_WithLimit_SkipsValueIndexEntirely's invariant to the unlimited (MaxTraces=0,
// falls through to the plain scan path) case: a match-all query still has no leaf predicate for
// the value index to look up regardless of limit, so it must never be consulted here either.
func TestFetch_MatchAll_Unlimited_SkipsValueIndexEntirely(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 0})
	require.NoError(t, err)

	spansets := drainSpansets(ctx, t, resp)
	assert.Len(t, spansets, 2, "match-all must still answer correctly via the unbounded scan path")
	assert.False(t, spy.called.Load(),
		"a match-all query has no leaf predicate for the value index to look up -- it must never be consulted at all, limited or not")
}

// TestFetch_MatchAll_WithLimit_SkipsValueIndexEntirely proves the match-all hook is checked
// BEFORE tryIndexFetch ever runs -- a spy wrapping the configured value-index store must never
// be consulted for a match-all query, even with a real, fully-covered index configured and a
// positive limit present (the two conditions that would otherwise authorize/attempt the
// value-index path for a filter query).
func TestFetch_MatchAll_WithLimit_SkipsValueIndexEntirely(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 10000})
	require.NoError(t, err)

	spansets := drainSpansets(ctx, t, resp)
	assert.Len(t, spansets, 2, "match-all must still answer correctly via the newest-first materializer")
	assert.False(t, spy.called.Load(),
		"a match-all query has no leaf predicate for the value index to look up -- it must never be consulted at all")
}

// TestFetch_MatchAll_SliceJob_WindowScopingIsSafe proves the actual safety property a #487
// time-slice job depends on for a match-all query: since the materializer never touches the
// value index (no VI-side time-partitioning to lean on, unlike the filter path), the ONLY thing
// preventing a slice job from returning spans outside its narrowed [Start, End) window is
// QueryTraceQLWithProgram's own per-row time filtering (SPEC-STREAM-4, stream.go). This test
// pins that property directly rather than merely assuming it: writes traces at known, widely
// separated, distinct StartTimeUnixNano values, requests a narrowed window covering only the
// middle subset with IndexOnly=true (the real #487 slice-job shape), and asserts the response
// contains EXACTLY that subset -- not every trace in the block.
func TestFetch_MatchAll_SliceJob_WindowScopingIsSafe(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	const n = 5
	meta := writeMultiLeafANDFixture(t, dir, n)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	// writeMultiLeafANDFixture spaces trace i (1-indexed) at flooredNow + (i-1)*2min. Narrow the
	// window to cover only traces 2 and 3 (indices 1 and 2, 0-indexed), leaving traces 1, 4, and
	// 5 genuinely outside the window -- if window scoping were NOT applied at the row level,
	// every one of the n traces in this single block would come back instead.
	nowNano := uint64(time.Now().UnixNano())
	const nanosPerMinute = 60 * uint64(time.Second)
	flooredNow := (nowNano / nanosPerMinute) * nanosPerMinute
	windowStart := flooredNow + 1*2*nanosPerMinute // trace index 1 (0-indexed): "trace 2"
	windowEnd := flooredNow + 2*2*nanosPerMinute   // trace index 2 (0-indexed): "trace 3"

	ctx := context.Background()
	req := traceql.FetchSpansRequest{
		StartTimeUnixNanos: windowStart,
		EndTimeUnixNanos:   windowEnd,
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: n * 10, IndexOnly: true})
	require.NoError(t, err, "a match-all slice job with real coverage for its narrowed window must answer, not decline")

	spansets := drainSpansets(ctx, t, resp)
	gotTraceIDs := make(map[byte]bool, len(spansets))
	for _, ss := range spansets {
		require.NotEmpty(t, ss.TraceID)
		gotTraceIDs[ss.TraceID[0]] = true
	}
	wantTraceIDs := map[byte]bool{2: true, 3: true} // trace IDs are byte(i+1) in writeMultiLeafANDFixture
	assert.Equal(t, wantTraceIDs, gotTraceIDs,
		"the slice job's narrowed window must be the actual safety boundary -- only traces 2 and 3 fall inside it")
}
