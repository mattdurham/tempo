package vblockpack

// slice_errors_test.go — pins the #487/#217 IndexOnly contract: a time-slice job
// (opts.IndexOnly / req.IndexOnly) whose value index would otherwise routinely decline never
// silently falls back to a full block scan or full-scan metrics path — that part is unchanged.
// What CHANGED under #217 Phase 1: instead of hard-failing with ErrSliceIndexCoverageGap, the
// decline is now TOLERATED as an empty, successful (PartialStatus=PARTIAL-carrying, at the wire
// level) result, so one uncovered minute no longer fails the whole tenant-wide query. IndexOnly
// =false (today's default) keeps the existing decline-then-scan behavior exactly — see
// TestFetch_NoIndexReaderFallsBackToScan (value_index_inconsistency_test.go), which already
// exercises common.SearchOptions{} (IndexOnly's zero value is false) and needs no changes for
// this contract.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestFetch_IndexOnlyToleratesRoutineDeclineAsEmptyPartial (#217 task 1.1 rename/update of the
// former TestFetch_IndexOnlyReturnsTypedErrorOnRoutineDecline) pins the CURRENT search-path
// contract: with no value-index reader configured (a routine decline) and IndexOnly=true, Fetch
// must NOT fail — it tolerates the decline as an empty, successful result (never falling back
// to a full block scan either, which remains unsafe for a narrowed-window slice job).
func TestFetch_IndexOnlyToleratesRoutineDeclineAsEmptyPartial(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> routine decline

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, tenant, uuid.New(), "svc-alpha", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx, req := svcAlphaFetchReq()
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "an IndexOnly slice job's routine decline must be tolerated (#217), not hard-error")
	require.NotNil(t, resp.Results)
	ss, iterErr := resp.Results.Next(ctx)
	require.NoError(t, iterErr)
	require.Nil(t, ss, "tolerated decline must yield zero spansets, not a full block scan")
}

// TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally pins the #487 IndexOnly
// feature's PRIMARY success path: a slice job (IndexOnly=true) whose value index has
// genuine coverage for the query answers normally from the index, with no error and
// no fallback scan. Mirrors TestFetch_IndexDataInconsistencyFailsQuery's real
// coverage setup (populated block + real VI entries via fakeVISink,
// value_index_inconsistency_test.go) but WITHOUT corrupting the block's data, so the
// index path resolves cleanly instead of hitting an inconsistency.
func TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx, req := svcAlphaFetchReq()
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "IndexOnly with full index coverage must answer normally, not error")
	defer resp.Results.Close()

	var spansets []*traceql.Spanset
	for {
		ss, err := resp.Results.Next(ctx)
		require.NoError(t, err)
		if ss == nil {
			break
		}
		spansets = append(spansets, ss)
	}
	require.NotEmpty(t, spansets, "IndexOnly with full coverage must return the matching trace")
}

// TestFetch_IndexOnlyToleratedDecline_ReportsRealIndexBytesRead (#218 holistic-review MEDIUM fix)
// pins that the tolerated coverage-gap early return threads the bytes already spent probing the
// index into the returned FetchSpansResponse.IndexBytes closure, instead of silently dropping
// them. The scenario: `resource.service.name = "svc-alpha"` has genuine, real value-index
// coverage (writeSvcBlock's real VI files, real bytes read resolving this leaf) ANDed with
// `duration > 1ms`, a leaf that is architecturally undecidable against the value index
// (vibuilder.decidableTimeBucketThreshold, task #204) and therefore always declines. #210/#213
// made this AND-of-mixed-leaves case a genuine coverage-gap decline (ErrSliceIndexCoverageGap
// under IndexOnly) rather than silently dropping the undecidable leaf — the real bytes already
// spent resolving the covered leaf must still be reported, not lost, when the whole query then
// tolerates as an empty PARTIAL result.
func TestFetch_IndexOnlyToleratedDecline_ReportsRealIndexBytesRead(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(context.Background(),
		`{ resource.service.name = "svc-alpha" && duration > 1ms }`, false)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{
			{
				Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
				Op:        traceql.OpEqual,
				Operands:  traceql.Operands{traceql.NewStaticString("svc-alpha")},
			},
			{
				Attribute: traceql.NewIntrinsic(traceql.IntrinsicDuration),
				Op:        traceql.OpGreater,
				Operands:  traceql.Operands{traceql.NewStaticDuration(time.Millisecond)},
			},
		},
		AllConditions: true,
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "a coverage-gap decline on an IndexOnly slice job must be tolerated, not hard-error")
	require.NotNil(t, resp.Results)
	ss, iterErr := resp.Results.Next(ctx)
	require.NoError(t, iterErr)
	require.Nil(t, ss, "a tolerated coverage-gap decline must yield zero spansets")

	require.NotNil(t, resp.IndexBytes, "IndexBytes must be set on the tolerated coverage-gap path")
	require.Greater(t, resp.IndexBytes(), uint64(0),
		"the real bytes already spent resolving the covered svc-alpha leaf must be reported, not dropped")
}

// TestQueryRange_IndexOnlyTolerantResponseWhenNoVICoverage (#217 task 1.1 rename/update of the
// former TestQueryRange_IndexOnlyReturnsTypedErrorWhenNoVICoverage) is the metrics analog: with
// no value-index reader configured, IndexOnly=true must return a normal, empty,
// PartialStatus=PARTIAL response rather than erroring or falling through to the cube path or
// the full-scan metrics path.
func TestQueryRange_IndexOnlyTolerantResponseWhenNoVICoverage(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> no coverage

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, tenant, uuid.New(), "svc-alpha", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "an IndexOnly slice job with no index coverage must be tolerated (#217), not hard-error")
	require.NotNil(t, resp)
	require.Equal(t, tempopb.PartialStatus_PARTIAL, resp.Status)
	require.NotEmpty(t, resp.Message)
	require.Empty(t, resp.Series, "tolerated decline must carry no series data")
}

// TestQueryRange_IndexOnlyTolerantResponseOnGroupByInnerDecline (#217 task 1.1 rename/update of
// the former TestQueryRange_IndexOnlyReturnsTypedErrorOnGroupByInnerDecline) is the CRITICAL
// regression test for holistic-review Issue 1: QueryRange's FIRST gate (indexCovered, based on
// BuildValueIndexSourceForMetrics's own ANY-leaf-resolved ok) passes here — the block has
// genuine, real VI coverage for the filter leaf, written via writeSvcBlock exactly as
// TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally's positive-path fixture. But the
// query's aggregate SHAPE (count_over_time() with a group-by) is one
// ExecuteTraceMetricsFromVI's own SECOND, deeper decline gate declines regardless of leaf
// coverage. Before #217, this converted to a hard ErrSliceIndexCoverageGap error; now it is
// tolerated as an empty, PartialStatus=PARTIAL response — the whole tenant-wide query no longer
// fails over this one slice's unsupported aggregate shape.
func TestQueryRange_IndexOnlyTolerantResponseOnGroupByInnerDecline(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "an IndexOnly slice job whose query has an unsupported aggregate shape "+
		"(group-by) must be tolerated (#217), not hard-error, even though the filter leaf itself has real index coverage")
	require.NotNil(t, resp)
	require.Equal(t, tempopb.PartialStatus_PARTIAL, resp.Status)
	require.NotEmpty(t, resp.Message)
}

// TestQueryRange_IndexOnly_CoveredButEmpty_StaysComplete_NotPartial (#217 task 1.5, the
// over/under-declining boundary test): a slice job for a COVERED-but-genuinely-EMPTY minute
// (the value index has real, working coverage for the queried column — writeSvcBlock's real VI
// files — but the specific queried value has zero matching spans) must answer confidently with
// Status=COMPLETE (the zero value), NOT PartialStatus_PARTIAL. This is the negative-space
// companion to TestQueryRange_IndexOnlyTolerantResponseWhenNoVICoverage: that test proves a
// GENUINELY uncovered minute becomes tolerable PARTIAL; this test proves a covered-but-empty
// minute must NOT be conflated with a coverage gap (NOTE-VI-033's own "a covered-but-empty
// lookup is an authoritative zero-matches answer, not a decline" contract, now also checked at
// the PartialStatus wire level introduced by #217). Tests both against tryIndexFetch's existing
// two-gate design directly, per the plan's own instruction — no new gate is added here.
func TestQueryRange_IndexOnly_CoveredButEmpty_StaysComplete_NotPartial(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	// The "resource.service.name" column IS covered (real VI files exist for it, written by
	// writeSvcBlock exactly as the real write path produces them) -- just for a DIFFERENT value
	// ("svc-alpha") than the one this query asks about ("svc-does-not-exist").
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: `{ resource.service.name = "svc-does-not-exist" } | count_over_time()`,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "a covered-but-empty query must answer confidently, not decline")
	require.NotNil(t, resp)
	require.Equal(t, tempopb.PartialStatus_COMPLETE, resp.Status,
		"a covered-but-empty minute must be a real, confident zero -- never conflated with a coverage gap's PARTIAL")
	require.Empty(t, resp.Message)
}

// TestFetch_IndexOnly_CoveredButEmpty_NeverMarksPartialSignal (#217 task 1.5, search-path
// companion to TestQueryRange_IndexOnly_CoveredButEmpty_StaysComplete_NotPartial): a covered
// column with zero matches for the specific queried value must never trip
// markSliceCoveragePartial's side channel (slice_partial_response.go) — the mechanism
// querier.go's SearchBlock reads to decide whether to set the wire-level
// SearchResponse.Status=PARTIAL. Both this test and its QueryRange sibling exercise
// tryIndexFetch's existing two-gate design directly, per the plan's own instruction; no new
// gate is added for either.
func TestFetch_IndexOnly_CoveredButEmpty_NeverMarksPartialSignal(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	baseCtx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-does-not-exist" }`, false)
	ctx, sig := WithSliceCoveragePartialSignal(baseCtx)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-does-not-exist")},
		}},
		AllConditions: true,
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "a covered-but-empty query must answer confidently, not decline")
	require.NotNil(t, resp.Results)
	ss, iterErr := resp.Results.Next(ctx)
	require.NoError(t, iterErr)
	require.Nil(t, ss, "a genuinely non-matching value must yield zero spansets")

	partial, message := sig.State()
	require.False(t, partial, "a covered-but-empty minute must never mark the coverage-gap partial signal")
	require.Empty(t, message)
}
