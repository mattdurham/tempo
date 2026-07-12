package vblockpack

// fetch_bounded_dispatch_test.go — F-8 (issue #481 parts 2/3, team-lead rulings R7/R8/R11-AMENDED/
// R17): real-conversion-path regression guards for Fetch/QueryRange's decline-routing rewrite —
// through the REAL production entry points (blockpackBlock.Fetch/QueryRange), never a direct
// tryIndexFetch/declineOutcomeBounded call (tryindexfetch_bounded_test.go already covers that
// narrower unit boundary). Each test pins one row of the routing table Fetch/QueryRange now
// implement instead of an unconditional scan fallback:
//
//   - a routine per-block decline WITH a limit present routes to F-2's/F-3's bounded
//     newest-first path and succeeds (never scans, never hard-errors)
//   - a routine per-block decline WITHOUT a limit hard-errors with ErrSearchNoCoverage
//     (search) or one of blockpack's F-4 sentinels (metrics) — never scans
//   - vr==nil (value_index_query.enabled=false) now hard-errors with
//     ErrMaterializedIndexBuilding for search too (Phase 0 removed the last unconditional
//     scan site) — regardless of any limit present, since there is no index to have
//     declined against in the first place
//   - vr==nil for METRICS is handled the same way it always was: a distinct, permanent
//     config-level typed error (ErrMetricsValueIndexDisabled), never a scan
//   - a #487 slice job (IndexOnly=true) NEVER routes to the bounded path regardless of any
//     limit present (R11-AMENDED's absolute priority) — it always hard-errors with
//     ErrSliceIndexCoverageGap on a routine decline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// requirePresentDeclineReq returns a Fetch request equivalent to `{ span.foo }` — a
// RequirePresent-only leaf with no comparison at all, confirmed (via debug probe, see
// tryindexfetch_bounded_test.go's own doc comment) to be the one leaf shape an installed-but-EMPTY
// value-index store genuinely declines on (NOTE-VI-033: every comparison/equality/range/OR-multi-
// value leaf against an empty store instead answers an AUTHORITATIVE ZERO-MATCHES result, which is
// NOT a routine decline).
func requirePresentDeclineReq() (context.Context, traceql.FetchSpansRequest) {
	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ span.foo }`, false)
	return ctx, traceql.FetchSpansRequest{}
}

// erroringVIStore wraps a real valueIndexStore and forces every List call to fail, simulating a
// build-time object-store discovery failure at BuildValueIndexSource's own gate — a per-block
// failure mode a frontend's coarse, tenant-level VCNT-based Selective classification could never
// have predicted (R17's "coarse-signal" case). This is deliberately a DIFFERENT decline mechanism
// than requirePresentDeclineReq's ok=false/no-representable-column case: it exercises
// tryIndexFetch's OTHER decline branch (BuildValueIndexSource returning a non-nil err), which also
// routes through declineOutcomeBounded per its own doc comment.
type erroringVIStore struct {
	valueIndexStore
}

func (e *erroringVIStore) List(_ context.Context, _ string) ([]string, error) {
	return nil, errors.New("simulated object-store discovery failure")
}

// writeThreeNodeChainBlock writes a 3-span trace chained root(svc-a) -> child(svc-b) ->
// grandchild(svc-c), each ParentSpanId pointing at its predecessor, so a 3-node structural query
// (`{svc-a} >> {svc-b} >> {svc-c}`) has a genuine match. Mirrors writeParentChildBlock
// (structural_dispatch_test.go) generalized to a 3-node chain — F-3's bounded structural path
// (blockpack-side) specifically targets chains that flatten to OTHER than exactly 2 nodes, so a
// real 3-node fixture is required to exercise it via Fetch rather than the 2-node structural
// index-driven path's own (unrelated) decline/answer contract.
func writeThreeNodeChainBlock(t *testing.T, dir string, blockID uuid.UUID) *backend.BlockMeta {
	t.Helper()
	const tenant = "test-tenant"
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	traceID := []byte{0x43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x43}
	rootSpanID := []byte{0x43, 0, 0, 0, 0, 0, 0, 0x01}
	childSpanID := []byte{0x43, 0, 0, 0, 0, 0, 0, 0x02}
	grandchildSpanID := []byte{0x43, 0, 0, 0, 0, 0, 0, 0x03}
	now := uint64(time.Now().UnixNano())

	mkAttr := func(k, v string) *tempocommon.KeyValue {
		return &tempocommon.KeyValue{Key: k, Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}}
	}

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-a")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: rootSpanID, Name: "root-op",
					StartTimeUnixNano: now, EndTimeUnixNano: now + uint64(300*time.Millisecond),
				}}}},
			},
			{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-b")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: childSpanID, ParentSpanId: rootSpanID, Name: "child-op",
					StartTimeUnixNano: now + uint64(10*time.Millisecond), EndTimeUnixNano: now + uint64(200*time.Millisecond),
				}}}},
			},
			{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-c")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: grandchildSpanID, ParentSpanId: childSpanID, Name: "grandchild-op",
					StartTimeUnixNano: now + uint64(20*time.Millisecond), EndTimeUnixNano: now + uint64(100*time.Millisecond),
				}}}},
			},
		},
	}

	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(5 * time.Minute)
	resultMeta, err := CreateBlock(
		context.Background(), &common.BlockConfig{}, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW),
	)
	require.NoError(t, err)
	return resultMeta
}

// TestFetch_LowSelectivityNoCoverageLeaf_WithLimit_HardErrors is Phase 6's
// (plan-scan-fallback.md) rewrite of the former "_UsesBoundedRecentFirst_NotScan" test: once
// Phases 2-4 landed real early-stopping index resolution, tryIndexFetch's own call site already
// tries BuildValueIndexSourceBounded FIRST whenever boundedAuthorized is true (see
// value_index_query.go) -- there is no SEPARATE bounded raw-block-scan path left for Fetch to
// route a decline to afterward. A routine filter-path decline (RequirePresent-only leaf,
// empty-but-configured store) with a limit present now hard-errors with ErrSearchNoCoverage,
// exactly like the no-limit case -- boundedAuthorized no longer changes this outcome for filter
// queries.
func TestFetch_LowSelectivityNoCoverageLeaf_WithLimit_HardErrors(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes") // installed but EMPTY — a genuine routine decline

	block, _ := createFetchTestBlock(t)

	ctx, req := requirePresentDeclineReq()
	_, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 5})
	require.Error(t, err, "Phase 6: a routine filter decline with a limit present must now hard-error too -- "+
		"the bounded early-stopping attempt already ran inside tryIndexFetch and found nothing")
	require.True(t, errors.Is(err, ErrSearchNoCoverage))
}

// TestFetch_SelectiveShapedQuery_PerBlockBuildError_WithLimit_HardErrors is Phase 6's rewrite of
// the former "_UsesBoundedRecentFirst" test: a query shape a frontend's coarse, tenant-level
// VCNT classification would call Selective (a plain equality leaf) still declines on THIS
// specific block (a build-time object-store discovery failure, erroringVIStore) -- the
// coarse-signal case R17 documents. With no separate bounded-scan path left for the filter
// path (Phase 6), this now hard-errors with a limit present exactly as it would without one.
func TestFetch_SelectiveShapedQuery_PerBlockBuildError_WithLimit_HardErrors(t *testing.T) {
	withVIQueryReader(t, &erroringVIStore{valueIndexStore: &fakeVISink{}}, "indexes")

	block, _ := createFetchTestBlock(t)

	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-a" }`, false)
	req := traceql.FetchSpansRequest{}
	_, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 5})
	require.Error(t, err, "Phase 6: a per-block build-time index error with a limit present must now hard-error too")
	require.True(t, errors.Is(err, ErrSearchNoCoverage))
}

// TestFetch_ThreeNodeStructuralChain_WithLimit_HardErrors is Phase 7's rewrite of the former
// "_UsesBounded" test: a genuine 3-node structural chain, under the default (non-slice) dispatch
// where tryStructuralIndexFetch always declines (value_index_structural_query.go's own
// !indexOnly guard), with a limit present, must now hard-error — Phase 7 removed the #481-part-2
// bounded-scan mechanism this test used to route through, and a structural decline under
// per-block dispatch never gets ANY bounded path, index or scan (Phase 6's asymmetry finding).
func TestFetch_ThreeNodeStructuralChain_WithLimit_HardErrors(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes") // installed but EMPTY — a genuine routine decline

	dir := t.TempDir()
	meta := writeThreeNodeChainBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	query := `{ resource.service.name = "svc-a" } >> { resource.service.name = "svc-b" } >> { resource.service.name = "svc-c" }`
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{}
	_, err = block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 5})
	require.Error(t, err, "a 3-node structural chain decline with a limit present must now hard-error too")
	require.True(t, errors.Is(err, ErrSearchNoCoverage))
}

// TestFetch_VIDisabled_HardErrors is the Phase 0 (issue #481's vr==nil-scan-removal follow-on)
// required pin: with NO value-index reader configured at all (vr == nil, the config-level
// deployment-absence category), Fetch must hard-error with ErrMaterializedIndexBuilding — even
// with no limit present at all — rather than falling back to an unconditional full scan. Renamed
// from TestFetch_VIDisabled_StillFullScans_Unchanged, which pinned the OPPOSITE (now removed)
// behavior; there is no supported deployment that scans as a fallback for vr == nil anymore.
func TestFetch_VIDisabled_HardErrors(t *testing.T) {
	withVIQueryReader(t, nil, "") // index path disabled entirely

	block, _ := createFetchTestBlock(t)

	ctx, req := requirePresentDeclineReq()
	_, err := block.Fetch(ctx, req, common.SearchOptions{}) // no limit at all
	require.Error(t, err, "vr == nil must hard-error, never fall back to an unconditional scan")
	require.True(t, errors.Is(err, ErrMaterializedIndexBuilding),
		"err = %v, want ErrMaterializedIndexBuilding", err)
}

// TestFetch_VIDisabled_HardErrors_WithLimit is the required companion to
// TestFetch_VIDisabled_HardErrors: the SAME vr == nil scenario, but WITH a limit present
// (common.SearchOptions.MaxTraces > 0) — the realistic case, since virtually every real Tempo
// search request carries a page-size/top-N limit. A limit present must NOT change the outcome:
// vr == nil still hard-errors, since there is no index to have declined against in the first
// place (a limit alone cannot authorize a bounded path when there is no index at all). Renamed
// from TestFetch_VIDisabled_StillFullScans_Unchanged_WithLimit, which pinned the OPPOSITE (now
// removed) behavior.
func TestFetch_VIDisabled_HardErrors_WithLimit(t *testing.T) {
	withVIQueryReader(t, nil, "") // index path disabled entirely

	block, _ := createFetchTestBlock(t)

	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-a" }`, false)
	req := traceql.FetchSpansRequest{}
	_, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 20})
	require.Error(t, err, "vr == nil must hard-error even with a limit present (the realistic case)")
	require.True(t, errors.Is(err, ErrMaterializedIndexBuilding),
		"err = %v, want ErrMaterializedIndexBuilding", err)
}

// TestFetch_VIDisabled_Metrics_ConfigError is the F-8 required R8 pin for the metrics side: with
// NO value-index reader configured (vr == nil), QueryRange must surface blockpack's distinct
// config-level ErrMetricsValueIndexDisabled sentinel rather than falling back to any scan — R8's
// metrics polarity is the OPPOSITE of search's (vr == nil means "cannot answer at all", never
// "scan instead").
func TestFetch_VIDisabled_Metrics_ConfigError(t *testing.T) {
	withVIQueryReader(t, nil, "") // index path disabled entirely

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, "test-tenant", uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: false})
	require.Error(t, err, "vr == nil must surface a typed config error for metrics, never fall "+
		"back to a scan")
	require.True(t, errors.Is(err, blockpack.ErrMetricsValueIndexDisabled),
		"err = %v, want blockpack.ErrMetricsValueIndexDisabled", err)
}

// TestFetch_UnsupportedMetricsShape_ProductionDefault_TypedError_NoScan is the F-8 required test:
// under the PRODUCTION DEFAULT (IndexOnly=false, not a #487 slice job), a metrics query whose
// aggregate shape ExecuteMetricsTraceQL's own SECOND gate declines (a group-by, unsupported by the
// VI-answerable shape rule) must surface as blockpack.ErrMetricsShapeNotAnswerable — F-4 deleted
// ExecuteTraceMetrics's internal full-scan fallback entirely, so there is no scan left to fall
// back to even in the non-slice-job, default-dispatch case. Mirrors
// TestQueryRange_IndexOnlyReturnsTypedErrorOnGroupByInnerDecline (slice_errors_test.go) with the
// OPPOSITE IndexOnly polarity, to confirm this typed-error contract holds unconditionally, not
// only under a #487 slice job.
func TestFetch_UnsupportedMetricsShape_ProductionDefault_TypedError_NoScan(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: false})
	require.Error(t, err, "an unsupported aggregate shape must fail even under the production "+
		"default (non-slice-job) dispatch — there is no scan fallback left to reach")
	require.True(t, errors.Is(err, blockpack.ErrMetricsShapeNotAnswerable),
		"err = %v, want blockpack.ErrMetricsShapeNotAnswerable", err)
}

// TestFetch_SliceJob_Decline_HardErrors_NeverBounded is the F-8 R11-AMENDED mandatory test: a
// #487 slice job (IndexOnly=true) hitting a routine decline must hard-error with
// ErrSliceIndexCoverageGap EVEN WHEN a limit is present on the request — indexOnly takes ABSOLUTE
// priority over boundedAuthorized (backend_block.go: `boundedAuthorized := !opts.IndexOnly &&
// spanLimit > 0`), since a bounded-but-partial read across an already-narrowed slice window would
// compound two different kinds of incompleteness. Mutation-verified: this invariant is enforced by
// TWO independent layers (Fetch's own boundedAuthorized derivation, and declineOutcomeBounded's
// own indexOnly-first branch in value_index_query.go) — this test only goes red when BOTH layers
// are broken simultaneously, confirmed by mutating each individually (still green) and then both
// together (red), before restoring both to their original state via byte-for-byte sha256-verified
// copies.
func TestFetch_SliceJob_Decline_HardErrors_NeverBounded(t *testing.T) {
	withVIQueryReader(t, &fakeVISink{}, "indexes") // installed but EMPTY — a genuine routine decline

	block, _ := createFetchTestBlock(t)

	ctx, req := requirePresentDeclineReq()
	_, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true, MaxTraces: 5})
	require.Error(t, err, "a slice job's routine decline must hard-error even with a limit present")
	require.True(t, errors.Is(err, ErrSliceIndexCoverageGap),
		"err = %v, want ErrSliceIndexCoverageGap (IndexOnly must win over boundedAuthorized)", err)
}
