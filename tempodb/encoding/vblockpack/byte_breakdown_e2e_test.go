package vblockpack

// byte_breakdown_e2e_test.go — issue #218 Phase 7 (mandatory end-to-end verification).
//
// Every prior phase's tests drive ONE layer of the stack in isolation: Phase 4 drives
// (*blockpackBlock).Fetch directly and reads its IndexBytes/DataFileBytes closures; Phase 2/5
// drive (*blockpackBlock).QueryRange directly; Phase 6 drives the frontend combiners
// (modules/frontend/combiner/response_metrics_test.go) with hand-built *tempopb.SearchMetrics
// inputs. Per this project's standing lesson (unit-level review has repeatedly missed real bugs
// in this codebase's history), this file wires the query-execution layers together through the
// REAL production entry points end-to-end:
//
//  1. pkg/traceql.Engine.ExecuteSearch (the actual search entry point every querier/frontend
//     caller uses) driven against a REAL vblockpack block, proving engine.go's
//     fetchSpansResponse.IndexBytes()/DataFileBytes() threading (Phase 4) actually reaches
//     tempopb.SearchResponse.Metrics in production, not just in Fetch's own return value.
//  2. (*blockpackBlock).QueryRange (the real metrics query-range entry point) driven against a
//     real VI-backed block, asserting the exact real value and that a second identical real call
//     is deterministic — the shape the frontend combiner (Phase 6, verified independently by
//     modules/frontend/combiner/response_metrics_test.go's real-combiner-function tests) sums.
//  3. The same QueryRange entry point driven through a REAL, fully-covered cube registry entry
//     (mirrors cubequerypath_partial_fallback_test.go's harness), proving CubeBytesRead is
//     populated and IndexBytesRead/DataFileBytesRead/VcntBytesRead stay exactly 0 through the
//     real dispatch, not just at buildCubeQueryResponse's own unit level.
//  4. (*blockpackBlock).FindTraceByID, confirming TraceByIDMetrics stays genuinely unwired (nil,
//     not a stale/zero struct that could later be mistaken for "measured, always 0").
//
// NOTE: this file deliberately does NOT import modules/frontend/combiner. Doing so from this
// package creates a real import cycle (vblockpack -> combiner -> pkg/api -> tempodb ->
// vblockpack) — combiner transitively depends on tempodb, which depends on vblockpack itself.
// That is a structural fact about the module graph, not something to work around here; the
// combiner's own summing logic is exercised against the real production Combine functions by
// modules/frontend/combiner/response_metrics_test.go (TestQueryRangeMetricsCombiner_*,
// TestSearchMetricsCombiner_*, TestMetadataMetricsCombiner_*), which together with this file's
// per-response, real-query-path assertions cover the full pipeline.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// spansetFetcherFor adapts *blockpackBlock's common.Searcher-shaped Fetch/FetchSpans (which take
// a common.SearchOptions) to traceql.SpansetFetcher (which does not) via
// traceql.NewSpansetFetcherWrapperBoth — the exact same adapter
// modules/querier/querier_query_range.go's real production queryBlock uses for every other
// storage encoding, so traceql.Engine.ExecuteSearch below drives the REAL Fetch/FetchSpans
// methods exactly as production does.
func spansetFetcherFor(block *blockpackBlock) traceql.SpansetFetcher {
	return traceql.NewSpansetFetcherWrapperBoth(
		func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, common.SearchOptions{})
		},
		func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansOnlyResponse, error) {
			return block.FetchSpans(ctx, req, common.SearchOptions{})
		},
	)
}

// item 1a: a real, VI-answered filtered search query driven through the REAL
// traceql.Engine.ExecuteSearch entry point (not a direct Fetch call, and not a hand-built
// fetcher as pkg/traceql/engine_test.go's own IndexBytes/DataFileBytes test uses). Cross-checks
// the exact real istats.BytesRead value against the same "index.bytes_read" span attribute
// search_index_bytes_test.go's Fetch-level test already asserts on, proving engine.go's
// threading (Phase 4) reaches the actual SearchResponse in production.
func TestE2E_ExecuteSearch_VIAnswered_ReportsIndexBytesRead(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-alpha" }`, false)
	searchReq := &tempopb.SearchRequest{Query: `{ resource.service.name = "svc-alpha" }`, Limit: 100}

	engine := traceql.NewEngine()
	resp, err := engine.ExecuteSearch(ctx, searchReq, spansetFetcherFor(block))
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)
	require.NotEmpty(t, resp.Traces, "the real query must have matched svc-alpha's real spans")

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok, "ExecuteSearch must have driven the real Fetch, which emits its own span")
	rawWant, ok := attrValue(span, "index.bytes_read")
	require.True(t, ok, "the value-index-answered path must record index.bytes_read on the span")
	wantIndexBytes, err := strconv.ParseInt(rawWant, 10, 64)
	require.NoError(t, err)
	require.NotZero(t, wantIndexBytes, "a real VI-answered query must have read a nonzero number of index bytes")

	require.EqualValues(t, wantIndexBytes, resp.Metrics.IndexBytesRead,
		"issue #218 Phase 7: traceql.Engine.ExecuteSearch's real SearchResponse.Metrics.IndexBytesRead "+
			"must equal the exact real istats.BytesRead value, end-to-end through the real engine entry point")
	require.EqualValues(t, 0, resp.Metrics.DataFileBytesRead,
		"an index-answered query performs no block scan; DataFileBytesRead must be exactly 0")
}

// item 1b: a real match-all bounded search (never consults the value index) driven through the
// same real traceql.Engine.ExecuteSearch entry point, proving DataFileBytesRead — not just
// IndexBytesRead — reaches the real SearchResponse end-to-end.
func TestE2E_ExecuteSearch_MatchAllBounded_ReportsDataFileBytesRead(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	searchReq := &tempopb.SearchRequest{Query: `{}`, Limit: 10000}

	engine := traceql.NewEngine()
	resp, err := engine.ExecuteSearch(ctx, searchReq, spansetFetcherFor(block))
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok, "ExecuteSearch must have driven the real Fetch, which emits its own span")

	var wantDataFileBytes int64
	for _, kv := range span.Attributes() {
		key := string(kv.Key)
		if strings.HasPrefix(key, "scan.") && strings.HasSuffix(key, ".bytes_read") {
			wantDataFileBytes += kv.Value.AsInt64()
		}
	}
	require.NotZero(t, wantDataFileBytes, "the bounded materializer must have performed a real block scan with nonzero bytes")

	require.EqualValues(t, 0, resp.Metrics.IndexBytesRead, "match-all bypasses the value index entirely; IndexBytesRead must be exactly 0")
	require.EqualValues(t, wantDataFileBytes, resp.Metrics.DataFileBytesRead,
		"issue #218 Phase 7: traceql.Engine.ExecuteSearch's real SearchResponse.Metrics.DataFileBytesRead "+
			"must equal the exact sum of scan.<step>.bytes_read span attributes, end-to-end through the real engine entry point")
}

// item 2: a real, VI-answered metrics query-range driven through the REAL
// (*blockpackBlock).QueryRange entry point, TWICE (mirroring two per-minute #217 slice jobs
// answering the same window) — this is the explicit regression guard for Phase 2's behavior
// change (before #218, InspectedBytes was unconditionally 0 on this path) at the real dispatch
// entry point, and proves the per-response value is deterministic/real (not a random or
// leftover value from a prior call) across repeated real invocations — the exact shape
// modules/frontend/combiner/response_metrics_test.go's TestQueryRangeMetricsCombiner_
// SumsIndexDataFileAndCubeBytes then sums against the real production Combine function.
func TestE2E_QueryRange_VIAnswered_ReportsIndexBytesRead_EqualsInspectedBytes(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq(`{ resource.service.name = "svc-alpha" } | count_over_time()`)

	resp1, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp1.Metrics)
	require.NotZero(t, resp1.Metrics.IndexBytesRead, "a real VI-answered query must report non-zero IndexBytesRead")
	require.Equal(t, resp1.Metrics.IndexBytesRead, resp1.Metrics.InspectedBytes,
		"issue #218: InspectedBytes must exactly mirror IndexBytesRead on the VI metrics path, not silently "+
			"stay 0 (this is the Phase 2 behavior-change regression guard, driven through the real QueryRange entry point)")

	// A second real call, standing in for a second #217 per-minute slice job answering the same
	// tenant/query — proves the real per-response value the combiner sums is stable and genuinely
	// re-derived from real I/O each call, not a fluke of a single invocation.
	resp2, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.NoError(t, err)
	require.Equal(t, resp1.Metrics.IndexBytesRead, resp2.Metrics.IndexBytesRead,
		"two identical real queries against the same real block must read the same real byte count")
	require.Equal(t, resp2.Metrics.IndexBytesRead, resp2.Metrics.InspectedBytes,
		"the InspectedBytes==IndexBytesRead invariant must hold on every real call, not just the first")
	require.Zero(t, resp2.Metrics.CubeBytesRead, "a VI-answered query never touches cube")
}

// item 3: a real, FULLY cube-covered metrics query-range driven through the REAL
// (*blockpackBlock).QueryRange entry point (mirrors cubequerypath_partial_fallback_test.go's
// harness, but with a watermark that fully covers the requested window so tryQueryFromCube
// answers immediately at backend_block.go's `case ok && cubeResp.Status != PARTIAL` branch,
// never touching VI/scan at all). Proves Phase 5's per-response guarantee
// (IndexBytesRead/DataFileBytesRead/VcntBytesRead == 0 on a cube-answered response) holds not
// just at buildCubeQueryResponse's own unit level
// (TestBuildCubeQueryResponse_SetsExactCubeBytesRead_AndLeavesOtherByteFieldsZero) but through the
// real QueryRange dispatch chain (registry lookup -> route -> S3 fetch -> rollup -> response).
func TestE2E_QueryRange_CubeAnswered_ReportsCubeBytesRead_OtherFieldsZero(t *testing.T) {
	tenant := "cube-e2e-tenant"
	const query = `{} | count_over_time() by (resource.service.name)`

	now := time.Now()
	start := now.Add(-10 * time.Minute)
	startNano := uint64(start.UnixNano())
	endNano := uint64(now.UnixNano())
	minMinute := uint32(startNano / 60_000_000_000) //nolint:gosec // test timestamps are always positive
	maxMinute := uint32(endNano / 60_000_000_000)   //nolint:gosec // test timestamps are always positive
	require.Greater(t, maxMinute, minMinute, "test window must span at least 2 minutes")

	// Real cube registry entry with FULL watermark coverage of the requested window (unlike
	// cubequerypath_partial_fallback_test.go's deliberately-partial coverage) — this is the
	// "cube answers immediately, never falls through to VI/scan" branch.
	idHex := blockpack.CubeComputeID(tenant, []string{"resource.service.name"}, nil, []string{blockpack.CubeDurationColumn})
	entry := blockpack.CubeRegistryEntry{
		CubeID:     idHex,
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: minMinute, MaxMinute: maxMinute}},
		Resolution: 1,
		CreatedAt:  1000,
	}
	// Issue #504: the cube registry is Postgres-only now (no blob/index.json fallback) --
	// loadEntries (which tryQueryFromCube calls below) reads through cqp.pgPool, so the
	// fixture entry must be seeded into a real (ephemeral testcontainers) Postgres instance,
	// not the old fake blob objStore.
	pgPool := newTestPostgresPool(t)
	reg := blockpack.NewPgCubeRegistry(pgPool, tenant)
	require.NoError(t, reg.Add(context.Background(), entry))

	// Real cube L0 file, served through a real *minio.Client hitting a local fake S3 endpoint
	// (mirrors cubequerypath_partial_fallback_test.go's own newFakeCubeS3Client/
	// buildRealPartialCubeFile helpers, reused as-is).
	cubeFileData := buildRealPartialCubeFile(t, minMinute)
	fakeClient := newFakeCubeS3Client(t, cubeFileData)

	// #508: files/lister built directly from fakeClient, mirroring ConfigureCubeQueryPath's own
	// adapter construction -- vi stays nil since this test only exercises the FOUND (cube
	// answers immediately) path, never the creation trigger.
	qp := blockpack.NewCubeQueryPath(
		&cubeFileStore{client: fakeClient, bucket: "test-bucket"},
		&minioVIStore{client: fakeClient, bucket: "test-bucket"},
		nil, pgPool, blockpack.CubeQueryPathConfig{},
	)
	cqp := &cubeQueryPath{
		client: fakeClient,
		bucket: "test-bucket",
		pgPool: pgPool,
		qp:     qp,
	}
	withCubeQueryPath(t, cqp)

	// A real VI-backed block too (so a 0 IndexBytesRead below is genuinely "cube bypassed VI",
	// not merely "VI was never configured at all").
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")
	meta, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 10)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: query,
		Start: startNano,
		End:   endNano,
		Step:  uint64(time.Minute.Nanoseconds()),
	}

	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, tempopb.PartialStatus_COMPLETE, resp.Status, "a fully-covered cube watermark must answer COMPLETE, not PARTIAL/deferred")
	require.NotNil(t, resp.Metrics)
	require.NotZero(t, resp.Metrics.CubeBytesRead, "a real cube-answered response must report a nonzero CubeBytesRead")
	require.Zero(t, resp.Metrics.IndexBytesRead, "a cube-answered response never touches VI")
	require.Zero(t, resp.Metrics.DataFileBytesRead, "a cube-answered response never touches block scan")
	require.Zero(t, resp.Metrics.VcntBytesRead, "a cube-answered response never touches VCNT")
}

// item 4: a real trace-by-ID lookup driven through the REAL (*blockpackBlock).FindTraceByID
// entry point, confirming TraceByIDMetrics stays genuinely unwired end-to-end: FindTraceByID
// never constructs a TraceByIDMetrics at all (backend_block.go returns
// &tempopb.TraceByIDResponse{Trace: trace}, leaving Metrics nil). This is the "confirm it's
// still true end-to-end, not silently leaking a stale/wrong value" check the plan calls for — a
// regression that started setting resp.Metrics to a non-nil-but-zero struct, or to a stale
// nonzero value from an unrelated read, would both be caught by this exact assertion.
func TestE2E_FindTraceByID_MetricsStayUnwired(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	blockID := uuid.New()
	meta, _ := writeSvcBlock(t, dir, viStore, tenant, blockID, "svc-alpha", 10)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	// Recover the real trace ID writeSvcBlock wrote by searching for it first (writeSvcBlock
	// does not return the trace ID directly), through the real Fetch path.
	ctx, req := svcAlphaFetchReq()
	fetchResp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
	var traceID common.ID
	for {
		ss, nerr := fetchResp.Results.Next(ctx)
		require.NoError(t, nerr)
		if ss == nil {
			break
		}
		if len(ss.Spans) > 0 {
			traceID = ss.TraceID
			break
		}
	}
	fetchResp.Results.Close()
	require.NotEmpty(t, traceID, "must have discovered a real trace ID to look up")

	resp, err := block.FindTraceByID(context.Background(), traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Trace, "the real trace-by-id index must have resolved the real trace")
	require.Nil(t, resp.Metrics,
		"issue #218 Phase 7: TraceByIDMetrics is documented as not-yet-wired on the blockpack path -- "+
			"this must stay genuinely nil end-to-end, not a stale/zero-value struct")
}
