package vblockpack

// cubequerypath_partial_fallback_test.go — CRITICAL fix (post-#217 review,
// .bob/state/217-review.md Issue 1): tryQueryFromCube's own PARTIAL answer must not
// short-circuit backend_block.go's QueryRange before the pre-existing VI/scan path gets a
// chance to answer the full requested window. Pre-#217, ok=false ALWAYS fell through to that
// fallback; post-#217, cube.Route's Found=true (with a narrower Covered{Min,Max}Minute) for ANY
// partial overlap meant QueryRange returned cube's own truncated answer unconditionally, never
// even attempting the fallback.
//
// This file drives the REAL production QueryRange entry point end to end: a real cube registry
// entry (partial watermark coverage) + a real cube L0 file (production write path — Accumulator.
// Add -> Encode -> OpenCubeReaderFromBytes, per this package's own "Lesson 2" convention) served
// through a real *minio.Client pointed at a local httptest fake S3 endpoint (mirroring
// minio_notfound_test.go's own established pattern for exercising the real minio-go request path
// without a live S3/minio server), plus a real VI-backed block (writeSvcBlock/withVISink/
// withVIQueryReader, mirroring cubequerypath_warming_test.go).
//
// Documented limitation (see NOTE-CUBE-027, cube SPECS/NOTES addendum dated 2026-07-13): every
// cube-answerable query has a group-by clause (tryQueryFromCube declines immediately otherwise),
// and blockpack's VI-only metrics engine (vm.MetricsShapeIsVIAnswerable, #481 having removed the
// scan-based engine outright) can NEVER answer a group-by query — it always declines with
// ErrMetricsShapeNotAnswerable regardless of actual data coverage. So "cube partial AND VI/scan
// has full coverage" is not constructible via any real query in the CURRENT codebase; the only
// reachable real-world outcome is "cube partial AND VI/scan declines," which is exactly what this
// test drives and asserts on. The fix is still implemented as specified (deferring cube's partial
// answer instead of returning it immediately) because it is architecturally correct, matches the
// stated "never decline a query we have coverage for" principle, and future-proofs against any
// later relaxation of VI's group-by restriction — but it is a no-op for every query reachable
// today. This test proves the DEFERRAL genuinely happens (observable via the
// cube.partial_deferred_to_vi_scan and metrics.decline_reason span attributes, which only the
// fixed code sets — the pre-fix code returns before ever calling ExecuteMetricsTraceQL, so neither
// attribute exists) and that cube's own partial answer still correctly serves as the
// fallback-of-last-resort once VI/scan declines, preserving #217's original guarantee.
import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcepbv1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tracepbv1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// newFakeCubeS3Client spins up a local httptest server that answers ListObjectsV2 with exactly
// one key (derived from the request's own prefix, so it works for any tenant/cubeID) and answers
// every other GET with data's raw bytes — enough surface for cqp.listObjects/cqp.getObject
// (cubequerypath.go), which talk to cqp.client directly rather than through the objectStore()
// injection seam. Mirrors minio_notfound_test.go's own established pattern of driving the REAL
// minio-go request path against a local fake HTTP endpoint instead of a hand-rolled CubeObjectStore
// fake, which would bypass the exact list+download code this test needs to exercise.
func newFakeCubeS3Client(t *testing.T, data []byte) *minio.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			prefix := r.URL.Query().Get("prefix")
			key := prefix + "L0-0000.cube"
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>test-bucket</Name><Prefix>%s</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
<Contents><Key>%s</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;etag1&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`, prefix, key, len(data))
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"etag1"`)
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(server.Close)

	client, err := minio.New(server.URL[len("http://"):], &minio.Options{
		Creds:  credentials.NewStaticV4("test", "test", ""),
		Secure: false,
		Region: "us-east-1",
	})
	require.NoError(t, err)
	return client
}

// buildRealPartialCubeFile builds a real, minimal cube L0 file (production write path, this
// package's own "Lesson 2" convention — never a hand-built Cell/AggCell literal) with one cell at
// minute for service "svc-alpha", dimensioned on "resource.service.name" (the "resource."-prefixed
// form, since extractFilters/extractGroupByDims below parse the QUERY TEXT via TraceQL's own
// parser, which requires a properly-prefixed attribute reference — unlike the hand-built
// RoutingResult tests elsewhere in this package that bypass query-text parsing entirely and can
// use the bare "service.name" intrinsic shortcut) — matching realCubeReaderAtMinute's own
// established pattern in cubequerypath_test.go.
func buildRealPartialCubeFile(t *testing.T, minute uint32) []byte {
	t.Helper()
	def := blockpack.CubeDefinition{
		Dim1Column: "resource.service.name",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{0x04},
		Resolution: 1,
	}
	acc, err := blockpack.NewCubeAccumulator(def, minute)
	require.NoError(t, err)
	sv := &tempoSpanValues{
		span: &tracepbv1.Span{StartTimeUnixNano: 0, EndTimeUnixNano: 1_000_000_000},
		resource: &resourcepbv1.Resource{
			Attributes: []*commonpbv1.KeyValue{
				{Key: "service.name", Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: "svc-alpha"}}},
			},
		},
	}
	_, err = acc.Add(sv)
	require.NoError(t, err)
	data, err := acc.Encode()
	require.NoError(t, err)
	return data
}

// TestQueryRange_CubePartialCoverage_DefersToVIScan_FallsBackWhenVIDeclines is the CRITICAL fix's
// TDD regression test. Against the pre-fix code, tryQueryFromCube's ok=true (Status=PARTIAL)
// return is used UNCONDITIONALLY at backend_block.go's cube-path branch -- ExecuteMetricsTraceQL
// is never even called, so neither the cube.partial_deferred_to_vi_scan nor the
// metrics.decline_reason span attribute this test asserts on can exist. Against the fixed code,
// the partial answer is deferred, VI/scan is genuinely attempted (and, per this package's own
// architectural constraint -- see this file's own doc comment -- always declines for a group-by
// query), and cube's partial answer is used as the fallback-of-last-resort, exactly reproducing
// #217's original "never decline a query we have coverage for" guarantee via the correct code path
// instead of an accidental unconditional short-circuit.
func TestQueryRange_CubePartialCoverage_DefersToVIScan_FallsBackWhenVIDeclines(t *testing.T) {
	tenant := "cube-partial-fallback-tenant"
	const query = `{} | count_over_time() by (resource.service.name)`

	now := time.Now()
	start := now.Add(-10 * time.Minute)
	startNano := uint64(start.UnixNano())
	endNano := uint64(now.UnixNano())
	minMinute := uint32(startNano / 60_000_000_000) //nolint:gosec // test timestamps are always positive
	maxMinute := uint32(endNano / 60_000_000_000)   //nolint:gosec // test timestamps are always positive
	require.Greater(t, maxMinute, minMinute, "test window must span at least 2 minutes")
	coveredMax := minMinute + (maxMinute-minMinute)/2

	// Real cube registry entry with PARTIAL watermark coverage: only [minMinute, coveredMax]
	// of the requested [minMinute, maxMinute] window is covered. Issue #504: the cube
	// registry is Postgres-only now (no blob/index.json fallback) -- loadEntries (which
	// tryQueryFromCube calls below) reads through cqp.pgPool, so this must be seeded into a
	// real (ephemeral testcontainers) Postgres instance, not the old fake blob objStore via
	// cubequerypath.go's (now-removed) objectStore() DI seam.
	idHex := blockpack.CubeComputeID(tenant, []string{"resource.service.name"}, nil, []string{blockpack.CubeDurationColumn})
	entry := blockpack.CubeRegistryEntry{
		CubeID:     idHex,
		Tenant:     tenant,
		Dimensions: []string{"resource.service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: minMinute, MaxMinute: coveredMax}},
		Resolution: 1,
		CreatedAt:  1000,
	}
	pgPool := newTestPostgresPool(t)
	reg := blockpack.NewPgCubeRegistry(pgPool, tenant)
	require.NoError(t, reg.Add(context.Background(), entry))

	// Real cube L0 file with a cell inside the covered sub-range, served through a real
	// *minio.Client hitting a local fake S3 endpoint (cqp.client/cqp.listObjects/cqp.getObject
	// are unrelated to the registry -- see cubequerypath.go).
	cubeFileData := buildRealPartialCubeFile(t, minMinute)
	fakeClient := newFakeCubeS3Client(t, cubeFileData)

	// #508: files/lister built directly from fakeClient, mirroring ConfigureCubeQueryPath's own
	// adapter construction -- vi stays nil since this test only exercises the FOUND (partial
	// coverage) path, never the creation trigger.
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

	// A real VI-backed block (mirrors cubequerypath_warming_test.go): VI IS configured
	// (vr != nil), so ExecuteMetricsTraceQL takes the opts.ValueIndex != nil branch and
	// genuinely attempts ExecuteTraceMetricsFromVI, rather than short-circuiting at the
	// zeroth/config-level ErrMetricsValueIndexDisabled category.
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

	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.NoError(t, err, "cube's partial answer must be used as the fallback-of-last-resort, never a hard error, once VI/scan also declines")
	require.NotNil(t, resp)
	require.Equal(t, tempopb.PartialStatus_PARTIAL, resp.Status, "the response must still be marked PARTIAL -- this is cube's own truncated answer, not a complete one")
	require.NotEmpty(t, resp.Series, "cube's real rolled-up data for the covered sub-range must be present in the fallback response")

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.QueryRange")
	require.True(t, ok, "QueryRange must emit its own span")

	deferredAttr, hasDeferred := attrValue(span, "cube.partial_deferred_to_vi_scan")
	require.True(t, hasDeferred, "cube's PARTIAL answer must be deferred (not returned immediately) -- this attribute only exists on the FIXED code path")
	require.Equal(t, "true", deferredAttr)

	declineReasonAttr, hasReason := attrValue(span, "metrics.decline_reason")
	require.True(t, hasReason, "VI/scan must actually be attempted once cube's answer is deferred -- observable via metrics.decline_reason, which the pre-fix code never sets in this scenario because ExecuteMetricsTraceQL is never called")
	require.Equal(t, "shape_not_answerable", declineReasonAttr, "VI is structurally group-by-incapable (vm.MetricsShapeIsVIAnswerable) -- this is the ONLY reachable decline reason for a cube-shaped query today")

	usedAttr, hasUsed := attrValue(span, "cube.used")
	require.True(t, hasUsed)
	require.Equal(t, "true", usedAttr)

	fallbackAttr, hasFallback := attrValue(span, "cube.partial_fallback")
	require.True(t, hasFallback, "the response returned must be explicitly marked as cube's fallback-of-last-resort answer")
	require.Equal(t, "true", fallbackAttr)
}
