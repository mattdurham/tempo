package frontend

// vcnt_fetch_test.go — TDD coverage for issue #487's T3/#153 frontend-local VCNT fetch and
// QueryPlan call site. Uses the real local backend (tempodb/backend/local) rather than a
// hand-rolled fake, so fetchVCNTSection is exercised against the exact backend.RawReader
// contract (Find reports leaf files, List does not — see fetchVCNTSection's own doc comment).

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"testing"

	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

const testIndexPrefix = "indexes"

// vcntObj encodes a self-describing .vcnt object for one column, mirroring the real
// block-builder's write path (blockpack.SortVCNTRecords + EncodeVCNTFile).
func vcntObj(t *testing.T, column string, timeStart uint64, values map[string]int64) []byte {
	t.Helper()
	var recs []blockpack.VCNTRecord
	for v, c := range values {
		recs = append(recs, blockpack.VCNTRecord{
			ColumnName: column,
			Value:      []byte(v),
			TimeStart:  timeStart,
			TimeEnd:    timeStart,
			Count:      c,
		})
	}
	blockpack.SortVCNTRecords(recs)
	return blockpack.EncodeVCNTFile(recs, 0)
}

// writeVCNTObject writes data at the exact key layout VCNTObjectKey documents:
// tenant-a/value_counts/<colHash>/<file>.vcnt.
func writeVCNTObject(t *testing.T, rawW backend.RawWriter, column string, data []byte) {
	t.Helper()
	colHash := blockpack.VCNTColHash(column)
	name := blockpack.VCNTFormatFilename(0, blockpack.VCNTNewID())
	keypath := backend.KeyPath{"tenant-a", "value_counts", colHash}
	require.NoError(t, rawW.Write(t.Context(), name, keypath, bytes.NewReader(data), int64(len(data)), nil))
}

// writeVCNTObjectV2 writes data using the v2 filename format with an explicit wall-clock
// range, for pruning tests (writeVCNTObject above stays v1-shaped on purpose, exercising the
// "unknown range, always fetch" fallback path).
func writeVCNTObjectV2(t *testing.T, rawW backend.RawWriter, column string, data []byte, wallMinSec, wallMaxSec uint64) {
	t.Helper()
	colHash := blockpack.VCNTColHash(column)
	name := blockpack.VCNTFormatFilenameV2(0, wallMinSec, wallMaxSec, blockpack.VCNTNewID())
	keypath := backend.KeyPath{"tenant-a", "value_counts", colHash}
	require.NoError(t, rawW.Write(t.Context(), name, keypath, bytes.NewReader(data), int64(len(data)), nil))
}

func newLocalRawReadWriter(t *testing.T) (backend.RawReader, backend.RawWriter) {
	t.Helper()
	_, rawW, _, err := local.New(&local.Config{Path: path.Join(t.TempDir(), "traces")})
	require.NoError(t, err)
	rawR, ok := rawW.(backend.RawReader)
	require.True(t, ok, "local backend's writer must also satisfy RawReader for this test's read-back")
	return rawR, rawW
}

func TestFetchVCNTSection_NilRawReaderReturnsNil(t *testing.T) {
	// A real, deliberately chosen window (not a leftover placeholder) — irrelevant here since
	// a nil rawR short-circuits before the prune check is ever reached.
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), nil, "tenant-a", []string{"resource.service.name"}, 0, 200)
	require.Nil(t, data)
	require.Nil(t, dir)
	require.Zero(t, filesCount)
	require.Zero(t, bytesRead)
}

func TestFetchVCNTSection_NoDimsReturnsNil(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	// A real, deliberately chosen window (not a leftover placeholder) — irrelevant here since
	// no dims means the per-file loop never runs.
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), rawR, "tenant-a", nil, 0, 200)
	require.Nil(t, data)
	require.Nil(t, dir)
	require.Zero(t, filesCount)
	require.Zero(t, bytesRead)
}

// TEST-487-fetch-1: fetchVCNTSection finds and merges the .vcnt objects written under a
// dim's key prefix into one consolidated section whose selectivity matches what was written.
func TestFetchVCNTSection_MergesObjectsForRequestedDims(t *testing.T) {
	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"

	writeVCNTObject(t, rawW, "resource.service.name",
		vcntObj(t, "resource.service.name", 60, map[string]int64{"api": 5, "web": 3}))
	writeVCNTObject(t, rawW, "resource.service.name",
		vcntObj(t, "resource.service.name", 120, map[string]int64{"api": 2}))
	// A second dim's object must not be pulled in when it wasn't requested.
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 9}))

	// This test's fixtures are v1-shaped (writeVCNTObject), so the window below is inert by
	// design — the new TestFetchVCNTSection_* pruning tests below are what actually exercises it.
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), rawR, tenant, []string{"resource.service.name"}, 0, 200)
	require.NotNil(t, data)
	require.NotEmpty(t, dir)
	// filesCount/bytesRead (issue #493 Task 4c): exactly the 2 resource.service.name objects
	// written above must be counted -- the unrequested span.http.method object must not.
	require.Equal(t, 2, filesCount)
	require.Positive(t, bytesRead)

	est, err := blockpack.VCNTSelectivityInRange(data, dir, "resource.service.name", []byte("api"), 0, 200)
	require.NoError(t, err)
	require.True(t, est.Covered)
	require.Equal(t, int64(7), est.Count) // 5 (t=60) + 2 (t=120)

	// The unrequested dim must not have leaked into the merged section.
	unrequested, err := blockpack.VCNTSelectivityInRange(data, dir, "span.http.method", []byte("GET"), 0, 200)
	require.NoError(t, err)
	require.False(t, unrequested.Covered)
}

func TestFetchVCNTSection_MissingDimYieldsNoCoverageNotError(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	// A real, deliberately chosen window (not a leftover placeholder) — irrelevant here since
	// there is no coverage for this dim at all.
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), rawR, "tenant-a", []string{"resource.service.name"}, 0, 200)
	require.Nil(t, data)
	require.Nil(t, dir)
	require.Zero(t, filesCount)
	require.Zero(t, bytesRead)
}

func TestBuildQueryPlan_NilRawReaderReturnsNilPlan(t *testing.T) {
	plan, err := buildQueryPlan(context.Background(), nil, "tenant-a", `{ span.http.method = "GET" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_CompileFailureReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, err := buildQueryPlan(context.Background(), rawR, "tenant-a", `{ not a valid traceql`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_EmptyQueryReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, err := buildQueryPlan(context.Background(), rawR, "tenant-a", "", 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

// TestBuildQueryPlan_UnresolvableQueryReturnsNilPlanWithoutFetching pins the holistic-review
// Issue 4 fix: allLeavesResolvable (vblockpack.CheckIndexCoverage) is checked BEFORE
// fetchVCNTSection's S3 Find+Read fan-out, since a query that fails it always resolves to
// DispatchBlockSharded regardless of VCNT signal — paying for the fetch first would be wasted
// I/O for every such query. No value-index query reader is configured in this test process, so
// CheckIndexCoverage declines (mirroring tryIndexFetch's own "index disabled" decline) —
// buildQueryPlan must return nil (not a non-nil DispatchBlockSharded plan) via the short-circuit,
// and rawR — wrapped in countingRawReader — must see ZERO Find/Read calls.
func TestBuildQueryPlan_UnresolvableQueryReturnsNilPlanWithoutFetching(t *testing.T) {
	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 9}))

	counting := &countingRawReader{RawReader: rawR}
	plan, err := buildQueryPlan(context.Background(), counting, tenant, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan, "an unresolvable query (no value-index reader configured) must short-circuit to a nil plan")
	require.Equal(t, 0, counting.findCalls, "CheckIndexCoverage must be checked before any VCNT fetch I/O — zero Find calls expected")
	require.Equal(t, 0, counting.readCalls, "CheckIndexCoverage must be checked before any VCNT fetch I/O — zero Read calls expected")
}

// TEST-495-fetch-1: a v2-shaped .vcnt file whose embedded range provably does not overlap the
// query window must never be Read (issue #495) — asserted via countingRawReader's actual Read
// call count, not merely via the output section (R7a mechanism assertion). Find still runs — it
// must, to discover the file exists at all — only Read must be zero.
func TestFetchVCNTSection_OutOfRangeV2FileNeverFetched(t *testing.T) {
	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	writeVCNTObjectV2(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 500, map[string]int64{"GET": 1}), 500, 600)

	counting := &countingRawReader{RawReader: rawR}
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), counting, tenant, []string{"span.http.method"}, 0, 100)
	require.Nil(t, data)
	require.Nil(t, dir)
	require.Zero(t, filesCount)
	require.Zero(t, bytesRead)
	require.Equal(t, 0, counting.readCalls, "out-of-range v2 file must never be Read")
}

// TEST-495-fetch-2: a v2-shaped .vcnt file whose embedded range overlaps the query window must
// still be fetched (sibling positive case).
func TestFetchVCNTSection_InRangeV2FileStillFetched(t *testing.T) {
	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	writeVCNTObjectV2(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 500, map[string]int64{"GET": 1}), 500, 600)

	counting := &countingRawReader{RawReader: rawR}
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), counting, tenant, []string{"span.http.method"}, 100, 700)
	require.NotNil(t, data)
	require.NotEmpty(t, dir)
	require.Equal(t, 1, filesCount)
	require.Positive(t, bytesRead)
	require.Equal(t, 1, counting.readCalls, "in-range v2 file must be Read")
}

// TEST-495-fetch-3: a v1-shaped .vcnt file (unknown range) must always be fetched regardless of
// the query window (issue #495 R7b).
func TestFetchVCNTSection_V1ShapedFileStillFetchedRegardlessOfWindow(t *testing.T) {
	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 1}))

	counting := &countingRawReader{RawReader: rawR}
	data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), counting, tenant, []string{"span.http.method"}, 900, 1000)
	require.NotNil(t, data)
	require.NotEmpty(t, dir)
	require.Equal(t, 1, filesCount)
	require.Positive(t, bytesRead)
	require.Equal(t, 1, counting.readCalls, "v1-shaped file must be fetched despite unrelated window")
}

// TEST-495-fetch-4: query windows that merely touch a v2 file's boundary must still be treated
// as overlapping and fetched (issue #495 R7c) — pins the inclusive-both-ends formula at the
// integration level.
func TestFetchVCNTSection_BoundaryTouchingWindowsIncluded(t *testing.T) {
	tenant := "tenant-a"
	for _, window := range []struct{ minTS, maxTS uint64 }{
		{200, 300},
		{0, 100},
	} {
		rawR, rawW := newLocalRawReadWriter(t)
		writeVCNTObjectV2(t, rawW, "span.http.method",
			vcntObj(t, "span.http.method", 100, map[string]int64{"GET": 1}), 100, 200)

		counting := &countingRawReader{RawReader: rawR}
		data, dir, filesCount, bytesRead := fetchVCNTSection(context.Background(), counting, tenant, []string{"span.http.method"}, window.minTS, window.maxTS)
		require.NotNil(t, data, "window [%d,%d]: expected boundary-touching file to be fetched", window.minTS, window.maxTS)
		require.NotEmpty(t, dir)
		require.Equal(t, 1, filesCount)
		require.Positive(t, bytesRead)
		require.Equal(t, 1, counting.readCalls, "window [%d,%d]: expected boundary-touching file to be Read", window.minTS, window.maxTS)
	}
}

// countingRawReader wraps a real backend.RawReader and counts Find/Read calls, so a test can
// assert fetchVCNTSection's S3 I/O never ran (holistic-review Issue 4: allLeavesResolvable must
// gate the fetch, not the other way around).
type countingRawReader struct {
	backend.RawReader
	findCalls int
	readCalls int
}

func (c *countingRawReader) Find(ctx context.Context, keypath backend.KeyPath, f backend.FindFunc) error {
	c.findCalls++
	return c.RawReader.Find(ctx, keypath, f)
}

func (c *countingRawReader) Read(ctx context.Context, name string, keyPath backend.KeyPath, cacheInfo *backend.CacheInfo) (io.ReadCloser, int64, error) {
	c.readCalls++
	return c.RawReader.Read(ctx, name, keyPath, cacheInfo)
}

// F-6 (issue #481 parts 2/3): real-conversion-path tests for buildQueryPlanFromProgram's new
// ClassifyProgramVCNT/SelectSearchStrategy wiring. Each test configures a real (test-scoped)
// value-index reader via vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, ...) so
// CheckIndexCoverage reports resolvable (emptyVIStore's own doc comment, metrics_query_range_
// sharder_test.go, explains why an empty store still satisfies the "shape is buildable" half of
// resolvability), writes real VCNT objects via the local backend to drive selectivity
// classification, and compiles a real query via blockpack.CompileTraceQL/
// CompileTraceQLMetricsFilter — never a hand-built Selectivity/*blockpack.Program value.

// TestBuildQueryPlanFromProgram_LowSelectivityWithLimit_SelectsBoundedRecentFirst is a MUST per
// plan-f.md Task 6: a search query whose predicate value covers most of the column's live spans
// (LowSelectivity) with a limit present must select DispatchBoundedRecentFirst (R3).
func TestBuildQueryPlanFromProgram_LowSelectivityWithLimit_SelectsBoundedRecentFirst(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	// "GET" accounts for 900/1000 of the column's live spans over [0,200) — LowSelectivity.
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

	plan, err := buildQueryPlan(context.Background(), rawR, tenant,
		`{ span.http.method = "GET" }`, 0, 200, 1000, true /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, blockpack.DispatchBoundedRecentFirst, plan.Strategy)
}

// TestBuildMetricsQueryPlanFromProgram_LowSelectivityWithoutLimit_NeverBoundedRecentFirst is a
// MUST per R2/plan-f.md Task 6 — the wrong-answer guard: metrics is NEVER bounded-served (a
// truncated aggregate is a wrong answer, not a partial one). A resolvable, LowSelectivity metrics
// query must plan-time-decline (ErrPlanTimeLowSelectivityNoLimit), never select
// DispatchBoundedRecentFirst regardless of anything resembling a "limit" on the metrics side.
func TestBuildMetricsQueryPlanFromProgram_LowSelectivityWithoutLimit_NeverBoundedRecentFirst(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

	plan, err := buildMetricsQueryPlan(context.Background(), rawR, tenant,
		`{ span.http.method = "GET" } | rate()`, 0, 200, 1000)
	require.Error(t, err, "a resolvable, LowSelectivity metrics query must plan-time-decline, not dispatch")
	require.True(t, errors.Is(err, ErrPlanTimeLowSelectivityNoLimit))
	require.Nil(t, plan, "no plan must be returned alongside a plan-time decline error")
}

// TestBuildQueryPlanFromProgram_UnknownSelectivity_WithLimit_SelectsBoundedRecentFirst covers
// R3's UnknownSelectivity+limit row: no VCNT signal at all for the queried column (not even an
// empty-but-present record) classifies as UnknownSelectivity, which — per R3 — is treated as
// bounded-eligible when a limit exists (worst case: a few extra low-yield blocks, kept safe by
// the hard cap).
func TestBuildQueryPlanFromProgram_UnknownSelectivity_WithLimit_SelectsBoundedRecentFirst(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	// No VCNT object written at all for this column — ClassifyProgramVCNT must read UnknownSelectivity.

	plan, err := buildQueryPlan(context.Background(), rawR, tenant,
		`{ span.http.method = "GET" }`, 0, 200, 1000, true /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, blockpack.DispatchBoundedRecentFirst, plan.Strategy)
}

// TestBuildQueryPlanFromProgram_UnknownSelectivity_WithoutLimit_StaysIndexOnly covers R3's
// UnknownSelectivity+no-limit row: with no selectivity signal AND no limit, there is no safe way
// to guess whether an unbounded-equivalent read would be cheap or catastrophic, so the query
// falls through to the existing index-only path (DispatchBlockSharded/DispatchTimeSliced,
// hard-erroring on decline downstream) rather than being bounded-served.
func TestBuildQueryPlanFromProgram_UnknownSelectivity_WithoutLimit_StaysIndexOnly(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	tenant := "tenant-a"

	plan, err := buildQueryPlan(context.Background(), rawR, tenant,
		`{ span.http.method = "GET" }`, 0, 200, 1000, false /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.NotEqual(t, blockpack.DispatchBoundedRecentFirst, plan.Strategy,
		"UnknownSelectivity with no limit must stay on the index-only path, never bounded")
}

// recordedSpansFrontend installs an in-process span recorder as the global tracer provider and
// refreshes this package's own tracer var so recorded spans flow into it -- mirrors
// vblockpack's recordedSpans (tempodb/encoding/vblockpack/observability_test.go) exactly, per
// R7's "EXACT setupTestTracer pattern" requirement (this package's own tracer var, not a
// hand-rolled provider it never reads from).
func recordedSpansFrontend(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("modules/frontend")
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return rec
}

// frontendSpanByName returns the first recorded span with the given operation name.
func frontendSpanByName(spans []sdktrace.ReadOnlySpan, name string) (sdktrace.ReadOnlySpan, bool) {
	for _, s := range spans {
		if s.Name() == name {
			return s, true
		}
	}
	return nil, false
}

// frontendAttrs converts a recorded span's attributes into a lookup map for assertions.
func frontendAttrs(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(s.Attributes()))
	for _, kv := range s.Attributes() {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

// TestBuildQueryPlanFromProgram_AttachesQualificationOutcome (issue #493 Task 4a) drives the
// REAL buildQueryPlanFromProgram entry point (via buildQueryPlan/buildMetricsQueryPlan, R7) for
// each of its 4 qualification outcomes, asserting plan.qualification_outcome on whatever span was
// already active on ctx via trace.ContextWithSpan -- exactly the "attach to the EXISTING span"
// contract sub-task 4a specifies (search_sharder.go/metrics_query_range_sharder.go already do
// this same thing with their own real frontend.ShardSearch/frontend.QueryRangeSharder.* spans;
// this test uses a plain span standing in for either, since the attribute's presence and value
// don't depend on which caller's span it is).
func TestBuildQueryPlanFromProgram_AttachesQualificationOutcome(t *testing.T) {
	rec := recordedSpansFrontend(t)

	runWithSpan := func(t *testing.T, fn func(ctx context.Context)) sdktrace.ReadOnlySpan {
		t.Helper()
		// Reset before each subtest -- frontendSpanByName returns the FIRST span matching a
		// name, and every subtest below reuses "test.caller", so a stale recording from an
		// earlier subtest would otherwise be picked up instead of this run's own span.
		rec.Reset()
		ctx, span := tracer.Start(context.Background(), "test.caller")
		fn(ctx)
		span.End()
		s, ok := frontendSpanByName(rec.Ended(), "test.caller")
		require.True(t, ok)
		return s
	}

	t.Run("not_indexable", func(t *testing.T) {
		// No value-index reader configured for this subtest -- CheckIndexCoverage declines.
		rawR, rawW := newLocalRawReadWriter(t)
		writeVCNTObject(t, rawW, "span.http.method", vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 9}))

		span := runWithSpan(t, func(ctx context.Context) {
			plan, err := buildQueryPlan(ctx, rawR, "tenant-a", `{ span.http.method = "GET" }`, 0, 200, 1000, false)
			require.NoError(t, err)
			require.Nil(t, plan)
		})
		got, ok := frontendAttrs(span)["plan.qualification_outcome"]
		require.True(t, ok)
		require.Equal(t, "not_indexable", got.AsString())
	})

	t.Run("low_selectivity_no_limit_search", func(t *testing.T) {
		restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
		defer restore()
		rawR, rawW := newLocalRawReadWriter(t)
		writeVCNTObject(t, rawW, "span.http.method", vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

		span := runWithSpan(t, func(ctx context.Context) {
			plan, err := buildQueryPlan(ctx, rawR, "tenant-a", `{ span.http.method = "GET" }`, 0, 200, 1000, false)
			require.Error(t, err)
			require.Nil(t, plan)
		})
		got, ok := frontendAttrs(span)["plan.qualification_outcome"]
		require.True(t, ok)
		require.Equal(t, "low_selectivity_no_limit_search", got.AsString())
	})

	t.Run("low_selectivity_metrics_no_partial_aggregate", func(t *testing.T) {
		restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
		defer restore()
		rawR, rawW := newLocalRawReadWriter(t)
		writeVCNTObject(t, rawW, "span.http.method", vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

		span := runWithSpan(t, func(ctx context.Context) {
			plan, err := buildMetricsQueryPlan(ctx, rawR, "tenant-a", `{ span.http.method = "GET" } | rate()`, 0, 200, 1000)
			require.Error(t, err)
			require.Nil(t, plan)
		})
		got, ok := frontendAttrs(span)["plan.qualification_outcome"]
		require.True(t, ok)
		require.Equal(t, "low_selectivity_metrics_no_partial_aggregate", got.AsString())
	})

	t.Run("qualified", func(t *testing.T) {
		restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
		defer restore()
		rawR, rawW := newLocalRawReadWriter(t)
		// "POST" is the minority value (100/1000) -- Selective, always falls through to a real
		// qualified plan regardless of hasLimit.
		writeVCNTObject(t, rawW, "span.http.method", vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

		var capturedPlan *blockpack.QueryPlan
		span := runWithSpan(t, func(ctx context.Context) {
			plan, err := buildQueryPlan(ctx, rawR, "tenant-a", `{ span.http.method = "POST" }`, 0, 200, 1000, false)
			require.NoError(t, err)
			require.NotNil(t, plan)
			capturedPlan = plan
		})
		attrs := frontendAttrs(span)
		got, ok := attrs["plan.qualification_outcome"]
		require.True(t, ok)
		require.Equal(t, "qualified", got.AsString())
		// This fixture's Selective, no-limit query resolves to DispatchTimeSliced -- pin the
		// actual string value (not just presence) against dispatchStrategyString's real output
		// for the plan this exact call returned, so a future change to the strategy-to-string
		// mapping (or to what strategy this fixture qualifies for) is caught.
		strategyAttr, ok := attrs["plan.strategy"]
		require.True(t, ok, "plan.strategy must be attached on the qualified/success path")
		require.Equal(t, blockpack.DispatchTimeSliced, capturedPlan.Strategy,
			"test fixture assumption: a Selective, no-limit query qualifies for DispatchTimeSliced")
		require.Equal(t, dispatchStrategyString(capturedPlan.Strategy), strategyAttr.AsString())
		require.Equal(t, "time_sliced", strategyAttr.AsString())
		_, ok = attrs["plan.slice_count"]
		require.True(t, ok, "plan.slice_count must be attached on the qualified/success path")
		_, ok = attrs["plan.slices_est_known_fraction"]
		require.True(t, ok, "plan.slices_est_known_fraction must be attached on the qualified/success path")
	})
}

// TestBuildQueryPlanFromProgram_AttachesLeadDetail (issue #493 Task 4b) proves the lead leaf's
// both-sides cost detail is attached to the span exactly when known, driving the REAL
// buildQueryPlan entry point (R7) against real VCNT bytes -- never a hand-built LeadDetail.
func TestBuildQueryPlanFromProgram_AttachesLeadDetail(t *testing.T) {
	rec := recordedSpansFrontend(t)
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	// "GET" is 900 of the column's 1000 live spans over [0,200).
	writeVCNTObject(t, rawW, "span.http.method", vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

	ctx, span := tracer.Start(context.Background(), "test.caller")
	plan, err := buildQueryPlan(ctx, rawR, "tenant-a", `{ span.http.method = "GET" }`, 0, 200, 1000, true)
	require.NoError(t, err)
	require.NotNil(t, plan)
	span.End()

	s, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)
	attrs := frontendAttrs(s)

	col, ok := attrs["plan.lead_column"]
	require.True(t, ok)
	require.Equal(t, "span.http.method", col.AsString())
	cost, ok := attrs["plan.lead_index_cost"]
	require.True(t, ok)
	require.Equal(t, int64(900), cost.AsInt64())
	total, ok := attrs["plan.lead_column_total"]
	require.True(t, ok)
	require.Equal(t, int64(1000), total.AsInt64())
}

// TestFetchVCNTFetch_EmitsChildSpanWithFileStats (issue #493 Task 4c) drives the REAL
// buildQueryPlanFromProgram entry point (via buildQueryPlan, R7) and asserts the
// "frontend.vcntFetch" child span carries files.count/bytes.read matching the real objects
// written -- the only genuinely new span in this whole plan.
func TestFetchVCNTFetch_EmitsChildSpanWithFileStats(t *testing.T) {
	rec := recordedSpansFrontend(t)
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	// "POST" is the minority value (Selective) across both objects combined -- avoids the
	// LowSelectivity plan-time decline path so this test can assert on a real, qualified plan.
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 5, "POST": 3}))
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 120, map[string]int64{"GET": 2}))

	plan, err := buildQueryPlan(context.Background(), rawR, "tenant-a", `{ span.http.method = "POST" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.NotNil(t, plan)

	s, ok := frontendSpanByName(rec.Ended(), "frontend.vcntFetch")
	require.True(t, ok, "expected a frontend.vcntFetch child span")
	attrs := frontendAttrs(s)
	filesCount, ok := attrs["files.count"]
	require.True(t, ok)
	require.EqualValues(t, 2, filesCount.AsInt64())
	bytesRead, ok := attrs["bytes.read"]
	require.True(t, ok)
	require.Positive(t, bytesRead.AsInt64())
}
