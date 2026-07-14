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
	"sync"
	"testing"
	"time"

	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
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
	plan, _, err := buildQueryPlan(context.Background(), nil, "tenant-a", nil, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_CompileFailureReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", nil, `{ not a valid traceql`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_EmptyQueryReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", nil, "", 0, 200, 1000, false)
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
	plan, _, err := buildQueryPlan(context.Background(), counting, tenant, nil, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
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

// TestBuildQueryPlanFromProgram_LowSelectivityWithLimit_FallsThroughToResolvabilityPath (Phase 7,
// plan-scan-fallback.md) replaces the retired DispatchBoundedRecentFirst assertion: once
// SelectSearchStrategy no longer intercepts LowSelectivity+hasLimit into a dedicated bounded
// strategy, the query instead falls through to the existing cost/perMinuteForLead/BuildQueryPlan
// flow — the SAME resolvability-only gate every other qualified query goes through, resolving to
// DispatchTimeSliced here (a resolvable, VCNT-covered leaf). The querier's own per-block
// bounded-index path (tempo's backend_block.go) now handles the LIMIT dimension directly, without
// any plan-time signal.
func TestBuildQueryPlanFromProgram_LowSelectivityWithLimit_FallsThroughToResolvabilityPath(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, rawW := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	// "GET" accounts for 900/1000 of the column's live spans over [0,200) — LowSelectivity.
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

	plan, _, err := buildQueryPlan(context.Background(), rawR, tenant, nil,
		`{ span.http.method = "GET" }`, 0, 200, 1000, true /* hasLimit */)
	require.NoError(t, err, "LowSelectivity+hasLimit no longer plan-time-declines -- a limit alone never forces a decline")
	require.NotNil(t, plan)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
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

	plan, _, err := buildMetricsQueryPlan(context.Background(), rawR, tenant, nil,
		`{ span.http.method = "GET" } | rate()`, 0, 200, 1000)
	require.Error(t, err, "a resolvable, LowSelectivity metrics query must plan-time-decline, not dispatch")
	require.True(t, errors.Is(err, ErrPlanTimeLowSelectivityNoLimit))
	require.Nil(t, plan, "no plan must be returned alongside a plan-time decline error")
}

// TestBuildQueryPlanFromProgram_UnknownSelectivity_WithLimit_FallsThroughToResolvabilityPath
// (Phase 7, plan-scan-fallback.md) covers UnknownSelectivity+limit: no VCNT signal at all for the
// queried column (not even an empty-but-present record) classifies as UnknownSelectivity, which
// now falls through to the same cost/perMinuteForLead/BuildQueryPlan flow as every other
// non-decline outcome — resolving to DispatchTimeSliced via BuildQueryPlan's own documented
// VCNT-blind fallback (a resolvable-but-VCNT-blind plan still qualifies for DispatchTimeSliced).
func TestBuildQueryPlanFromProgram_UnknownSelectivity_WithLimit_FallsThroughToResolvabilityPath(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	tenant := "tenant-a"
	// No VCNT object written at all for this column — ClassifyProgramVCNT must read UnknownSelectivity.

	plan, _, err := buildQueryPlan(context.Background(), rawR, tenant, nil,
		`{ span.http.method = "GET" }`, 0, 200, 1000, true /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
}

// TestBuildQueryPlanFromProgram_UnknownSelectivity_HasLimitNoLongerAffectsOutcome (Phase 7,
// plan-scan-fallback.md, replaces the retired _StaysIndexOnly test) pins that hasLimit no longer
// differentiates UnknownSelectivity's outcome at all now that DispatchBoundedRecentFirst is gone
// -- WithLimit and WithoutLimit both reach the identical BuildQueryPlan fallthrough and produce
// the identical DispatchTimeSliced strategy, proving hasLimit's only remaining effect on this
// function is via LowSelectivity's planTimeDecline gate, never via strategy selection itself.
func TestBuildQueryPlanFromProgram_UnknownSelectivity_HasLimitNoLongerAffectsOutcome(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	tenant := "tenant-a"

	plan, _, err := buildQueryPlan(context.Background(), rawR, tenant, nil,
		`{ span.http.method = "GET" }`, 0, 200, 1000, false /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy,
		"hasLimit=false must produce the same strategy as hasLimit=true for UnknownSelectivity now")
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
			plan, _, err := buildQueryPlan(ctx, rawR, "tenant-a", nil, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
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
			plan, _, err := buildQueryPlan(ctx, rawR, "tenant-a", nil, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
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
			plan, _, err := buildMetricsQueryPlan(ctx, rawR, "tenant-a", nil, `{ span.http.method = "GET" } | rate()`, 0, 200, 1000)
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
			plan, _, err := buildQueryPlan(ctx, rawR, "tenant-a", nil, `{ span.http.method = "POST" }`, 0, 200, 1000, false)
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
	plan, _, err := buildQueryPlan(ctx, rawR, "tenant-a", nil, `{ span.http.method = "GET" }`, 0, 200, 1000, true)
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

	plan, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", nil, `{ span.http.method = "POST" }`, 0, 200, 1000, false)
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

// fakeFrontendUsageRecorder is this package's own local usageRecorder fake (the frontend
// package cannot reuse vblockpack's unexported fakeUsageRecorder, which lives in a different
// package) — installed via vblockpack.ConfigureViUsageRecorder, an unexported-interface-
// parameter/exported-function pattern that is structurally satisfied across package
// boundaries in Go, no test-only exported wrapper needed.
type fakeFrontendUsageRecorder struct {
	mu    sync.Mutex
	calls []string // colName per call
}

func (f *fakeFrontendUsageRecorder) RecordUse(
	_ context.Context, _, colName, _ string, _ time.Time,
) (blockpack.TriggerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, colName)
	return blockpack.TriggerResult{}, nil
}

func (f *fakeFrontendUsageRecorder) columns() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	cols := make([]string, len(f.calls))
	copy(cols, f.calls)
	return cols
}

// countingVIStoreFrontend wraps emptyVIStore, counting Size/ReadAt calls so a test can assert
// the discovery-only mechanism contract: the frontend plan-time path must never download a
// value-index file's content, only List it via IndexFileCache.FilesForTimeRange.
type countingVIStoreFrontend struct {
	emptyVIStore
	sizeCalls, readAtCalls int
}

func (c *countingVIStoreFrontend) Size(key string) (int64, error) {
	c.sizeCalls++
	return c.emptyVIStore.Size(key)
}

func (c *countingVIStoreFrontend) ReadAt(key string, p []byte, off int64) (int, error) {
	c.readAtCalls++
	return c.emptyVIStore.ReadAt(key, p, off)
}

// TestBuildQueryPlanFromProgram_RecordsUsageForUncoveredNonDedicatedColumn drives the REAL
// buildQueryPlan entry point with a genuinely uncovered, non-dedicated column, asserting the
// configured frontend-side recorder fires for it — proving RecordUsageIfNoIndexCoverage is
// actually reachable from buildQueryPlanFromProgram, not just correct in isolation.
func TestBuildQueryPlanFromProgram_RecordsUsageForUncoveredNonDedicatedColumn(t *testing.T) {
	rec := &fakeFrontendUsageRecorder{}
	vblockpack.ConfigureViUsageRecorder(rec)
	t.Cleanup(func() { vblockpack.ConfigureViUsageRecorder(nil) })

	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	_, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", nil, `{ span.never.indexed.rec1 = "x" }`, 0, 200, 1000, false)
	require.NoError(t, err)

	assert.Equal(t, []string{"span.never.indexed.rec1"}, rec.columns())
}

// TestBuildQueryPlanFromProgram_DedicatedColumnNeverRecorded: an otherwise-uncovered column
// already declared dedicated must never be recorded, even though it would otherwise qualify.
func TestBuildQueryPlanFromProgram_DedicatedColumnNeverRecorded(t *testing.T) {
	rec := &fakeFrontendUsageRecorder{}
	vblockpack.ConfigureViUsageRecorder(rec)
	t.Cleanup(func() { vblockpack.ConfigureViUsageRecorder(nil) })

	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	dedicated := backend.DedicatedColumns{{Scope: backend.DedicatedColumnScopeSpan, Name: "never.indexed.rec2"}}
	_, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", dedicated, `{ span.never.indexed.rec2 = "x" }`, 0, 200, 1000, false)
	require.NoError(t, err)

	assert.Empty(t, rec.columns())
}

// TestBuildQueryPlanFromProgram_UsageRecordingNeverDownloadsContent is the mechanism-not-outcome
// test proving the frontend-level call path is discovery-only, never downloading content.
//
// Deviates from plan.md's literal "covered case via mapListVIStore" framing: BuildValueIndexSource
// (called by CheckIndexCoverage, which runs immediately AFTER RecordUsageIfNoIndexCoverage in the
// same buildQueryPlanFromProgram call) genuinely downloads file content for any column with a
// real, discoverable file (verified by reading vibuilder.BuildSource/lookupColumn/
// queryKeysRanged) — so a "covered" scenario would show sizeCalls/readAtCalls > 0 from
// CheckIndexCoverage's own, unrelated download, not from RecordUsageIfNoIndexCoverage, making the
// assertion fail for reasons that have nothing to do with this task's correctness. An uncovered
// scenario isolates the intended claim just as well: queryKeysRanged short-circuits to zero
// downloads when FilesForTimeRange discovers no files at all, so this proves BOTH halves of the
// plan-time path (the new call and the existing CheckIndexCoverage gate) are discovery-only end
// to end for a column with nothing to fetch.
func TestBuildQueryPlanFromProgram_UsageRecordingNeverDownloadsContent(t *testing.T) {
	rec := &fakeFrontendUsageRecorder{}
	vblockpack.ConfigureViUsageRecorder(rec)
	t.Cleanup(func() { vblockpack.ConfigureViUsageRecorder(nil) })

	counting := &countingVIStoreFrontend{}
	restore := vblockpack.ConfigureValueIndexQueryForTest(counting, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	_, _, _ = buildQueryPlan(context.Background(), rawR, "tenant-a", nil, `{ span.never.indexed.rec3 = "x" }`, 0, 200, 1000, false)

	assert.Equal(t, 0, counting.sizeCalls, "must never download a value-index file's content")
	assert.Equal(t, 0, counting.readAtCalls, "must never download a value-index file's content")
}

// TestBuildQueryPlanFromProgram_RunsIndependentlyOfCheckIndexCoverageOutcome drives the exact
// scenario the "Exact insertion point" correction exists to fix: a query mixing an unindexable
// leaf on one column (forcing CheckIndexCoverage's AllLeavesIndexable gate to decline the WHOLE
// query) with an indexable, genuinely-uncovered leaf on a DIFFERENT column — the second column
// must still be recorded, proving RecordUsageIfNoIndexCoverage runs BEFORE, and independent of,
// the CheckIndexCoverage early return.
func TestBuildQueryPlanFromProgram_RunsIndependentlyOfCheckIndexCoverageOutcome(t *testing.T) {
	rec := &fakeFrontendUsageRecorder{}
	vblockpack.ConfigureViUsageRecorder(rec)
	t.Cleanup(func() { vblockpack.ConfigureViUsageRecorder(nil) })

	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	rawR, _ := newLocalRawReadWriter(t)
	plan, _, err := buildQueryPlan(context.Background(), rawR, "tenant-a", nil,
		`{ span.custom.attr != "x" && span.never.indexed.rec4 = "y" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan, "CheckIndexCoverage must decline this mixed unindexable/indexable query")

	assert.Contains(t, rec.columns(), "span.never.indexed.rec4",
		"RecordUsageIfNoIndexCoverage must run before CheckIndexCoverage's early return, independent of its outcome")
}

// TestBuildQueryPlan_HalfWindowBackfill_NowDispatchesTimeSlicedInsteadOfDeclining (#217, Phase
// 2.2) is the verification test for the grounding pass's finding #3: buildQueryPlanFromProgram
// already hardcodes allLeavesResolvable=true when calling blockpack.BuildQueryPlan, because
// CheckIndexCoverage gates the call above it — so simplifying CheckIndexCoverage (Phase 2.1) to
// stop consulting watermarks needs NO change at this call site. Before #217, a column whose
// backfill watermark covered only HALF the query window made CheckIndexCoverage decline the
// WHOLE query (via BuildValueIndexSource's whole-window gate), so buildQueryPlan returned
// (nil, nil). After #217, the SAME watermark state has zero effect on CheckIndexCoverage — this
// test proves buildQueryPlan now returns a non-nil DispatchTimeSliced plan for that exact
// scenario, letting each resulting per-minute slice decide its own coverage locally.
func TestBuildQueryPlan_HalfWindowBackfill_NowDispatchesTimeSlicedInsteadOfDeclining(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	tenant := "tenant-a"
	// The query window is [0,200]; the column's backfill watermark only confirms coverage from
	// 100 onward — a genuine "full backfill for half the window" partial-coverage shape, which
	// pre-#217 would decline the whole window via CheckIndexCoverage's now-removed
	// BuildValueIndexSource consultation.
	restoreWatermarks := vblockpack.ConfigureViWatermarkCacheForTest(tenant, map[string]blockpack.ColumnWatermark{
		"span.http.method": {Triggered: true, Done: false, WatermarkSec: 100},
	})
	defer restoreWatermarks()

	rawR, rawW := newLocalRawReadWriter(t)
	// "GET" accounts for 900/1000 of the column's live spans over [0,200) -- LowSelectivity,
	// so hasLimit=true is required to avoid the SEPARATE (unrelated to this test) plan-time
	// low-selectivity-without-limit decline, mirroring
	// TestBuildQueryPlanFromProgram_LowSelectivityWithLimit_FallsThroughToResolvabilityPath's
	// identical fixture.
	writeVCNTObject(t, rawW, "span.http.method",
		vcntObj(t, "span.http.method", 60, map[string]int64{"GET": 900, "POST": 100}))

	plan, _, err := buildQueryPlan(context.Background(), rawR, tenant, nil,
		`{ span.http.method = "GET" }`, 0, 200, 1000, true /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan, "a half-window backfill watermark must no longer decline the whole query")
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
}
