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
// tenant-a/<indexPrefix>/unique_values/<colHash>/<file>.vcnt.
func writeVCNTObject(t *testing.T, rawW backend.RawWriter, column string, data []byte) {
	t.Helper()
	colHash := blockpack.VCNTColHash(column)
	name := blockpack.VCNTFormatFilename(0, blockpack.VCNTNewID())
	keypath := backend.KeyPath{"tenant-a", testIndexPrefix, "unique_values", colHash}
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
	data, dir := fetchVCNTSection(context.Background(), nil, "tenant-a", testIndexPrefix, []string{"resource.service.name"})
	require.Nil(t, data)
	require.Nil(t, dir)
}

func TestFetchVCNTSection_NoDimsReturnsNil(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	data, dir := fetchVCNTSection(context.Background(), rawR, "tenant-a", testIndexPrefix, nil)
	require.Nil(t, data)
	require.Nil(t, dir)
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

	data, dir := fetchVCNTSection(context.Background(), rawR, tenant, testIndexPrefix, []string{"resource.service.name"})
	require.NotNil(t, data)
	require.NotEmpty(t, dir)

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
	data, dir := fetchVCNTSection(context.Background(), rawR, "tenant-a", testIndexPrefix, []string{"resource.service.name"})
	require.Nil(t, data)
	require.Nil(t, dir)
}

func TestBuildQueryPlan_NilRawReaderReturnsNilPlan(t *testing.T) {
	plan, err := buildQueryPlan(context.Background(), nil, "tenant-a", testIndexPrefix, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_CompileFailureReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, err := buildQueryPlan(context.Background(), rawR, "tenant-a", testIndexPrefix, `{ not a valid traceql`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_EmptyQueryReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan, err := buildQueryPlan(context.Background(), rawR, "tenant-a", testIndexPrefix, "", 0, 200, 1000, false)
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
	plan, err := buildQueryPlan(context.Background(), counting, tenant, testIndexPrefix, `{ span.http.method = "GET" }`, 0, 200, 1000, false)
	require.NoError(t, err)
	require.Nil(t, plan, "an unresolvable query (no value-index reader configured) must short-circuit to a nil plan")
	require.Equal(t, 0, counting.findCalls, "CheckIndexCoverage must be checked before any VCNT fetch I/O — zero Find calls expected")
	require.Equal(t, 0, counting.readCalls, "CheckIndexCoverage must be checked before any VCNT fetch I/O — zero Read calls expected")
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

	plan, err := buildQueryPlan(context.Background(), rawR, tenant, testIndexPrefix,
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

	plan, err := buildMetricsQueryPlan(context.Background(), rawR, tenant, testIndexPrefix,
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

	plan, err := buildQueryPlan(context.Background(), rawR, tenant, testIndexPrefix,
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

	plan, err := buildQueryPlan(context.Background(), rawR, tenant, testIndexPrefix,
		`{ span.http.method = "GET" }`, 0, 200, 1000, false /* hasLimit */)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.NotEqual(t, blockpack.DispatchBoundedRecentFirst, plan.Strategy,
		"UnknownSelectivity with no limit must stay on the index-only path, never bounded")
}
