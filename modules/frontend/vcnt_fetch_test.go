package frontend

// vcnt_fetch_test.go — TDD coverage for issue #487's T3/#153 frontend-local VCNT fetch and
// QueryPlan call site. Uses the real local backend (tempodb/backend/local) rather than a
// hand-rolled fake, so fetchVCNTSection is exercised against the exact backend.RawReader
// contract (Find reports leaf files, List does not — see fetchVCNTSection's own doc comment).

import (
	"bytes"
	"context"
	"io"
	"path"
	"testing"

	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
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
	plan := buildQueryPlan(context.Background(), nil, "tenant-a", testIndexPrefix, `{ span.http.method = "GET" }`, 0, 200, 1000)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_CompileFailureReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan := buildQueryPlan(context.Background(), rawR, "tenant-a", testIndexPrefix, `{ not a valid traceql`, 0, 200, 1000)
	require.Nil(t, plan)
}

func TestBuildQueryPlan_EmptyQueryReturnsNilPlan(t *testing.T) {
	rawR, _ := newLocalRawReadWriter(t)
	plan := buildQueryPlan(context.Background(), rawR, "tenant-a", testIndexPrefix, "", 0, 200, 1000)
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
	plan := buildQueryPlan(context.Background(), counting, tenant, testIndexPrefix, `{ span.http.method = "GET" }`, 0, 200, 1000)
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
