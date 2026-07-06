package vblockpack

import (
	"errors"
	"io"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

// withVIQueryReader installs store as the process-level index-driven query reader for the
// duration of the test and restores the prior state on cleanup, mirroring withVISink
// (valueindex_test.go) on the write side. FindTraceByID now requires the index
// unconditionally (NOTE-VI-073) — any test exercising it against a non-empty block must
// wire this up (typically together with withVISink using the same *fakeVISink instance, so
// CreateBlock's real WriteValueIndexL0 write path populates the same store this reads from).
func withVIQueryReader(t *testing.T, store valueIndexStore, indexPrefix string) {
	t.Helper()
	viQueryReaderMu.Lock()
	prev := viQueryReaderPtr
	if store == nil {
		viQueryReaderPtr = nil
	} else {
		viQueryReaderPtr = &viQueryReader{
			store:       store,
			caches:      make(map[string]*blockpack.IndexFileCache),
			indexPrefix: indexPrefix,
			ttl:         time.Minute,
		}
	}
	viQueryReaderMu.Unlock()
	t.Cleanup(func() {
		viQueryReaderMu.Lock()
		viQueryReaderPtr = prev
		viQueryReaderMu.Unlock()
	})
}

// minioVIStore must satisfy all three blockpack read-side interfaces: Lister and
// ValueIndexFileStore drive the search/metrics index path; LookupStore (List + Get)
// drives the trace-by-ID index path (issue #468). A drift in any of these root
// interfaces breaks the build here, at the store's single definition, rather than
// silently at a distant call site.
var (
	_ blockpack.Lister              = (*minioVIStore)(nil)
	_ blockpack.ValueIndexFileStore = (*minioVIStore)(nil)
	_ blockpack.LookupStore         = (*minioVIStore)(nil)
	_ blockpack.TraceIndexGetter    = (*minioVIStore)(nil)
)

// mapNotFound translates a minio 404 / NoSuchKey into
// blockpack.ErrValueIndexFileNotFound so the index builder skips a file the
// compactor deleted out from under a stale listing (blockpack issue #399 point 5),
// while passing every other error through unchanged.
func TestMapNotFound(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, mapNotFound(nil))
	})

	t.Run("NoSuchKey maps to sentinel", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}
		got := mapNotFound(err)
		assert.True(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound),
			"a NoSuchKey/404 must map to the not-found sentinel")
	})

	t.Run("404 status alone maps to sentinel", func(t *testing.T) {
		err := minio.ErrorResponse{StatusCode: 404}
		got := mapNotFound(err)
		assert.True(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})

	t.Run("transient error passes through", func(t *testing.T) {
		orig := errors.New("connection reset by peer")
		got := mapNotFound(orig)
		assert.Equal(t, orig, got)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound),
			"a non-404 error must NOT be treated as not-found — it must still abort")
	})

	t.Run("io.EOF passes through unchanged", func(t *testing.T) {
		// readWhole tolerates io.EOF from a short read; mapNotFound must not
		// reclassify it as not-found.
		got := mapNotFound(io.EOF)
		assert.Equal(t, io.EOF, got)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})

	t.Run("server error 500 passes through", func(t *testing.T) {
		err := minio.ErrorResponse{Code: "InternalError", StatusCode: 500}
		got := mapNotFound(err)
		assert.False(t, errors.Is(got, blockpack.ErrValueIndexFileNotFound))
	})
}

// TestNanoWindowToSec_MinuteFloorAlignsWithWriteSide is the regression test for
// the cross-repo correctness coupling documented in blockpack's
// valueindex_extract.go:buildSpanStartSecByRef (NOTE-VI-05x): VI's TimeSec
// entries are floored to the minute ((startNano/1e9)/60*60, blockpack
// valueindex_extract.go:179) at write time. If the query-side minSec bound
// here is NOT floored to the same alignment, a genuinely in-range span whose
// minute-floored TimeSec falls just below a non-minute-aligned query start is
// silently dropped by valueindex.BucketFile.LookupValue's exact-inclusion
// filter (bucketquery.go:89), with no fallback (the index path reports
// "coverage found" and answers the query incompletely). This test proves the
// false-negative scenario is closed.
func TestNanoWindowToSec_MinuteFloorAlignsWithWriteSide(t *testing.T) {
	// Query window starts 45s past a minute boundary: epoch nanosecond
	// ...045_000_000_000. A span at ...050s (5s later, genuinely inside the
	// query window) minute-floors to TimeSec = ...000 (the minute boundary)
	// at write time, per buildSpanStartSecByRef's (v/1e9)/60*60 formula.
	const minuteBoundary = 120 * 1_000_000_000                  // an arbitrary whole minute, in ns
	queryStartNano := uint64(minuteBoundary + 45*1_000_000_000) // ...045
	queryEndNano := uint64(minuteBoundary + 90*1_000_000_000)   // ...090, unbounded-max not exercised here

	minSec, _ := nanoWindowToSec(queryStartNano, queryEndNano)

	// The write-side formula, duplicated here deliberately (not imported) so
	// this test fails loudly if either side's formula changes without the
	// other — see blockpack valueindex_extract.go:179.
	spanStartNano := uint64(minuteBoundary + 50*1_000_000_000) // ...050, inside the query window
	spanTimeSec := (spanStartNano / 1_000_000_000) / 60 * 60   // write-side minute floor

	assert.LessOrEqual(t, minSec, spanTimeSec,
		"query-side minSec must be floored to the same minute alignment as write-side "+
			"TimeSec, or a genuinely in-range span (spanTimeSec=%d) is dropped by "+
			"LookupValue's exact minSec<=TimeSec<=maxSec filter when minSec=%d > spanTimeSec",
		spanTimeSec, minSec)
}
