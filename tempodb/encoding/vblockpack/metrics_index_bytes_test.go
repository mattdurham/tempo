package vblockpack

// metrics_index_bytes_test.go — issue #218 Phase 2b. blockpack's TraceMetricsResult field was
// renamed BytesRead -> IndexBytesRead and is now genuinely populated by
// ExecuteTraceMetricsFromVI. convertTraceMetricsResult must copy that value into BOTH
// SearchMetrics.IndexBytesRead (the new per-source field) and SearchMetrics.InspectedBytes (the
// pre-existing aggregate field, which always read 0 on this path before this fix since
// result.BytesRead was never populated). Both assertions below pin the EXACT value, not just
// non-zero, so a regression that zeroes or drops either field is caught precisely.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestConvertTraceMetricsResult_PropagatesIndexBytesRead pins convertTraceMetricsResult's exact
// mapping in isolation, independent of any real value-index I/O: a hand-built
// blockpack.TraceMetricsResult with a known IndexBytesRead must produce a response whose
// IndexBytesRead AND InspectedBytes both equal that exact value.
func TestConvertTraceMetricsResult_PropagatesIndexBytesRead(t *testing.T) {
	const knownBytes = 424242

	result := &blockpack.TraceMetricsResult{
		IndexBytesRead: knownBytes,
	}
	req := countOverTimeReq(`{} | count_over_time()`)

	resp := convertTraceMetricsResult(result, req)

	require.NotNil(t, resp.Metrics)
	require.EqualValues(t, knownBytes, resp.Metrics.IndexBytesRead,
		"IndexBytesRead must equal result.IndexBytesRead exactly")
	require.EqualValues(t, knownBytes, resp.Metrics.InspectedBytes,
		"InspectedBytes must equal result.IndexBytesRead exactly (issue #218: this used to always "+
			"read 0 because result.BytesRead was never populated on this path)")
}

// TestQueryRange_VIMetricsPath_ReportsIndexBytesReadAndInspectedBytes drives the REAL
// (b *blockpackBlock).QueryRange through a real write path and a real, VI-answered filtered
// count_over_time() query (mirrors unfiltered_metrics_vi_misattribution_test.go's sanity-check
// pattern). Before this fix, resp.Metrics.InspectedBytes was always 0 on this path regardless of
// how much real VI I/O occurred; it must now equal resp.Metrics.IndexBytesRead exactly, and that
// value must be non-zero (real VI files were genuinely read to answer this query).
func TestQueryRange_VIMetricsPath_ReportsIndexBytesReadAndInspectedBytes(t *testing.T) {
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
	resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)

	require.NotZero(t, resp.Metrics.IndexBytesRead,
		"a real VI-answered query must report non-zero IndexBytesRead")
	require.Equal(t, resp.Metrics.IndexBytesRead, resp.Metrics.InspectedBytes,
		"issue #218: InspectedBytes must exactly mirror IndexBytesRead on the VI metrics path, "+
			"not silently stay 0")
}
