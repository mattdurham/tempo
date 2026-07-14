package combiner

import (
	"net/http/httptest"
	"testing"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/stretchr/testify/require"
)

func cacheMissResponse() PipelineResponse {
	rec := httptest.NewRecorder()
	rec.Header().Set(TempoCacheHeader, TempoCacheMiss)
	return &testPipelineResponse{r: rec.Result()}
}

func cacheHitResponse() PipelineResponse {
	rec := httptest.NewRecorder()
	rec.Header().Set(TempoCacheHeader, TempoCacheHit)
	return &testPipelineResponse{r: rec.Result()}
}

// issue #218 Phase 6: SearchMetricsCombiner sums per-job search-path index/data-file bytes.
func TestSearchMetricsCombiner_SumsIndexAndDataFileBytes(t *testing.T) {
	mc := NewSearchMetricsCombiner()

	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 100, DataFileBytesRead: 200}, cacheMissResponse())
	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 10, DataFileBytesRead: 20}, cacheMissResponse())

	require.Equal(t, uint64(110), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(220), mc.Metrics.DataFileBytesRead)
}

func TestSearchMetricsCombiner_CacheHitContributesZero(t *testing.T) {
	mc := NewSearchMetricsCombiner()

	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 100, DataFileBytesRead: 200}, cacheHitResponse())

	require.Equal(t, uint64(0), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(0), mc.Metrics.DataFileBytesRead)
}

// issue #218 Phase 6: QueryRangeMetricsCombiner sums per-job metrics query-range bytes from
// both the VI-answered path (IndexBytesRead/DataFileBytesRead) and the cube-answered path
// (CubeBytesRead).
func TestQueryRangeMetricsCombiner_SumsIndexDataFileAndCubeBytes(t *testing.T) {
	mc := NewQueryRangeMetricsCombiner()

	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 100, DataFileBytesRead: 200, CubeBytesRead: 300}, cacheMissResponse())
	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 10, DataFileBytesRead: 20, CubeBytesRead: 30}, cacheMissResponse())

	require.Equal(t, uint64(110), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(220), mc.Metrics.DataFileBytesRead)
	require.Equal(t, uint64(330), mc.Metrics.CubeBytesRead)
}

func TestQueryRangeMetricsCombiner_CacheHitContributesZero(t *testing.T) {
	mc := NewQueryRangeMetricsCombiner()

	mc.Combine(&tempopb.SearchMetrics{IndexBytesRead: 100, DataFileBytesRead: 200, CubeBytesRead: 300}, cacheHitResponse())

	require.Equal(t, uint64(0), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(0), mc.Metrics.DataFileBytesRead)
	require.Equal(t, uint64(0), mc.Metrics.CubeBytesRead)
}

// issue #218 Phase 6: MetadataMetricsCombiner sums these fields mechanically for consistency,
// even though no construction site populates them yet (tracked as a gap, not a bug here).
func TestMetadataMetricsCombiner_SumsIndexDataFileAndVcntBytes(t *testing.T) {
	mc := NewMetadataMetricsCombiner()

	mc.Combine(&tempopb.MetadataMetrics{IndexBytesRead: 100, DataFileBytesRead: 200, VcntBytesRead: 300}, cacheMissResponse())
	mc.Combine(&tempopb.MetadataMetrics{IndexBytesRead: 10, DataFileBytesRead: 20, VcntBytesRead: 30}, cacheMissResponse())

	require.Equal(t, uint64(110), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(220), mc.Metrics.DataFileBytesRead)
	require.Equal(t, uint64(330), mc.Metrics.VcntBytesRead)
}

func TestMetadataMetricsCombiner_CacheHitContributesZero(t *testing.T) {
	mc := NewMetadataMetricsCombiner()

	mc.Combine(&tempopb.MetadataMetrics{IndexBytesRead: 100, DataFileBytesRead: 200, VcntBytesRead: 300}, cacheHitResponse())

	require.Equal(t, uint64(0), mc.Metrics.IndexBytesRead)
	require.Equal(t, uint64(0), mc.Metrics.DataFileBytesRead)
	require.Equal(t, uint64(0), mc.Metrics.VcntBytesRead)
}
