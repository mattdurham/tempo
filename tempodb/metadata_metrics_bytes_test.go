package tempodb

// metadata_metrics_bytes_test.go — issue #218 Phase 8. MetadataMetrics (SearchTags(V2)Response /
// SearchTagValues(V2)Response's Metrics) now carries the same IndexBytesRead/DataFileBytesRead/
// VcntBytesRead breakdown SearchMetrics already carries (Phases 1-7), but no construction site
// populated them until this phase. Tag/tag-value search on a vblockpack block never consults the
// value index or VCNT (backend_block.go's SearchTags/SearchTagValues/SearchTagValuesV2/
// FetchTagValues/FetchTagNames build their TraceQL query with no ValueIndex set), so
// DataFileBytesRead is the only field capable of being nonzero here — this file drives the real
// tempodb.Reader entry points (not a hand-built response) against a real vblockpack block to
// prove that value now reaches MetadataMetrics, mirroring the exact InspectedBytes value that was
// already correct before this phase, while IndexBytesRead/VcntBytesRead stay exactly 0.

import (
	"context"
	"testing"

	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
	"github.com/stretchr/testify/require"
)

// vblockpackTestConfig is testConfig with the block version forced to vblockpack — testConfig's
// own default (encoding.DefaultEncoding(), vparquet4) already threads DataFileBytesRead
// correctly (block_search_tags.go's mcb(rr.BytesRead()) predates this phase), so a vblockpack-
// specific test is required to catch the vblockpack tag-search methods' previously-discarded
// MetricsCallback parameter (this phase's actual fix).
func vblockpackTestConfig(t *testing.T) (Reader, Writer, Compactor) {
	r, w, c, _ := testConfig(t, 0, func(cfg *Config) {
		cfg.Block.Version = vblockpack.VersionString
	})
	return r, w, c
}

func writeVblockpackTagsBlock(t *testing.T) (Reader, common.BackendBlock) {
	t.Helper()
	r, w, _ := vblockpackTestConfig(t)

	blockID := backend.NewUUID()
	wal := w.WAL()
	meta := &backend.BlockMeta{BlockID: blockID, TenantID: testTenantID}
	head, err := wal.NewBlock(meta, model.CurrentEncoding)
	require.NoError(t, err)

	id := test.ValidTraceID(nil)
	tr := test.MakeTraceWithTags(id, "test-service", 7)
	// (*vblockpack.walBlock).Append expects a raw marshaled tempopb.Trace — unlike the
	// vparquet WAL blocks, it does not go through model.SegmentDecoder's ToObject framing,
	// so writeTraceToWal (which wraps via ToObject) is not usable here.
	raw, err := tr.Marshal()
	require.NoError(t, err)
	require.NoError(t, head.Append(id, raw, 0, 0, true))

	block, err := w.CompleteBlock(context.Background(), head)
	require.NoError(t, err)

	return r, block
}

// TestVblockpack_SearchTags_ReportsDataFileBytesRead is the mutation-test regression guard for
// this phase's fix to (*blockpackBlock).SearchTags: before the fix, the MetricsCallback parameter
// was named "_" and never invoked, so InspectedBytes/DataFileBytesRead were both unconditionally
// 0 for a vblockpack tag search (the exact same class of bug Phase 2 fixed for the metrics path).
func TestVblockpack_SearchTags_ReportsDataFileBytesRead(t *testing.T) {
	r, block := writeVblockpackTagsBlock(t)

	resp, err := r.SearchTags(context.Background(), block.BlockMeta(), &tempopb.SearchTagsBlockRequest{
		SearchReq: &tempopb.SearchTagsRequest{Scope: ""},
	}, common.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)

	require.NotZero(t, resp.Metrics.InspectedBytes, "SearchTags must report nonzero InspectedBytes on a real vblockpack block")
	require.Equal(t, resp.Metrics.InspectedBytes, resp.Metrics.DataFileBytesRead,
		"issue #218 Phase 8: SearchTags never consults the value index or VCNT, so DataFileBytesRead must exactly equal InspectedBytes")
	require.Zero(t, resp.Metrics.IndexBytesRead, "SearchTags never consults the value index")
	require.Zero(t, resp.Metrics.VcntBytesRead, "SearchTags never consults VCNT")
}

// TestVblockpack_SearchTagValues_ReportsDataFileBytesRead is the mutation-test regression guard
// for (*blockpackBlock).SearchTagValues — before the fix, executeQuery's real QueryStats were
// discarded (matches, _ := ...) so the MetricsCallback was never invoked.
func TestVblockpack_SearchTagValues_ReportsDataFileBytesRead(t *testing.T) {
	r, block := writeVblockpackTagsBlock(t)

	resp, err := r.SearchTagValues(context.Background(), block.BlockMeta(), &tempopb.SearchTagValuesBlockRequest{
		SearchReq: &tempopb.SearchTagValuesRequest{TagName: "service.name"},
	}, common.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)
	require.Contains(t, resp.TagValues, "test-service")

	require.NotZero(t, resp.Metrics.InspectedBytes, "SearchTagValues must report nonzero InspectedBytes on a real vblockpack block")
	require.Equal(t, resp.Metrics.InspectedBytes, resp.Metrics.DataFileBytesRead,
		"issue #218 Phase 8: SearchTagValues never consults the value index or VCNT, so DataFileBytesRead must exactly equal InspectedBytes")
	require.Zero(t, resp.Metrics.IndexBytesRead, "SearchTagValues never consults the value index")
	require.Zero(t, resp.Metrics.VcntBytesRead, "SearchTagValues never consults VCNT")
}

// TestVblockpack_SearchTagValuesV2_ReportsDataFileBytesRead is the mutation-test regression guard
// for (*blockpackBlock).SearchTagValuesV2.
func TestVblockpack_SearchTagValuesV2_ReportsDataFileBytesRead(t *testing.T) {
	r, block := writeVblockpackTagsBlock(t)

	resp, err := r.SearchTagValuesV2(context.Background(), block.BlockMeta(), &tempopb.SearchTagValuesRequest{
		TagName: "resource.service.name",
	}, common.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotNil(t, resp.Metrics)

	require.NotZero(t, resp.Metrics.InspectedBytes, "SearchTagValuesV2 must report nonzero InspectedBytes on a real vblockpack block")
	require.Equal(t, resp.Metrics.InspectedBytes, resp.Metrics.DataFileBytesRead,
		"issue #218 Phase 8: SearchTagValuesV2 never consults the value index or VCNT, so DataFileBytesRead must exactly equal InspectedBytes")
	require.Zero(t, resp.Metrics.IndexBytesRead, "SearchTagValuesV2 never consults the value index")
	require.Zero(t, resp.Metrics.VcntBytesRead, "SearchTagValuesV2 never consults VCNT")
}
