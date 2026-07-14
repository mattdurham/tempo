package vblockpack

// search_index_bytes_test.go — issue #218 Phase 4. Fetch's FetchSpansResponse now carries two
// additional closures, IndexBytes and DataFileBytes, alongside the pre-existing Bytes (whole
// block size). Both tests below drive Fetch through the REAL write/query path (real block, real
// on-disk value-index files, real block scan) and cross-check the closures' returned values
// against the exact same istats.BytesRead / scan.<step>.bytes_read numbers Fetch already
// promotes onto its OTel span (issue #465) — not just "nonzero".

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestFetch_MatchAllBounded_ReportsDataFileBytesRead drives a real match-all query with a
// positive limit through the bounded newest-first materializer (QueryNewestFirstMatchAll,
// Phase 6b). This path never consults the value index at all (isMatchAll short-circuits before
// tryIndexFetch/tryStructuralIndexFetch), so IndexBytes() must report exactly 0, while
// DataFileBytes() must equal the real sum of the "scan.<step>.bytes_read" span attributes Fetch
// already promotes today.
func TestFetch_MatchAllBounded_ReportsDataFileBytesRead(t *testing.T) {
	rec, tp := recordedSpans(t)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 10000})
	require.NoError(t, err)
	defer resp.Results.Close()
	for {
		ss, nerr := resp.Results.Next(ctx)
		require.NoError(t, nerr)
		if ss == nil {
			break
		}
	}

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok, "Fetch must emit a vblockpack.backendBlock.Fetch span")

	var wantDataFileBytes int64
	for _, kv := range span.Attributes() {
		key := string(kv.Key)
		if strings.HasPrefix(key, "scan.") && strings.HasSuffix(key, ".bytes_read") {
			wantDataFileBytes += kv.Value.AsInt64()
		}
	}
	require.NotZero(t, wantDataFileBytes, "the bounded materializer must have performed a real block scan with nonzero bytes")

	require.NotNil(t, resp.IndexBytes, "IndexBytes closure must be set")
	require.NotNil(t, resp.DataFileBytes, "DataFileBytes closure must be set")
	require.EqualValues(t, 0, resp.IndexBytes(), "match-all bypasses the value index entirely; IndexBytes must be exactly 0")
	require.EqualValues(t, wantDataFileBytes, resp.DataFileBytes(),
		"DataFileBytes must equal the exact sum of scan.<step>.bytes_read span attributes")
}

// TestFetch_VIAnsweredSearch_ReportsIndexBytesRead drives a real, VI-answered filtered search
// query (mirrors value_index_inconsistency_test.go's svcAlphaFetchReq pattern) through the real
// write path and a real on-disk value index. tryIndexFetch answers the query directly
// (indexAnswered=true), so no block scan happens: DataFileBytes() must be exactly 0, while
// IndexBytes() must equal the exact real istats.BytesRead value Fetch already promotes onto its
// OTel span as "index.bytes_read" (issue #465).
func TestFetch_VIAnsweredSearch_ReportsIndexBytesRead(t *testing.T) {
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

	ctx, req := svcAlphaFetchReq()
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
	defer resp.Results.Close()
	for {
		ss, nerr := resp.Results.Next(ctx)
		require.NoError(t, nerr)
		if ss == nil {
			break
		}
	}

	require.NoError(t, tp.ForceFlush(context.Background()))
	span, ok := spanByName(rec.Ended(), "vblockpack.backendBlock.Fetch")
	require.True(t, ok, "Fetch must emit a vblockpack.backendBlock.Fetch span")

	rawWant, ok := attrValue(span, "index.bytes_read")
	require.True(t, ok, "the value-index-answered path must record index.bytes_read on the span")
	wantIndexBytes, err := strconv.ParseInt(rawWant, 10, 64)
	require.NoError(t, err)
	require.NotZero(t, wantIndexBytes, "a real VI-answered query must have read a nonzero number of index bytes")

	require.NotNil(t, resp.IndexBytes, "IndexBytes closure must be set")
	require.NotNil(t, resp.DataFileBytes, "DataFileBytes closure must be set")
	require.EqualValues(t, wantIndexBytes, resp.IndexBytes(),
		"IndexBytes must equal the exact real istats.BytesRead value recorded on the span")
	require.EqualValues(t, 0, resp.DataFileBytes(), "an index-answered query performs no block scan; DataFileBytes must be exactly 0")
}
