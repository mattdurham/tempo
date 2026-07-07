package vblockpack

// slice_errors_test.go — pins the #487 IndexOnly contract: a time-slice job
// (opts.IndexOnly / req.IndexOnly) must fail with ErrSliceIndexCoverageGap when the
// value index would otherwise routinely decline, never silently fall back to a full
// block scan or full-scan metrics path. IndexOnly=false (today's default) keeps the
// existing decline-then-scan behavior exactly — see TestFetch_NoIndexReaderFallsBackToScan
// (value_index_inconsistency_test.go), which already exercises common.SearchOptions{}
// (IndexOnly's zero value is false) and needs no changes for this contract.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestFetch_IndexOnlyReturnsTypedErrorOnRoutineDecline pins the #487 search-path
// contract: with no value-index reader configured (a routine decline) and
// IndexOnly=true, Fetch must fail with ErrSliceIndexCoverageGap rather than falling
// back to a full block scan.
func TestFetch_IndexOnlyReturnsTypedErrorOnRoutineDecline(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> routine decline

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, tenant, uuid.New(), "svc-alpha", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx, req := svcAlphaFetchReq()
	_, err = block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "an IndexOnly slice job must fail on a routine decline, not scan")
	require.True(t, errors.Is(err, ErrSliceIndexCoverageGap), "err = %v, want ErrSliceIndexCoverageGap", err)
}

// TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally pins the #487 IndexOnly
// feature's PRIMARY success path: a slice job (IndexOnly=true) whose value index has
// genuine coverage for the query answers normally from the index, with no error and
// no fallback scan. Mirrors TestFetch_IndexDataInconsistencyFailsQuery's real
// coverage setup (populated block + real VI entries via fakeVISink,
// value_index_inconsistency_test.go) but WITHOUT corrupting the block's data, so the
// index path resolves cleanly instead of hitting an inconsistency.
func TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally(t *testing.T) {
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
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "IndexOnly with full index coverage must answer normally, not error")
	defer resp.Results.Close()

	var spansets []*traceql.Spanset
	for {
		ss, err := resp.Results.Next(ctx)
		require.NoError(t, err)
		if ss == nil {
			break
		}
		spansets = append(spansets, ss)
	}
	require.NotEmpty(t, spansets, "IndexOnly with full coverage must return the matching trace")
}

// TestQueryRange_IndexOnlyReturnsTypedErrorWhenNoVICoverage is the metrics analog:
// with no value-index reader configured, IndexOnly=true must fail QueryRange with
// ErrSliceIndexCoverageGap rather than falling through to the cube path or the
// full-scan metrics path.
func TestQueryRange_IndexOnlyReturnsTypedErrorWhenNoVICoverage(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> no coverage

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, tenant, uuid.New(), "svc-alpha", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "an IndexOnly slice job must fail when the index has no coverage, not fall back to a scan")
	require.True(t, errors.Is(err, ErrSliceIndexCoverageGap), "err = %v, want ErrSliceIndexCoverageGap", err)
}

// TestQueryRange_IndexOnlyReturnsTypedErrorOnGroupByInnerDecline is the CRITICAL regression
// test for holistic-review Issue 1: QueryRange's FIRST gate (indexCovered, based on
// BuildValueIndexSourceForMetrics's own ANY-leaf-resolved ok) passes here — the block has
// genuine, real VI coverage for the filter leaf, written via writeSvcBlock exactly as
// TestFetch_IndexOnlyTrueWithFullCoverageAnswersNormally's positive-path fixture. But the
// query's aggregate SHAPE (count_over_time() with a group-by) is one
// ExecuteTraceMetricsFromVI's own SECOND, deeper decline gate declines regardless of leaf
// coverage. Before this fix, blockpack.ExecuteMetricsTraceQL had no way to know this was an
// IndexOnly job, so this exact decline silently fell back to a full, un-windowed scan even
// under IndexOnly — precisely the double-counting risk IndexOnly exists to prevent. This test
// pins that QueryRange now converts blockpack's own ErrValueIndexNoCoverage into
// ErrSliceIndexCoverageGap instead.
func TestQueryRange_IndexOnlyReturnsTypedErrorOnGroupByInnerDecline(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := &tempopb.QueryRangeRequest{
		Query: `{ resource.service.name = "svc-alpha" } | count_over_time() by (resource.service.name)`,
		Start: uint64(time.Now().Add(-10 * time.Minute).UnixNano()),
		End:   uint64(time.Now().UnixNano()),
		Step:  uint64(time.Minute.Nanoseconds()),
	}
	_, err = block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "an IndexOnly slice job whose query has an unsupported aggregate shape "+
		"(group-by) must fail even though the filter leaf itself has real index coverage")
	require.True(t, errors.Is(err, ErrSliceIndexCoverageGap), "err = %v, want ErrSliceIndexCoverageGap", err)
}
