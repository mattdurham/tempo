package vblockpack

// structural_dispatch_test.go — DT1/DT2 (plan-d.md, issue #489): pins the new index-driven
// structural dispatch branch in Fetch (backend_block.go) and its indexOnly/coverage-gap
// contract (blockpack.ErrStructuralIndexCoverageGap), mirroring slice_errors_test.go's own
// IndexOnly contract tests for the filter path.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// structQuery is the 2-node positive-operator structural query exercised by this file's tests: a
// root span in "svc-root" whose descendant (>>) is in "svc-leaf".
const structQuery = `{ resource.service.name = "svc-root" } >> { resource.service.name = "svc-leaf" }`

// writeParentChildBlock writes a single 2-span trace (root in svc-root, child in svc-leaf,
// child.ParentSpanId = root.SpanId) through the real CreateBlock write path, so the process-level
// value-index sink (withVISink) populates BOTH the search VI (resource.service.name) and the
// trace-by-id VI/TraceGroup files structQuery's index-driven path needs — mirrors writeSvcBlock's
// (value_index_inconsistency_test.go) real-write-path convention, generalized to two distinct
// services/spans instead of N identical ones.
func writeParentChildBlock(t *testing.T, dir string, blockID uuid.UUID) (*backend.BlockMeta, string) {
	t.Helper()
	const tenant = "test-tenant"
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	traceID := []byte{0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42}
	rootSpanID := []byte{0x42, 0, 0, 0, 0, 0, 0, 0x01}
	childSpanID := []byte{0x42, 0, 0, 0, 0, 0, 0, 0x02}
	now := uint64(time.Now().UnixNano())

	mkAttr := func(k, v string) *tempocommon.KeyValue {
		return &tempocommon.KeyValue{Key: k, Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}}
	}

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-root")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: rootSpanID, Name: "root-op",
					StartTimeUnixNano: now, EndTimeUnixNano: now + uint64(200*time.Millisecond),
				}}}},
			},
			{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-leaf")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: childSpanID, ParentSpanId: rootSpanID, Name: "child-op",
					StartTimeUnixNano: now + uint64(10*time.Millisecond), EndTimeUnixNano: now + uint64(110*time.Millisecond),
				}}}},
			},
		},
	}

	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(5 * time.Minute)
	resultMeta, err := CreateBlock(
		context.Background(), &common.BlockConfig{}, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW),
	)
	require.NoError(t, err)
	return resultMeta, filepath.Join(dir, tenant, blockID.String(), DataFileName)
}

// structuralFetchReq returns a context carrying structQuery as the original TraceQL query
// (common.WithOriginalTraceQLQuery) — every call site pairs it with a zero-value
// traceql.FetchSpansRequest{}, since Fetch derives the query from the context, not Conditions.
func structuralFetchReq() context.Context {
	return common.WithOriginalTraceQLQuery(context.Background(), structQuery, false)
}

// drainSpansets reads every Spanset out of resp.Results, closing it when done.
func drainSpansets(ctx context.Context, t *testing.T, resp traceql.FetchSpansResponse) []*traceql.Spanset {
	t.Helper()
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
	return spansets
}

// spyValueIndexStore wraps a valueIndexStore and records whether any of its methods were
// invoked, so a test can assert the structural index-driven path was never even attempted.
type spyValueIndexStore struct {
	valueIndexStore
	called atomic.Bool
}

func (s *spyValueIndexStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.called.Store(true)
	return s.valueIndexStore.List(ctx, prefix)
}

func (s *spyValueIndexStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.called.Store(true)
	return s.valueIndexStore.Get(ctx, key)
}

func (s *spyValueIndexStore) Size(key string) (int64, error) {
	s.called.Store(true)
	return s.valueIndexStore.Size(key)
}

func (s *spyValueIndexStore) ReadAt(key string, p []byte, off int64) (int, error) {
	s.called.Store(true)
	return s.valueIndexStore.ReadAt(key, p, off)
}

// TestFetch_StructuralQuery_TriesIndexPathBeforeScanFallback (DT1) pins the new dispatch
// branch's PRIMARY success path: a genuine #487 slice job (IndexOnly=true) whose block has real
// search-VI + trace-by-id/TraceGroup coverage for a 2-node structural query answers from the
// index, with no error, finding the correct (child) match.
func TestFetch_StructuralQuery_TriesIndexPathBeforeScanFallback(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "a genuine #487 slice job with full index coverage must answer from the index, not error")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, 1, "the index-driven structural path must find the matching trace")
	require.NotEmpty(t, spansets[0].Spans, "must return at least the matched (child) span")
	require.True(t, spy.called.Load(), "the index-driven structural path must have been attempted")
}

// TestFetch_StructuralQuery_FallsBackToScanOnDecline (DT1) pins the routine-decline contract:
// under the default (non-slice) dispatch, IndexOnly=false, the new branch declines immediately
// (see value_index_structural_query.go's own doc comment) and the existing string-based scan path
// still answers the query correctly, unchanged.
func TestFetch_StructuralQuery_FallsBackToScanOnDecline(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes") // real coverage exists, but IndexOnly=false below

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: false})
	require.NoError(t, err)

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, 1, "the scan-path fallback must still find the matching trace")
	require.NotEmpty(t, spansets[0].Spans)
}

// TestFetch_StructuralQuery_NeverDispatchedAsBlockSharded (DT1) is the correctness-critical
// guard: a default block-sharded job (IndexOnly=false) must NEVER even attempt the index-driven
// structural path, since ExecuteStructuralFromIndex has no per-block ownership restriction and
// would otherwise return the full query answer from every block overlapping the window (see
// value_index_structural_query.go's package doc comment). Mutation-verification: this test was
// confirmed to FAIL (spy.called becomes true) against a mutated tryStructuralIndexFetch with the
// `if !indexOnly { return ... }` guard removed, before being left in its correct, passing state.
func TestFetch_StructuralQuery_NeverDispatchedAsBlockSharded(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: false})
	require.NoError(t, err)
	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, 1, "correctness must be preserved via the scan path")

	require.False(t, spy.called.Load(),
		"a default block-sharded (IndexOnly=false) Fetch call must never attempt the structural index path")
}

// TestFetch_StructuralIndexOnly_ReturnsTypedErrorOnCoverageGap (DT2) pins the #487 typed-error
// contract for structural slice jobs: with no value-index reader configured (a routine decline)
// and IndexOnly=true, Fetch must fail with blockpack.ErrStructuralIndexCoverageGap rather than
// falling back to a full scan.
func TestFetch_StructuralIndexOnly_ReturnsTypedErrorOnCoverageGap(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> routine decline

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	_, err = block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "a structural IndexOnly slice job must fail on a routine decline, not scan")
	require.True(t, errors.Is(err, blockpack.ErrStructuralIndexCoverageGap),
		"err = %v, want blockpack.ErrStructuralIndexCoverageGap", err)
}

// TestFetch_StructuralIndexOnlyFalse_StillFallsBackToScan (DT2) pins that IndexOnly=false (the
// default) keeps the pre-existing decline-then-scan behavior exactly, with no value-index reader
// configured at all — mirrors slice_errors_test.go's own IndexOnly=false precedent for the filter
// path (TestFetch_NoIndexReaderFallsBackToScan, value_index_inconsistency_test.go).
func TestFetch_StructuralIndexOnlyFalse_StillFallsBackToScan(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "")

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: false})
	require.NoError(t, err, "IndexOnly=false must keep falling back to a full scan when no index is configured")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, 1)
	require.NotEmpty(t, spansets[0].Spans)
}

// TestFetch_StructuralIndexOnly_DataInconsistencyFailsQuery (DT2) pins the OTHER half of the
// typed-error family: a genuine index/data inconsistency (the index has real coverage, but the
// served data file cannot resolve it) must ALSO fail the query rather than scan — mirrors
// TestFetch_IndexDataInconsistencyFailsQuery's (value_index_inconsistency_test.go) corruption
// technique for the filter path, applied to the structural path. This is deliberately a
// DIFFERENT error than blockpack.ErrStructuralIndexCoverageGap (that sentinel is reserved for
// coverage GAPS, not data corruption) — tryStructuralIndexFetch's own two branches
// (value_index_structural_query.go) must fail the query in BOTH cases, never scan in either.
func TestFetch_StructuralIndexOnly_DataInconsistencyFailsQuery(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	blockID := uuid.New()
	meta, dataPath := writeParentChildBlock(t, dir, blockID)

	// Overwrite the block's data with an empty block's bytes (different id, its own dir), so the
	// pages the index names have no block start in the served data — same technique
	// TestFetch_IndexDataInconsistencyFailsQuery uses for the filter path.
	scratch := t.TempDir()
	metaEmpty, dataPathEmpty := writeEmptyBlock(t, scratch, tenant, uuid.New())
	emptyBytes, err := os.ReadFile(dataPathEmpty)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dataPath, emptyBytes, 0o644))
	meta.Size_ = metaEmpty.Size_ // reader is bound to meta.Size_; match the served bytes

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := structuralFetchReq()
	req := traceql.FetchSpansRequest{}
	_, err = block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "a structural index/data inconsistency must fail the query, not silently scan")
	require.False(t, errors.Is(err, blockpack.ErrStructuralIndexCoverageGap),
		"a genuine data inconsistency is a DIFFERENT failure than a coverage gap: err = %v", err)
}

// negatedStructQuery is the 2-node NEGATED structural query exercised by
// TestFetch_NegatedStructuralQuery_TriesIndexPathBeforeScanFallback. D6's candidate discovery is
// driven EXCLUSIVELY by the RIGHT (tested) side (blockpack's own
// ExecuteNegatedStructuralFromIndex package doc comment) — the left/negated side is never
// VI-resolved, so the right leg must have a REAL match for the query to find any candidate at all.
// Right = "svc-leaf" (writeParentChildBlock's real child span) anchors discovery; left =
// "svc-missing" (a service no span in the fixture has) is the vacuous-truth case — since nothing
// matches the left filter at all, the child span has no ancestor satisfying it, so !>> holds and
// the child span (the right/tested side) is the returned match.
const negatedStructQuery = `{ resource.service.name = "svc-missing" } !>> { resource.service.name = "svc-leaf" }`

// TestFetch_NegatedStructuralQuery_TriesIndexPathBeforeScanFallback (DT1b, issue #489 holistic
// review HIGH finding) pins the new D6 dispatch branch: a genuine #487 slice job (IndexOnly=true)
// with real search-VI + trace-by-id/TraceGroup coverage for a 2-node NEGATED structural query
// answers from blockpack.QueryNegatedStructuralFromIndex, with no error, finding the correct
// (child) match — mirrors TestFetch_StructuralQuery_TriesIndexPathBeforeScanFallback's positive-op
// shape exactly, reusing the SAME real-write-path fixture (writeParentChildBlock).
func TestFetch_NegatedStructuralQuery_TriesIndexPathBeforeScanFallback(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	spy := &spyValueIndexStore{valueIndexStore: viStore}
	withVIQueryReader(t, spy, "indexes")

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(context.Background(), negatedStructQuery, false)
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.NoError(t, err, "a genuine #487 slice job with full index coverage must answer a negated query from the index, not error")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, 1, "the index-driven negated structural path must find the matching trace")
	require.NotEmpty(t, spansets[0].Spans, "must return at least the matched (child) span")
	require.True(t, spy.called.Load(), "the index-driven negated structural path must have been attempted")
}

// TestFetch_NegatedStructuralIndexOnly_ReturnsTypedErrorOnCoverageGap (DT1b/DT2) pins that
// negated ops get the SAME typed-error/indexOnly contract as positive ops (tryNegatedStructuralIndexFetch
// reuses structuralDeclineOutcome unchanged) — mirrors
// TestFetch_StructuralIndexOnly_ReturnsTypedErrorOnCoverageGap exactly, with a negated query.
func TestFetch_NegatedStructuralIndexOnly_ReturnsTypedErrorOnCoverageGap(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled -> routine decline

	meta, _ := writeParentChildBlock(t, dir, uuid.New())

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(context.Background(), negatedStructQuery, false)
	req := traceql.FetchSpansRequest{}
	_, err = block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})
	require.Error(t, err, "a negated structural IndexOnly slice job must fail on a routine decline, not scan")
	require.True(t, errors.Is(err, blockpack.ErrStructuralIndexCoverageGap),
		"err = %v, want blockpack.ErrStructuralIndexCoverageGap", err)
}
