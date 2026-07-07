package vblockpack

// value_index_inconsistency_test.go — pins the NOTE-VI-078 (issue #481) contract:
// the search/metrics value index is AUTHORITATIVE for the columns it covers. When
// the index HAS coverage and produces matches but names a block/page the data file
// cannot resolve, that is index corruption — Fetch must FAIL the query rather than
// mask it with a silent full scan. This mirrors the trace-by-id path's NOTE-VI-071
// posture (a matched span the reader cannot resolve is an error, not a fallback).
//
// It also pins the still-standing exception: a query the index genuinely CANNOT
// answer (no reader configured / no coverage — a routine decline) still falls back
// to a correct full scan and returns results, unchanged by NOTE-VI-078.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
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

// emptyBlockIterator yields no traces, producing a structurally-empty block whose
// pages the value index for a populated block will fail to resolve.
type emptyBlockIterator struct{}

func (emptyBlockIterator) Next(context.Context) (common.ID, *tempopb.Trace, error) {
	return nil, nil, io.EOF
}
func (emptyBlockIterator) Close() {}

// writeSvcBlock writes a single-trace block whose resource.service.name is svc,
// with spanCount spans over a small row-group size so the block has multiple pages.
// The shared viStore captures the value-index files the write path emits. It
// returns the resulting meta and the block's on-disk data path.
func writeSvcBlock(
	t *testing.T,
	dir string,
	viStore *fakeVISink,
	tenant string,
	blockID uuid.UUID,
	svc string,
	spanCount int,
) (*backend.BlockMeta, string) {
	t.Helper()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	traceID := []byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9}
	spans := make([]*tempotrace.Span, spanCount)
	for i := 0; i < spanCount; i++ {
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            []byte{byte(i%250 + 1), byte(i / 250), 0, 0, 0, 0, 0, 1},
			Name:              "op",
			StartTimeUnixNano: now,
			EndTimeUnixNano:   now + 1,
		}
	}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{{
					Key:   "service.name",
					Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: svc}},
				}},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: spans}},
		}},
	}

	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(5 * time.Minute)
	cfg := &common.BlockConfig{RowGroupSizeBytes: 200} // small ⇒ many pages

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return resultMeta, filepath.Join(dir, tenant, blockID.String(), DataFileName)
}

// writeEmptyBlock writes a block with no traces and returns its meta and data path.
func writeEmptyBlock(t *testing.T, dir, tenant string, blockID uuid.UUID) (*backend.BlockMeta, string) {
	t.Helper()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	meta := backend.NewBlockMeta(tenant, blockID, VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(5 * time.Minute)
	resultMeta, err := CreateBlock(context.Background(), &common.BlockConfig{RowGroupSizeBytes: 1000},
		meta, emptyBlockIterator{}, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return resultMeta, filepath.Join(dir, tenant, blockID.String(), DataFileName)
}

// svcAlphaFetchReq returns a Fetch request equivalent to `{ resource.service.name = "svc-alpha" }`.
func svcAlphaFetchReq() (context.Context, traceql.FetchSpansRequest) {
	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-alpha" }`, false)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-alpha")},
		}},
		AllConditions: true,
	}
	return ctx, req
}

// TestFetch_IndexDataInconsistencyFailsQuery pins NOTE-VI-078: when the value index
// covers the query column and produces matches, but the data file those matches
// name cannot resolve their pages, Fetch returns an error (authoritative-index
// corruption) instead of silently scanning to a possibly-wrong result.
//
// The inconsistency is constructed deterministically: write a populated,
// multi-page block A (its value-index entries name A's pages, stamped with
// sourceRef = blockObjectKey(tenant, A)), then overwrite A's data object with a
// structurally-EMPTY block's bytes. A's index still covers the query and matches
// spans, but the served empty block has no block start for the pages the index
// names ⇒ QueryTraceQLFromIndex surfaces an index/data inconsistency, which Fetch
// must now propagate as a query error.
func TestFetch_IndexDataInconsistencyFailsQuery(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	blockID := uuid.New()
	metaA, dataPathA := writeSvcBlock(t, dir, viStore, tenant, blockID, "svc-alpha", 300)

	// Overwrite A's data with an empty block's bytes (different id, its own dir), so
	// the pages A's index names have no block start in the served data.
	scratch := t.TempDir()
	metaEmpty, dataPathEmpty := writeEmptyBlock(t, scratch, tenant, uuid.New())
	emptyBytes, err := os.ReadFile(dataPathEmpty)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dataPathA, emptyBytes, 0o644))
	metaA.Size_ = metaEmpty.Size_ // reader is bound to meta.Size_; match the served bytes

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx, req := svcAlphaFetchReq()
	_, err = block.Fetch(ctx, req, common.SearchOptions{})
	require.Error(t, err, "an authoritative-index/data inconsistency must fail the query, not silently scan (NOTE-VI-078)")
	require.Contains(t, err.Error(), "value index inconsistency")
}

// TestFetch_NoIndexReaderFallsBackToScan pins the still-standing exception: with no
// value-index query reader configured, tryIndexFetch is a routine decline (not an
// error), so Fetch falls back to a correct full scan and returns the match.
// NOTE-VI-078 changes ONLY the index/data-inconsistency case, not routine "the
// index cannot answer" declines.
func TestFetch_NoIndexReaderFallsBackToScan(t *testing.T) {
	dir := t.TempDir()
	withVISink(t, nil, "")
	withVIQueryReader(t, nil, "") // index path disabled ⇒ must scan

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, &fakeVISink{}, tenant, uuid.New(), "svc-alpha", 3)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx, req := svcAlphaFetchReq()
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err, "with no index reader, Fetch must fall back to a correct scan, not error")
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
	require.NotEmpty(t, spansets, "scan fallback must still find the matching trace")
}
