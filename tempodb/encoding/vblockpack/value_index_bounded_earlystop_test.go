package vblockpack

// value_index_bounded_earlystop_test.go — Phase 2 (plan-scan-fallback.md) integration-style test:
// a real limit-bearing, single-leaf FILTER query with a genuine multi-block low-selectivity shape,
// through the REAL tempodb-level Fetch call chain (not a hand-constructed QueryOptions), confirming
// tryIndexFetch's new BuildValueIndexSourceBounded wiring produces the exact newest-limit answer
// end to end rather than merely "the right count."

import (
	"context"
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

// writeMinuteSpacedFilterFixture writes nSpans DISTINCT traces, each a single span with
// resource.service.name=svc, spaced 2 MINUTES apart (span i strictly chronologically older than
// span i+1). The value index floors TimeSec to the minute boundary (SPEC-VI-4/NOTE-VI-051) — a
// spacing narrower than a minute (e.g. writeRecentFirstMultiBlockFilterFixture's 1-second spacing,
// designed for the OLD RecentFirstBudget's block-count-based divergence, not this test's
// timestamp-based one) would collapse every entry into the SAME (TimeSec, CanonicalValue) value-
// index group, making newest-first order unobservable at this test's granularity. Trace index i is
// encoded into TraceID[14:16] so a test can assert exactly which traces came back, by identity.
func writeMinuteSpacedFilterFixture(t *testing.T, nSpans int, svc string) *blockpackBlock {
	t.Helper()
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	traces := make([]*tempopb.Trace, nSpans)
	ids := make([][]byte, nSpans)
	for i := 0; i < nSpans; i++ {
		traceID := make([]byte, 16)
		traceID[0] = 0x9A
		traceID[14] = byte(i >> 8)                                   //nolint:gosec
		traceID[15] = byte(i)                                        //nolint:gosec
		spanID := []byte{0x9B, byte(i >> 8), byte(i), 0, 0, 0, 0, 1} //nolint:gosec
		startNano := now + uint64(i)*uint64(2*time.Minute)           // strictly increasing, 2m apart: span i is older than span i+1
		traces[i] = &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{{
						Key:   "service.name",
						Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: svc}},
					}},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: spanID, Name: "op",
					StartTimeUnixNano: startNano, EndTimeUnixNano: startNano + 1000,
				}}}},
			}},
		}
		ids[i] = traceID
	}

	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(time.Duration(nSpans+1) * 2 * time.Minute)
	cfg := &common.BlockConfig{Blockpack: common.BlockpackConfig{MaxSpansPerBlock: 1}}

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return newBackendBlock(resultMeta, backend.NewReader(rawR))
}

// TestFetch_SingleLeafWithLimit_BoundedIndexPath_ExactNewestMatch is the Phase 2 required
// integration test: a real 10-independent-trace fixture with a REAL working value-index reader
// configured (not vr==nil — this exercises the index-driven path, not Phase 0's hard-error
// branch), and a limit of 4 present. Asserts the exact 4 newest traces are returned, by trace-index
// identity (writeMinuteSpacedFilterFixture encodes trace index i into TraceID[14:16], with span i
// strictly chronologically older than span i+1) — not merely that 4 traces came back.
func TestFetch_SingleLeafWithLimit_BoundedIndexPath_ExactNewestMatch(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	const nSpans = 10
	const limit = 4
	block := writeMinuteSpacedFilterFixture(t, nSpans, "svc-a")

	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ resource.service.name = "svc-a" }`, false)
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: limit})
	require.NoError(t, err, "a single-leaf query with a limit present must answer from the "+
		"bounded index path, never hard-error and never an implicit scan")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, limit, "must return exactly limit traces, not merely at least limit "+
		"(this fixture's per-trace-per-minute shape makes an exact count achievable)")

	gotIndices := make(map[int]bool, len(spansets))
	for _, ss := range spansets {
		idx := int(ss.TraceID[14])<<8 | int(ss.TraceID[15])
		gotIndices[idx] = true
	}
	wantIndices := map[int]bool{6: true, 7: true, 8: true, 9: true} // the 4 newest of 10 (i=0..9)
	require.Equal(t, wantIndices, gotIndices,
		"bounded index path must return EXACTLY the newest-limit traces by recency, not an "+
			"arbitrary subset of the right size")
}

// writeMinuteSpacedORFixture writes nSpans DISTINCT traces, minute-spaced apart exactly like
// writeMinuteSpacedFilterFixture, but alternating which of two leaves each trace satisfies: EVEN
// trace index i sets resource.service.name="svc-a" (and a non-matching span.http.method), ODD i
// sets span.http.method="GET" (and a non-matching resource.service.name) — so no trace matches
// BOTH leaves, and the OR union of the two leaves covers every trace exactly once, by exactly one
// leaf. This exercises Phase 3's flat-OR-of-leaves path genuinely combining two DIFFERENT
// columns' per-leaf newest-first results, not two copies of the same leaf.
func writeMinuteSpacedORFixture(t *testing.T, nSpans int) *blockpackBlock {
	t.Helper()
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	traces := make([]*tempopb.Trace, nSpans)
	ids := make([][]byte, nSpans)
	for i := 0; i < nSpans; i++ {
		traceID := make([]byte, 16)
		traceID[0] = 0x9C
		traceID[14] = byte(i >> 8)                                   //nolint:gosec
		traceID[15] = byte(i)                                        //nolint:gosec
		spanID := []byte{0x9D, byte(i >> 8), byte(i), 0, 0, 0, 0, 1} //nolint:gosec
		startNano := now + uint64(i)*uint64(2*time.Minute)

		svc, method := "svc-nomatch", "POST"
		if i%2 == 0 {
			svc = "svc-a"
		} else {
			method = "GET"
		}
		traces[i] = &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{{
						Key:   "service.name",
						Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: svc}},
					}},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: spanID, Name: "op",
					StartTimeUnixNano: startNano, EndTimeUnixNano: startNano + 1000,
					Attributes: []*tempocommon.KeyValue{{
						Key:   "http.method",
						Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: method}},
					}},
				}}}},
			}},
		}
		ids[i] = traceID
	}

	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(time.Duration(nSpans+1) * 2 * time.Minute)
	cfg := &common.BlockConfig{Blockpack: common.BlockpackConfig{MaxSpansPerBlock: 1}}

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return newBackendBlock(resultMeta, backend.NewReader(rawR))
}

// TestFetch_FlatORWithLimit_BoundedIndexPath_ExactNewestMatch is the Phase 3 required
// integration test: a real 10-independent-trace fixture where each trace matches exactly ONE of
// two OR'd leaves (resource.service.name="svc-a" OR span.http.method="GET"), through the REAL
// tempodb-level Fetch call chain, with a limit of 4 present. Asserts the exact 4 newest traces
// are returned by identity — proving the executor's ViUnionNewestFirst merge (metrics_trace.go)
// and vibuilder's per-leaf newest-first resolution (BuildSourceBounded's flatOR branch) combine
// correctly end to end, not merely that 4 traces of the right count came back.
func TestFetch_FlatORWithLimit_BoundedIndexPath_ExactNewestMatch(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	const nSpans = 10
	const limit = 4
	block := writeMinuteSpacedORFixture(t, nSpans)

	ctx := common.WithOriginalTraceQLQuery(
		context.Background(), `{ resource.service.name = "svc-a" || span.http.method = "GET" }`, false,
	)
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: limit})
	require.NoError(t, err, "a flat-OR-of-leaves query with a limit present must answer from the "+
		"bounded index path, never hard-error and never an implicit scan")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, limit, "must return exactly limit traces, not merely at least limit")

	gotIndices := make(map[int]bool, len(spansets))
	for _, ss := range spansets {
		idx := int(ss.TraceID[14])<<8 | int(ss.TraceID[15])
		gotIndices[idx] = true
	}
	wantIndices := map[int]bool{6: true, 7: true, 8: true, 9: true} // the 4 newest of 10 (i=0..9)
	require.Equal(t, wantIndices, gotIndices,
		"bounded OR index path must return EXACTLY the newest-limit traces by recency across BOTH "+
			"leaves, not an arbitrary subset of the right size")
}
