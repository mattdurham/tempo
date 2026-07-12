package vblockpack

// phase8_oracle_test.go — Phase 8 (plan-scan-fallback.md) cross-cutting correctness suite.
//
// This file covers single-leaf and multi-leaf-OR only (written while Phases 6/7 were still
// in flight, per team-lead's sequencing). AND and structural coverage is NOT added here —
// after Phases 6/7 landed, dedicated exact-oracle-match and landmine-negative-control tests
// for both shapes were confirmed to already exist and pass, so this file was deliberately
// NOT extended to avoid duplicating them:
//   - AND: internal/modules/vibuilder/builder_bounded_and_test.go (blockpack) —
//     TestBuildSourceBoundedMultiLeafAND_{AnchorConfirm_DoesNotUnderReport,ExactOracleMatch,
//     WrongAnchorStillCorrect} — plus this repo's own
//     TestFetch_MultiLeafAND_WithLimit_ExactNewestLimitAnswer_EndToEnd (real Fetch call chain).
//   - Structural: structural_index_seed_ordering_internal_test.go (blockpack) —
//     TestChooseDiscoverySeed_NewestFirst_{ExactOracleMatch,MutationGuard_RevertsToLexicographic}
//     — plus this repo's own TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch
//     (Phase 6's dispatch-asymmetry guard).
//
// This test's job, per the plan's own Q5 finding, is to prove "early-stopping resolution
// returns EXACTLY the same top-N-by-recency result a full resolution would have produced" —
// not "looks plausible," not "right count." It runs the SAME fixture and the SAME query shapes
// through BOTH the old unbounded path (MaxTraces=0, the oracle) and the new bounded path
// (MaxTraces=N), through the REAL tempodb-level Fetch call chain, asserting exact top-N
// equality by trace identity.
//
// The fixture is deliberately a 3-way partition (not just 2-way, like the individual Phase
// 2/3 tests) so the OR case's union genuinely excludes a non-trivial "neither" subset, not
// merely two disjoint match sets covering 100% of the fixture.

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

// writePhase8ThreeWayFixture writes nSpans DISTINCT, minute-spaced traces (span i strictly
// chronologically older than span i+1), partitioned three ways by i%3:
//   - i%3==0: resource.service.name="svc-a", span.http.method="POST"  (matches leaf A only)
//   - i%3==1: resource.service.name="svc-other", span.http.method="GET" (matches leaf B only)
//   - i%3==2: resource.service.name="svc-other", span.http.method="POST" (matches NEITHER — decoy)
//
// Trace index i is encoded into TraceID[14:16] so a test can identify exactly which traces came
// back. MaxSpansPerBlock=1 (one internal blockpack block per span) mirrors this session's other
// bounded-dispatch fixtures.
func writePhase8ThreeWayFixture(t *testing.T, nSpans int) *blockpackBlock {
	t.Helper()
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	traces := make([]*tempopb.Trace, nSpans)
	ids := make([][]byte, nSpans)
	for i := 0; i < nSpans; i++ {
		traceID := make([]byte, 16)
		traceID[0] = 0x9E
		traceID[14] = byte(i >> 8)                                   //nolint:gosec
		traceID[15] = byte(i)                                        //nolint:gosec
		spanID := []byte{0x9F, byte(i >> 8), byte(i), 0, 0, 0, 0, 1} //nolint:gosec
		startNano := now + uint64(i)*uint64(2*time.Minute)

		svc, method := "svc-other", "POST"
		switch i % 3 {
		case 0:
			svc = "svc-a"
		case 1:
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

// traceIndicesFromSpansets extracts the encoded trace index (TraceID[14:16]) from each spanset.
func traceIndicesFromSpansets(spansets []*traceql.Spanset) map[int]bool {
	out := make(map[int]bool, len(spansets))
	for _, ss := range spansets {
		idx := int(ss.TraceID[14])<<8 | int(ss.TraceID[15])
		out[idx] = true
	}
	return out
}

// oracleTopN runs query against block with NO limit (MaxTraces=0, the unbounded/oracle path —
// boundedAuthorized derives false, so Fetch takes the ordinary BuildValueIndexSource path) and
// returns the newest n trace indices from the FULL result set, by identity — the ground truth
// TestPhase8_ExactOracleParity's bounded runs are checked against.
func oracleTopN(t *testing.T, block *blockpackBlock, query string, n int) map[int]bool {
	t.Helper()
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err, "oracle (unbounded) run must succeed")
	spansets := drainSpansets(ctx, t, resp)

	indices := make([]int, 0, len(spansets))
	for idx := range traceIndicesFromSpansets(spansets) {
		indices = append(indices, idx)
	}
	// Newest-first is just descending index order in this fixture (index encodes recency).
	for i := 0; i < len(indices); i++ {
		for j := i + 1; j < len(indices); j++ {
			if indices[j] > indices[i] {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}
	require.GreaterOrEqual(t, len(indices), n, "oracle set must contain at least n matches for a meaningful top-n slice")
	want := make(map[int]bool, n)
	for _, idx := range indices[:n] {
		want[idx] = true
	}
	return want
}

// TestPhase8_ExactOracleParity_SingleLeafAndOR is the Phase 8 (non-AND scope) cross-cutting
// correctness test: for BOTH a single-leaf query and a multi-leaf OR query, over the SAME
// 3-way-partitioned fixture, the bounded (early-stopping) path's result must be EXACTLY the
// unbounded oracle's top-limit entries by recency — not merely the right count, and not merely
// "looks plausible" (the plan's own Q5 finding). Ties Phase 2's and Phase 3's individually-tested
// properties together over one shared fixture, as the plan's Phase 8 charter requires.
func TestPhase8_ExactOracleParity_SingleLeafAndOR(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	const nSpans = 12
	block := writePhase8ThreeWayFixture(t, nSpans)

	t.Run("SingleLeaf", func(t *testing.T) {
		query := `{ resource.service.name = "svc-a" }`
		const limit = 2
		want := oracleTopN(t, block, query, limit)

		ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
		resp, err := block.Fetch(ctx, traceql.FetchSpansRequest{}, common.SearchOptions{MaxTraces: limit})
		require.NoError(t, err)
		spansets := drainSpansets(ctx, t, resp)
		require.Len(t, spansets, limit)
		got := traceIndicesFromSpansets(spansets)
		require.Equal(t, want, got, "bounded single-leaf result must exactly match the unbounded oracle's top-limit")
	})

	t.Run("MultiLeafOR", func(t *testing.T) {
		query := `{ resource.service.name = "svc-a" || span.http.method = "GET" }`
		const limit = 3
		want := oracleTopN(t, block, query, limit)

		ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
		resp, err := block.Fetch(ctx, traceql.FetchSpansRequest{}, common.SearchOptions{MaxTraces: limit})
		require.NoError(t, err)
		spansets := drainSpansets(ctx, t, resp)
		require.Len(t, spansets, limit)
		got := traceIndicesFromSpansets(spansets)
		require.Equal(t, want, got, "bounded multi-leaf-OR result must exactly match the unbounded oracle's top-limit, "+
			"across the union of BOTH leaves, correctly excluding the decoy (neither-leaf) traces")
	})
}
