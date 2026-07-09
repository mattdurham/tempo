package vblockpack

// recentfirst_budget_test.go — issue #481-FOLLOWUP-1: the vr==nil WithLimit regression pins
// (fetch_bounded_dispatch_test.go, structural_dispatch_test.go) could not turn genuinely red on
// revert of the vr==nil fixes (#76/#77), because the PRODUCTION budget default (MaxBlocks=50)
// never diverges from an unbounded scan at any test-fixture size reasonable for a fast unit test —
// bounded and full scans read the identical block set. This file provides the test seam
// (setBoundedRecentFirstPolicyForTest) and real multi-block fixture writers needed to make bounded
// vs. unbounded provably diverge: MaxSpansPerBlock=1 forces one blockpack-internal block per span
// (create.go maps common.BlockConfig.Blockpack.MaxSpansPerBlock straight to
// blockpack.WriterConfig.MaxBlockSpans), and a small injected MaxBlocks then genuinely truncates
// the block set — mirroring blockpack's own writeRecentFirstBigBlocks fixture lesson
// (recentfirst_test.go) at the tempo layer.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// setBoundedRecentFirstPolicyForTest installs policy as the querier's bounded newest-first budget
// for the duration of the test and restores the prior policy on cleanup, mirroring
// withVIQueryReader's (value_index_query_test.go) established process-level-singleton pattern.
func setBoundedRecentFirstPolicyForTest(t *testing.T, policy blockpack.RecentFirstBudget) {
	t.Helper()
	boundedRecentFirstPolicyMu.Lock()
	prev := boundedRecentFirstPolicy
	boundedRecentFirstPolicy = policy
	boundedRecentFirstPolicyMu.Unlock()
	t.Cleanup(func() {
		boundedRecentFirstPolicyMu.Lock()
		boundedRecentFirstPolicy = prev
		boundedRecentFirstPolicyMu.Unlock()
	})
}

// writeRecentFirstMultiBlockFilterFixture writes nSpans DISTINCT traces, each a single span with
// resource.service.name=svc, strictly increasing StartTimeUnixNano (span i is chronologically
// older than span i+1), via common.BlockConfig{Blockpack.MaxSpansPerBlock: 1} — one span per
// blockpack-internal block, guaranteed. A tight MaxBlocks budget then reads only the K NEWEST
// blocks (K < nSpans), each corresponding to exactly one distinct trace, so bounded vs. unbounded
// diverge in trace COUNT, not merely internal ordering — an observable, fixture-size-independent
// signal, unlike relying on QueryStats/ExecutionPath (which carries no bounded-vs-unbounded
// marker at all).
func writeRecentFirstMultiBlockFilterFixture(t *testing.T, nSpans int, svc string) *blockpackBlock {
	t.Helper()
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	traces := make([]*tempopb.Trace, nSpans)
	ids := make([][]byte, nSpans)
	for i := 0; i < nSpans; i++ {
		traceID := make([]byte, 16)
		traceID[0] = 0x7A
		traceID[14] = byte(i >> 8)                                   //nolint:gosec
		traceID[15] = byte(i)                                        //nolint:gosec
		spanID := []byte{0x7B, byte(i >> 8), byte(i), 0, 0, 0, 0, 1} //nolint:gosec
		startNano := now + uint64(i)*uint64(time.Second)             // strictly increasing: span i is older than i+1
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
	meta.EndTime = time.Now().Add(time.Duration(nSpans+1) * time.Second)
	cfg := &common.BlockConfig{Blockpack: common.BlockpackConfig{MaxSpansPerBlock: 1}}

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return newBackendBlock(resultMeta, backend.NewReader(rawR))
}

// writeRecentFirstMultiBlockChainFixture writes nChains INDEPENDENT 2-node structural chains
// (root in svc-root, child in svc-leaf, child.ParentSpanId = root.SpanId), each chain's root/child
// pair strictly newer than the previous chain's, via MaxSpansPerBlock=1 (one blockpack-internal
// block per span, 2*nChains blocks total). A tight MaxBlocks budget (e.g. 2) then reads only the
// newest chain's root+child pair in full — completing exactly that ONE chain — while every older
// chain is wholesale-excluded (R15: a trace missing either half of a chain within the read window
// is incomplete, never partially reported). An unbounded scan completes ALL nChains chains. This
// makes bounded-vs-unbounded diverge in MATCHED CHAIN COUNT, mirroring
// writeRecentFirstMultiBlockFilterFixture's discriminating-signal design for the structural path.
func writeRecentFirstMultiBlockChainFixture(t *testing.T, nChains int) *blockpackBlock {
	t.Helper()
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	now := uint64(time.Now().UnixNano())
	mkAttr := func(k, v string) *tempocommon.KeyValue {
		return &tempocommon.KeyValue{Key: k, Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}}
	}

	var traces []*tempopb.Trace
	var ids [][]byte
	for c := 0; c < nChains; c++ {
		traceID := make([]byte, 16)
		traceID[0] = 0x7C
		traceID[14] = byte(c >> 8)                                        //nolint:gosec
		traceID[15] = byte(c)                                             //nolint:gosec
		rootSpanID := []byte{0x7D, byte(c >> 8), byte(c), 0, 0, 0, 0, 1}  //nolint:gosec
		childSpanID := []byte{0x7D, byte(c >> 8), byte(c), 0, 0, 0, 0, 2} //nolint:gosec
		// Chain c's pair occupies [now + 2c, now + 2c+1) seconds — strictly newer than chain c-1's.
		rootStart := now + uint64(2*c)*uint64(time.Second)
		childStart := now + uint64(2*c+1)*uint64(time.Second)

		traces = append(traces,
			&tempopb.Trace{ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-root")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: rootSpanID, Name: "root-op",
					StartTimeUnixNano: rootStart, EndTimeUnixNano: rootStart + 1000,
				}}}},
			}}},
			&tempopb.Trace{ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-leaf")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId: traceID, SpanId: childSpanID, ParentSpanId: rootSpanID, Name: "child-op",
					StartTimeUnixNano: childStart, EndTimeUnixNano: childStart + 1000,
				}}}},
			}}},
		)
		ids = append(ids, traceID, traceID)
	}

	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(time.Duration(2*nChains+1) * time.Second)
	cfg := &common.BlockConfig{Blockpack: common.BlockpackConfig{MaxSpansPerBlock: 1}}

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	return newBackendBlock(resultMeta, backend.NewReader(rawR))
}
