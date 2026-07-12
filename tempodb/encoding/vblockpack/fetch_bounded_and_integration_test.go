package vblockpack

// fetch_bounded_and_integration_test.go — Phase 6 (plan-scan-fallback.md) integration-style
// test: a real limit-bearing FILTER query with a genuine multi-leaf AND, low-selectivity shape
// (every trace matches both conditions), through the REAL blockpackBlock.Fetch call chain (not
// a hand-constructed QueryOptions/tryIndexFetch call) -- confirming the whole filter-path
// dispatch rewiring (tryIndexFetch -> BuildValueIndexSourceBounded -> vibuilder's Phase 4
// anchor+confirm resolution) produces the exact newest-limit answer end to end, through a real
// write path (CreateBlock + withVISink) rather than a hand-built value-index fixture.

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeMultiLeafANDFixture writes n traces, each matching BOTH resource.service.name="svc-a"
// AND span.http.method="GET" (an unselective, every-trace-matches AND shape -- the exact query
// shape Phase 4's anchor+confirm design must resolve correctly), at n distinct, consecutive
// StartTimeUnixNano values so a newest-limit comparison is unambiguous. Real write path
// (CreateBlock), so the process-level value-index sink (withVISink, set by the caller) emits
// real VI files for both columns.
func writeMultiLeafANDFixture(t *testing.T, dir string, n int) *backend.BlockMeta {
	t.Helper()
	const tenant = "test-tenant"
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	mkAttr := func(k, v string) *tempocommon.KeyValue {
		return &tempocommon.KeyValue{Key: k, Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}}
	}

	// The value index floors TimeSec to 60-second (minute) granularity on write
	// (floorToMinuteSec's write-side counterpart) -- entries spaced by mere seconds would all
	// collapse into the SAME minute-floored group, making newest-first ordering
	// indistinguishable within that one group. Floor `now` to a minute boundary first, then
	// space entries by a full 2 minutes each so every trace lands in its OWN, deterministically
	// distinct group.
	nowNano := uint64(time.Now().UnixNano())
	const nanosPerMinute = 60 * uint64(time.Second)
	flooredNow := (nowNano / nanosPerMinute) * nanosPerMinute
	traces := make([]*tempopb.Trace, n)
	ids := make([][]byte, n)
	for i := 0; i < n; i++ {
		tid := []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		ids[i] = tid
		startNano := flooredNow + uint64(i)*2*nanosPerMinute
		traces[i] = &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-a")}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId:           tid,
					SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 1},
					Name:              "op",
					StartTimeUnixNano: startNano,
					EndTimeUnixNano:   startNano + uint64(10*time.Millisecond),
					Attributes:        []*tempocommon.KeyValue{mkAttr("http.method", "GET")},
				}}}},
			}},
		}
	}

	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta(tenant, uuid.New(), VersionString)
	meta.StartTime = time.Now().Add(-2 * time.Minute)
	meta.EndTime = time.Now().Add(time.Duration(n+2) * 2 * time.Minute) // covers every trace's 2-minute-spaced startNano
	resultMeta, err := CreateBlock(context.Background(), &common.BlockConfig{}, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	require.Equal(t, int64(n), resultMeta.TotalObjects)
	return resultMeta
}

// TestFetch_MultiLeafAND_WithLimit_ExactNewestLimitAnswer_EndToEnd is Phase 6's required
// integration test: through the real Fetch call chain, a multi-leaf AND query with more true
// matches than the request's limit must return EXACTLY the newest-limit traces, proving Phase
// 4's anchor+confirm resolution is wired correctly end to end (not just at the vibuilder unit
// level -- builder_bounded_and_test.go in the blockpack repo already covers that boundary).
func TestFetch_MultiLeafAND_WithLimit_ExactNewestLimitAnswer_EndToEnd(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	const n = 8
	const limit = 3
	meta := writeMultiLeafANDFixture(t, dir, n)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(
		context.Background(), `{ resource.service.name = "svc-a" && span.http.method = "GET" }`, false,
	)
	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: limit})
	require.NoError(t, err, "a multi-leaf AND query with a limit and real index coverage must answer from the index, not error")

	spansets := drainSpansets(ctx, t, resp)
	require.Len(t, spansets, limit, "must return exactly limit traces, not more or fewer")

	// The n traces were written with StartTimeUnixNano strictly increasing by index (trace i+1
	// is newer than trace i), so the newest `limit` traces are indices n-1, n-2, ..., n-limit.
	gotTraceIDs := make(map[byte]bool, limit)
	for _, ss := range spansets {
		require.NotEmpty(t, ss.TraceID)
		gotTraceIDs[ss.TraceID[0]] = true
	}
	wantTraceIDs := map[byte]bool{}
	for i := n - limit; i < n; i++ {
		wantTraceIDs[byte(i+1)] = true
	}
	assert.Equal(t, wantTraceIDs, gotTraceIDs, "must return exactly the newest-limit traces by time, not an arbitrary same-size subset")
}
