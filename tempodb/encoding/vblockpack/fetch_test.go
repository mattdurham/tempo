package vblockpack

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

// createFetchTestBlock writes a block with two traces:
//   - trace A: resource.service.name="svc-a", span name="get-users", span.http.method="GET"
//   - trace B: resource.service.name="svc-b", span name="post-orders", span.http.method="POST"
func createFetchTestBlock(t *testing.T) (*blockpackBlock, *backend.BlockMeta) {
	t.Helper()

	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceIDA := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	traceIDB := []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	now := uint64(time.Now().UnixNano())

	mkAttr := func(k, v string) *tempocommon.KeyValue {
		return &tempocommon.KeyValue{Key: k, Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}}
	}

	traces := []*tempopb.Trace{
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-a")},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDA,
						SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
						Name:              "get-users",
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(100*time.Millisecond),
						Attributes:        []*tempocommon.KeyValue{mkAttr("http.method", "GET")},
					}},
				}},
			}},
		},
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{mkAttr("service.name", "svc-b")},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDB,
						SpanId:            []byte{2, 0, 0, 0, 0, 0, 0, 2},
						Name:              "post-orders",
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(200*time.Millisecond),
						Attributes:        []*tempocommon.KeyValue{mkAttr("http.method", "POST")},
					}},
				}},
			}},
		},
	}
	ids := [][]byte{traceIDA, traceIDB}

	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	cfg := &common.BlockConfig{}

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)
	require.Equal(t, int64(2), resultMeta.TotalObjects)

	return newBackendBlock(resultMeta, r), resultMeta
}

// collectFetch runs Fetch with the given conditions and returns all spans grouped by trace.
func collectFetch(t *testing.T, block *blockpackBlock, conditions []traceql.Condition, allConditions bool) []*traceql.Spanset {
	t.Helper()
	ctx := context.Background()
	req := traceql.FetchSpansRequest{
		Conditions:    conditions,
		AllConditions: allConditions,
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
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

// TestFetch_MatchAll verifies that an empty condition list (→ "{}") returns all spans.
func TestFetch_MatchAll(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	spansets := collectFetch(t, block, nil, false)

	t.Logf("match-all: got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
		for _, sp := range ss.Spans {
			t.Logf("    spanID=%x", sp.ID())
			v, ok := sp.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicName))
			t.Logf("    name=%v ok=%v", v, ok)
		}
	}

	require.Len(t, spansets, 2, "expected 2 spansets (one per trace)")
}

// TestFetch_BySpanName verifies filtering by span intrinsic name.
func TestFetch_BySpanName(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewIntrinsic(traceql.IntrinsicName),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("get-users")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by name='get-users': got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
		for _, sp := range ss.Spans {
			v, _ := sp.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicName))
			t.Logf("    name=%v", v)
		}
	}

	require.Len(t, spansets, 1, "expected exactly 1 spanset matching name='get-users'")
}

// TestFetch_ByResourceServiceName verifies filtering by resource.service.name.
func TestFetch_ByResourceServiceName(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-a")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by resource.service.name='svc-a': got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
		for _, sp := range ss.Spans {
			v, ok := sp.AttributeFor(traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"))
			t.Logf("    resource.service.name=%v ok=%v", v, ok)
		}
	}

	require.Len(t, spansets, 1, "expected exactly 1 spanset matching resource.service.name='svc-a'")
}

// TestFetch_BySpanAttribute verifies filtering by an explicit span-scoped attribute.
func TestFetch_BySpanAttribute(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("GET")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by span.http.method='GET': got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
		for _, sp := range ss.Spans {
			v, ok := sp.AttributeFor(traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method"))
			t.Logf("    span.http.method=%v ok=%v", v, ok)
		}
	}

	require.Len(t, spansets, 1, "expected exactly 1 spanset matching span.http.method='GET'")
}

// TestFetch_UnscopedAttribute verifies filtering by an unscoped (.attr) attribute.
// Unscoped in Tempo means AttributeScopeNone — the storage layer should check span scope.
func TestFetch_UnscopedAttribute(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeNone, false, "http.method"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("GET")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by .http.method='GET' (unscoped): got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
	}

	// This exposes whether unscoped attributes are handled correctly.
	require.Len(t, spansets, 1, "expected exactly 1 spanset matching .http.method='GET'")
}

// TestFetch_UnscopedServiceName verifies that .service.name (unscoped) finds traces
// stored with resource.service.name — service.name is always a resource attribute.
func TestFetch_UnscopedServiceName(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeNone, false, "service.name"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("svc-a")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by .service.name='svc-a' (unscoped): got %d spanset(s)", len(spansets))
	for _, ss := range spansets {
		t.Logf("  traceID=%x  spans=%d", ss.TraceID, len(ss.Spans))
	}

	require.Len(t, spansets, 1, "expected exactly 1 spanset matching .service.name='svc-a'")
}

// TestFetch_NoMatch verifies that a condition matching nothing returns empty results.
func TestFetch_NoMatch(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{
		{
			Attribute: traceql.NewIntrinsic(traceql.IntrinsicName),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("nonexistent-span")},
		},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("filter by name='nonexistent-span': got %d spanset(s)", len(spansets))
	require.Empty(t, spansets, "expected no spansets for a non-matching filter")
}

// TestFetch_AttributeFor verifies that AttributeFor returns correct values on fetched spans.
func TestFetch_AttributeFor(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	// Fetch all spans.
	spansets := collectFetch(t, block, nil, false)
	require.Len(t, spansets, 2)

	// Collect all span names and service names from fetched spans.
	names := map[string]bool{}
	services := map[string]bool{}

	for _, ss := range spansets {
		for _, sp := range ss.Spans {
			if v, ok := sp.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicName)); ok {
				names[v.EncodeToString(false)] = true
			}
			if v, ok := sp.AttributeFor(traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name")); ok {
				services[v.EncodeToString(false)] = true
			}
		}
	}

	t.Logf("span names found: %v", names)
	t.Logf("service names found: %v", services)

	require.True(t, names["get-users"], "expected to find span name 'get-users'")
	require.True(t, names["post-orders"], "expected to find span name 'post-orders'")
	require.True(t, services["svc-a"], "expected to find resource.service.name='svc-a'")
	require.True(t, services["svc-b"], "expected to find resource.service.name='svc-b'")
}

// TestFetch_OpNoneConditions mirrors the conditions the engine sends for metadata fetching.
// All conditions use OpNone (fetch-only, no filter) — this should return all spans.
func TestFetch_OpNoneConditions(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	// Mimic the SearchMetaConditions the engine adds.
	conditions := []traceql.Condition{
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicTraceRootService), Op: traceql.OpNone},
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicTraceRootSpan), Op: traceql.OpNone},
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicTraceDuration), Op: traceql.OpNone},
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicDuration), Op: traceql.OpNone},
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicName), Op: traceql.OpNone},
	}

	spansets := collectFetch(t, block, conditions, true)

	t.Logf("OpNone-only conditions: got %d spanset(s)", len(spansets))
	require.Len(t, spansets, 2, "OpNone conditions should not filter — expect all 2 traces")
}

// TestFetch_SpansetMetadata verifies that Fetch populates the four trace-level
// metadata fields on each returned Spanset.
func TestFetch_SpansetMetadata(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	// Fetch all spans with no filter.
	spansets := collectFetch(t, block, nil, false)
	require.Len(t, spansets, 2, "expected 2 spansets")

	// Build a map from service name -> spanset for deterministic assertions.
	byService := map[string]*traceql.Spanset{}
	for _, ss := range spansets {
		byService[ss.RootServiceName] = ss
	}

	// Trace A: svc-a, span "get-users", duration ~100ms
	ssA, ok := byService["svc-a"]
	require.True(t, ok, "expected spanset with RootServiceName='svc-a'")
	require.Equal(t, "get-users", ssA.RootSpanName, "RootSpanName for svc-a")
	require.NotZero(t, ssA.StartTimeUnixNanos, "StartTimeUnixNanos for svc-a must be non-zero")
	require.InDelta(t, uint64(100*time.Millisecond), ssA.DurationNanos, float64(2*time.Millisecond),
		"DurationNanos for svc-a should be ~100ms")

	// Trace B: svc-b, span "post-orders", duration ~200ms
	ssB, ok := byService["svc-b"]
	require.True(t, ok, "expected spanset with RootServiceName='svc-b'")
	require.Equal(t, "post-orders", ssB.RootSpanName, "RootSpanName for svc-b")
	require.NotZero(t, ssB.StartTimeUnixNanos, "StartTimeUnixNanos for svc-b must be non-zero")
	require.InDelta(t, uint64(200*time.Millisecond), ssB.DurationNanos, float64(2*time.Millisecond),
		"DurationNanos for svc-b should be ~200ms")
}

// TestFetch_SecondPass_PassThrough verifies that a SecondPass function that returns
// the spanset unchanged results in all spansets being returned, and that SecondPass
// is called exactly once per trace.
func TestFetch_SecondPass_PassThrough(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	callCount := 0
	req := traceql.FetchSpansRequest{
		Conditions:    nil,
		AllConditions: false,
		SecondPass: func(ss *traceql.Spanset) ([]*traceql.Spanset, error) {
			callCount++
			return []*traceql.Spanset{ss}, nil
		},
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
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

	require.Len(t, spansets, 2, "pass-through SecondPass should keep all 2 spansets")
	require.Equal(t, 2, callCount, "SecondPass must be called once per trace (2 traces)")
}

// TestFetch_SecondPass_FilterAll verifies that a SecondPass function that returns
// nil (no spansets) causes all spansets to be discarded.
func TestFetch_SecondPass_FilterAll(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	ctx := context.Background()
	req := traceql.FetchSpansRequest{
		Conditions:    nil,
		AllConditions: false,
		SecondPass: func(ss *traceql.Spanset) ([]*traceql.Spanset, error) {
			return nil, nil // discard every spanset
		},
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
	defer resp.Results.Close()

	ss, err := resp.Results.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, ss, "SecondPass returning nil should discard all spansets; iterator must be empty")
}
