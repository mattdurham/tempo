package vblockpack

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
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

// TestFetch_KindFilter verifies that {kind=client} correctly filters spans by OTLP SpanKind.
func TestFetch_KindFilter(t *testing.T) {
	// Build a block with two traces:
	//   trace A: span with Kind=CLIENT
	//   trace B: span with Kind=SERVER
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceIDA := []byte{0xa, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xa}
	traceIDB := []byte{0xb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xb}
	now := uint64(time.Now().UnixNano())

	traces := []*tempopb.Trace{
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-client"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDA,
						SpanId:            []byte{0xa, 0, 0, 0, 0, 0, 0, 0xa},
						Name:              "client-span",
						Kind:              tempotrace.Span_SPAN_KIND_CLIENT,
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(50*time.Millisecond),
					}},
				}},
			}},
		},
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-server"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDB,
						SpanId:            []byte{0xb, 0, 0, 0, 0, 0, 0, 0xb},
						Name:              "server-span",
						Kind:              tempotrace.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: now + uint64(10*time.Millisecond),
						EndTimeUnixNano:   now + uint64(60*time.Millisecond),
					}},
				}},
			}},
		},
	}

	meta := backend.NewBlockMeta("single-tenant", uuid.New(), VersionString)
	meta.TotalRecords = 1

	iter := &mockIterator{traces: traces, ids: [][]byte{traceIDA, traceIDB}}
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)

	// Query for kind=client — should return only trace A.
	conditions := []traceql.Condition{
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicKind), Op: traceql.OpEqual, Operands: []traceql.Static{traceql.NewStaticKind(traceql.KindClient)}},
	}
	spansets := collectFetch(t, blk, conditions, false)
	require.Len(t, spansets, 1, "kind=client should match exactly 1 trace (svc-client)")
	require.Equal(t, "svc-client", spansets[0].RootServiceName)

	// Query for kind=server — should return only trace B.
	conditions = []traceql.Condition{
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicKind), Op: traceql.OpEqual, Operands: []traceql.Static{traceql.NewStaticKind(traceql.KindServer)}},
	}
	spansets = collectFetch(t, blk, conditions, false)
	require.Len(t, spansets, 1, "kind=server should match exactly 1 trace (svc-server)")
	require.Equal(t, "svc-server", spansets[0].RootServiceName)
}

// TestFetch_StatusFilter verifies that {status=error} and {status=ok} correctly filter spans by OTLP status code.
func TestFetch_StatusFilter(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceIDA := []byte{0xc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xc}
	traceIDB := []byte{0xd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xd}
	now := uint64(time.Now().UnixNano())

	traces := []*tempopb.Trace{
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-error"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDA,
						SpanId:            []byte{0xc, 0, 0, 0, 0, 0, 0, 0xc},
						Name:              "error-span",
						Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_ERROR},
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(50*time.Millisecond),
					}},
				}},
			}},
		},
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-ok"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDB,
						SpanId:            []byte{0xd, 0, 0, 0, 0, 0, 0, 0xd},
						Name:              "ok-span",
						Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_OK},
						StartTimeUnixNano: now + uint64(10*time.Millisecond),
						EndTimeUnixNano:   now + uint64(60*time.Millisecond),
					}},
				}},
			}},
		},
	}

	meta := backend.NewBlockMeta("single-tenant", uuid.New(), VersionString)
	meta.TotalRecords = 1

	iter := &mockIterator{traces: traces, ids: [][]byte{traceIDA, traceIDB}}
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)

	// Query for status=error — should return only trace A.
	conditions := []traceql.Condition{
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicStatus), Op: traceql.OpEqual, Operands: []traceql.Static{traceql.NewStaticStatus(traceql.StatusError)}},
	}
	spansets := collectFetch(t, blk, conditions, false)
	require.Len(t, spansets, 1, "status=error should match exactly 1 trace (svc-error)")
	require.Equal(t, "svc-error", spansets[0].RootServiceName)

	// Query for status=ok — should return only trace B.
	conditions = []traceql.Condition{
		{Attribute: traceql.NewIntrinsic(traceql.IntrinsicStatus), Op: traceql.OpEqual, Operands: []traceql.Static{traceql.NewStaticStatus(traceql.StatusOk)}},
	}
	spansets = collectFetch(t, blk, conditions, false)
	require.Len(t, spansets, 1, "status=ok should match exactly 1 trace (svc-ok)")
	require.Equal(t, "svc-ok", spansets[0].RootServiceName)
}

// TestFetch_KindStatusAttributeFor verifies that AttributeFor correctly translates
// OTLP int values for span:kind and span:status into Tempo's traceql enum values.
//
// Root cause of the original bug: OTLP and Tempo use different int orderings for
// Kind (OTLP SERVER=2, CLIENT=3 vs Tempo KindClient=2, KindServer=3) and Status
// (OTLP OK=1, ERROR=2 vs Tempo StatusError=0, StatusOk=1, StatusUnset=2).
// Without explicit conversion, kind/status filtering would return wrong spans.
func TestFetch_KindStatusAttributeFor(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceIDA := []byte{0xe, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xe}
	traceIDB := []byte{0xf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf}
	now := uint64(time.Now().UnixNano())

	traces := []*tempopb.Trace{
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-client"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDA,
						SpanId:            []byte{0xe, 0, 0, 0, 0, 0, 0, 0xe},
						Name:              "client-error-span",
						Kind:              tempotrace.Span_SPAN_KIND_CLIENT,
						Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_ERROR},
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(50*time.Millisecond),
					}},
				}},
			}},
		},
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-server"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceIDB,
						SpanId:            []byte{0xf, 0, 0, 0, 0, 0, 0, 0xf},
						Name:              "server-ok-span",
						Kind:              tempotrace.Span_SPAN_KIND_SERVER,
						Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_OK},
						StartTimeUnixNano: now + uint64(10*time.Millisecond),
						EndTimeUnixNano:   now + uint64(60*time.Millisecond),
					}},
				}},
			}},
		},
	}

	meta := backend.NewBlockMeta("single-tenant", uuid.New(), VersionString)
	meta.TotalRecords = 1

	iter := &mockIterator{traces: traces, ids: [][]byte{traceIDA, traceIDB}}
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)

	// Fetch all spans with no filter and check AttributeFor returns the right Tempo enum values.
	spansets := collectFetch(t, blk, nil, false)
	require.Len(t, spansets, 2)

	byService := map[string]*traceql.Spanset{}
	for _, ss := range spansets {
		byService[ss.RootServiceName] = ss
	}

	// Verify svc-client span: OTLP CLIENT (3) → Tempo KindClient (2), OTLP ERROR (2) → Tempo StatusError (0)
	clientSS := byService["svc-client"]
	require.NotNil(t, clientSS)
	require.Len(t, clientSS.Spans, 1)
	clientSpan := clientSS.Spans[0]

	kindVal, ok := clientSpan.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicKind))
	require.True(t, ok, "span:kind should be present on client span")
	kind, ok := kindVal.Kind()
	require.True(t, ok)
	require.Equal(t, traceql.KindClient, kind, "OTLP CLIENT=3 must map to Tempo KindClient=2")

	statusVal, ok := clientSpan.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicStatus))
	require.True(t, ok, "span:status should be present on error span")
	status, ok := statusVal.Status()
	require.True(t, ok)
	require.Equal(t, traceql.StatusError, status, "OTLP ERROR=2 must map to Tempo StatusError=0")

	// Verify svc-server span: OTLP SERVER (2) → Tempo KindServer (3), OTLP OK (1) → Tempo StatusOk (1)
	serverSS := byService["svc-server"]
	require.NotNil(t, serverSS)
	require.Len(t, serverSS.Spans, 1)
	serverSpan := serverSS.Spans[0]

	kindVal, ok = serverSpan.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicKind))
	require.True(t, ok, "span:kind should be present on server span")
	kind, ok = kindVal.Kind()
	require.True(t, ok)
	require.Equal(t, traceql.KindServer, kind, "OTLP SERVER=2 must map to Tempo KindServer=3")

	statusVal, ok = serverSpan.AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicStatus))
	require.True(t, ok, "span:status should be present on ok span")
	status, ok = statusVal.Status()
	require.True(t, ok)
	require.Equal(t, traceql.StatusOk, status, "OTLP OK=1 must map to Tempo StatusOk=1")
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

// TestFetch_TimeRangeSkip verifies that Fetch returns empty results immediately when the
// query time range falls entirely outside the block's actual span timestamps.
// This exercises the r.BlocksInTimeRange early-return path in Fetch().
//
// The block is created with spans at current time. A query window ending 1 year ago
// has no overlap — BlocksInTimeRange should return a non-nil empty slice, triggering
// the early return with an empty iterator.
func TestFetch_TimeRangeSkip(t *testing.T) {
	block, resultMeta := createFetchTestBlock(t)

	// Sanity check: block has a real time range set by setBlockTimeRange.
	require.False(t, resultMeta.StartTime.IsZero(), "block StartTime must be set by setBlockTimeRange")
	require.False(t, resultMeta.EndTime.IsZero(), "block EndTime must be set by setBlockTimeRange")

	ctx := context.Background()

	// Query window: 1 year ago to 6 months ago — entirely before the block's spans.
	oneYearAgo := uint64(time.Now().Add(-365 * 24 * time.Hour).UnixNano())
	sixMonthsAgo := uint64(time.Now().Add(-180 * 24 * time.Hour).UnixNano())

	req := traceql.FetchSpansRequest{
		Conditions:         nil,
		StartTimeUnixNanos: oneYearAgo,
		EndTimeUnixNanos:   sixMonthsAgo,
	}

	resp, err := block.Fetch(ctx, req, common.SearchOptions{})
	require.NoError(t, err)
	defer resp.Results.Close()

	var results []*traceql.Spanset
	for {
		ss, err := resp.Results.Next(ctx)
		require.NoError(t, err)
		if ss == nil {
			break
		}
		results = append(results, ss)
	}

	require.Empty(t, results, "time-range skip: block with future spans must return empty when queried in the past")
}

// TestFetch_BoolAttrConvertedToString verifies that boolean span attributes are
// returned as string "true"/"false" in AllAttributesFunc, not as Go bool.
// Prevents a panic in Grafana's Tempo datasource plugin when mixed bool/string
// attribute values appear across spans (data frame column type must be uniform).
func TestFetch_BoolAttrConvertedToString(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceID := []byte{0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x20}
	now := uint64(time.Now().UnixNano())

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-bool-test"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x20, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "span-with-bool",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + 1000,
					Attributes: []*tempocommon.KeyValue{
						{Key: "grpc.wait_for_ready", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BoolValue{BoolValue: true}}},
					},
				}},
			}},
		}},
	}

	meta := &backend.BlockMeta{
		BlockID:  backend.NewUUID(),
		TenantID: "test",
		Version:  VersionString,
	}
	meta.TotalRecords = 1

	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)
	spansets := collectFetch(t, blk, nil, false)
	require.Len(t, spansets, 1)

	boolAttr := traceql.NewAttribute("grpc.wait_for_ready")
	for _, ss := range spansets {
		for _, span := range ss.Spans {
			attrs := span.AllAttributes()
			if v, ok := attrs[boolAttr]; ok {
				// Must be string type — prevents Grafana data frame type panic.
				require.Equal(t, traceql.TypeString, v.Type,
					"bool span attribute must be converted to string for Grafana compatibility")
			}
		}
	}
}

// TestFetch_AllAttributesReturnsAllTypes verifies that AllAttributes returns
// string, int, and float span attributes with correct types, and that booleans
// are always returned as strings (preventing Grafana data frame type panics).
func TestFetch_AllAttributesReturnsAllTypes(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceID := []byte{0x21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x21}
	now := uint64(time.Now().UnixNano())

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-types"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x21, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "typed-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + 1000,
					Attributes: []*tempocommon.KeyValue{
						{Key: "str.attr", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "hello"}}},
						{Key: "int.attr", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: 42}}},
						{Key: "float.attr", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_DoubleValue{DoubleValue: 3.14}}},
						{Key: "bool.true", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BoolValue{BoolValue: true}}},
						{Key: "bool.false", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BoolValue{BoolValue: false}}},
					},
				}},
			}},
		}},
	}

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: "test", Version: VersionString}
	meta.TotalRecords = 1
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	resultMeta, err := CreateBlock(ctx, &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)

	strAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "str.attr")
	intAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "int.attr")
	floatAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "float.attr")
	boolTrueAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "bool.true")
	boolFalseAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "bool.false")

	// Populate conditions so that requestedAttrs is non-empty and AllAttributesFunc
	// actually exercises the type-conversion logic for each attribute.
	conditions := []traceql.Condition{
		{Attribute: strAttr},
		{Attribute: intAttr},
		{Attribute: floatAttr},
		{Attribute: boolTrueAttr},
		{Attribute: boolFalseAttr},
	}
	spansets := collectFetch(t, blk, conditions, false)
	require.Len(t, spansets, 1)
	require.Len(t, spansets[0].Spans, 1)

	attrs := spansets[0].Spans[0].AllAttributes()

	v, ok := attrs[strAttr]
	require.True(t, ok, "str.attr must be present in AllAttributes")
	require.Equal(t, traceql.TypeString, v.Type, "string attr must be TypeString")

	v, ok = attrs[intAttr]
	require.True(t, ok, "int.attr must be present in AllAttributes")
	require.Equal(t, traceql.TypeInt, v.Type, "int attr must be TypeInt")

	v, ok = attrs[floatAttr]
	require.True(t, ok, "float.attr must be present in AllAttributes")
	require.Equal(t, traceql.TypeFloat, v.Type, "float attr must be TypeFloat")

	v, ok = attrs[boolTrueAttr]
	require.True(t, ok, "bool.true must be present in AllAttributes")
	require.Equal(t, traceql.TypeString, v.Type, "bool=true must be TypeString")
	require.Equal(t, "true", v.EncodeToString(false))

	v, ok = attrs[boolFalseAttr]
	require.True(t, ok, "bool.false must be present in AllAttributes")
	require.Equal(t, traceql.TypeString, v.Type, "bool=false must be TypeString")
	require.Equal(t, "false", v.EncodeToString(false))
}

// TestFetch_AllAttributesLimitedToQueryConditions verifies that AllAttributesFunc
// only returns attributes from the query conditions, not ALL span attributes.
// This matches parquet's selective fetch behavior and prevents Grafana's Tempo
// datasource from panicking on inconsistent attribute sets across spans
// (data frame column type must be uniform: nullable *string vs plain string).
func TestFetch_AllAttributesLimitedToQueryConditions(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceID := []byte{0x22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x22}
	now := uint64(time.Now().UnixNano())

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-selective"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x22, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "span-with-many-attrs",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + 1000,
					Attributes: []*tempocommon.KeyValue{
						{Key: "http.method", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "GET"}}},
						{Key: "http.status_code", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: 200}}},
						{Key: "db.system", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "postgres"}}},
					},
				}},
			}},
		}},
	}

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: "test", Version: VersionString}
	meta.TotalRecords = 1
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	resultMeta, err := CreateBlock(ctx, &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}, meta, iter, r, w)
	require.NoError(t, err)
	blk := newBackendBlock(resultMeta, r)

	// Query filtering on http.method only — AllAttributes should only return that column.
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{
			{Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method"), Op: traceql.OpEqual, Operands: []traceql.Static{traceql.NewStaticString("GET")}},
		},
	}
	resp, err := blk.Fetch(ctx, req, common.DefaultSearchOptions())
	require.NoError(t, err)

	ss, err := resp.Results.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, ss)
	require.Len(t, ss.Spans, 1)

	attrs := ss.Spans[0].AllAttributes()

	// Use scoped attributes to match what columnNameToAttribute returns for span.* columns.
	httpMethod := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method")
	httpStatus := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.status_code")
	dbSystem := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "db.system")

	// http.method was in conditions — must be present.
	_, hasMethod := attrs[httpMethod]
	require.True(t, hasMethod, "queried attribute http.method must be in AllAttributes")

	// http.status_code and db.system were NOT in conditions — must be absent.
	_, hasStatus := attrs[httpStatus]
	require.False(t, hasStatus, "non-queried http.status_code must NOT be in AllAttributes — prevents Grafana data frame type panic")
	_, hasDB := attrs[dbSystem]
	require.False(t, hasDB, "non-queried db.system must NOT be in AllAttributes")
}

// TestFetch_SetsAttributeMatched verifies that Fetch() sets the attributeMatched
// attribute on every returned spanset so that asTraceSearchMetadata can populate
// SpanSet.Matched in search responses.
func TestFetch_SetsAttributeMatched(t *testing.T) {
	block, _ := createFetchTestBlock(t)
	spansets := collectFetch(t, block, nil, false)
	require.NotEmpty(t, spansets, "expected at least one spanset")

	for _, ss := range spansets {
		var matchedAttr *traceql.SpansetAttribute
		for _, att := range ss.Attributes {
			if att.Name == traceql.AttributeMatched {
				matchedAttr = att
				break
			}
		}
		require.NotNil(t, matchedAttr, "spanset missing attributeMatched")
		n, ok := matchedAttr.Val.Int()
		require.True(t, ok, "attributeMatched value must be int")
		require.GreaterOrEqual(t, n, len(ss.Spans), "attributeMatched must be >= returned span count")
		require.Greater(t, n, 0, "span count must be > 0")
	}
}

// staticFieldsProvider is a test-only SpanFieldsProvider that serves a fixed map of fields.
// It lets us construct a blockpackSpan with fully controlled column content without going
// through the full write→read pipeline — making unit tests for AllAttributesFunc fast,
// explicit, and independent of blockpack internals.
type staticFieldsProvider struct {
	fields map[string]any
}

func (s *staticFieldsProvider) GetField(name string) (any, bool) {
	v, ok := s.fields[name]
	return v, ok
}

func (s *staticFieldsProvider) IterateFields(fn func(name string, value any) bool) {
	for k, v := range s.fields {
		if !fn(k, v) {
			return
		}
	}
}

// allLeakColumns is the exhaustive list of blockpack columns that Fetch always selects
// (via selectCols in backend_block.go) and that must NOT appear in AllAttributes() when
// they were not part of the query conditions. This is the regression contract: if a new
// always-selected column is added to selectCols, add it here and ensure it is gated.
//
// Columns intentionally omitted from this list:
//   - span:id / span:parent_id / span:start / span:end / trace:id — not exposed by
//     columnNameToAttribute (returns false), so they never reach AllAttributes at all
var allLeakColumns = []struct {
	col  string
	attr traceql.Attribute
	// value must match the Go type that blockpack stores for this column
	value any
	label string
}{
	{
		col:   "span:name",
		attr:  traceql.NewIntrinsic(traceql.IntrinsicName),
		value: "test-span",
		label: "IntrinsicName",
	},
	{
		col:   "span:status",
		attr:  traceql.NewIntrinsic(traceql.IntrinsicStatus),
		value: int64(1), // OTLP STATUS_CODE_OK
		label: "IntrinsicStatus",
	},
	{
		col:   "span:kind",
		attr:  traceql.NewIntrinsic(traceql.IntrinsicKind),
		value: int64(2), // OTLP SPAN_KIND_SERVER
		label: "IntrinsicKind",
	},
	{
		col:   "span:status_message",
		attr:  traceql.NewIntrinsic(traceql.IntrinsicStatusMessage),
		value: "some message",
		label: "IntrinsicStatusMessage",
	},
	{
		col:   "resource.service.name",
		attr:  traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"),
		value: "my-service",
		label: "resource.service.name",
	},
	{
		col:   "span:duration",
		attr:  traceql.NewIntrinsic(traceql.IntrinsicDuration),
		value: uint64(100_000_000), // 100ms in nanoseconds
		label: "IntrinsicDuration",
	},
}

// TestAllAttributes_DirectUnit_NoRequestedAttrsFiltersAll is a pure unit test of
// AllAttributesFunc. It constructs a blockpackSpan directly with all known leak columns
// populated and empty requestedAttrs, then asserts that every leak column is absent from
// the output.
//
// This is the strongest possible regression guard: it does not go through Fetch, blockpack
// I/O, or SelectColumns. If AllAttributesFunc stops gating on requestedAttrs — or if a new
// always-fetched column is added without a gate — this test catches it immediately.
//
// Contract: add any new always-fetched column to allLeakColumns above.
func TestAllAttributes_DirectUnit_NoRequestedAttrsFiltersAll(t *testing.T) {
	// Build field map with ALL leak columns populated (including span:name).
	fields := make(map[string]any, len(allLeakColumns))
	for _, lc := range allLeakColumns {
		fields[lc.col] = lc.value
	}

	sp := &blockpackSpan{
		match: blockpack.SpanMatch{
			TraceID: "00000000000000000000000000000001",
			SpanID:  "0000000000000001",
			Fields:  &staticFieldsProvider{fields: fields},
		},
		// requestedAttrs is nil/empty — simulates a {} match-all query.
	}

	attrs := sp.AllAttributes()

	// Every column must be absent — including IntrinsicName. No attribute appears
	// unless it was explicitly in req.Conditions, matching parquet4 behavior.
	for _, lc := range allLeakColumns {
		_, present := attrs[lc.attr]
		require.False(t, present,
			"%s must NOT appear in AllAttributes when not in requestedAttrs (leak column %q)",
			lc.label, lc.col)
	}
}

// TestAllAttributes_DirectUnit_RequestedAttrsAllowThrough verifies the positive case:
// each leak column appears in AllAttributes when it is explicitly in requestedAttrs.
// Run as a sub-test per column so failures are pinpointed.
func TestAllAttributes_DirectUnit_RequestedAttrsAllowThrough(t *testing.T) {
	for _, lc := range allLeakColumns {
		lc := lc // capture
		t.Run(lc.label, func(t *testing.T) {
			fields := map[string]any{lc.col: lc.value}
			sp := &blockpackSpan{
				match: blockpack.SpanMatch{
					TraceID: "00000000000000000000000000000001",
					SpanID:  "0000000000000001",
					Fields:  &staticFieldsProvider{fields: fields},
				},
				requestedAttrs: map[traceql.Attribute]struct{}{
					lc.attr: {},
				},
			}
			attrs := sp.AllAttributes()
			_, present := attrs[lc.attr]
			require.True(t, present,
				"%s must appear in AllAttributes when it is in requestedAttrs", lc.label)
		})
	}
}

// TestAllAttributes_MatchAllReturnsEmptyAttributes is the end-to-end regression guard.
// It runs a real {} match-all Fetch and asserts that none of the known leak columns
// appear in AllAttributes() on any returned span. Covers the full Fetch→blockpackSpan
// pipeline — if selectCols in Fetch gains a new always-included column that isn't gated
// in AllAttributesFunc, this test breaks.
func TestAllAttributes_MatchAllReturnsEmptyAttributes(t *testing.T) {
	block, _ := createFetchTestBlock(t)
	spansets := collectFetch(t, block, nil, false)
	require.Len(t, spansets, 2, "expected 2 spansets from createFetchTestBlock")

	for _, ss := range spansets {
		for _, sp := range ss.Spans {
			attrs := sp.AllAttributes()

			// Check every known leak column explicitly. If you add a new always-fetched
			// column to selectCols in backend_block.go, add it to allLeakColumns above
			// and ensure AllAttributesFunc gates it.
			for _, lc := range allLeakColumns {
				_, present := attrs[lc.attr]
				require.False(t, present,
					"traceID=%x: %s must not appear in AllAttributes for match-all {} query — "+
						"add a requestedAttrs gate in AllAttributesFunc for column %q",
					ss.TraceID, lc.label, lc.col)
			}
		}
	}
}

// TestAllAttributes_QueriedAttributesAppear verifies that when a query conditions on
// span.http.method, that attribute DOES appear in AllAttributes() on the matched span,
// and that non-queried leak columns remain absent even in a filtered query.
func TestAllAttributes_QueriedAttributesAppear(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{{
		Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method"),
		Op:        traceql.OpEqual,
		Operands:  traceql.Operands{traceql.NewStaticString("GET")},
	}}
	spansets := collectFetch(t, block, conditions, true)
	require.Len(t, spansets, 1)

	sp := spansets[0].Spans[0]
	attrs := sp.AllAttributes()

	// Queried attribute must be present.
	httpAttr := traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "http.method")
	v, ok := attrs[httpAttr]
	require.True(t, ok, "http.method must appear in AllAttributes when it was in query conditions")
	require.Equal(t, "GET", v.EncodeToString(false), "http.method value must be 'GET'")

	// Leak columns must remain absent even when other conditions are present.
	for _, lc := range allLeakColumns {
		_, present := attrs[lc.attr]
		require.False(t, present,
			"%s must NOT appear in AllAttributes even in filtered query — only queried attrs allowed (column %q)",
			lc.label, lc.col)
	}
}

// TestAllAttributes_QueriedIntrinsicAppears verifies that when a query conditions on
// IntrinsicStatus, it appears in AllAttributes() on the matched span.
func TestAllAttributes_QueriedIntrinsicAppears(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceID := []byte{0x30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x30}
	now := uint64(time.Now().UnixNano())

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-err"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x30, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "err-span",
					Status:            &tempotrace.Status{Code: tempotrace.Status_STATUS_CODE_ERROR},
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(10*time.Millisecond),
				}},
			}},
		}},
	}

	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: "test", Version: VersionString}
	meta.TotalRecords = 1
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}
	resultMeta, err := CreateBlock(ctx, &common.BlockConfig{}, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)

	conditions := []traceql.Condition{{
		Attribute: traceql.NewIntrinsic(traceql.IntrinsicStatus),
		Op:        traceql.OpEqual,
		Operands:  traceql.Operands{traceql.NewStaticStatus(traceql.StatusError)},
	}}
	spansets := collectFetch(t, blk, conditions, true)
	require.Len(t, spansets, 1)

	sp := spansets[0].Spans[0]
	attrs := sp.AllAttributes()

	// IntrinsicStatus was queried — must appear.
	statusAttr := traceql.NewIntrinsic(traceql.IntrinsicStatus)
	v, ok := attrs[statusAttr]
	require.True(t, ok, "IntrinsicStatus must appear in AllAttributes when it was in query conditions")
	st, ok := v.Status()
	require.True(t, ok, "AllAttributes value for IntrinsicStatus must be Status type")
	require.Equal(t, traceql.StatusError, st, "status value must be StatusError")

	// Other leak columns must not appear just because status was queried.
	kindAttr := traceql.NewIntrinsic(traceql.IntrinsicKind)
	_, hasKind := attrs[kindAttr]
	require.False(t, hasKind, "IntrinsicKind must NOT appear when only IntrinsicStatus was queried")

	svcAttr := traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name")
	_, hasSvc := attrs[svcAttr]
	require.False(t, hasSvc, "resource.service.name must NOT appear when only IntrinsicStatus was queried")

	// IntrinsicName must NOT appear when it was not in the query conditions.
	nameAttr := traceql.NewIntrinsic(traceql.IntrinsicName)
	_, hasName := attrs[nameAttr]
	require.False(t, hasName, "IntrinsicName must NOT appear when it was not in query conditions")
}

// TestAllAttributes_QueriedNameAppears verifies that IntrinsicName appears in AllAttributes
// when it is explicitly included in query conditions — same rule as all other attributes.
func TestAllAttributes_QueriedNameAppears(t *testing.T) {
	block, _ := createFetchTestBlock(t)

	conditions := []traceql.Condition{{
		Attribute: traceql.NewIntrinsic(traceql.IntrinsicName),
		Op:        traceql.OpNone,
	}}
	spansets := collectFetch(t, block, conditions, false)
	require.NotEmpty(t, spansets)

	nameAttr := traceql.NewIntrinsic(traceql.IntrinsicName)
	for _, ss := range spansets {
		for _, sp := range ss.Spans {
			attrs := sp.AllAttributes()
			_, ok := attrs[nameAttr]
			require.True(t, ok, "IntrinsicName must appear in AllAttributes when it was in query conditions")
		}
	}
}

// TestWALFindTraceByID verifies that walBlock.FindTraceByID finds and returns
// a stored trace after sealCurrent seals the active dirty segment.
func TestWALFindTraceByID(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	traceID := []byte{0x40, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x40}
	now := uint64(time.Now().UnixNano())

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-wal"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x40, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "wal-findtrace-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(50*time.Millisecond),
				}},
			}},
		}},
	}

	startSec := uint32(time.Now().Unix())
	require.NoError(t, wal.AppendTrace(traceID, tr, startSec, startSec+1, false))

	resp, err := wal.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp, "FindTraceByID must return non-nil response for a stored trace")
	require.NotNil(t, resp.Trace, "FindTraceByID response must contain a non-nil Trace")
	require.NotEmpty(t, resp.Trace.ResourceSpans, "returned trace must have at least one ResourceSpans")
}

// TestWALFindTraceByID_SecondCallNotNil is a regression test for the "works once,
// 404 on refresh" bug: snapshot() was calling buf.Reset() + writer re-init on every
// call, discarding accumulated data so the second FindTraceByID returned nil.
func TestWALFindTraceByID_SecondCallNotNil(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	traceID := []byte{0x41, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x41}
	now := uint64(time.Now().UnixNano())

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-wal-refresh"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x41, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "wal-refresh-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(50*time.Millisecond),
				}},
			}},
		}},
	}

	startSec := uint32(time.Now().Unix())
	require.NoError(t, wal.AppendTrace(traceID, tr, startSec, startSec+1, false))

	// First call — must succeed.
	resp1, err := wal.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp1, "first FindTraceByID must return non-nil")
	require.NotNil(t, resp1.Trace)

	// Second call without any new appends — must still return the same trace.
	// This was the bug: snapshot() called buf.Reset(), so the second call returned nil.
	resp2, err := wal.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp2, "second FindTraceByID (refresh) must return non-nil — regression guard")
	require.NotNil(t, resp2.Trace)
	require.NotEmpty(t, resp2.Trace.ResourceSpans, "second call must return trace with spans")
}

// TestFetch_SpanCapAtDefaultSpansPerSpanSet verifies that Fetch() caps the returned
// spans array at traceql.DefaultSpansPerSpanSet (3) while keeping AttributeMatched
// equal to the true total — matching standard Tempo search behaviour and preventing
// oversized payloads from reaching the Grafana UI.
func TestFetch_SpanCapAtDefaultSpansPerSpanSet(t *testing.T) {
	const totalSpans = 5 // deliberately > DefaultSpansPerSpanSet (3)

	ctx := context.Background()
	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	traceID := []byte{0xe, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xe}
	now := uint64(time.Now().UnixNano())

	spans := make([]*tempotrace.Span, totalSpans)
	for i := range spans {
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            []byte{0xe, 0, 0, 0, 0, 0, 0, byte(i + 1)},
			Name:              "span",
			StartTimeUnixNano: now + uint64(i)*uint64(time.Millisecond),
			EndTimeUnixNano:   now + uint64(i)*uint64(time.Millisecond) + uint64(10*time.Millisecond),
		}
	}

	traces := []*tempopb.Trace{
		{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-many"}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: spans}},
			}},
		},
	}

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	iter := &mockIterator{traces: traces, ids: [][]byte{traceID}}
	cfg := &common.BlockConfig{}
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	blk := newBackendBlock(resultMeta, r)
	spansets := collectFetch(t, blk, nil, false)

	require.Len(t, spansets, 1, "expected 1 spanset")
	ss := spansets[0]

	// Spans array must be capped at DefaultSpansPerSpanSet.
	require.LessOrEqual(t, len(ss.Spans), traceql.DefaultSpansPerSpanSet,
		"returned spans must be capped at DefaultSpansPerSpanSet (%d), got %d",
		traceql.DefaultSpansPerSpanSet, len(ss.Spans))

	// AttributeMatched must reflect the true total, not the capped count.
	var matchedAttr *traceql.SpansetAttribute
	for _, att := range ss.Attributes {
		if att.Name == traceql.AttributeMatched {
			matchedAttr = att
			break
		}
	}
	require.NotNil(t, matchedAttr, "spanset missing AttributeMatched")
	n, ok := matchedAttr.Val.Int()
	require.True(t, ok, "AttributeMatched value must be int")
	require.Equal(t, totalSpans, n,
		"AttributeMatched must equal total span count (%d), not capped count (%d)",
		totalSpans, len(ss.Spans))
}

// TestWALFetch_SpanCapAtDefaultSpansPerSpanSet mirrors TestFetch_SpanCapAtDefaultSpansPerSpanSet
// for walBlock.Fetch. Verifies that the WAL path caps returned spans at DefaultSpansPerSpanSet
// and sets AttributeMatched to the uncapped total.
func TestWALFetch_SpanCapAtDefaultSpansPerSpanSet(t *testing.T) {
	const totalSpans = 5 // deliberately > DefaultSpansPerSpanSet (3)

	ctx := context.Background()
	tmpDir := t.TempDir()
	traceID := []byte{0xf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf}
	now := uint64(time.Now().UnixNano())

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	block, err := createWALBlock(meta, tmpDir, 0)
	require.NoError(t, err)

	spans := make([]*tempotrace.Span, totalSpans)
	for i := range spans {
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            []byte{0xf, 0, 0, 0, 0, 0, 0, byte(i + 1)},
			Name:              "wal-span",
			StartTimeUnixNano: now + uint64(i)*uint64(time.Millisecond),
			EndTimeUnixNano:   now + uint64(i)*uint64(time.Millisecond) + uint64(10*time.Millisecond),
		}
	}
	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "wal-svc"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{Spans: spans}},
		}},
	}
	require.NoError(t, block.AppendTrace(traceID, tr, 0, 0, false))

	req := traceql.FetchSpansRequest{}
	resp, err := block.Fetch(ctx, req, common.DefaultSearchOptions())
	require.NoError(t, err)
	defer resp.Results.Close()

	ss, err := resp.Results.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, ss, "expected at least one spanset from WAL Fetch")

	// Spans must be capped at DefaultSpansPerSpanSet.
	require.LessOrEqual(t, len(ss.Spans), traceql.DefaultSpansPerSpanSet,
		"WAL Fetch: returned spans must be capped at DefaultSpansPerSpanSet (%d), got %d",
		traceql.DefaultSpansPerSpanSet, len(ss.Spans))

	// AttributeMatched must reflect the true total.
	var matchedAttr *traceql.SpansetAttribute
	for _, att := range ss.Attributes {
		if att.Name == traceql.AttributeMatched {
			matchedAttr = att
			break
		}
	}
	require.NotNil(t, matchedAttr, "WAL spanset missing AttributeMatched")
	n, ok := matchedAttr.Val.Int()
	require.True(t, ok, "AttributeMatched value must be int")
	require.Equal(t, totalSpans, n,
		"WAL AttributeMatched must equal total span count (%d), not capped count (%d)",
		totalSpans, len(ss.Spans))
}

// TestWALFindTraceByID_MultiSegment verifies that FindTraceByID finds traces across multiple
// sealed segments. The test appends trace A, triggers a seal via FindTraceByID (which calls
// sealCurrent), appends traces B and C, then verifies all three are findable.
func TestWALFindTraceByID_MultiSegment(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	traceIDA := []byte{0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42}
	traceIDB := []byte{0x43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x43}
	traceIDC := []byte{0x44, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x44}
	now := uint64(time.Now().UnixNano())

	mkTrace := func(traceID []byte, spanID byte, svcName, spanName string) *tempopb.Trace {
		return &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: svcName}}},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{{
					Spans: []*tempotrace.Span{{
						TraceId:           traceID,
						SpanId:            []byte{spanID, 0, 0, 0, 0, 0, 0, 0x01},
						Name:              spanName,
						StartTimeUnixNano: now,
						EndTimeUnixNano:   now + uint64(50*time.Millisecond),
					}},
				}},
			}},
		}
	}

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	startSec := uint32(time.Now().Unix())

	// Append trace A — segment 0 (dirty)
	require.NoError(t, wal.AppendTrace(traceIDA, mkTrace(traceIDA, 0x42, "svc-a", "span-a"), startSec, startSec+1, false))

	// FindTraceByID seals current dirty segment into flushed[0]
	respA1, err := wal.FindTraceByID(ctx, traceIDA, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, respA1, "trace A must be findable after first seal")

	// Append traces B and C — they go into a new dirty segment
	require.NoError(t, wal.AppendTrace(traceIDB, mkTrace(traceIDB, 0x43, "svc-b", "span-b"), startSec, startSec+1, false))
	require.NoError(t, wal.AppendTrace(traceIDC, mkTrace(traceIDC, 0x44, "svc-c", "span-c"), startSec, startSec+1, false))

	// FindTraceByID for B seals new segment into flushed[1]; now two segments exist
	respB, err := wal.FindTraceByID(ctx, traceIDB, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, respB, "trace B must be findable from second segment")
	require.NotNil(t, respB.Trace)
	require.NotEmpty(t, respB.Trace.ResourceSpans)

	// Trace A must still be visible (it lives in flushed[0])
	respA2, err := wal.FindTraceByID(ctx, traceIDA, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, respA2, "trace A must still be findable from first segment after second seal")
	require.NotNil(t, respA2.Trace)
	require.NotEmpty(t, respA2.Trace.ResourceSpans)

	// Trace C must also be visible
	respC, err := wal.FindTraceByID(ctx, traceIDC, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, respC, "trace C must be findable")
	require.NotNil(t, respC.Trace)
	require.NotEmpty(t, respC.Trace.ResourceSpans)
}

// TestWALConcurrentAppendAndFind validates lock discipline and flushed-slice safety
// under concurrent AppendTrace + FindTraceByID. Run with -race to detect data races.
func TestWALConcurrentAppendAndFind(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	traceID := []byte{0x50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x50}
	now := uint64(time.Now().UnixNano())

	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-concurrent"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x50, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "concurrent-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(50*time.Millisecond),
				}},
			}},
		}},
	}

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	walBlk, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	startSec := uint32(time.Now().Unix())

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = walBlk.AppendTrace(traceID, tr, startSec, startSec+1, false)
		}()
		go func() {
			defer wg.Done()
			_, _ = walBlk.FindTraceByID(ctx, traceID, common.SearchOptions{})
		}()
	}
	wg.Wait()
}

// TestWALFindTraceByID_AfterFlushToDisk verifies that FindTraceByID and Fetch
// still return data after walBlock.Flush() writes all segments to disk and
// clears the in-memory flushed[] slice.
func TestWALFindTraceByID_AfterFlushToDisk(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	traceID := []byte{0x60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x60}
	now := uint64(time.Now().UnixNano())
	nowSec := uint32(time.Now().Unix())

	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{Key: "service.name", Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-disk"}}},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           traceID,
					SpanId:            []byte{0x60, 0, 0, 0, 0, 0, 0, 0x01},
					Name:              "disk-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(50*time.Millisecond),
				}},
			}},
		}},
	}

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	require.NoError(t, wal.AppendTrace(traceID, tr, nowSec, nowSec+1, false))

	// Flush to disk — this sets flushed=nil, writer=nil, buf=nil.
	require.NoError(t, wal.Flush())

	// FindTraceByID must still return the trace from disk.
	resp, err := wal.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp, "FindTraceByID must find trace after Flush() to disk")
	require.NotNil(t, resp.Trace)
	require.NotEmpty(t, resp.Trace.ResourceSpans)

	// Second call must also return the trace (idempotent disk reads).
	resp2, err := wal.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp2, "FindTraceByID second call must also find trace after Flush()")

	// Fetch (search) must also return the trace from disk.
	fetchReq := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{},
		StartTimeUnixNanos: now - uint64(time.Minute),
		EndTimeUnixNanos:   now + uint64(time.Minute),
	}
	fetchResp, err := wal.Fetch(ctx, fetchReq, common.SearchOptions{})
	require.NoError(t, err)
	var found bool
	for {
		ss, err := fetchResp.Results.Next(ctx)
		require.NoError(t, err)
		if ss == nil {
			break
		}
		found = true
	}
	require.True(t, found, "Fetch must return spans after Flush() to disk")
}
