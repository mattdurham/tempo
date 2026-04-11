package vblockpack

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestWALToBackend_Iterator verifies the WAL→backend promotion path:
// AppendTrace → Flush() → Iterator() → CreateBlock → FindTraceByID.
// This catches the bug where Iterator() returned empty after Flush() set writer=nil.
func TestWALToBackend_Iterator(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	walMeta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal, err := createWALBlock(walMeta, tmpDir, 0)
	require.NoError(t, err)

	// Write a trace into the WAL block.
	now := uint64(time.Now().UnixNano())
	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: []*tempotrace.Span{{
					TraceId:           []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
					SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
					Name:              "wal-span",
					StartTimeUnixNano: now,
					EndTimeUnixNano:   now + uint64(50*time.Millisecond),
				}},
			}},
		}},
	}
	startSec := uint32(time.Now().Unix())
	require.NoError(t, wal.AppendTrace(nil, tr, startSec, startSec+1, false))

	// Flush — mirrors what tempodb.CompleteBlockWithBackend does before calling Iterator.
	require.NoError(t, wal.Flush())

	// Iterator must return the trace even after Flush() set writer=nil.
	iter, err := wal.Iterator()
	require.NoError(t, err)
	defer iter.Close()

	id, got, err := iter.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, got, "iterator must return a trace after Flush+Iterator; got nil (empty block)")
	require.NotNil(t, id)

	_, eof, err := iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, eof)

	// Also verify the full promotion path: CreateBlock → FindTraceByID.
	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	wal2Meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	wal2, err := createWALBlock(wal2Meta, t.TempDir(), 0)
	require.NoError(t, err)
	require.NoError(t, wal2.AppendTrace(nil, tr, startSec, startSec+1, false))
	require.NoError(t, wal2.Flush())

	iter2, err := wal2.Iterator()
	require.NoError(t, err)

	blockMeta, err := CreateBlock(ctx, &common.BlockConfig{}, wal2Meta, iter2, r, w)
	require.NoError(t, err)
	require.Equal(t, int64(1), blockMeta.TotalObjects, "block must have 1 trace after WAL promotion")

	block := newBackendBlock(blockMeta, r)
	traceID := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	resp, err := block.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp, "FindTraceByID must find the trace promoted from WAL")
}

func TestRoundTrip_WriteAndReadBlock(t *testing.T) {
	t.Log("Testing write then read roundtrip")

	// Setup
	ctx := context.Background()
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 100 * 1024 * 1024,
	}

	// Create test trace with known data
	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								TraceId:           traceID,
								SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 1},
								Name:              "test-span",
								StartTimeUnixNano: uint64(time.Now().UnixNano()),
								EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond * 100).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	iter := &mockIterator{
		traces: []*tempopb.Trace{trace},
		ids:    [][]byte{traceID},
	}

	// Create temporary backend
	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{
		Path: tempDir,
	})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	t.Log("Writing block...")

	// Write the block
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)
	require.NotNil(t, resultMeta)
	require.Equal(t, int64(1), resultMeta.TotalObjects)
	require.Greater(t, resultMeta.Size_, uint64(0))

	t.Logf("Block written: %d traces, %d bytes", resultMeta.TotalObjects, resultMeta.Size_)

	// Now read the block back
	t.Log("Reading block back...")

	enc := Encoding{}
	block, err := enc.OpenBlock(resultMeta, r)
	require.NoError(t, err)
	require.NotNil(t, block)

	t.Log("Block opened successfully!")

	// Verify the trace can be read back using FindTraceByID
	t.Log("Verifying trace can be retrieved by ID...")
	response, err := block.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, response, "expected to find trace by ID after roundtrip")
	require.NotNil(t, response.Trace, "expected trace in response after roundtrip")

	// Verify the trace has the expected structure
	require.Greater(t, len(response.Trace.ResourceSpans), 0, "expected at least one ResourceSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans), 0, "expected at least one ScopeSpan")
	require.Greater(t, len(response.Trace.ResourceSpans[0].ScopeSpans[0].Spans), 0, "expected at least one Span")

	foundSpan := response.Trace.ResourceSpans[0].ScopeSpans[0].Spans[0]
	require.Equal(t, "test-span", foundSpan.Name, "expected span name to match after roundtrip")

	t.Logf("Roundtrip verified: found trace with span name '%s'", foundSpan.Name)
	t.Log("Roundtrip test completed!")
}

// TestSearchMetadataRoundtrip verifies that Search results contain correct RootServiceName
// and RootTraceName. This catches the bug where buildTraceMetadata omitted these fields,
// causing Grafana trace-list rows to show blank service and span names.
//
// The test input must include resource.service.name and a root span (no ParentSpanId)
// so that SpanMatchesMetadata can derive the expected values. A vacuous test — one that
// uses empty resource attributes — would pass even with the bug present.
func TestSearchMetadataRoundtrip(t *testing.T) {
	ctx := context.Background()

	traceID := []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	now := uint64(time.Now().UnixNano())

	trace := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				Resource: &temporesource.Resource{
					Attributes: []*tempocommon.KeyValue{
						{
							Key: "service.name",
							Value: &tempocommon.AnyValue{
								Value: &tempocommon.AnyValue_StringValue{StringValue: "my-test-service"},
							},
						},
					},
				},
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: []*tempotrace.Span{
							{
								// Root span: no ParentSpanId
								TraceId:           traceID,
								SpanId:            []byte{2, 0, 0, 0, 0, 0, 0, 1},
								Name:              "root-operation",
								StartTimeUnixNano: now,
								EndTimeUnixNano:   now + uint64(200*time.Millisecond),
							},
							{
								// Child span: has ParentSpanId
								TraceId:           traceID,
								SpanId:            []byte{2, 0, 0, 0, 0, 0, 0, 2},
								ParentSpanId:      []byte{2, 0, 0, 0, 0, 0, 0, 1},
								Name:              "child-operation",
								StartTimeUnixNano: now + uint64(10*time.Millisecond),
								EndTimeUnixNano:   now + uint64(150*time.Millisecond),
							},
						},
					},
				},
			},
		},
	}

	iter := &mockIterator{
		traces: []*tempopb.Trace{trace},
		ids:    [][]byte{traceID},
	}

	tmpDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)

	enc := Encoding{}
	block, err := enc.OpenBlock(resultMeta, r)
	require.NoError(t, err)

	resp, err := block.Search(ctx, &tempopb.SearchRequest{Limit: 10}, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Traces, 1, "expected exactly one trace in search results")

	got := resp.Traces[0]
	require.Equal(t, "my-test-service", got.RootServiceName,
		"RootServiceName must be populated from resource.service.name; empty means buildTraceMetadata is broken")
	require.Equal(t, "root-operation", got.RootTraceName,
		"RootTraceName must be the root span name (no ParentSpanId); empty means root detection is broken")
	require.Greater(t, got.DurationMs, uint32(0), "DurationMs must be non-zero")
	require.Greater(t, got.StartTimeUnixNano, uint64(0), "StartTimeUnixNano must be non-zero")
}
