package vblockpack

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

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
