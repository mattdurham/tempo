package vblockpack

import (
	"context"
	"io"
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

// mockIterator provides test traces for CreateBlock
type mockIterator struct {
	traces []*tempopb.Trace
	ids    [][]byte
	index  int
}

func (m *mockIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	if m.index >= len(m.traces) {
		return nil, nil, io.EOF
	}

	id := m.ids[m.index]
	trace := m.traces[m.index]
	m.index++

	return id, trace, nil
}

func (m *mockIterator) Close() {}

// createTestTrace creates a simple test trace
func createTestTrace(traceID []byte, spanCount int) *tempopb.Trace {
	spans := make([]*tempotrace.Span, spanCount)
	for i := 0; i < spanCount; i++ {
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            []byte{byte(i), 0, 0, 0, 0, 0, 0, 1},
			Name:              "test-span",
			StartTimeUnixNano: uint64(time.Now().UnixNano()),
			EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond * 100).UnixNano()),
		}
	}

	return &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{
			{
				ScopeSpans: []*tempotrace.ScopeSpans{
					{
						Spans: spans,
					},
				},
			},
		},
	}
}

func TestCreateBlock_SingleTrace(t *testing.T) {
	t.Log("Testing CreateBlock with a single trace")

	// Setup
	ctx := context.Background()
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 100 * 1024 * 1024, // 100MB
	}

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := createTestTrace(traceID, 3)

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

	// Wrap with proper backend interfaces
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	t.Log("Calling CreateBlock...")

	// Call CreateBlock
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)

	t.Log("CreateBlock returned")

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, resultMeta)
	require.Equal(t, int64(1), resultMeta.TotalObjects)
	require.Greater(t, resultMeta.Size_, uint64(0))

	t.Logf("Block created successfully: %d traces, %d bytes", resultMeta.TotalObjects, resultMeta.Size_)
}

func TestCreateBlock_MultipleTraces(t *testing.T) {
	t.Log("Testing CreateBlock with multiple traces")

	// Setup
	ctx := context.Background()
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 100 * 1024 * 1024,
	}

	// Create 10 test traces
	traceCount := 10
	traces := make([]*tempopb.Trace, traceCount)
	ids := make([][]byte, traceCount)

	for i := 0; i < traceCount; i++ {
		traceID := make([]byte, 16)
		traceID[0] = byte(i)
		ids[i] = traceID
		traces[i] = createTestTrace(traceID, 5) // 5 spans each
	}

	iter := &mockIterator{
		traces: traces,
		ids:    ids,
	}

	// Create temporary backend
	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{
		Path: tempDir,
	})
	require.NoError(t, err)

	// Wrap with proper backend interfaces
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	t.Logf("Calling CreateBlock with %d traces...", traceCount)

	// Call CreateBlock
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)

	t.Log("CreateBlock returned")

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, resultMeta)
	require.Equal(t, int64(traceCount), resultMeta.TotalObjects)
	require.Greater(t, resultMeta.Size_, uint64(0))

	t.Logf("Block created successfully: %d traces, %d bytes", resultMeta.TotalObjects, resultMeta.Size_)
}

func TestCreateBlock_EmptyIterator(t *testing.T) {
	t.Log("Testing CreateBlock with empty iterator")

	// Setup
	ctx := context.Background()
	cfg := &common.BlockConfig{
		RowGroupSizeBytes: 100 * 1024 * 1024,
	}

	iter := &mockIterator{
		traces: []*tempopb.Trace{},
		ids:    [][]byte{},
	}

	// Create temporary backend
	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{
		Path: tempDir,
	})
	require.NoError(t, err)

	// Wrap with proper backend interfaces
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)

	t.Log("Calling CreateBlock with empty iterator...")

	// Call CreateBlock
	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)

	t.Log("CreateBlock returned")

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, resultMeta)
	require.Equal(t, int64(0), resultMeta.TotalObjects)

	t.Logf("Block created successfully with 0 traces")
}
