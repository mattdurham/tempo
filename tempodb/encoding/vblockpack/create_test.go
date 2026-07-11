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

func (m *mockIterator) Next(_ context.Context) (common.ID, *tempopb.Trace, error) {
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

// TestCreateBlock_SetsMetaVersion_EvenWhenInputMetaHasNone is the regression test for a real
// bug found by the backend-agnostic VI/cube integration test (modules/frontend/
// vi_backfill_local_integration_test.go): tempodb.go's CompleteBlockWithBackend builds its
// own inMeta as a bare &backend.BlockMeta{...} literal (TenantID/BlockID/TotalObjects/
// StartTime/EndTime/DedicatedColumns only -- Version is NEVER copied, verified directly
// against tempodb.go:430-438) before calling VersionedEncoding.CreateBlock, relying on
// CreateBlock itself to set the returned meta's Version. vparquet4's CreateBlock honors this
// contract (constructs a fresh meta via backend.NewBlockMeta(tenantID, blockID, VersionString),
// vparquet4/create.go:143) but vblockpack's CreateBlock (this package) previously never set
// meta.Version at all -- it mutates and returns the SAME meta pointer it was given, Version
// field untouched. Every existing test above pre-populates meta.Version via
// backend.NewBlockMeta(..., VersionString) before calling CreateBlock, masking the gap; this
// test deliberately mirrors the REAL caller's bare-meta shape instead.
func TestCreateBlock_SetsMetaVersion_EvenWhenInputMetaHasNone(t *testing.T) {
	ctx := context.Background()
	cfg := &common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024}

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	trace := createTestTrace(traceID, 1)
	iter := &mockIterator{traces: []*tempopb.Trace{trace}, ids: [][]byte{traceID}}

	tempDir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: tempDir})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	// Bare meta literal -- NO Version set -- mirrors tempodb.go's real inMeta construction
	// exactly (not backend.NewBlockMeta, which would set Version as a side effect and mask
	// this bug).
	meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: "test-tenant"}

	resultMeta, err := CreateBlock(ctx, cfg, meta, iter, r, w)
	require.NoError(t, err)
	require.NotNil(t, resultMeta)

	require.Equal(t, VersionString, resultMeta.Version,
		"CreateBlock must set meta.Version itself -- callers building a bare meta (exactly "+
			"like tempodb.go's real CompleteBlockWithBackend inMeta) must not end up with an "+
			"unopenable block (encoding.OpenBlock dispatches on meta.Version and fails on empty)")
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
