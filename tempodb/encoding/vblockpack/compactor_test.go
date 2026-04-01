package vblockpack

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func TestNewCompactor(t *testing.T) {
	opts := common.CompactionOptions{
		MaxBytesPerTrace: 1000000,
		OutputBlocks:     1,
		BlockConfig: common.BlockConfig{
			RowGroupSizeBytes: 100 * 1024 * 1024,
		},
	}

	compactor := NewCompactor(opts)
	require.NotNil(t, compactor)
	require.Equal(t, opts, compactor.opts)
}

func TestMaxSpansFromConfig(t *testing.T) {
	tests := []struct {
		rowGroupBytes int
		expected      int
	}{
		{0, 0},
		{50 * 1024, 50},   // below minimum → clamped to 100
		{100 * 1024, 100}, // exactly minimum
		{2000 * 1024, 2000},
		{20000 * 1024, 10000}, // above maximum → clamped to 10000
	}
	for _, tt := range tests {
		cfg := &common.BlockConfig{RowGroupSizeBytes: tt.rowGroupBytes}
		got := maxSpansFromConfig(cfg)
		if tt.rowGroupBytes == 0 {
			require.Equal(t, 0, got)
		} else if tt.rowGroupBytes < 100*1024 {
			require.Equal(t, 100, got)
		} else if tt.rowGroupBytes > 10000*1024 {
			require.Equal(t, 10000, got)
		} else {
			require.Equal(t, tt.expected, got)
		}
	}
}

func TestCompactorCompact_EmptyInputs(t *testing.T) {
	c := NewCompactor(common.CompactionOptions{})
	metas, err := c.Compact(t.Context(), nil, nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, metas)
}

// TestCompact_NoDuplicateSpans is a regression test for the compaction path
// double-writing trace:id to the intrinsic accumulator.
//
// Before the fix, compacting a dual-storage block (v3: identity fields in both block
// columns and intrinsic section) caused addRowFromBlock to feed trace:id once and
// feedIntrinsicsFromIndex to feed it a second time. The resulting compacted block had
// two intrinsic entries per span, so GetTraceByID returned each span twice.
func TestCompact_NoDuplicateSpans(t *testing.T) {
	const spanCount = 20
	ctx := context.Background()
	now := uint64(time.Now().UnixNano())

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Build a trace with spanCount distinct spans.
	spans := make([]*tempotrace.Span, spanCount)
	for i := range spans {
		spanID := [8]byte{byte(i + 1)}
		spans[i] = &tempotrace.Span{
			TraceId:           traceID,
			SpanId:            spanID[:],
			Name:              "span",
			StartTimeUnixNano: now + uint64(i)*1000,
			EndTimeUnixNano:   now + uint64(i)*1000 + 1000,
		}
	}
	tr := &tempopb.Trace{
		ResourceSpans: []*tempotrace.ResourceSpans{{
			ScopeSpans: []*tempotrace.ScopeSpans{{
				Spans: spans,
			}},
		}},
	}

	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)
	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)

	// Write a fresh block.
	meta := backend.NewBlockMeta("test-tenant", uuid.New(), VersionString)
	blockMeta, err := CreateBlock(ctx, &common.BlockConfig{}, meta, &mockIterator{
		traces: []*tempopb.Trace{tr},
		ids:    [][]byte{traceID},
	}, r, w)
	require.NoError(t, err)

	// Compact the single block into a new block.
	opts := common.CompactionOptions{
		BlockConfig: common.BlockConfig{RowGroupSizeBytes: 100 * 1024 * 1024},
	}
	compactor := NewCompactor(opts)
	outputMetas, err := compactor.Compact(ctx, log.NewNopLogger(), r, w, []*backend.BlockMeta{blockMeta})
	require.NoError(t, err)
	require.Len(t, outputMetas, 1, "expected exactly one output block")

	// Query the compacted block.
	compacted := newBackendBlock(outputMetas[0], r)
	resp, err := compacted.FindTraceByID(ctx, traceID, common.SearchOptions{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Trace)

	// Count total spans — must equal spanCount, not 2×spanCount.
	got := 0
	for _, rs := range resp.Trace.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			got += len(ss.Spans)
		}
	}
	require.Equal(t, spanCount, got, "compacted block must return each span exactly once")
}
