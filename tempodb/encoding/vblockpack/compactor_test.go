package vblockpack

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/backend"
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

// NOTE-477 (blockpack issue #395): newRangeBlockProvider must derive the block size from
// BlockMeta.Size_ and never download the full object via StreamReader, which on the S3
// backend is a full readAll. backend.MockReader.StreamReader panics, so a successful call
// with Size_ set proves the StreamReader path was not taken; an unset Size_ falls back to
// StreamReader and therefore panics, proving the fallback is wired only for legacy blocks.
func TestNewRangeBlockProvider_UsesMetaSizeNoStreamReader(t *testing.T) {
	r := &backend.MockReader{}
	m := &backend.BlockMeta{
		BlockID:  backend.UUID(uuid.New()),
		TenantID: "test-tenant",
		Size_:    4096,
	}

	prov, err := newRangeBlockProvider(context.Background(), r, m)
	require.NoError(t, err)
	require.NotNil(t, prov)

	size, err := prov.Size()
	require.NoError(t, err)
	require.Equal(t, int64(4096), size, "provider size must come from BlockMeta.Size_")
}

func TestNewRangeBlockProvider_LegacyZeroSizeFallsBackToStreamReader(t *testing.T) {
	r := &backend.MockReader{}
	m := &backend.BlockMeta{
		BlockID:  backend.UUID(uuid.New()),
		TenantID: "test-tenant",
		Size_:    0, // legacy block written before size tracking
	}

	// MockReader.StreamReader panics; reaching it proves the legacy fallback is wired.
	require.PanicsWithValue(t, "StreamReader is not yet supported for mock reader", func() {
		_, _ = newRangeBlockProvider(context.Background(), r, m)
	})
}
