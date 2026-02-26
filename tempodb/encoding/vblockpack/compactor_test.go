package vblockpack

import (
	"testing"

	"github.com/stretchr/testify/require"

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
