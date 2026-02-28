package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockpackConfigDefaults(t *testing.T) {
	cfg := BlockpackConfig{}
	cfg.applyDefaults()

	assert.Equal(t, DefaultBlockpackCompressionCodec, cfg.CompressionCodec)
	assert.Equal(t, DefaultBlockpackCompressionLevel, cfg.CompressionLevel)
	assert.Equal(t, DefaultBlockpackColumnBlockSize, cfg.ColumnBlockSize)
	assert.Equal(t, DefaultBlockpackWriteBufferSize, cfg.WriteBufferSize)
	assert.Equal(t, DefaultBlockpackDictionaryMaxSize, cfg.DictionaryMaxSize)
	assert.Equal(t, DefaultBlockpackMinHashPermutations, cfg.MinHashPermutations)
	assert.True(t, cfg.EnableDictionary)
	assert.True(t, cfg.EnableMinHash)
	assert.True(t, cfg.EnableBitPacking)
}

func TestBlockpackConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		cfg         BlockpackConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			cfg: BlockpackConfig{
				CompressionCodec:    "zstd",
				CompressionLevel:    3,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: false,
		},
		{
			name: "empty config - no validation",
			cfg: BlockpackConfig{
				CompressionCodec: "",
			},
			expectError: false,
		},
		{
			name: "invalid codec",
			cfg: BlockpackConfig{
				CompressionCodec:    "invalid",
				CompressionLevel:    3,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: true,
			errorMsg:    "invalid blockpack compression codec",
		},
		{
			name: "negative compression level",
			cfg: BlockpackConfig{
				CompressionCodec:    "zstd",
				CompressionLevel:    -1,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: true,
			errorMsg:    "compression level must be non-negative",
		},
		{
			name: "negative column block size",
			cfg: BlockpackConfig{
				CompressionCodec:    "zstd",
				CompressionLevel:    3,
				ColumnBlockSize:     -1,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: true,
			errorMsg:    "column block size must be non-negative",
		},
		{
			name: "negative write buffer size",
			cfg: BlockpackConfig{
				CompressionCodec:    "zstd",
				CompressionLevel:    3,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     -1,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: true,
			errorMsg:    "write buffer size must be non-negative",
		},
		{
			name: "valid snappy codec",
			cfg: BlockpackConfig{
				CompressionCodec:    "snappy",
				CompressionLevel:    0,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: false,
		},
		{
			name: "valid lz4 codec",
			cfg: BlockpackConfig{
				CompressionCodec:    "lz4",
				CompressionLevel:    0,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: false,
		},
		{
			name: "valid none codec",
			cfg: BlockpackConfig{
				CompressionCodec:    "none",
				CompressionLevel:    0,
				ColumnBlockSize:     64 * 1024,
				WriteBufferSize:     1024 * 1024,
				DictionaryMaxSize:   1024 * 1024,
				MinHashPermutations: 128,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBlockConfigWithBlockpack(t *testing.T) {
	cfg := &BlockConfig{}
	// Apply defaults without flags
	cfg.Blockpack.applyDefaults()
	cfg.BloomFP = DefaultBloomFP
	cfg.BloomShardSizeBytes = DefaultBloomShardSizeBytes

	// Verify blockpack defaults are applied
	assert.Equal(t, DefaultBlockpackCompressionCodec, cfg.Blockpack.CompressionCodec)
	assert.Equal(t, DefaultBlockpackCompressionLevel, cfg.Blockpack.CompressionLevel)
	assert.True(t, cfg.Blockpack.EnableDictionary)
	assert.True(t, cfg.Blockpack.EnableMinHash)
	assert.True(t, cfg.Blockpack.EnableBitPacking)

	// Verify validation includes blockpack
	err := ValidateConfig(cfg)
	require.NoError(t, err)
}

func TestBlockConfigBlockpackValidationFailure(t *testing.T) {
	cfg := &BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
		Blockpack: BlockpackConfig{
			CompressionCodec:  "invalid",
			CompressionLevel:  3,
			ColumnBlockSize:   64 * 1024,
			WriteBufferSize:   1024 * 1024,
			DictionaryMaxSize: 1024 * 1024,
		},
	}

	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "blockpack config validation failed")
}
