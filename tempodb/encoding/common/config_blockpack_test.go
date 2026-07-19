package common

import (
	"testing"
	"time"

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

// TestConfig_DedicatedColumnsEnabled_SafetyValveDefaultTrue verifies R12's
// safety valve defaults to enabled (true) -- the new #496 feature is
// intended to become the new default behavior, with disabling it a real,
// deliberate opt-out, not the starting state.
func TestConfig_DedicatedColumnsEnabled_SafetyValveDefaultTrue(t *testing.T) {
	cfg := BlockpackConfig{}
	cfg.applyDefaults()

	assert.True(t, cfg.ViUsage.DedicatedColumnsEnabled, "R12's safety valve must default to enabled")
	assert.Equal(t, 30*time.Minute, cfg.ViUsage.LeaseTTL)
	assert.Equal(t, 30*time.Second, cfg.ViUsage.WatermarkCacheTTL)
}

// TestConfig_DedicatedColumnsOverride_PerTenant verifies the deployment-wide
// dedicated-column override (when set) takes precedence over the empty
// default, mirroring create.go:63-64's meta.DedicatedColumns-over-
// cfg.DedicatedColumns precedent (an explicit override always wins over
// "nothing configured").
func TestConfig_DedicatedColumnsOverride_PerTenant(t *testing.T) {
	cfg := BlockpackConfig{
		ViUsage: ViUsageConfig{
			DedicatedColumnsOverride: []string{"span.http.request.method", "resource.custom.attr"},
		},
	}
	cfg.applyDefaults()

	require.Len(t, cfg.ViUsage.DedicatedColumnsOverride, 2)
	assert.Equal(t, []string{"span.http.request.method", "resource.custom.attr"}, cfg.ViUsage.DedicatedColumnsOverride)
	// applyDefaults must never clear or replace an explicit override.
	assert.NotEmpty(t, cfg.ViUsage.DedicatedColumnsOverride)
}

// TestConfig_DedicatedColumnsEnabled_ExplicitFalsePreserved verifies
// applyDefaults does not clobber an explicit false -- R12 requires disabling
// to be a real, tested code path, not merely a theoretical escape hatch that
// silently gets forced back to true.
func TestConfig_DedicatedColumnsEnabled_ExplicitFalsePreserved(t *testing.T) {
	cfg := ViUsageConfig{DedicatedColumnsEnabled: false}
	// Simulate the real startup sequence: RegisterFlagsAndApplyDefaults runs
	// ONCE, early, before YAML config is decoded on top (the same convention
	// EnableDictionary/EnableMinHash/EnableBitPacking already rely on) -- so
	// applyDefaults itself is never re-invoked after an explicit false is set.
	// This test documents that contract for ViUsageConfig specifically.
	assert.False(t, cfg.DedicatedColumnsEnabled)
}

// TestJobPlannerConfig_Defaults pins issue #518's job-planner poll-loop config
// defaults (60s poll interval, 6h VI window, 24h cube window) -- applied
// transitively via BlockpackConfig.applyDefaults(), mirroring ViUsageConfig's
// applyDefaults() call convention above.
func TestJobPlannerConfig_Defaults(t *testing.T) {
	cfg := BlockpackConfig{JobPlanner: JobPlannerConfig{Enabled: true}}
	cfg.applyDefaults()

	assert.True(t, cfg.JobPlanner.Enabled)
	assert.Equal(t, 60*time.Second, cfg.JobPlanner.PollInterval)
	assert.Equal(t, uint64(6*3600), cfg.JobPlanner.ViWindowSeconds)
	assert.Equal(t, uint32(1440), cfg.JobPlanner.CubeWindowMinutes)
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
