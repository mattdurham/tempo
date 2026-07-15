package tempodb

import (
	"errors"
	"testing"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	"github.com/grafana/tempo/tempodb/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyToOptions(t *testing.T) {
	opts := common.DefaultSearchOptions()
	cfg := SearchConfig{}

	// test defaults
	cfg.ApplyToOptions(&opts)
	require.Equal(t, opts.PrefetchTraceCount, DefaultPrefetchTraceCount)
	require.Equal(t, opts.ChunkSizeBytes, uint32(DefaultSearchChunkSizeBytes))
	require.Equal(t, opts.ReadBufferCount, DefaultReadBufferCount)
	require.Equal(t, opts.ReadBufferSize, DefaultReadBufferSize)

	// test parameter fields are left alone
	opts.StartPage = 1
	opts.TotalPages = 2
	opts.MaxBytes = 3
	cfg.ApplyToOptions(&opts)
	require.Equal(t, opts.StartPage, 1)
	require.Equal(t, opts.TotalPages, 2)
	require.Equal(t, opts.MaxBytes, 3)

	// test non defaults
	cfg.ChunkSizeBytes = 4
	cfg.PrefetchTraceCount = 5
	cfg.ReadBufferCount = 6
	cfg.ReadBufferSizeBytes = 7
	cfg.ApplyToOptions(&opts)
	require.Equal(t, cfg.ChunkSizeBytes, uint32(4))
	require.Equal(t, cfg.PrefetchTraceCount, 5)
	require.Equal(t, cfg.ReadBufferCount, 6)
	require.Equal(t, cfg.ReadBufferSizeBytes, 7)
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		cfg            *Config
		expectedConfig *Config
		err            error
	}{
		// nil config fails
		{
			err: errors.New("config should be non-nil"),
		},
		// nil wal fails
		{
			cfg: &Config{},
			err: errors.New("wal config should be non-nil"),
		},
		// nil block fails
		{
			cfg: &Config{
				WAL: &wal.Config{},
			},
			err: errors.New("block config should be non-nil"),
		},
		// WAL version is always set to block version
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
			},
		},
		// WAL version is always overwritten with block version, even if previously set
		{
			cfg: &Config{
				WAL: &wal.Config{
					Version: "vParquet3",
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
			},
		},
		// issue #504: CubeTenants non-empty with no Postgres configured hard-fails at startup
		// (the cube registry is Postgres-only, no blob/index.json fallback).
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						CubeTenants: []string{"tenant-a"},
					},
				},
				Postgres: nil,
			},
			err: errors.New("blockpack.cube_tenants is non-empty but postgres is not configured: the cube registry requires postgres (issue #504)"),
		},
		// empty CubeTenants passes regardless of whether Postgres is configured.
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
				Postgres: nil,
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
				},
				Postgres: nil,
			},
		},
		// non-empty CubeTenants passes when Postgres is configured.
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						CubeTenants: []string{"tenant-a"},
					},
				},
				Postgres: &postgres.Config{},
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						CubeTenants: []string{"tenant-a"},
					},
				},
				Postgres: &postgres.Config{},
			},
		},
		// issue #504 follow-up (2026-07-15 incident): ValueIndexEnabled+S3 with no Postgres
		// configured hard-fails too -- the cube QUERY path (ConfigureCubeQueryPath) is wired
		// unconditionally on this exact condition, with NO CubeTenants gate, so leaving it
		// unchecked let a real deployment (tempo-dev-test-03's querier) run for weeks with cube
		// creation silently, permanently dead and zero log signal.
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						ValueIndexEnabled: true,
					},
				},
				Backend:  backend.S3,
				Postgres: nil,
			},
			err: errors.New("blockpack.value_index_enabled is true with an S3 backend but postgres is not configured: the cube query path silently never creates cubes without it"),
		},
		// same condition, Postgres configured: passes.
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						ValueIndexEnabled: true,
					},
				},
				Backend:  backend.S3,
				Postgres: &postgres.Config{},
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						ValueIndexEnabled: true,
					},
				},
				Backend:  backend.S3,
				Postgres: &postgres.Config{},
			},
		},
		// ValueIndexEnabled+non-S3 backend with no Postgres: passes -- ConfigureCubeQueryPath
		// itself is S3-only (tempodb.go's own gate), so this combination never reaches it.
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						ValueIndexEnabled: true,
					},
				},
				Backend:  backend.Local,
				Postgres: nil,
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: vparquet5.VersionString,
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             vparquet5.VersionString,
					Blockpack: common.BlockpackConfig{
						ValueIndexEnabled: true,
					},
				},
				Backend:  backend.Local,
				Postgres: nil,
			},
		},
	}

	for _, test := range tests {
		err := validateConfig(test.cfg)
		require.Equal(t, test.err, err)

		if test.expectedConfig != nil {
			require.Equal(t, test.expectedConfig, test.cfg)
		}
	}
}

func TestDeprecatedVersions(t *testing.T) {
	tests := []struct {
		cfg            *Config
		expectedConfig *Config
		err            string
	}{
		{
			cfg: &Config{
				WAL: &wal.Config{
					Version: "vParquet5-preview1", // silently overwritten with the block version
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             "vParquet4",
				},
			},
			expectedConfig: &Config{
				WAL: &wal.Config{
					Version: "vParquet4",
				},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             "vParquet4",
				},
			},
		},
		{
			cfg: &Config{
				WAL: &wal.Config{},
				Block: &common.BlockConfig{
					BloomFP:             0.01,
					BloomShardSizeBytes: 1,
					Version:             "vParquet5-preview1",
				},
			},
			err: "vParquet5-preview1 is not a valid block version for creating blocks",
		},
	}

	for _, test := range tests {
		err := validateConfig(test.cfg)
		if test.err == "" {
			require.Equal(t, nil, err)
		} else {
			assert.Contains(t, err.Error(), test.err)
		}

		if test.expectedConfig != nil {
			require.Equal(t, test.expectedConfig, test.cfg)
		}
	}
}

func TestValidateCompactorConfig(t *testing.T) {
	compactorConfig := CompactorConfig{
		MaxCompactionRange: 0,
	}

	expected := errors.New("compaction window can't be 0")
	actual := compactorConfig.validate()

	require.Equal(t, expected, actual)
}
