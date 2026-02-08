package common

import (
	"flag"
	"fmt"

	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/backend"
)

const (
	DefaultBloomFP              = .01
	DefaultBloomShardSizeBytes  = 100 * 1024
	DefaultIndexDownSampleBytes = 1024 * 1024
	DefaultIndexPageSizeBytes   = 250 * 1024

	// Blockpack defaults
	DefaultBlockpackCompressionCodec    = "zstd"
	DefaultBlockpackCompressionLevel    = 3
	DefaultBlockpackColumnBlockSize     = 64 * 1024   // 64KB
	DefaultBlockpackWriteBufferSize     = 1024 * 1024 // 1MB
	DefaultBlockpackDictionaryMaxSize   = 1024 * 1024 // 1MB
	DefaultBlockpackMinHashPermutations = 128
)

const DeprecatedError = "%s is no longer supported, please use %s or later"

// BlockConfig holds configuration options for newly created blocks
type BlockConfig struct {
	BloomFP             float64 `yaml:"bloom_filter_false_positive"`
	BloomShardSizeBytes int     `yaml:"bloom_filter_shard_size_bytes"`
	Version             string  `yaml:"version"`

	// parquet fields
	RowGroupSizeBytes int `yaml:"parquet_row_group_size_bytes"`

	// vParquet3 fields
	DedicatedColumns backend.DedicatedColumns `yaml:"parquet_dedicated_columns"`

	// blockpack fields
	Blockpack BlockpackConfig `yaml:"blockpack"`

	// used internally. If true, the block will be created by default with the nocompact flag set.
	CreateWithNoCompactFlag bool `yaml:"-"`
}

// BlockpackConfig holds configuration options for blockpack format
type BlockpackConfig struct {
	// Compression codec to use (zstd, snappy, lz4, none)
	CompressionCodec string `yaml:"compression_codec"`

	// Compression level (codec-specific, typically 1-22 for zstd)
	CompressionLevel int `yaml:"compression_level"`

	// ColumnBlockSize is the target size for column blocks in bytes
	ColumnBlockSize int `yaml:"column_block_size"`

	// WriteBufferSize is the buffer size for writing blocks
	WriteBufferSize int `yaml:"write_buffer_size"`

	// EnableDictionary enables dictionary encoding for string columns
	EnableDictionary bool `yaml:"enable_dictionary"`

	// DictionaryMaxSize is the maximum size of dictionary before falling back to plain encoding
	DictionaryMaxSize int `yaml:"dictionary_max_size"`

	// EnableMinHash enables MinHash indexing for similarity search
	EnableMinHash bool `yaml:"enable_minhash"`

	// MinHashPermutations is the number of permutations for MinHash (default: 128)
	MinHashPermutations int `yaml:"minhash_permutations"`

	// EnableBitPacking enables bit-packing for integer columns
	EnableBitPacking bool `yaml:"enable_bit_packing"`
}

func (cfg *BlockConfig) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	f.Float64Var(&cfg.BloomFP, util.PrefixConfig(prefix, "trace.block.v2-bloom-filter-false-positive"), DefaultBloomFP, "Bloom Filter False Positive.")
	f.IntVar(&cfg.BloomShardSizeBytes, util.PrefixConfig(prefix, "trace.block.v2-bloom-filter-shard-size-bytes"), DefaultBloomShardSizeBytes, "Bloom Filter Shard Size in bytes.")

	cfg.RowGroupSizeBytes = 100_000_000 // 100 MB
	cfg.DedicatedColumns = backend.DefaultDedicatedColumns()

	// Apply blockpack defaults
	cfg.Blockpack.applyDefaults()
}

// applyDefaults applies default values to blockpack configuration
func (cfg *BlockpackConfig) applyDefaults() {
	if cfg.CompressionCodec == "" {
		cfg.CompressionCodec = DefaultBlockpackCompressionCodec
	}
	if cfg.CompressionLevel == 0 {
		cfg.CompressionLevel = DefaultBlockpackCompressionLevel
	}
	if cfg.ColumnBlockSize == 0 {
		cfg.ColumnBlockSize = DefaultBlockpackColumnBlockSize
	}
	if cfg.WriteBufferSize == 0 {
		cfg.WriteBufferSize = DefaultBlockpackWriteBufferSize
	}
	if cfg.DictionaryMaxSize == 0 {
		cfg.DictionaryMaxSize = DefaultBlockpackDictionaryMaxSize
	}
	if cfg.MinHashPermutations == 0 {
		cfg.MinHashPermutations = DefaultBlockpackMinHashPermutations
	}
	// Booleans default to false, so we enable by default
	if !cfg.EnableDictionary {
		cfg.EnableDictionary = true
	}
	if !cfg.EnableMinHash {
		cfg.EnableMinHash = true
	}
	if !cfg.EnableBitPacking {
		cfg.EnableBitPacking = true
	}
}

// validate validates blockpack configuration
func (cfg *BlockpackConfig) validate() error {
	// If codec is empty, assume defaults haven't been applied yet - skip validation
	// This allows configs to be created without explicitly setting blockpack options
	if cfg.CompressionCodec == "" {
		return nil
	}

	validCodecs := map[string]bool{
		"zstd":   true,
		"snappy": true,
		"lz4":    true,
		"none":   true,
	}
	if !validCodecs[cfg.CompressionCodec] {
		return fmt.Errorf("invalid blockpack compression codec %q, must be one of: zstd, snappy, lz4, none", cfg.CompressionCodec)
	}

	if cfg.CompressionLevel < 0 {
		return fmt.Errorf("blockpack compression level must be non-negative, got %d", cfg.CompressionLevel)
	}

	if cfg.ColumnBlockSize < 0 {
		return fmt.Errorf("blockpack column block size must be non-negative, got %d", cfg.ColumnBlockSize)
	}

	if cfg.WriteBufferSize < 0 {
		return fmt.Errorf("blockpack write buffer size must be non-negative, got %d", cfg.WriteBufferSize)
	}

	if cfg.DictionaryMaxSize < 0 {
		return fmt.Errorf("blockpack dictionary max size must be non-negative, got %d", cfg.DictionaryMaxSize)
	}

	if cfg.MinHashPermutations < 0 {
		return fmt.Errorf("blockpack minhash permutations must be non-negative, got %d", cfg.MinHashPermutations)
	}

	return nil
}

// ValidateConfig returns true if the config is valid
func ValidateConfig(b *BlockConfig) error {
	if b.BloomFP <= 0.0 || b.BloomFP >= 1.0 {
		return fmt.Errorf("invalid bloom filter fp rate %v", b.BloomFP)
	}

	if b.BloomShardSizeBytes <= 0 {
		return fmt.Errorf("positive value required for bloom-filter shard size")
	}

	// Check for deprecated version,
	// TODO - Cyclic dependency makes this awkward to improve by using the
	// deprecation information in the encoding itself, in the versioned logic
	// in the parent folder. So we are checking raw strings here.
	/*if b.Version == "vParquet2" {
		return fmt.Errorf(DeprecatedError, "vParquet2", "vParquet3")
	}*/

	// Validate blockpack configuration
	if err := b.Blockpack.validate(); err != nil {
		return fmt.Errorf("blockpack config validation failed: %w", err)
	}

	// TODO - log or pass warnings up the chain?
	_, err := b.DedicatedColumns.Validate()
	return err
}
