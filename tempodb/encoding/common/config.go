package common

import (
	"flag"
	"fmt"
	"time"

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
	DefaultBlockpackMaxSpansPerBlock    = 2000
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

	// MaxSpansPerBlock controls how many spans are written per blockpack block.
	// Defaults to 2000 if zero.
	MaxSpansPerBlock int `yaml:"max_spans_per_block"`

	// MaxOutputFileSize is the maximum size in bytes of each compaction output file.
	// When a compacted output block would exceed this size, blockpack splits it into
	// a new file. Zero means no size limit.
	MaxOutputFileSize int64 `yaml:"max_output_file_size"`

	// FileCachePath is the root directory for the disk-backed file cache.
	// Leave empty to disable the disk cache.
	FileCachePath string `yaml:"file_cache_path"`

	// FileCacheMaxBytes is the maximum size of the disk cache in bytes (default: 4GB).
	FileCacheMaxBytes int64 `yaml:"file_cache_max_bytes"`

	// LRUCacheBytes is the maximum size of the process-level in-memory LRU cache in bytes.
	// This cache holds footer and file metadata only — sketch/bloom data should not be
	// retained here. Defaults to 32MB.
	LRUCacheBytes int64 `yaml:"lru_cache_bytes"`

	// EmbeddingURL is the base URL of an embedding server (e.g. HuggingFace TEI).
	// When set, spans are embedded at block creation time and VECTOR_AI()/VECTOR_ALL()
	// queries are supported. Leave empty to disable embedding.
	EmbeddingURL string `yaml:"embedding_url"`

	// VectorDimension is the expected embedding vector dimension (e.g. 768 for nomic-embed-text-v1.5).
	// Required when EmbeddingURL is set. Controls the writer's VectorIndex section.
	VectorDimension int `yaml:"vector_dimension"`

	// EmbeddingConcurrentBatches is the number of concurrent POST /embed requests sent to the
	// embedding server during block creation. Higher values saturate more server pods.
	// Defaults to the blockpack default (4) when 0.
	EmbeddingConcurrentBatches int `yaml:"embedding_concurrent_batches"`

	// EmbeddingBatchSize is the number of texts sent in a single POST /embed request.
	// Larger values reduce HTTP round trips when span texts are short.
	// Must not exceed the embedding server's max-batch-tokens / avg tokens per text.
	// Defaults to the blockpack default (32) when 0.
	EmbeddingBatchSize int `yaml:"embedding_batch_size"`

	// EmbeddingMaxTextLength is the maximum character length of assembled span text sent to the
	// embedding server. Shorter limits reduce token count for long spans (error messages, stack
	// traces), improving GPU throughput. Defaults to 24000 (~8192 tokens) when 0.
	EmbeddingMaxTextLength int `yaml:"embedding_max_text_length"`

	// MemCacheServers is a list of memcache server addresses for raw block data
	// (ToCTypeBlock entries — large, primarily benefits pod-local disk/memory cache).
	// Leave empty to disable the remote memcache tier for block data.
	MemCacheServers []string `yaml:"memcache_servers"`
	// MetadataMemCacheServers is a list of memcache server addresses for metadata
	// (ToC blob, bloom, range index, intrinsic columns — small, high value in shared remote cache).
	// When set, metadata and block data use separate cache tiers (TieredCache).
	// When empty, all data shares the same cache tier (MemCacheServers).
	MetadataMemCacheServers []string `yaml:"metadata_memcache_servers"`

	// MemoryCacheBytes is the size of the in-process LRU memory cache (default: 256MB).
	// 256 MB in-process LRU cache is always on; set to 0 to disable.
	MemoryCacheBytes int64 `yaml:"memory_cache_bytes"`

	// ValueIndexConsumer configures the value-index consumer Tempo target
	// (-target=value-index-consumer). Disabled by default.
	ValueIndexConsumer ValueIndexConsumerConfig `yaml:"value_index_consumer"`

	// ValueIndexCompactor configures the value-index compactor Tempo target
	// (-target=value-index-compactor). Disabled by default.
	ValueIndexCompactor ValueIndexCompactorConfig `yaml:"value_index_compactor"`

	// ValueIndexQuery configures the querier-side index-driven query path
	// (blockpack issue #461). When enabled, the blockpack querier discovers and
	// downloads value-index files to answer search/metrics queries with block-level
	// pruning, falling back to a full block scan when the index lacks coverage.
	// Disabled by default; queriers opt in via value_index_query.enabled.
	ValueIndexQuery ValueIndexQueryConfig `yaml:"value_index_query"`

	// ValueIndexEnabled turns on the synchronous in-process value-index write path
	// (blockpack NOTE-VI-042, issue #464). When true, the block-builder and
	// compactor write L0 value-index files (blockpack.WriteValueIndexL0) to object
	// storage after each block flush / compaction, with no Redis broker. Disabled
	// by default. Must be enabled on writer targets (block-builder, backend-worker)
	// for the querier's value_index_query path to have files to read.
	ValueIndexEnabled bool `yaml:"value_index_enabled"`

	// ValueIndexPrefix is the S3 key prefix under which the synchronous write path
	// stores value-index files. It must match the querier's
	// value_index_query.index_prefix and the compactor index_prefix
	// (default: "indexes").
	ValueIndexPrefix string `yaml:"value_index_prefix"`

	// CubeTenants is the list of tenant IDs for which the cube ingest manager
	// should be configured on writer targets (block-builder, backend-worker).
	// The manager loads each tenant's cube registry at startup and accumulates
	// per-minute span counts for all active cubes. Requires ValueIndexEnabled.
	// An empty list disables cube ingest. Typical value: ["11638"].
	CubeTenants []string `yaml:"cube_tenants"`

	// CubeCompactorEnabled turns on periodic cube file compaction on the
	// value-index-compactor target. When enabled, the compactor merges
	// many 1-minute L0 cube files into hourly L0 groups, then rolls them
	// up into L1 files.
	CubeCompactorEnabled bool `yaml:"cube_compactor_enabled"`

	// CubeCompactorInterval is the period between cube compaction passes
	// (default: 5 minutes).
	CubeCompactorInterval time.Duration `yaml:"cube_compactor_interval"`
}

// ValueIndexQueryConfig configures the querier-side index-driven query path.
type ValueIndexQueryConfig struct {
	// Enabled turns on the index-driven query path on this querier.
	Enabled bool `yaml:"enabled"`
	// IndexPrefix is the S3 key prefix under which value-index files live. It must
	// match the consumer/compactor index_prefix (default: "indexes").
	IndexPrefix string `yaml:"index_prefix"`
	// CacheTTL is the refresh interval for the in-process index-file listing cache
	// (blockpack issue #462). Zero uses the blockpack default (30s).
	CacheTTL time.Duration `yaml:"cache_ttl"`
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
	if cfg.MaxSpansPerBlock == 0 {
		cfg.MaxSpansPerBlock = DefaultBlockpackMaxSpansPerBlock
	}
	if cfg.FileCacheMaxBytes == 0 {
		cfg.FileCacheMaxBytes = 4 * 1024 * 1024 * 1024 // 4GB
	}
	if cfg.LRUCacheBytes == 0 {
		cfg.LRUCacheBytes = 32 * 1024 * 1024 // 32MB — footer/metadata only
	}
	if cfg.MemoryCacheBytes == 0 {
		cfg.MemoryCacheBytes = 256 << 20 // 256MB
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
	// Value-index write path (NOTE-VI-042, issue #464): default the prefix to
	// "indexes" so it matches the querier's value_index_query.index_prefix and the
	// compactor index_prefix when value_index_enabled is set without an explicit
	// prefix.
	if cfg.ValueIndexPrefix == "" {
		cfg.ValueIndexPrefix = "indexes"
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

	if cfg.MaxSpansPerBlock < 0 {
		return fmt.Errorf("blockpack max spans per block must be non-negative, got %d", cfg.MaxSpansPerBlock)
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

// ValueIndexConsumerConfig configures the value-index consumer Tempo target.
// Mirrors valueindexconsumer.Config for YAML decoding without a direct blockpack import.
type ValueIndexConsumerConfig struct {
	Enabled            bool          `yaml:"enabled"`
	RedisAddr          string        `yaml:"redis_addr"`
	StreamName         string        `yaml:"stream_name"`
	ConsumerGroup      string        `yaml:"consumer_group"`
	ConsumerName       string        `yaml:"consumer_name"`
	IndexPrefix        string        `yaml:"index_prefix"`
	Columns            []string      `yaml:"columns"`
	FlushInterval      time.Duration `yaml:"flush_interval"`
	PollTimeout        time.Duration `yaml:"poll_timeout"`
	ClaimIdleThreshold time.Duration `yaml:"claim_idle_threshold"`
	BatchSize          int           `yaml:"batch_size"`
}

// ValueIndexCompactorConfig configures the value-index compactor Tempo target.
// Mirrors valueindexcompactor.Config for YAML decoding without a direct blockpack import.
type ValueIndexCompactorConfig struct {
	Enabled               bool          `yaml:"enabled"`
	IndexPrefix           string        `yaml:"index_prefix"`
	Tenants               []string      `yaml:"tenants"`
	CompactInterval       time.Duration `yaml:"compact_interval"`
	CompactThresholdFiles int           `yaml:"compact_threshold_files"`
	MaxOutputBytes        int64         `yaml:"max_output_bytes"`
	// ShardCount and ShardIndex partition the column space across replicas.
	// Each replica only compacts columns where colHash[0] % ShardCount == ShardIndex.
	// When ShardCount <= 1 all columns are processed (no sharding).
	// Typically injected via SHARD_COUNT / SHARD_INDEX environment variables.
	ShardCount int `yaml:"shard_count"`
	ShardIndex int `yaml:"shard_index"`
}
