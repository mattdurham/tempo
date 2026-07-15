package tempodb

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/grafana/tempo/modules/cache/memcached"
	"github.com/grafana/tempo/modules/cache/redis"
	"github.com/grafana/tempo/modules/postgres"

	"github.com/grafana/tempo/pkg/cache"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/azure"
	backend_cache "github.com/grafana/tempo/tempodb/backend/cache"
	"github.com/grafana/tempo/tempodb/backend/gcs"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/pool"
	"github.com/grafana/tempo/tempodb/wal"
)

const (
	DefaultBlocklistPoll                  = 5 * time.Minute
	DefaultMaxTimePerTenant               = 5 * time.Minute
	DefaultBlocklistPollConcurrency       = uint(50)
	DefaultBlocklistPollTenantConcurrency = uint(1)
	DefaultRetentionConcurrency           = uint(10)
	DefaultTenantIndexBuilders            = 2
	DefaultTolerateConsecutiveErrors      = 1
	DefaultTolerateTenantFailures         = 1

	DefaultEmptyTenantDeletionAge = 12 * time.Hour

	DefaultPrefetchTraceCount   = 1000
	DefaultSearchChunkSizeBytes = 1_000_000
	DefaultReadBufferCount      = 32
	DefaultReadBufferSize       = 1 * 1024 * 1024
)

// Config holds the entirety of tempodb configuration
// Defaults are in modules/storage/config.go
type Config struct {
	Pool   *pool.Config        `yaml:"pool,omitempty"`
	WAL    *wal.Config         `yaml:"wal"`
	Block  *common.BlockConfig `yaml:"block"`
	Search *SearchConfig       `yaml:"search"`

	BlocklistPoll                          time.Duration `yaml:"blocklist_poll"`
	BlocklistPollConcurrency               uint          `yaml:"blocklist_poll_concurrency"`
	BlocklistPollTenantConcurrency         uint          `yaml:"blocklist_poll_tenant_concurrency"`
	BlocklistPollFallback                  bool          `yaml:"blocklist_poll_fallback"`
	BlocklistPollTenantIndexBuilders       int           `yaml:"blocklist_poll_tenant_index_builders"`
	BlocklistPollStaleTenantIndex          time.Duration `yaml:"blocklist_poll_stale_tenant_index"`
	BlocklistPollJitterMs                  int           `yaml:"blocklist_poll_jitter_ms"`
	BlocklistPollTolerateConsecutiveErrors int           `yaml:"blocklist_poll_tolerate_consecutive_errors"`
	BlocklistPollTolerateTenantFailures    int           `yaml:"blocklist_poll_tolerate_tenant_failures"`

	EmptyTenantDeletionEnabled bool          `yaml:"empty_tenant_deletion_enabled"`
	EmptyTenantDeletionAge     time.Duration `yaml:"empty_tenant_deletion_age"`

	// backends
	Backend string        `yaml:"backend"`
	Local   *local.Config `yaml:"local"`
	GCS     *gcs.Config   `yaml:"gcs"`
	S3      *s3.Config    `yaml:"s3"`
	Azure   *azure.Config `yaml:"azure"`

	// legacy cache config. this is loaded by tempodb and added to the cache
	// provider on construction
	Cache           string                  `yaml:"cache"`
	BackgroundCache *cache.BackgroundConfig `yaml:"background_cache"`
	Memcached       *memcached.Config       `yaml:"memcached"`
	Redis           *redis.Config           `yaml:"redis"`

	// Postgres is the opt-in backend for the viusage/cube registries and the
	// file catalog (2026-07-11). Nil means "not configured" -- the S3/Local/
	// GCS/Azure blob-backed registry path stays the default, untouched.
	Postgres *postgres.Config `yaml:"postgres"`

	BloomCacheCfg backend_cache.BloomConfig `yaml:",inline"`
}

type CacheControlConfig struct {
	Footer      bool `yaml:"footer"`
	ColumnIndex bool `yaml:"column_index"`
	OffsetIndex bool `yaml:"offset_index"`
}

type SearchConfig struct {
	// v2 blocks
	ChunkSizeBytes     uint32 `yaml:"chunk_size_bytes"`
	PrefetchTraceCount int    `yaml:"prefetch_trace_count"`

	// vParquet blocks
	ReadBufferCount     int `yaml:"read_buffer_count"`
	ReadBufferSizeBytes int `yaml:"read_buffer_size_bytes"`
	// todo: consolidate caching config in one spot
	CacheControl CacheControlConfig `yaml:"cache_control"`
}

func (c *SearchConfig) RegisterFlagsAndApplyDefaults(string, *flag.FlagSet) {
	c.ChunkSizeBytes = DefaultSearchChunkSizeBytes
	c.PrefetchTraceCount = DefaultPrefetchTraceCount
	c.ReadBufferCount = DefaultReadBufferCount
	c.ReadBufferSizeBytes = DefaultReadBufferSize
}

func (c SearchConfig) ApplyToOptions(o *common.SearchOptions) {
	o.ChunkSizeBytes = c.ChunkSizeBytes
	o.PrefetchTraceCount = c.PrefetchTraceCount
	o.ReadBufferCount = c.ReadBufferCount
	o.ReadBufferSize = c.ReadBufferSizeBytes

	if o.ChunkSizeBytes == 0 {
		o.ChunkSizeBytes = DefaultSearchChunkSizeBytes
	}
	if o.PrefetchTraceCount <= 0 {
		o.PrefetchTraceCount = DefaultPrefetchTraceCount
	}
	if o.ReadBufferSize <= 0 {
		o.ReadBufferSize = DefaultReadBufferSize
	}
	if o.ReadBufferCount <= 0 {
		o.ReadBufferCount = DefaultReadBufferCount
	}
}

// CompactorConfig contains compaction configuration options
type CompactorConfig struct {
	MaxCompactionRange      time.Duration `yaml:"compaction_window"`
	MaxCompactionObjects    int           `yaml:"max_compaction_objects"`
	MaxBlockBytes           uint64        `yaml:"max_block_bytes"`
	BlockRetention          time.Duration `yaml:"block_retention"`
	CompactedBlockRetention time.Duration `yaml:"compacted_block_retention"`
	RetentionConcurrency    uint          `yaml:"retention_concurrency"`
	MaxTimePerTenant        time.Duration `yaml:"max_time_per_tenant"`
	CompactionCycle         time.Duration `yaml:"compaction_cycle"`
}

func (cfg *CompactorConfig) RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet) {
	// fill in default values
	cfg.MaxTimePerTenant = DefaultMaxTimePerTenant
	cfg.CompactionCycle = DefaultCompactionCycle
	cfg.CompactedBlockRetention = time.Hour
	cfg.RetentionConcurrency = DefaultRetentionConcurrency

	f.DurationVar(&cfg.BlockRetention, util.PrefixConfig(prefix, "block-retention"), 14*24*time.Hour, "Duration to keep blocks/traces.")
	f.IntVar(&cfg.MaxCompactionObjects, util.PrefixConfig(prefix, "max-objects-per-block"), 6000000, "Maximum number of traces in a compacted block.")
	f.Uint64Var(&cfg.MaxBlockBytes, util.PrefixConfig(prefix, "max-block-bytes"), 100*1024*1024*1024 /* 100GB */, "Maximum size of a compacted block.")
	f.DurationVar(&cfg.MaxCompactionRange, util.PrefixConfig(prefix, "compaction-window"), time.Hour, "Maximum time window across which to compact blocks.")
}

func (cfg *CompactorConfig) validate() error {
	if cfg.MaxCompactionRange == 0 {
		return errors.New("compaction window can't be 0")
	}

	return nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config should be non-nil")
	}

	if cfg.WAL == nil {
		return errors.New("wal config should be non-nil")
	}

	if cfg.Block == nil {
		return errors.New("block config should be non-nil")
	}

	// WAL version always matches the block version
	cfg.WAL.Version = cfg.Block.Version

	err := cfg.WAL.Validate()
	if err != nil {
		return fmt.Errorf("wal config validation failed: %w", err)
	}

	err = common.ValidateConfig(cfg.Block)
	if err != nil {
		return fmt.Errorf("block config validation failed: %w", err)
	}

	_, err = encoding.FromVersionForWrites(cfg.Block.Version)
	if err != nil {
		return fmt.Errorf("block version validation failed: %w", err)
	}

	// issue #504: the cube registry is Postgres-only now (no blob/index.json fallback), so
	// enabling cube ingest for any tenant without a configured Postgres pool is a broken
	// config, not a silently-degraded one -- fail fast at startup rather than let
	// vblockpack.ConfigureCubeManager/ConfigureCubeScheduler/ConfigureCubeQueryPath panic or
	// silently no-op the first time they actually try to touch the registry.
	if len(cfg.Block.Blockpack.CubeTenants) > 0 && cfg.Postgres == nil {
		return errors.New("blockpack.cube_tenants is non-empty but postgres is not configured: the cube registry requires postgres (issue #504)")
	}

	// The check above does NOT cover this: ConfigureCubeQueryPath (tempodb.go) is called
	// unconditionally whenever ValueIndexEnabled+S3, with no CubeTenants gate at all -- it's the
	// querier-side opportunistic cube-creation path (tryQueryFromCube/maybeCreateCube), distinct
	// from the ingest-side cube manager the CubeTenants check actually guards. A nil pgPool there
	// doesn't panic or error -- it declines silently, every query, forever, with zero log signal
	// (2026-07-15 incident: querier ran for weeks with cube creation permanently, invisibly
	// dead because this exact gap was never caught). Fail fast instead.
	if cfg.Block.Blockpack.ValueIndexEnabled && cfg.Backend == backend.S3 && cfg.Postgres == nil {
		return errors.New("blockpack.value_index_enabled is true with an S3 backend but postgres is not configured: the cube query path silently never creates cubes without it")
	}

	return nil
}
