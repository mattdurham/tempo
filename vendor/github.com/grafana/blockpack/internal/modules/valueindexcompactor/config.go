package valueindexcompactor

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE-VI-017: see internal/modules/valueindexcompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Default configuration values for the value-index compactor.
const (
	// DefaultIndexPrefix is the object-storage key prefix under which value
	// index files live. Must match the consumer's index_prefix.
	DefaultIndexPrefix = "indexes"
	// DefaultCompactInterval is the wall-clock period between compaction passes
	// over all tenants and columns.
	DefaultCompactInterval = 5 * time.Minute
	// DefaultCompactThresholdFiles is the number of same-level files a column
	// directory must accumulate before it is compacted.
	DefaultCompactThresholdFiles = 8
	// DefaultMaxOutputBytes is the approximate max serialized size of one output
	// file before the merge splits into multiple files. 0 disables splitting.
	DefaultMaxOutputBytes = 256 << 20 // 256 MiB
)

// Config configures the value-index compactor. It maps to the optional
// `value_index_compactor` YAML block.
//
//	value_index_compactor:
//	  index_prefix: "indexes"
//	  compact_interval: 5m
//	  compact_threshold_files: 8
//	  max_output_bytes: 268435456
//	  tenants:
//	    - "11638"     # explicit list, or "*" for all tenants
type Config struct {
	// Registerer is the Prometheus registerer used to expose value-index
	// compactor metrics (NOTE-VI-023). When nil, all metrics are no-ops and no
	// registration occurs. It is not settable from YAML; the embedder (tempo)
	// injects it programmatically.
	Registerer prometheus.Registerer `yaml:"-"`
	// IndexPrefix is the object-storage key prefix for value index files.
	// Defaults to DefaultIndexPrefix when empty. Must match the consumer.
	IndexPrefix string `yaml:"index_prefix"`
	// Tenants is the explicit list of tenant IDs to compact. A single entry of
	// "*" means all tenants (discovered by listing the prefix). Must be
	// non-empty.
	Tenants []string `yaml:"tenants"`
	// CompactInterval is the period between compaction passes. Defaults to
	// DefaultCompactInterval when <= 0.
	CompactInterval time.Duration `yaml:"compact_interval"`
	// CompactThresholdFiles is the min same-level file count that triggers a
	// column compaction. Defaults to DefaultCompactThresholdFiles when <= 0.
	CompactThresholdFiles int `yaml:"compact_threshold_files"`
	// MaxOutputBytes is the approximate max serialized output-file size before
	// splitting. Defaults to DefaultMaxOutputBytes when < 0 is not allowed; 0 is
	// honored as "no split".
	MaxOutputBytes int64 `yaml:"max_output_bytes"`
	// Enabled turns the compactor on. When false the service does nothing.
	Enabled bool `yaml:"enabled"`
	// ShardCount is the total number of compactor replicas sharing work. When > 1
	// each replica processes only the columns whose hash maps to its ShardIndex,
	// splitting the column space evenly. 0 or 1 means no sharding (all columns).
	ShardCount int `yaml:"shard_count"`
	// ShardIndex is the zero-based index of this replica (0 .. ShardCount-1).
	// Columns are assigned by: int(colHash[0:2], 16) % ShardCount == ShardIndex.
	// Typically injected via the SHARD_INDEX environment variable.
	ShardIndex int `yaml:"shard_index"`
}

// withDefaults returns a copy of c with empty/zero fields filled in.
func (c Config) withDefaults() Config {
	if c.IndexPrefix == "" {
		c.IndexPrefix = DefaultIndexPrefix
	}
	if c.CompactInterval <= 0 {
		c.CompactInterval = DefaultCompactInterval
	}
	if c.CompactThresholdFiles <= 0 {
		c.CompactThresholdFiles = DefaultCompactThresholdFiles
	}
	if c.MaxOutputBytes < 0 {
		c.MaxOutputBytes = DefaultMaxOutputBytes
	}
	return c
}

// allTenants reports whether the config requests compaction of every tenant
// (a single "*" entry), in which case tenants are discovered by listing.
func (c Config) allTenants() bool {
	return len(c.Tenants) == 1 && c.Tenants[0] == "*"
}
