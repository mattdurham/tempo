package valuecountscompactor

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE: see internal/modules/valuecountscompactor/NOTES.md.
// Any changes to this file must be reflected there.

// Default configuration values for the value-counts compactor.
const (
	// DefaultCompactInterval is the wall-clock period between compaction passes
	// over all tenants and columns.
	DefaultCompactInterval = 5 * time.Minute
	// DefaultCompactThresholdFiles is the number of same-level files a column
	// directory must accumulate before it is compacted.
	DefaultCompactThresholdFiles = 2
	// DefaultCompactBatchBytes is the maximum total input bytes per merge job.
	// Files are added to the batch (oldest first) until adding the next one
	// would exceed this limit. 0 means no cap.
	DefaultCompactBatchBytes int64 = 1 << 30 // 1 GiB
	// DefaultMaxRecordsPerMerge bounds peak decoded memory directly (~300B RSS/record,
	// team-brainstormer's estimate, brainstorm.md Addendum 2) since CompactBatchBytes alone
	// caps compressed bytes, not decoded record count, and weak compression at VCNT's small
	// per-file sizes can let a byte-capped batch admit far more records than the byte number
	// suggests. Estimate pending real dev-03 file-count/cardinality data.
	DefaultMaxRecordsPerMerge = 3_000_000
	// DefaultMaxTimeSpanPerMerge bounds the wall-clock width [minSec,maxSec] a single cluster
	// of same-level files may span before compactColumn splits it into a separate cluster
	// (issue #494). 24h is a conservative starting default pending real tuning data from
	// production query-window/retention observation — see NOTES.md.
	DefaultMaxTimeSpanPerMerge uint64 = 24 * 60 * 60 // 86400 seconds
)

// Config configures the value-counts compactor. It maps to the optional
// `value_counts_compactor` YAML block.
//
//	value_counts_compactor:
//	  compact_interval: 5m
//	  compact_threshold_files: 8
//	  tenants:
//	    - "11638"     # explicit list, or "*" for all tenants
type Config struct {
	// Registerer is the Prometheus registerer used to expose value-counts
	// compactor metrics. When nil, all metrics are no-ops and no registration
	// occurs. It is not settable from YAML; the embedder (tempo) injects it
	// programmatically.
	Registerer prometheus.Registerer `yaml:"-"`
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
	// CompactBatchBytes is the maximum total input size (bytes) per merge job.
	// Files are accumulated oldest-first until the next file would push the
	// total over this limit. Defaults to DefaultCompactBatchBytes when <= 0.
	CompactBatchBytes int64 `yaml:"compact_batch_bytes"`
	// MaxRecordsPerMerge bounds the number of decoded records admitted into a
	// single mergeLevel call, independent of CompactBatchBytes' compressed-byte
	// cap. Defaults to DefaultMaxRecordsPerMerge when <= 0.
	MaxRecordsPerMerge int `yaml:"max_records_per_merge"`
	// MaxTimeSpanPerMerge bounds the wall-clock time span [minSec,maxSec] a single
	// time-cluster of same-level candidate files may span before compactColumn splits it into
	// a separate cluster (issue #494). Defaults to DefaultMaxTimeSpanPerMerge when 0 — this
	// differs from clusterByTimeRange's own pure-function "0 means no cap" semantics, since no
	// production code path can invoke clusterByTimeRange with 0 after withDefaults runs; see
	// clusterByTimeRange's doc comment.
	MaxTimeSpanPerMerge uint64 `yaml:"max_time_span_per_merge"`
	// Enabled turns the compactor on. When false the service does nothing.
	Enabled bool `yaml:"enabled"`
	// ShardCount is the total number of compactor replicas sharing work. When > 1
	// each replica processes only the columns whose hash maps to its ShardIndex,
	// splitting the column space evenly. 0 or 1 means no sharding (all columns).
	ShardCount int `yaml:"shard_count"`
	// ShardIndex is the zero-based index of this replica (0 .. ShardCount-1).
	// Typically injected via the SHARD_INDEX environment variable.
	ShardIndex int `yaml:"shard_index"`
}

// withDefaults returns a copy of c with empty/zero fields filled in.
func (c Config) withDefaults() Config {
	if c.CompactInterval <= 0 {
		c.CompactInterval = DefaultCompactInterval
	}
	if c.CompactThresholdFiles <= 0 {
		c.CompactThresholdFiles = DefaultCompactThresholdFiles
	}
	if c.CompactBatchBytes <= 0 {
		c.CompactBatchBytes = DefaultCompactBatchBytes
	}
	if c.MaxRecordsPerMerge <= 0 {
		c.MaxRecordsPerMerge = DefaultMaxRecordsPerMerge
	}
	if c.MaxTimeSpanPerMerge == 0 {
		c.MaxTimeSpanPerMerge = DefaultMaxTimeSpanPerMerge
	}
	return c
}

// allTenants reports whether the config requests compaction of every tenant
// (a single "*" entry), in which case tenants are discovered by listing.
func (c Config) allTenants() bool {
	return len(c.Tenants) == 1 && c.Tenants[0] == "*"
}
