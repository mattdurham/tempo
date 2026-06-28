package valueindexconsumer

import "time"

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// Default configuration values for the value-index consumer.
const (
	// DefaultStreamName is the Redis stream key consumed when none is set. It
	// must match the publisher's stream (blockevents.DefaultStreamName).
	DefaultStreamName = "blockpack-events"
	// DefaultConsumerGroup is the Redis Streams consumer group used when none is
	// set. All consumer instances share one group so each message is processed
	// exactly once.
	DefaultConsumerGroup = "value-index-builders"
	// DefaultFlushInterval is the elapsed-time flush window evaluated between
	// messages (NOTE-VI-020). Each output file covers roughly this much
	// wall-clock time, keeping files sortable by xid. It is sized to safely
	// exceed a single L1 block's processing time so the between-message check
	// fires reliably rather than being perpetually deferred by one long ingest.
	DefaultFlushInterval = 15 * time.Minute
	// DefaultMaxColumnBufferBytes is the per-column buffer size that triggers a
	// size-driven flush independently of the timer.
	DefaultMaxColumnBufferBytes = 64 << 20 // 64 MiB
	// DefaultBatchSize is the max number of jobs claimed per Poll call.
	// Set to 1: claim one job at a time, process it, then claim the next.
	// The flush timer accumulates entries from multiple jobs and flushes
	// them together — the batch is time-driven, not claim-driven.
	DefaultBatchSize = 1
	// DefaultPollTimeout bounds how long a poll blocks waiting for messages
	// before returning empty so the flush timer can be evaluated.
	DefaultPollTimeout = 5 * time.Second
	// DefaultIndexPrefix is the object-storage key prefix under which value
	// index files are written.
	DefaultIndexPrefix = "indexes"
	// DefaultClaimBatchSize is the max number of stale-pending entries claimed
	// per XAutoClaim cursor step during startup reclaim.
	DefaultClaimBatchSize = 500
)

// Config configures the value-index consumer. It maps to the optional
// `value_index_consumer` YAML block.
//
//	value_index_consumer:
//	  redis_addr: "redis:6379"
//	  stream_name: "blockpack-events"
//	  consumer_group: "value-index-builders"
//	  flush_interval: 5m
//	  max_column_buffer_bytes: 67108864
//	  columns:
//	    - span:name
//	    - resource.service.name
type Config struct {
	// RedisAddr is the host:port of the Redis server backing the stream.
	RedisAddr string `yaml:"redis_addr"`
	// RqliteURL is the HTTP URL of the rqlite cluster backing the job table
	// (e.g. "http://rqlite:4001"). Used by NewRqliteConsumer; ignored by the
	// Redis consumer. See NOTE-VI-021.
	RqliteURL string `yaml:"rqlite_url"`
	// StreamName is the Redis stream key consumed. Defaults to
	// DefaultStreamName when empty.
	StreamName string `yaml:"stream_name"`
	// ConsumerGroup is the Redis Streams consumer group name. Defaults to
	// DefaultConsumerGroup when empty.
	ConsumerGroup string `yaml:"consumer_group"`
	// ConsumerName uniquely identifies this consumer instance within the group.
	// When empty the Redis consumer is constructed with a generated name.
	ConsumerName string `yaml:"consumer_name"`
	// IndexPrefix is the object-storage key prefix for value index files.
	// Defaults to DefaultIndexPrefix when empty.
	IndexPrefix string `yaml:"index_prefix"`
	// Columns is the set of column names to index. Columns not in this list are
	// ignored. Must be non-empty.
	Columns []string `yaml:"columns"`
	// FlushInterval is the timer-driven flush window. Defaults to
	// DefaultFlushInterval when <= 0.
	FlushInterval time.Duration `yaml:"flush_interval"`
	// PollTimeout bounds a single queue poll. Defaults to DefaultPollTimeout
	// when <= 0.
	PollTimeout time.Duration `yaml:"poll_timeout"`
	// ClaimIdleThreshold is the minimum idle time a pending (delivered but
	// unacked) message must have accumulated before this consumer reclaims it
	// from a dead/restarted instance on startup. Defaults to 2 × PollTimeout
	// when <= 0. See NOTE-VI-019.
	ClaimIdleThreshold time.Duration `yaml:"claim_idle_threshold"`
	// BatchSize is the max messages fetched per poll. Defaults to
	// DefaultBatchSize when <= 0.
	BatchSize int `yaml:"batch_size"`
	// Enabled turns the consumer on. When false the service does nothing.
	Enabled bool `yaml:"enabled"`
}

// withDefaults returns a copy of c with empty/zero fields filled in.
func (c Config) withDefaults() Config {
	if c.StreamName == "" {
		c.StreamName = DefaultStreamName
	}
	if c.ConsumerGroup == "" {
		c.ConsumerGroup = DefaultConsumerGroup
	}
	if c.IndexPrefix == "" {
		c.IndexPrefix = DefaultIndexPrefix
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultFlushInterval
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = DefaultPollTimeout
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.ClaimIdleThreshold <= 0 {
		c.ClaimIdleThreshold = 2 * c.PollTimeout
	}
	return c
}

// columnSet returns the configured columns as a lookup set.
func (c Config) columnSet() map[string]struct{} {
	set := make(map[string]struct{}, len(c.Columns))
	for _, col := range c.Columns {
		set[col] = struct{}{}
	}
	return set
}
