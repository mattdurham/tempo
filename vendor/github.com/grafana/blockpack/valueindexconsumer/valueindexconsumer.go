// Package valueindexconsumer is the importable surface for the value-index
// consumer service.
//
// The implementation lives in internal/modules/valueindexconsumer. Go's
// internal package rule prevents external modules (tempo) from importing that
// package directly, so this thin subpackage re-exports the minimal API needed
// to run the consumer: the Config, the Service, the Redis-backed Consumer, and
// the Consumer/Extractor/ObjectPutter interfaces the Service depends on.
//
// The consumer is the second stage of the value-index pipeline (publisher #397,
// consumer #398, compactor #399): it consumes blockpack "create" events, reads
// the referenced blockpack to extract per-column entries, accumulates them in
// per-column buffers, and flushes time-windowed L0 value index files to object
// storage. NOTE-VI-016.
package valueindexconsumer

import (
	"github.com/grafana/blockpack/internal/modules/valueindexconsumer"
)

// Config configures the value-index consumer (the optional
// `value_index_consumer` YAML block). When Enabled is false the service does
// nothing.
type Config = valueindexconsumer.Config

// Message is a single queue delivery: a block event plus its opaque ack ID.
type Message = valueindexconsumer.Message

// ColumnEntry is one extracted observation for one configured column.
type ColumnEntry = valueindexconsumer.ColumnEntry

// Consumer pulls block-event messages off a queue and acknowledges them.
type Consumer = valueindexconsumer.Consumer

// Extractor reads a blockpack file and yields per-column entries.
type Extractor = valueindexconsumer.Extractor

// ObjectPutter is the object-storage write surface the service needs.
// blockpack.WritableStorage satisfies it.
type ObjectPutter = valueindexconsumer.ObjectPutter

// Service is the consume → accumulate → flush orchestrator.
type Service = valueindexconsumer.Service

// RedisConsumer is a Redis Streams consumer-group Consumer.
type RedisConsumer = valueindexconsumer.RedisConsumer

// RqliteConsumer is a pull-based Consumer backed by the rqlite job table.
type RqliteConsumer = valueindexconsumer.RqliteConsumer

// Default configuration values.
const (
	DefaultStreamName           = valueindexconsumer.DefaultStreamName
	DefaultConsumerGroup        = valueindexconsumer.DefaultConsumerGroup
	DefaultFlushInterval        = valueindexconsumer.DefaultFlushInterval
	DefaultMaxColumnBufferBytes = valueindexconsumer.DefaultMaxColumnBufferBytes
	DefaultBatchSize            = valueindexconsumer.DefaultBatchSize
	DefaultPollTimeout          = valueindexconsumer.DefaultPollTimeout
	DefaultIndexPrefix          = valueindexconsumer.DefaultIndexPrefix
)

// NewService builds a consumer service. cfg.Columns must be non-empty; the
// consumer, extractor and store must be non-nil.
func NewService(cfg Config, consumer Consumer, extractor Extractor, store ObjectPutter) (*Service, error) {
	return valueindexconsumer.NewService(cfg, consumer, extractor, store)
}

// NewRedisConsumer dials Redis, ensures the consumer group exists, and returns a
// ready Consumer.
func NewRedisConsumer(cfg Config) (*RedisConsumer, error) {
	return valueindexconsumer.NewRedisConsumer(cfg)
}

// NewRqliteConsumer dials rqlite, ensures the job schema exists, and returns a
// ready pull-based Consumer backed by the job table.
func NewRqliteConsumer(cfg Config) (*RqliteConsumer, error) {
	return valueindexconsumer.NewRqliteConsumer(cfg)
}
