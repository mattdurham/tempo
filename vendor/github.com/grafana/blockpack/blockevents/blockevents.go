// Package blockevents is the importable surface for publishing blockpack
// lifecycle events to a queue.
//
// The implementation lives in internal/modules/blockevents. Go's internal
// package rule prevents external modules (tempo) from importing that package
// directly, so this thin subpackage re-exports the minimal publisher API that
// tempo's block-creation and compaction paths need. NOTE-VI-015.
//
// After a blockpack file is created, a "create" event is published so the value
// index builder can index the block asynchronously. Deletions are handled
// organically by the value index compactor (source-existence check), so no
// delete events are emitted.
package blockevents

import (
	"github.com/grafana/blockpack/internal/modules/blockevents"
)

// Action is the kind of lifecycle event for a blockpack file.
type Action = blockevents.Action

// ActionCreate signals that a new blockpack file was written to object storage.
const ActionCreate = blockevents.ActionCreate

// Message is a single blockpack lifecycle event.
type Message = blockevents.Message

// Config configures block-event publishing (the optional `block_events` YAML
// block). When Enabled is false NewPublisher returns a no-op publisher.
type Config = blockevents.Config

// Publisher publishes blockpack lifecycle events to a queue. Implementations
// are safe for concurrent use.
type Publisher = blockevents.Publisher

// NoopPublisher discards every event. It is the zero-cost default when no queue
// is configured.
type NoopPublisher = blockevents.NoopPublisher

// ChanPublisher is an in-process Publisher delivering events on a channel, for
// tests and in-process consumers.
type ChanPublisher = blockevents.ChanPublisher

// RedisStreamsPublisher publishes events to a Redis stream, non-blocking on the
// hot path with a bounded internal buffer.
type RedisStreamsPublisher = blockevents.RedisStreamsPublisher

// ErrPublisherClosed is returned by Publish after the publisher has been closed.
var ErrPublisherClosed = blockevents.ErrPublisherClosed

// NewPublisher returns a Publisher for cfg. When cfg.Enabled is false it returns
// a NoopPublisher; otherwise it dials Redis and starts a background drain
// goroutine.
func NewPublisher(cfg Config) (Publisher, error) {
	return blockevents.NewPublisher(cfg)
}

// NewNoopPublisher returns a Publisher that discards all events.
func NewNoopPublisher() *NoopPublisher {
	return blockevents.NewNoopPublisher()
}

// NewChanPublisher returns an in-process ChanPublisher with the given buffer size.
func NewChanPublisher(buffer int) *ChanPublisher {
	return blockevents.NewChanPublisher(buffer)
}
