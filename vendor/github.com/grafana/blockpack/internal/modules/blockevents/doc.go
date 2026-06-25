// Package blockevents defines a minimal, queue-agnostic publisher for blockpack
// lifecycle events.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// After a blockpack file is created (by the block-builder or by the compactor),
// a "create" event is published so downstream consumers — notably the value
// index builder — can rebuild or maintain their state asynchronously. Deletions
// are NOT published: the value index compactor handles retention organically by
// checking whether each entry's source blockpack still exists.
//
// # Message format
//
//	{"action": "create", "path": "s3://bucket/<tenant>/<block-id>/data.blockpack"}
//
// The message is intentionally minimal — the consumer reads tenant, columns and
// entries from the file itself.
//
// # Implementations
//
//   - RedisStreamsPublisher — production publisher backed by Redis Streams.
//     Non-blocking on the hot path: messages are enqueued onto a bounded internal
//     buffer and drained by a background goroutine. If the buffer is full the
//     message is dropped (the value index can always rebuild from scratch, so a
//     lost create event is not fatal).
//   - NoopPublisher — zero-cost default when no queue is configured.
//   - ChanPublisher — in-process publisher exposing a channel, for tests.
//
// The Publisher interface is deliberately small so SQS, NATS or Kafka backends
// can be added later without changing callers.
package blockevents
