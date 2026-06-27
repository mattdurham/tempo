// Package valueindexconsumer implements the value-index consumer service.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// The consumer is the second stage of the value-index pipeline (publisher #397,
// consumer #398, compactor #399). It pulls blockpack "create" events off a
// queue, extracts per-column (value, trace_id, source_ref, block_id, time_sec)
// tuples from the referenced blockpack file, accumulates them in per-column
// in-memory buffers, and flushes time-windowed L0 value index files to object
// storage.
//
// # Pipeline
//
//	pull messages from queue (Redis Streams consumer group)
//	  → for each message: extract per-column entries from the blockpack
//	  → accumulate in a per-column buffer
//	  → flush a column when its buffer exceeds the size limit OR the flush timer fires
//	  → write one L0 value index file per flushed column to object storage
//	  → ack the messages that contributed only after their flush succeeds
//
// # Key properties
//
//   - Per-column independent flush: a hot column does not block a sparse one.
//   - Time-windowed output: each L0 file covers a bounded window so files are
//     naturally sortable by creation time (xid).
//   - At-least-once with ack-after-flush: messages are acked only after the S3
//     writes that depend on them succeed. A crash mid-flush leaves messages
//     unacked; they are redelivered and any duplicate entries are removed by the
//     value index compactor.
//
// # Decoupling
//
// The blockpack-reading step is abstracted behind the Extractor interface so the
// orchestration (accumulate/flush/ack) is unit-testable without a real reader or
// object store, and so the reader integration can evolve independently.
package valueindexconsumer
