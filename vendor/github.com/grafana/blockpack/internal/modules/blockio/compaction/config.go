package compaction

import (
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// Config is a blockpack data type.
type Config struct {
	// NOTE: EnableV2Output removed (2026-06-29, v2 lean format unconditional).
	// All output is now FooterV9 (v2 lean format).
	// MaxOutputFileSize removed: Tempo's compaction scheduler enforces max_block_bytes
	// on input selection, so output is already bounded. The internal size estimate
	// (fixed 2048 bytes/span) was inaccurate and could spuriously split the sort
	// window, breaking the global (svcName, spanName, minHash) sort guarantee.

	// ValueIndexSink, if non-nil, is called once per output blockpack file after it
	// is successfully flushed. The Reader is opened over the staged file bytes and is
	// owned by the compactor; the sink MUST NOT close it. Any error returned by the
	// sink aborts compaction for that output file.
	// This replaces the async rqlite/Redis event pipeline for callers that prefer
	// synchronous in-process value indexing.
	ValueIndexSink   func(r *reader.Reader) error
	StagingDir       string
	DedicatedColumns []modules_blockio.DedicatedColumn
	MaxSpansPerBlock int

	// OmitIntrinsicIdentityColumns historically dropped trace:id/span:id/span:parent_id from
	// the compacted output's IntrinsicTOC (NOTE-476, issue #394), relying on the SpanTree as a
	// fallback identity store. The IntrinsicTOC (#433) and SpanTree (#434) have been removed;
	// v2 identity lives in block columns. See WriterConfig.OmitIntrinsicIdentityColumns —
	// should remain OFF.
	OmitIntrinsicIdentityColumns bool
}
