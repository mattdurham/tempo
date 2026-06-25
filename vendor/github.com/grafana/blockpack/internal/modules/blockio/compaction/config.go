package compaction

import modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"

// Config is a blockpack data type.
type Config struct {
	StagingDir       string
	DedicatedColumns []modules_blockio.DedicatedColumn
	MaxSpansPerBlock int

	// OmitIntrinsicIdentityColumns drops the trace:id/span:id/span:parent_id columns from the
	// compacted output's IntrinsicTOC (NOTE-476, issue #394). Identity is served from the
	// SpanTree section instead. See WriterConfig.OmitIntrinsicIdentityColumns. The source
	// blocks' identity is sourced from their own SpanTree during recompaction when they were
	// themselves written with this flag.
	OmitIntrinsicIdentityColumns bool
	// MaxOutputFileSize removed: Tempo's compaction scheduler enforces max_block_bytes
	// on input selection, so output is already bounded. The internal size estimate
	// (fixed 2048 bytes/span) was inaccurate and could spuriously split the sort
	// window, breaking the global (svcName, spanName, minHash) sort guarantee.
}
