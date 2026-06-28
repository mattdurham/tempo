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

	// RestoreIdentityBlockColumns writes the trace:id/span:id/span:parent_id columns back into
	// the compacted output's per-inner-block payloads (NOTE-V2-004, issue #420). This makes each
	// block self-contained for v2 direct ranged-GET fetch (#424). See
	// WriterConfig.RestoreIdentityBlockColumns. Defaults OFF (identity intrinsic-only per
	// NOTE-469); the v1→v2 rewrite pass (#425) sets it on compaction output.
	RestoreIdentityBlockColumns bool

	// OmitIntrinsicTOC skips writing the file-level IntrinsicTOC section in the compacted
	// output (NOTE-V2-005, issue #421). See WriterConfig.OmitIntrinsicTOC. Only effective
	// alongside RestoreIdentityBlockColumns (the writer enforces this); the v1→v2 rewrite pass
	// (#425) sets both so the compacted output is a fully self-contained v2 file with no
	// redundant IntrinsicTOC. Defaults OFF (IntrinsicTOC written as before).
	OmitIntrinsicTOC bool
	// MaxOutputFileSize removed: Tempo's compaction scheduler enforces max_block_bytes
	// on input selection, so output is already bounded. The internal size estimate
	// (fixed 2048 bytes/span) was inaccurate and could spuriously split the sort
	// window, breaking the global (svcName, spanName, minHash) sort guarantee.
}
