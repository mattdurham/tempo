package compaction

import modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"

// Config is a blockpack data type.
type Config struct {
	StagingDir       string
	DedicatedColumns []modules_blockio.DedicatedColumn
	MaxSpansPerBlock int
	// MaxOutputFileSize removed: Tempo's compaction scheduler enforces max_block_bytes
	// on input selection, so output is already bounded. The internal size estimate
	// (fixed 2048 bytes/span) was inaccurate and could spuriously split the sort
	// window, breaking the global (svcName, spanName, minHash) sort guarantee.
}
