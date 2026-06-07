package compaction

import modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"

// Config is a blockpack data type.
type Config struct {
	StagingDir        string
	DedicatedColumns  []modules_blockio.DedicatedColumn
	MaxOutputFileSize int64
	MaxSpansPerBlock  int
}
