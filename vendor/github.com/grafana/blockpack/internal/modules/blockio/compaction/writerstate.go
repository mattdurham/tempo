package compaction

import (
	"os"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
)

type writerState struct {
	w          *modules_blockio.Writer
	f          *os.File // staging file — writer streams directly to disk, not to memory
	stagedPath string
	spanCount  int
}
