package compaction

import (
	"bytes"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
)

type writerState struct {
	w         *modules_blockio.Writer
	buf       *bytes.Buffer
	spanCount int
}
