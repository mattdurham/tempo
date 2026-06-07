package executor

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type topKEntry struct {
	block    *modules_reader.Block
	ts       uint64
	blockIdx int
	rowIdx   int
}
