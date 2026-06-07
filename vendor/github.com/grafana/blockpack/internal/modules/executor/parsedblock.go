package executor

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type parsedBlock struct {
	Block    *modules_reader.Block
	BlockIdx int
}
