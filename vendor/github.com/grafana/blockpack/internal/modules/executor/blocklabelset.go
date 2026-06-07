package executor

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type blockLabelSet struct {
	block    *modules_reader.Block
	colMap   map[string]int
	overlay  map[string]string
	deleted  map[string]bool
	colNames []string
	colCols  []*modules_reader.Column
	rowIdx   int
}
