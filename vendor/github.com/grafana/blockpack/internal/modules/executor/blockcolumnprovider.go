package executor

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

type blockColumnProvider struct {
	block        *modules_reader.Block
	scratch      []int // reusable backing store for collectStreamInto; see NOTE-107
	scratchInUse bool  // true while scratch is borrowed by an active rowSet
}
