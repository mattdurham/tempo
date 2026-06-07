package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type dictAccum struct {
	index    map[string]int
	numIndex map[[8]byte]int
	entries  []dictEntry
	colType  shared.ColumnType
}
