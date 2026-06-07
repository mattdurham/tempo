package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type parsedRangeIndex struct {
	entries          []rangeEntry
	float64BoundsRaw []byte
	stringBounds     []string
	bytesBounds      [][]byte
	bucketMin        int64
	bucketMax        int64
	colType          shared.ColumnType
}
