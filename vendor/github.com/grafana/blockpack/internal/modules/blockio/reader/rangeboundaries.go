package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// RangeBoundaries is a blockpack data type.
type RangeBoundaries struct {
	Float64Bounds []float64
	StringBounds  []string
	BytesBounds   [][]byte
	BucketMin     int64
	BucketMax     int64
	ColType       shared.ColumnType
}
