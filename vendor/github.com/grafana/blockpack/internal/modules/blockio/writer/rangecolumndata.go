package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type rangeColumnData struct {
	values        map[string][]uint32
	numValues     map[[8]byte][]uint32
	kllInt64      *KLL[int64]
	kllUint64     *KLL[uint64]
	kllFloat64    *KLL[float64]
	kllStr        *KLLString
	kllBytes      *KLLBytes
	blocks        []blockRange
	boundaries    []int64
	float64Bounds []float64
	stringBounds  []string
	bytesBounds   [][]byte
	bucketMin     int64
	bucketMax     int64
	colType       shared.ColumnType
}
