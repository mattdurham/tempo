package writer

type uint64ColumnBuilder struct {
	colName string
	values  []uint64
	present []bool
	minVal  uint64
	maxVal  uint64
	hasVals bool
}
