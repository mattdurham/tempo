package writer

type int64ColumnBuilder struct {
	values  []int64
	present []bool
	minVal  int64
	maxVal  int64
	hasVals bool
}
