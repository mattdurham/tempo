package writer

type bytesColumnBuilder struct {
	colName string
	values  [][]byte
	present []bool
}
