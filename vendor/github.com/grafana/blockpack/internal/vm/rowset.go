package vm

// RowSet is a blockpack data type.
type RowSet interface {
	Add(rowIdx int)
	Contains(rowIdx int) bool
	Size() int
	IsEmpty() bool
	ToSlice() []int
}
