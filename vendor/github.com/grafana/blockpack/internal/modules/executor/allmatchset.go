package executor

// allMatchSet is a RowSet that matches every row index in [0, n).
type allMatchSet struct {
	n int
}
