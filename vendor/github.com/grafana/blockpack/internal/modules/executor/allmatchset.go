package executor

// allMatchSet is a RowSet that matches every row index in [0, n).
type allMatchSet struct {
	n int
}

// emptyRowSet is a RowSet that contains no rows. It is the result of evaluating a structural
// node's predicate against a block that the node's own planBlocks set excluded: block-level
// range/intrinsic-TOC pruning is exact (and bloom pruning carries the same already-accepted
// file-level FPR semantics), so a block absent from a node's selected set has no span matching
// that node — its rowset is necessarily empty. Returning this sentinel instead of running
// ColumnPredicate skips the node's user-attribute column decode/scan on that block (NOTE-425).
type emptyRowSet struct{}

func (emptyRowSet) Add(_ int)           {}
func (emptyRowSet) Contains(_ int) bool { return false }
func (emptyRowSet) Size() int           { return 0 }
func (emptyRowSet) IsEmpty() bool       { return true }
func (emptyRowSet) ToSlice() []int      { return nil }
