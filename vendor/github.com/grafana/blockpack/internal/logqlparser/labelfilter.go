package logqlparser

// LabelFilter is a blockpack data type.
type LabelFilter struct {
	Name  string
	Value string
	Op    FilterOp
}
