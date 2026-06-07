package vm

// AggregateSpec is a blockpack data type.
type AggregateSpec struct {
	Function string
	Field    string
	GroupBy  []string
	Quantile float64
}
