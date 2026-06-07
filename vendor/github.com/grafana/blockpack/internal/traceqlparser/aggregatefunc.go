package traceqlparser

// AggregateFunc is a blockpack data type.
type AggregateFunc struct {
	Name     string
	Field    string
	Quantile float64
}
