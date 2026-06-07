package logqlparser

// VectorAggExpr is a blockpack data type.
type VectorAggExpr struct {
	Inner   *MetricExpr
	Op      string
	GroupBy []string
	Without []string
	Param   int
}
