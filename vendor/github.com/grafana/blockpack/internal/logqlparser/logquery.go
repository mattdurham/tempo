package logqlparser

// LogQuery is a blockpack data type.
type LogQuery struct {
	Selector  *LogSelector
	Metric    *MetricExpr
	VectorAgg *VectorAggExpr
}
