package traceqlparser

// MetricsQuery is a blockpack data type.
type MetricsQuery struct {
	Filter   *FilterExpression
	Pipeline *PipelineStage
}
