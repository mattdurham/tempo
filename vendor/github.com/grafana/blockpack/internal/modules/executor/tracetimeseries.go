package executor

// TraceTimeSeries is a blockpack data type.
type TraceTimeSeries struct {
	Labels []TraceMetricLabel
	Values []float64
}
