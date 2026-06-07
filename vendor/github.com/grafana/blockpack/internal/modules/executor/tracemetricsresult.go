package executor

// TraceMetricsResult is a blockpack data type.
type TraceMetricsResult struct {
	Series        []TraceTimeSeries
	BytesRead     int64
	BlocksScanned int
}
