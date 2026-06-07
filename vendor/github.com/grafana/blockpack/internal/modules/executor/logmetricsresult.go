package executor

// LogMetricsResult is a blockpack data type.
type LogMetricsResult struct {
	Rows          []LogMetricsRow
	BytesRead     int64
	BlocksScanned int
}
