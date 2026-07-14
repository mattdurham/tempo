package executor

// TraceMetricsResult is a blockpack data type.
type TraceMetricsResult struct {
	Series []TraceTimeSeries
	// IndexBytesRead is the total byte size of value-index files downloaded from object
	// storage while resolving this metrics query — ExecuteTraceMetricsFromVI's queried
	// ValueIndexSource's ValueIndexBuildStats.BytesRead, when the source tracks it (issue
	// #218). Zero for a source that doesn't record file I/O (e.g. a test fake).
	IndexBytesRead int64
	BlocksScanned  int
}
