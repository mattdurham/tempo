package benchmark

import "time"

type QueryResult struct {
	RowCount      int
	Elapsed       time.Duration
	IOOps         int
	BytesRead     int64
	BlocksScanned int
	TraceCount    int
}
