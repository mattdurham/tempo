package main

type TraceMetricsComparisonResult struct {
	QueryName          string
	Format             string
	TimeNs             int64
	TimeMs             float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	BytesPerIO         float64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Groups             int64
	TimeValues         int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}
