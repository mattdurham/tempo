package main

type OTELDemoResult struct {
	QueryName          string
	Format             string
	TimeNs             int64
	TimeMs             float64
	CPUMs              float64
	BytesRead          int64
	BytesReadMB        float64
	IOOps              int64
	BlocksScanned      int64
	MemoryBytes        int64
	MemoryMB           float64
	Allocs             int64
	Traces             int64
	Spans              int64
	CostS3Get          float64
	CostS3Xfer         float64
	CostLambdaCompute  float64
	CostLambdaRequests float64
	CostTotal          float64
}
