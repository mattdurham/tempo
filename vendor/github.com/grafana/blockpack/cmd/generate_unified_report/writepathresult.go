package main

type WritePathResult struct {
	Scale        string
	Format       string
	TimeNs       int64
	TimeMs       float64
	CPUMs        float64
	BytesWritten int64
	BytesMB      float64
	MemoryBytes  int64
	MemoryMB     float64
	Allocs       int64
	SpansWritten int64
	NsPerSpan    float64
}
