package vm

// TimeBucketSpec is a blockpack data type.
type TimeBucketSpec struct {
	Enabled       bool
	StartTime     int64
	EndTime       int64
	StepSizeNanos int64
}
