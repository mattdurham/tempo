package blockpack

// LogMetricOptions is a blockpack data type.
type LogMetricOptions struct {
	GroupBy   []string
	StartNano int64
	EndNano   int64
	StepNano  int64
}
