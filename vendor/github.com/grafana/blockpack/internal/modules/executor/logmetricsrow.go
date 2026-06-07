package executor

// LogMetricsRow is a blockpack data type.
type LogMetricsRow struct {
	Values   map[string]float64
	GroupKey []string
}
