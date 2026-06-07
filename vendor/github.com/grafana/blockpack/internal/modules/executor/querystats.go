package executor

import "time"

// QueryStats is a blockpack data type.
type QueryStats struct {
	ExecutionPath string
	Steps         []StepStats
	TotalDuration time.Duration
}
