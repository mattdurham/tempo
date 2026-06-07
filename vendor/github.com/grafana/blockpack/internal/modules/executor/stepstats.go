package executor

import "time"

// StepStats is a blockpack data type.
type StepStats struct {
	Metadata  map[string]any
	Name      string
	Duration  time.Duration
	BytesRead int64
	IOOps     int
}
