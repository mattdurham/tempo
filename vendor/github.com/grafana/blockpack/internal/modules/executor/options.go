package executor

import "github.com/grafana/blockpack/internal/modules/queryplanner"

// Options is a blockpack data type.
type Options struct {
	TimeRange  queryplanner.TimeRange
	Limit      int
	StartBlock int
	BlockCount int
}
