package executor

import "github.com/grafana/blockpack/internal/modules/queryplanner"

// CollectOptions is a blockpack data type.
type CollectOptions struct {
	TimestampColumn string
	SelectColumns   []string
	TimeRange       queryplanner.TimeRange
	Limit           int
	StartBlock      int
	BlockCount      int
	Direction       queryplanner.Direction
	AllColumns      bool
}
