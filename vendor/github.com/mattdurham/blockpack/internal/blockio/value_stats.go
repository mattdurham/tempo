package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// StatsType is an alias to shared.StatsType for backward compatibility
type StatsType = shared.StatsType

const (
	// StatsTypeNone indicates no statistics.
	StatsTypeNone = shared.StatsTypeNone
	// StatsTypeString indicates string statistics.
	StatsTypeString = shared.StatsTypeString
	// StatsTypeInt64 indicates int64 statistics.
	StatsTypeInt64 = shared.StatsTypeInt64
	// StatsTypeFloat64 indicates float64 statistics.
	StatsTypeFloat64 = shared.StatsTypeFloat64
	// StatsTypeBool indicates boolean statistics.
	StatsTypeBool = shared.StatsTypeBool
)

// AttributeStats is an alias to shared.AttributeStats for backward compatibility
type AttributeStats = shared.AttributeStats
