package executor

// Query planning and explanation constants
const (
	// Efficiency thresholds for query explanations
	HighEfficiencyThreshold     = 80.0
	ModerateEfficiencyThreshold = 50.0
	LowEfficiencyThreshold      = 20.0

	// Fast/slow path classification
	FastPathPruningThreshold = 50.0
	SlowPathPruningThreshold = 20.0

	// Hint priorities
	CriticalPriority = 5
	HighPriority     = 4
	MediumPriority   = 3
	LowPriority      = 2

	// Tree traversal limits
	MaxTreeDepth = 100

	// Display and formatting
	MaxQueryDisplayLength    = 80
	QueryTruncationSuffix    = 3 // "..." length
	MinPruningForHint        = 60
	DedicatedIndexHintMin    = 30
	LowEfficiencyThreshold10 = 10

	// Query limits for DoS protection
	MaxQueryLimit = 2000000 // Maximum number of results to prevent memory exhaustion

	// PlantUML color threshold
	PlantUMLYellowThreshold = 30
)
