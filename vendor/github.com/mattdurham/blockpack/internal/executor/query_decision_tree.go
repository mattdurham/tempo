package executor

// DecisionTreeNode represents a node in the query execution decision tree.
// Each node represents a stage in query execution (pruning, scanning, etc.)
// and tracks the decisions made at that stage.
type DecisionTreeNode struct {
	Metrics       map[string]interface{} `json:"metrics"`       // Additional metrics (bytes read, matches, etc.)
	Stage         string                 `json:"stage"`         // Stage name (e.g., "time_range", "dedicated_index", "scanning")
	Reason        string                 `json:"reason"`        // Human-readable reason for decisions
	Decisions     []BlockDecisionDetail  `json:"decisions"`     // Per-block decisions at this stage
	Children      []*DecisionTreeNode    `json:"children"`      // Next stages in execution
	InputBlocks   int                    `json:"inputBlocks"`   // Blocks entering this stage
	OutputBlocks  int                    `json:"outputBlocks"`  // Blocks leaving this stage
	PrunedCount   int                    `json:"prunedCount"`   // Blocks pruned at this stage
	PrunedPercent float64                `json:"prunedPercent"` // Percentage pruned (0-100)
	FastPath      bool                   `json:"fastPath"`      // True if this is a fast execution path
	SlowPath      bool                   `json:"slowPath"`      // True if this is a slow execution path
}

// BlockDecisionDetail provides detailed information about a single block's decision
type BlockDecisionDetail struct {
	Decision   string `json:"decision"`   // "pruned", "passed", "scanned"
	Reason     string `json:"reason"`     // Reason for decision (e.g., "time_range_before_query")
	BlockID    int    `json:"blockId"`    // Block index
	SpanCount  int    `json:"spanCount"`  // Number of spans in this block
	BytesRead  int64  `json:"bytesRead"`  // Bytes read from disk (if scanned)
	MatchCount int    `json:"matchCount"` // Number of matching spans (if scanned)
}
