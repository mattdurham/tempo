package executor

// Explanation provides human-readable insight into query execution
type Explanation struct {
	Level    string // "INFO", "WARNING", "INSIGHT", "SUCCESS"
	Category string // "pruning", "index_usage", "slow_path", "optimization"
	Message  string
}

// OptimizationHint suggests improvements to query performance
type OptimizationHint struct {
	Category   string // "index", "query_rewrite", "time_range"
	Problem    string
	Suggestion string
	Impact     string // Estimated impact description
	Priority   int    // 1-5, where 5 is critical
}
