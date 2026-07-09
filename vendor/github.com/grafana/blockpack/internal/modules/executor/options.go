package executor

import "github.com/grafana/blockpack/internal/modules/queryplanner"

// Options is a blockpack data type.
//
// SPEC-STRUCT-13: Direction and RecentFirstBudget (issue #481 part 2, F-3) activate the bounded
// newest-first structural execution path for ExecuteStructural's 1e/1f decline categories
// (structural chains that flatten to other than exactly 2 nodes, any polarity). RecentFirstBudget
// != nil implies Direction == queryplanner.Backward should also be set by the caller —
// ExecuteStructural does not infer Direction from budget presence, mirroring
// executor.CollectOptions' explicit-field convention (F-2). When RecentFirstBudget is nil, both
// fields are ignored and ExecuteStructural behaves exactly as before this phase (full
// predicate-selected block set, no early stop, no incomplete-trace exclusion).
type Options struct {
	RecentFirstBudget *RecentFirstBudget
	TimeRange         queryplanner.TimeRange
	Limit             int
	StartBlock        int
	BlockCount        int
	Direction         queryplanner.Direction
}
