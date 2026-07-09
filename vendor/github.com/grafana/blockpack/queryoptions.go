package blockpack

import "time"

// RecentFirstBudget activates the bounded, pointed newest-first execution strategy
// (issue #481 part 2). Blocks are read newest-first (Direction=Backward, WantSort=false —
// deliberately NOT the globally-correct MostRecent topK heap path) and reading stops at the
// FIRST cap reached: Limit matches found, MaxBlocks blocks read, MaxBytes bytes read, or
// MaxDuration elapsed. A zero field in the struct means "no cap on that dimension" — but at
// least one of Limit/MaxBlocks/MaxBytes/MaxDuration MUST be positive, or the strategy has no
// way to ever stop (validateQueryOptions rejects an all-zero budget).
//
// This is a STRATEGY-ENGINE signal, not a user-facing hint — unlike MostRecent, which
// guarantees globally correct top-K order via a full scan of all selected blocks
// (see stream.go's topKScanBlocks), RecentFirstBudget explicitly trades that guarantee for a
// hard I/O bound: it is reached ONLY when tempo's frontend has classified the query as
// LowSelectivity/no-coverage with a limit present and decided a bounded-but-not-exhaustive
// read is an acceptable answer for THIS query shape. Never set this from a user's
// `with (most_recent=true)` hint. validateQueryOptions also rejects RecentFirstBudget being
// set together with MostRecent (mutually exclusive strategy-engine vs. user-facing fields).
//
// Never set for IndexOnly slice jobs — the tempo dispatch layer enforces this (R11);
// QueryOptions itself carries no IndexOnly notion, so there is nothing here to validate: a
// #487 time-sliced job's IndexOnly-ness is expressed at tempo's dispatch/TraceMetricOptions
// layer, never on this struct, and the dangerous combination (a slice job reaching the
// bounded path) can only arise there — enforced by the plan-time strategy decision
// (DispatchBoundedRecentFirst is only selected on the search-frontend path, never for
// DispatchTimeSliced-originated jobs) and an explicit querier-side test, not by this type.
//
// SPEC-ROOT-023: RecentFirstBudget bounded pointed-execution activation contract.
type RecentFirstBudget struct {
	MaxBlocks   int
	MaxBytes    int64
	MaxDuration time.Duration
}

// QueryOptions is a blockpack data type.
type QueryOptions struct {
	RecentFirstBudget *RecentFirstBudget
	SelectColumns     []string
	StartNano         uint64
	EndNano           uint64
	Limit             int
	StartBlock        int
	BlockCount        int
	MostRecent        bool
}
