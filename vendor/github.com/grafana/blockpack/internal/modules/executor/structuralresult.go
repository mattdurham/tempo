package executor

// StructuralResult is a blockpack data type.
//
// SPEC-STRUCT-13: BudgetStopped and BlocksRead (issue #481 part 2, F-3) are populated only when
// Options.RecentFirstBudget was set; they are zero-value for the unbounded path (unchanged
// behavior). BudgetStopped is true when a MaxBlocks/MaxBytes/MaxDuration cap stopped block
// collection before the full predicate-selected block set was read.
//
// SPEC-STRUCT-14: IncompleteTraceCount is the number of traces excluded WHOLESALE from
// evaluation because at least one assembled span carried a non-empty parent reference that did
// not resolve within the read block set — the bounded path skips the unbounded path's cross-block
// trace-completion scan (R15), so such a reference means the true ancestor may live in an unread
// block, not that it genuinely doesn't exist. Excluding wholesale (never partially evaluating)
// avoids a false positive on negated operators (!>>, !>, !~) that would otherwise trigger on a
// merely-unread ancestor. Also zero-value for the unbounded path.
type StructuralResult struct {
	Matches              []SpanMatch
	IncompleteTraceCount int
	BlocksRead           int
	BudgetStopped        bool
}
