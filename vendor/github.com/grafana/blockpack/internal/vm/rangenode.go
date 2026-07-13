package vm

// RangeNode is a blockpack data type.
type RangeNode struct {
	Min          *Value
	Max          *Value
	Column       string
	Pattern      string
	Values       []Value
	Children     []RangeNode
	MinInclusive bool
	MaxInclusive bool
	IsOR         bool
	// RequirePresent marks a leaf that can match only rows where Column is present
	// (non-null). Set for "!= \"\"" predicates, which cannot be expressed as a value
	// range but still require the attribute to exist. Used by the executor's ColStats
	// block pruning (NOTE-446, issue #364) to skip blocks where present_count == 0.
	// A RequirePresent leaf carries no Values/Min/Max — it constrains existence only.
	RequirePresent bool
	// NeqPairedRange marks a RequirePresent leaf produced by the SCOPED `attr != V` rewrite
	// (extractNeqNode/extractNeqNumericNode, NOTE-453/454) whose immediate next sibling, in
	// the SAME node slice these functions return, is the value-bearing `attr > V OR attr < V`
	// range-OR composite from that exact same rewrite call.
	//
	// task #213 (CRITICAL regression fix): NEVER set for the UNSCOPED rewrite's
	// OR-of-two-RequirePresent-leaves shape — that shape has no value-bearing sibling of its
	// own at all, by design (composing per-scope range-OR across two scopes was judged not
	// worth the complexity — see extractNeqNode's own unscoped-branch comment). Also never set
	// for the scoped rewrite against an intrinsic-refs-fast-path column
	// (isIntrinsicRefsColumn skips the range-OR append entirely for those columns, so there is
	// no sibling to pair with either).
	//
	// vibuilder's leaf loop (collectLeaves, builder.go) uses this bit, and ONLY this bit, to
	// decide whether a RequirePresent leaf may be paired with a specific sibling's own leaf
	// data via SliceValueIndexSource.MarkRequirePresentLeaf — never via a bare column-name
	// match, which task #213 found could silently substitute an unrelated leaf's data
	// whenever an independent leaf elsewhere in the query happened to reference the identical
	// expanded column name (exactly what unscopedCols produces for every unscoped attribute).
	NeqPairedRange bool
}
