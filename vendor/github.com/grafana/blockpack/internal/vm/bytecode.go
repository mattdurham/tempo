// Package vm provides virtual machine execution for query evaluation.
package vm

// Value represents a runtime value in the VM

// int64, float64, string, bool, []byte, []Value, or nil

// ValueType represents the runtime type of a value in the VM.
type ValueType byte

const (
	// TypeNil represents a nil value.
	TypeNil ValueType = iota
	// TypeInt represents an int64 value.
	TypeInt
	// TypeFloat represents a float64 value.
	TypeFloat
	// TypeString represents a string value.
	TypeString
	// TypeBool represents a boolean value.
	TypeBool
	// TypeDuration represents a duration value (int64 nanoseconds).
	TypeDuration
	// TypeBytes represents a byte slice value.
	TypeBytes
)

// ColumnPredicate is a compiled query closure that executes against blockpack data.
// This is the core of the closure-based execution model - the SQL/TraceQL compiler
// generates this function directly, eliminating the need for bytecode interpretation.
type ColumnPredicate func(provider ColumnDataProvider) (RowSet, error)

// RowCallback is called for each row that matches the predicate
// Returns false to stop iteration early
type RowCallback func(rowIdx int) bool

// StreamingColumnPredicate filters rows and calls callback for each match
// Returns number of matches and any error
type StreamingColumnPredicate func(provider ColumnDataProvider, callback RowCallback) (int, error)

// RangeNode is a node in the block-pruning predicate tree.
//
// Leaf node (len(Children) == 0): Column names the fully-scoped column to look up.
// At most one of Values, Min/Max, or Pattern is meaningful:
//   - Values non-empty: equality / point-lookup (values are OR'd together).
//   - Min or Max non-nil: interval lookup for range predicates (>, >=, <, <=).
//   - Pattern non-empty: regex pattern; buildPredicates extracts a literal prefix.
//
// Composite node (len(Children) > 0): IsOR controls combination semantics.
//   - IsOR=false (AND): block must satisfy ALL children.
//   - IsOR=true  (OR):  block must satisfy AT LEAST ONE child with a range index;
//     children whose column has no range index are skipped (treated as empty).
//
// Unscoped attribute predicates (e.g. .service.name = "bob") are expanded at compile
// time into OR composites covering resource.*, span.*, and log.* scoped children.

// interval lower bound (nil = no lower bound)
// interval upper bound (nil = no upper bound)

// Leaf fields — set when len(Children) == 0.
// fully-scoped column (e.g. "resource.service.name", "span:duration")
// regex pattern for prefix-based range pruning

// equality lookup values; multiple values are OR'd

// MinInclusive is true when Min comes from >= (>= x) rather than > (> x).
// Used by the intrinsic flat-column scan to decide inclusive vs exclusive lower bound.

// MaxInclusive is true when Max comes from <= (<= x) rather than < (< x).

// Composite fields — set when len(Children) > 0.

// QueryPredicates holds block-level pruning predicates extracted from a compiled query.

// Nodes is the top-level AND-combined list of pruning predicates.
// Each node is either a leaf (Column + values/range/pattern) or a composite (IsOR + Children).

// Columns lists every attribute column accessed by the query.
// Used by ProgramWantColumns to select columns for the first-pass block decode.
// Includes columns from negation predicates that cannot appear in Nodes.

// NeedsColumnData reports whether this program requires full column data access
// (column predicates or streaming predicates).
// A nil Program returns false — lean reader is sufficient for trace-index-only lookups.
func (p *Program) NeedsColumnData() bool {
	if p == nil {
		return false
	}
	return p.ColumnPredicate != nil || p.StreamingColumnPredicate != nil
}

// Program represents a compiled TraceQL or SQL expression

// ColumnPredicate filters rows using bulk column scans (fast)
// Direct column-scan execution closure for WHERE clause
// Streaming version for aggregation (avoids RowSet)
// Extracted predicates for block-level pruning

// Original TraceQL query (optional, for debugging)
