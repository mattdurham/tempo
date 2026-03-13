// Package vm provides virtual machine execution for query evaluation.
package vm

import (
	"regexp"
)

// InstructionFunc is a closure that executes a single bytecode instruction
// It returns the next instruction index to execute
type InstructionFunc func(vm *VM) int

// Value represents a runtime value in the VM
type Value struct {
	Data interface{} // int64, float64, string, bool, []byte, []Value, or nil
	Type ValueType
}

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
type RangeNode struct {
	Min *Value // interval lower bound (nil = no lower bound)
	Max *Value // interval upper bound (nil = no upper bound)
	// Leaf fields — set when len(Children) == 0.
	Column  string // fully-scoped column (e.g. "resource.service.name", "span:duration")
	Pattern string // regex pattern for prefix-based range pruning

	Values   []Value // equality lookup values; multiple values are OR'd
	Children []RangeNode

	// Composite fields — set when len(Children) > 0.
	IsOR bool
}

// QueryPredicates holds block-level pruning predicates extracted from a compiled query.
type QueryPredicates struct {
	// Nodes is the top-level AND-combined list of pruning predicates.
	// Each node is either a leaf (Column + values/range/pattern) or a composite (IsOR + Children).
	Nodes []RangeNode

	// Columns lists every attribute column accessed by the query.
	// Used by ProgramWantColumns to select columns for the first-pass block decode.
	// Includes columns from negation predicates that cannot appear in Nodes.
	Columns []string
}

// Program represents a compiled TraceQL or SQL expression
type Program struct {
	// === Closure-based Execution (SQL) ===
	// ColumnPredicate filters rows using bulk column scans (fast)
	ColumnPredicate          ColumnPredicate          // Direct column-scan execution closure for WHERE clause
	StreamingColumnPredicate StreamingColumnPredicate // Streaming version for aggregation (avoids RowSet)
	Predicates               *QueryPredicates         // Extracted predicates for block-level pruning

	OriginalQuery string // Original TraceQL query (optional, for debugging)

	// === VM Bytecode Execution (TraceQL) ===
	Instructions []InstructionFunc // Closure for each instruction
	Constants    []Value           // Constant pool
	Attributes   []string          // Attribute names for lookups
	Regexes      []*RegexCache     // Compiled regexes for =~ and !~
}

// RegexCache holds a compiled regex pattern
type RegexCache struct {
	Regex   *regexp.Regexp // Nil until lazily compiled
	Pattern string
}
