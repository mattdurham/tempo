// Package vm provides virtual machine execution for query evaluation.
package vm

import (
	"regexp"

	"github.com/theory/jsonpath"
)

// InstructionFunc is a closure that executes a single bytecode instruction
// It returns the next instruction index to execute
type InstructionFunc func(vm *VM) int

// Value represents a runtime value in the VM
type Value struct {
	Data interface{} // int64, float64, string, bool, []byte, []Value, *JSONValue, or nil
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
	// TypeArray represents an array value ([]Value).
	TypeArray
	// TypeJSON represents a JSON value.
	TypeJSON
)

// UnscopedColumnPrefix prefixes predicate keys that represent an unscoped attribute
// expanded into multiple scoped columns.
const UnscopedColumnPrefix = "__unscoped__:"

// JSONPathNoCache signals that a JSONPath should be parsed at runtime.
const JSONPathNoCache = ^uint16(0)

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

// RangePredicate represents a range constraint on a column (e.g., >= 400, < 500)
type RangePredicate struct {
	MinValue     *Value // nil = no lower bound
	MaxValue     *Value // nil = no upper bound
	MinInclusive bool   // true for >=, false for >
	MaxInclusive bool   // true for <=, false for <
}

// QueryPredicates contains extracted predicates for block-level filtering
// Moved here from executor package to avoid circular dependency
type QueryPredicates struct {
	// Attribute equality filters: attribute name -> values
	AttributeEquals map[string][]Value

	// Whether query accesses specific dedicated columns
	DedicatedColumns map[string][]Value // column name -> values

	// Unscoped dedicated columns expanded into OR across scopes.
	// Key is UnscopedColumnPrefix + raw attribute path.
	UnscopedColumnNames map[string][]string

	// Regex patterns on dedicated columns (for LIKE queries)
	// Maps column name -> regex pattern
	DedicatedColumnsRegex map[string]string

	// Range predicates on regular attributes (e.g., status_code >= 400)
	AttributeRanges map[string]*RangePredicate

	// Range predicates on dedicated columns (e.g., span.http.status_code >= 400)
	DedicatedRanges map[string]*RangePredicate

	// Attributes accessed (for bloom filter checking)
	AttributesAccessed []string

	// Trace ID filters (if any)
	TraceIDEquals [][16]byte

	// Time range filters (already handled separately in QueryOptions)
	HasTimeFilter bool

	// Whether the query contains OR operations
	// If true, AttributeEquals predicates cannot be safely used for block pruning
	HasOROperations bool
}

// Program represents a compiled TraceQL or SQL expression
type Program struct {
	// === Closure-based Execution (SQL) ===
	// ColumnPredicate filters rows using bulk column scans (fast)
	ColumnPredicate          ColumnPredicate          // Direct column-scan execution closure for WHERE clause
	StreamingColumnPredicate StreamingColumnPredicate // Streaming version for aggregation (avoids RowSet)
	Predicates               *QueryPredicates         // Extracted predicates for block-level pruning

	// === Aggregation (SQL GROUP BY / aggregate functions) ===
	AggregationPlan *AggregationPlan // Aggregation specification (nil if not an aggregate query)
	OriginalQuery   string           // Original TraceQL query (optional, for debugging)

	// === VM Bytecode Execution (TraceQL) ===
	Instructions []InstructionFunc // Closure for each instruction
	Constants    []Value           // Constant pool
	Attributes   []string          // Attribute names for lookups
	Regexes      []*RegexCache     // Compiled regexes for =~ and !~
	JSONPaths    []*JSONPathCache  // Compiled JSONPath queries
}

// AggFunction represents the type of aggregation function
type AggFunction int

const (
	// AggCount represents a count aggregation function.
	AggCount AggFunction = iota
	// AggSum represents a sum aggregation function.
	AggSum
	// AggAvg represents an average aggregation function.
	AggAvg
	// AggMin represents a minimum aggregation function.
	AggMin
	// AggMax represents a maximum aggregation function.
	AggMax
	// AggRate represents a rate aggregation function.
	AggRate
	// AggQuantile represents a quantile aggregation function.
	AggQuantile
	// AggHistogram represents a histogram aggregation function.
	AggHistogram
	// AggStddev represents a standard deviation aggregation function.
	AggStddev
)

// AggSpec specifies a single aggregation operation
type AggSpec struct {
	Field    string      // Field name to aggregate (empty for COUNT(*))
	Alias    string      // Output column name (optional)
	Function AggFunction // The aggregation function to apply
	Quantile float64     // Quantile value (0-1) for QUANTILE function
}

// AggregationPlan describes how to perform aggregation on query results
type AggregationPlan struct {
	GroupByFields []string  // Fields to group by (empty for global aggregation)
	Aggregates    []AggSpec // Aggregation functions to compute
}

// RegexCache holds a compiled regex pattern
type RegexCache struct {
	Regex   *regexp.Regexp // Nil until lazily compiled
	Pattern string
}

// JSONPathCache holds a compiled JSONPath query.
type JSONPathCache struct {
	Compiled *jsonpath.Path // Nil until lazily compiled
	Path     string
}
