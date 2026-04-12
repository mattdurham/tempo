// Package vm provides virtual machine execution for query evaluation.
package vm

// Value represents a runtime value in the VM
type Value struct {
	Data any // int64, float64, string, bool, []byte, []Value, or nil
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

// Vector search constants.
const (
	// DefaultVectorLimit is the default top-K results returned by a VECTOR() predicate.
	DefaultVectorLimit = 10
	// DefaultVectorThreshold is the minimum cosine similarity for a span to be included.
	DefaultVectorThreshold float32 = 0.3
)

// RangeNode is a node in the block-pruning predicate tree.
//
// Leaf node (len(Children) == 0): Column names the fully-scoped column to look up.
// At most one of Values, Min/Max, or Pattern is meaningful:
//   - Values non-empty: equality / point-lookup (values are OR'd together).
//   - Min or Max non-nil: interval lookup for range predicates (>, >=, <, <=).
//   - Pattern non-empty: regex pattern; buildPredicates extracts a literal prefix.
//   - QueryVector non-nil: VECTOR() ranking predicate — centroid pruning in planBlocks.
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

	// QueryVector is non-nil for VECTOR() ranking predicates.
	// When set, planBlocks uses centroid distance to prune entire files/blocks.
	QueryVector []float32

	// VectorThreshold is the minimum cosine similarity for centroid pruning.
	// Only meaningful when QueryVector is non-nil.
	VectorThreshold float32

	// MinInclusive is true when Min comes from >= (>= x) rather than > (> x).
	// Used by the intrinsic flat-column scan to decide inclusive vs exclusive lower bound.
	MinInclusive bool
	// MaxInclusive is true when Max comes from <= (<= x) rather than < (< x).
	MaxInclusive bool
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

// TextEmbedder is the interface for converting text to embedding vectors.
// This keeps the VM decoupled from the embedder implementation — callers provide
// any implementation (HTTP backend, yzma, mock, etc.) via CompileOptions.
type TextEmbedder interface {
	Embed(text string) ([]float32, error)
}

// ScoredRow holds a row index and its cosine similarity score from vector scoring.
type ScoredRow struct {
	RowIdx int
	Score  float32
}

// VectorScorer is a compiled vector-scoring closure that runs AFTER traditional
// predicates have produced a candidate RowSet. It accepts a point-lookup accessor
// for the embedding column and the candidate set, and returns scored rows at or
// above the query threshold sorted by score descending.
//
// getVec(rowIdx) returns the embedding vector for a row and whether it is present.
// Only candidate rows (those in candidates) are scored — non-candidates are skipped.
type VectorScorer func(getVec func(rowIdx int) ([]float32, bool), candidates RowSet) []ScoredRow

// NeedsColumnData reports whether this program requires full column data access
// (column predicates, streaming predicates, or vector scoring).
// A nil Program returns false — lean reader is sufficient for trace-index-only lookups.
func (p *Program) NeedsColumnData() bool {
	if p == nil {
		return false
	}
	return p.ColumnPredicate != nil || p.StreamingColumnPredicate != nil || p.VectorScorer != nil
}

// Program represents a compiled TraceQL or SQL expression
type Program struct {
	// ColumnPredicate filters rows using bulk column scans (fast)
	ColumnPredicate          ColumnPredicate          // Direct column-scan execution closure for WHERE clause
	StreamingColumnPredicate StreamingColumnPredicate // Streaming version for aggregation (avoids RowSet)
	Predicates               *QueryPredicates         // Extracted predicates for block-level pruning

	// VectorScorer is set when the query contains a VECTOR_AI() or VECTOR_ALL() predicate.
	// It runs after ColumnPredicate produces candidates, scoring only those rows.
	// When nil, no vector scoring is applied.
	VectorScorer VectorScorer

	OriginalQuery string // Original TraceQL query (optional, for debugging)

	// VectorColumn is the block column name that VectorScorer reads via point lookup.
	// "__embedding__" for VECTOR_AI, "__embedding_all__" for VECTOR_ALL.
	VectorColumn string

	// Vector search fields — non-zero when the query contains a VECTOR() predicate.
	// QueryVector is pre-computed from the VECTOR() query text at compile time.
	QueryVector []float32
	// VectorLimit is the top-K result count (default: DefaultVectorLimit).
	VectorLimit int
	// HasVector is true when the query contains a VECTOR_AI() or VECTOR_ALL() predicate.
	HasVector bool
	// VectorAll is true for VECTOR_ALL() — ranks by stored all-fields embedding (no query vector needed).
	VectorAll bool
}
