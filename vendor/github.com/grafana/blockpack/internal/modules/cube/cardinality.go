package cube

// NOTE: SPEC-CUBE-013 — CardinalityGate guards cube creation by checking per-dimension
// distinct-value counts and combined (dim1×dim2) cell estimates against configurable
// limits. It uses the value count index (valuecounts.ValuesInRange) and never reads data
// blocks. UUID/high-entropy columns are always rejected.

import (
	"fmt"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
)

// CardinalityLimits configures the cardinality gate thresholds.
type CardinalityLimits struct {
	// MaxDistinctPerDim is the maximum distinct values per single dimension (default 1000).
	MaxDistinctPerDim int
	// MaxCombinedCells is the maximum distinct (dim1,dim2) pairs across the window (default 50000).
	MaxCombinedCells int
}

// DefaultCardinalityLimits returns the ticket-specified default limits.
func DefaultCardinalityLimits() CardinalityLimits {
	return CardinalityLimits{MaxDistinctPerDim: 1000, MaxCombinedCells: 50_000}
}

// CardinalityError is returned when the cardinality gate rejects a cube.
// The Reason field is a human-readable explanation suitable for returning to users.
type CardinalityError struct {
	Reason     string
	Suggestion string
}

func (e *CardinalityError) Error() string { return e.Reason }

// CheckCardinality evaluates the cardinality gate for a proposed cube over
// [minTS, maxTS] using pre-loaded value count data. data and dir are a single
// VCNT section already fetched by the caller (so this function has zero S3 I/O).
//
// Returns nil when the gate passes. Returns *CardinalityError when rejected.
func CheckCardinality(
	data []byte,
	dir []valuecounts.ChunkDirEntry,
	dims []string,
	limits CardinalityLimits,
	minTS, maxTS uint64,
) error {
	if limits.MaxDistinctPerDim == 0 {
		limits = DefaultCardinalityLimits()
	}

	// Per-dimension distinct-value check.
	dimValues := make([][]valuecounts.ValueCount, len(dims))
	for i, col := range dims {
		if isHighEntropy(col) {
			return &CardinalityError{
				Reason: fmt.Sprintf(
					"dimension %q is a high-entropy column (UUID/trace/span IDs cannot be cube dimensions)",
					col,
				),
				Suggestion: "use low-cardinality dimensions such as resource.service.name or span:status_code",
			}
		}
		vals, err := valuecounts.ValuesInRange(data, dir, col, minTS, maxTS)
		if err != nil {
			return fmt.Errorf("cube: cardinality check column %q: %w", col, err)
		}
		if len(vals) > limits.MaxDistinctPerDim {
			return &CardinalityError{
				Reason: fmt.Sprintf(
					"dimension %q has %d distinct values (limit %d)",
					col, len(vals), limits.MaxDistinctPerDim,
				),
				Suggestion: fmt.Sprintf("consider a filter to reduce cardinality e.g. %s = 'some-value'", col),
			}
		}
		dimValues[i] = vals
	}

	// Combined (dim1 × dim2) cell count estimate (only when 2 dimensions).
	if len(dims) == 2 && len(dimValues[0]) > 0 && len(dimValues[1]) > 0 {
		combined := len(dimValues[0]) * len(dimValues[1])
		if combined > limits.MaxCombinedCells {
			return &CardinalityError{
				Reason: fmt.Sprintf(
					"combined cell estimate (%s × %s = %d) exceeds limit %d",
					dims[0], dims[1], combined, limits.MaxCombinedCells,
				),
				Suggestion: "use dimensions with fewer distinct values or add a filter",
			}
		}
	}

	return nil
}

// isHighEntropy reports whether a column name is known to carry UUID-class or high-entropy
// values that would make it an impractical cube dimension.
func isHighEntropy(col string) bool {
	// Reject well-known identity columns.
	highEntropyCols := map[string]struct{}{
		"trace:id":       {},
		"span:id":        {},
		"span:parent_id": {},
		"__trace_id__":   {},
		"traceID":        {},
		"span_id":        {},
	}
	if _, ok := highEntropyCols[col]; ok {
		return true
	}
	// Reject columns whose name contains common UUID/ID suffixes.
	for _, suffix := range []string{"_id", "_uuid", "_guid", "trace_id", "span_id"} {
		if len(col) >= len(suffix) && col[len(col)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

// isHexString reports whether s is a valid hex string (UUID-like entropy heuristic).
