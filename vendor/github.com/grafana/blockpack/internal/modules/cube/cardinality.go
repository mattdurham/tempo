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
	// MaxCombinedCellBytes caps the estimated total wire-format bytes the resulting cube's
	// combined cells would occupy (combinedCells * recordWidthFor(len(aggAttrs)), #491 E-7) —
	// a cube with few cells but many large aggAttr records can still be rejected even when
	// MaxCombinedCells alone would pass. Default derived from today's MaxCombinedCells*12-byte
	// pure-count figure as a SIZING HEURISTIC only (not a compatibility mechanism — no file,
	// hash, or wire byte from any prior format is preserved by this calculation).
	MaxCombinedCellBytes int
}

// DefaultCardinalityLimits returns the ticket-specified default limits.
func DefaultCardinalityLimits() CardinalityLimits {
	return CardinalityLimits{
		MaxDistinctPerDim:    1000,
		MaxCombinedCells:     50_000,
		MaxCombinedCellBytes: 50_000 * 12,
	}
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
// aggAttrs is the proposed cube's materialized attribute set (#491, E-7) — it feeds the
// combined-byte-cost check below via recordWidthFor (chunk.go's single source of truth for
// the record-width formula); it does not affect the per-dimension or combined-cell-count
// checks, which are purely dimension-shaped.
//
// Returns nil when the gate passes. Returns *CardinalityError when rejected.
func CheckCardinality(
	data []byte,
	dir []valuecounts.ChunkDirEntry,
	dims []string,
	aggAttrs []AggAttrDef,
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

	// Combined cell count estimate: dim1 x dim2 for two dimensions, dim1 alone for one.
	var combinedCells int
	switch {
	case len(dims) == 2 && len(dimValues[0]) > 0 && len(dimValues[1]) > 0:
		combinedCells = len(dimValues[0]) * len(dimValues[1])
	case len(dims) == 1 && len(dimValues[0]) > 0:
		combinedCells = len(dimValues[0])
	}

	if len(dims) == 2 && combinedCells > limits.MaxCombinedCells {
		return &CardinalityError{
			Reason: fmt.Sprintf(
				"combined cell estimate (%s × %s = %d) exceeds limit %d",
				dims[0], dims[1], combinedCells, limits.MaxCombinedCells,
			),
			Suggestion: "use dimensions with fewer distinct values or add a filter",
		}
	}

	// Combined byte-cost estimate: the SAME combinedCells figure above, but weighted by
	// recordWidthFor(len(aggAttrs)) — a cube with few cells but many/wide aggAttr records can
	// still be rejected even when the plain cell-count check above would pass (#491 E-7).
	if combinedCells > 0 {
		recordWidth := recordWidthFor(len(aggAttrs))
		estimatedBytes := combinedCells * recordWidth
		if estimatedBytes > limits.MaxCombinedCellBytes {
			return &CardinalityError{
				Reason: fmt.Sprintf(
					"estimated cube size (%d cells x %d bytes/cell = %d bytes) exceeds limit %d bytes",
					combinedCells, recordWidth, estimatedBytes, limits.MaxCombinedCellBytes,
				),
				Suggestion: "reduce the number of materialized aggAttrs, or use lower-cardinality dimensions",
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
