package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// buildPredicates converts a compiled vm.Program into queryplanner.Predicate values
// for bloom-filter and range-index block pruning.
//
// For AND queries (HasOROperations = false):
//   - Range columns (from program.Predicates.DedicatedColumns) produce a
//     Predicate with both Columns (bloom) and Values (range index). Blocks survive
//     only if the bloom check passes AND the range index covers the queried value.
//   - Other accessed columns (not in DedicatedColumns) produce bloom-only Predicates.
//
// For OR queries (HasOROperations = true):
//   - All accessed columns are combined into a single bloom-only Predicate.
//     Range-index pruning is skipped because a block not indexed for one OR branch
//     may still satisfy another branch — unsafe to AND-prune.
func buildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
	if program == nil || program.Predicates == nil {
		return nil
	}
	preds := program.Predicates

	if preds.HasOROperations {
		cols := preds.AttributesAccessed
		if len(cols) == 0 {
			return nil
		}
		combined := make([]string, len(cols))
		copy(combined, cols)
		return []queryplanner.Predicate{{Columns: combined}}
	}

	// AND query: build range-aware predicates for range-indexed columns, bloom-only
	// predicates for everything else.
	result := make([]queryplanner.Predicate, 0, len(preds.AttributesAccessed))

	for col, vals := range preds.DedicatedColumns {
		colType, hasIndex := r.RangeColumnType(col)
		if !hasIndex {
			result = append(result, queryplanner.Predicate{Columns: []string{col}})
			continue
		}
		encodedVals := make([]string, 0, len(vals))
		for _, v := range vals {
			if enc, ok := encodeValue(v, colType); ok {
				encodedVals = append(encodedVals, enc)
			}
		}
		result = append(result, queryplanner.Predicate{
			Columns: []string{col},
			Values:  encodedVals,
			ColType: colType,
		})
	}

	// Bloom-only predicates for accessed columns not covered by range index.
	for _, col := range preds.AttributesAccessed {
		if _, isDedicated := preds.DedicatedColumns[col]; !isDedicated {
			result = append(result, queryplanner.Predicate{Columns: []string{col}})
		}
	}

	return result
}

// BuildPredicates is an exported thin wrapper around buildPredicates.
// Used by api.go's StreamTraceQLModules inline execute loop.
func BuildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
	return buildPredicates(r, program)
}

// encodeValue encodes a vm.Value to the range-index wire format (SPECS §5.2.1):
//   - String / RangeString: raw string
//   - Int64 / RangeDuration: 8-byte little-endian int64 bits
//   - Uint64 / RangeUint64: 8-byte little-endian uint64 bits
//   - Float64 / RangeFloat64: 8-byte little-endian IEEE-754 float64 bits
func encodeValue(v vm.Value, colType modules_shared.ColumnType) (string, bool) {
	switch colType {
	case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
		if v.Type == vm.TypeString {
			if s, ok := v.Data.(string); ok {
				return s, true
			}
		}

	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		var n int64
		switch v.Type {
		case vm.TypeInt:
			if i, ok := v.Data.(int64); ok {
				n = i
			} else {
				return "", false
			}
		case vm.TypeDuration:
			if i, ok := v.Data.(int64); ok {
				n = i
			} else {
				return "", false
			}
		default:
			return "", false
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(n)) //nolint:gosec
		return string(buf[:]), true

	case modules_shared.ColumnTypeRangeUint64:
		if v.Type == vm.TypeInt {
			if i, ok := v.Data.(int64); ok {
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], uint64(i)) //nolint:gosec
				return string(buf[:]), true
			}
		}

	case modules_shared.ColumnTypeRangeFloat64:
		if v.Type == vm.TypeFloat {
			if f, ok := v.Data.(float64); ok {
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
				return string(buf[:]), true
			}
		}
	}

	return "", false
}
