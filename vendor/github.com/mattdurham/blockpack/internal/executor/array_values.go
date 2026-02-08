package executor

import (
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

// decodeArrayColumn extracts and decodes an array value from a column at the given row index.
//
// Return values:
//   - (nil, nil): Column is nil, row index is out of bounds, or no value exists at idx
//   - ([]ArrayValue{}, nil): Empty array exists at idx (valid empty array, not missing value)
//   - (nil, error): Decoding failed (corrupted data, invalid format)
//   - ([]ArrayValue{...}, nil): Successfully decoded non-empty array
//
// Note: This function cannot distinguish between "no value at idx" and "column doesn't exist"
// because both return (nil, nil). Callers should check col existence before calling if needed.
func decodeArrayColumn(col *blockpack.Column, idx int) ([]blockpack.ArrayValue, error) {
	if col == nil {
		return nil, nil
	}
	// Bounds check: BytesValueView returns false if idx is out of bounds or value not present
	data, ok := col.BytesValueView(idx)
	if !ok {
		return nil, nil // No value at idx, out of bounds, or value not present
	}
	if len(data) == 0 {
		return nil, nil // Empty encoded data
	}
	return blockpack.DecodeArray(data)
}

// arrayValuesToVM converts blockpack array values to VM values.
// Returns an empty slice (not nil) for empty input to ensure consistent behavior
// for VM operations that distinguish between "no array" (nil) and "empty array" ([]vm.Value{}).
// This allows callers to treat empty arrays as valid arrays rather than missing values.
func arrayValuesToVM(values []blockpack.ArrayValue) []vm.Value {
	if len(values) == 0 {
		return []vm.Value{} // Intentionally return empty slice, not nil
	}
	out := make([]vm.Value, 0, len(values))
	for _, val := range values {
		switch val.Type {
		case blockpack.ArrayTypeString:
			out = append(out, vm.Value{Type: vm.TypeString, Data: val.Str})
		case blockpack.ArrayTypeInt64:
			out = append(out, vm.Value{Type: vm.TypeInt, Data: val.Int})
		case blockpack.ArrayTypeFloat64:
			out = append(out, vm.Value{Type: vm.TypeFloat, Data: val.Float})
		case blockpack.ArrayTypeBool:
			out = append(out, vm.Value{Type: vm.TypeBool, Data: val.Bool})
		case blockpack.ArrayTypeBytes:
			out = append(out, vm.Value{Type: vm.TypeBytes, Data: val.Bytes})
		case blockpack.ArrayTypeDuration:
			out = append(out, vm.Value{Type: vm.TypeDuration, Data: val.Int})
		default:
			// Skip unknown types instead of crashing. This handles corrupted data
			// or future array types gracefully. Unknown values are omitted from the result.
			// Log a warning if we have a logger available in the future.
			continue
		}
	}
	return out
}
