package executor

import (
	"encoding/hex"
	"fmt"

	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	blockpack "github.com/mattdurham/blockpack/internal/types"
)

// LazySpanFields provides on-demand access to span fields from blockpack storage.
// This avoids allocating a map with all fields upfront.
// Uses block indices instead of pointers to reduce memory usage - keeps only
// one reference to blocks slice instead of thousands of references to individual blocks.
type LazySpanFields struct {
	blocks         []*blockpackio.Block // Shared reference to blocks slice
	blockIdx       int                  // Index into blocks slice
	spanIdx        int                  // Row index within the block
	matchedColumns map[string]any       // If set, only project these columns (for OR queries)
}

// NewLazySpanFields creates a new LazySpanFields instance for accessing span fields
// from a blockpack block at a specific row index.
// Returns error if blocks is nil, blockIdx is out of bounds, or spanIdx is negative.
func NewLazySpanFields(blocks []*blockpackio.Block, blockIdx int, spanIdx int) (*LazySpanFields, error) {
	if blocks == nil {
		return nil, fmt.Errorf("blocks cannot be nil")
	}
	if blockIdx < 0 || blockIdx >= len(blocks) {
		return nil, fmt.Errorf("blockIdx %d out of bounds for %d blocks", blockIdx, len(blocks))
	}
	if spanIdx < 0 {
		return nil, fmt.Errorf("spanIdx cannot be negative: %d", spanIdx)
	}
	// Note: We can't validate spanIdx upper bound here without accessing the block
	// which might be expensive. The SpanCount check happens at field access time.
	return &LazySpanFields{
		blocks:   blocks,
		blockIdx: blockIdx,
		spanIdx:  spanIdx,
	}, nil
}

// MapSpanFields provides field access from a Go map.
// This is useful for metadata queries or when fields are already materialized.
type MapSpanFields struct {
	fields map[string]any
}

// NewMapSpanFields creates a new MapSpanFields instance from a map.
func NewMapSpanFields(fields map[string]any) *MapSpanFields {
	return &MapSpanFields{fields: fields}
}

// GetField retrieves a single field by name from the map.
// Returns (value, true) if the field exists, (nil, false) otherwise.
func (msf *MapSpanFields) GetField(name string) (any, bool) {
	if msf.fields == nil {
		return nil, false
	}
	val, ok := msf.fields[name]
	return val, ok
}

// IterateFields calls the provided function for each field in the map.
// The callback should return true to continue iteration, false to stop.
func (msf *MapSpanFields) IterateFields(fn func(name string, value any) bool) {
	if msf.fields == nil {
		return
	}
	for name, value := range msf.fields {
		if !fn(name, value) {
			return
		}
	}
}

// isIntrinsicField checks if a column name represents an intrinsic span field.
// Uses the canonical IntrinsicFieldNames list defined in intrinsics.go.
func isIntrinsicField(columnName string) bool {
	return IsIntrinsicField(columnName)
}

// GetField retrieves a single field by name from the blockpack block.
// Returns (value, true) if the field exists, (nil, false) otherwise.
// For OR queries with matchedColumns set, only returns fields that contributed to the match.
func (lsf *LazySpanFields) GetField(name string) (any, bool) {
	if lsf.blocks == nil || lsf.blockIdx < 0 || lsf.blockIdx >= len(lsf.blocks) {
		return nil, false
	}
	block := lsf.blocks[lsf.blockIdx]
	if block == nil {
		return nil, false
	}
	// Validate spanIdx is within bounds before accessing row data
	if lsf.spanIdx < 0 || lsf.spanIdx >= block.SpanCount() {
		return nil, false
	}
	columnName := attributePathToColumnName(name)

	// If matchedColumns is set (OR query), only return fields that matched
	// BUT: Always allow intrinsic fields (start_time, duration, trace:id, span:id, etc.)
	// These are not part of the WHERE clause and should always be available
	// Note: matchedColumns uses attribute names (e.g., "resource.service.name"), not column names
	if lsf.matchedColumns != nil && !isIntrinsicField(columnName) {
		if _, matched := lsf.matchedColumns[name]; !matched {
			return nil, false // Field didn't contribute to match
		}
	}

	col := block.GetColumn(columnName)
	if col == nil {
		return nil, false
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if v, ok := col.StringValue(lsf.spanIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeInt64:
		if v, ok := col.Int64Value(lsf.spanIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeUint64:
		if v, ok := col.Uint64Value(lsf.spanIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeBool:
		if v, ok := col.BoolValue(lsf.spanIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeFloat64:
		if v, ok := col.Float64Value(lsf.spanIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeBytes:
		if v, ok := col.BytesValueView(lsf.spanIdx); ok {
			// Encode IDs as hex strings for consistent JSON serialization.
			// encodeID() uses hex.EncodeToString() which allocates a new string and copies
			// the data, so it's safe even though 'v' is a view into arena memory.
			if name == "trace:id" || name == "span:id" || name == "span:parent_id" {
				return encodeID(v), true
			}
			// CRITICAL: BytesValueView returns a view into arena memory that may be reused.
			// We MUST copy the bytes to avoid corruption when arena memory is recycled.
			bytesCopy := make([]byte, len(v))
			copy(bytesCopy, v)
			return bytesCopy, true
		}
	}
	return nil, false
}

// IterateFields calls the provided function for each field in the span.
// If the function returns false, iteration stops.
// For OR queries with matchedColumns set, only iterates over columns that contributed to the match.
// getColumnValueForIteration extracts a typed value from a column at the span index.
// ASSUMES: block and spanIdx have already been validated by the caller.
// This is a private helper method called only from IterateFields after validation.
func (lsf *LazySpanFields) getColumnValueForIteration(col *blockpack.Column, name string) (value any, ok bool) {
	// Defensive nil check: col should never be nil when called from IterateFields,
	// but check anyway to prevent panics if this method is ever called directly.
	if col == nil {
		return nil, false
	}
	switch col.Type {
	case blockpack.ColumnTypeString:
		value, ok = col.StringValue(lsf.spanIdx)
	case blockpack.ColumnTypeInt64:
		value, ok = col.Int64Value(lsf.spanIdx)
	case blockpack.ColumnTypeUint64:
		value, ok = col.Uint64Value(lsf.spanIdx)
	case blockpack.ColumnTypeBool:
		value, ok = col.BoolValue(lsf.spanIdx)
	case blockpack.ColumnTypeFloat64:
		value, ok = col.Float64Value(lsf.spanIdx)
	case blockpack.ColumnTypeBytes:
		var v []byte
		v, ok = col.BytesValueView(lsf.spanIdx)
		if ok {
			// encodeID() uses hex.EncodeToString() which allocates and copies,
			// making it safe even though 'v' is a view into arena memory.
			if name == "trace:id" || name == "span:id" || name == "span:parent_id" {
				value = encodeID(v)
			} else {
				// CRITICAL: BytesValueView returns a view into arena memory that may be reused.
				// We MUST copy the bytes to avoid corruption when arena memory is recycled.
				bytesCopy := make([]byte, len(v))
				copy(bytesCopy, v)
				value = bytesCopy
			}
		}
	}
	return value, ok
}

func (lsf *LazySpanFields) IterateFields(fn func(name string, value any) bool) {
	if lsf.blocks == nil || lsf.blockIdx < 0 || lsf.blockIdx >= len(lsf.blocks) {
		return
	}
	block := lsf.blocks[lsf.blockIdx]
	if block == nil {
		return
	}
	// Validate spanIdx is within bounds before accessing row data
	if lsf.spanIdx < 0 || lsf.spanIdx >= block.SpanCount() {
		return
	}

	// If matchedColumns is set (OR query with column-first scanning),
	// iterate over matched columns PLUS intrinsic fields
	if lsf.matchedColumns != nil {
		// First, iterate over matched columns
		for name := range lsf.matchedColumns {
			// Map attribute name to column name
			columnName := attributePathToColumnName(name)
			// Get the column value from the block
			col := block.GetColumn(columnName)
			if col == nil {
				continue
			}

			value, valueOK := lsf.getColumnValueForIteration(col, name)
			if valueOK {
				if !fn(columnName, value) {
					return
				}
			}
		}

		// Also iterate over intrinsic fields (not filtered by OR projection)
		// Uses canonical list from intrinsics.go
		for _, intrinsicName := range IntrinsicFieldNames {
			// Skip if already iterated as a matched column
			if _, alreadyDone := lsf.matchedColumns[intrinsicName]; alreadyDone {
				continue
			}

			col := block.GetColumn(intrinsicName)
			if col == nil {
				continue
			}

			value, valueOK := lsf.getColumnValueForIteration(col, intrinsicName)
			if valueOK {
				if !fn(intrinsicName, value) {
					return
				}
			}
		}
		return
	}

	// Normal path: iterate over all columns
	for name, col := range block.Columns() {
		value, ok := lsf.getColumnValueForIteration(col, name)
		if ok {
			if !fn(name, value) {
				return
			}
		}
	}
}

// encodeID converts a byte slice ID to a hex string.
func encodeID(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return hex.EncodeToString(id)
}
