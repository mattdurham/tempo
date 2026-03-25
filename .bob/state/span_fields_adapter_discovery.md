# Discovery: NewSpanFieldsAdapter and IterateFields Implementation

## Overview

`NewSpanFieldsAdapter` is a lazy field accessor that bridges blockpack block data to the SpanMatchCallback contract. It provides two key methods: `GetField()` for direct lookups and `IterateFields()` for enumeration of all fields present at a specific span row.

## File Locations

- **Implementation**: `/home/matt/source/blockpack-tempo/internal/modules/blockio/span_fields.go`
- **Predicate/Column planning**: `/home/matt/source/blockpack-tempo/internal/modules/executor/predicates.go`
- **Usage in API**: `/home/matt/source/blockpack-tempo/api.go`
- **Block reader interface**: `/home/matt/source/blockpack-tempo/internal/modules/blockio/reader/block.go`

## NewSpanFieldsAdapter Implementation

### Type Definition

```go
type modulesSpanFieldsAdapter struct {
	block  *modules_reader.Block
	rowIdx int
}

func NewSpanFieldsAdapter(block *modules_reader.Block, rowIdx int) modules_shared.SpanFieldsProvider {
	return &modulesSpanFieldsAdapter{block: block, rowIdx: rowIdx}
}
```

**Location**: `span_fields.go:12-25`

### GetField Method

```go
func (a *modulesSpanFieldsAdapter) GetField(name string) (any, bool) {
	col := modulesLookupColumn(a.block, name)
	return modulesGetValue(col, a.rowIdx)
}
```

**Location**: `span_fields.go:92-95`

**Behavior**:
- Performs lazy lookup via `modulesLookupColumn()` which:
  - First tries exact name match via `block.GetColumn(name)`
  - Falls back to resource/span prefixes for unscoped names (names without dots or colons)
    - Tries `"resource." + name`
    - Tries `"span." + name`
- Returns the typed value and a boolean indicating if the column and row value exist
- Handles all column types: String, Int64, Uint64, Float64, Bool, Bytes, UUID, and Range variants

### IterateFields Method

```go
func (a *modulesSpanFieldsAdapter) IterateFields(fn func(name string, value any) bool) {
	seen := make(map[string]struct{})
	for key, col := range a.block.Columns() {
		// NOTE-ITER-1: skip body-parsed auto-columns; they are not original attributes.
		if key.Type == modules_shared.ColumnTypeRangeString {
			continue
		}
		if _, already := seen[key.Name]; already {
			continue
		}
		v, ok := modulesGetValue(col, a.rowIdx)
		if !ok {
			continue
		}
		seen[key.Name] = struct{}{}
		if !fn(key.Name, v) {
			return
		}
	}
}
```

**Location**: `span_fields.go:107-126`

### What IterateFields Iterates Over

1. **Source**: `a.block.Columns()` — a `map[shared.ColumnKey]*Column` from the blockpack Block

2. **Column iteration behavior**:
   - **ColumnKey structure** (`shared/types.go:ColumnKey`):
     ```go
     type ColumnKey struct {
         Name string    // e.g., "resource.service.name", "span.http.method", "trace:id"
         Type ColumnType // e.g., ColumnTypeString, ColumnTypeRangeString, ColumnTypeInt64
     }
     ```

3. **Filtering logic**:
   - **Skips** `ColumnTypeRangeString` columns (these are auto-parsed body fields from log:body, not original attributes — NOTE-ITER-1)
   - **Deduplicates by name**: When multiple typed variants of the same attribute exist (rare, but possible from OTLP), only the first found variant with a present value is emitted
   - **Presence check**: Only emits columns where the value is present at the current row (via `modulesGetValue()`)
   - **Early exit**: If the callback returns false, stops iteration immediately

4. **Included columns**:
   - All original block columns: intrinsics (trace:id, span:id, span:parent_id, span:name, span:duration, span:status, span:kind)
   - All resource attributes (resource.service.name, resource.host.name, etc.)
   - All span user attributes (span.http.method, span.user.id, etc.)
   - All specialized columns (e.g., span:start, span:end for time bounds)

5. **Excluded columns**:
   - ColumnTypeRangeString variants (auto-derived from log:body parsing per SPEC-11.5)

## Column Decoding Requirements

### No Pre-Decoding Required

`IterateFields()` does **NOT** require any columns to be pre-decoded. It:
1. Calls `block.Columns()` which returns the column metadata map
2. Uses lazy lookup and value extraction via `modulesGetValue()`
3. Only reads from disk (via column value accessors) for columns that actually have values at this row

### Integration with ProgramWantColumns

The two-pass decode flow uses `ProgramWantColumns()` to optimize which columns are eagerly decoded:

**Location**: `predicates.go:210-244`

```go
// ProgramWantColumns returns the minimal set of column names needed to evaluate program.
//
// Sources (unioned):
//  1. Leaf Column values from the RangeNode tree (preds.Nodes) — collected recursively.
//  2. preds.Columns — explicit column list for attributes that need decode but not pruning:
//     negations (!=, !~), log:body for line filters, pushdown label-filter columns.
//  3. extra — caller-supplied columns (e.g. identity columns like trace:id, span:id).
//
// Returns nil if program has no predicates, which ParseBlockFromBytes treats as "all columns".
func ProgramWantColumns(program *vm.Program, extra ...string) map[string]struct{} {
	if program == nil || program.Predicates == nil {
		return nil
	}
	p := program.Predicates
	if len(p.Nodes) == 0 && len(p.Columns) == 0 && len(extra) == 0 {
		return nil
	}

	cols := make(map[string]struct{})
	// Collect all leaf column names from the Nodes tree.
	collectNodeColumns(p.Nodes, cols)
	// Add explicit Columns (negations, log:body, etc. that have no pruning node).
	for _, c := range p.Columns {
		cols[c] = struct{}{}
	}
	for _, c := range extra {
		cols[c] = struct{}{}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}
```

### ProgramWantColumns Return Value

Returns `map[string]struct{}` containing column names to eagerly decode:

- **Key**: Column name as a string (e.g., "span.http.method", "resource.service.name", "trace:id")
- **Value**: Empty struct (set membership test only)
- **Nil return**: If the program has no predicates, queries, or explicit columns — interpreted as "decode all columns"

**Example usage** (from `api.go`):
```go
wantColumns := modules_executor.ProgramWantColumns(leftProg, "trace:id", "span:id", "span:parent_id")
if right := modules_executor.ProgramWantColumns(rightProg, "trace:id", "span:id", "span:parent_id"); right != nil {
    // Union with right program's columns
}
```

## Data Flow

1. **Block decode**: `ParseBlockFromBytes()` calls `ProgramWantColumns()` to get the minimal set
2. **Eager decode**: Only columns in the want set are decoded during block initialization
3. **Lazy access**: `NewSpanFieldsAdapter` and `IterateFields()` access columns on-demand without re-decoding
4. **Result building**: Callback functions (e.g., SpanMatchCallback) use `IterateFields()` to enumerate all present fields for constructing span results

## Deduplication Logic

If a column exists in multiple type variants (rare), `IterateFields()` uses a `seen` map to ensure each unique name is only emitted once:

```go
if _, already := seen[key.Name]; already {
    continue
}
```

This prevents duplicate emissions when the same attribute (e.g., "service.name") is stored in multiple format variants.

## Summary

- **NewSpanFieldsAdapter**: Lazy field accessor that bridges blockpack Block data to individual spans
- **IterateFields()**: Enumerates all columns (minus ColumnTypeRangeString auto-derived fields) present at a span row
- **Column iteration**: Iterates over `block.Columns()` map; no pre-decoding required
- **ProgramWantColumns()**: Returns minimal column set for first-pass eager decode; returns `map[string]struct{}` or nil
- **Deduplication**: Handles same-name/different-type columns by emitting only the first present variant
- **Integration**: Used in query result callbacks to build span match outputs with all available fields
