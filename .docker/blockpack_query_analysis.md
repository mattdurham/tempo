# BlockPack TraceQL Query Analysis: String Equality on Span Attributes

## Summary

For a TraceQL query like `{span.aws.request_id = "1f9202e7-4e50-59e2-b67c-60c893d98e8a"}` (exact string equality on a span attribute), **BlocksForRange IS called and does correctly prune blocks**. The query planner includes string equality attributes in the range index lookup.

## Key Files & Functions

1. **Query Planning Entry**: `/home/mdurham/source/blockpack/api.go:577` - `streamFilterQuery()`
2. **Block Selection Pipeline**: `/home/mdurham/source/blockpack/internal/modules/executor/plan_blocks.go:24` - `planBlocks()`
3. **Predicate Building**: `/home/mdurham/source/blockpack/internal/modules/executor/predicates.go:94` - `BuildPredicates()`
4. **Range Index Query**: `/home/mdurham/source/blockpack/internal/modules/blockio/reader/reader.go:289` - `BlocksForRange()`
5. **Leaf Block Set Evaluation**: `/home/mdurham/source/blockpack/internal/modules/queryplanner/selection.go:17` - `leafBlockSet()`

## Data Flow for String Equality Query

### Step 1: Query Compilation (`streamFilterQuery`)
File: `/home/mdurham/source/blockpack/api.go:577-584`

```go
func streamFilterQuery(r *Reader, filterExpr *traceqlparser.FilterExpression, opts QueryOptions, fn spanMatchFn) error {
    program, compileErr := vm.CompileTraceQLFilter(filterExpr)
    if compileErr != nil {
        return fmt.Errorf("compile TraceQL filter: %w", compileErr)
    }
    return streamFilterProgram(r, program, opts, fn)
}
```

The TraceQL filter is compiled into a `vm.Program` containing predicate nodes.

### Step 2: Block Selection via `planBlocks`
File: `/home/mdurham/source/blockpack/internal/modules/executor/plan_blocks.go:24-94`

```go
func planBlocks(
    r *modules_reader.Reader,
    program *vm.Program,
    tr queryplanner.TimeRange,
    opts queryplanner.PlanOptions,
) *queryplanner.Plan {
    predicates := BuildPredicates(r, program)  // Line 30
    planner := queryplanner.NewPlanner(r)
    plan := planner.PlanWithOptions(predicates, tr, opts)  // Line 32
    // ... additional pruning steps ...
    return plan
}
```

The program is converted to queryplanner predicates and used for range index pruning.

### Step 3: Predicate Building from VM Program
File: `/home/mdurham/source/blockpack/internal/modules/executor/predicates.go:94-128`

```go
func BuildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
    // ... validation ...
    result := make([]queryplanner.Predicate, 0, len(preds.Nodes))
    for _, node := range preds.Nodes {
        p := translateNode(r, node)
        result = append(result, p)
    }
    return result
}
```

For a leaf node with equality values (like `span.aws.request_id = "..."`), `translateNode()` handles it:

```go
// Values: equality / point-lookup (bloom + range-index point match).
if len(node.Values) > 0 {
    colType, hasIndex := r.RangeColumnType(col)
    if !hasIndex {
        return queryplanner.Predicate{Columns: []string{col}}
    }
    encodedVals := make([]string, 0, len(node.Values))
    for _, v := range node.Values {
        if enc, ok := encodeValue(v, colType); ok {
            encodedVals = append(encodedVals, enc)
        }
    }
    return queryplanner.Predicate{
        Columns: []string{col},
        Values:  encodedVals,
        ColType: colType,
    }
}
```

**CRITICAL**: If the column `span.aws.request_id` has a range index (checked via `r.RangeColumnType(col)`), the predicate includes the encoded values. If no range index exists, it returns only the column name (bloom-only).

### Step 4: Range Index Pruning via `leafBlockSet`
File: `/home/mdurham/source/blockpack/internal/modules/queryplanner/selection.go:17-55`

```go
func leafBlockSet(r BlockIndexer, pred Predicate) (map[int]struct{}, error) {
    if len(pred.Values) == 0 || len(pred.Columns) != 1 {
        return nil, nil // not indexable
    }
    col := pred.Columns[0]
    if _, ok := r.RangeColumnType(col); !ok {
        return nil, nil // no range index for this column
    }

    // ... IntervalMatch handling ...

    // Union all block sets for each query value.
    union := make(map[int]struct{})
    for _, val := range pred.Values {
        blocks, err := r.BlocksForRange(col, val)  // LINE 46 - THE KEY CALL
        if err != nil {
            return nil, err
        }
        for _, b := range blocks {
            union[b] = struct{}{}
        }
    }
    return union, nil
}
```

**This is where BlocksForRange is called** (line 46). For each query value (in this case, the exact string "1f9202e7-4e50-59e2-b67c-60c893d98e8a"), the reader queries the range index.

### Step 5: Range Index Lookup
File: `/home/mdurham/source/blockpack/internal/modules/blockio/reader/reader.go:289-320`

```go
func (r *Reader) BlocksForRange(colName string, queryValue shared.RangeValueKey) ([]int, error) {
    if err := r.ensureRangeColumnParsed(colName); err != nil {
        return nil, err
    }

    idx := r.rangeParsed[colName]
    entries := idx.entries

    if len(entries) == 0 {
        return nil, nil
    }

    // Binary search: find the last entry whose lower bound ≤ queryValue.
    hi := sort.Search(len(entries), func(i int) bool {
        return compareRangeKey(idx.colType, entries[i].lower, queryValue) > 0
    })

    found := hi - 1
    if found < 0 {
        return nil, nil // queryValue is below all lower boundaries
    }

    blockIDs := entries[found].blockIDs
    result := make([]int, len(blockIDs))
    for i, id := range blockIDs {
        result[i] = int(id)
    }

    return result, nil
}
```

The method performs a **binary search** to find all blocks whose value range includes the query value.

## What Columns Are Indexed?

The range index stores min/max values for all columns, **except**:
1. `trace:id` (excluded because it's unique per trace)
2. `bool` type columns (excluded because cardinality ≤2)

File: `/home/mdurham/source/blockpack/internal/modules/blockio/writer/writer_block.go:1478-1485`

```go
// Feed range column index.
// Excluded: trace:id (unique per trace, not useful for block pruning)
//           Bool (no Range* equivalent; cardinality is always ≤2)
if name != traceIDColumnName && typ != shared.ColumnTypeBool {
    if key := encodeRangeKey(typ, val); key != "" {
        b.updateMinMax(name, typ, key)
    }
}
```

This means **span.aws.request_id IS included in the range index** because it's a string column and gets indexed via `updateMinMax()`.

### Where Span Attributes Get Added to Range Index

File: `/home/mdurham/source/blockpack/internal/modules/blockio/writer/writer_block.go:475-485`

```go
// --- Span attributes (interned column names, zero-copy values) ---
for _, kv := range span.Attributes {
    if kv == nil {
        continue
    }
    name := b.internColName(kv.Key, b.spanColNames, "span.")
    val := protoToAttrValue(kv.Value)
    b.addPresent(rowIdx, name, val.Type, val)  // LINE 483 - calls updateMinMax internally
}
```

The `addPresent()` function (lines 1437-1486) handles all span attributes and internally:
1. Adds the value to the column builder
2. **Calls `updateMinMax()` to track min/max for range index** (lines 1481-1485)

## Column Name Format

When a span attribute like `aws.request_id` is encountered:
1. It's prefixed with "span." via `internColName()`
2. Result: **"span.aws.request_id"**
3. This exact column name is used when querying the range index

File: `/home/mdurham/source/blockpack/internal/modules/blockio/writer/writer_block.go:481`

```go
name := b.internColName(kv.Key, b.spanColNames, "span.")
```

The TraceQL query `{span.aws.request_id = "..."}` uses the same column name format.

## Block Pruning Logic

The pruning happens in `leafBlockSet()` at `/home/mdurham/source/blockpack/internal/modules/queryplanner/selection.go:42-54`:

```go
// Union all block sets for each query value.
// Return non-nil even when empty — index was consulted, no blocks matched any value.
union := make(map[int]struct{})
for _, val := range pred.Values {
    blocks, err := r.BlocksForRange(col, val)
    if err != nil {
        return nil, err
    }
    for _, b := range blocks {
        union[b] = struct{}{}
    }
}
return union, nil
```

**Result semantics**:
- Returns a **non-nil map** when the range index was successfully consulted
- The map contains block IDs that **may** satisfy the predicate
- An **empty map** means the index found no matching blocks (all candidates pruned)
- Used in `pruneByIndexAll()` to intersect with candidate block set

## Conditions Triggering BlocksForRange

1. **Column must have a range index**
   - File: `/home/mdurham/source/blockpack/internal/modules/queryplanner/selection.go:22-24`
   - Checked via `r.RangeColumnType(col)` returning a valid type

2. **Predicate must be a leaf with Values** (equality)
   - File: `/home/mdurham/source/blockpack/internal/modules/executor/predicates.go:119-132`
   - Only point lookups (Values slice) trigger BlocksForRange
   - Range predicates (Min/Max) use BlocksForRangeInterval instead

3. **Not an OR with unconstrained children**
   - File: `/home/mdurham/source/blockpack/internal/modules/queryplanner/selection.go:73-77`
   - But still returns nil if column is absent (no range index)

## Test Coverage

The functionality is tested in:
- `/home/mdurham/source/blockpack/internal/modules/blockio/reader/reader_test.go:1049-1089` - `TestBlocksForRange_StringColumn()`
- `/home/mdurham/source/blockpack/internal/modules/blockio/reader/reader_test.go:1152+` - `TestBlocksForRangeInterval_StringColumn()`

## Conclusion

For the query `{span.aws.request_id = "1f9202e7-4e50-59e2-b67c-60c893d98e8a"}`:

1. BlocksForRange **IS called** via `leafBlockSet()` → line 46 in selection.go
2. The column name **"span.aws.request_id"** is passed exactly as-is to the range index
3. String equality attributes **ARE included** in the range index (stored at write time)
4. Block pruning **IS correctly applied** - only blocks whose min/max range includes the value are considered
5. If no blocks match, an empty set is returned, causing all blocks to be pruned for an AND predicate
