# Memory Leak Fixes - Summary

This document summarizes the memory leak fixes implemented in the executor package.

## Changes Made

### 1. LazySpanFields Release Method (`lazy_fields.go`)

Added a `Release()` method to clear references and enable garbage collection:

```go
func (lsf *LazySpanFields) Release() {
    lsf.blocks = nil
    lsf.matchedColumns = nil
}
```

**Purpose**: LazySpanFields instances hold references to the shared blocks slice, which prevents garbage collection. The Release() method should be called after query results are fully serialized and sent to the client.

**Lifecycle Documentation**:
- Create LazySpanFields for query execution
- Use GetField/IterateFields during query processing
- Serialize results to client response
- Call Release() to clear references
- Do NOT access fields after Release()

### 2. Capacity Hints for Hot Path Allocations

#### column_scanner.go

**Line 272 - ScanEqual matchedRows map**:
```go
// Before: matchedRows := make(map[int]bool)
// After:  matchedRows := make(map[int]bool, cs.block.SpanCount()/10)
```
Pre-sizes based on ~10% selectivity estimate (conservative for typical queries).

**Line 25 - NewColumnMatchResult**:
```go
// Before: MatchedRows: make(map[int]struct{})
// After:  MatchedRows: make(map[int]struct{}, 32)
```
Default capacity of 32 for common small result sets.

**Line 55 - NewSpanMatchInfo**:
```go
// Before: MatchedColumns: make(map[string]any)
// After:  MatchedColumns: make(map[string]any, 4)
```
Pre-sizes for typical OR queries with 2-5 columns.

**Lines 1805, 1827 - SetOperations OR/AND**:
```go
// OR: Pre-size for sum of all result sizes (upper bound)
combined := make(map[int]*SpanMatchInfo, estimatedSize)

// AND: Pre-size by first result (intersection will be smaller)
combined := make(map[int]*SpanMatchInfo, len(results[0].MatchedRows))
```

#### column_scanner_streaming.go

**Line 21 - StreamScanEqual matchedRows map**:
```go
// Before: matchedRows := make(map[int]bool)
// After:  matchedRows := make(map[int]bool, cs.block.SpanCount()/10)
```
Matches the non-streaming implementation.

#### structural_operators.go

**Line 23 - BuildTraceGraph traceSpans map**:
```go
// Before: traceSpans := make(map[string][]*BlockpackSpanMatch)
// After:  traceSpans := make(map[string][]*BlockpackSpanMatch, estimatedTraces)
```
Estimates ~10 spans per trace (common for microservices).

**Line 30 - BuildTraceGraph graphs map**:
```go
// Before: graphs := make(map[string]*TraceSpanGraph)
// After:  graphs := make(map[string]*TraceSpanGraph, len(traceSpans))
```
Exact sizing based on number of traces.

**Lines 96, 115, 173 - Descendant/Child operation sets**:
```go
// matchSet, lhsSet, rhsSet all sized by input span counts
matchSet := make(map[string]bool, len(matches))
lhsSet := make(map[string]bool, len(lhsSpans))
rhsSet := make(map[string]bool, len(rhsSpans))
```

**Lines 219, 228 - SiblingOf parent mapping**:
```go
// parentToLhsSpanIDs: ~1 unique parent per 3 spans
parentToLhsSpanIDs := make(map[string]map[string]bool, len(lhsSpans)/3+1)

// siblings: ~2-4 children per parent
siblings = make(map[string]bool, 3)
```

**Lines 286, 292 - AncestorOf seen tracking**:
```go
rhsSet := make(map[string]bool, len(rhsSpans))
seen := make(map[string]bool, len(rhsSpans))
```

## Performance Impact

### Memory Allocation Reduction
- **Before**: Maps allocated with zero capacity, causing multiple reallocations as they grow
- **After**: Maps pre-sized based on estimated needs, reducing reallocation overhead

### Expected Improvements
1. **Fewer allocations**: Pre-sizing eliminates most map growth operations
2. **Better cache locality**: Contiguous memory allocated upfront
3. **Reduced GC pressure**: Fewer intermediate allocations to collect
4. **LazySpanFields.Release()**: Enables prompt garbage collection of block data after query completion

### Conservative Sizing Strategy
- Estimates are conservative (10% selectivity, 10 spans/trace, etc.)
- Maps will still grow if needed, but most queries fit within initial capacity
- Avoids over-allocation while providing significant benefit for typical workloads

## Testing

### Test Coverage
- `lazy_fields_lifecycle_test.go`: Tests Release() method lifecycle
  - Verifies fields are accessible before Release()
  - Verifies fields are inaccessible after Release()
  - Tests multiple Release() calls (idempotent)
  - Tests both GetField and IterateFields after Release()

### Existing Tests
All existing tests pass with these changes:
- `TestLazySpanFields_*`: Verifies LazySpanFields behavior
- `TestColumnScanner_*`: Verifies column scanning with capacity hints
- Structural operator tests verify graph building with pre-sized maps

## Recommendations

### Usage Guidelines
1. **Always call Release()** on LazySpanFields after serializing query results
2. **Do not access fields** after calling Release()
3. **Consider Release() in defer** for error handling paths

Example:
```go
lsf, err := NewLazySpanFields(blocks, blockIdx, spanIdx)
if err != nil {
    return err
}
defer lsf.Release() // Ensure cleanup even on error

// Use lsf.GetField() / lsf.IterateFields()
results := serializeResults(lsf)

// Release() called automatically by defer
return sendResults(results)
```

### Future Improvements
1. Monitor actual selectivity in production to refine capacity estimates
2. Consider adaptive sizing based on query patterns
3. Add metrics to track map growth frequency
4. Consider object pooling for frequently allocated maps

## Rationale for Capacity Hints

### Why 10% selectivity?
- Conservative estimate for filter predicates
- Avoids over-allocation for highly selective queries
- Handles typical "find errors" or "find slow requests" patterns

### Why 10 spans per trace?
- Common depth for microservice architectures (2-4 hops × 2-3 spans/service)
- Balances memory vs. allocation overhead
- Traces with 100+ spans are rare and will still work (map grows automatically)

### Why 32 for ColumnMatchResult?
- Small enough to avoid waste on tiny result sets
- Large enough to avoid immediate reallocation for common queries
- Power of 2 for efficient map bucketing

### Why 4 for MatchedColumns?
- OR queries typically combine 2-5 predicates
- Matches common patterns like "status=error OR http.status_code>=500"
- Small overhead if unused (16-32 bytes)
