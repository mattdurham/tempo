# Discovery: Blockpack Column Projection Capabilities

## Executive Summary

Blockpack **does support column projection** — it decodes only requested columns from disk. However, the current implementation has a fundamental architectural limitation: **block I/O is always full-block reads** (one S3 request per block), and column filtering happens **in-memory** after the bytes are fetched, NOT during the read operation itself.

This means column projection saves **decode CPU and memory** but provides **zero I/O savings**. For queries that need only a few columns (e.g., metrics queries reading only timestamps), this is a ~50-80% memory saving but the disk I/O pattern remains unchanged.

## Key Finding: The Column Filtering Pipeline

### 1. Query Compilation → Column Set

**File**: `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`

The `ProgramWantColumns()` function extracts a minimal set of columns needed to evaluate a TraceQL query:

```go
// Lines 210-244
func ProgramWantColumns(program *vm.Program, extra ...string) map[string]struct{} {
    // Returns nil if no predicates (= all columns decoded)
    // Otherwise unions:
    // 1. Leaf columns from the RangeNode tree (via collectNodeColumns)
    // 2. preds.Columns (explicit columns: negations, log:body, etc.)
    // 3. extra (caller-supplied: trace:id, span:id, etc.)
}

// Example: {service.name = "api"}
// Returns: map[string]struct{}{"service.name": {}}
// Example: {duration > 5ms}
// Returns: map[string]struct{}{"span:duration": {}}
```

This is called **once per block** in both trace and log execution paths.

### 2. Block Decode → Column Filtering

**File**: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/block_parser.go`

The `parseBlockColumnsReuse()` function implements the column filter at decode time:

```go
// Lines 103-113
func parseBlockColumnsReuse(
    rawBytes []byte,
    wantColumns map[string]struct{},    // nil = all columns
    prevBlock *Block,
    meta shared.BlockMeta,
    intern map[string]string,
) (*Block, error) {
    // Two-phase decode:
    // PHASE 1 (lines 146-203): Eagerly decode columns in wantColumns
    // PHASE 2 (lines 213-246): Register other columns as "lazy" (rawEncoding only)
}
```

**Key mechanism**:
- **Eager decode** (lines 146-203): For each column in `wantColumns`, decompress and materialize the full values (StringDict, Int64Idx, etc.)
- **Lazy registration** (lines 213-246): For columns NOT in `wantColumns`, store only the raw compressed bytes (`rawEncoding` field) and a pointer into the block's RawBytes
- When a lazy column is accessed later, it triggers `decodeNow()` which decompresses on-demand

This saves **memory for large columns not needed in the query**.

### 3. Execution Paths Using Column Projection

**File**: `vendor/github.com/grafana/blockpack/internal/modules/executor/executor.go`

**Path 1 — Collect (search queries)**:
```go
// Lines 66-210: Executor.Collect()
wantColumns := ProgramWantColumns(program)  // line 79
// ...
bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)  // line 170 (first pass)
// ...
if wantColumns != nil {
    bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)  // line 188 (second pass)
}
```

Two-pass decode:
1. **First pass**: Decode `wantColumns` (predicates) to evaluate ColumnPredicate
2. **Second pass**: If any rows match, re-decode `searchMetaColumns ∪ wantColumns` for output

**Path 2 — Execute (simpler path)**:
```go
// Lines 64-122
wantColumns := ProgramWantColumns(program, "trace:id", "span:id", "log:trace_id", "log:span_id")
// ...
bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)  // single pass only
```

### 4. Query Planner (No Column Awareness)

**File**: `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/planner.go`

The query planner **does NOT** know about column projection. It:
- Uses bloom filters and range indices to prune blocks
- Calls `BlocksForRange(col string, queryValue ...)` for each predicate column
- Returns a list of block indices to fetch

**There is no "fetch only these columns" interface** at the query planner level. Column filtering is entirely handled during the block decode phase, not at block selection time.

## Architecture: Where Column Projection Happens

```
Query Plan (TraceQL)
  ↓
ProgramWantColumns(program)
  ↓ (returns map[string]struct{})
GetBlockWithBytes(blockIdx, ...) — FULL BLOCK READ (always entire block)
  ↓ (returns raw []byte)
ParseBlockFromBytes(raw, wantColumns, meta)
  ↓
  ├─ parseBlockColumnsReuse(raw, wantColumns, ...)
  │  ├─ Phase 1: Eagerly decode columns in wantColumns
  │  └─ Phase 2: Lazily register columns NOT in wantColumns
  │
  └─ Block {
       columns: map[ColumnKey]*Column  (only wantColumns decoded)
       lazyColumnStore: []Column       (other columns as rawEncoding refs)
     }
```

**Key invariant**: The entire block is fetched from disk/S3, but only requested columns are decompressed and materialized in memory.

## I/O Impact: No Change at Block Level

Blockpack **does not support selective column I/O** because:

1. **Blocks are immutable, monolithic files** — metadata describes all columns, data section is a sequential concatenation
2. **All column metadata is read upfront** (`parseColumnMetadataArray` scans the full metadata array)
3. **No per-column offset tracking for selective reads** — the metadata records byte offsets, but block format doesn't support partial reads
4. **Design choice**: Full-block read minimizes S3 request latency (50-100ms per request far outweighs bytes transferred)

From the CLAUDE.md design document:
> "**Always read entire blocks in a single I/O operation. Never add per-column I/O.**
> This system targets object storage (S3/GCS/Azure) where request latency dominates cost."

## How to Estimate the Benefit

For broad metrics queries like `{}|rate()`:

| Scenario | I/O Savings | Decode/Memory Savings |
|----------|------------|-------------------|
| `{}` (all spans) | 0% | 0% (all columns decoded) |
| `{service.name="api"}` | 0% | ~30-40% (skip 60-70% of columns) |
| `{service.name="api" && duration>50ms}` | 0% | ~40-50% (skip service attr cols) |
| `{}|rate()` — timestamp only | 0% | ~90-95% (skip almost everything) |

The **0% I/O saving** is the root cause of the 4.5x slowdown on `{dur>50ms}` queries:
- Blockpack: Reads 288MB block, decodes all columns (search path doesn't know only 2 columns needed), scans all spans
- Parquet: Projects 51MB (only `service.name` + `duration`), scans only matching spans

## Current Implementation Status

### ✅ Implemented (Column Filtering)

- `ProgramWantColumns()` — extracts needed columns from compiled program
- `parseBlockColumnsReuse(..., wantColumns, ...)` — two-phase decode with lazy registration
- `CollectOptions.AllColumns` — override to force full decode for `IterateFields()` enumeration (NOTE-028)
- Both `Collect` and `Execute` use column filtering for predicate evaluation

### ❌ Not Implemented (Needed for Performance)

1. **Query planner column awareness** — planner doesn't know which columns are needed
   - Would require new interface method: `QueryPlanner.FetchBlocksForColumns(blocks, cols)`
   - But doesn't help because entire block must be read anyway

2. **Per-column I/O at block level** — not possible without file format change
   - Would require: storing column data in separate files/sections with independent seeks
   - Current blockpack format: block = header + metadata + sequential column data + trace index + footer
   - No support for "skip column X's data section" in a single block fetch

3. **Streaming decode** — decoder is all-or-nothing per block
   - Could potentially: start materializing predicate columns while block bytes are still flowing in
   - Current: waits for `GetBlockWithBytes` to complete, then parses

## Code References

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| Column extraction | `executor/predicates.go` | 221-244 | `ProgramWantColumns()` — union of columns from RangeNode tree + explicit Columns + extra |
| Column filtering | `blockio/reader/block_parser.go` | 103-257 | `parseBlockColumnsReuse()` with two-phase decode |
| Lazy column tracking | `blockio/reader/block.go` | 37-62 | `IsDecoded()`, `EnsureDecoded()` — checks and materializes lazy columns |
| First pass (search) | `executor/stream.go` | 79-192 | `Collect()` — calls `ParseBlockFromBytes` twice (predicate, then results) |
| Second pass config | `executor/stream.go` | 27-31 | `CollectOptions.AllColumns` — NOTE-028 |
| Eager decode | `blockio/reader/block_parser.go` | 146-203 | Loop over column metadata; skip if not in wantColumns |
| Lazy registration | `blockio/reader/block_parser.go` | 213-246 | Create lazy Column entries with rawEncoding slices |

## Recommendations for Tempo Integration

1. **For selective queries** (`{service="api"}`): Column projection is working; decoder saves ~30-40% memory
2. **For broad queries** (`{}`): No benefit until blockpack implements block-level column I/O (requires file format change)
3. **For metrics queries** (`{}|rate()`): Current 4.5x slowdown cannot be fixed without per-column I/O at block level
4. **Workaround for metrics**:
   - Metrics daemon could create index files mapping (block_id → timestamp_range)
   - Query planner could read this index instead of full blocks
   - Or Tempo could pre-filter metrics queries to known high-selectivity blocks

## Summary

Blockpack column projection **exists and works**, but is fundamentally limited by the full-block read architecture. It saves **CPU and memory** during decode but not **I/O bytes or requests**. This is by design for object storage (where latency >> bytes), but creates a performance cliff for broad queries with low-selectivity predicates that still need entire blocks.
