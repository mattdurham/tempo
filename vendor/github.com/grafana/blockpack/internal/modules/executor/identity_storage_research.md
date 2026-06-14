# Research: Eliminating Dual Storage for Identity Columns (span:id, trace:id, span:parent_id)

**Date:** 2026-04-22  
**Status:** Feasibility Analysis  
**Scope:** span:id, trace:id, span:parent_id (3 identity columns)

---

## Executive Summary

Blockpack currently stores the three identity columns (**span:id**, **trace:id**, **span:parent_id**) in **dual storage**: both the block column payloads AND the intrinsic TOC section. This decision was made to avoid O(N) reverse lookups when materializing results. Removing dual storage would save approximately 20% file size but is **not feasible without significant architectural changes** due to the symmetry of access patterns between fast-path (intrinsic-only) queries and block-scan queries.

**Key Finding:** The exclusive-intrinsic model was attempted (PR #172, 2026-03-25) but reverted (commit 97171ac, 2026-03-27) after discovering an O(8.6B) operation worst-case performance regression when materializing results for span filters.

---

## Historical Context

### PR #172 Attempt: Exclusive-Intrinsic Storage (Reverted)

**Commit:** `f9b9a88` feat: intrinsic column index with dual-storage and VM range fix (#172)  
**Rolled Back:** commit `97171ac` fix: restore dual storage for intrinsic columns — fix PR #172 performance regression (#174)

PR #172 attempted to store identity columns **exclusively in the intrinsic TOC section**, removing `addPresent()` calls that wrote them to block payloads. This was motivated by:
- Eliminating ~20% redundant file size
- Simplifying the write path

**Why It Failed:**

The intrinsic TOC section is **sorted by VALUE** (for range scans and bloom pruning), not by BlockRef (block ID, row index). Reverse lookups — materializing field values for a known `(blockIdx, rowIdx)` during result collection — required scanning all N intrinsic entries because:

1. The intrinsic section uses paging and is partitioned by VALUE range, not BlockRef range
2. Page-level min/max bounds the VALUE range within the page, not the BlockRef range
3. There is no secondary index from BlockRef → value
4. No fast-path exists to locate a span's values in the intrinsic index

**Performance Impact:**

- **Worst case:** 2.8M spans × 11 intrinsic columns × 14 files = **O(8.6B) operations per query**
- **Benchmark:** Block column O(1) lookup = 309 ns/op vs intrinsic scan O(N) = 19186 ns/op (**62x slower**)

Evidence:
- Commit `97171ac` includes benchmarks `BenchmarkCollect_IntrinsicMaterialization` and `BenchmarkIntrinsicReverseLookup/BuildRefMap` that detect this regression
- File: `internal/modules/blockio/reader/intrinsic_reverse_bench_test.go` (266 lines of regression tests)

### Decision: Restore Dual Storage (Current State)

**Commit:** `97171ac` fix: restore dual storage for intrinsic columns — fix PR #172 performance regression (#174)

Dual storage was restored because:

1. **Dual Purpose Access Patterns:**
   - **Intrinsic-fast-path queries** (pure intrinsic predicates, zero-block-read): read span:id, trace:id from intrinsic TOC for filtering → O(1) value-range lookups
   - **Block-scan queries** (user attributes or mixed queries): filter blocks, then materialize identity columns for result tuples → O(1) row access via block columns

2. **Size Trade-off is Acceptable:**
   - ~20% file size increase vs 62x query latency regression
   - Object storage latency (50–100 ms per API call) dominates query cost
   - Extra bytes transferred are negligible vs query time savings

3. **Simplicity:**
   - Executor code is simpler: identity columns are available in both paths without special-case handling
   - `nilIntrinsicScan` exists but is now a conservative no-op for new files (block columns are always populated)

---

## Current Storage Architecture

### Identity Columns (3 columns)

```
Column Name      Data Type       Uses in Intrinsic     Uses in Block Columns
─────────────────────────────────────────────────────────────────────────
span:id          bytes[...]      Range index prep      Block scan result rows
trace:id         bytes[16]       Bloom filter, range   Block scan result rows
span:parent_id   bytes[...]      Range index prep      Block scan result rows
```

### Write Path (writer/writer_block.go)

All three are written via **dual-path calls**:

1. **feedIntrinsicBytes() → intrinsic accumulator**
   ```go
   b.feedIntrinsicBytes(traceIDColumnName, shared.ColumnTypeBytes, traceID, rowIdx)
   b.feedIntrinsicBytes(spanIDColumnName, shared.ColumnTypeBytes, spanID, rowIdx)
   b.feedIntrinsicBytes(spanParentIDColumnName, shared.ColumnTypeBytes, parentSpanID, rowIdx)
   ```

2. **addPresent() → block column payload**
   ```go
   b.addPresent(rowIdx, traceIDColumnName, ..., shared.AttrValue{Type: ..., Bytes: traceID})
   b.addPresent(rowIdx, spanIDColumnName, ..., shared.AttrValue{Type: ..., Bytes: spanID})
   b.addPresent(rowIdx, spanParentIDColumnName, ..., shared.AttrValue{Type: ..., Bytes: parentSpanID})
   ```

**Functions involved:**
- `feedSpanIdentifiers()` — feeds all three (called from `addRowFromProto`, `addRowFromTempoProto`, `addRowFromBlock`)
- `applyTraceID()`, `applySpanID()`, `applySpanParentID()` — compaction path dual storage

---

## Query Paths Using Identity Columns

### 1. Intrinsic-Fast-Path Queries (Zero-Block-Read)

**File:** `internal/modules/executor/stream.go`  
**Function:** `ExecuteTraceMetricsIntrinsic()`

**Query Pattern:** Pure intrinsic predicates (e.g., `{service.name="svc"} | rate()`)

**Access Path:**
1. `BuildPredicates()` reads `r.RangeColumnType(col)` for span:id, trace:id, span:parent_id
2. `translateNode()` attempts to encode values for range-index pruning
3. For bytes columns (span:id, trace:id, span:parent_id): skip range encoding (line 753: "Bytes flat column — not range-searchable")
4. Call `r.GetIntrinsicColumn(name)` to read intrinsic column via `lookupIntrinsicFieldsTyped()`
5. Return results **without reading any block columns**

**Result Materialization:** Uses intrinsic column values directly, no block reads needed

**Impact if block columns removed:** ✓ **No impact** — this path reads from intrinsic TOC only

---

### 2. Block-Scan Queries with Identity Filters

**File:** `internal/modules/executor/stream.go`  
**Function:** `scanBlocks()`

**Query Pattern:** Span ID filters (e.g., `{span.id="abc123..."}`) or parent ID filters

**Access Path:**
1. `ParseBlockFromBytes()` with `wantColumns` including block column definitions
2. Block columns are lazy-decoded on first access
3. Column values read via `col.StringValue(rowIdx)` or `col.BytesValue(rowIdx)` — O(1) access
4. Results are immediately populated into `SpanMatch` tuples

**Materialization:** Row values accessed via block columns — **O(1) per row**

**Critical Detail:** Executor code does NOT call `GetIntrinsicColumn()` for identity columns during block scan. It uses `blockColumnProvider.lookupColumn()` which returns the block-column version.

**Impact if block columns removed:** ✗ **SEVERE** — would force O(N) intrinsic scan for every block-scan query with identity filters

---

### 3. Mixed Queries (Both User Attributes and Intrinsic Predicates)

**File:** `internal/modules/executor/stream.go`  
**Function:** `ExecuteTraceMetrics()`, `Collect()`

**Query Pattern:** Predicates on both user attributes and identity columns
- Example: `{service.name="svc" && span.id="xyz"}`

**Access Path:**
1. Same as intrinsic-fast-path: `BuildPredicates()` builds range index predicates
2. Planner selects blocks based on predicates (bloom, range index)
3. Each selected block is scanned via `scanBlocks()`
4. During scan: identity columns are read from block payloads (O(1))
5. User attributes are read from block columns or require full scan

**Materialization:** Dual path
- Intrinsic columns from block payloads (O(1))
- User attributes from block columns (O(1) or O(N) depending on encoding)

**Impact if block columns removed:** ✗ **SEVERE** — mixed query materialization would require O(N) intrinsic scan per block

---

### 4. GetTraceByID Path (Tempo API)

**File:** `internal/modules/blockio/reader/intrinsic_reader.go`  
**Function:** `GetIntrinsicColumn()`

**Access Pattern:** Direct trace:id lookup without user attributes

**Current:** Uses intrinsic TOC exclusively (zero block reads)

**Impact if block columns removed:** ✓ **No impact** — this path uses intrinsic TOC only

---

## Detailed Feasibility Analysis

### Path 1: Remove Block Columns, Keep Intrinsic-Only

**What would break:**

1. **Block-scan query materialization:**
   - `spanMatchFromBlock()` → `SpanMatchFromRow()` needs identity column values
   - Currently reads from block payloads: O(1) per row
   - Fallback: scan intrinsic column O(N) for each block O(N) = O(N²) worst case

2. **Mixed query result collection:**
   - User attribute queries → block scan (required)
   - Then need identity columns → currently block columns, O(1)
   - Fallback: intrinsic reverse lookup O(N) per span

3. **Executor code path chaining:**
   - `nilIntrinsicScan()` exists but is documented as "conservative no-op"
   - Would need to become a real fallback path for all queries
   - Would require building reverse index: `map[BlockRef] → intrinsicRowValues`
   - Memory cost: 28–60 MB per open file × potentially 1000s of readers per query

**Estimated Performance Regression:**

For a query scanning 1000 blocks with 2000 spans/block:
- Current (block columns): 1000 × 2000 × 309 ns = 618 ms
- Fallback (intrinsic scan): 1000 × 2000 × 19186 ns = 38.4 seconds
- **62x slower**

---

### Path 2: Keep Intrinsic TOC, Remove Block Columns, Add BlockRef Index

**What would be needed:**

1. Add a `refIndex` field to `IntrinsicColumn`:
   ```go
   type IntrinsicColumn struct {
       // ... existing fields ...
       refIndex map[uint32][]uint16  // [blockID] → []rowIdx within that block
   }
   ```

2. Build the index lazily on first reverse lookup:
   ```go
   func (ic *IntrinsicColumn) EnsureRefIndex() error { ... }
   ```

3. Update writer to populate this index during intrinsic section construction

4. Modify executor `nilIntrinsicScan()` to use the index for O(log N) or O(1) lookups

**Issues:**

1. **Memory Cost per File:**
   - Average: `(100K distinct spans × 4 bytes) = 400 KB` per index
   - For 14 files with 10M+ spans: **400 KB × 3 columns × 14 = 16.8 MB** per Reader
   - **Unacceptable at scale:** Tempo deployments maintain 1000s of concurrent Readers

2. **Writer Complexity:**
   - Intrinsic columns are written in arbitrary order during `flushBlocks` parallel phase
   - Building `refIndex` requires two passes or locking
   - Adds serialization barrier to parallel write path

3. **Maintenance Burden:**
   - Breaks the clean separation: intrinsic section = VALUE-ordered, block columns = REF-ordered
   - New invariant: intrinsic columns must also maintain refIndex
   - Every new query type or result path needs to consider which access pattern to use

---

### Path 3: Store Identity Columns in Block Columns Only (Remove Intrinsic)

**What would break:**

1. **Intrinsic-Fast-Path Queries:**
   - `ExecuteTraceMetricsIntrinsic()` reads from intrinsic TOC
   - Fallback: read all matching blocks just to get identity columns
   - No longer "zero-block-read" — semantics change

2. **Bloom Filter Pruning:**
   - `fileBloomSvcNames` can be extended for trace:id, but requires new file-level indexes
   - File-level bloom currently does not exist for trace:id (not stored in intrinsic)
   - Would require adding file-level trace:id bloom at writer time

3. **Range Index:**
   - span:id and span:parent_id are not range-indexed (they're bytes, unordered)
   - span:id is used in bloom-only predicates currently
   - Removing intrinsic storage means losing the bloom-first prune opportunity

**Benefits:** ~20% file size savings

**Drawbacks:**
- Breaks the semantic "intrinsic fast path is zero-block-read" contract
- Requires reader changes to handle missing intrinsic columns
- Incompatible with lean readers for trace:id lookups

---

## Detailed Assessment: Identity Columns' Role in Query Execution

### span:id (Span Identifier)

**Storage in Intrinsic:**
- Not used for range-index pruning (bytes, unordered)
- Stored for file-level bloom filter (if enabled)
- Used in `BuildPredicates()` as bloom-only predicate

**Storage in Block Columns:**
- Primary use: result materialization in `SpanMatchFromRow()`
- O(1) access during block scan

**Query Impact Analysis:**

1. **Intrinsic-only queries** (e.g., `{service.name="prod"}`):
   - Do not filter on span:id
   - Do not need span:id values until result collection
   - Currently: read from intrinsic fast-path, span:id NOT needed
   - If blocks only: would need to read blocks to get span:id
   - **Significant impact:** Changes "zero-block-read" queries to require block reads

2. **Queries with span:id filter** (e.g., `{span.id="xyz..."}` or tree walk):
   - Intrinsic TOC provides bloom predicate for block pruning
   - Block scan provides row-level filtering
   - Result materialization uses block columns
   - **Impact:** Only block column removal is critical; intrinsic is optional

**Conclusion:** span:id dual storage is **justified** for intrinsic-fast-path zero-block-read queries

---

### trace:id (Trace Identifier)

**Storage in Intrinsic:**
- Used in file-level bloom filter (line 226 in reader/NOTES.md NOTE-008: "file-level bloom filters for service.name and trace:id")
- Range-index: not stored (bytes, unordered)
- File-level bloom enables `GetTraceByID()` zero-block-read fast path

**Storage in Block Columns:**
- Primary use: result materialization in `SpanMatchFromRow()`
- O(1) access during block scan
- Used by compaction path `addRowFromBlock()` to feed intrinsic accumulator

**Query Impact Analysis:**

1. **GetTraceByID queries** (Tempo UI):
   - Intrinsic bloom: checks file for trace:id presence
   - Block reads only if bloom hits
   - Result: spans in matching blocks, materialized with block-column trace:id
   - **If blocks only:** would need to read all blocks to find trace, then read again for value
   - **Impact:** Severe performance regression

2. **Metrics queries with trace:id filtering**:
   - Intrinsic bloom for block pruning
   - Block scan for row-level matches
   - Materialization: block columns
   - **If blocks only:** same issue as span:id

**Conclusion:** trace:id dual storage is **critical** for GetTraceByID and file-level bloom efficiency

---

### span:parent_id (Parent Span Identifier)

**Storage in Intrinsic:**
- Not used for range-index pruning (bytes, unordered)
- Used in tree-walk queries to find parent relationships

**Storage in Block Columns:**
- Used in structured queries that traverse parent pointers
- Result materialization

**Query Impact Analysis:**

1. **Tree-walk queries** (e.g., find root span):
   - Intrinsic TOC: not indexed for tree walks
   - Block scan: scan all spans to find parent relationships
   - **If blocks only:** no change; still requires block scan

2. **Direct parent lookups** (e.g., `{span.parent_id="xyz"}`):
   - Similar to span:id: needs intrinsic bloom and block columns
   - **If blocks only:** same regression

**Conclusion:** span:parent_id dual storage is **justified** for consistency with span:id and trace:id

---

## Potential Optimization Directions (Non-Breaking)

If blockpack wants to reduce the 20% file size increase from dual storage without removing it, consider:

### 1. Compress Intrinsic Block Columns Separately

Currently both intrinsic AND block columns are snappy-compressed at the outer level. Options:
- Use different compression for intrinsic columns (higher compression ratio)
- Store only non-identity columns in block payloads; fetch identity from intrinsic during scan
- **Trade-off:** Adds I/O complexity but retains O(1) fast-path

### 2. Delta Compression Across Spans

span:id and span:parent_id are often identical or share common prefixes within a block. Apply delta encoding at the block level:
- **Before:** Every span stores full 8-16 byte ID
- **After:** Store one baseline ID, then offsets/prefixes
- **Estimated savings:** 30-50% of identity column storage
- **Implementation:** Column encoding level (no architectural change needed)

### 3. Dictionary Encoding for Common IDs

If workloads have many repeated trace IDs within a file, use dictionary encoding in block payloads:
- Similar to string dictionaries already used
- Reference frequent IDs by index instead of full value
- **Estimated savings:** 20-40% depending on cardinality

### 4. Lazy Population of Block Columns for Identity

Implement true lazy population: populate identity columns in block payloads **only on first use**, not at write time:
- Writer: skip `addPresent()` for identity columns
- Reader: populate from intrinsic on first `ParseBlockFromBytes()` call if needed
- **Trade-off:** Requires reader-side lazy materialization logic; more complex
- **Benefit:** Saves 20% file size for intrinsic-only queries (which don't access block columns)

---

## Recommendations

### Short Term (No Risk)

1. **Do not attempt to remove dual storage.** The PR #172 experiment proved this is a performance cliff.

2. **Document the decision** in code comments and NOTES.md for future engineers:
   - Explain the O(8.6B) regression that occurred
   - Link to the reverted PR and benchmarks
   - Make it clear this is an intentional trade-off, not an oversight

3. **Monitor file size impact** in production:
   - Current blockpack files are 20% larger than parquet due to dual storage
   - Acceptable given query performance, but track if this becomes a blocker

### Medium Term (If Size Becomes Critical)

1. **Apply delta/dictionary compression** (Option 2 or 3 above):
   - No architectural change
   - Can reduce identity column size by 30-50%
   - Write-time only, read path unchanged

2. **Per-column compression tuning:**
   - Store identity columns with higher compression ratio
   - Use different algorithm (zstd) for intrinsic column blobs if beneficial
   - Profile real-world data to quantify savings

### Not Recommended

1. ~~Exclusive-intrinsic model~~ (PR #172 already tried, 62x slower)
2. ~~BlockRef index in intrinsic~~ (16.8 MB memory per 14 readers)
3. ~~Block-only storage~~ (breaks zero-block-read queries)

---

## Summary Table: Query Path Sensitivity to Dual Storage

| Query Type | Intrinsic TOC Used? | Block Columns Used? | Impact if Blocks Removed | Impact if Intrinsic Removed |
|---|---|---|---|---|
| Intrinsic-only (zero-block) | ✓ (bloom, values) | ✗ | None | Severe (forces block reads) |
| GetTraceByID | ✓ (bloom) | ✓ (result) | Severe (O(N) reverse lookup) | Moderate (no bloom, read all blocks) |
| Block scan + user attrs | ✓ (bloom) | ✓ (result) | Severe (O(N) per block) | Moderate (no bloom) |
| Tree walk (span:parent_id) | ✗ | ✓ (structure) | Severe (O(N) scan) | None |
| Span ID filter | ✓ (bloom) | ✓ (result) | Severe (O(N) lookup) | Moderate (no bloom) |

---

## References

**Key Commits:**
- `97171ac` — Revert PR #172: restore dual storage, explain O(8.6B) regression
- `f9b9a88` — Original PR #172: exclusive-intrinsic attempt
- `8129ce4` — Performance optimization that preceded the rollback

**Design Documents:**
- `internal/modules/blockio/writer/NOTES.md` — NOTE-001, NOTE-002 (detailed rationale)
- `internal/modules/blockio/reader/NOTES.md` — NOTE-008 (identity field storage decision)
- `internal/modules/executor/NOTES.md` — NOTE-050 (intrinsic signal columns description)

**Benchmarks:**
- `internal/modules/blockio/reader/intrinsic_reverse_bench_test.go` — BenchmarkIntrinsicReverseLookup
- `internal/modules/executor/intrinsic_bench_test.go` — BenchmarkCollect_IntrinsicMaterialization
- `internal/modules/blockio/span_fields_bench_test.go` — Per-span field access cost

**Code:**
- `internal/modules/blockio/writer/writer_block.go:feedSpanIdentifiers()` — Dual storage at write time
- `internal/modules/executor/column_provider.go:nilIntrinsicScan()` — Fallback for missing columns (currently no-op)
- `internal/modules/executor/stream.go:spanMatchFromBlock()` → `SpanMatchFromRow()` — Result materialization
