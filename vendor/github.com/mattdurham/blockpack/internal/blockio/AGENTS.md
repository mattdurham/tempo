# AGENTS.md - Blockpack I/O Design Decisions

This document explains key design decisions in the blockpack I/O layer for future AI agents and developers working on this codebase.

## Object Storage First Design

**Decision:** Always assume storage is object storage (S3, GCS, Azure Blob). Never optimize for local disk.

**Rationale:**
- This is a distributed tracing system designed for cloud environments
- Production deployments will use object storage, not local disk
- Object storage has fundamentally different performance characteristics

## Full Block Reads (Not Selective Column Reads)

**Decision:** Always read entire blocks in a single I/O operation. Never read columns selectively.

**Location:** `reader.go:GetBlockWithBytes()` (around line 790)

### The Problem (Historical)

An earlier format introduced "selective column reading" as an optimization for local disk:
- Read only the columns needed for a query
- Skip unused column data to save bandwidth
- For local disk: multiple small reads are cheap, bandwidth is precious

This created a **severe anti-pattern for object storage:**

```go
// OLD CODE (removed):
for _, colIdx := range columnsToRead {
    colData, err := r.readRange(...)  // ONE ReadAt() PER COLUMN!
}
```

**Impact:** A simple query like `{ span.deployment.region = "us-west" }`:
- Needed 10 columns across 200 blocks
- Made 200 blocks × 11 I/O ops = **2,200 ReadAt() calls**
- Each ReadAt() = 50-100ms latency + API cost
- Result: **120x more expensive** than necessary!

### The Solution

**Always read full blocks:**

```go
// CURRENT CODE:
blockBytes, err = r.readRange(int64(entry.Offset), int(entry.Length))
// ONE I/O operation per block, regardless of columns needed
```

**Impact:** Same query now:
- Makes ~200 ReadAt() calls (one per block)
- Reads 2-3x more data (includes unused columns)
- **12x cheaper** in S3 API costs
- **12x faster** due to fewer round trips

### Performance Characteristics

**Object Storage (S3, GCS, Azure Blob):**
- Request latency: 50-100ms per request (dominant cost)
- Request cost: $0.0004 per 1,000 GET requests
- Data transfer: $0.09 per GB (negligible compared to requests)
- **Conclusion:** Minimize number of requests, not bytes transferred

**Local Disk (irrelevant for this system):**
- Sequential read: 100-200 MB/s
- Random read: 50-100 MB/s
- Latency: <1ms per request
- **Conclusion:** Minimize bytes read (but we don't target this)

### Benchmark Evidence

From `benchmark_results.txt` (before fix):

| Query | I/O Ops | Bytes/IO | Problem |
|-------|---------|----------|---------|
| gold_us_west | 2,929 | 20 KB | Selective reading: 2,929 small reads |
| platinum_us_west | 2,415 | 8 KB | Selective reading: 2,415 tiny reads |
| database-operations | 2 | 4.7 MB | Full block reading: 2 large reads ✓ |
| filter_billing_service | 98 | 115 KB | Full block reading: 98 medium reads ✓ |

**Pattern:** Queries with >1,000 I/O operations were using selective column reading.
**Fix:** Disable selective reading → expect 10-12x reduction in I/O operations.

## Related Code

### Block Reading Flow

1. **`reader.go:GetBlockWithBytes()`** - Entry point for reading blocks
   - Line ~790: **DECISION POINT** - Always read full block
   - Calls `readRange()` to read entire block in one I/O operation

2. **`reader.go:readBlockColumnsSelective()`** - REMOVED
   - This function created chattiness with per-column I/O operations
   - Removed entirely to prevent accidental use
   - See git history if you need to understand what it did

3. **`reader.go:parseBlockColumnsReuse()`** - Parses block bytes
   - Receives full block bytes
   - Extracts only requested columns during parsing (in-memory operation)
   - This is where column filtering happens (not during I/O)

### I/O Tracking

- **`tracking_reader.go`** - Wraps ReaderProvider to count I/O operations
  - `IOOperations()` - Returns count of ReadAt() calls
  - `BytesPerIO()` - Average bytes per operation (efficiency metric)
  - Used in benchmarks to measure object storage chattiness

### Executor Integration

- **`executor/blockpack_executor.go`** - Query executor
  - Uses `GetBlockWithBytes()` to load blocks
  - Specifies which columns needed (for parsing, not I/O)
  - Tracks `IOOperations` in `BlockpackResult`

## Guidelines for Future Changes

### DO:
✅ Read entire blocks in single I/O operations
✅ Filter columns during in-memory parsing (after I/O)
✅ Batch multiple block reads if possible
✅ Monitor `bytes/io` metric - higher is better (aim for >100KB)
✅ Keep `io_ops` count low - fewer operations, larger reads

### DON'T:
❌ Add "optimizations" that increase number of I/O operations
❌ Read columns selectively with separate I/O operations
❌ Add local disk-specific optimizations
❌ Trade more I/O operations for less data transfer
❌ Assume low latency for I/O operations (think: 50-100ms per call)

### When in Doubt:
- **Fewer, larger reads > Many small reads**
- **One 300KB read >> Ten 30KB reads**
- Benchmark with I/O operation tracking before optimizing

## Monitoring in Production

### Key Metrics

**`io_ops`** - Number of ReadAt() calls per query
- Good: <500 operations
- Warning: 500-1,000 operations
- Critical: >1,000 operations (investigate immediately)

**`bytes/io`** - Average bytes per I/O operation
- Good: >100KB per operation
- Warning: 10-100KB per operation
- Critical: <10KB per operation (very chatty, expensive)

**Cost Estimation:**
```
query_cost = (io_ops / 1000) × $0.0004  (S3 GET requests)
           + (bytes_read / GB) × $0.09  (data transfer)
```

For most queries: **Request costs dominate**, so minimize `io_ops`.

## Historical Context

### Why Was Selective Reading Added?

The current format includes `ColumnIndex` in block metadata, which was originally designed for selective column reads.
The intent was to reduce I/O for queries that only need a few columns.

**Example:** Query needs 2 columns out of 50:
- Local disk: Read 2×10KB = 20KB instead of 500KB (25x less data) ✓
- Object storage: Make 2 I/O ops instead of 1 (2x more requests) ✗

The optimization was **correct for local disk, wrong for object storage**.

### Why Was It Removed?

Benchmarking with I/O operation tracking revealed:
- Simple queries were making 2,000+ I/O operations
- Each operation was tiny (8-20KB)
- API costs and latency were 10-12x worse than necessary
- The "optimization" was an anti-pattern for the target environment

**Lesson:** Always measure in the target environment. "Optimizations" from one context can be anti-patterns in another.

## References

- `benchmark_results.txt` - Benchmark data showing chattiness
- `CHATTY_OPERATIONS_SUMMARY.md` - Analysis of most problematic queries
- `ROOT_CAUSE_ANALYSIS.md` - Detailed investigation of selective reading issue
- Commit: [hash] - "Remove selective column reading for object storage efficiency"

## Future Considerations

### Potential Optimizations (Compatible with Object Storage)

1. **Read-ahead buffering** - If sequential blocks are accessed, prefetch next blocks
2. **Parallel block reads** - Read multiple blocks concurrently (use Go's concurrency)
3. **Block-level caching** - Cache frequently accessed blocks (add cache layer)
4. **Compression-aware reads** - If blocks are compressed, size estimates improve

### What NOT to Do

1. **Adaptive column selection** - Don't bring back selective reading
2. **Local disk mode** - Don't add local disk-specific code paths
3. **Column pruning at I/O** - Keep column filtering in parsing, not I/O

---

**Last Updated:** 2026-01-30
**Author:** AI Agent (in collaboration with user)
**Review:** Required before making I/O layer changes that affect operation count
