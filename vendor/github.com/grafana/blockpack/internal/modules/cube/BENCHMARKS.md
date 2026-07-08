# Cube Module Benchmarks

## BENCH-CUBE-001: Cell Encode/Decode Throughput

**Goal:** Measure cell encode/decode throughput (cells/sec).

**Baseline target:** >1M cells/sec encode, >1M cells/sec decode.

**Rationale:** Cell encode/decode is the inner loop of Writer.Flush and Reader.GetCell. At 1M cells/sec, encoding 10k cells takes 10ms (acceptable for L0 flush).

**Benchmark:**

```go
func BenchmarkCellEncode(b *testing.B) {
    cell := Cell{Minute: 12345, Dim1ID: 42, Dim2ID: 99, Count: 1000}
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = EncodeCell(cell)
    }
}

func BenchmarkCellDecode(b *testing.B) {
    buf := EncodeCell(Cell{Minute: 12345, Dim1ID: 42, Dim2ID: 99, Count: 1000})
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = DecodeCell(buf)
    }
}
```

**Metrics:**

- ops/sec (cells/sec)
- B/op (bytes allocated per cell)
- allocs/op (allocations per cell)

**Acceptance:** >1M cells/sec, <20 B/op, <1 allocs/op.

**File:** `internal/modules/cube/cell_test.go:BenchmarkCellEncode/Decode`

---

## BENCH-CUBE-002: Chunk Encode/Decode Throughput

**Goal:** Measure chunk encode/decode throughput (cells/sec) for varying chunk sizes.

**Baseline target:** >100k cells/sec encode (2048 cells = 20ms), >200k cells/sec decode.

**Rationale:** Chunk encode is the bottleneck in Writer.Flush (snappy compression). At 100k cells/sec, flushing 10k cells takes 100ms (acceptable for 5-minute flush windows).

**Benchmark:**

```go
func BenchmarkChunkEncode(b *testing.B) {
    cells := make([]Cell, 2048)
    for i := range cells {
        cells[i] = Cell{Minute: uint32(i), Dim1ID: uint16(i%100), Dim2ID: uint16(i%20), Count: uint32(i)}
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = EncodeChunk(cells)
    }
}

func BenchmarkChunkDecode(b *testing.B) {
    cells := make([]Cell, 2048)
    for i := range cells {
        cells[i] = Cell{Minute: uint32(i), Dim1ID: uint16(i%100), Dim2ID: uint16(i%20), Count: uint32(i)}
    }
    compressed, _ := EncodeChunk(cells)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = DecodeChunk(compressed)
    }
}
```

**Varying chunk size:** Run benchmarks for 512, 1024, 2048, 4096 cells to validate 2048 is optimal (NOTE-CUBE-004).

**Metrics:**

- cells/sec
- B/op (bytes allocated)
- Compression ratio (raw bytes / compressed bytes)

**Acceptance:** >100k cells/sec encode, >200k cells/sec decode, compression ratio 2-3x.

**File:** `internal/modules/cube/chunk_test.go:BenchmarkChunkEncode/Decode`

---

## BENCH-CUBE-003: Reader GetCell Latency (Random Access)

**Goal:** Measure GetCell latency (μs per lookup) for cold-cache and hot-cache scenarios.

**Baseline target:** <50 μs per lookup for 10k cells (cold cache).

**Rationale:** Query latency dominated by S3 GET (50-100ms). Binary search overhead must be <0.1% of total query time. At <50 μs per cell lookup, 100 cell lookups add 5ms (negligible vs 100ms S3 GET).

**Benchmark:**

```go
func BenchmarkReaderGetCell(b *testing.B) {
    // Build test file with 10k cells in 5 chunks
    cells := generateRandomCells(10000, 100, 20)
    tmpFile := writeTempCubeFile(cells)
    defer os.Remove(tmpFile)

    r, _ := OpenReader(tmpFile)
    queries := generateRandomQueries(1000, cells)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        q := queries[i%len(queries)]
        _, _ = r.GetCell(q.Minute, q.Dim1, q.Dim2)
    }
}
```

**Scenarios:**

- **Cold cache:** Flush CPU cache before each lookup (realistic for per-query Reader construction)
- **Hot cache:** Repeated lookups in same file (best case)

**Metrics:**

- ns/op (latency per lookup)
- B/op (bytes allocated per lookup)
- allocs/op (allocations per lookup)

**Acceptance:** <50,000 ns/op (50 μs) cold cache, <10 B/op, <1 allocs/op.

**File:** `internal/modules/cube/reader_test.go:BenchmarkReaderGetCell`

---

## BENCH-CUBE-004: Reader GetCellsInRange Throughput

**Goal:** Measure GetCellsInRange throughput (cells/sec) for time-bounded range queries.

**Baseline target:** >500k cells/sec.

**Rationale:** Range queries (e.g., `rate_over_time[5m]`) are the dominant query pattern. At 500k cells/sec, fetching 1000 cells takes 2ms (acceptable for query execution).

**Benchmark:**

```go
func BenchmarkReaderGetCellsInRange(b *testing.B) {
    cells := generateRandomCells(10000, 100, 20)
    tmpFile := writeTempCubeFile(cells)
    defer os.Remove(tmpFile)

    r, _ := OpenReader(tmpFile)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = r.GetCellsInRange(100, 200, "dim1_val", "dim2_val")
    }
}
```

**Metrics:**

- ops/sec (range queries/sec)
- cells/sec (cells returned per sec)
- B/op (bytes allocated per query)

**Acceptance:** >500k cells/sec, <1 KB/op.

**File:** `internal/modules/cube/reader_test.go:BenchmarkReaderGetCellsInRange`

---

## BENCH-CUBE-005: Binary Search Depth (Verify O(log n))

**Goal:** Verify binary search depth is O(log n) by instrumenting comparisons per lookup.

**Baseline target:** log₂(chunks) + log₂(cells_per_chunk) comparisons.

**Rationale:** For 10k cells in 5 chunks: log₂(5) + log₂(2048) = 2.3 + 11 = ~14 comparisons per lookup (theoretical maximum).

**Test:**

```go
func TestBinarySearchDepth(t *testing.T) {
    cells := generateSequentialCells(10240) // 5 chunks × 2048 cells
    tmpFile := writeTempCubeFile(cells)
    defer os.Remove(tmpFile)

    r, _ := OpenReader(tmpFile)

    // Instrument binary search to count comparisons
    var compCount int
    r.compareFunc = func() { compCount++ }

    _, _ = r.GetCell(5000, "dim1", "dim2")

    expected := int(math.Ceil(math.Log2(5))) + int(math.Ceil(math.Log2(2048)))
    if compCount > expected*2 {
        t.Errorf("Binary search depth %d exceeds 2× theoretical max %d", compCount, expected)
    }
}
```

**Acceptance:** Comparison count ≤ 2× theoretical max (allows for implementation overhead).

**File:** `internal/modules/cube/reader_test.go:TestBinarySearchDepth`

---

## BENCH-CUBE-006: Memory Footprint

**Goal:** Measure memory footprint (RSS) for Reader with 10k cells loaded.

**Baseline target:** <10 MB RSS.

**Rationale:** Reader is constructed fresh per query (INVARIANT 0e1f00d5 from lth context). Memory must be bounded to avoid RSS spikes on high-QPS deployments.

**Test:**

```go
func TestReaderMemoryFootprint(t *testing.T) {
    cells := generateRandomCells(10000, 100, 20)
    tmpFile := writeTempCubeFile(cells)
    defer os.Remove(tmpFile)

    runtime.GC()
    var m1 runtime.MemStats
    runtime.ReadMemStats(&m1)

    r, _ := OpenReader(tmpFile)
    _, _ = r.GetCell(100, "dim1", "dim2") // Force load

    runtime.GC()
    var m2 runtime.MemStats
    runtime.ReadMemStats(&m2)

    allocated := m2.Alloc - m1.Alloc
    if allocated > 10*1024*1024 {
        t.Errorf("Memory footprint %d bytes exceeds 10 MB", allocated)
    }
}
```

**Acceptance:** <10 MB allocated per Reader.

**File:** `internal/modules/cube/reader_test.go:TestReaderMemoryFootprint`

---

## Summary Table

| Benchmark | Target | Rationale |
|-----------|--------|-----------|
| BENCH-CUBE-001 | >1M cells/sec encode/decode | Inner loop of flush/read |
| BENCH-CUBE-002 | >100k cells/sec encode | Flush bottleneck |
| BENCH-CUBE-003 | <50 μs per GetCell | Query latency <0.1% overhead vs S3 GET |
| BENCH-CUBE-004 | >500k cells/sec range | Dominant query pattern |
| BENCH-CUBE-005 | O(log n) comparisons | Verify binary search correctness |
| BENCH-CUBE-006 | <10 MB RSS | Bounded memory per Reader |

**Addendum (2026-07-08, issue #491, E-3):** BENCH-CUBE-001 through BENCH-CUBE-006 above all
benchmark the plain, unchanged 12-byte `Cell` path (`EncodeCell`/`DecodeCell`/`GetCell`/
`GetCellsInRange`) — wire-format v2's `NumAggAttrs==0` case is byte-identical to what these
benchmarks already measure, so none of the existing targets are stale. Note also that `GetCell`/
`GetCellsInRange` themselves are confirmed dead code in production as of E-3's Reader
generalization (the live read path uses `GetCellsRange`/`Rollup`/`CubeRollup`) — these benchmarks
remain useful as micro-benchmarks of the underlying codec, just not as a measurement of a
production-hot path. **Not yet benchmarked:** the `AggCell` path
(`EncodeAggCell`/`DecodeAggCell`/`Writer.AddAggCell`/`Reader.GetAggCellsInRange`), whose per-cell
byte cost scales as `12 + numAggAttrs*540` — e.g. 5 aggAttrs is 2712 bytes/cell, ~226x the base
cell size. Throughput, memory footprint, and snappy-compression-ratio characteristics at realistic
`numAggAttrs` counts (1-10) are unmeasured as of this phase. This is a genuine gap, not a claim
that existing targets need revision — flagging for a future benchmark task once real aggAttr data
volumes are exercised end-to-end in production.
