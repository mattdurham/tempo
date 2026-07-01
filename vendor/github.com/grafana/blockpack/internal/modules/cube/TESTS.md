# Cube Module Test Plan

## TEST-CUBE-007: Chunk Encode/Decode Round-Trip

**Goal:** Verify `DecodeChunk(EncodeChunk(cells)) == cells` for all cell counts (0, 1, 2048).

**Test cases:**

- Empty chunk: `[]Cell{}` → encoded → decoded → verify empty slice
- Single cell: `[]Cell{{Minute: 100, Dim1ID: 1, Dim2ID: 2, Count: 10}}` → verify exact match
- Full chunk: 2048 cells (generate programmatically with sequential minutes) → verify all cells match
- Verify compressed size < raw size for 2048 cells (snappy effectiveness)

**Acceptance:** All test cases pass, no panics on decode.

**File:** `internal/modules/cube/chunk_test.go:TestChunkEncodeDecodeRoundTrip`

---

## TEST-CUBE-008: Chunk Snappy Compression

**Goal:** Verify snappy-compressed chunks produce smaller output than raw input for realistic cell counts.

**Test cases:**

- 2048 cells with varied dimensions (100 distinct dim1, 20 distinct dim2) → verify `len(compressed) < len(raw)`
- Measure compression ratio: expect ~60-70% compression (24 KB → 8-12 KB)

**Acceptance:** Compression ratio within expected range, no decompression errors.

**File:** `internal/modules/cube/chunk_test.go:TestChunkCompressionRatio`

---

## TEST-CUBE-009: Reader GetCell (Hit and Miss Cases)

**Goal:** Verify `Reader.GetCell(minute, dim1, dim2)` returns correct count for present cells and `(0, false)` for absent cells.

**Test setup:** Build a test file with 300 cells in 3 chunks:

- Chunk 0: minutes 100-199, dim1="auth", dim2="200", counts [1..100]
- Chunk 1: minutes 200-299, dim1="auth", dim2="200", counts [101..200]
- Chunk 2: minutes 300-399, dim1="billing", dim2="404", counts [201..300]

**Test cases:**

- **Hit in chunk 0:** `GetCell(150, "auth", "200")` → `(51, true)`
- **Hit in chunk 2:** `GetCell(350, "billing", "404")` → `(251, true)`
- **Miss (minute out of range):** `GetCell(9999, "auth", "200")` → `(0, false)`
- **Miss (dim1 not in dict):** `GetCell(150, "nonexistent", "200")` → `(0, false)`
- **Miss (dim2 not in dict):** `GetCell(150, "auth", "999")` → `(0, false)`

**Acceptance:** All hit cases return correct count, all miss cases return `(0, false)`, no panics.

**File:** `internal/modules/cube/reader_test.go:TestReaderGetCell`

---

## TEST-CUBE-010: Reader GetCellsInRange (Chunk Pruning)

**Goal:** Verify `Reader.GetCellsInRange(minMinute, maxMinute, dim1, dim2)` returns only cells in the time range and prunes chunks outside the range.

**Test setup:** Build a test file with 5 chunks spanning minutes 0-499 (100 cells/chunk).

**Test cases:**

- **Full range:** `GetCellsInRange(0, 499, dim1, dim2)` → all 500 cells
- **Partial range:** `GetCellsInRange(120, 230, dim1, dim2)` → cells in minutes 120-230 only
- **Empty range:** `GetCellsInRange(600, 700, dim1, dim2)` → no cells
- **Cross-chunk range:** `GetCellsInRange(90, 310, dim1, dim2)` → cells from chunks 0,1,2,3
- **Single-minute range:** `GetCellsInRange(150, 150, dim1, dim2)` → 1 cell (minute 150)

**Acceptance:** All ranges return correct cell subsets, chunk pruning verified via instrumentation (log or count decompressed chunks).

**File:** `internal/modules/cube/reader_test.go:TestReaderGetCellsInRange`

---

## TEST-CUBE-011: Reader Binary Search Correctness

**Goal:** Verify binary search on chunk directory and within-chunk cells is correct under edge cases.

**Test cases:**

- **Exact boundary:** Cell at `minute = chunk[i].MinMinute` (first cell in chunk) → found in chunk i, not chunk i-1
- **Last cell in chunk:** Cell at last minute in chunk 0 → found in chunk 0, not chunk 1
- **Between chunks:** Query minute falls between chunk 0 max and chunk 1 min → return `(0, false)` (no such cell)
- **Single-chunk file:** Binary search with only 1 chunk (edge case: no directory scan)

**Acceptance:** All edge cases return correct results, no off-by-one errors.

**File:** `internal/modules/cube/reader_test.go:TestReaderBinarySearchEdgeCases`

---

## TEST-CUBE-012: End-to-End Round-Trip (Writer → Reader)

**Goal:** Verify `Writer.Flush()` → `Reader.OpenReader()` → `Reader.GetCell()` preserves all cell counts exactly.

**Test setup:**

1. Generate 5000 random cells: random minute in [0, 10000], 100 distinct dim1 values, 20 distinct dim2 values, random count in [1, 1000]
2. Write all cells via `Writer.AddCell()`, then `Writer.Flush()` to temp file
3. Open temp file via `Reader.OpenReader()`
4. For each original cell: verify `Reader.GetCell(minute, dim1, dim2)` returns exact count

**Test cases:**

- All 5000 cells found with correct counts
- Query non-existent cell → `(0, false)`
- Verify file size is reasonable (sparse storage, not bloated)

**Acceptance:** 100% of cells found, no false positives, file size < 100 KB (5000 cells × 12 bytes = 60 KB raw + overhead).

**File:** `internal/modules/cube/reader_test.go:TestReaderEndToEndRoundTrip` (integration test)

---

## Coverage Goals

- **Target coverage:** >80% across all cube module files
- **Priority:**
  - 100% coverage on encode/decode functions (critical correctness)
  - 100% coverage on binary search logic (edge cases)
  - 80%+ on Reader.GetCell/GetCellsInRange (query path)
  - 70%+ on Writer (optional, can defer to #443)

## Ingest accumulation (issue #443)

- **TEST-CUBE-013** `TestAccumulatorAdd` — spans increment matching cells; distinct
  `(dim1,dim2)` pairs produce distinct cells.
- **TEST-CUBE-013b** `TestCubeFilename` — `<tenant>/cubes/<hex id>/L0-<xid>.cube` layout.
- **TEST-CUBE-014** `TestAccumulatorFilter` — `duration < threshold` (and missing column) not counted.
- **TEST-CUBE-015** `TestAccumulatorSkipsMissingDims` — span missing either dimension is skipped.
- **TEST-CUBE-016** `TestAccumulatorFlushRoundTrip` — `FlushTo` → store → `cube.Reader` returns
  exact counts; every cell carries the accumulator's minute.
- **TEST-CUBE-017** `TestAccumulatorResetNoDoubleCount` — flush resets; next minute does not carry over.
- **TEST-CUBE-018** `TestAccumulatorFlushEmptyNoOp` — idle minute writes nothing.
- **TEST-CUBE-019** `TestWriterEncodeMatchesFlush` — `Encode()` bytes equal the `Flush()` file.
