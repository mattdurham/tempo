# Discovery: Blockpack Intrinsic Column Format for Paged TOC Support

## Overview

Blockpack stores intrinsic column data (span:duration, span:name, resource.service.name, etc.) in a dedicated section of the file with a Table of Contents (TOC) and column data blobs. Current architecture uses a single flat TOC and monolithic column blobs. This discovery documents the existing format to plan paged TOC support.

---

## 1. IntrinsicColumn Struct Definition

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go:84-97`

```go
type IntrinsicColumn struct {
	// For flat columns (IntrinsicFormatFlat):
	Uint64Values []uint64   // non-nil for ColumnTypeUint64
	BytesValues  [][]byte   // non-nil for ColumnTypeBytes
	BlockRefs    []BlockRef // parallel to Uint64Values / BytesValues

	// For dict columns (IntrinsicFormatDict):
	DictEntries []IntrinsicDictEntry

	Name        string
	Count       uint32     // total number of rows stored (present rows only)
	Type        ColumnType
	Format      uint8      // 0x01=IntrinsicFormatFlat, 0x02=IntrinsicFormatDict
}

type IntrinsicDictEntry struct {
	BlockRefs []BlockRef
	Value     string     // string representation (for Int64: decimal string)
	Int64Val  int64      // set for Int64 type
}

// BlockRef identifies one row's location: (blockIdx, rowIdx) pair
type BlockRef struct {
	BlockIdx uint16 // block index in file
	RowIdx   uint16 // row index within block
}
```

---

## 2. Intrinsic Column Formats

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go:46-58`

Two formats are defined:

```
IntrinsicFormatFlat  uint8 = 0x01  // flat array: delta-encoded uint64 or length-prefixed bytes
IntrinsicFormatDict  uint8 = 0x02  // dictionary (string or int64 enum columns)
IntrinsicFormatVersion uint8 = 0x01 // first byte of each intrinsic column blob
```

### Format Selection Rules

- **Flat (uint64)**: `span:duration`, `span:start`, `span:end` — numeric columns that are sorted
- **Flat (bytes)**: UUID/binary columns
- **Dict (string)**: `span:name`, `span:status_message`, `resource.service.name` — distinct string values
- **Dict (int64)**: `span:kind`, `span:status`, `log:severity_number` — enum columns

---

## 3. Wire Format Details

### 3.1 IntrinsicColMeta (TOC Entry)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go:106-119`

```go
type IntrinsicColMeta struct {
	// Pointer fields first for better GC scan alignment.
	Name string // column name (e.g. "span:duration", "span:name")
	Min  string // encoded lower boundary value (8-byte LE for numeric, raw string for string/bytes)
	Max  string // encoded upper boundary value

	// Scalar fields.
	Offset uint64     // absolute file offset of the column data blob
	Length uint32     // byte length of the column data blob (snappy-compressed)
	Count  uint32     // total number of rows stored (present rows only)
	Type   ColumnType // ColumnTypeUint64, ColumnTypeBytes, ColumnTypeString, ColumnTypeInt64
	Format uint8      // IntrinsicFormatFlat or IntrinsicFormatDict
}
```

---

### 3.2 TOC Encoding (DecodeTOC / encodeTOC)

Files:
- Decoder: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go:18-96`
- Encoder: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go:428-473`

```
Wire format (uncompressed, then snappy-compressed):

toc_version[1]  = 0x01
col_count[4 LE] = number of entries

per entry:
  name_len[2 LE] + name[name_len]
  col_type[1]
  format[1]          = IntrinsicFormatFlat or IntrinsicFormatDict
  offset[8 LE]       = absolute file offset of column blob
  length[4 LE]       = byte length of column blob
  count[4 LE]        = number of rows present
  min_len[2 LE] + min[min_len]     = encoded min value
  max_len[2 LE] + max[max_len]     = encoded max value
```

**Encoding Rules for Min/Max:**
- Numeric (uint64, int64): 8-byte little-endian binary representation
- String: Raw UTF-8 string bytes
- Bytes: Raw bytes

---

### 3.3 Flat Column Encoding (IntrinsicFormatFlat)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go:220-307`

```
Wire format (uncompressed, then snappy-compressed):

format_version[1]  = 0x01
format[1]          = 0x01 (IntrinsicFormatFlat)
col_type[1]        = ColumnType byte

row_count[4 LE]    = total rows (N)
block_idx_width[1] = bytes per blockIdx in refs (1 or 2)
row_idx_width[1]   = bytes per rowIdx in refs (1 or 2)

VALUES SECTION:
  uint64 columns: delta-encoded uint64[N]
    first value is absolute, subsequent values are v[i]-v[i-1]
    (values sorted ascending so all deltas ≥0)

  bytes columns: length-prefixed bytes[N]
    len[2 LE] + raw_bytes for each value
    (values sorted lexicographically)

REFS SECTION:
  BlockRef[N] parallel to values
  Each BlockRef is (blockIdx, rowIdx) using variable-width encoding
  (blockIdx width from block_idx_width, rowIdx width from row_idx_width)
```

**Variable-Width Encoding Rules:**
- If max value ≤ 255: use 1 byte
- If max value > 255: use 2 bytes (little-endian uint16)
- Applied independently to blockIdx and rowIdx

---

### 3.4 Dict Column Encoding (IntrinsicFormatDict)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go:309-369`

```
Wire format (uncompressed, then snappy-compressed):

format_version[1]  = 0x01
format[1]          = 0x02 (IntrinsicFormatDict)
col_type[1]        = ColumnType byte

value_count[4 LE]  = number of unique values (V)
block_idx_width[1] = bytes per blockIdx (1 or 2)
row_idx_width[1]   = bytes per rowIdx (1 or 2)

per value (sorted by value):
  value_len[2 LE]  = byte length of value string (0 for int64: 8 bytes follow)
  value[value_len OR 8 bytes LE int64]
  ref_count[4 LE]  = number of block refs for this value (R)
  refs[R × (block_idx_width+row_idx_width)] = (blockIdx, rowIdx) per occurrence
```

**Value Sorting Rules:**
- String values: lexicographic string comparison
- Int64 values: numeric int64 comparison (ascending)
- Sentinel for int64: value_len=0 means 8 bytes of int64 follows

---

## 4. Decoding Functions

### 4.1 DecodeTOC

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go:18-96`

```go
func DecodeTOC(blob []byte) ([]IntrinsicColMeta, error)
```

- Decompresses snappy-compressed blob
- Parses toc_version, col_count, per-entry metadata
- Returns slice of `IntrinsicColMeta` entries
- Called during file open in `Reader.parseIntrinsicTOC()`

### 4.2 DecodeIntrinsicColumnBlob

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go:98-252`

```go
func DecodeIntrinsicColumnBlob(blob []byte) (*IntrinsicColumn, error)
```

Workflow:
1. Snappy-decompress blob
2. Parse format_version, format byte, col_type
3. Parse row_count (or value_count for dict)
4. Route to flat or dict decoding path
5. For flat: delta-decode uint64s OR copy length-prefixed bytes
6. For dict: iterate unique values, per-value decode refs into BlockRefs slice
7. Return populated `IntrinsicColumn` struct

---

## 5. BlockRef Structure

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go:77-82`

```go
type BlockRef struct {
	BlockIdx uint16 // identifies which block (0–65534 per MaxBlocks=65535)
	RowIdx   uint16 // identifies row within that block (0–65534 per MaxBlockSpans)
}
```

**Role:**
- Stored in parallel arrays (flat) or per-dict-entry (dict)
- Maps sorted intrinsic values back to their source spans
- Used by `BlocksFromIntrinsicTOC()` to identify which blocks match a predicate
- Variable-width encoded (1 or 2 bytes per field) to save space for small files

---

## 6. How Intrinsic Columns Are Written

### 6.1 Accumulation Phase

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go:40-128`

During block building, span intrinsic values are fed into per-column accumulators:

```go
// Feed uint64 intrinsic (span:duration, span:start, span:end)
func (a *intrinsicAccumulator) feedUint64(
	name string, colType ColumnType, val uint64, blockIdx uint16, rowIdx int
)

// Feed string intrinsic (span:name, span:status_message, resource.service.name)
func (a *intrinsicAccumulator) feedString(
	name string, colType ColumnType, val string, blockIdx uint16, rowIdx int
)

// Feed int64 intrinsic (span:kind, span:status, log:severity_number)
func (a *intrinsicAccumulator) feedInt64(
	name string, colType ColumnType, val int64, blockIdx uint16, rowIdx int
)
```

**Data Structures:**
- `flatAccum`: accumulates uint64Values or bytesValues + parallel refs
- `dictAccum`: maps unique string/int64 values to indices, collects refs per unique value
- `intrinsicAccumulator`: holds flatCols and dictCols maps by column name

### 6.2 Encoding Phase

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go:143-153`

```go
func (a *intrinsicAccumulator) encodeColumn(name string) ([]byte, error)
```

Per column:
1. Route to `encodeFlatColumn()` or `encodeDictColumn()`
2. Sort by value (ascending for numeric, lexicographic for string)
3. Compute min/max for TOC
4. Encode to wire format
5. Snappy-compress
6. Return compressed blob

### 6.3 TOC Writing

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer.go:814-869`

```go
func (w *Writer) writeIntrinsicSection() (tocOffset uint64, tocLen uint32, err error)
```

Workflow:
1. Iterate accumulated column names
2. Encode each column blob
3. Write column blobs to file at current file position
4. Collect IntrinsicColMeta entries (offset, length, count, min/max from encoder)
5. Call `encodeTOC(entries)` to serialize and snappy-compress TOC
6. Write TOC blob
7. Return TOC offset and length for v4 footer

**Safety Cap:** If total intrinsic rows exceed `MaxIntrinsicRows` (10M), entire section is skipped (returns 0, 0, nil).

---

## 7. How Intrinsic Columns Are Read and Used

### 7.1 TOC Parsing

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/intrinsic_reader.go:13-36`

```go
func (r *Reader) parseIntrinsicTOC() error
```

Called during `NewReaderFromProvider()`:
1. Check footer version is v4 and intrinsicIndexLen > 0
2. Read TOC blob from file
3. Call `shared.DecodeTOC(blob)`
4. Store entries in `r.intrinsicIndex` (map[string]IntrinsicColMeta)

### 7.2 Lazy Decoding

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/intrinsic_reader.go:75-123`

```go
func (r *Reader) GetIntrinsicColumn(name string) (*IntrinsicColumn, error)
```

Lazy decode pattern:
1. Check `r.intrinsicIndex` for column metadata
2. Check in-memory Reader cache (`r.intrinsicDecoded`)
3. Check process-level cache (`parsedIntrinsicCache`) — shares across Reader instances
4. If not cached:
   - Read column blob from file using `cache.GetOrFetch()`
   - Call `shared.DecodeIntrinsicColumnBlob(blob)`
   - Cache both locally and process-wide
5. Return decoded `IntrinsicColumn`

**Performance Note:** Single I/O per column, decoded result immutable, shared across Readers.

### 7.3 Block Pruning via Intrinsic TOC

File: `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go:238-336`

```go
func BlocksFromIntrinsicTOC(r *Reader, program *vm.Program) []int
```

Pruning strategy for each predicate leaf:

1. **Step 1: TOC min/max check** (no I/O)
   - `intrinsicTOCOverlaps(meta, leaf)` compares TOC min/max against query range
   - Returns empty if no possible matches

2. **Step 2: Dict column matching** (I/O: load blob once)
   - Call `r.GetIntrinsicColumn(colName)` → lazy decode
   - For exact-equality predicates: iterate DictEntries, collect BlockRefs for matching values
   - Return distinct block indices

3. **Step 3: Flat column matching** (I/O: load blob once)
   - Call `r.GetIntrinsicColumn(colName)` → lazy decode
   - For range predicates: binary-search sorted Uint64Values for matching range
   - Collect BlockRefs for matching rows
   - Return distinct block indices

4. **Result:** Blocks from all predicates are AND-combined (intersected)

**Note:** Only top-level AND leaves are evaluated; OR nodes are skipped (conservative).

---

## 8. Intrinsic Column Names (Metadata)

File: `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go:190-217`

```go
// Trace signal intrinsics (file must have signal_type=0x01):
var traceIntrinsicColumns = map[string]struct{}{
	"trace:id",              {},
	"span:id",               {},
	"span:parent_id",        {},
	"span:name",             {},
	"span:kind",             {},
	"span:start",            {},
	"span:end",              {},
	"span:duration",         {},
	"span:status",           {},
	"span:status_message",   {},
	"resource.service.name", {},
}

// Log signal intrinsics (file must have signal_type=0x02):
var logIntrinsicColumns = map[string]struct{}{
	"log:timestamp",          {},
	"log:observed_timestamp": {},
	"log:severity_number",    {},
	"log:severity_text",      {},
	"log:trace_id",           {},
	"log:span_id",            {},
	"log:flags",              {},
	"resource.service.name",  {},
}
```

**Intrinsic-Only Optimization:** `ProgramIsIntrinsicOnly(program)` returns true if all referenced columns are in the intrinsic set (fast-path: no block data needed).

---

## 9. Footer Integration (v4)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go:48-49`

```
FooterV4Version uint16 = 4
FooterV4Size    uint   = 34

Footer layout (v4):
  version[2]              = 0x0004
  header_offset[8 LE]
  compact_offset[8 LE]
  compact_len[4 LE]
  intrinsic_index_offset[8 LE]  ← pointer to TOC blob
  intrinsic_index_len[4 LE]     ← size of TOC blob (snappy-compressed)
```

**v4 Footer Structure:**
- Read from last 34 bytes of file
- `intrinsic_index_offset` = 0 means no intrinsic section (v3 file or section skipped)
- Both offset and length must be > 0 to indicate valid section

---

## 10. Existing Index Structures (No Current Paged TOC)

### 10.1 Trace Index (Block-level)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/trace_index.go`

Stores block IDs and trace ID ranges per block. Not used for intrinsic column selection.

### 10.2 Sketch Index (Column-major stats)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/sketch_index.go`

Stores HLL cardinality, CMS, TopK, and BinaryFuse8 per column per block. Independent of intrinsic columns.

### 10.3 Range Index (Column value ranges)

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/range_index.go`

Stores global min/max per column. Used for block pruning via bloom + range overlap checks.

**Current Architecture:** Intrinsic TOC is monolithic (not paged). All entries read in single `DecodeTOC()` call during file open.

---

## 11. Bloom Filter Infrastructure

File: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/bloom.go`

Trace ID bloom filter for file-level trace ID filtering:

```go
// TraceIDBloomSize computes size: (traceCount × 10 bits / 8) clamped to [128 bytes, 1 MiB]
func TraceIDBloomSize(traceCount int) int

// AddTraceIDToBloom adds trace ID using Kirsch-Mitzenmacher double-hashing
func AddTraceIDToBloom(bloom []byte, traceID [16]byte)

// TestTraceIDBloom checks membership (false-positive rate ~0.8% with k=7)
func TestTraceIDBloom(bloom []byte, traceID [16]byte) bool
```

**Note:** Separate from intrinsic column sketches. Operates on trace IDs only.

---

## 12. Key Constraints for Paged TOC Design

### 12.1 From Analysis

1. **BlockRef sizing:** blockIdx and rowIdx use variable-width encoding (1 or 2 bytes each)
   - Implication: cannot split refs array mid-encoding; boundaries must align to BlockRef boundaries

2. **Dict column structure:** Each value has its own ref_count + refs array
   - Implication: cannot easily split dict values across pages without restructuring per-value metadata

3. **Flat column sorting:** Values are sorted before encoding; refs parallel to sorted values
   - Implication: sorting must complete before paging (in-memory phase)

4. **Min/Max in TOC:** Global column min/max stored in TOC entry
   - Implication: min/max known after encoding; paging doesn't affect this

5. **Variable-width ref encoding:** blockIdx and rowIdx widths computed from all refs
   - Implication: ref encoding width must be global (no per-page variation)

6. **Lazy decode caching:** IntrinsicColumn fully decoded then cached
   - Implication: paged decode must assemble full IntrinsicColumn on first access

7. **Binary search on flat columns:** Reader binary-searches sorted uint64Values for range queries
   - Implication: paging must preserve sorted order within and across pages

### 12.2 Current Limits

- `MaxIntrinsicRows` = 10M per file (safety cap)
- `MaxStringLen` = 10.5 MB per value
- `MaxBytesLen` = 10.5 MB per value
- No per-page size limit currently defined

---

## 13. Summary of Current Format

| Component | Current State | Format |
|-----------|---------------|--------|
| **TOC** | Monolithic | Snappy-compressed; single `DecodeTOC()` call |
| **Column Blobs** | Per-column, monolithic | Snappy-compressed; sorted values + parallel refs |
| **BlockRef** | Parallel to values/entries | Variable-width (1-2 bytes per field); no cross-blob refs |
| **Dict Values** | Per-value, sorted | String or int64; per-value ref_count + refs array |
| **Flat Values** | Delta-encoded (uint64) or length-prefixed (bytes) | Sorted ascending/lexicographic |
| **Min/Max** | Per-column global | Encoded as string (8-byte LE for numeric, raw for string) |
| **Intrinsic-Only Fast Path** | Exists | `ProgramIsIntrinsicOnly()` + `BlocksFromIntrinsicTOC()` |
| **Lazy Decode** | Yes | Per-column, process-wide cache |
| **Cross-Page References** | N/A (no paging) | N/A |

---

## Files Reference

| File | Purpose |
|------|---------|
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go` | IntrinsicColumn, BlockRef, IntrinsicColMeta structs |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go` | Format version, format codes, MaxIntrinsicRows |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go` | DecodeTOC, DecodeIntrinsicColumnBlob |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go` | flatAccum, dictAccum, encodeFlatColumn, encodeDictColumn, encodeTOC |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/intrinsic_reader.go` | parseIntrinsicTOC, GetIntrinsicColumn, lazy decode |
| `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go` | BlocksFromIntrinsicTOC, intrinsicDictMatches, intrinsicFlatMatches |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/sketch_index.go` | Column sketch (HLL, CMS, TopK, Fuse) — independent of intrinsic |
| `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/bloom.go` | Trace ID bloom filter — independent of intrinsic |

---

## Next Steps for Paged TOC

Key design decisions to make:

1. **Page boundary alignment:** Natural boundaries = IntrinsicColMeta entries (keep TOC paged, not value blobs)
2. **Per-page metadata:** Page boundaries, entry counts, cumulative row counts?
3. **Backward compatibility:** v4 footer unchanged, v4 files without paging still valid
4. **Read path:** Stream page-by-page on first column access, assemble full IntrinsicColumn
5. **BlockRef consistency:** Ensure variable-width encoding handled per-page
6. **Dict value boundaries:** Can dict values split across pages, or must each page have complete dict entries?
