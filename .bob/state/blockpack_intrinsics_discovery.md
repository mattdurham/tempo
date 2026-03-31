# Discovery: Blockpack Intrinsic Columns Architecture

## Overview

Blockpack implements a dedicated intrinsic column storage system for trace span metadata. Intrinsic columns store well-known OTEL span proto fields separately from user-defined attributes, enabling compact encoding and efficient range-index-based pruning. The implementation consists of three layers: writer accumulation, codec serialization, and reader decoding.

---

## File Structure

| File | Purpose |
|------|---------|
| `/home/matt/source/blockpack/internal/modules/blockio/writer/intrinsic_accum.go` | Accumulator for intrinsic column data during block writing |
| `/home/matt/source/blockpack/internal/modules/blockio/shared/intrinsic_codec.go` | Snappy compression/decompression and wire format encoding/decoding |
| `/home/matt/source/blockpack/internal/modules/blockio/reader/intrinsic_reader.go` | Reader API for intrinsic column access and lazy decoding |
| `/home/matt/source/blockpack/internal/modules/blockio/shared/types.go` | ColumnType definitions (0-12) and AttrKV/ColumnKey types |
| `/home/matt/source/blockpack/internal/modules/blockio/shared/constants.go` | Intrinsic format version and limit constants |
| `/home/matt/source/blockpack/internal/modules/executor/predicates.go` | BlocksFromIntrinsicTOC and predicate building (currently handles column pruning via range index) |

---

## Intrinsic Columns Stored

### Trace Signal (Signal Type 0x01)

Per SPECS.md §11.1, span intrinsics are stored as columns with `:` (colon) separator:

| Column Name | Type | Source |
|---|---|---|
| `trace:id` | Bytes | Span.TraceId |
| `trace:state` | String | Span.TraceState |
| `span:id` | Bytes | Span.SpanId |
| `span:parent_id` | Bytes | Span.ParentSpanId |
| `span:name` | String | Span.Name |
| `span:kind` | Int64 | Span.Kind (enum 0-5) |
| `span:start` | Uint64 | Span.StartTimeUnixNano |
| `span:end` | Uint64 | Span.EndTimeUnixNano |
| `span:duration` | Uint64 | EndTimeUnixNano − StartTimeUnixNano |
| `span:status` | Int64 | Span.Status.Code (enum 0-2) |
| `span:status_message` | String | Span.Status.Message |
| `span:dropped_attrs` | Uint64 | Span.DroppedAttributesCount |
| `span:dropped_events` | Uint64 | Span.DroppedEventsCount |
| `span:dropped_links` | Uint64 | Span.DroppedLinksCount |
| `resource:schema_url` | String | ResourceSpans.SchemaUrl |
| `scope:schema_url` | String | ScopeSpans.SchemaUrl |

### Log Signal (Signal Type 0x02)

Per SPECS.md §11.4, log intrinsics replace span intrinsics:

| Column Name | Type | Source |
|---|---|---|
| `log:timestamp` | Uint64 | LogRecord.TimeUnixNano (always present) |
| `log:observed_timestamp` | Uint64 | LogRecord.ObservedTimeUnixNano (always present) |
| `log:body` | String | LogRecord.Body (always present) |
| `log:severity_number` | Int64 | LogRecord.SeverityNumber |
| `log:severity_text` | String | LogRecord.SeverityText |
| `log:trace_id` | Bytes | LogRecord.TraceId |
| `log:span_id` | Bytes | LogRecord.SpanId |
| `log:flags` | Uint64 | LogRecord.Flags |

---

## Architecture Components

### 1. Writer Accumulation (`intrinsic_accum.go`)

**Role:** Row-by-row collection of intrinsic values during block building.

**Structure:**

```go
// intrinsicAccumulator holds per-column accumulators for a file being written.
// One instance lives on Writer; fed row-by-row during block building.
type intrinsicAccumulator struct {
	flatCols map[string]*flatAccum    // uint64 or bytes columns
	dictCols map[string]*dictAccum    // string or int64 columns
}

// flatAccum accumulates values for flat columns (uint64 or bytes).
// All appended rows are stored in parallel arrays (values + refs).
type flatAccum struct {
	uint64Values []uint64              // Delta-encoded uint64 values
	bytesValues  [][]byte              // Variable-length bytes values
	refs         []BlockRef            // parallel block/row references
	colType      shared.ColumnType
}

// dictAccum accumulates values for dictionary columns (string or int64).
// Deduplicates values via index map; collects all refs per unique value.
type dictAccum struct {
	index   map[string]int            // encoded value → index in entries
	entries []dictEntry               // unique values + refs per value
	colType shared.ColumnType
}

// dictEntry holds one unique value and all block refs for that value.
type dictEntry struct {
	refs     []BlockRef
	strVal   string
	int64Val int64
}
```

**Key Methods:**

- `feedUint64(name, colType, val, blockIdx, rowIdx)` — Add uint64 value (span:start, span:end, span:duration, span:dropped_*)
- `feedString(name, colType, val, blockIdx, rowIdx)` — Add string value (span:name, span:status_message)
- `feedInt64(name, colType, val, blockIdx, rowIdx)` — Add int64 value (span:kind, span:status)
- `encodeColumn(name)` — Serialize accumulated column to snappy blob (flat or dict format)
- `computeMinMax(name)` — Extract min/max wire values after encoding (for TOC)
- `rowCount(name)` — Total rows for column
- `overCap()` — Check if any column exceeds `MaxIntrinsicRows` (65536)

**Adding New Columns:**

To add a new intrinsic column, the writer must:
1. Define it in SPECS.md §11.1 or §11.4 with correct type
2. Call `feedUint64`, `feedString`, or `feedInt64` at the appropriate source (span proto field)
3. The accumulator automatically handles deduplication (dict) or delta encoding (flat)
4. Writer encodes all accumulated columns at block flush time

---

### 2. Codec / Wire Format (`intrinsic_codec.go`)

**Role:** Snappy compression and binary serialization of intrinsic column data.

**TOC (Table of Contents) Format:**

Metadata about all intrinsic columns in a block is encoded in a snappy-compressed blob:

```
toc_version[1]  = 0x01
col_count[4 LE] = number of columns

per column:
  name_len[2 LE] + name[name_len]
  col_type[1]          = ColumnType byte
  format[1]            = IntrinsicFormatFlat (0x01) or IntrinsicFormatDict (0x02)
  offset[8 LE]         = byte offset in intrinsic section
  length[4 LE]         = byte length of snappy blob
  count[4 LE]          = row count (flat) or unique value count (dict)
  min_len[2 LE] + min[min_len]      = wire-encoded min value
  max_len[2 LE] + max[max_len]      = wire-encoded max value
```

**Flat Column Format (uint64 or bytes):**

```
format_version[1]  = 0x01
format[1]          = IntrinsicFormatFlat (0x01)
col_type[1]        = ColumnType byte
row_count[4 LE]    = total rows

block_idx_width[1] = 1 or 2 bytes per blockIdx in refs (var-width)
row_idx_width[1]   = 1 or 2 bytes per rowIdx in refs (var-width)

uint64 path:
  values[row_count × 8] = delta-encoded (v[0] absolute, v[i] = v[i-1] + delta)
bytes path:
  per row: len[2 LE] + bytes[len]

refs[row_count × (blockIdx_width + rowIdx_width)] = (blockIdx, rowIdx) parallel to values
```

Values are sorted before encoding (ascending for uint64, lexicographic for bytes).

**Dict Column Format (string or int64):**

```
format_version[1]  = 0x01
format[1]          = IntrinsicFormatDict (0x02)
col_type[1]        = ColumnType byte
value_count[4 LE]  = number of unique values

block_idx_width[1] = 1 or 2 bytes per blockIdx in all refs (var-width)
row_idx_width[1]   = 1 or 2 bytes per rowIdx in all refs (var-width)

per value (sorted by value):
  string path: len[2 LE] + string[len]
  int64 path:  len[2 LE] = 0 (sentinel) + int64_val[8 LE]
  ref_count[4 LE] = number of block refs for this value
  refs[ref_count × (blockIdx_width + rowIdx_width)] = (blockIdx, rowIdx)
```

Values are sorted before encoding (ascending int64 or lexicographic string).

**Type Definitions (Missing - Need Implementation):**

```go
// BlockRef identifies a row location within a block.
type BlockRef struct {
	BlockIdx uint16  // Block index (0-255 fits uint8; 256+ fits uint16)
	RowIdx   uint16  // Row index within block (0-65535)
}

// IntrinsicColMeta holds TOC metadata for one intrinsic column.
type IntrinsicColMeta struct {
	Name   string        // Column name (e.g. "span:name")
	Type   ColumnType    // ColumnType byte (0-12)
	Format uint8         // IntrinsicFormatFlat (0x01) or IntrinsicFormatDict (0x02)
	Offset uint64        // Byte offset in intrinsic data section
	Length uint32        // Byte length of snappy blob
	Count  uint32        // Row count (flat) or unique value count (dict)
	Min    string        // Wire-encoded minimum value
	Max    string        // Wire-encoded maximum value
}

// IntrinsicColumn holds decoded column data.
type IntrinsicColumn struct {
	Name           string                // Column name (populated by reader)
	Type           ColumnType            // ColumnType byte
	Format         uint8                 // IntrinsicFormatFlat or IntrinsicFormatDict
	Count          uint32                // Row count

	// Flat path (Format == IntrinsicFormatFlat):
	Uint64Values   []uint64              // Delta-decoded uint64 values
	BytesValues    [][]byte              // Variable-length bytes values
	BlockRefs      []BlockRef            // Parallel refs to values

	// Dict path (Format == IntrinsicFormatDict):
	DictEntries    []IntrinsicDictEntry  // Unique values + their refs
}

// IntrinsicDictEntry holds one unique value and all row locations.
type IntrinsicDictEntry struct {
	Value     string        // String value (when not int64)
	Int64Val  int64         // Int64 value (when colType is Int64 or RangeInt64)
	BlockRefs []BlockRef    // All (blockIdx, rowIdx) for this value
}

// Intrinsic format version and format bytes (constants).
const (
	IntrinsicFormatVersion = 0x01
	IntrinsicFormatFlat    = 0x01  // uint64 or bytes
	IntrinsicFormatDict    = 0x02  // string or int64
)
```

**Key Functions:**

- `DecodeTOC(blob []byte) ([]IntrinsicColMeta, error)` — Decompress and parse TOC blob
- `DecodeIntrinsicColumnBlob(blob []byte) (*IntrinsicColumn, error)` — Decode column data blob

---

### 3. Reader Decoding (`intrinsic_reader.go`)

**Role:** Reader-side API for accessing intrinsic columns with lazy decoding.

**Key Methods on Reader:**

- `parseIntrinsicTOC()` — Called during `NewReaderFromProvider`; parses TOC from footer
  - Only called for footer version V4 (added 2026-03-xx) with non-zero intrinsic section
  - Stores `intrinsicIndex map[string]IntrinsicColMeta` for O(1) metadata lookup
  - No I/O for metadata; only metadata is loaded

- `HasIntrinsicSection() bool` — Check if file has intrinsic columns

- `IntrinsicColumnMeta(name) (IntrinsicColMeta, bool)` — Get TOC metadata (cheap, no I/O)

- `IntrinsicColumnNames() []string` — Get all column names, sorted alphabetically

- `GetIntrinsicColumn(name) (*IntrinsicColumn, error)` — Get decoded column (lazy decode)
  - First call: read blob from storage (via cache), decompress, decode
  - Subsequent calls: return cached result
  - Returns nil if file has no intrinsic section or column not present
  - Thread-unsafe without external sync

**Caching:**

Intrinsic columns use the Reader's `cache` (RW provider with L1/L2 caching):
- Cache key format: `{fileID}/intrinsic/{columnName}`
- Avoids re-reading/decompressing same column multiple times
- Per-Reader in-memory cache (`r.intrinsicDecoded`) holds decoded results

---

## How New Intrinsic Columns Are Added

### Writer Side (writer/intrinsic_accum.go)

1. **Define in SPECS.md §11.1 or §11.4** with:
   - Column name (must include `:` separator)
   - ColumnType (String, Int64, Uint64, or Bytes)
   - Source proto field

2. **Feed values during span write** by calling:
   - `feedUint64("span:start", ColumnTypeUint64, startNano, blockIdx, rowIdx)`
   - `feedString("span:name", ColumnTypeString, name, blockIdx, rowIdx)`
   - `feedInt64("span:kind", ColumnTypeInt64, kindEnum, blockIdx, rowIdx)`

3. **Accumulator automatically handles:**
   - Type selection (flat for numeric, dict for high-cardinality string)
   - Deduplication (dict columns)
   - Delta encoding (flat uint64)
   - Min/max tracking for range index

### Reader Side (reader/intrinsic_reader.go)

1. **No code changes required** — reader automatically:
   - Parses TOC to discover all columns
   - Provides lazy-decode API
   - Handles both flat and dict formats transparently

### Executor / Query Planning Side (executor/predicates.go)

The predicates file currently handles column pruning via range index:
- `buildPredicates()` converts VM predicates to queryplanner predicates
- Intrinsic column names used directly in predicate building (e.g., "span:duration", "span:kind")
- **Does NOT currently use BlocksFromIntrinsicTOC** (function name suggests future use)
- Range index (built from min/max in IntrinsicColMeta) enables pruning on intrinsic columns

---

## Current Implementation Status

### Working Components

1. **Writer Accumulation** (`intrinsic_accum.go`)
   - Flat and dict accumulators complete
   - Feed methods for uint64, string, int64
   - Column encoding (snappy + wire format)
   - TOC encoding

2. **Codec** (`intrinsic_codec.go`)
   - TOC decode (`DecodeTOC`)
   - Column blob decode (`DecodeIntrinsicColumnBlob`)
   - Handles both flat and dict formats
   - Snappy compression/decompression

3. **Reader** (`intrinsic_reader.go`)
   - TOC parsing (`parseIntrinsicTOC`)
   - Lazy column decode (`GetIntrinsicColumn`)
   - Metadata queries (`IntrinsicColumnNames`, `IntrinsicColumnMeta`)
   - Caching support

### Missing Type Definitions

**Status:** Compilation fails with "undefined: IntrinsicColumn" and related types.

The following struct types are referenced but NOT defined in the codebase:

```go
type BlockRef struct {
	BlockIdx uint16
	RowIdx   uint16
}

type IntrinsicColMeta struct {
	Name   string
	Type   ColumnType
	Format uint8
	Offset uint64
	Length uint32
	Count  uint32
	Min    string
	Max    string
}

type IntrinsicColumn struct {
	Name         string
	Type         ColumnType
	Format       uint8
	Count        uint32
	Uint64Values []uint64
	BytesValues  [][]byte
	BlockRefs    []BlockRef
	DictEntries  []IntrinsicDictEntry
}

type IntrinsicDictEntry struct {
	Value     string
	Int64Val  int64
	BlockRefs []BlockRef
}
```

**Location:** These types should be defined in `/home/matt/source/blockpack/internal/modules/blockio/shared/types.go` alongside ColumnType, AttrKV, and ColumnKey.

**Missing Constants:**

```go
const (
	IntrinsicFormatVersion = 0x01
	IntrinsicFormatFlat    = 0x01
	IntrinsicFormatDict    = 0x02
	MaxIntrinsicRows       = 65536  // based on usage in intrinsic_accum.go overCap()
)
```

**Location:** These should be in `/home/matt/source/blockpack/internal/modules/blockio/shared/constants.go`.

---

## Integration with Query Planning (predicates.go)

Currently, `predicates.go` contains:

1. **buildPredicates()** — Converts VM RangeNodes to queryplanner.Predicate for bloom + range-index pruning

2. **searchMetaColumns()** — Returns columns needed for Tempo search result construction:
   - "trace:id", "span:id", "span:start", "span:end", "span:duration", "span:name", "span:parent_id", "resource.service.name"

3. **ProgramWantColumns()** — Minimal column set needed to evaluate a program (used for column projection)
   - Collects leaf columns from RangeNode tree
   - Adds explicit Columns (negations, log:body)
   - Used by two-pass column decode for performance

**Future Enhancement Note:** The function name `BlocksFromIntrinsicTOC` suggests future predicate optimization leveraging intrinsic TOC metadata, but currently no such function exists. This could be used to:
- Pre-check block viability before full range-index lookup
- Leverage exact value dictionaries for dict columns
- Optimize repeated queries on the same column

---

## Summary

**What:** Blockpack stores OTEL span and log intrinsics (well-known proto fields) in a dedicated columnar section with two encoding formats:
- **Flat:** delta-encoded uint64 or variable-length bytes
- **Dict:** deduplicated string or int64 values with reference indirection

**Where:**
- Writer accumulation: `writer/intrinsic_accum.go`
- Serialization codec: `shared/intrinsic_codec.go`
- Reader API: `reader/intrinsic_reader.go`
- Type definitions: Missing — should be in `shared/types.go`

**How columns are added:** Via `feedUint64`, `feedString`, `feedInt64` during span writing; reader discovers columns automatically from TOC.

**Current blocker:** Struct types (BlockRef, IntrinsicColMeta, IntrinsicColumn, IntrinsicDictEntry) must be defined in shared/types.go and constants (IntrinsicFormatVersion, MaxIntrinsicRows) in shared/constants.go for compilation to succeed.

**New column checklist:**
1. Add to SPECS.md §11.1 or §11.4
2. Call feed method in writer at appropriate proto field
3. No reader code changes needed — auto-discovered from TOC
4. Column name with `:` separator automatically creates range index entries for pruning
