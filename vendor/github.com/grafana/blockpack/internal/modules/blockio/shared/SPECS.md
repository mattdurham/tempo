# blockio/shared — Interface and Behaviour Specification

This document defines the public contracts, types, and invariants for the
`internal/modules/blockio/shared` package. This package is the shared foundation for all
blockio components (writer, reader, compaction, queryplanner).

---

## 1. Package Responsibility

`shared` provides:
- **ColumnType enum**: logical type identifiers for all column types.
- **BlockMeta**: parsed block index entry with spatial and temporal metadata.
- **Trace-ID bloom filter API**: bloom filter for trace ID lookup (compact index).
- **RLE codecs**: presence bitset and index RLE encoding/decoding.
- **CoalesceConfig**: parameters for I/O merging.
- **Constants**: format magic numbers, version constants, limits.

`shared` has **no I/O**, **no allocations beyond what is necessary**, and **no external
dependencies** beyond the Go standard library.

---

## 2. ColumnType

```go
type ColumnType uint8

const (
    ColumnTypeString        ColumnType = 0
    ColumnTypeInt64         ColumnType = 1
    ColumnTypeUint64        ColumnType = 2
    ColumnTypeFloat64       ColumnType = 3
    ColumnTypeBool          ColumnType = 4
    ColumnTypeBytes         ColumnType = 5
    ColumnTypeRangeInt64    ColumnType = 6
    ColumnTypeRangeUint64   ColumnType = 7
    ColumnTypeRangeDuration ColumnType = 8
    ColumnTypeRangeFloat64  ColumnType = 9
    ColumnTypeRangeBytes    ColumnType = 10
    ColumnTypeRangeString   ColumnType = 11
    ColumnTypeUUID          ColumnType = 12 // string column stored as 16-byte binary UUID; StringValue() returns formatted UUID
    ColumnTypeVectorF32     ColumnType = 13 // flat float32 array; dimension stored in column encoding header
)
```

**Invariant:** ColumnType values 0–5 are "plain" types (direct values); 6–11 are "range"
types used in the range index; 12 is `ColumnTypeUUID` (a binary-encoded string variant);
13 is `ColumnTypeVectorF32` (flat float32 array for semantic embeddings). Values 14–255
are reserved.

**Wire format:** ColumnType is stored as a single byte in the column encoding header.
Do not reorder or remove values; doing so would break backward compatibility with existing
blockpack files.

Readers encountering `ColumnTypeVectorF32 = 13` in a block column header during non-vector
queries must lazy-skip the column (not request it in `wantColumns`). Only semantic search
queries request this column type.

---

## 3. BlockMeta

```go
type BlockMeta struct {
    Offset          uint64
    Length          uint64
    MinStart        uint64
    MaxStart        uint64
    SpanCount       uint32
    MinTraceID      [16]byte
    MaxTraceID      [16]byte
    Kind            BlockKind
}
```

### 3.1 Offset and Length

Byte offset and length of the block payload within the blockpack file. Together they define
the single contiguous I/O region for the block.

### 3.2 MinStart and MaxStart

Minimum and maximum `log:timestamp` (for log files) or `span:start` (for trace files) across
all spans in the block. Zero means "unknown" (not epoch). Used for time-range pruning.

**Invariant:** A block with `MinStart == MaxStart == 0` is always included in time-range
queries (unknown time; matches all ranges).

### 3.3 MinTraceID and MaxTraceID

Min and max trace IDs (lexicographic byte order) across all spans in the block. Used for
trace-ID pruning.

---

## 4. Trace-ID Bloom Filter API

The `shared` package retains bloom filter functions for **trace-ID lookup only**. These are
used by the compact index (`v2` format) to build a per-file bloom filter over trace IDs,
enabling `BlocksForTraceID` to skip blocks that definitely do not contain a queried trace.

Column-name bloom (`ColumnNameBloom`) was removed on 2026-03-07. CMS subsumes it: if a
column was never written to a block, `BlockCMS` returns nil and the planner passes
conservatively — identical behavior to a bloom miss, but CMS also provides value-level
pruning. See `NOTE-BLOOM-REMOVAL` in NOTES.md.

### 4.1 TraceIDBloomSize

```go
func TraceIDBloomSize(traceCount int) int
```

Returns the byte size of the bloom filter for the given trace count.

### 4.2 AddTraceIDToBloom / TestTraceIDBloom

```go
func AddTraceIDToBloom(bloom []byte, traceID [16]byte)
func TestTraceIDBloom(bloom []byte, traceID [16]byte) bool
```

Standard bloom filter operations over trace IDs. `TestTraceIDBloom` returns `true` when
the trace *may* be present; `false` only when it is definitely absent.

---

## 5. RLE Codecs

### 5.1 Presence RLE

Encodes a bitset (which rows have a column value present) as a run-length encoded byte slice.

```go
func EncodePresenceRLE(bitset []byte, nBits int) ([]byte, error)
func DecodePresenceRLE(data []byte, nBits int) ([]byte, error)
func IsPresent(bitset []byte, idx int) bool
func CountPresent(bitset []byte, nBits int) int
```

**Format:** Version byte (currently 0x01) followed by RLE-encoded runs of (length, byte) pairs.

**Invariant:** Round-trip property — `decode(encode(b, n), n)` must reproduce `b` bit-for-bit.
**Invariant:** `IsPresent` on an out-of-bounds index returns `false` (no panic).

### 5.2 Index RLE

Encodes a `[]uint32` dictionary index array as RLE data.

```go
func EncodeIndexRLE(indexes []uint32) ([]byte, error)
func DecodeIndexRLE(data []byte, count int) ([]uint32, error)
```

**Invariant:** Round-trip property — `decode(encode(s), len(s))` must produce `s` exactly.
**Invariant:** `DecodeIndexRLE` returns an error if the encoded data contains fewer elements
than `count`.

---

## 6. CoalesceConfig

```go
type CoalesceConfig struct {
    MaxGapBytes   int64   // max allowed gap between adjacent blocks to still merge
    MaxWasteRatio float64 // max ratio of gap bytes to useful bytes (1.0 = 100%)
    MaxReadBytes  int64   // max total bytes in a single coalesced request (0 = no limit)
}

var AggressiveCoalesceConfig = CoalesceConfig{
    MaxGapBytes:   4 * 1024 * 1024, // 4 MB
    MaxWasteRatio: 1.0,             // no waste limit
    MaxReadBytes:  8 * 1024 * 1024, // 8 MB per coalesced request
}
```

`AggressiveCoalesceConfig` merges any two adjacent blocks within 4 MB of each other, with
no waste-ratio constraint, capped at 8 MB per coalesced request. Appropriate for object
storage (S3/GCS) where per-request latency dominates I/O cost.

`MaxReadBytes` caps the total bytes in a single coalesced request. A block that is itself
larger than `MaxReadBytes` is still read whole; the limit only prevents additional blocks
from being merged in. Zero means no limit.

---

## 7. Format Constants

| Constant | Value | Description |
|---|---|---|
| `MagicNumber` | `0xC011FEA1` | File magic at byte 0 |
| `CompactIndexMagic` | `0xC01DC1DE` | Block index section magic |
| `TSIndexMagic` | `0xC011FEED` | TS index section magic |
| `VectorIndexMagic` | `0x56454349` | Vector index section magic ("VECI") |
| `VectorIndexVersion` | `0x01` | VectorIndex section version |
| `FooterV5Version` | 5 | V5 footer: V4 + vectorIndexOffset[8] + vectorIndexLen[4] |
| `FooterV5Size` | 46 | V5 footer size in bytes |
| `VersionV10` | 10 | File format version 10 |
| `VersionV11` | 11 | File format version 11 |
| `VersionV12` | 12 | File format version 12 (snappy metadata + signal type) |
| `SignalTypeTrace` | `0x01` | File contains OTEL trace spans |
| `SignalTypeLog` | `0x02` | File contains OTEL log records |

### Well-Known Vector Column Names

| Constant | Value | Description |
|---|---|---|
| `EmbeddingColumnName` | `__embedding__` | Float32 vector embedding column (ColumnTypeVectorF32) |
| `EmbeddingTextColumnName` | `__embedding_text__` | Source text used to generate the embedding (ColumnTypeString) |

Double-underscore prefix signals internal/synthetic columns that are generated at ingest
time and not present in the original OTLP payload.

---

## SPEC-V14-001: V14 Format Version Constants
*Added: 2026-04-10*

V14 introduces per-column outer snappy compression and independently-addressable metadata
sections. The following constants define the V14 format.

```go
VersionBlockV14   uint8  = 14  // block header version byte for V14 blocks
VersionBlockEncV3 uint8  = 3   // enc_version inside each column blob: no internal zstd sub-segments

FooterV7Version uint16 = 7    // footer version field value written by V14 writer
FooterV7Size    uint   = 18   // magic[4]+version[2]+dir_offset[8]+dir_len[4]

// Section type constants for V14 type-keyed directory entries (6 fixed file-level sections).
// Values 0x07+ are reserved for future type-keyed sections.
SectionBlockIndex  uint8 = 0x01  // block index section
SectionRangeIndex  uint8 = 0x02  // range index section
SectionTraceIndex  uint8 = 0x03  // trace ID index section (combines compact + full index)
SectionTSIndex     uint8 = 0x04  // timestamp index section
SectionSketchIndex uint8 = 0x05  // sketch index section (HLL, TopK, bloom per column)
SectionFileBloom   uint8 = 0x06  // file-level bloom filter section (FBLM)

// DirEntryKind constants distinguish the two section directory entry kinds.
DirEntryKindType uint8 = 0x00  // type-keyed entry (one of the 6 fixed sections)
DirEntryKindName uint8 = 0x01  // name-keyed entry (one file-level intrinsic column blob)
```

**Invariants:**
- All 6 section type constants are distinct and nonzero.
- `FooterV7Size == 18` is fixed; any change requires a new footer version.
- V14 files have no V3/V4/V12 footer or file header. The last 18 bytes are always the V7 footer (FooterV7Version == 7).
- File-level intrinsic column blobs use name-keyed entries (`DirEntryKindName`), not a single type-keyed section.

Back-ref: `internal/modules/blockio/shared/constants.go`

---

## SPEC-V14-002: Section Directory Entry Wire Formats
*Added: 2026-04-10 | Updated: 2026-04-10 — heterogeneous entry kinds*

The V14 section directory is a snappy-compressed blob. When decoded it contains:
`entry_count[4]` followed by `entry_count` entries of variable size.

Each entry starts with `entry_kind[1]` which determines the entry format:

**Type-keyed entry** (`entry_kind == DirEntryKindType == 0x00`) — 14 bytes total:
```go
// DirEntryType describes one of the 6 fixed file-level sections.
// Wire: entry_kind[1]=0x00 + section_type[1] + offset[8] + compressed_len[4] = 14 bytes.
type DirEntryType struct {
    SectionType   uint8   // one of SectionBlockIndex..SectionFileBloom (0x01–0x06)
    Offset        uint64  // absolute byte offset of the section in the file
    CompressedLen uint32  // byte length of the snappy-compressed section blob
}
```

| Field | Type | Bytes | Description |
|---|---|---|---|
| `entry_kind` | uint8 | 1 | 0x00 = type-keyed |
| `section_type` | uint8 | 1 | Section type identifier (0x01–0x06) |
| `offset` | uint64 LE | 8 | Absolute byte offset of section data in file |
| `compressed_len` | uint32 LE | 4 | Byte length of compressed section blob |

**Name-keyed entry** (`entry_kind == DirEntryKindName == 0x01`) — 15+len(name) bytes total:
```go
// DirEntryName describes one file-level intrinsic column blob.
// Wire: entry_kind[1]=0x01 + name_len[2] + name + offset[8] + compressed_len[4] = 15+len(name) bytes.
type DirEntryName struct {
    Name          string  // intrinsic column name (e.g. "span:name", "span:duration")
    Offset        uint64  // absolute byte offset of the column blob in the file
    CompressedLen uint32  // byte length of the snappy-compressed column blob
}
```

| Field | Type | Bytes | Description |
|---|---|---|---|
| `entry_kind` | uint8 | 1 | 0x01 = name-keyed |
| `name_len` | uint16 LE | 2 | Byte length of column name string |
| `name` | bytes | name_len | Column name (UTF-8) |
| `offset` | uint64 LE | 8 | Absolute byte offset of column blob in file |
| `compressed_len` | uint32 LE | 4 | Byte length of compressed column blob |

**Invariants:**
- The directory itself is snappy-compressed; `dir_offset`+`dir_len` in the V7 footer point to the compressed blob.
- Readers MUST build `map[uint8]DirEntryType` and `map[string]DirEntryName` for O(1) lookup.
- An absent section has no entry in the directory (not a zero-length entry).
- There is no `SectionIntrinsic` type-keyed entry; each intrinsic column gets its own name-keyed entry.

Back-ref: `internal/modules/blockio/shared/types.go:DirEntryType`, `types.go:DirEntryName`

---

## SPEC-V14-003: V14 Footer (FooterV7) Wire Layout
*Added: 2026-04-10*

The V14 footer occupies the last 18 bytes of every V14 file.

| Field | Type | Bytes | Description |
|---|---|---|---|
| `magic` | uint32 LE | 4 | Must equal `MagicNumber` (0xC011FEA1) |
| `version` | uint16 LE | 2 | Must equal `FooterV7Version` (7) |
| `dir_offset` | uint64 LE | 8 | Absolute byte offset of the snappy-compressed section directory |
| `dir_len` | uint32 LE | 4 | Byte length of the snappy-compressed section directory |

**Read sequence for V14 files:**
1. Read last 18 bytes → verify magic and version == 7.
2. Read `dir_len` bytes at `dir_offset` → snappy-decompress → parse `entry_count[4]` + N entries.
3. For each entry: read `entry_kind[1]`; if 0x00 → unmarshal `DirEntryType`; if 0x01 → unmarshal `DirEntryName`.
4. Build `map[uint8]DirEntryType` and `map[string]DirEntryName` for O(1) lookup.
5. Eager: read and parse `SectionBlockIndex`.
6. Lazy: read and parse all other sections on demand.

Back-ref: `internal/modules/blockio/writer/metadata.go:writeFooterV7`,
`internal/modules/blockio/reader/parser.go:readFooter`

---

## SPEC-V14-004: V14 Column TOC Entry Format
*Added: 2026-04-10*

Each column metadata entry in the V14 block TOC uses compressed+uncompressed length fields
instead of the V12 `data_len[8]` field. Total entry size is unchanged.

**V14 column TOC entry wire format:**

| Field | Type | Bytes | Description |
|---|---|---|---|
| `name_len` | uint16 LE | 2 | Length of column name string in bytes |
| `name` | bytes | N | Column name (UTF-8, not null-terminated) |
| `col_type` | uint8 | 1 | ColumnType enum value |
| `data_offset` | uint64 LE | 8 | Byte offset of the snappy-compressed column blob, relative to block start |
| `compressed_len` | uint32 LE | 4 | Byte length of the snappy-compressed column blob on disk |
| `uncompressed_len` | uint32 LE | 4 | Byte length after snappy decompression |

**Invariants:**
- `data_offset` is relative to the start of the block payload (not the file).
- Each column blob at `data_offset` is `snappy.Encode(rawEncodingPayload)` where
  `rawEncodingPayload` starts with `enc_version[1]=3` + `encoding_kind[1]` followed by
  encoding-specific bytes with no internal zstd sub-segments.
- `uncompressed_len` MUST equal `snappy.DecodedLen(compressedBlob)` and MUST NOT exceed
  `MaxBlockSize` to prevent decompression-bomb attacks.

Back-ref: `internal/modules/blockio/writer/writer_block.go`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## 8. Limits

| Constant | Value | Description |
|---|---|---|
| `MaxSpans` | 1,000,000 | Max spans per file |
| `MaxBlocks` | 65,535 | Max blocks per file (uint16 block ID in trace index) |
| `MaxColumns` | 10,000 | Max columns per file |
| `MaxDictionarySize` | 1,000,000 | Max dictionary entries per column |
| `MaxStringLen` | 10,485,760 (10 MB) | Max string value length |
| `MaxBytesLen` | 10,485,760 (10 MB) | Max bytes value length |
| `MaxBlockSize` | 1,073,741,824 (1 GB) | Max block payload size |
| `MaxMetadataSize` | 268,435,456 (256 MiB) | Max metadata section size (raised from 100 MiB 2026-03-06 for sketch data scaling) |

Back-ref: `internal/modules/blockio/shared/constants.go`
