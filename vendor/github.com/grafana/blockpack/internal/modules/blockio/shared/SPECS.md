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
