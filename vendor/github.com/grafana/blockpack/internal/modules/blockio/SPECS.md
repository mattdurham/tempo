# Blockpack Binary Format Specification

This document defines the complete on-disk binary format for `.blockpack` files. All
multi-byte integers are **little-endian** unless noted otherwise. All offsets are absolute
byte positions within the file unless noted as relative to a block start.

---

## 0. Complete Format Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                        BLOCKPACK FILE FORMAT  (v11 / footer v3)                     ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  byte 0                                                                              ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ BLOCK 0                                                                        │ ║
║  │  ┌──────────────────────────────────────────────────────────────────────────┐  │ ║
║  │  │ BLOCK HEADER  (24 bytes)                                                  │  │ ║
║  │  │  magic[4]=0xC011FEA1 · version[1] · reserved[3]                          │  │ ║
║  │  │  span_count[4] · col_count[4] · trace_count[4] · trace_table_len[4]      │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ COLUMN METADATA  (col_count × entry)                                      │  │ ║
║  │  │  name_len[2] · name · type[1]                                             │  │ ║
║  │  │  data_offset[8] · data_len[8] · stats_offset[8] · stats_len[8]           │  │ ║
║  │  │  ↑ repeated col_count times, sorted by column name                        │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ COLUMN STATS  (one blob per column, same order as metadata)               │  │ ║
║  │  │  has_values[1]  (if 1: type-specific min + max fields follow)             │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ COLUMN DATA  (one encoded blob per span-level column)                     │  │ ║
║  │  │  enc_version[1]=2 · encoding_kind[1]                                      │  │ ║
║  │  │                                                                            │  │ ║
║  │  │  kind 1/2   Dictionary    index_width[1] · dict_zstd[4+N]                │  │ ║
║  │  │             row_count[4] · presence_rle[4+N] · indexes[N×width]          │  │ ║
║  │  │  kind 3/4   InlineBytes   row_count[4] · presence_rle[4+N]               │  │ ║
║  │  │             [present_count[4]] · per value: len[4] · bytes               │  │ ║
║  │  │  kind 5     DeltaUint64   span_count[4] · presence_rle[4+N]              │  │ ║
║  │  │             base[8] · width[1] · offsets_zstd[4+N]                       │  │ ║
║  │  │  kind 6/7   RLEIndexes    index_width[1] · dict_zstd[4+N]                │  │ ║
║  │  │             row_count[4] · presence_rle[4+N]                             │  │ ║
║  │  │             index_count[4] · rle_len[4] · rle_data[N]                    │  │ ║
║  │  │  kind 8/9   XORBytes      span_count[4] · presence_rle[4+N]              │  │ ║
║  │  │             xor_data_zstd[4+N]                                            │  │ ║
║  │  │  kind 10/11 PrefixBytes   span_count[4] · presence_rle[4+N]              │  │ ║
║  │  │             prefix_dict_zstd[4+N] · suffix_data_zstd[4+N]               │  │ ║
║  │  │  kind 12/13 DeltaDict     index_width[1] · dict_zstd[4+N]                │  │ ║
║  │  │             row_count[4] · presence_rle[4+N]                             │  │ ║
║  │  │             delta_indexes_zstd[4+N]                                       │  │ ║
║  │  │                                                                            │  │ ║
║  │  │  presence_rle = rle_len[4] · rle_data  (bitset: bit i set = row i present)│  │ ║
║  │  │  odd kinds = dense · even kinds = sparse (only present rows stored)        │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ TRACE TABLE  (only if trace_count > 0)                                    │  │ ║
║  │  │  trace_count[4] · col_count[4]                                            │  │ ║
║  │  │  per column: name_len[2] · name · type[1] · data_len[4] · data           │  │ ║
║  │  └──────────────────────────────────────────────────────────────────────────┘  │ ║
║  ├─ BLOCK 1 (same structure) ────────────────────────────────────────────────────┤ ║
║  ├─ ...                                                                            │ ║
║  └─ BLOCK N-1 ─────────────────────────────────────────────────────────────────── ┘ ║
║                                                                                      ║
║  header_offset  ◄── stored in footer                                                ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ FILE HEADER  (21 bytes)                                                        │ ║
║  │  magic[4]=0xC011FEA1 · version[1] · metadata_offset[8] · metadata_len[8]     │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
║                                                                                      ║
║  metadata_offset  ◄── from file header                                              ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ METADATA SECTION  (metadata_len bytes)                                         │ ║
║  │                                                                                 │ ║
║  │  ── BLOCK INDEX ──────────────────────────────────────────────────────────    │ ║
║  │  block_count[4]                                                                 │ ║
║  │  per block entry:                                                               │ ║
║  │    offset[8] · length[8] · kind[1]  ← kind byte added in v11                  │ ║
║  │    span_count[4] · min_start[8] · max_start[8]                                 │ ║
║  │    min_trace_id[16] · max_trace_id[16]                                          │ ║
║  │    column_name_bloom[32]  (256-bit bloom of column names in this block)         │ ║
║  │    value_stats  (variable: stats_count[1] + per-attr: name · type · min·max)   │ ║
║  │                                                                                 │ ║
║  │  ── RANGE INDEX ───────────────────────────────────────────────    │ ║
║  │  range_count[4]                                                             │ ║
║  │  per range column:                                                          │ ║
║  │    name_len[2] · name · type[1] · has_buckets[1]                               │ ║
║  │    (if has_buckets=1: min[8] · max[8] · boundary_count[4]                      │ ║
║  │     boundaries[N×8] · typed_count[4] · typed_boundaries…)                      │ ║
║  │    value_count[4]                                                               │ ║
║  │    per value: value_key (type-encoded) · block_id_count[4] · block_ids[N×4]    │ ║
║  │                                                                                 │ ║
║  │  ── COLUMN INDEX ─────────────────────────────────────────────────────────    │ ║
║  │  per block:                                                                     │ ║
║  │    col_count[4]                                                                 │ ║
║  │    per column: name_len[2] · name · offset[4] · length[4]                      │ ║
║  │    (offset and length are relative to block payload start)                      │ ║
║  │                                                                                 │ ║
║  │  ── TRACE BLOCK INDEX ────────────────────────────────────────────────────    │ ║
║  │  fmt_version[1]=0x01 · trace_count[4]                                           │ ║
║  │  per trace (sorted by trace_id):                                                │ ║
║  │    trace_id[16] · block_count[2]                                                │ ║
║  │    per block: block_id[2] · span_count[2] · span_indices[span_count×2]         │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
║                                                                                      ║
║  compact_offset  ◄── from footer  (absent if compact_len = 0)                       ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ COMPACT TRACE INDEX                                                             │ ║
║  │  magic[4]=0xC01DC1DE · version[1]=1 · block_count[4]                           │ ║
║  │  block_table: block_count × { file_offset[8] · file_length[4] }  (12 bytes ea) │ ║
║  │  fmt_version[1]=0x01 · trace_count[4]                                           │ ║
║  │  per trace: trace_id[16] · block_count[2]                                       │ ║
║  │             per block: block_id[2] · span_count[2] · span_indices[N×2]         │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
║                                                                                      ║
║  file_size - 22  (footer always 22 bytes)                                           ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ FOOTER  (22 bytes)                                                              │ ║
║  │  version[2]=3 · header_offset[8] · compact_offset[8] · compact_len[4]         │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## 1. Constants

```
MagicNumber        = 0xC011FEA1  // 4-byte file/block magic ("COLLFEA1" truncated)
CompactIndexMagic  = 0xC01DC1DE  // 4-byte compact trace index section magic

VersionV10  uint8  = 10          // Block encoding version (current minimum)
VersionV11  uint8  = 11          // Block encoding version (adds Kind byte to block index entry)

FooterV3Version uint16 = 3       // Current footer (22 bytes)

ColumnEncodingVersion uint8 = 2  // Per-column encoding header version

CompactIndexVersion uint8 = 1    // Compact trace index section version

ColumnNameBloomBits  = 256       // Bits in the column name bloom filter
ColumnNameBloomBytes = 32        // Bytes in the column name bloom filter
```

### 1.1 Limits

| Constant             | Value        | Description |
|----------------------|-------------|-------------|
| MaxSpans             | 1,000,000   | Maximum spans per block |
| MaxBlocks            | 100,000     | Maximum blocks per file |
| MaxColumns           | 10,000      | Maximum columns per block |
| MaxDictionarySize    | 1,000,000   | Maximum dictionary entries per column |
| MaxStringLen         | 10,485,760  | Maximum string/bytes value length (10 MB) |
| MaxBytesLen          | 10,485,760  | Maximum bytes value length (10 MB) |
| MaxBlockSize         | 1,073,741,824 | Maximum uncompressed block size (1 GB) |
| MaxMetadataSize      | 104,857,600 | Maximum metadata section size (100 MB) |
| MaxTraceCount        | 1,000,000   | Maximum unique traces per block |
| MaxNameLen           | 1,024        | Maximum column/field name length |
| MaxCompactSectionSize| 52,428,800  | Maximum compact trace index size (50 MB) |
| DefaultRangeBuckets | 1,000    | Default number of KLL quantile buckets per range column |
| RangeBucketKeyMaxLen | 50      | Maximum byte length of a RangeString/RangeBytes bucket key |

---

## 2. File Layout

```
┌─────────────────────┐  offset 0
│  Block 0 payload    │
├─────────────────────┤
│  Block 1 payload    │
├─────────────────────┤
│       ...           │
├─────────────────────┤
│  Block N-1 payload  │
├─────────────────────┤  header_offset (stored in footer)
│  File Header        │  21 bytes
├─────────────────────┤
│  Metadata Section   │  metadata_len bytes
├─────────────────────┤
│  Compact Trace Index│  compact_len bytes (absent if compact_len=0)
├─────────────────────┤  file_size - 22
│  Footer             │  22 bytes
└─────────────────────┘
```

The reader reads the last 22 bytes as the footer. The version field at offset 0 must equal 3.

### 2.1 Streaming Write Model

The file layout is intentionally write-ordered: blocks appear before the index structures
so that a compliant writer can produce a valid file using a **forward-only, non-seeking
output stream** (e.g., a network socket, pipe, or object storage upload).

A compliant writer MUST:

1. Write block payloads to the output stream immediately as each block is finalized.
   Blocks are written in order (block 0, block 1, …, block N-1) and must not be buffered
   in memory waiting for later blocks.
2. Record the absolute byte offset and length of each block as it is written. These values
   populate the Block Index in the Metadata Section.
3. After all blocks are written, append in order:
   - File Header
   - Metadata Section (block index + range index + column index + trace block index)
   - Compact Trace Index (if present)
   - Footer
4. Never seek backwards. The output stream is treated as append-only.

A compliant writer MUST NOT:

- Buffer all blocks in memory before writing any of them.
- Emit the header or footer before all block data.
- Require random-write access to the output.

**Consequence for readers:** A reader cannot locate any data without first reading the
footer (at the known position `file_size - 22`). The footer's `header_offset` field points
to the File Header, which in turn provides `metadata_offset` for the Metadata Section.
Block payloads are then located by offset values stored in the Metadata Section's Block
Index. There is no seekable structure at byte 0.

---

## 3. Footer (22 bytes)

The footer occupies the last 22 bytes of the file.

| Field          | Type       | Bytes | Description |
|----------------|-----------|-------|-------------|
| version        | uint16 LE | 2     | Must equal 3 |
| header_offset  | uint64 LE | 8     | Absolute byte offset of the File Header |
| compact_offset | uint64 LE | 8     | Absolute byte offset of the Compact Trace Index section |
| compact_len    | uint32 LE | 4     | Byte length of the Compact Trace Index section (0 = absent) |

If `compact_len == 0` the Compact Trace Index section is absent and `compact_offset` is ignored.

---

## 4. File Header (21 bytes)

Located at `header_offset` from the footer.

| Field           | Type       | Bytes | Description |
|-----------------|-----------|-------|-------------|
| magic           | uint32 LE | 4     | Must equal 0xC011FEA1 |
| version         | uint8     | 1     | File version (10 or 11) |
| metadata_offset | uint64 LE | 8     | Absolute byte offset of the Metadata Section |
| metadata_len    | uint64 LE | 8     | Byte length of the Metadata Section |

---

## 5. Metadata Section

Located at `metadata_offset`, length `metadata_len`. Contains:

1. **Block Index** — one entry per block in write order
2. **Range Index** — inverted index: value → block set
3. **Column Index** — per-block per-column byte offsets (for selective read)
4. **Trace Block Index** — trace ID → block + span row indices

### 5.1 Block Index

```
block_count    uint32 LE       // Number of blocks
[block_count × blockIndexEntry]
```

#### Block Index Entry (v10)

| Field           | Type           | Bytes | Description |
|-----------------|---------------|-------|-------------|
| offset          | uint64 LE     | 8     | Absolute file offset of block payload |
| length          | uint64 LE     | 8     | Byte length of block payload |
| span_count      | uint32 LE     | 4     | Number of spans in the block |
| min_start       | uint64 LE     | 8     | Minimum span start timestamp (nanoseconds) |
| max_start       | uint64 LE     | 8     | Maximum span start timestamp (nanoseconds) |
| min_trace_id    | [16]byte      | 16    | Minimum trace ID (lexicographic) |
| max_trace_id    | [16]byte      | 16    | Maximum trace ID (lexicographic) |
| column_name_bloom | [32]byte    | 32    | 256-bit bloom filter of column names |
| value_stats     | ValueStats    | var   | Per-attribute value statistics (see §5.1.1) |

**Total fixed: 100 bytes + ValueStats.**

#### Block Index Entry (v11 addition)

After `length` and before `span_count`, a single `kind` byte is inserted:

| Field  | Type  | Bytes | Description |
|--------|------|-------|-------------|
| kind   | uint8 | 1     | Block entry kind: 0 = leaf (only value in use) |

**v11 layout:** offset(8) + length(8) + kind(1) + span_count(4) + min_start(8) + max_start(8) + min_trace_id(16) + max_trace_id(16) + column_name_bloom(32) + value_stats(var).

#### 5.1.1 ValueStats Wire Format

ValueStats contains per-attribute statistics used for block pruning.

```
stats_count    uint8     // Number of attribute stat entries
[stats_count × AttributeStatEntry]
```

Each `AttributeStatEntry`:
```
name_len       uint16 LE
name           [name_len]byte
stats_type     uint8            // 0=None, 1=String, 2=Int64, 3=Float64, 4=Bool
[type-specific min/max fields]
```

Stats type payload:
- **None (0):** no additional bytes
- **String (1):** `min_len(4 LE uint32) + min_bytes + max_len(4 LE uint32) + max_bytes`
- **Int64 (2):** `min(8 LE int64) + max(8 LE int64)`
- **Float64 (3):** `min_bits(8 LE uint64) + max_bits(8 LE uint64)` (IEEE 754 bit representation)
- **Bool (4):** `min(1 uint8) + max(1 uint8)`

### 5.2 Range Index

Immediately follows the block index.

```
range_count    uint32 LE       // Number of range columns

[range_count × RangeColumnEntry]
```

#### RangeColumnEntry

Every column except `trace:id` and Bool columns MUST appear in the range index. All range columns use range bucketing; there is no exact-value index path. The `column_type` field MUST be one of the Range* types (6–11).

```
name_len       uint16 LE
name           [name_len]byte
column_type    uint8              // ColumnType (Range* only, see §7)
```

Bucket metadata always follows:

```
bucket_min     int64 LE           // Minimum value across all blocks
bucket_max     int64 LE           // Maximum value across all blocks
boundary_count uint32 LE
boundaries     [boundary_count × int64 LE]   // KLL quantile boundaries
typed_count    uint32 LE          // Type-specific boundary count (see below)
[typed boundaries — format depends on column_type]
```

Typed boundary formats (after typed_count):
- **RangeFloat64:** `typed_count × float64_bits(8 LE uint64)`
- **RangeString:** `typed_count × (len(4 LE uint32) + string_bytes)`
- **RangeBytes:** `typed_count × (len(4 LE uint32) + byte_data)`
- **RangeInt64/RangeUint64/RangeDuration:** `typed_count` must be 0; no additional bytes.

Then the value list:

```
value_count    uint32 LE
[value_count × RangeValueEntry]
```

#### RangeValueEntry

```
value_key      [type-specific encoding]     // The value key (see §5.2.1)
block_id_count uint32 LE
block_ids      [block_id_count × uint32 LE] // Sorted block IDs containing this value
```

#### 5.2.1 Value Key Encoding by Column Type

All range types use the **lower boundary value of the bucket** as the key — not a compact integer
bucket ID. This makes the index self-describing: a caller can find the right bucket for a query
value by binary-searching the stored keys without loading the full bucket metadata.

| ColumnType | Key Wire Format |
|---|---|
| RangeInt64, RangeDuration | `length_prefix(1 uint8) + key_data` where length_prefix is 8 and key_data is the lower boundary as int64 LE |
| RangeUint64 | `length_prefix(1 uint8) + key_data` where length_prefix is 8 and key_data is the lower boundary as uint64 LE |
| RangeFloat64 | `length_prefix(1 uint8) + key_data` where length_prefix is 8 and key_data is the lower boundary as float64 IEEE-754 bits LE |
| RangeString | `len(4 LE uint32) + key_data` where `key_data` is the lower boundary string truncated to `min(len(boundary), RangeBucketKeyMaxLen)` bytes |
| RangeBytes  | `len(4 LE uint32) + key_data` where `key_data` is the lower boundary bytes truncated to `min(len(boundary), RangeBucketKeyMaxLen)` bytes |

`RangeBucketKeyMaxLen = 50`. String/bytes boundary truncation may cause two distinct
boundaries to share a key prefix; their block ID sets are merged (acceptable false-positive rate).

Value keys within a column are stored in lexicographic sort order (sorted by their encoded
string representation before serialization).

#### Lookup semantics

To find blocks for a query value `V`:

1. Encode `V` using the key wire format above (e.g. 8-byte LE for int64, raw string for
   RangeString).
2. Binary-search the stored entries (sorted by ascending lower boundary) for the entry with
   the **largest lower boundary ≤ encoded(V)**.
3. Return that entry's block IDs. If no entry has a lower boundary ≤ encoded(V), return empty.

Example: entries `cal→[1,4,5]` and `cat→[3,4,5]`. A query for `"cam"` finds `cal ≤ cam < cat`
and returns `[1,4,5]`. A query for `"cat"` finds `cat ≤ cat` and returns `[3,4,5]`.

The comparison is type-aware: for numeric types (RangeInt64, RangeUint64, RangeFloat64,
RangeDuration) the 8-byte LE key is decoded to its numeric type before comparison. For
RangeString/RangeBytes, raw byte comparison is used. This is necessary because LE-encoded
values do not sort correctly under lexicographic byte comparison (e.g. `enc(256) < enc(10)`
as a byte string).

#### 5.2.2 Bucket Lower Bound Invariant

Every stored bucket key (the key written to the wire) MUST be lexicographically (or
numerically, for numeric types) **less than or equal to** the smallest value in that
bucket. Violations break the binary search at query time: if the stored key is greater than
the query value, the search will overshoot and return no results for that bucket.

Writers MUST enforce this invariant when computing bucket keys from KLL boundaries. If
`bounds[bid] > key` for any value being remapped (which can occur when KLL sampling is
non-representative), the writer MUST clamp the stored key to `min(bounds[bid], key)` before
writing.

#### 5.2.3 Bucket Distribution Requirement

Range buckets SHOULD be approximately uniformly populated. With `DefaultRangeBuckets =
1000`, each bucket should contain roughly **0.1% of the total distinct values** (and
transitively, 0.1% of the block IDs that contain those values).

**Why this matters for pruning:** A query predicate on a column eliminates all blocks not in
the matching bucket(s). If one bucket contains 99% of all block IDs, the range index
provides no meaningful pruning for the 99% of queries that land in that bucket.

**How to achieve this:** The KLL sketch used to compute bucket boundaries MUST see a
representative, unbiased sample of the column's distinct values. The writer computes KLL
boundaries from the deduped range index (§3 of NOTES.md §17) after all blocks are
built, rather than from per-span samples during block construction. This ensures every
distinct value is represented and the resulting quantiles divide the value space uniformly.

### 5.3 Column Index

Immediately follows the Range Index. Contains per-block per-column byte offsets.

```
[block_count × ColumnIndexBlock]
```

Each `ColumnIndexBlock`:
```
column_count   uint32 LE
[column_count × ColumnIndexEntry]
```

Each `ColumnIndexEntry`:
```
name_len       uint16 LE
name           [name_len]byte
col_offset     uint32 LE    // Byte offset relative to block start
col_length     uint32 LE    // Byte length of column data within block
```

Offsets and lengths here are relative to the block payload start (not the file start).
Both `col_offset + col_length` must be ≤ `blockIndexEntry.length`.

### 5.4 Trace Block Index

Immediately follows the Column Index.

```
format_version uint8         // Must equal 0x01
trace_count    uint32 LE
[trace_count × TraceEntry]   // Sorted by trace_id (lexicographic)
```

Each `TraceEntry`:
```
trace_id       [16]byte      // 128-bit trace ID
block_count    uint16 LE     // Number of blocks containing this trace
[block_count × TraceBlockEntry]
```

Each `TraceBlockEntry`:
```
block_id       uint16 LE     // Index into the block array
span_count     uint16 LE     // Number of spans in this block belonging to this trace
span_indices   [span_count × uint16 LE]  // Row indices within the block
```

---

## 6. Compact Trace Index Section

Located at `compact_offset`, length `compact_len` (v3 footer only). Provides a
self-contained trace lookup structure independent of the full metadata.

```
magic          uint32 LE     // Must equal 0xC01DC1DE
version        uint8         // Must equal 1
block_count    uint32 LE
block_table    [block_count × CompactBlockEntry]
format_version uint8         // 0x01 (same as Trace Block Index §5.4)
trace_count    uint32 LE
[trace_count × TraceEntry]   // Same format as §5.4 TraceEntry
```

Each `CompactBlockEntry`:
```
file_offset    uint64 LE     // Absolute file byte offset of block payload
file_length    uint32 LE     // Byte length of block payload
```

---

## 7. Column Types

| ID | Name | Go Type | Notes |
|----|------|---------|-------|
| 0  | String   | string  | May be auto-stored as 16-byte binary if UUID |
| 1  | Int64    | int64   | |
| 2  | Uint64   | uint64  | Used for timestamps (ns), counters |
| 3  | Float64  | float64 | |
| 4  | Bool     | bool    | Stored as uint8 (0/1) internally |
| 5  | Bytes    | []byte  | |
| 6  | RangeInt64    | int64   | Range-bucketed for high-cardinality indexing |
| 7  | RangeUint64   | uint64  | Range-bucketed |
| 8  | RangeDuration | int64   | Duration in nanoseconds, range-bucketed |
| 9  | RangeFloat64  | float64 | Range-bucketed |
| 10 | RangeBytes    | []byte  | Range-bucketed |
| 11 | RangeString   | string  | Range-bucketed |

Valid type byte range: 0–11. Values 12–255 are reserved.

---

## 8. Block Payload Format

Each block payload is a self-contained binary blob with its own header.

### 8.1 Block Header (24 bytes)

| Field          | Type       | Bytes | Offset | Description |
|----------------|-----------|-------|--------|-------------|
| magic          | uint32 LE | 4     | 0      | Must equal 0xC011FEA1 |
| version        | uint8     | 1     | 4      | Block version (10 or 11) |
| reserved       | [3]byte   | 3     | 5      | Must be zero |
| span_count     | uint32 LE | 4     | 8      | Number of spans in this block |
| column_count   | uint32 LE | 4     | 12     | Total number of columns (span + trace) |
| trace_count    | uint32 LE | 4     | 16     | Number of unique traces in this block |
| trace_table_len| uint32 LE | 4     | 20     | Byte length of the trace table at end of block |

### 8.2 Column Metadata Array

Immediately follows the block header. One entry per column, `column_count` entries total.

Each `ColumnMetadataEntry`:
```
name_len     uint16 LE
name         [name_len]byte
col_type     uint8              // ColumnType (see §7)
data_offset  uint64 LE          // Absolute byte offset of column data (within full block payload)
data_len     uint64 LE          // Byte length of column data; 0 = trace-level column (no span data)
stats_offset uint64 LE          // Absolute byte offset of column stats
stats_len    uint64 LE          // Byte length of column stats
```

**Trace-level columns** have `data_len = 0` and `data_offset = 0`. Their data lives in the
Trace Table at the end of the block. All offsets are relative to the start of the block
payload (offset 0 = first byte of block magic).

Column metadata entries are written in lexicographic order by column name. Span-level
columns come first; trace-level columns follow.

### 8.3 Column Stats Section

Immediately follows the Column Metadata Array. One stats blob per column in the same order
as the metadata entries.

#### Column Stats Wire Format

```
has_values   uint8    // 1 = stats present; 0 = no values in column (remaining bytes absent)
```

If `has_values == 1`, type-specific fields follow:

| Column Type | Additional bytes |
|---|---|
| String | `min_len(4 LE uint32) + min_bytes + max_len(4 LE uint32) + max_bytes` |
| Bytes  | `min_len(4 LE uint32) + min_bytes + max_len(4 LE uint32) + max_bytes` |
| Int64  | `min(8 LE int64) + max(8 LE int64)` |
| Uint64 | `min(8 LE uint64) + max(8 LE uint64)` |
| Float64 | `min_bits(8 LE uint64) + max_bits(8 LE uint64)` (IEEE 754 bit representation) |
| Bool   | `min(1 uint8) + max(1 uint8)` (0=false, 1=true) |

### 8.4 Column Data Section

Immediately follows the Column Stats Section. One data blob per span-level column (not
trace-level) in the same order as the metadata entries. The absolute offset and length of
each blob are recorded in the corresponding `ColumnMetadataEntry`.

All column data blobs start with the **Column Encoding Header**:

```
encoding_version  uint8    // Must equal 2
encoding_kind     uint8    // See §9
```

### 8.5 Trace Table (optional)

Located at `max_data_end` to `max_data_end + trace_table_len`. Only present if
`trace_count > 0`.

```
trace_count    uint32 LE
column_count   uint32 LE
[column_count × TraceTableColumnEntry]
```

Each `TraceTableColumnEntry`:
```
name_len    uint16 LE
name        [name_len]byte
col_type    uint8
data_len    uint32 LE
data        [data_len]byte     // Same column encoding format as span columns (§8.4)
```

Trace table columns have `trace_count` rows, not `span_count`. They are expanded to
span-level at query time using the `trace.index` span column (a uint64 column mapping
each span row to its trace index).

---

## 9. Column Encoding Kinds

All encodings begin with the 2-byte Column Encoding Header (§8.4). The encoding_kind byte
determines the remainder of the wire format.

### Encoding Kind Table

| Kind | Value | Name | Applicable Types |
|------|-------|------|-----------------|
| 1  | Dictionary | All types | Default |
| 2  | SparseDictionary | All types | >50% nulls |
| 3  | InlineBytes | Bytes | bytes columns |
| 4  | SparseInlineBytes | Bytes | bytes + >50% nulls |
| 5  | DeltaUint64 | Uint64 | timestamps, monotonic data |
| 6  | RLEIndexes | All types | low-cardinality |
| 7  | SparseRLEIndexes | All types | low-cardinality + >50% nulls |
| 8  | XORBytes | Bytes | ID columns (span:id, etc.) |
| 9  | SparseXORBytes | Bytes | ID columns + >50% nulls |
| 10 | PrefixBytes | Bytes | URL/path columns |
| 11 | SparsePrefixBytes | Bytes | URL/path columns + >50% nulls |
| 12 | DeltaDictionary | Bytes | trace:id (sorted, sequential) |
| 13 | SparseDeltaDictionary | Bytes | trace:id + >50% nulls |

### 9.1 Presence RLE

Used in most encodings. A compact bitset with RLE compression:

```
rle_len      uint32 LE
rle_data     [rle_len]byte     // RLE-encoded presence bitset
```

The decoded result is a bitset of `span_count` bits. Bit `i` is set if span `i` has a
value. The RLE format within `rle_data` is defined in `shared/index_rle.go`:

```
version      uint8 = 1
run_count    uint32 LE
[run_count × Run]
```
Each `Run`:
```
length       uint32 LE    // Number of consecutive same-value bits
value        uint32 LE    // 0 = absent, 1 = present
```

### 9.2 Dictionary Encodings (kinds 1, 2, 6, 7)

Used for String, Int64, Uint64, Bool, Float64, and Bytes types.

```
index_width    uint8                    // 1, 2, or 4 (bytes per index value)
dict_len       uint32 LE               // Byte length of compressed dictionary
dict_data      [dict_len]byte           // zstd-compressed dictionary body
row_count      uint32 LE               // Must equal block span_count
presence_rle   [see §9.1]
indexes        [see below]
```

**Dictionary body (after zstd decompress):**
```
entry_count    uint32 LE
[entry_count × DictEntry]
```

Entry format by type:
- **String:** `len(4 LE uint32) + string_bytes`
- **Int64:** `value(8 LE int64)`
- **Uint64:** `value(8 LE uint64)`
- **Bool:** `value(1 uint8)` (0 or 1)
- **Float64:** `bits(8 LE uint64)` (IEEE 754)
- **Bytes:** `len(4 LE uint32) + byte_data`

**Index array** (after presence RLE), depends on encoding kind:

*Dense (kinds 1, 6):* indexes for all `row_count` rows, including nulls.

*Sparse (kinds 2, 7):* only indexes for present rows, preceded by a count:
```
present_count  uint32 LE
[present_count × index_value]   // width bytes each
```

*RLE (kinds 6, 7):* instead of raw indexes:
```
index_count    uint32 LE         // Total number of index values
rle_len        uint32 LE         // Byte length of RLE data
rle_data       [rle_len]byte     // Index RLE format: version(1) + run_count(4) + runs
```

Index values are `index_width` bytes each (1, 2, or 4). For sparse RLE the decoded array
covers only the present rows (same as sparse dense).

### 9.3 Inline Bytes (kinds 3, 4)

Each value is stored directly without a dictionary.

```
row_count      uint32 LE
presence_rle   [see §9.1]
```

*Dense (kind 3):*
```
[row_count × (len(4 LE uint32) + byte_data)]
```

*Sparse (kind 4):*
```
present_count  uint32 LE
[present_count × (len(4 LE uint32) + byte_data)]   // Only present rows
```

### 9.4 Delta Uint64 (kind 5)

Stores uint64 values as offsets from a base value (minimum). Applied to timestamp and
monotonic counter columns.

```
span_count     uint32 LE
presence_rle   [see §9.1]       // rle_len(4) + rle_data
base_value     uint64 LE        // Minimum value in block (absolute)
width          uint8            // Bytes per offset: 0 (no values), 1, 2, 4, or 8
```

If `width == 0`, no offsets follow. Otherwise:
```
offset_len     uint32 LE
offsets_zstd   [offset_len]byte  // zstd-compressed offset array
```

After decompress, offsets are `width` bytes each for every present row in order.
Reconstructed value = `base_value + offset`.

### 9.5 XOR Bytes (kinds 8, 9)

Stores sequential byte values XOR'd against the previous value. Used for ID columns
(span:id, parent:id, etc.) which share common high bytes.

```
span_count     uint32 LE
presence_rle   [see §9.1]       // rle_len(4) + rle_data
xor_len        uint32 LE
xor_data_zstd  [xor_len]byte    // zstd-compressed XOR payload
```

XOR payload (after decompress): for each present row in order:
```
val_len        uint32 LE
xor_bytes      [val_len]byte    // XOR with previous present value; first value stored raw
```

Decode: `value[i] = xor_bytes[i] XOR value[i-1]` (byte-wise, up to min length; extra
bytes appended as-is).

Sparse variant (kind 9) is identical — sparseness is encoded entirely in the presence bitset.

### 9.6 Prefix Bytes (kinds 10, 11)

Stores byte values as (prefix_index, suffix) pairs. Suitable for URLs and paths with
shared common prefixes.

```
span_count      uint32 LE
presence_rle    [see §9.1]

prefix_dict_len uint32 LE
prefix_dict_zstd [prefix_dict_len]byte   // zstd-compressed prefix dictionary

suffix_data_len uint32 LE
suffix_data_zstd [suffix_data_len]byte   // zstd-compressed suffix section
```

**Prefix dictionary** (after decompress):
```
prefix_count   uint32 LE
[prefix_count × (len(4 LE uint32) + prefix_bytes)]
```

**Suffix section** (after decompress):
```
prefix_index_width  uint8           // 1, 2, or 4 bytes per prefix index
[per present row × SuffixEntry]
```

Each `SuffixEntry`:
```
prefix_idx     [prefix_index_width bytes LE]   // Index into prefix dictionary; 0xFFFFFFFF = no prefix
suffix_len     uint32 LE
suffix_bytes   [suffix_len]byte
```

Reconstructed value: `prefix_dict[prefix_idx] + suffix_bytes`. If `prefix_idx == 0xFFFFFFFF`
(or equivalent for smaller widths), value = `suffix_bytes` only.

Sparse variant (kind 11) has the same wire format; sparseness is in the presence bitset.

### 9.7 Delta Dictionary Bytes (kinds 12, 13)

Stores a bytes dictionary with delta-encoded indexes. Optimized for `trace:id` where spans
are sorted, making adjacent rows reference nearby dictionary entries.

```
index_width    uint8            // Present but unused for delta decoding; always read
dict_len       uint32 LE
dict_data_zstd [dict_len]byte   // zstd-compressed dictionary (same format as §9.2 Bytes dict)
row_count      uint32 LE
presence_rle   [see §9.1]
delta_len      uint32 LE
delta_data_zstd [delta_len]byte  // zstd-compressed delta-encoded index array
```

**Delta array** (after decompress): `int32 LE` per row.

*Dense (kind 12):* one delta per row (including null rows).
*Sparse (kind 13):* one delta per **present** row only.

Decode: accumulate deltas starting from 0. `index[i] = prev + delta[i]`. Result must be in
`[0, dict_size)`.

---

## 10. Bloom Filter

The `column_name_bloom` field in every Block Index Entry is a 256-bit (32-byte) bloom
filter. It encodes which column names appear in that block.

To test for membership: compute `FNV1a(name) % 256` and `MurmurHash3(name) % 256` and
check that both corresponding bits are set.

The writer uses `SetBit(bloom[:], hash % 256)` for two independent hash functions per
column name. Readers use `IsBitSet(bloom[:], hash % 256)` for both; a block is pruned
if either bit is unset.

---

## 11. Column Naming Convention

Column names follow a two-part convention that separates OTLP intrinsics from
user-defined attributes:

| Separator | Meaning | Examples |
|-----------|---------|---------|
| `:` (colon) | OTLP intrinsic field — a fixed, well-known field from the OTLP Span, Resource, or Scope proto. Name is assigned by the format, not the user. | `span:name`, `trace:id`, `trace:state` |
| `.` (dot) | User-defined attribute — a key from the OTLP attributes map, prefixed by its scope. Name comes from the instrumentation library or SDK. | `span.http.method`, `resource.service.name`, `scope.library.name` |

**Rule:** A writer MUST NOT use `:` in any column derived from a user attribute map,
and MUST NOT use `.` in any column derived from a fixed OTLP proto field.

### 11.1 Intrinsic Column Names

The following column names are reserved by the format and populated from fixed OTLP
proto fields. A compliant writer MUST capture every field listed here. Fields that are
absent or zero-valued in the source proto MAY be omitted from the block to save space.

| Column Name              | Type    | Source |
|--------------------------|---------|--------|
| `trace:id`               | Bytes   | Span.TraceId |
| `trace:state`            | String  | Span.TraceState |
| `span:id`                | Bytes   | Span.SpanId |
| `span:parent_id`         | Bytes   | Span.ParentSpanId |
| `span:name`              | String  | Span.Name |
| `span:kind`              | Int64   | Span.Kind |
| `span:start`             | Uint64  | Span.StartTimeUnixNano |
| `span:end`               | Uint64  | Span.EndTimeUnixNano |
| `span:duration`          | Uint64  | Span.EndTimeUnixNano − Span.StartTimeUnixNano |
| `span:status`            | Int64   | Span.Status.Code |
| `span:status_message`    | String  | Span.Status.Message |
| `span:dropped_attrs`     | Uint64  | Span.DroppedAttributesCount |
| `span:dropped_events`    | Uint64  | Span.DroppedEventsCount |
| `span:dropped_links`     | Uint64  | Span.DroppedLinksCount |
| `resource:schema_url`    | String  | ResourceSpans.SchemaUrl |
| `scope:schema_url`       | String  | ScopeSpans.SchemaUrl |

### 11.2 User-Attribute Column Names

User-defined attributes from the OTLP attributes maps are stored with a scope prefix
followed by a dot and the attribute key as-is:

| Column Name Pattern | Source |
|---------------------|--------|
| `resource.{key}`    | ResourceAttributes[key] |
| `span.{key}`        | SpanAttributes[key] |
| `scope.{key}`       | ScopeAttributes[key] |

A writer MUST store every attribute present in the source data. The value type is
inferred from the OTLP `AnyValue` union (String → String, Int → Int64, Double →
Float64, Bool → Bool, Bytes → Bytes).

### 11.3 Implementation-Specific Columns

The following column names are used by specific implementations and are NOT part of
the canonical format. Readers MUST tolerate their presence and MUST NOT require them.

| Column Name   | Type   | Description |
|---------------|--------|-------------|
| `trace.index` | Uint64 | Maps span row → trace index within the block (old blockio writer only) |
