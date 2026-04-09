# Blockpack Binary Format Specification

This document defines the complete on-disk binary format for `.blockpack` files. All
multi-byte integers are **little-endian** unless noted otherwise. All offsets are absolute
byte positions within the file unless noted as relative to a block start.

---

## 0. Complete Format Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                       BLOCKPACK FILE FORMAT  (v11-v12 / footer v3)                  ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║  byte 0                                                                              ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ BLOCK 0                                                                        │ ║
║  │  ┌──────────────────────────────────────────────────────────────────────────┐  │ ║
║  │  │ BLOCK HEADER  (24 bytes)                                                  │  │ ║
║  │  │  magic[4]=0xC011FEA1 · version[1] · reserved[3]                          │  │ ║
║  │  │  span_count[4] · col_count[4] · reserved2[8]                             │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ COLUMN METADATA  (col_count × entry)                                      │  │ ║
║  │  │  name_len[2] · name · type[1]                                             │  │ ║
║  │  │  data_offset[8] · data_len[8] · reserved[16] (stats_offset/len, always 0)│  │ ║
║  │  │  ↑ repeated col_count times, sorted by column name                        │  │ ║
║  │  ├──────────────────────────────────────────────────────────────────────────┤  │ ║
║  │  │ COLUMN DATA  (one encoded blob per column)                                │  │ ║
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
║  │  (V12: entire section is snappy-compressed; decompress before parsing)          │ ║
║  │                                                                                 │ ║
║  │  ── BLOCK INDEX ──────────────────────────────────────────────────────────    │ ║
║  │  block_count[4]                                                                 │ ║
║  │  per block entry:                                                               │ ║
║  │    offset[8] · length[8] · kind[1]  ← kind byte added in v11                  │ ║
║  │    span_count[4] · min_start[8] · max_start[8]                                 │ ║
║  │    min_trace_id[16] · max_trace_id[16]                                          │ ║
║  │    (column_name_bloom[32] removed in 2026-03-07 — see NOTE-BLOOM-REMOVAL)        │ ║
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
║  │  ── COLUMN INDEX (stub, always empty) ────────────────────────────────────    │ ║
║  │  per block: col_count[4] = 0                                                    │ ║
║  │                                                                                 │ ║
║  │  ── TRACE BLOCK INDEX ────────────────────────────────────────────────────    │ ║
║  │  fmt_version[1]=0x02 · trace_count[4]                                           │ ║
║  │  per trace (sorted by trace_id):                                                │ ║
║  │    trace_id[16] · block_count[2] · block_id[2] × N                             │ ║
║  │  (v1 legacy: block_id[2] · span_count[2] · span_indices[N×2] — readable)       │ ║
║  │                                                                                 │ ║
║  │  ── TS INDEX ────────────────────────────────────────────────────────────    │ ║
║  │  magic[4]=0xC011FEED · version[1]=1 · count[4]                                  │ ║
║  │  per block (sorted by min_ts ascending):                                        │ ║
║  │    block_idx[4] · min_ts[8] · max_ts[8]                         (20 bytes ea)   │ ║
║  │                                                                                 │ ║
║  │  ── SKETCH INDEX (column-major; absent if no blocks) ────────────────────    │ ║
║  │  magic[4]=0x534B5443 ("SKTC") · num_blocks[4] · num_columns[4]                 │ ║
║  │  per column (sorted by name):                                                   │ ║
║  │    col_name_len[2] · col_name[N]                                                │ ║
║  │    presence[⌈num_blocks/8⌉ bytes]  (bitset: 1=column present in block)          │ ║
║  │    distinct[num_blocks × 4]  (uint32 per block, 0 if absent — HLL cardinality)  │ ║
║  │    topk_k[1]=20                                                                 │ ║
║  │    per present block: entry_count[1] × { fp[8 LE] · count[2 LE] }              │ ║
║  │    cms_depth[1]=4 · cms_width[2 LE]=64                                          │ ║
║  │    per present block: cms_data[depth × width × 2]  (512 bytes)                  │ ║
║  │    per present block: fuse_len[4 LE] · fuse_data[fuse_len]                      │ ║
║  └────────────────────────────────────────────────────────────────────────────────┘ ║
║                                                                                      ║
║  compact_offset  ◄── from footer  (absent if compact_len = 0)                       ║
║  ┌────────────────────────────────────────────────────────────────────────────────┐ ║
║  │ COMPACT TRACE INDEX                                                             │ ║
║  │  magic[4]=0xC01DC1DE · version[1]=2 · block_count[4]                           │ ║
║  │  bloom_bytes[4] · bloom_data[bloom_bytes]                                       │ ║
║  │  block_table: block_count × { file_offset[8] · file_length[4] }  (12 bytes ea) │ ║
║  │  fmt_version[1]=0x02 · trace_count[4]                                           │ ║
║  │  per trace: trace_id[16] · block_count[2] · block_id[2] × N                   │ ║
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
VersionV12  uint8  = 12          // V12: snappy-compressed metadata section + extended header with signal_type byte at offset 21

SignalTypeTrace uint8  = 0x01        // File header signal type: OTEL trace spans
SignalTypeLog   uint8  = 0x02        // File header signal type: OTEL log records

FooterV3Version uint16 = 3       // Current footer (22 bytes)

ColumnEncodingVersion uint8 = 2  // Per-column encoding header version

CompactIndexVersion  uint8 = 1   // Legacy compact trace index section version (no bloom)
CompactIndexVersion2 uint8 = 2   // Compact trace index version with trace ID bloom filter

// ColumnNameBloomBits and ColumnNameBloomBytes removed 2026-03-07 (see NOTE-BLOOM-REMOVAL).

// Trace ID bloom filter constants (used in compact index v2):
TraceIDBloomK             = 7        // Kirsch-Mitzenmacher hash functions
TraceIDBloomBitsPerTrace  = 10       // Bloom bits per trace ID (~0.8% FP rate with k=7)
TraceIDBloomMinBytes      = 128      // Minimum bloom filter size (bytes)
TraceIDBloomMaxBytes      = 1<<20    // Maximum bloom filter size (bytes, 1 MiB cap)
```

### 1.1 Limits

| Constant             | Value        | Description |
|----------------------|-------------|-------------|
| MaxSpans             | 1,000,000   | Maximum spans per block |
| MaxBlocks            | 65,535      | Maximum blocks per file (uint16 block ID in trace index) |
| MaxColumns           | 10,000      | Maximum columns per block |
| MaxDictionarySize    | 1,000,000   | Maximum dictionary entries per column |
| MaxStringLen         | 10,485,760  | Maximum string/bytes value length (10 MB) |
| MaxBytesLen          | 10,485,760  | Maximum bytes value length (10 MB) |
| MaxBlockSize         | 1,073,741,824 | Maximum uncompressed block size (1 GB) |
| MaxMetadataSize      | 268,435,456 | Maximum metadata section size (256 MiB) |
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
   - Metadata Section (block index + range index + column index stub + trace block index)
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

## 4. File Header (v10–v11: 21 bytes; v12+: 22 bytes)

Located at `header_offset` from the footer.

| Field           | Type       | Bytes | Description |
|-----------------|-----------|-------|-------------|
| magic           | uint32 LE | 4     | Must equal 0xC011FEA1 |
| version         | uint8     | 1     | File version: 10, 11, or 12 (12 = snappy-compressed metadata + extended header with signal_type) |
| metadata_offset | uint64 LE | 8     | Absolute byte offset of the Metadata Section |
| metadata_len    | uint64 LE | 8     | Byte length of the Metadata Section |

### 4.1 File Header v12 Extension

When `version == 12`, the file header is 22 bytes. An additional byte at offset 21 encodes
the signal type:

| Field       | Type  | Bytes | Offset | Description |
|-------------|-------|-------|--------|-------------|
| signal_type | uint8 | 1     | 21     | 0x01 = trace spans, 0x02 = log records |

Readers that do not recognize version 12 MUST reject the file with a version error.
Writers producing log files MUST write version 12 and signal_type = 0x02.
Writers producing trace files with version 12 MUST write signal_type = 0x01.

---

## 5. Metadata Section

Located at `metadata_offset`, length `metadata_len`. Contains:

1. **Block Index** — one entry per block in write order
2. **Range Index** — inverted index: value → block set
3. **Column Index stub** — always empty (col_count=0 per block); retained for format compatibility
4. **Trace Block Index** — trace ID → block + span row indices

### 5.0 V12 Metadata Encoding

For files with file header `version = 12`, the `metadata_len` bytes at `metadata_offset`
are a snappy-compressed blob. Readers MUST decompress the blob with snappy before parsing
the sub-sections below. After decompression, the decompressed byte slice is parsed
identically to V10/V11 metadata.

The `metadata_len` field in the file header stores the **compressed** byte length (the
on-disk size). After decompression, readers MUST validate that the decoded length does not
exceed `MaxMetadataSize` (256 MiB) to prevent decompression-bomb attacks.

Block payloads, the compact trace index, and the footer are NOT compressed; they are
unaffected by V12. Block index entries inside the metadata section use the V11 layout
(with the `kind` byte) even in V12 files, since block encoding is unchanged.

### 5.1 Block Index

**Writer:** always written (`writeBlockIndexSection` in `writer/metadata.go`).
**Reader:** always parsed at open time (`parseBlockIndex` in `reader/parser.go`); stored as `r.blockMetas`.
**Query use:** all fields are actively consumed — `Offset`/`Length` drive block I/O, `SpanCount` bounds row iteration, `MinStart`/`MaxStart` prune by time range, `MinTraceID`/`MaxTraceID` prune by trace ID, `Kind` is reserved for future block types. (`ColumnNameBloom` was removed 2026-03-07 — see NOTE-BLOOM-REMOVAL in NOTES.md.)

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
**Total fixed: 68 bytes.** (`column_name_bloom[32]` was removed 2026-03-07 — old total was 100 bytes.)

#### Block Index Entry (v11 addition)

After `length` and before `span_count`, a single `kind` byte is inserted:

| Field  | Type  | Bytes | Description |
|--------|------|-------|-------------|
| kind   | uint8 | 1     | Block entry kind: 0 = leaf (only value in use) |

**v11 layout:** offset(8) + length(8) + kind(1) + span_count(4) + min_start(8) + max_start(8) + min_trace_id(16) + max_trace_id(16). Total: 69 bytes per entry. (`column_name_bloom(32)` removed 2026-03-07.)

### 5.2 Range Index

**Writer:** always written (`writeRangeIndexSection` in `writer/metadata.go`).
**Reader:** scanned lazily at open time (`scanRangeIndexOffsets` in `reader/parser.go`); only the byte range of each column entry is stored (`r.rangeOffsets`). Full parsing happens on demand per column per query.
**Query use:** actively consumed — the executor calls `GetRangeIndex(colName)` to retrieve block IDs for a given attribute value range, enabling block pruning before any I/O.

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

Writers MUST use `bounds[i]` (the KLL quantile boundary) as the bucket key for all types.
For string/bytes types, the key is truncated to `RangeBucketKeyMaxLen` bytes. Using
`bounds[i]` directly ensures that all blocks assigned to bucket `i` share the same key,
which is required for the reader's binary search to find all blocks in that bucket.
The lower-bound invariant is automatically satisfied because `bounds[i]` is always ≤ the
smallest value mapped to bucket `i` by `findBucket*`.

#### 5.2.3 Bucket Distribution Requirement

Range buckets SHOULD be approximately uniformly populated. With `DefaultRangeBuckets =
1000`, each bucket should contain roughly **0.1% of the total distinct values** (and
transitively, 0.1% of the block IDs that contain those values).

**Why this matters for pruning:** A query predicate on a column eliminates all blocks not in
the matching bucket(s). If one bucket contains 99% of all block IDs, the range index
provides no meaningful pruning for the 99% of queries that land in that bucket.

**How to achieve this:** The KLL sketch used to compute bucket boundaries MUST see a
representative sample of the column's value distribution. The writer feeds per-block min
and max values into the KLL sketch incrementally during block building (see NOTES.md §3
and §17). At Flush time, `applyRangeBuckets` calls `kll.Boundaries` to compute bucket
boundaries, then assigns each block to all overlapping buckets via range-overlap. This
ensures every block's value range is represented and the resulting quantiles divide the
value space approximately uniformly.

### 5.3 Trace Block Index

**Writer:** always written (`writeTraceBlockIndexSection` in `writer/metadata.go`).
**Reader:** always parsed at open time (`parseTraceBlockIndex` in `reader/parser.go`); stored as `r.traceIndex`. Also duplicated in the Compact Trace Index (§6) for readers that avoid parsing the full metadata.
**Query use:** actively consumed — `BlocksForTraceID(traceID)` looks up `r.traceIndex` (or the compact index) to find which blocks contain a given trace. At fetch time (`GetTraceByID`), the already-loaded block is scanned in-memory for rows matching the trace ID. See NOTE-37.

Immediately follows the column index stub.

Two format versions are defined:

#### Version 1 (legacy — with per-block span indices)
```
format_version uint8         // 0x01
trace_count    uint32 LE
[trace_count × TraceEntry]   // Sorted by trace_id (lexicographic)
```

Each `TraceEntry`:
```
trace_id       [16]byte      // 128-bit trace ID
block_count    uint16 LE     // Number of blocks containing this trace
[block_count × TraceBlockEntryV1]
```

Each `TraceBlockEntryV1`:
```
block_id       uint16 LE     // Index into the block array
span_count     uint16 LE     // Number of spans in this block belonging to this trace
span_indices   [span_count × uint16 LE]  // Row indices within the block (discarded on read)
```

#### Version 2 (current — block IDs only)
```
format_version uint8         // 0x02
trace_count    uint32 LE
[trace_count × TraceEntry]   // Sorted by trace_id (lexicographic)
```

Each `TraceEntry`:
```
trace_id       [16]byte      // 128-bit trace ID
block_count    uint16 LE     // Number of blocks containing this trace
block_ids      [block_count × uint16 LE]  // Block indices (no span indices stored)
```

Span indices are not stored. The reader scans the `trace:id` column in the already-loaded
block to find matching rows. This is valid because blocks are always fetched in a single
full I/O (SPEC-007), so the scan is pure CPU work with no additional I/O.
Back-ref: `reader/parser.go:parseTraceBlockIndex`, `api.go:GetTraceByID`

---

## 6. Compact Trace Index Section

Located at `compact_offset`, length `compact_len` (v3 footer only). Provides a
self-contained trace lookup structure independent of the full metadata.

Two versions are defined:

### Version 1 (legacy, no bloom filter)
```
magic          uint32 LE     // Must equal 0xC01DC1DE
version        uint8         // Must equal 1
block_count    uint32 LE
block_table    [block_count × CompactBlockEntry]
format_version uint8         // 0x01 (same as Trace Block Index §5.4)
trace_count    uint32 LE
[trace_count × TraceEntry]   // Same format as §5.4 TraceEntry
```

### Version 2 (current, with trace ID bloom filter)
```
magic          uint32 LE     // Must equal 0xC01DC1DE
version        uint8         // Must equal 2
block_count    uint32 LE
bloom_bytes    uint32 LE     // Byte length of the trace ID bloom filter
bloom_data     [bloom_bytes] // Bloom filter bytes (see §11 for algorithm)
block_table    [block_count × CompactBlockEntry]
format_version uint8         // 0x02 (Trace Block Index §5.3 version 2 — block IDs only)
trace_count    uint32 LE
[trace_count × TraceEntry]   // Same format as §5.3 TraceEntry version 2
```

The bloom filter allows `BlocksForTraceIDCompact` to return nil for absent trace IDs
without performing a hash map lookup; the trace-index map is still parsed and allocated
as part of opening the compact index. Size is computed as `max(TraceIDBloomMinBytes,
min(TraceIDBloomMaxBytes, traceCount × TraceIDBloomBitsPerTrace / 8))` bytes. Readers that
encounter an unknown version must return an error.

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

**Writer:** written as the first 24 bytes of every block (`writer/writer_block.go`).
**Reader:** parsed by `parseBlockHeader` in `reader/block_parser.go`; `span_count` and `col_count` are used on every block read.
**Query use:** `span_count` sets the row iteration bound; `col_count` drives the column metadata loop. `magic` and `version` are validated. `reserved` and `reserved2` are skipped.

| Field          | Type       | Bytes | Offset | Description |
|----------------|-----------|-------|--------|-------------|
| magic          | uint32 LE | 4     | 0      | Must equal 0xC011FEA1 |
| version        | uint8     | 1     | 4      | Block version (10 or 11) |
| reserved       | [3]byte   | 3     | 5      | Must be zero |
| span_count     | uint32 LE | 4     | 8      | Number of spans in this block |
| column_count   | uint32 LE | 4     | 12     | Number of columns in this block |
| reserved2      | [8]byte   | 8     | 16     | Must be zero (formerly trace_count + trace_table_len, removed) |

### 8.2 Column Metadata Array

**Writer:** written after the block header, one entry per column (`writer/writer_block.go`).
**Reader:** parsed by `parseColumnMetadataArray` in `reader/block_parser.go`; `data_offset`, `data_len`, and `col_type` are used. `stats_offset` and `stats_len` are skipped (16 bytes, always 0).
**Query use:** actively consumed — `data_offset`/`data_len` locate each column's encoded payload; `col_type` drives decoding.

Immediately follows the block header. One entry per column, `column_count` entries total.

Each `ColumnMetadataEntry`:
```
name_len     uint16 LE
name         [name_len]byte
col_type     uint8              // ColumnType (see §7)
data_offset  uint64 LE          // Absolute byte offset of column data (within full block payload)
data_len     uint64 LE          // Byte length of column data
reserved     [16]byte           // stats_offset[8] + stats_len[8] — always 0; retained for compat
```

All offsets are relative to the start of the block payload (offset 0 = first byte of
block magic). Column metadata entries are written in lexicographic order by column name.

### 8.3 Column Data Section

**Writer:** written after the Column Metadata Array, one encoded blob per column (`writer/writer_block.go`).
**Reader:** decoded on demand via the encoding-specific decoder selected by `encoding_kind` (`reader/block_parser.go`).
**Query use:** actively consumed — the VM evaluates predicates against decoded column values for every span row that passes block-level pruning.

Immediately follows the Column Metadata Array. One data blob per span-level column (not
trace-level) in the same order as the metadata entries. The absolute offset and length of
each blob are recorded in the corresponding `ColumnMetadataEntry`.

All column data blobs start with the **Column Encoding Header**:

```
encoding_version  uint8    // Must equal 2
encoding_kind     uint8    // See §9
```

---

## 9. Column Encoding Kinds

All encodings begin with the 2-byte Column Encoding Header (§8.3). The encoding_kind byte
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

### 10.1 Column Name Bloom Filter (Removed 2026-03-07)

The `column_name_bloom` field was removed from every Block Index Entry. See
NOTE-BLOOM-REMOVAL in NOTES.md. CMS (Count-Min Sketch) subsumes column presence:
an absent column has CMS count 0, which the planner treats conservatively (block passes).

### 10.2 Trace ID Bloom Filter (Compact Index §6 version 2)

The `bloom_data` field in the Compact Trace Index (version 2) is a variable-length bloom
filter encoding all trace IDs present in the file. It is used by `BlocksForTraceIDCompact`
to eliminate files that definitely do not contain a queried trace ID without allocating or
searching the trace index hash map.

**Size:** `max(TraceIDBloomMinBytes=128, min(TraceIDBloomMaxBytes=1<<20, n×TraceIDBloomBitsPerTrace/8))`
where `n` is the number of distinct trace IDs. With `TraceIDBloomBitsPerTrace=10` and
`TraceIDBloomK=7` hash functions, the expected false-positive rate is ≈0.8%.

**Hash algorithm:** Kirsch-Mitzenmacher double-hashing. Let `h1 = LittleEndian.Uint64(traceID[0:8])`
and `h2 = LittleEndian.Uint64(traceID[8:16]) | 1` (force odd stride). For `i in [0, k)`:
`pos = (h1 + i×h2) mod (m×8)`, then set/test `bloom[pos/8] bit (pos%8)`. Trace IDs are
random 128-bit values and serve as their own entropy source — no additional hashing is needed.

**Vacuous semantics:** A nil or zero-length bloom slice always returns `true` (safe fallback
for version-1 compact indexes and old readers). No false negatives are possible.

Back-ref: `internal/modules/blockio/shared/bloom.go:AddTraceIDToBloom`, `TestTraceIDBloom`

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

**Dual storage (as of 2026-03-26):** The modules writer stores these columns in BOTH the
block column payload (for O(1) row access during result materialization) AND the intrinsic
TOC section (for fast range scans, bloom pruning, and zero-block-read query paths). See
writer NOTE-002 for rationale. PR #172 temporarily used exclusive-intrinsic storage but was
rolled back due to O(8.6B) reverse-lookup cost at scale.

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

### 11.4 Log Signal Intrinsic Column Names

When a blockpack file has signal_type = 0x02 (log), blocks contain the following
intrinsic columns instead of the span intrinsics (§11.1):

| Column Name                   | Type    | Source                          | Notes |
|-------------------------------|---------|---------------------------------|-------|
| `log:timestamp`               | Uint64  | LogRecord.TimeUnixNano          | Always present |
| `log:observed_timestamp`      | Uint64  | LogRecord.ObservedTimeUnixNano  | Always present |
| `log:body`                    | String  | LogRecord.Body (AnyValue→Str)   | Always present |
| `log:severity_number`         | Int64   | LogRecord.SeverityNumber        | Null when zero |
| `log:severity_text`           | String  | LogRecord.SeverityText          | Null when empty |
| `log:trace_id`                | Bytes   | LogRecord.TraceId               | Null when absent |
| `log:span_id`                 | Bytes   | LogRecord.SpanId                | Null when absent |
| `log:flags`                   | Uint64  | LogRecord.Flags                 | Null when zero |

User-defined attribute column names for log files use these prefixes:

| Column Name Pattern | Source |
|---------------------|--------|
| `resource.{key}`    | ResourceLogs.Resource.Attributes[key] |
| `scope.{key}`       | ScopeLogs.Scope.Attributes[key] |
| `log.{key}`         | LogRecord.Attributes[key] |

Sort key for log files: (resource.service.name ASC, MinHashSig ASC, log:timestamp ASC).
Log files have no trace block index; the trace block index section is written with
trace_count = 0 (valid per §5.3 format). MinStart/MaxStart in BlockMeta carry the
min/max log:timestamp for block-level time range pruning.

### 11.5 Auto-Parsed Log Body Columns

When a log record body is a JSON object or a logfmt string, the writer automatically
parses all top-level key-value pairs and stores them as additional sparse range string
columns using the `log.{key}` naming convention.

**Detection heuristic:** If the trimmed body starts with `{`, attempt JSON unmarshal.
Otherwise, attempt logfmt decode.

**Extraction rules:**
- JSON: all top-level keys are extracted. String values stored as-is. Non-string values
  (numbers, booleans, nested objects) are stored as `fmt.Sprint(value)`.
- logfmt: all `key=value` pairs are extracted. Keys and values stored as-is. A plain
  text body with no `=` pairs produces nil (not treated as structured logfmt).
- Nested JSON objects are NOT recursed — only top-level fields are stored.

**Failure semantics:** If the body cannot be parsed (or is empty), no extra columns are
added and no error is reported. The `log:body` intrinsic column is always stored
regardless (NOTE 37, NOTE-007).

**No field cap:** All extracted fields become range string columns. There is no limit on
the number of fields extracted per record.

**Column type:** All auto-parsed body fields are stored as `ColumnTypeRangeString` to
enable min/max block-level pruning.

**Interaction with `log.{key}` attribute columns:** Both OTLP LogRecord.Attributes and
auto-parsed body fields use the `log.{key}` prefix. Body-parsed fields are processed
first in `addLogRecordFromProto` and are stored as `ColumnTypeRangeString` columns.
OTLP LogRecord.Attributes are processed after and are stored as `ColumnTypeString`
columns. Because the `ColumnKey` type includes the column type, a shared key name
(e.g., `log.level`) results in TWO separate columns — one `ColumnTypeRangeString` (body)
and one `ColumnTypeString` (attribute) — rather than a collision or skip. No deduplication
occurs at ingest time. Block-level pruning operates on the `ColumnTypeRangeString`
(body-parsed) column.

Back-ref: `internal/modules/blockio/writer/writer_log_body.go:parseLogBody`,
          `internal/modules/blockio/writer/writer_log.go:addLogRecordFromProto`

## 12. Per-File Timestamp Index (TS Index)

The TS index is an optional section appended to the decompressed metadata after the trace
block index. It is written by all new files (2026-03-02 onward). Old files that lack this
section are handled gracefully by the reader.

### 12.1 Wire Format (little-endian)

```
magic[4]    = 0xC011FEED
version[1]  = 1
count[4]    = number of entries (one per block)
per entry (20 bytes):
  min_ts[8]   = BlockMeta.MinStart (Unix nanoseconds)
  max_ts[8]   = BlockMeta.MaxStart (Unix nanoseconds)
  block_id[4] = index into the block index array (0-based)
```

### 12.2 Ordering

Entries are sorted by min_ts ascending. This enables O(log N) binary search for blocks
overlapping a query time window.

### 12.3 Overlap Semantics

A block overlaps query window [queryMin, queryMax] when:
  block.max_ts >= queryMin  AND  block.min_ts <= queryMax

Blocks with min_ts == max_ts == 0 (unknown time; trace files) are always included.

### 12.4 Backward Compatibility

Old readers that do not know about this section will ignore trailing bytes in the metadata
section (the metadata parser is pos-based and does not assert pos == len(data) at end).
New readers that open old files receive (nil, 0, nil) from parseTSIndex and degrade
gracefully to metadata-scan mode in BlocksInTimeRange.

## SPEC-POOL-1
**SpanFieldsAdapter pool ownership contract** — `NewSpanFieldsAdapter` and `ReleaseSpanFieldsAdapter`
implement a `sync.Pool`-backed adapter lifecycle for `modulesSpanFieldsAdapter`. The following
rules govern correct use:

1. **Acquire:** `NewSpanFieldsAdapter(block, rowIdx)` returns a pooled adapter bound to the given
   block and row. The caller owns the adapter from this point until `ReleaseSpanFieldsAdapter` is
   called.

2. **Release timing:** `ReleaseSpanFieldsAdapter` must be called after the adapter's last use —
   after `Clone()` returns (which materializes all fields out of the adapter) or after the span
   callback returns without cloning. It must NOT be called while the adapter's fields are still
   being read (e.g., not inside the iteration callback, not before `Clone()` returns).

3. **Post-Clone safety:** Once `SpanMatch.Clone()` has been called, the returned `SpanMatch`
   holds a `materializedSpanFields` (not the adapter), so releasing the adapter is safe.

4. **Nil / non-adapter safety:** `ReleaseSpanFieldsAdapter` is a no-op if the argument is nil or
   does not implement `*modulesSpanFieldsAdapter` — safe to call on any `SpanFieldsProvider`.

5. **Block pointer release:** `putSpanFieldsAdapter` zeroes `a.block` and `a.rowIdx` before
   returning the adapter to the pool, preventing the pool from retaining live Block references.

Back-ref: `internal/modules/blockio/span_fields.go:NewSpanFieldsAdapter`,
          `internal/modules/blockio/span_fields.go:ReleaseSpanFieldsAdapter`,
          `api.go:streamFilterProgram`, `api.go:streamLogProgram`,
          `reader.go:GetTraceByID`

---

## SPEC-FBLM-1
**FileBloom section** — optional trailing section in the metadata blob, written after the sketch index.
Stores a BinaryFuse8 filter for `resource.service.name` values, enabling O(1) file-level rejection
for service-name equality predicates without opening any block.

Wire format:
```
magic[4 LE]     = 0x46424C4D ("FBLM")
version[1]      = 0x01
col_count[4 LE]
per column (col_count entries, sorted by name):
  name_len[2 LE] + name[name_len bytes]
  fuse_len[4 LE] + fuse_data[fuse_len bytes]
```

Properties:
- `fuse_len = 0` means no values were observed (no data bytes follow); reader treats as `true` (conservative).
- `col_count` is bounded to ≤ 1000 and validated against remaining bytes on parse.
- Old readers that do not know about the FileBloom section will stop parsing after the sketch index without corruption (graceful degradation).
- BinaryFuse8 guarantees no false negatives (SPEC-SK-12); false positive rate ~0.39% (SPEC-SK-13).

Back-ref: `internal/modules/blockio/writer/file_bloom.go:writeFileBloomSection`,
          `internal/modules/blockio/reader/file_bloom.go:parseFileBloomSection`,
          `internal/modules/executor/plan_blocks.go:fileLevelBloomReject`
