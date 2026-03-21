# Intrinsic Columns

Intrinsic columns are a small set of core span attributes stored at the **file level** — once, sorted, deduplicated across all spans in the file — rather than per block. Because they are read in a single I/O operation, intrinsic columns enable tag discovery, service-name bloom filters, and field iteration without touching any block data.

## Source

| File | Purpose |
|---|---|
| [`internal/modules/blockio/writer/intrinsic_accum.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/intrinsic_accum.go) | Accumulation at write time |
| [`internal/modules/blockio/reader/reader.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/reader/reader.go) | Reading intrinsics at query time |
| [`internal/modules/blockio/shared/SPECS.md`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/shared/SPECS.md) | Specs for intrinsic column contract |

---

## What Are Intrinsic Columns

Most span attributes in blockpack are stored per-block: each block is self-contained with its own column encodings. Intrinsic columns break this pattern. They are accumulated during block construction via the `intrinsicAccumulator`, then written as a contiguous section (with a TOC blob) in the file footer. A single read loads all intrinsic data for the entire file.

The 8 intrinsic columns are:

| Column | Type | Description |
|--------|------|-------------|
| `trace:id` | bytes (flat) | 128-bit trace identifier for each span |
| `span:id` | bytes (flat) | 64-bit span identifier |
| `span:parent_id` | bytes (flat) | 64-bit parent span identifier |
| `span:name` | string (dict) | Operation name |
| `span:kind` | int64 (dict) | Span kind enum (CLIENT, SERVER, PRODUCER, etc.) |
| `span:start` | uint64 (flat) | Start timestamp, nanoseconds since epoch |
| `span:duration` | uint64 (flat) | Duration in nanoseconds |
| `resource.service.name` | string (dict) | Service name from resource attributes |

Additional conditional intrinsic columns include `span:status`, `span:status_message`, and `log:timestamp` (for log-signal files).

**Note:** `span:end` is deliberately excluded from the intrinsic section — it is synthesized on read from `span:start + span:duration`, saving ~6% of file size.

---

## Why File-Level

These attributes are accessed on almost every query:
- `span:start` / `span:duration` — nearly every TraceQL predicate and all time-range filters
- `resource.service.name` — the most common filter in practice; also used to build the file-level bloom filter (FBLM)
- `span:name`, `span:kind` — frequently queried intrinsic attributes
- `trace:id`, `span:id` — required for trace reconstruction and span-by-ID fetch

Storing these per-block would mean one I/O per block just to read basic metadata — at ~50 blocks per file and 50ms per object-storage request, that is 2.5 seconds of serial I/O before any span data is evaluated. By co-locating them in a single section, one I/O reads all intrinsics for the entire file.

Additionally, intrinsic columns enable:
- **Tag discovery** (`SearchTags`, `SearchTagValues`): scan `resource.service.name` or `span:name` distinct values without reading any block
- **File-level bloom filter (FBLM)**: the `resource.service.name` intrinsic data provides the input keys for the file-level BinaryFuse8 filter
- **Time-range filtering**: `span:start` values guide fast block selection before block data is read

---

## Encoding

Intrinsic columns use two encoding strategies:

**Flat columns** (`trace:id`, `span:id`, `span:parent_id`, `span:start`, `span:duration`): values and `BlockRef` pointers are stored in parallel arrays, sorted by value at flush time. A `BlockRef` is a `(BlockIdx uint16, RowIdx uint16)` pair pointing back to the block and row where this value lives.

**Dict columns** (`span:name`, `span:kind`, `resource.service.name`): values are deduplicated into a dictionary. Each unique string (or int64) maps to a list of `BlockRef` pointers. Repeated values cost O(1) additional storage (only a new `BlockRef` is appended, not the value bytes again).

For large files (>10 000 rows in any column, controlled by `MaxIntrinsicRows`), columns switch from a monolithic v1 format to a **paged v2 format** that splits data into independently-readable pages. If any column exceeds `MaxIntrinsicRows` total rows, the entire intrinsic section is written as an empty TOC to avoid unbounded growth.

Both flat and dict formats are snappy-compressed at the blob level.

---

## How Used at Query Time

The reader loads the intrinsic section once via `writeIntrinsicSection`-compatible decoding:

- **`IterateFields`**: enumerates all distinct `resource.service.name`, `span:name`, etc. values for tag/value discovery, without reading block data.
- **`GetField(name, value)`**: looks up a specific field value in the intrinsic dict, returning all `BlockRef` pointers that contain it — a direct index from value to blocks.
- **Service name bloom input**: the FBLM writer reads all distinct `resource.service.name` values from the intrinsic section during file construction to build the file-level `BinaryFuse8`.
- **Time-range pre-filter**: `span:start` flat column provides sorted timestamps for identifying which blocks overlap the query's time range before any block I/O.

---

## Pros and Cons

| Aspect | Detail |
|--------|--------|
| **Zero per-block I/O** | All core attributes accessible in one read regardless of file size |
| **Fast tag discovery** | `SearchTags`/`SearchTagValues` touch only the intrinsic section |
| **Enables file-level bloom** | `resource.service.name` intrinsics are the input for FBLM construction |
| **Deduplication** | Dict columns: repeated service names or span names cost only a `BlockRef` (4 bytes), not the string again |
| **Buffering required** | All intrinsic values must be accumulated in memory until `Flush()` — cannot be streamed per-block |
| **Growth cap** | Files with >10 000 total rows in any intrinsic column write an empty TOC to avoid unbounded memory |
| **8 fixed columns** | Adding a new intrinsic column requires a format version bump and changes to both writer and reader |

---

## Size

Intrinsic column size is proportional to total rows (not distinct values for flat columns, distinct values for dict columns):

- **Flat columns** (`trace:id`, `span:id`, `span:start`, `span:duration`): `rows × (value_size + 4 bytes BlockRef)`. For a 10 000-span file: `trace:id` ≈ 10 000 × (16 + 4) = 200 KB; `span:start` ≈ 10 000 × (8 + 4) = 120 KB.
- **Dict columns** (`span:name`, `resource.service.name`): `distinct_values × string_size + rows × 4 bytes`. For 100 distinct service names and 10 000 rows: dict ≈ 100 × 20 bytes + 10 000 × 4 = 42 KB.

All blobs are snappy-compressed. Typical intrinsic section for a 50-block, 100 000-span file: **1–5 MB uncompressed, 200 KB–1 MB compressed**.

---

## Related Files

- Writer accumulation: [`internal/modules/blockio/writer/intrinsic_accum.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/intrinsic_accum.go)
- Writer integration: [`internal/modules/blockio/writer/writer.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/writer.go) (`writeIntrinsicSection`)
- Block builder feeds: [`internal/modules/blockio/writer/writer_block.go`](https://github.com/grafana/blockpack/blob/main/internal/modules/blockio/writer/writer_block.go) (`feedIntrinsicUint64`, `feedIntrinsicString`, etc.)

---

Back to [Write-Path](Write-Path.md)
