# File Format

Blockpack is a columnar binary format for OpenTelemetry trace data. A single file contains everything needed to answer a query — block payloads, indexes, and metadata — with no external catalog required.

## File Layout

```
┌──────────────────────────────────────────┐
│  Block 0 payload (columnar span data)    │
├──────────────────────────────────────────┤
│  Block 1 payload                         │
├──────────────────────────────────────────┤
│  ...                                     │
├──────────────────────────────────────────┤
│  Block N payload                         │
├──────────────────────────────────────────┤
│  Metadata blob                           │
│  ├── Block index (offset + size per blk) │
│  ├── Dedicated column indexes            │
│  ├── Trace block index                   │
│  └── File-level bloom filter (FBLM)      │
├──────────────────────────────────────────┤
│  Fixed header (magic, version, offsets)  │
├──────────────────────────────────────────┤
│  Footer (10 bytes)                       │
└──────────────────────────────────────────┘
```

Files are read from the footer backward: the footer gives the header offset, the header gives metadata offsets, and the metadata gives block offsets. This enables efficient random access without scanning the entire file.

## Blocks

Each block contains approximately 2,000 spans stored in columnar layout. Spans are clustered by service name and MinHash similarity before being written so that blocks tend to hold spans from the same service, improving compression ratios and pruning effectiveness.

### Column Encoding

Every span attribute is a separate column. At write time, blockpack selects the best encoding for each column's observed value distribution:

| Encoding | Best for |
|---|---|
| Dictionary | Low-cardinality strings and numerics; payload Zstd-compressed |
| Sparse Dictionary | Dictionary with sparse presence (>50% nulls) |
| Delta Uint64 | Uint64 columns with small range or high cardinality |
| RLE Indexes | Index arrays with cardinality ≤ 3 (boolean-like enums) |
| XOR Bytes | Byte columns (non-URL): XOR against previous row, then Zstd |
| Prefix Bytes | URL/path columns: shared-prefix dictionary encoding |
| Delta Dictionary | `trace:id` column: delta-encoded sorted dictionary |
| + sparse variants | Sparse (>50% null) variants of the above |

After column encoding, each block is compressed with **Zstd**, achieving 3–5× additional size reduction on top of encoding. Combined, blockpack reaches **10–50× compression** over raw OTLP.

## Block Metadata

Each block carries a `BlockMeta` record stored in the metadata blob:

| Field | Purpose |
|---|---|
| `MinNano` / `MaxNano` | Time range — used for time-window pruning |
| `MinTraceID` / `MaxTraceID` | Trace ID range — used for trace lookup pruning |
| Column bloom filters | BinaryFuse8 per column — used to reject blocks that don't contain a queried value |
| Range index entries | Per-column min/max bucket assignments (derived from file-level KLL sketch) — used for range predicate pruning |
| Row count | Number of spans |

## Indexes and Filters

Each of these structures is stored in the metadata blob and consulted during query planning — before any block payload bytes are fetched.

### Block Index

Maps each block's ULID to its byte offset and size in the file. Loaded once on file open. Every block fetch goes through this index to locate the byte range to read.

### Trace Block Index

A sorted array of `(traceID, blockID)` pairs. Enables O(log N) binary search for trace-by-ID lookups without reading any block payloads. Used by `FindTraceByID`.

### Dedicated Column Indexes

For columns marked as high-cardinality at write time, blockpack builds a global inverted index: `value → []blockID`. At query time this allows O(1) block selection for exact-match predicates on those columns — skipping all per-block bloom checks entirely.

### Bloom Filters (BinaryFuse8)

Blockpack uses [BinaryFuse8](https://github.com/FastFilter/xorfilter) filters — a space-efficient probabilistic data structure that answers "does value X exist in this set?" with a ~0.4% false positive rate and ~1 byte per entry.

There are two levels:

**File-level bloom (`FBLM` section):**
Covers `resource.service.name` across all spans in the entire file. Checked first — if the queried service is absent, zero blocks are read and the query returns immediately.

**Block-level bloom:**
One filter per column per block, built over the column's distinct values. Used during the pruning phase to reject blocks that cannot contain a queried value.
False positives are tolerated (they cause unnecessary block reads); false negatives are impossible by construction.

### KLL Range Sketches

At write time, blockpack builds a [KLL sketch](https://arxiv.org/abs/1603.05346) (Karnin-Lang-Liberty) per numeric column across all blocks. The sketch is used to compute ~1,000 quantile bucket boundaries for the file-level range index. The sketch itself is not stored on disk — only the derived bucket boundaries are.

Each block's contribution to the range index is recorded as the set of KLL buckets whose range overlaps that block's `[min, max]` values for that column.

At query time, the planner uses **interval matching**: for a predicate `duration > X`, a block is retained only if its recorded bucket range overlaps `[X, ∞)`. Blocks with no overlapping buckets are pruned without reading any payload bytes.

For OR predicates, the planner keeps any block that satisfies at least one arm. A block need only pass the weaker condition to survive.

### CMS (Count-Min Sketch)

Blockpack maintains a [Count-Min Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) per block for approximate frequency counting of high-cardinality string values. The CMS answers "roughly how many times does value X appear in this block?" in O(1) time and constant space.

Used during query planning to score and rank blocks by estimated match density before issuing I/O. Blocks with a CMS score of zero for all queried values can be skipped; blocks with high scores are prioritised to allow early termination once enough results are found.

## I/O Design

Blockpack targets **object storage** (S3, GCS, Azure Blob) where request latency (50–100ms) dominates cost, not bytes transferred. The core invariant:

> **One I/O per block — always read the entire block.**

Per-column selective reads were tried and removed: they caused 10–120× more API calls with no meaningful reduction in bytes transferred. All column filtering happens in-memory after the single block fetch.

| Metric | Good | Warning | Critical |
|---|---|---|---|
| `io_ops` | < 500 | 500–1000 | > 1000 |
| `bytes/io` | > 100KB | 10–100KB | < 10KB |
