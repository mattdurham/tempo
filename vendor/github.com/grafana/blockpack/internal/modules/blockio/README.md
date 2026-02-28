# Blockpack blockio — Writer & Reader

The `blockio` packages implement the complete read and write path for the blockpack columnar
storage format. This document provides a human-readable overview of what the packages do,
how they fit together, and how to extend them.

For machine-readable format details see **SPECS.md**. For design rationale and invariants
see **NOTES.md**. For test coverage requirements see **TESTS.md**.

---

## What is a blockpack file?

A blockpack file stores OpenTelemetry trace spans in a columnar binary format designed for
fast querying on object storage (S3, GCS, Azure Blob). Instead of storing each span as a
row, values for each column (e.g., `span:name`, `http.method`, `span:duration`) are stored
together. This enables:

- **High compression:** similar values packed together compress 10–50× better than rows.
- **Fast predicate pushdown:** query engines scan only the columns they need.
- **Block pruning:** multi-level indexes allow skipping entire blocks that cannot match a query.

---

## File Layout

```
┌─────────────────────────────────────┐
│  Block 0                            │  ← columnar span data, compressed
│  Block 1                            │
│  Block 2                            │
│   ...                               │
│  Block N-1                          │
├─────────────────────────────────────┤
│  File Header (21 bytes)             │  ← magic, version, metadata pointer
├─────────────────────────────────────┤
│  Metadata Section                   │  ← block index, dedicated index,
│                                     │    column index, trace index
├─────────────────────────────────────┤
│  Compact Trace Index (optional)     │  ← fast trace-by-ID lookup
├─────────────────────────────────────┤
│  Footer (10 or 22 bytes)            │  ← pointer back to header
└─────────────────────────────────────┘
```

The reader starts by reading the footer at the end of the file, which points to the header,
which points to the metadata. Blocks are scattered in the file and referenced by offset from
the metadata.

---

## Write Path

```
1. AddTracesData / AddSpan
   └── Buffer span with resource/scope metadata

2. (repeat until Flush)

3. Flush
   ├── ComputeSortKey (service.name + MinHash signature + TraceID)
   ├── Sort span buffer
   ├── For each batch of MaxBlockSpans spans:
   │   ├── Build columnBuilder for each attribute
   │   ├── Encode columns (choose encoding based on type and data characteristics)
   │   ├── Write block payload to output stream
   │   └── Record block metadata (bloom, timestamps, dedicated values)
   ├── Build dedicated column index from log
   ├── Compute range bucket boundaries (KLL sketches → bucket edges)
   ├── Write metadata section (block index + dedicated index + column index + trace index)
   ├── Write file header
   ├── Write compact trace index
   └── Write footer
```

### Configuration

```go
cfg := WriterConfig{
    OutputStream:  outputWriter, // required
    MaxBlockSpans: 2000,         // optional; default 2000, max 65535
}
w, err := NewWriterWithConfig(cfg)
```

### Thread Safety

The Writer is **not thread-safe**. All calls must come from one goroutine. Concurrent
calls will panic.

---

## Read Path

```
1. NewReaderFromProvider(provider, opts...)
   ├── readFooter (last 22 bytes)
   ├── readHeader (21 bytes at footer.headerOffset)
   └── parseV5MetadataLazy
       ├── Parse block index entries
       ├── Record dedicated index offsets (lazy — not parsed yet)
       ├── Parse column index (per-block per-column byte offsets)
       └── Parse trace block index

2. CoalesceBlocks(blockOrder, maxReadSize)
   └── Merge adjacent blocks into fewer, larger I/O requests

3. GetBlockWithBytes(blockIdx, wantColumns, ...)
   ├── readRange (ONE I/O call — reads entire block)
   └── parseBlockColumnsReuse (in-memory column filtering and decoding)

4. Block.GetColumn(name)
   └── Return typed Column for query evaluation
```

### Provider Interface and Wrappers

`ReaderProvider` is the single interface all storage backends implement:

```go
type ReaderProvider interface {
    Size() (int64, error)
    ReadAt(p []byte, off int64, dataType DataType) (int, error)
}
```

`dataType` is a hint (`"footer"`, `"metadata"`, `"column"`, `"index"`, …) that lets
caching layers tune their behaviour per read type.

Each wrapper embeds an `underlying ReaderProvider` and delegates to it, adding one
capability. Wrappers are composed by nesting, not via options:

```
┌─────────────────────────────────────────────────────────┐
│  DefaultProvider                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  cache: RangeCachingProvider                       │  │
│  │    sub-range capable in-memory cache;              │  │
│  │    if requested range is inside any cached range,  │  │
│  │    served without I/O                              │  │
│  │  ┌─────────────────────────────────────────────┐  │  │
│  │  │  underlying: TrackingReaderProvider          │  │  │
│  │  │    counts ReadAt calls and bytes that        │  │  │
│  │  │    reach actual storage (cache misses only)  │  │  │
│  │  │  ┌───────────────────────────────────────┐  │  │  │
│  │  │  │  underlying: <your storage provider>  │  │  │  │
│  │  │  │  (filesystem, S3, GCS, memory, …)     │  │  │  │
│  │  │  └───────────────────────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
│  tracker: *TrackingReaderProvider  ← held separately     │
│           for stats access (BytesRead, IOOperations)     │
└─────────────────────────────────────────────────────────┘
```

`DefaultProvider` is the standard ready-made composition:

```go
provider := NewDefaultProvider(myStorageProvider)
r, err   := NewReaderFromProvider(provider)
```

To add artificial latency (S3 simulation in tests):

```go
provider := NewDefaultProviderWithLatency(myStorageProvider, 15*time.Millisecond)
```

Custom compositions are built by wrapping concrete types directly:

```go
tracker, _ := NewTrackingReaderProvider(myStorageProvider)
cache, _   := NewRangeCachingProvider(tracker)
// cache is the outer provider; tracker is held separately for stats
```

---

## Column Types

| Type | Description | Typical Encoding |
|------|-------------|-----------------|
| String | UTF-8 text; UUIDs stored as Bytes automatically | Dictionary |
| Int64 | Signed 64-bit integer | Dictionary |
| Uint64 | Unsigned 64-bit integer; used for timestamps | Delta (timestamps), Dictionary |
| Float64 | IEEE 754 double | Dictionary |
| Bool | True/false (stored as uint8 0/1) | Dictionary |
| Bytes | Raw byte slice | XOR (IDs), Prefix (URLs), Dictionary |

Range-bucketed variants (RangeInt64, RangeUint64, RangeDuration, RangeFloat64,
RangeString, RangeBytes) use KLL-computed bucket boundaries in the dedicated index for
block-level pruning of high-cardinality numeric/string columns.

---

## Encoding Selection

The writer picks the best encoding for each column automatically:

```
Column type = Bytes?
├── isIDColumn (span:id, *.id, *_id, span:parent_id, ...)
│   └── XOR encoding (nearby IDs XOR well)
│       Exception: trace:id → DeltaDictionary (sorted)
├── isURLColumn (*.url, *.uri, *.path, http.target, ...)
│   └── Prefix encoding (shared URL prefix stripped)
├── isArrayColumn (event.*, link.*, instrumentation.*)
│   └── Dictionary encoding (array blob is opaque)
└── otherwise → Dictionary encoding

Column type = Uint64?
├── shouldUseDeltaEncoding? (range ≤ 65535, or high cardinality + small range)
│   └── Delta uint64 (store as base + offset, zstd-compressed)
└── otherwise → Dictionary encoding

Any type, null ratio > 50%?
└── Use Sparse variant of chosen encoding (only store present values)

Any type, cardinality very low?
└── Use RLE variant (run-length encode the index array)
```

---

## Block Pruning Hierarchy

Queries can skip blocks at multiple levels before decoding any column data:

1. **Timestamp range:** `block.MinStart` / `block.MaxStart` vs query time range.
2. **Trace ID range:** `block.MinTraceID` / `block.MaxTraceID` for trace lookup.
3. **Column bloom filter:** 256-bit bloom on column names; skip blocks that cannot have the queried column.
4. **Dedicated column index:** exact or range lookup by attribute value; returns the set of blocks that contain the value.
5. **Column min/max stats:** `col.Stats.IntMin/IntMax` etc. for numeric range queries.
6. **Value stats:** per-attribute value statistics in the block index entry for additional pruning.

---

## Intrinsic Column Names

These are populated automatically from OTLP fields and are always available (if the span
has the corresponding field):

| Column | Source | Type |
|--------|--------|------|
| `span:id` | `Span.SpanId` | Bytes |
| `span:name` | `Span.Name` | String |
| `span:kind` | `Span.Kind` | Int64 |
| `span:parent_id` | `Span.ParentSpanId` | Bytes |
| `span:start_time` | `Span.StartTimeUnixNano` | Uint64 |
| `span:end_time` | `Span.EndTimeUnixNano` | Uint64 |
| `span:duration` | end − start | Uint64 |
| `span:status_code` | `Span.Status.Code` | Int64 |
| `span:status_message` | `Span.Status.Message` | String |
| `trace:id` | `Span.TraceId` | Bytes |
| `trace:state` | `Span.TraceState` | String |
| `trace.index` | Internal | Uint64 |
| `service.name` | `resource.service.name` | String |
| `resource.{key}` | `ResourceAttributes` | (varies) |
| `span.{key}` | `SpanAttributes` | (varies) |

---

## Extending the Format

### New column type

1. Add constant to `shared/types.go`.
2. Add any required aliases/usages in the writer and reader packages.
3. Add `set*` method on Writer (`writer_span.go`).
4. Add `columnBuilder` case in `writer/column_builder.go` (`newColumnBuilder` + `buildData`).
5. Add matching decoder in `reader/column.go` (`decodeDictionaryColumn` switch).
6. Add stats encode/decode in `writer/column_stats.go` and `reader/parser.go`.

### New encoding kind

1. Add constant to `writer/constants.go` and `reader/constants.go`.
2. Implement encoder in `writer/column_builder_*.go`.
3. Implement decoder in `reader/column.go` (`readColumnEncoding` switch + decode function).
4. Update SPECS.md §9 with the new wire format.

### New dedicated index type

1. Add case in `writer/writer_helpers.go` `recordDedicatedValue`.
2. Add case in `writer/writer_metadata.go` `writeDedicatedValue`.
3. Add case in `reader/dedicated.go` `readDedicatedValue`.
4. Add case in `reader/metadata.go` `scanDedicatedIndexOffsets`.

### New reader option

1. Add function in `reader/builder.go` returning `ReaderOption`.
2. Wire into `NewReaderFromProvider`.

---

## Building and Testing

```bash
# Run all writer tests
go test ./internal/blockio/writer/...

# Run all reader tests
go test ./internal/blockio/reader/...

# Run round-trip tests (in parent blockio package)
go test ./internal/blockio/...

# Run with race detector (required before committing)
go test -race ./internal/blockio/...

# Run precommit checks (required before any commit)
make precommit
```

---

## Key Invariants (Never Break)

- **Never add per-column I/O.** All block data must be read in a single `readRange` call.
- **Dictionary slices must use heap allocation**, never arena. They outlive the arena.
- **Writer is not thread-safe.** Concurrent access will panic by design.
- **MaxBlockSpans must not exceed 65535.** Span indices are uint16 in the trace block index.
- **Footer detection reads the last 22 bytes.** Both v2 and v3 footers must be detectable
  from this fixed window.
- **Lazy dedicated index.** Never parse dedicated indexes eagerly during metadata load.
