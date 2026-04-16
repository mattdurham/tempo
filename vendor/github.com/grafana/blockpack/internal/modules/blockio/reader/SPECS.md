# Reader Module — Specifications

## SPEC-001: Column.IsPresent() — Always Available After ParseBlockFromBytes
*Added: 2026-03-05*
*Updated: 2026-04-14 — presenceOnce removed; IsPresent now triggers full decode*

`Column.IsPresent(idx int) bool` returns the correct presence value for any column
returned by `ParseBlockFromBytes`, regardless of whether the column was eagerly decoded
(in `wantColumns`) or lazily registered (not in `wantColumns`).

For eagerly-decoded columns, IsPresent() reads the already-populated `Present` slice
directly (decoded.Load() is true, needsDecode() returns false — zero extra work).

For lazily-registered columns, IsPresent() triggers full decode (snappy decompress via
`ensureDecompressed` then `decodeNow` via `decodeOnce`) on the first call, populating
all value slices and the presence bitmap together. Subsequent calls on the same column
skip decode (decoded.Load() is true). Concurrent callers are serialized by decodeOnce.

NOTE-CONC-001: IsPresent() always goes through needsDecode() (atomic load) before
reading c.Present. This establishes the happens-before chain from decodeOnce.Do before
any read of the c.Present pointer.

Back-ref: `internal/modules/blockio/reader/block.go:IsPresent`

---

## SPEC-002: Column Value Accessors — May Trigger Lazy Decode
*Added: 2026-03-05*
*Updated: 2026-04-14 — entry guard changed from rawEncoding!=nil to decoded atomic.Bool*

`StringValue`, `Int64Value`, `Uint64Value`, `Float64Value`, `BoolValue`, `BytesValue`
check `c.needsDecode()` (i.e. `!c.decoded.Load()`) on entry. If true, they call
`decodeNow()` to perform snappy decompression (`ensureDecompressed`) and full column
decode before accessing typed fields. rawEncoding and compressedEncoding MUST NOT be
read outside of their respective Once closures — the `decoded` atomic.Bool is the only
safe cross-goroutine entry guard (NOTE-CONC-001).

**Contract:** After the first call to any value accessor on a lazily-registered column,
subsequent calls return values from the fully-decoded column without re-decoding.
`decoded.Load()` returns true after the first successful or failed decode.

**Error behavior:** If `decodeNow()` encounters a decode or decompression error,
`Present` is set to `[]byte{}` and `decoded.Store(true)` is called, so all value
accessors return zero/false for all rows (column treated as absent).

Back-ref: `internal/modules/blockio/reader/block.go:StringValue`,
`internal/modules/blockio/reader/column.go:decodeNow`,
`internal/modules/blockio/reader/block.go:needsDecode`

---

## SPEC-003: ParseBlockFromBytes — All Columns Registered After Return
*Added: 2026-03-05*

After `ParseBlockFromBytes(raw, wantColumns, meta)` returns:
- Columns in `wantColumns` are **eagerly decoded** (presence + values fully available, decoded==true).
- All other columns with `compressedLen > 0` are **lazily registered** with `compressedEncoding`
  set. Both presence and values are deferred to the first accessor call (SPEC-V14-002).
- Columns with `compressedLen == 0` (trace-level columns) are absent.
- When `wantColumns == nil`, all columns are eagerly decoded (original behavior unchanged).

**NOTE-001:** Lazy registration replaces the old requirement for a second pass via
`AddColumnsToBlock` to access non-predicate columns.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/reader.go:ParseBlockFromBytes`

---

## SPEC-V14-002: Deferred Snappy Decompression for V14 Lazy Columns
*Added: 2026-04-14*

For V14 files, each column blob on disk is snappy-compressed. When `wantColumns` is
non-nil and a column is not requested, its compressed bytes are stored in
`Column.compressedEncoding` as a zero-copy sub-slice of the block's rawBytes. No
snappy.Decode call is made at registration time.

Snappy decompression is deferred to the first accessor call via `ensureDecompressed()`
(guarded by `decompressOnce`), which populates `rawEncoding` and clears
`compressedEncoding`. Full column decode then proceeds via `decodeNow()` (guarded by
`decodeOnce`). Together these ensure:

- Zero CPU and memory cost for non-wanted columns that are never accessed.
- At most one decompression per column per block, safe for concurrent callers.
- A TOC bomb guard at registration time: `uncompressedLen > MaxBlockSize` skips
  registration without decompressing (SPEC-ROOT-012).

**Invariant:** After `parseBlockColumnsReuse`, a lazy column has exactly one of:
- `decoded==true` (eagerly decoded), OR
- `compressedEncoding != nil` (registered lazy, not yet decompressed).

After `ensureDecompressed` + `decodeNow` complete, both `compressedEncoding` and
`rawEncoding` are nil; `decoded==true`.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/column.go:ensureDecompressed`,
`internal/modules/blockio/reader/block.go:compressedEncoding`

---

## SPEC-004: Process-Level Cache — File-Level Parsed Objects
*Added: 2026-03-23*

The reader package maintains four process-level caches for file-level immutable
parsed objects. All four use `objectcache.Cache[T]` (bounded LRU, strong `*T` references,
default budget 20% of GOMEMLIMIT):

| Cache variable            | Key format                       | Value type                |
|---------------------------|----------------------------------|---------------------------|
| `parsedMetadataCache`     | `fileID`                         | `*parsedMetadata`         |
| `parsedSketchCache`       | `fileID+"/sketch"`               | `*sketchIndex`            |
| `parsedIntrinsicCache`    | `fileID+"/intrinsic/"+colName`   | `*shared.IntrinsicColumn` |
| `parsedIntrinsicTOCCache` | `fileID+"/intrinsic/toc"`        | `*intrinsicTOC`           |

**Invariants:**
- Cache operations are only performed when `r.fileID != ""` (prevents cross-file
  collisions when no FileID is set).
- `ClearCaches()` calls `.Clear()` on all four instances.
- Entries are evicted only by LRU pressure or `ClearCaches()`. The GC does not
  reclaim cached values; this is intentional to avoid the 50x re-parse regression
  from the previous weak-pointer design (see NOTE-003 addendum 2026-03-29, NOTE-OC-001).
- `metadataBytes` safety: `*Reader` copies the slice pointer from `parsedMetadata`
  at construction, establishing `Reader → metadataBytes` strong ref independently
  of the cache entry. Range index sub-slices remain valid for the reader's lifetime.

Back-ref: `internal/modules/blockio/reader/parser.go:ClearCaches`,
`internal/modules/blockio/reader/parser.go:parseV5MetadataLazy`,
`internal/modules/blockio/reader/intrinsic_reader.go:parseIntrinsicTOC`

---

## SPEC-005: Sub-Block Column I/O — Lazy Column Loading
*Added: 2026-03-24*
*Updated: 2026-03-24 — design revised after implementation revealed coalescing constraint*
*Updated: 2026-04-14 — added production-use restriction for ReadGroupColumnar*

### Motivation

`ReadGroup` currently fetches the full byte range of a coalesced block group from S3.
`parseBlockColumnsReuse` then decodes only `wantColumns`, skipping all others. This saves
CPU and memory at decode time, but the full block bytes are still transferred from S3.

Profiling shows this is the dominant I/O cost at production S3 latency (36ms avg GET):
- A 2000-span internal block is ~4 MB total
- A query like `{span.http.method="GET"}` needs only `span.http.method` + `trace:id` + `span:id` ≈ 600 KB
- 85% of transferred bytes are discarded without being decoded
- For `limit=20` with 56 matching blocks: ~190 MB wasted I/O per query

### Why Naïve Per-Block Per-Column Reads Don't Work

A first implementation attempted: for each block, issue a TOC read (4 KB) then targeted
per-column reads. Testing revealed this **increases** S3 request count and total latency:

- Original: ~18 coalesced range reads for `{status=error}` (adjacent blocks merged into one request)
- Naïve per-column: ~172 reads (1 TOC + N col reads per block, coalescing destroyed)

At 36ms per request: 18 × 36ms = 648ms → 172 × 36ms = 6.2s. **The coalescing benefit
must be preserved.** `ReadGroupColumnar` (in `columnar_read.go`) implements this approach
and is retained for future use once the design below is completed.

### Correct Design: Column-Range Coalescing

The key insight: instead of per-block per-column reads, compute all needed byte ranges
across the ENTIRE coalesced group, then coalesce those column ranges using the same
`CoalesceBlocks` logic used for block-level coalescing.

**Algorithm:**

1. **TOC phase (one coalesced read):** Issue a single read covering the TOC region of all
   blocks in the group. Each block's TOC occupies its first `tocHintBytes` (≤4 KB). Since
   blocks are adjacent in the coalesced range, this is one read of at most
   `N_blocks × tocHintBytes` bytes (e.g. 10 blocks × 4 KB = 40 KB).

2. **Column range collection:** Parse each block's TOC from the appropriate slice of the
   combined TOC read. For each `wantColumns` column in each block, record the absolute
   byte range `(blockAbsoluteOffset + col.dataOffset, col.dataLen)` in the S3 file.

3. **Column-range coalescing:** Apply `CoalesceBlocks`-style merging to the collected
   column ranges with a tight gap threshold (~64 KB). Columns of the same type are often
   stored at similar offsets across adjacent blocks, so many ranges will merge.

4. **Column data reads (1–3 requests):** Issue the merged column-range reads. For typical
   queries touching a few columns across a group of 10 blocks, this results in 1–3 reads
   of 1–5 MB total, compared to 1 read of 30–40 MB for the full group.

5. **Assembly:** For each block, build a sparse buffer (zeros except at wanted column
   offsets) and pass to `parseBlockColumnsReuse` with `wantColumns`.

**Request count comparison at 36ms/request:**

| Approach | Requests (10-block group, 3 cols) | Latency |
|----------|-----------------------------------|---------|
| Full group read (current) | 1 | 36ms |
| Naïve per-block per-col | 10 + 30 = 40 | 1.44s |
| **Column-range coalesced** | **1 (TOC) + 2 (col data) = 3** | **108ms** |

### Invariants

- **SPEC-005a:** When `wantColumns` is nil (all-column decode), the current full-block read
  path is used unchanged. Sub-block reads only apply when an explicit column set is
  requested.
- **SPEC-005b:** Column decode results are byte-for-byte identical to the full-block path.
  The sub-block read is a transparent I/O optimization, not a semantic change.
- **SPEC-005c:** `FetchedBlocks` counts logical block fetches, not physical S3 requests.
- **SPEC-005d:** If any TOC parse fails or a column offset is out of range, fall back to
  the full-group read for that group. Sub-block reads are best-effort.
- **SPEC-005e:** The combined TOC read for a group MUST be a single S3 request (not one
  per block) to preserve the latency benefit of coalescing.

### Expected Impact (after column-range coalescing)

| Query type | Current I/O | Target I/O | S3 reqs (current→target) |
|------------|-------------|------------|--------------------------|
| `{http.method=GET}` | 230 MB | ~15 MB | 55 → ~10 |
| `{svc + http.method}` | 230 MB | ~20 MB | 100 → ~12 |
| `{duration>100ms}` (intrinsic) | 0 | 0 | unchanged |
| `{}` (match-all, nil wantColumns) | full | full | unchanged |

### Current Status — Already Partially Implemented

The column decode filtering (`parseBlockColumnsReuse` with `wantColumns`) is fully
implemented and active. For every block read:
1. `ReadGroup` issues **one S3 range read** per coalesced group — optimal latency (one
   round-trip regardless of how many columns are needed).
2. `parseBlockColumnsReuse(raw, wantColumns, meta)` **decodes only `wantColumns`** from
   the already-fetched bytes — CPU never touches non-needed column data.

This satisfies the "parse only what you need" goal completely. The remaining gap vs parquet
is bandwidth: we transfer the full block (4 MB) even though we only need ~600 KB of column
data. At 36ms S3 latency this is the **correct trade-off** — one 4MB round-trip at 36ms
beats N smaller round-trips at 36ms each. (Measured: per-block per-column reads increased
latency from 236ms to 925ms for `{http.method=GET}`.)

The column-range coalescing algorithm above would close the bandwidth gap without adding
round-trips, but requires either:
  (a) A file-level column directory so all column offsets are available before any block
      reads (format change); or
  (b) S3 multi-range GET to fetch multiple non-contiguous byte ranges in one request.

`ReadGroupColumnar` in `columnar_read.go` is retained for future use when (a) or (b) is
available. **ReadGroupColumnar MUST NOT be used in production query paths.** It is exported
only because `package reader_test` (external test package) references it directly; it is
NOT part of the public API. TODO: Unexport when tests are moved to `package reader`.

Back-ref: `internal/modules/blockio/reader/columnar_read.go:ReadGroupColumnar`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/reader.go:ReadGroup`,
`internal/modules/blockio/reader/coalesce.go:CoalesceBlocks`

---

## SPEC-006: FileLayout() — Byte Invariant and Section Model
*Added: 2026-03-31*

`FileLayout()` returns a `FileLayoutReport` where:

1. **Physical byte invariant:** `sum(s.CompressedSize for s in Sections where !s.IsLogical) == FileSize`.
   Every byte on disk is represented by exactly one physical section.

2. **Logical sections:** Sections with `IsLogical: true` describe internal structure within
   an already-counted physical section (e.g. sub-components of `metadata.compressed`). They
   MUST NOT be counted toward the file size.

3. **V12 metadata model:** A single physical `metadata.compressed` section accounts for
   `r.metadataLen` bytes. Per-component breakdown (range index columns, sketch, file bloom)
   uses `IsLogical: true` sections with offsets relative to the decompressed metadata buffer.

4. **Intrinsic paged columns:** When a column uses the v2 paged format, emit one physical
   section per page (`intrinsic.column[name].page[N]`) plus a `intrinsic.column[name].page_toc`
   section for the TOC header bytes. Pages are physical file bytes; the invariant is preserved
   because pages plus toc replace (not supplement) the aggregate column section.

5. **Sketch section:** `SketchIndexInfo.TotalBytes` is the actual uncompressed sketch
   section size. `ColumnSketchStat.FuseBytes` and `TopKBytes` are actual per-block byte sizes.

6. **Range index:** Every numeric `RangeIndexBucket` (Int64, Uint64, Float64, Duration)
   has `End` populated (upper boundary). String/bytes buckets have empty `End` because
   upper bounds are not encoded in the wire format (empty for string/bytes columns).
   Every `RangeIndexColumn` has `BucketMin` and `BucketMax` from the wire-format global
   bounds for numeric types; string/bytes columns have empty `BucketMin`/`BucketMax`.

7. **FileBloom:** `FileLayoutReport.FileBloom` is nil for files without an FBLM section.
   When non-nil, `TotalBytes` is the raw byte size of the FBLM section.

Back-ref: `internal/modules/blockio/reader/layout.go:FileLayout`

---

## SPEC-007: V5 Footer Parsing and VectorIndex Lazy Load
*Added: 2026-04-02*

### V5 Footer Detection

The `readFooter()` method tries V5 (46 bytes) before V4 (34 bytes). Detection strategy:

1. If `fileSize >= FooterV5Size` (46): read 46 bytes from `fileSize-46`.
   - If `buf[0:2] == FooterV5Version (5)`: parse V5; extract `vectorIndexOffset` and `vectorIndexLen`.
   - If `buf[12:14] == FooterV4Version (4)`: V4 footer is embedded in the V5 buffer at offset 12; parse V4 without a second I/O.
   - Otherwise: fall through to separate V4 read.
2. If `fileSize < FooterV5Size` but `>= FooterV4Size` (34): read 34-byte V4 footer.
3. If `fileSize < FooterV4Size`: read 22-byte V3 footer.

**Invariant:** For V4 files large enough to trigger the V5 read, V4 is parsed from the same
I/O buffer — no extra I/O penalty for V4 files. The `TestLeanReader_ThreeIO` test enforces
the 3-I/O budget.

### VectorIndex Lazy Load

`vectorIndexOffset` and `vectorIndexLen` are parsed from the V5 footer but the section bytes
are NOT read during `readFooter`. Two lazy accessor methods are provided:

```go
// VectorIndexRaw reads the raw section bytes. Returns nil for V3/V4 files.
func (r *Reader) VectorIndexRaw() ([]byte, error)

// VectorIndex returns the parsed VectorIndex. Returns nil, nil for V3/V4 files.
// Thread-safe: guarded by vectorIndexOnce.
func (r *Reader) VectorIndex() (*VectorIndex, error)
```

**Rationale:** The vector index section can be large (codebook ~768KB + PQ codes 96B/vector).
Loading it eagerly for every file open would inflate memory on readers that never perform
semantic queries. Lazy loading ensures non-vector query paths pay zero vector I/O cost.

Back-ref: `internal/modules/blockio/reader/parser.go:readFooter`,
`internal/modules/blockio/reader/reader.go:VectorIndex`,
`internal/modules/blockio/reader/reader.go:VectorIndexRaw`,
`internal/modules/blockio/reader/vector_index.go:parseVectorIndexSection`

---

## SPEC-010: V14 Two-Phase Compact Trace Index Cache
*Added: 2026-04-12*

### Overview

V14 lean readers load the compact trace index in two phases to avoid the ~50 MB per-block
I/O cost of reading the full SectionTraceIndex blob on every `BlocksForTraceID` call.

### Phase 1 — Eager Header Load (~KB)

`ensureV14TraceSection` (called at most once per Reader via `v14TraceOnce`):

1. Probes `cache.GetOrFetch(fileID+"/v14/compact-header", ...)` to obtain the cached
   header bytes.
   - **Cold-miss path:** The `GetOrFetch` closure runs; invokes `readV14Section(SectionTraceIndex)`
     to fetch and decompress the full section blob, passes it to `splitV14CompactSection` to
     locate the header/trace-index split, copies and returns the header bytes, and captures
     `traceIdxBytes` from the split result. No provider I/O occurs for subsequent calls.
   - **Warm-hit path:** The `GetOrFetch` closure does NOT run; `readV14Section` is never
     called. `traceIdxBytes` remains nil after `GetOrFetch` returns.
2. Passes the header bytes to `parseCompactIndexBytesV14Header` to parse the bloom filter
   and block table from the header.
3. The header is cached under `fileID+"/v14/compact-header"` by the `GetOrFetch` call.
4. Calls `parseCompactIndexBytesV14Header(header)` which sets:
   - `r.compactParsed.blockTable` — file offsets for each internal block.
   - `r.compactParsed.traceIDBloom` — the trace ID bloom filter bytes.
   - `r.compactParsed.isV14TraceSection = true` — signals the V14 phase-2 fetch path.
5. Also populates `r.blockMetas` if not already set.
6. On a cold cache miss, also pre-populates `compactParsed.traceIndexRaw` from the same
   read (the trace index sub-slice is already in memory after `splitV14CompactSection`)
   (NOTE-014). On a warm cache hit, probes `r.cache.Get` for the section blob
   (`fileID+"/v14/sec/03/dec"`). If the blob is in-memory, splits it to pre-populate
   `traceIndexRaw` (zero I/O). If the blob was evicted, leaves `traceIndexRaw` nil —
   `ensureTraceIndexRaw` will fetch it on the first bloom hit (NOTE-015).

**Invariant SPEC-010a:** After `ensureV14TraceSection`, `compactParsed` is non-nil and the
bloom filter is available. `BlocksForTraceIDCompact` can evaluate bloom filter checks.
`compactParsed.traceIndexRaw` is pre-populated only when the decompressed section blob is
present in the in-memory cache at phase-1 time; if the blob was evicted, `traceIndexRaw`
remains nil and phase 2 (`ensureTraceIndexRaw`) fetches it on the first bloom hit.
`ensureV14TraceSection` never issues a provider read on the warm-hit path. See NOTE-015.

### Phase 2 — Lazy Trace Index Load (~50 MB, bloom hit only)

`ensureTraceIndexRaw` (called via `traceIndexOnce`):

1. Detects `compactParsed.isV14TraceSection == true`.
2. Fetches the trace index bytes via `cache.GetOrFetch(fileID+"/compact-trace-index", ...)`.
   The fetch function re-invokes `readV14Section(SectionTraceIndex)` (the full decompressed
   blob is cached by `readV14Section` under `fileID+"/v14/sec/03/dec"`), then calls
   `splitV14CompactSection` and returns the trace index sub-slice copied into a fresh buffer.
3. Validates `fmt_version` of the trace index bytes.
4. Assigns the result to `compactParsed.traceIndexRaw`.

**Invariant SPEC-010b:** Phase 2 runs at most once per Reader (guarded by `traceIndexOnce`).
`readV14Section` is idempotent (cached) so the re-read in phase 2 does not incur an extra
network round-trip after the first call.

### Cache Key Mapping

| Cache key                          | Contents                                    | Written by                  |
|------------------------------------|---------------------------------------------|-----------------------------|
| `fileID+"/v14/sec/03/dec"`         | Full decompressed SectionTraceIndex blob    | `readV14Section`            |
| `fileID+"/v14/compact-header"`     | Header sub-slice (bloom + block table only) | `ensureV14TraceSection`     |
| `fileID+"/compact-trace-index"`    | Trace index bytes                           | `ensureTraceIndexRaw`       |
| `fileID+"/block/"+blockIdx`        | Raw block bytes (per-block entry)           | `ReadGroup`                 |

### `splitV14CompactSection` Contract

- Input: decompressed SectionTraceIndex blob.
- Output: `(header, traceIndex)` sub-slices of input — no copy.
- Error: returns error if magic is bad, version is unsupported, data is truncated, or
  the trace index portion is less than 5 bytes.
- Callers that cache the header or trace index must copy them (`append([]byte(nil), s...)`).

Back-ref: `internal/modules/blockio/reader/trace_index.go:splitV14CompactSection`,
`internal/modules/blockio/reader/trace_index.go:parseCompactIndexBytesV14Header`,
`internal/modules/blockio/reader/trace_index.go:ensureTraceIndexRaw`,
`internal/modules/blockio/reader/parser.go:ensureV14TraceSection`,
`internal/modules/blockio/reader/reader.go:compactTraceIndex.isV14TraceSection`

---

## SPEC-011: Raw Block Byte Reads Must Route Through Cache

**Invariant:** `ReadGroup` and `ReadBlocks` must never call `ReadCoalescedBlocks` or
`provider.ReadAt` without first checking `r.cache`. The canonical implementation:

1. For each `blockID` in the group, probe `r.cache.Get(fileID+"/block/"+blockID)`.
2. If all blocks hit: return the cached slices — zero S3 I/O.
3. On any miss: call `ReadCoalescedBlocks` for the full group, store every fetched block via
   `r.cache.Put(fileID+"/block/"+blockID, data)`, then return.

The `r.fileID == ""` guard bypasses the cache (no stable key) — callers that construct a
`Reader` without a fileID explicitly opt out of block-level caching.

**Why fetch the full group on a partial miss (not just the missing blocks)?**
Re-coalescing a partial subset of blocks adds code complexity and risks issuing more S3
requests (smaller, non-coalesced reads). Since the coalesced group was already computed for
optimal I/O, and re-reading a cached block from a pooled buffer costs nanoseconds vs the
~75 ms S3 round-trip, fetching the full group on any miss is the correct trade-off.

**Cache Put errors are discarded:** A Put failure (e.g., eviction, size limit) means the
next read re-fetches from S3 — degraded performance but not incorrect behavior.

Back-ref: `internal/modules/blockio/reader/reader.go:Reader.ReadGroup`,
`internal/modules/blockio/reader/reader.go:Reader.ReadBlocks`
