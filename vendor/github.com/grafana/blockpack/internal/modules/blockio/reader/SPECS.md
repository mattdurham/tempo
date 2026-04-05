# Reader Module — Specifications

## SPEC-001: Column.IsPresent() — Always Available After ParseBlockFromBytes
*Added: 2026-03-05*

`Column.IsPresent(idx int) bool` returns the correct presence value for any column
returned by `ParseBlockFromBytes`, regardless of whether the column was eagerly decoded
(in `wantColumns`) or lazily registered (not in `wantColumns`).

Lazy registration decodes the presence bitset immediately via `decodePresenceOnly`.
`IsPresent()` MUST NOT trigger a full decode (`decodeNow()`).

Back-ref: `internal/modules/blockio/reader/block.go:IsPresent`

---

## SPEC-002: Column Value Accessors — May Trigger Lazy Decode
*Added: 2026-03-05*

`StringValue`, `Int64Value`, `Uint64Value`, `Float64Value`, `BoolValue`, `BytesValue`
check `rawEncoding != nil` on entry. If set, they call `decodeNow()` to perform the full
decode before accessing typed fields.

**Contract:** After the first call to any value accessor on a lazily-registered column,
subsequent calls return values from the fully-decoded column without re-decoding.

**Error behavior:** If `decodeNow()` encounters a decode error, `rawEncoding` is cleared
and all value accessors return zero/false for all rows (column treated as absent).

Back-ref: `internal/modules/blockio/reader/block.go:StringValue`,
`internal/modules/blockio/reader/column.go:decodeNow`

---

## SPEC-003: ParseBlockFromBytes — All Columns Registered After Return
*Added: 2026-03-05*

After `ParseBlockFromBytes(raw, wantColumns, meta)` returns:
- Columns in `wantColumns` are **eagerly decoded** (presence + values fully available).
- All other columns with `dataLen > 0` are **lazily registered** (presence available,
  values deferred until first accessor call).
- Columns with `dataLen == 0` (trace-level columns) are absent.
- When `wantColumns == nil`, all columns are eagerly decoded (original behavior unchanged).

**NOTE-001:** Lazy registration replaces the old requirement for a second pass via
`AddColumnsToBlock` to access non-predicate columns.

Back-ref: `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/reader.go:ParseBlockFromBytes`

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
available.

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
