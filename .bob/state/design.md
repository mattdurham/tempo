# Intrinsic Column Optimization: Raw-Byte Scanning + Paged Layout

## Problem

Blockpack intrinsic columns are stored as single monolithic snappy-compressed blobs. `DecodeIntrinsicColumnBlob` materializes ALL entries into Go structs (`IntrinsicColumn` with `[]BlockRef` slices) even when consumers only need a small subset:

- **Dict columns** (`span:status`, `span:kind`, `resource.service.name`): Builds BlockRef structs for ALL unique values, then `intrinsicDictMatchRefs` only uses refs for matching values. For `status=error` on 1M spans with 3 status values: builds 1M BlockRef structs, uses 250K.
- **Flat columns** (`span:start`, `span:duration`): Delta-decodes ALL uint64 values + builds ALL BlockRef structs, then `intrinsicFlatMatchRefs` binary-searches and copies a small range. For `duration > 100ms` on 1M spans with 50K matches: builds 2M structs (values + refs), uses 50K.

CPU profiling shows ~33% in `memmove` from building these Go structs.

## Goal

Two-phase optimization:
1. **Phase 1 — Raw-byte scanning** (no wire format change): Operate on decompressed bytes directly, skip non-matching entries without building Go structs. Quick win, no backward compat concerns.
2. **Phase 2 — Paged layout** (new wire format): Break column blobs into independently-compressed pages of ~10K records with per-page TOC (min/max + bloom). Enables skipping entire pages without even decompressing them.

## Phase 1: Raw-Byte Scanning

### Architecture

Add new functions that scan the decompressed column bytes directly without materializing the full `IntrinsicColumn`. These replace the current `GetIntrinsicColumn()` → `intrinsicDictMatchRefs()` / `intrinsicFlatMatchRefs()` pipeline for predicate evaluation.

### Dict Column Raw Scan

Current: `DecodeIntrinsicColumnBlob` → builds ALL `IntrinsicDictEntry` with ALL `[]BlockRef` → `intrinsicDictMatchRefs` iterates entries, collects matching refs.

New: `ScanDictColumnRefs(blob []byte, matchFn func(value string, int64Val int64) bool, maxRefs int) []BlockRef`

1. Snappy-decompress blob
2. Parse header (format_version, format, col_type, value_count, ref_widths)
3. For each dict entry:
   a. Read value (string or int64)
   b. Call `matchFn(value, int64Val)` — if false, read `ref_count`, advance `pos += ref_count * ref_size` (SKIP refs entirely)
   c. If true: read `ref_count`, decode BlockRefs into result slice
4. Return only matching refs — never builds non-matching BlockRef structs

**Expected savings**: For `status=error` (1 of 3 values): skip ~67% of refs. For `service.name="X"` (1 of 50 services): skip ~98% of refs.

### Flat Column Raw Scan

Current: `DecodeIntrinsicColumnBlob` → delta-decodes ALL uint64 + builds ALL `[]BlockRef` → `intrinsicFlatMatchRefs` binary-searches values, copies matching refs.

New: `ScanFlatColumnRefs(blob []byte, lo, hi uint64, hasLo, hasHi bool, maxRefs int) []BlockRef`

1. Snappy-decompress blob
2. Parse header (format_version, format, col_type, row_count, ref_widths)
3. Delta-decode uint64 values in a streaming pass — no slice allocation, just track running sum
4. Find start index (first value >= lo) and end index (first value > hi) — streaming binary search equivalent
5. Compute refs section offset: `refs_start = header_size + row_count * 8`
6. Seek to `refs_start + start * ref_size`, decode only the matching range of BlockRefs
7. Return only matching refs

**Expected savings**: Values decoded without allocation (streaming). Refs decoded only for matching range. For `duration > 100ms` where 5% match: decode 100% of values (streaming, no alloc) + 5% of refs.

### Flat Column Top-K Scan (for `span:start`)

New: `ScanFlatColumnTopKRefs(blob []byte, limit int, backward bool) []BlockRef`

1. Snappy-decompress blob
2. Parse header
3. For backward (MostRecent): refs are parallel to sorted values — the last `limit` refs correspond to the newest timestamps
4. Compute `refs_start + (row_count - limit) * ref_size`, decode only those refs
5. Return last N refs

**Expected savings**: For limit=20 on 1M entries: decode 20 refs instead of 1M. ~50000x less work.

### Integration Points

- `BlockRefsFromIntrinsicTOC` in `predicates.go`: Replace `r.GetIntrinsicColumn(colName)` + `intrinsicDictMatchRefs`/`intrinsicFlatMatchRefs` with raw scan functions. The raw scan functions take the compressed blob bytes (from cache) directly.
- `collectTopKFromIntrinsicRefs` in `stream.go`: Replace `r.GetIntrinsicColumn(opts.TimestampColumn)` with `ScanFlatColumnTopKRefs` for the timestamp scan.
- Cache: Raw scan functions operate on the compressed blob bytes. The L1/L2 cache already stores compressed bytes — no change needed. The `parsedIntrinsicCache` (decoded struct cache) becomes unused for these code paths.

### Files to Modify

- `shared/intrinsic_codec.go` — Add `ScanDictColumnRefs`, `ScanFlatColumnRefs`, `ScanFlatColumnTopKRefs`
- `reader/intrinsic_reader.go` — Add `GetIntrinsicColumnBlob(name) ([]byte, error)` to expose raw compressed bytes
- `executor/predicates.go` — Update `BlockRefsFromIntrinsicTOC` to use raw scan
- `executor/stream.go` — Update `collectTopKFromIntrinsicRefs` to use raw scan

## Phase 2: Paged Layout

### Architecture

Each intrinsic column on disk changes from one monolithic blob to a page TOC followed by N page blobs. The page TOC contains per-page statistics (min/max, row count, bloom filter for dict columns) enabling page-level pruning. Pages are independently snappy-compressed so only matching pages need decompression. The existing IntrinsicColMeta in the main TOC is unchanged — format version detection (0x01 = monolithic, 0x02 = paged) handles backward compatibility.

## On-Disk Layout (Per Column)

```
[Page TOC blob]  (snappy-compressed, ~1-5KB)
[Page 0 blob]    (snappy-compressed, ~10K records each)
[Page 1 blob]    (snappy-compressed, ~10K records each)
...
[Page N blob]    (snappy-compressed, ~10K records each)
```

The IntrinsicColMeta.Offset points to the start of the Page TOC blob. IntrinsicColMeta.Length covers Page TOC + all page blobs combined.

## Page TOC Wire Format

```
page_toc_version[1]   = 0x01
page_count[4 LE]
block_idx_width[1]    (global across all pages)
row_idx_width[1]      (global across all pages)
format[1]             (IntrinsicFormatFlat=0x01 or IntrinsicFormatDict=0x02)
col_type[1]           (ColumnType byte)

per page:
  offset[4 LE]         (byte offset relative to first page blob start)
  length[4 LE]         (compressed page blob size in bytes)
  row_count[4 LE]      (number of records in this page)
  min_len[2 LE] + min[min_len]   (page-local min value, encoded same as main TOC)
  max_len[2 LE] + max[max_len]   (page-local max value, encoded same as main TOC)
  bloom_len[2 LE] + bloom[bloom_len]  (bloom filter bytes; 0 for flat columns)
```

## Page Blob Wire Format

Pages do NOT repeat the column header (format_version, format, col_type, ref_widths) — those are in the Page TOC. Pages contain only data.

### Flat Page
```
values:  delta-encoded uint64[row_count]  (first value absolute, rest delta from previous)
         OR length-prefixed bytes[row_count]
refs:    BlockRef[row_count]  (variable-width using global block_idx_width/row_idx_width)
```

### Dict Page
Dict columns are paged by **row position** — the overall sorted dict entries are sliced into pages of ~10K total refs. Each page contains the dict entries that have refs in that page's row range.

```
value_count[4 LE]     (number of unique values with refs in this page)
per value:
  value_len[2 LE] + value[value_len]   (string; 0-sentinel + 8-byte LE for int64)
  ref_count[4 LE]
  refs[ref_count × ref_size]           (variable-width BlockRefs)
```

A dict value that spans multiple pages appears in each page with only its refs for that page's row range. The bloom filter in the Page TOC covers which unique values are present in each page.

## Bloom Filter Design

- **Dict columns only**: bloom over the unique string/int64 values present in that page
- **Flat columns**: no bloom — min/max on sorted data is sufficient for range queries
- **Sizing**: 10 bits per unique value, minimum 16 bytes, k=7 hash functions
- **Hash**: same Kirsch-Mitzenmacher double-hashing as existing TraceIDBloom (FNV-1a based, adapted for arbitrary bytes)

Typical dict cardinality per page:
- `span:status` (3 values): all 3 in most pages → bloom = 4 bytes (min 16)
- `span:kind` (5 values): all 5 in most pages → bloom = 16 bytes
- `resource.service.name` (50+ values): subset per page → bloom = 64 bytes
- `span:name` (1000+ values): subset per page → bloom = ~200 bytes

## Page Size

**10,000 records per page** (configurable constant `IntrinsicPageSize = 10000`).

For a column with 1M entries: 100 pages. Page TOC ~5-10KB. Each page blob ~40-200KB compressed depending on column type.

## Backward Compatibility

Detection via the first byte of the column data blob:
- **0x01** (IntrinsicFormatVersion) = v1 monolithic blob → existing `DecodeIntrinsicColumnBlob` path
- **0x02** (IntrinsicPagedVersion) = v2 paged → new `DecodeIntrinsicPageTOC` + per-page decode

Old readers (v1) will fail gracefully on v2 data (version check). New readers support both formats.

The main file-level TOC (IntrinsicColMeta) is unchanged. The main TOC still has global min/max and total count.

## Phase 2 Read Path Changes

### Page-Selective Raw Scan (primary path)

With paging + raw byte scanning combined, the read path becomes:

1. Read Page TOC (small, cached)
2. For each page: check min/max + bloom against predicate
3. For matching pages only: fetch compressed page bytes from cache
4. Raw-scan each page's bytes directly (Phase 1 scanning on smaller data)
5. Return only matching BlockRefs

### Top-K with Pages

`collectTopKFromIntrinsicRefs` for `span:start`:
1. Read Page TOC for `span:start`
2. For backward (MostRecent): start from last page
3. Fetch + raw-scan only last page(s) until limit refs collected
4. For limit=20 on 1M entries with 100 pages: fetch 1 page (10K entries), scan last 20

## New Types

```go
// PageMeta describes one page in a paged intrinsic column.
type PageMeta struct {
    Offset   uint32 // byte offset relative to first page blob
    Length   uint32 // compressed page blob size
    RowCount uint32 // number of records in this page
    Min      string // encoded min value (same encoding as IntrinsicColMeta)
    Max      string // encoded max value
    Bloom    []byte // bloom filter bytes (nil for flat columns)
}

// PagedIntrinsicColumn holds the page TOC for a paged column.
type PagedIntrinsicColumn struct {
    Pages         []PageMeta
    BlockIdxWidth uint8
    RowIdxWidth   uint8
    Format        uint8      // IntrinsicFormatFlat or IntrinsicFormatDict
    ColType       ColumnType
}
```

## Write Path Changes

### Accumulator Changes

`intrinsicAccumulator` gains a paging phase after sorting:
1. Sort all entries (existing)
2. Chunk into pages of IntrinsicPageSize records
3. Per page: compute min/max, build bloom (dict only), encode page blob, snappy-compress
4. Build Page TOC from page metadata
5. Encode + snappy-compress Page TOC
6. Write Page TOC + page blobs sequentially
7. Return total size for IntrinsicColMeta

### Format Selection

Always use v2 paged format when row count > IntrinsicPageSize (10K). For columns with ≤10K rows, use v1 monolithic format (no benefit from paging).

## Files to Modify

### Shared types and codec
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go` — add PageMeta, PagedIntrinsicColumn
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go` — add IntrinsicPagedVersion, IntrinsicPageSize, IntrinsicPageBloomBitsPerItem
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go` — add DecodePageTOC, DecodeIntrinsicPage, EncodePageTOC; update DecodeIntrinsicColumnBlob for v2 detection

### Writer
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go` — add paging logic to encodeFlatColumn/encodeDictColumn, add encodePagedColumn, bloom builder

### Reader
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/intrinsic_reader.go` — add GetIntrinsicPageTOC, GetIntrinsicColumnPages, update GetIntrinsicColumn for v2

### Executor (predicate evaluation)
- `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go` — update BlockRefsFromIntrinsicTOC to use page-level pruning

### Executor (top-K)
- `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go` — update collectTopKFromIntrinsicRefs to use page-level scan

### Bloom filter
- `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/bloom.go` — add generic bloom functions (not trace-ID specific) or add new file `intrinsic_bloom.go`

## Testing Strategy

### Phase 1 Tests
1. **Unit tests for raw scan**: `ScanDictColumnRefs` with match/no-match/partial-match values
2. **Unit tests for flat scan**: `ScanFlatColumnRefs` with range predicates, verify identical results to full decode path
3. **Unit tests for top-K scan**: `ScanFlatColumnTopKRefs` backward/forward, verify correct refs
4. **Fuzz test**: encode random data → compare raw-scan results vs full-decode results (must be identical)
5. **Integration**: replace predicate path → run full querybench suite → verify no regressions

### Phase 2 Tests
1. **Unit tests for codec**: encode page TOC → decode page TOC roundtrip
2. **Unit tests for page encode/decode**: flat pages, dict pages, verify values/refs match
3. **Unit tests for bloom**: false-positive rate, membership checks
4. **Integration test**: write paged column → read full column → verify identical to v1
5. **Backward compat test**: v1 monolithic still works with new reader
6. **Benchmark**: compare v1 full decode vs raw-scan vs paged raw-scan

## Expected Performance Impact

### Phase 1 (Raw-Byte Scanning, no format change)

| Scenario | Current | Raw Scan | Improvement |
|---|---|---|---|
| `status=error` (dict, 1/3 values, 1M spans) | Build 1M BlockRef structs | Build 250K structs, skip 750K | ~3-4x less alloc |
| `service.name="X"` (dict, 1/50 values, 1M spans) | Build 1M BlockRef structs | Build 20K structs, skip 980K | ~50x less alloc |
| `duration > 100ms` (flat, 5% match, 1M spans) | Build 1M uint64 + 1M BlockRef | Stream values (0 alloc) + build 50K refs | ~20x less alloc |
| Top-K limit=20, `span:start` 1M entries | Build 1M uint64 + 1M BlockRef | Seek to end, build 20 refs | ~50000x less alloc |

### Phase 2 (Paged Layout, on top of raw scanning)

| Scenario | Phase 1 | Phase 1+2 | Additional Improvement |
|---|---|---|---|
| `status=error` (dict, 1/3 values) | Decompress full blob, scan | Decompress full blob (error in every page) | Minimal — still need all pages |
| `service.name="X"` (1/50 services) | Decompress full blob, scan | Decompress ~2% of pages via bloom | ~50x less decompression |
| `duration > 100ms` (flat, 5% match) | Decompress full blob, stream | Decompress ~5 of 100 pages via min/max | ~20x less decompression |
| Top-K limit=20, `span:start` 1M entries | Decompress full blob, seek | Decompress 1 page of 100 | ~100x less decompression |
