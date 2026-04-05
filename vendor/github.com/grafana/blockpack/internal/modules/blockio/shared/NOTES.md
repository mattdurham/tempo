# blockio/shared — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/blockio/shared` package.

---

## 1. NOTE-BLOOM-REMOVAL: ColumnNameBloom Removed (2026-03-07)
*Added: 2026-03-07*

`ColumnNameBloom [32]byte` has been removed from `BlockMeta`. The column-name bloom filter
functions (`AddToBloom`, `TestBloom`, `BloomHash1`, `BloomHash2`, `SetBit`, `IsBitSet`,
`murmur32`) have been deleted from `bloom.go`.

**Rationale:** CMS subsumes bloom: if a column was never written to a block, `BlockCMS`
returns nil and the planner passes conservatively — identical behavior to a bloom miss.
CMS additionally provides value-level pruning for columns that ARE present. Column-name
bloom provided zero pruning for high-cardinality columns where every block has the column
(e.g. `resource.service.name`).

**Wire format impact:** Block index entries are 32 bytes smaller per block. Files written
before 2026-03-07 are unreadable with this version (breaking change, accepted for internal
format).

**Kept:** Trace-ID bloom functions (`AddTraceIDToBloom`, `TestTraceIDBloom`,
`TraceIDBloomSize`) are **not** removed — they serve a different purpose (compact index
trace-ID lookup).

---

## 2. *(Removed)* 256-Bit Column-Name Bloom Filter Size
*Added: 2026-03-05, removed: 2026-03-07*

The 256-bit (32-byte) `ColumnNameBloom` field and the `ColumnNameBloomBits`/`ColumnNameBloomBytes`
constants have been removed. See NOTE-BLOOM-REMOVAL (§1) for rationale.

---

## 3. ColumnKey for Type-Aware Column Maps
*Added: 2026-03-05*

**Decision:** `ColumnKey{Name, Type}` is used in maps that need to distinguish the same
attribute key with different types (e.g. `"foo"` as string vs. `"foo"` as int64).

**Rationale:** The OTLP data model permits the same attribute key to carry different types
across different spans within the same block. A name-only map key would cause silent
overwrite of the first column's data by the second. Using `ColumnKey` prevents this.

**Scope:** Only writer-internal column maps use `ColumnKey`. Range index
lookups remain name-only intentionally — at the block-pruning level, false positives
(treating int64 "foo" and string "foo" as the same column) are acceptable.

Back-ref: `internal/modules/blockio/shared/types.go:ColumnKey`

---

## 4. RLE Codecs for Presence and Index Data
*Added: 2026-03-05*

**Decision:** Column presence bitsets and dictionary index arrays are RLE-encoded rather
than stored as raw bit arrays.

**Rationale:** Blockpack columns typically have high locality — columns that are absent from
a row tend to be absent from many consecutive rows (e.g. a `span.http.status_code` column
is absent from non-HTTP spans). RLE compresses these runs to a small constant representation
regardless of span count.

For presence bitsets: a block with 2000 spans where a column is present in only 100 spans
can be represented in O(100) bytes rather than 250 bytes (2000/8).

For index arrays: spans from the same service or operation often repeat the same dictionary
entries consecutively, yielding high RLE compression.

Back-ref: `internal/modules/blockio/shared/presence_rle.go`,
`internal/modules/blockio/shared/index_rle.go`

---

## 6. NOTE-006: PageMeta Extended with Ref-Range Index (2026-03-28)
*Added: 2026-03-28*

**Decision:** Added `MinRef uint32`, `MaxRef uint32`, `RefBloom []byte` to `PageMeta`.

**Rationale:** Enables O(M × page_fraction) reverse lookups instead of O(N) full column
scans.

*Superseded by NOTE-007 (2026-03-29): RefBloom/MinRef/MaxRef removed. See NOTE-007 for rationale.*

---

## 7. NOTE-007: RefBloom Removed from Page TOC; v0x02 Not Supported (2026-03-29)
*Added: 2026-03-29*

**Decision:** Removed `RefBloom []byte`, `MinRef uint32`, and `MaxRef uint32` from
`PageMeta`. Removed `IntrinsicPageTOCVersion2` constant. `EncodePageTOC` writes version
0x01 only. `DecodePageTOC` only accepts version 0x01; v0x02 is not supported and returns
an error.

**Rationale:** RefBloom was designed to skip pages during reverse-lookup. After switching
field population entirely to `forEachBlockInGroups` (block reads), there are no remaining
callers. The ref-bloom provided zero pruning benefit at 10K entries/page with 256 bytes
(FPR ≈ 100% when full). Removal saves 256 bytes/page of storage and eliminates the bloom
maintenance cost at write time.

**Backward compat:** v0x02 files are not decoded. All production files write v0x01.

Back-ref: `shared/constants.go`, `shared/types.go`, `shared/intrinsic_codec.go`,
`writer/intrinsic_accum.go`

---

## 5. AttrKV Slice Instead of map[string]AttrValue
*Added: 2026-03-05*

**Decision:** `AttrKV` is a plain struct, and attribute sets are represented as `[]AttrKV`
rather than `map[string]AttrValue`.

**Rationale:** Per-span map allocations (header + hash buckets) create significant GC
pressure when processing millions of spans. A slice of `AttrKV` avoids map header allocation
entirely and is cache-friendly for small attribute sets (most spans have ≤ 20 attributes).
Lookup is O(N) where N is the attribute count, which is acceptable at the write path where
spans are processed once.

Back-ref: `internal/modules/blockio/shared/types.go:AttrKV`

---

## 8. NOTE-008: BUG-1 Fix — Bounds Check Before refsStart Arithmetic in Flat-Column Scan (2026-04-01)
*Added: 2026-04-01*

**Decision:** Added division-based bounds checks in `ScanFlatColumnRefs`, `ScanFlatColumnTopKRefs`,
and `ScanFlatColumnRefsFiltered` before computing `refsStart`. Each check uses `rowCount > (len(raw)-pos)/8`
rather than `pos+rowCount*8 > len(raw)` to avoid 32-bit integer overflow: on 32-bit platforms,
`rowCount*8` can wrap to a negative value when `rowCount > math.MaxInt/8`, making the addition-based
check ineffective.

**Rationale:** `rowCount` is an untrusted uint32 read from a snappy-decoded blob. A corrupt
or adversarially crafted blob can set `rowCount` to any value up to 2^32-1. Without this
check, `pos + rowCount*8` can produce a value larger than `len(raw)`, making `refsStart`
point well past the end of the buffer. Subsequent arithmetic on `refsStart`
(`refPos = refsStart + i*refSize`) could then produce values that look valid to the
`if refPos+refSize > len(raw)` per-ref guard (e.g. wrap-around on 32-bit platforms), leading
to out-of-bounds memory access.

The per-ref bounds checks (`if refPos+refSize > len(raw)`) remain in place as a defense-in-depth
second layer. This new check is the primary gate.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:ScanFlatColumnRefs`,
`ScanFlatColumnTopKRefs`, `ScanFlatColumnRefsFiltered`

---

## 9. NOTE-009: BUG-13 Fix — Validate blockW/rowW Are 1 or 2 in Flat-Column Scan (2026-04-01)
*Added: 2026-04-01*

**Decision:** Added `if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) { return nil }`
in all flat-column scan callers of `decodeRef`, and added the same check with an error return
in `decodeVariableWidthRef`.

**Rationale:** `blockW` and `rowW` are read from an untrusted snappy-decoded blob header.
Valid encoding uses 1-byte or 2-byte fields (supporting up to 256 or 65536 block/row indices
respectively). Any other value (0, 3, or higher) indicates a corrupt blob. Without this check,
`decodeRef` with `blockW==0` falls through to `binary.LittleEndian.Uint16` (reads 2 bytes for a
0-width field), producing a `BlockRef` whose block index is derived from the wrong bytes.
`decodeVariableWidthRef` had the same flaw. The fix makes both functions fail explicitly on
invalid widths rather than silently producing garbage.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodeVariableWidthRef`,
`ScanFlatColumnRefs`, `ScanFlatColumnTopKRefs`, `ScanFlatColumnRefsFiltered`
