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
scans. When `lookupIntrinsicFields` needs values for M target spans from an N-row column,
it previously decoded all N rows; now it decodes only pages whose ref-range covers at
least one of the M target refs.

**Wire format:** Page TOC version bumped to 0x02 (`IntrinsicPageTOCVersion2`). Version
0x01 files are read with conservative defaults (MinRef=0, MaxRef=^uint32(0), RefBloom=nil)
— all pages are decoded, no regression for old files.

**Encoding of packed ref:** `uint32(blockIdx)<<16|uint32(rowIdx)`. Bloom key is the 4-byte
little-endian encoding of this value. Same bloom parameters as value bloom
(K=7, BitsPerItem=10, min=16 bytes, max=4096 bytes).

**Access path:** `Reader.GetIntrinsicColumnForRefs` builds a `map[uint32]struct{}` from the
target refs and calls `shared.DecodePagedColumnBlobFiltered`. The result is NOT cached
(query-scoped partial column). The blob-level cache is still used for I/O efficiency.

Back-ref: `internal/modules/blockio/shared/types.go:PageMeta`,
`internal/modules/blockio/shared/intrinsic_ref_filter.go`,
`internal/modules/blockio/shared/intrinsic_codec.go:EncodePageTOC/DecodePageTOC`

*Superseded by NOTE-007 (2026-03-29): RefBloom/MinRef/MaxRef removed. See NOTE-007 for rationale.*

---

## 7. NOTE-007: RefBloom Removed from Page TOC (2026-03-29)
*Added: 2026-03-29*

**Decision:** Removed `RefBloom []byte`, `MinRef uint32`, and `MaxRef uint32` from
`PageMeta`. Removed `IntrinsicRefBloomBytes` (256) and `IntrinsicRefBloomK` (3) constants.
Removed `matchesRefFilter`, `refFilterRange`, `uint32ToLE` from `intrinsic_ref_filter.go`
(functions were only used for page-skipping which is now removed). `EncodePageTOC` now
writes version 0x01 (no ref-range fields). `DecodePageTOC` only accepts version 0x01;
v0x02 is no longer supported and returns an error.

**Rationale:** RefBloom was designed to skip pages during reverse-lookup (`lookupIntrinsicFields`).
After the companion change that switches field population entirely to `forEachBlockInGroups`
(block reads), there are no remaining calls to `lookupIntrinsicFields` from the Case A
(plain) path. The ref-bloom provided zero pruning benefit at 10K entries/page with 256 bytes
(FPR ≈ 100% when full). Removal saves 256 bytes/page of storage and eliminates the bloom
maintenance cost at write time. `IntrinsicPageTOCVersion2` constant has also been removed
from `shared/constants.go` — no code references v0x02 any longer.

**Backward compat:** v0x02 files are not decoded. All production files write v0x01. If a
v0x02 blob is encountered, `DecodePageTOC` returns a `"unknown version 2"` error (same as
any other unrecognised version).

*Addendum (2026-03-29):* Removed the read-and-discard v0x02 backward-compatibility branch
from `DecodePageTOC`. Removed `IntrinsicPageTOCVersion2` constant from `constants.go`.

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
