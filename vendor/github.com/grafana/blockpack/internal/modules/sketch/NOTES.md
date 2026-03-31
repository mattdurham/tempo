# Sketch Package — Design Notes

## NOTE-SK-01 (2026-03-07)
**HLL p=4 chosen — 16 registers, 16 bytes, ~26% std error.**

p=4 gives 2^4=16 registers, serialized as 16 bytes (one uint8 per register). Standard error is
~26%, which is acceptable for cardinality bucketing (low/medium/high distinctions needed for block
scoring). Exact cardinality is not required — we only need relative ordering between blocks for
ranking purposes. Using p=14 (the typical production HLL setting) would give 16KB per column per
block, which is 1000x larger and impractical for metadata storage.

## NOTE-SK-02 (2026-03-07, updated 2026-03-08)
**CMS w=64, d=4 chosen — 512 bytes per column per block, 2-byte counters.**

Width w=64 means each of the d=4 hash functions maps to one of 64 buckets. Total storage:
64 × 4 × 2 = 512 bytes per column per block. Four independent hash rows give a collision
probability of (1/64)^4 ≈ 6×10^-8, making the probability of a false zero low enough for pruning.
2-byte counters (uint16, max 65535) chosen over 1-byte to avoid saturation for hot values (e.g.,
`status_code="200"` may appear millions of times in a block). Saturating add is used: once a
counter reaches 65535, it stays there.

**Width was reduced from 256 to 64 (2026-03-08):** At 1GB scale with many blocks and columns,
w=256 produced ~150MB of decoded sketch metadata, exceeding the 100MB MaxMetadataSize limit.
w=64 reduces per-block CMS from 2048→512 bytes (4×), bringing decoded metadata to ~49MB at 1GB
scale. The higher collision probability (6×10^-8 vs 2.3×10^-10) is acceptable — false positives
from CMS only reduce pruning efficiency, never cause data loss.

## NOTE-SK-03 (2026-03-07)
**Hash function: FNV-32a with d independent seeds (seed_i = i × 2654435761).**

FNV-1a is chosen for CMS hashing because it is:
- Dependency-free (standard library hash/fnv or manual implementation)
- Fast (~2 ns/call)
- Reproducible across runs (deterministic, no randomization)
The d independent row hashes are derived by XOR-ing the FNV-32a output with `i * 2654435761`
(Knuth's multiplicative constant) and taking mod w=64.

FNV-1a 64-bit is used for HLL and for `HashForFuse`.

## NOTE-SK-04 (2026-03-07, updated 2026-03-08)
**Wire format: CMS = 512 bytes (little-endian uint16), HLL = 16 bytes (uint8 per register).**

CMS: 64 counters × 4 rows × 2 bytes each = 512 bytes, stored row-major, little-endian uint16.
HLL: 16 registers × 1 byte each = 16 bytes, stored as raw uint8 array.
Both are fixed-size, enabling direct positional reads in the sketch section parser without length
fields. The size is documented in SPEC-SK-03 and SPEC-SK-09.

## NOTE-SK-05 (2026-03-07)
**BinaryFuse8 chosen over bloom filter for per-block value membership.**

BinaryFuse8 provides:
- 0.39% false positive rate at ~9 bits/element (vs ~1% for Bloom at same space)
- 3 indexed reads per lookup (vs k scattered probes for Bloom)
- Better cache locality (3 reads in adjacent segments vs k random probes)
- "All keys upfront" construction is not a constraint here: the writer accumulates keys during
  block construction and calls NewBinaryFuse8 only at flush time.

The `github.com/FastFilter/xorfilter` library (Apache 2.0) provides the BinaryFuse8 implementation.
No false negatives are possible (SPEC-SK-12): once a key hash is in the filter, Contains always
returns true.

## NOTE-SK-06 (2026-03-07)
**TopK uses exact frequency map — block-bounded memory makes streaming unnecessary.**

TopK internally uses a `map[string]uint32` to count exact frequencies during block construction.
This is simpler and more accurate than streaming top-K algorithms (e.g., Space-Saving, Count-Min
Sketch top-K) because:
- Block size is bounded (MaxBlockSpans constant), so the map never grows unbounded
- Exact counts enable correct block scoring (freq/cardinality) without approximation error
- At flush time, the map is sorted and truncated to K=20 entries — O(n log n) once per block

Key truncation to 100 bytes prevents pathological cases with extremely long string values.
The wire format stores TopK inline within the column-major section (SPEC-SK-23): per present
block, a 1-byte entry count followed by (fp[8 LE uint64] + count[2 LE uint16]) pairs.
Fingerprints are `HashForFuse(key)` — raw strings are not stored. See SPEC-SK-23 for the
complete format.

## NOTE-SK-07 (2026-03-08)
**Block-major to column-major wire format transition.**

The original sketch section design stored data block-by-block: for block 0, all columns;
for block 1, all columns; and so on (block-major). This was convenient for the writer
(which builds sketches block-by-block as spans are added) but inefficient for the reader
at query time.

**Why block-major was suboptimal:** A query predicate like `resource.service.name = "auth"`
accesses one column across all N blocks. With block-major storage, reading the CMS for
`resource.service.name` across 256 blocks required 256 scattered reads (one per block's
column section), defeating prefetcher and CPU cache.

**New approach (column-major, SPEC-SK-20):** All data for one column is written contiguously:
presence bitset, then `distinct[0..N]`, then TopK entries for all present blocks, then CMS
counters for all present blocks, then fuse data for all present blocks. This matches the
query-time access pattern exactly.

**At write time the accumulation is still block-major:** `blockSketchSet` (a map of
column→`colSketch` per block) is unchanged. The pivot happens only during serialization:
`writeSketchIndexSection` collects all column names, then for each column iterates all
blocks. This is an O(numBlocks × numColumns) transpose at flush time — negligible compared
to the I/O that follows.

**Reader impact:** `parseSketchIndexSection` populates flat `[]uint32`, `[][]uint64`,
`[]*CountMinSketch`, `[]*BinaryFuse8` arrays per column. `ColumnSketch(col)` returns the
pre-parsed `*columnSketchData` struct, which implements `queryplanner.ColumnSketch`.
All bulk methods (`Distinct()`, `CMSEstimate()`, `TopKMatch()`, `FuseContains()`) return
pre-allocated slices — zero allocation at query time.

**Fingerprints instead of raw strings in TopK:** The old per-block TopK stored raw key
strings. The column-major format stores `HashForFuse(key)` fingerprints (uint64) instead.
This halves the storage for typical keys and enables direct comparison at query time without
rehashing (the query planner already calls `HashForFuse` on the query value).

Back-ref: `internal/modules/blockio/writer/sketch_index.go:writeSketchIndexSection`,
`internal/modules/blockio/reader/sketch_index.go:parseSketchIndexSection`
