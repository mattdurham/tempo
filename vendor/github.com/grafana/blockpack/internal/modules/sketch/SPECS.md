# Sketch Package — Specifications

## Overview

The `sketch` package provides three probabilistic data structures used for per-block, per-column
pruning and scoring in the blockpack query planner:

- `HyperLogLog` (p=4, 16 registers): cardinality estimation for block ranking
- `CountMinSketch` (w=64, d=4): frequency estimation for CMS pruning
- `BinaryFuse8`: value membership filter for hard pruning (no false negatives)

---

## SPEC-SK-01
**HLL Add never panics** — `HLL.Add(v)` must not panic for any input string, including empty string,
very long string, or Unicode.

## SPEC-SK-02
**HLL empty cardinality** — `HLL.Cardinality()` returns 0 for a freshly constructed HLL with no
elements added.

## SPEC-SK-03
**HLL marshal size** — `HLL.Marshal()` returns exactly 16 bytes (p=4 → 2^4=16 registers, 1 byte each).

## SPEC-SK-04
**HLL unmarshal validation** — `HLL.Unmarshal(b)` returns a non-nil error if `len(b) != 16`.

## SPEC-SK-05
**HLL round-trip** — After `h2.Unmarshal(h1.Marshal())`, `h2.Cardinality()` returns the same value
as `h1.Cardinality()`.

## SPEC-SK-06
**CMS Add never panics** — `CMS.Add(v, count)` must not panic for any input string or count value,
including count=0 and very large count.

## SPEC-SK-07
**CMS zero for unseen** — `CMS.Estimate(v)` returns 0 for any value that was never added.

## SPEC-SK-08
**CMS over-estimate guarantee** — `CMS.Estimate(v) >= actual_count(v)` always. A zero estimate
means the value is definitely absent (hard guarantee, no false negatives for zero).

## SPEC-SK-09
**CMS marshal size** — `CMS.Marshal()` returns exactly 512 bytes (w=64 × d=4 × 2 bytes per uint16
counter = 512).

## SPEC-SK-10
**CMS unmarshal validation** — `CMS.Unmarshal(b)` returns a non-nil error if `len(b) != 512`.

## SPEC-SK-11
**CMS round-trip** — After `cms2.Unmarshal(cms1.Marshal())`, `cms2.Estimate(v)` returns the same
value as `cms1.Estimate(v)` for all v.

## SPEC-SK-12
**Fuse8 no false negatives** — `BinaryFuse8.Contains(key)` must return `true` for every key that
was in the construction set passed to `NewBinaryFuse8`. False negatives are impossible.

## SPEC-SK-13
**Fuse8 may false positive** — `BinaryFuse8.Contains(key)` may return `true` for keys not in the
construction set. The expected false positive rate is ~0.39%.

## SPEC-SK-14
**Fuse8 round-trip** — After `f2.UnmarshalBinary(f1.MarshalBinary())`, `f2.Contains(k)` returns
the same result as `f1.Contains(k)` for all k.

## SPEC-SK-15
**Fuse8 empty filter** — `NewBinaryFuse8(nil)` and `NewBinaryFuse8([]uint64{})` return an empty
filter that returns `false` for all queries.

## SPEC-SK-16
**Canonical hash** — `sketch.HashForFuse(val string) uint64` is the single canonical hash function
for converting string values to fuse filter keys. It MUST be called identically at write time
(in writer's `add()`) and query time (in planner's predicate evaluation). Any change to
`HashForFuse` requires coordinated updates to both writer and query planner.
Back-ref: `internal/modules/sketch/hll.go:HashForFuse`

## SPEC-SK-17
**TopK sorted by count descending** — `TopK.Entries()` returns at most `TopKSize` (20) entries,
sorted by count descending. A stable secondary sort by key is applied for deterministic output
when counts are equal. An empty TopK returns nil.
Back-ref: `internal/modules/sketch/topk.go:Entries`

## SPEC-SK-18
**TopK fingerprint hashing** — `TopK.Add(key)` hashes the key via `HashForFuse(key)` (SPEC-SK-16)
and delegates to `AddFP`. No key string is stored; only the uint64 fingerprint and its count.
Back-ref: `internal/modules/sketch/topk.go:Add`

## SPEC-SK-26
**Concurrency safety** — `HyperLogLog` and `CountMinSketch` are NOT safe for concurrent use.
Callers must serialize all method calls (Add, Cardinality/Estimate, Marshal, Unmarshal).
`BinaryFuse8.Contains()` is safe for concurrent reads after construction (the filter is
immutable once built). `TopK` is NOT safe for concurrent use.

---

## Column-Major Sketch Wire Format (SPEC-SK-19 through SPEC-SK-25)

The following specs govern the column-major sketch section written by
`writer/sketch_index.go` and parsed by `reader/sketch_index.go`.

## SPEC-SK-19
**Section magic** — The sketch section starts with 4-byte little-endian magic `0x534B5443`
("SKTC"). Absence of this magic means the file predates the sketch index; readers must
degrade gracefully and treat all `ColumnSketch` results as nil.
Back-ref: `internal/modules/blockio/writer/sketch_index.go:sketchSectionMagic`

## SPEC-SK-20
**Column-major layout** — All sketch data for one column is serialized contiguously before
the next column begins. Within a column, data appears in this order:
1. name_len[2 LE] + name bytes
2. presence bitset (`ceil(numBlocks/8)` bytes, 1 bit per block)
3. distinct_count array (`numBlocks × 4` LE bytes, uint32 per block, 0 for absent)
4. TopK section (topk_k[1] followed by per-present-block entries)
5. CMS section (cms_depth[1] + cms_width[2 LE] + per-present-block counters)
6. Fuse section (per-present-block: fuse_len[4 LE] + fuse_data)

"Present" means the block has a non-zero presence bit for this column.
Back-ref: `internal/modules/blockio/writer/sketch_index.go:writeSketchIndexSection`

## SPEC-SK-21
**Presence bitset** — The presence bitset encodes which blocks contain the column. Bit `i`
(0-indexed from LSB of byte `i/8`) is 1 if block `i` has this column. Length is
`ceil(numBlocks/8)` bytes. The reader expands this to a `[]uint64` bitset for O(1)
word-level operations.
Back-ref: `internal/modules/blockio/reader/sketch_index.go:parseSketchIndexSection`

## SPEC-SK-22
**Distinct count array** — `distinct_count` has exactly `numBlocks` uint32 entries
(one per block, including absent blocks). Absent blocks have value 0. This is the
pre-computed `HLL.Cardinality()` value, evaluated at write time so readers do not
need to store HLL register arrays.
Back-ref: `internal/modules/blockio/writer/sketch_index.go` (distinct_count loop)

## SPEC-SK-23
**TopK wire format** — TopK entries are stored as (fingerprint, count) pairs in the
column-major sketch section written by `blockio/writer`:
- `topk_k[1]`: always `TopKSize` (20); readers use it as a sanity bound.
- Per present block: `topk_entry_count[1]` (0–K actual entries) followed by
  `topk_entry_count` pairs of `fp[8 LE uint64] + count[2 LE uint16]`.
- Fingerprints are `sketch.HashForFuse(key)` — the same canonical hash used by
  fuse filters and query-time lookup. Raw key strings are NOT stored in the wire format.

Note: `TopK.MarshalBinary()` (the in-memory serialization in the sketch package itself)
stores raw key strings for internal use. The writer converts keys to fingerprints when
writing the column-major sketch section. Readers only see fingerprints.
Back-ref: `internal/modules/blockio/writer/sketch_index.go` (TopK loop)

## SPEC-SK-24
**CMS wire format** — CMS counters are stored as:
- `cms_depth[1]`: always 4 (d=4).
- `cms_width[2 LE]`: always 64 (w=64).
- Per present block: `depth × width × 2` bytes of raw uint16 counters (little-endian),
  row-major order matching `CountMinSketch.Marshal()` output (512 bytes per block).
Back-ref: `internal/modules/blockio/writer/sketch_index.go` (CMS loop)

## SPEC-SK-25
**Fuse wire format** — BinaryFuse8 filters are stored as:
- Per present block: `fuse_len[4 LE uint32]` followed by `fuse_len` bytes of
  `BinaryFuse8.MarshalBinary()` output.
- `fuse_len == 0` is valid (empty filter; reader treats as conservative pass = true).
Back-ref: `internal/modules/blockio/writer/sketch_index.go` (Fuse loop)
