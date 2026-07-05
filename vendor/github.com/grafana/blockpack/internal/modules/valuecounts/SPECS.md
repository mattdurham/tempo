# valuecounts — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valuecounts` package. It complements `NOTES.md` (design rationale) and
`TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecountscompactor/SPECS.md`'s own separate `SPEC-VC-N` sequence — each
module's SPECS.md numbers from 1, per the established convention in
`valueindex/SPECS.md`/`valueindexcompactor/SPECS.md` sharing the `SPEC-VI-N` prefix with
independent per-file counters). IDs are assigned in ascending order and never reused or
renumbered; superseded entries are marked `[SUPERSEDED by SPEC-VC-N]` rather than deleted.

Next free ID: **SPEC-VC-4**.

---

## SPEC-VC-1: Compact retention semantics
*Added: 2026-07-02*

**Contract:** `Compact(records []Record) []Record` groups its input by the merge key
`(ColumnName, TimeStart, TimeEnd, Value)`, sums `Count` within each group, and drops any group
whose summed `Count` is `<= 0` from the output. A dropped group means the value's net live
count has reached zero across all inputs — it no longer exists in any live block and must not
appear in query results.

**Rules:**

- Grouping is exact on all four key fields; `TimeEnd` is part of the key (see NOTE-VC-002) —
  two records sharing `ColumnName`/`TimeStart`/`Value` but differing `TimeEnd` are never merged.
- `Count` is a signed `int64`; summation, not counting, is the merge operation.
- Groups summing to exactly `0` are dropped, not retained with `Count: 0` — VCNT has no
  zero-count tombstone representation.
- `Compact` sorts its input in place (via `Sort`) as part of computing group runs; callers
  needing the original order preserved must copy the slice first.
- Empty input (`len(records) == 0`) returns `nil`, not an empty non-nil slice. An input that
  compacts to zero live groups also returns `nil`.
- The returned slice is in canonical VCNT order (`ColumnName, TimeStart, Value, Count` per
  `Sort`).

**Rationale:** This is the self-healing delta-accounting mechanism described in NOTE-VC-001 —
retention and compaction are both expressed as negative-count records, with no separate
tombstone state to track or garbage-collect.

Back-ref: `internal/modules/valuecounts/compaction.go:25` (func `Compact`).

---

## SPEC-VC-2: EncodeVCNTFile / DecodeVCNTFile self-describing format contract
*Added: 2026-07-02*

**Contract:** `EncodeVCNTFile(records []Record, perChunk int) []byte` produces a VCNT object
that carries its own chunk directory, so any consumer can decode it directly from object
storage without a side-channel directory.

**Wire format:**

```
[snappy-chunked body, identical to EncodeRecords's body output]
[ChunkDirEntry × dirCount, each: min_column_len[2 LE] + min_column[N] + min_time_start[8 LE]
                                  + comp_off[4 LE] + comp_len[4 LE]]
[trailer: dir_count[4 LE] + body_len[4 LE] + magic[4 LE] = 0x56434E31 ("VCN1")]
```

Trailer size is fixed at 12 bytes. Directory entry encoding mirrors `section.go`'s existing
length-prefixed, little-endian conventions.

**`DecodeVCNTFile(data []byte) ([]Record, error)` rules:**

- Reads the trailing 12 bytes; if `len(data) < 12` or the magic field doesn't match
  `0x56434E31`, returns `ErrNotSelfDescribing` (wrapped via `%w`, `errors.Is`-comparable).
  **This is not an error condition for the caller to treat as corrupt data** — it signals "try
  `DecodeLegacyVCNTFile` instead."
- Validates `bodyLen` is within `[0, len(data)-trailerSize]` before slicing; out-of-bounds
  `bodyLen` is a genuine decode error (not `ErrNotSelfDescribing`).
- The trailer-supplied `dirCount` is validated against `len(dirBytes)/minDirEntrySize` before
  being used as a `make([]ChunkDirEntry, 0, dirCount)` capacity hint — an unvalidated,
  corrupted or malicious `dirCount` (e.g. `0xFFFFFFFF`) would otherwise trigger an
  unrecoverable OOM `runtime.throw` rather than a clean decode error (SPEC-ROOT-001, mirroring
  SPEC-ROOT-012's decompression-bomb-guard principle). A `dirCount` exceeding what the
  remaining bytes could possibly hold is a decode error, not a panic.
- On success, delegates to `DecodeAll(body, dir)` using the embedded directory.

**`DecodeLegacyVCNTFile(data []byte) ([]Record, error)` rules:**

- Reconstructs a single-entry directory (`ChunkDirEntry{CompOff: 0, CompLen: len(data)}`)
  covering the entire payload and decodes via `DecodeAll`.
- Valid **only** for objects that never exceeded one snappy chunk. Multi-chunk data fed
  through this path returns an error — `snappy.Decode` fails on a payload of multiple
  independently-compressed chunks concatenated together — it never returns partial or garbage
  records (SPEC-ROOT-010).

**`DecodeVCNTObject(data []byte) ([]Record, error)` rules:**

- Tries `DecodeVCNTFile` first. On success, returns its result.
- If `DecodeVCNTFile` returns `ErrNotSelfDescribing`, falls back to `DecodeLegacyVCNTFile`.
- Any other error from `DecodeVCNTFile` (e.g. malformed self-describing trailer/directory) is
  propagated directly — it is not treated as a legacy-format signal.
- This is the compactor's per-input-file decode entry point: it must transparently accept both
  self-describing files (this package's own future output) and legacy files (today's real
  `vcntwriter.go` write-path output, see NOTE-VC-005).

**Rationale:** `vcntwriter.go` (tempo-mrd, out of this repo's scope) discards the
`[]ChunkDirEntry` that `EncodeRecords` returns, so no persisted VCNT object today carries its
own directory. `EncodeVCNTFile`/`DecodeVCNTFile` close that gap for this package's own output;
`DecodeLegacyVCNTFile` and `DecodeVCNTObject` let a compactor built on this package correctly
read files written by the existing, un-migrated write path in the interim.

Back-ref: `internal/modules/valuecounts/selfdescribing.go` (`EncodeVCNTFile` line 35,
`DecodeVCNTFile` line 51, `decodeDirEntries` line 137, `DecodeLegacyVCNTFile` line 82,
`DecodeVCNTObject` line 95).

---

## SPEC-VC-3: CompactVCNTRecords — public re-export of Compact for external callers
*Added: 2026-07-02*

**Contract:** `blockpack.CompactVCNTRecords(records []VCNTRecord) []VCNTRecord` (root package,
`vcnt.go:46`) is a thin, behavior-preserving public wrapper around this package's
`Compact` (SPEC-VC-1): `func CompactVCNTRecords(records []VCNTRecord) []VCNTRecord { return
valuecounts.Compact(records) }`. It introduces no new semantics — every rule in SPEC-VC-1
(grouping by `(ColumnName, TimeStart, TimeEnd, Value)`, signed-count summation, drop-if-`<=0`,
sorts input in place, `nil` on empty/fully-dropped input, canonical output order) applies
identically through this entry point.

**Rationale for existing as a public wrapper:** exposes the real merge/compaction logic to
callers outside this module (specifically tempo-mrd's block-builder test suite) so they can
exercise genuine cross-block coalescing behavior end-to-end instead of maintaining a
test-local reimplementation of `Compact`'s grouping/summing/drop rules that could silently
drift from the real logic. `VCNTRecord` (`vcnt.go:21`) is already a type alias for
`valuecounts.Record`, so no data conversion occurs at the wrapper boundary.

**Scope note:** this is the only new root-package (`blockpack`) public API surface added for
this purpose — root `CLAUDE.md` requires explicit user permission before adding new public API
surface, obtained for this addition specifically; it should not be read as license to add
further `valuecounts` re-exports without the same explicit approval.

Back-refs: `vcnt.go:46` (`CompactVCNTRecords`), `internal/modules/valuecounts/compaction.go:25`
(`Compact`, SPEC-VC-1). Consumer: tempo-mrd's `tempodb/encoding/vblockpack/vcntwriter_test.go`
(`TestVCNTFlush_CrossBlockMinuteCoalescing`, out of this repo's scope — tempo-mrd has no
SPECS.md/NOTES.md convention for this code). See `NOTES.md` NOTE-VC-011.
