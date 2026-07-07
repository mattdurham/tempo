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

Next free ID: **SPEC-VC-7**.

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
  **[Updated by SPEC-VC-4, issue #490, task A-3/#110]** As of the legacy-decoder removal, this
  is no longer a signal to retry via a different decoder — `DecodeVCNTFile` is now the sole
  decode path (see SPEC-VC-4), so `ErrNotSelfDescribing` on real data represents a genuine
  format error. It remains a distinct sentinel (not folded into a generic decode error) because
  it still usefully distinguishes "not shaped like a self-describing file at all" from "shaped
  like one but has a malformed trailer/directory."
- Validates `bodyLen` is within `[0, len(data)-trailerSize]` before slicing; out-of-bounds
  `bodyLen` is a genuine decode error (not `ErrNotSelfDescribing`).
- The trailer-supplied `dirCount` is validated against `len(dirBytes)/minDirEntrySize` before
  being used as a `make([]ChunkDirEntry, 0, dirCount)` capacity hint — an unvalidated,
  corrupted or malicious `dirCount` (e.g. `0xFFFFFFFF`) would otherwise trigger an
  unrecoverable OOM `runtime.throw` rather than a clean decode error (SPEC-ROOT-001, mirroring
  SPEC-ROOT-012's decompression-bomb-guard principle). A `dirCount` exceeding what the
  remaining bytes could possibly hold is a decode error, not a panic.
- On success, delegates to `DecodeAll(body, dir)` using the embedded directory.

**`DecodeLegacyVCNTFile(data []byte) ([]Record, error)` rules: [SUPERSEDED by SPEC-VC-4 —
function removed outright, issue #490, task A-3/#110, 2026-07-07]**

- ~~Reconstructs a single-entry directory (`ChunkDirEntry{CompOff: 0, CompLen: len(data)}`)
  covering the entire payload and decodes via `DecodeAll`.~~
- ~~Valid only for objects that never exceeded one snappy chunk. Multi-chunk data fed through
  this path returns an error.~~
- Kept below for history only; see SPEC-VC-4 for the current (single-decode-path) contract.

**`DecodeVCNTObject(data []byte) ([]Record, error)` rules: [REVISED by SPEC-VC-4 — issue #490,
task A-3/#110, 2026-07-07; the function itself is NOT removed]**

- ~~Tries `DecodeVCNTFile` first. On success, returns its result. If `DecodeVCNTFile` returns
  `ErrNotSelfDescribing`, falls back to `DecodeLegacyVCNTFile`.~~ Now calls `DecodeVCNTFile`
  directly and propagates any error unchanged — there is no fallback decoder left to dispatch to.
- Retained as its own named function (not inlined into callers) because it has real production
  callers (`valuecountscompactor/service.go`, `vcnt.go:VCNTBuildSectionFromObjects`) that
  shouldn't need churn if a future VCNT format variant is ever added.
- See SPEC-VC-4 for the current authoritative contract.

**Rationale (historical):** `vcntwriter.go` (tempo-mrd, out of this repo's scope) used to
discard the `[]ChunkDirEntry` that `EncodeRecords` returns, so no persisted VCNT object could
carry its own directory. `EncodeVCNTFile`/`DecodeVCNTFile` closed that gap for this package's own
output; `DecodeLegacyVCNTFile` and `DecodeVCNTObject` let a compactor built on this package
correctly read files written by that un-migrated write path in the interim. **As of task
A-Tempo-1/#111 (2026-07-07), `vcntwriter.go` itself writes self-describing `EncodeVCNTFile`
output unconditionally, closing the gap at the source — see SPEC-VC-4.**

Back-ref: `internal/modules/valuecounts/selfdescribing.go` (`EncodeVCNTFile`,
`DecodeVCNTFile`, `decodeDirEntries`, `DecodeVCNTObject` — `DecodeLegacyVCNTFile` deleted, see
SPEC-VC-4).

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

**Scope note:** this was originally the only new root-package (`blockpack`) public API surface
added for this purpose under root `CLAUDE.md`'s per-addition-approval rule. **Superseded
2026-07-07:** the maintainer granted blanket public-API change permission for the read-path
modernization project (see `NOTES.md` NOTE-VC-015), which now covers further `valuecounts`
root-package re-export additions/removals without per-addition approval — see SPEC-VC-4/SPEC-VC-5
for the changes made under that grant.

Back-refs: `vcnt.go:46` (`CompactVCNTRecords`), `internal/modules/valuecounts/compaction.go:25`
(`Compact`, SPEC-VC-1). Consumer: tempo-mrd's `tempodb/encoding/vblockpack/vcntwriter_test.go`
(`TestVCNTFlush_CrossBlockMinuteCoalescing`, out of this repo's scope — tempo-mrd has no
SPECS.md/NOTES.md convention for this code). See `NOTES.md` NOTE-VC-011.

---

## SPEC-VC-4: DecodeVCNTFile is the sole supported decode path (legacy single-chunk reconstruction removed)
*Added: 2026-07-07*

**Contract:** `DecodeVCNTFile` is the only supported decode path for VCNT objects in this
package. The legacy single-chunk reconstruction fallback (`DecodeLegacyVCNTFile`, formerly
described under SPEC-VC-2) is removed — issue #490, task A-3/#110 — now that the write path
(tempo-mrd's `vcntwriter.go`, task A-Tempo-1/#111) always emits self-describing output
unconditionally, and the project-wide stored-data wipe retires any pre-existing legacy-shaped
objects.

**Rules:**

- `DecodeVCNTFile(data []byte) ([]Record, error)` behavior is UNCHANGED from SPEC-VC-2's
  original description (trailer/magic validation, `bodyLen` bounds check, `dirCount` OOM guard)
  — only its role changes, from "one of two dispatch targets" to "the only decode path."
- `ErrNotSelfDescribing` is KEPT as a distinct sentinel (still `errors.Is`-comparable) — it
  still usefully distinguishes "not shaped like a self-describing file" from a genuine
  malformed-trailer/directory decode error on data that IS self-describing. It no longer
  signals "retry via a different decoder" (there is none); on real input it represents a
  genuine format error.
- `DecodeVCNTObject(data []byte) ([]Record, error)` is KEPT as a named function — it now calls
  `DecodeVCNTFile` directly and propagates any error unchanged (no fallback dispatch). It
  remains the compactor's per-input-file decode entry point (`valuecountscompactor/service.go`,
  `vcnt.go:VCNTBuildSectionFromObjects`).
- No migration/dual-read window is provided or needed: the project's stored-data wipe (issue
  #490) removes any object that could still be in the legacy shape before this ships.

**Rationale:** One encoder, one decoder, per this project's no-backward-compatibility directive
— see `NOTES.md` NOTE-VC-015 for the full three-task sequencing (A-1/A-3/A-17) this contract is
the end-state of.

Back-refs: `internal/modules/valuecounts/selfdescribing.go` (`DecodeVCNTFile`, `DecodeVCNTObject`
— `DecodeLegacyVCNTFile` deleted). `NOTES.md` NOTE-VC-005 (addendum), NOTE-VC-015.

---

## SPEC-VC-5: EncodeVCNTFile — public re-export of EncodeVCNTFile for external callers
*Added: 2026-07-07*

**Contract:** `blockpack.EncodeVCNTFile(records []VCNTRecord, perChunk int) []byte` (root
package, `vcnt.go`) is a thin, behavior-preserving public wrapper around this package's
`EncodeVCNTFile` (SPEC-VC-2): `return valuecounts.EncodeVCNTFile(records, perChunk)`. It
introduces no new semantics — every rule in SPEC-VC-2's `EncodeVCNTFile` contract (wire
format, trailer layout) applies identically through this entry point.

**Note:** the root package's older `EncodeVCNTRecords` re-export (a non-self-describing,
single-chunk-only encoder) is REMOVED (issue #490, task A-17/#121, 2026-07-07) — it had zero
remaining consumers once tempo-mrd's `vcntwriter.go` switched to this function. `EncodeVCNTFile`
is now the sole root-package VCNT encoder. See `NOTES.md` NOTE-VC-015.

Back-refs: `vcnt.go:EncodeVCNTFile`, `internal/modules/valuecounts/selfdescribing.go:EncodeVCNTFile`
(SPEC-VC-2). Test: `vcnt_test.go:TestEncodeVCNTFile_RoundTrip`. Consumer: tempo-mrd's
`vcntwriter.go` (out of this repo's scope).

## SPEC-VC-6: SelectivityPerMinute — per-minute selectivity contract
*Added: 2026-07-07*

**Contract:** `SelectivityPerMinute(data []byte, dir []ChunkDirEntry, column string, value
[]byte, minTS, maxTS uint64) ([]MinuteCount, error)` decodes `[minTS, maxTS]` via
`DecodeTimeRange`, sums `Record.Count` for the exact `(column, value)` pair grouped by
`Record.TimeStart`, and returns one `MinuteCount{Minute, Count}` per distinct live minute,
sorted ascending by `Minute`. `Minute` is a unix-second, 60-aligned bucket boundary inherited
from the write path (tempo `vcntwriter.go`'s `minuteBucket`) — this function does not re-derive
or re-validate minute alignment, it trusts `Record.TimeStart` is already floored.

**Liveness rule:** a minute whose net summed `Count` is `<= 0` is dropped from the result, never
returned as `Count: 0` (same rule as `SelectivityInRange`/`ValuesInRange`, NOTE-VC-001). Records
for a different column or a different value in the same window contribute nothing.

**No files opened:** like every other function in this file, it operates only on the
already-decoded VCNT section (`data`/`dir`) and opens no blockpack data files.

Back-refs: `perminute.go:SelectivityPerMinute`, `MinuteCount`. Test: `perminute_test.go`. Issue
#487, task C1. See `NOTES.md` NOTE-VC-016.
