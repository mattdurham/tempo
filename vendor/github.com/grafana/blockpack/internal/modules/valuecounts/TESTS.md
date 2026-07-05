# valuecounts — Test Specifications

This document defines the required tests for the `internal/modules/valuecounts` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecountscompactor/TESTS.md`'s own separate `TEST-VC-N` sequence). IDs are
assigned in ascending order and never reused or renumbered.

Next free ID: **TEST-VC-7**.

---

## TEST-VC-1: TestEncodeDecodeVCNTFile_RoundTrip
*Added: 2026-07-02*

**Scenario:** `EncodeVCNTFile`/`DecodeVCNTFile` round-trip records spanning multiple chunks.

**Setup:** 10 records sorted into canonical order, encoded with `perChunk=3` (spans 4 chunks).

**Assertions:** `DecodeVCNTFile` returns no error; the decoded records equal the input set
under an order-independent multiset comparison (`requireRecordsEqual`/`recordKeys`).

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestEncodeDecodeVCNTFile_RoundTrip`.

---

## TEST-VC-2: TestDecodeVCNTFile_RejectsNonSelfDescribing
*Added: 2026-07-02*

**Scenario:** `DecodeVCNTFile` must distinguish "not self-describing" from generic corruption
so callers know to fall back to `DecodeLegacyVCNTFile` rather than treat the data as garbage.

**Setup:** Legacy `EncodeRecords` output (no embedded directory/trailer) fed directly to
`DecodeVCNTFile`.

**Assertions:** Returns a non-nil error, and `errors.Is(err, ErrNotSelfDescribing)` is true.

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTFile_RejectsNonSelfDescribing`.

---

## TEST-VC-3: TestDecodeLegacyVCNTFile_SingleChunkReconstruction
*Added: 2026-07-02*

**Scenario:** `DecodeLegacyVCNTFile` must correctly recover records from today's real
write-path shape: `EncodeRecords` output with its returned directory discarded (the
`vcntwriter.go` gap described in NOTE-VC-006).

**Setup:** Two records sharing `(ColumnName, TimeStart, TimeEnd)` encoded via `EncodeRecords`
with `perChunk=0` (verified by test setup to produce exactly 1 chunk); only the body bytes are
passed to `DecodeLegacyVCNTFile` (directory discarded, matching production behavior).

**Assertions:** Decoded records exactly match the input records (order-independent comparison).

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeLegacyVCNTFile_SingleChunkReconstruction`.

---

## TEST-VC-4: TestDecodeLegacyVCNTFile_MultiChunkDataErrors
*Added: 2026-07-02*

**Scenario:** SPEC-ROOT-010 guardrail — `DecodeLegacyVCNTFile`'s single-chunk assumption must
fail loudly, not silently produce partial or wrong data, when fed data that actually spans
multiple chunks.

**Setup:** 10 records encoded via `EncodeRecords` with `perChunk=3` (verified by test setup to
produce 2+ chunks), body bytes only (directory discarded) passed to `DecodeLegacyVCNTFile`.

**Assertions:** Returns a non-nil error (multi-chunk snappy payload fails to decode as a single
stream) rather than succeeding with truncated/garbage records.

**Spec invariants tested:** SPEC-VC-2, SPEC-ROOT-010.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeLegacyVCNTFile_MultiChunkDataErrors`.

---

## TEST-VC-5: TestDecodeVCNTObject_TriesSelfDescribingThenLegacy
*Added: 2026-07-02*

**Scenario:** `DecodeVCNTObject` — the compactor's per-input-file decode entry point — must
correctly handle all three object shapes it will encounter in production: self-describing
files, legacy single-chunk files, and genuinely corrupt data.

**Setup:** Table-driven with three cases: (1) `EncodeVCNTFile` output, (2) legacy
`EncodeRecords` (single-chunk) output, (3) arbitrary bytes (`{0x01, 0x02, 0x03}`) matching
neither format.

**Assertions:** Cases 1 and 2 decode successfully and match their respective input records;
case 3 returns a non-nil error.

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTObject_TriesSelfDescribingThenLegacy`.

---

## TEST-VC-6: TestDecodeVCNTFile_RejectsImpossibleDirCount
*Added: 2026-07-02*

**Scenario:** SPEC-ROOT-001/SPEC-ROOT-012-analog regression guard — `decodeDirEntries` must
reject a trailer-supplied `dirCount` that exceeds what the actual directory bytes could
possibly hold, rather than passing it straight into `make([]ChunkDirEntry, 0, dirCount)` as an
allocation hint (a `dirCount = 0xFFFFFFFF` against near-empty directory bytes previously
crashed the process with an unrecoverable `runtime: out of memory` throw, not a catchable
panic).

**Setup:** Hand-built 12-byte trailer only (`dirCount=0xFFFFFFFF`, `bodyLen=0`,
`magic=vcntFileMagic`) with zero actual directory bytes present, passed to `DecodeVCNTFile`.

**Assertions:** Returns a non-nil error; does not attempt the oversized allocation.

**Spec invariants tested:** SPEC-VC-2, SPEC-ROOT-001.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTFile_RejectsImpossibleDirCount`.
