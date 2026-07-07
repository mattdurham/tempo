# blockio/reader — Test Plan

This file documents the test coverage for the `blockio/reader` package.

---

## Overview

The `blockio/reader` package tests cover:
- Reader open and format version detection (V12, V13, V14)
- Block parsing: columnar, full, lazy decode (NOTE-001, NOTE-002)
- Cache behavior (NopCache, LRU)
- Block selection: bloom, range, trace index
- I/O coalescing: ReadGroup, ReadCoalescedBlocks
- Columnar read: ReadGroupColumnar, two-phase TOC
- Sketch index operations: HLL, TopK, bloom per column
- Concurrent column decode safety

---

## READER-TEST-001: Lazy Column Decode — Presence Only
**Function:** `TestLazyColumnDecode_PresenceOnlyCorrect`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** After ParseBlockFromBytes, `IsPresent()` correctly decodes the presence bitmap
from rawEncoding without triggering a full column decode.
**Spec invariant:** NOTE-001 — lazy column decode: presence on first IsPresent(), values on first accessor.

---

## READER-TEST-002: Lazy Column Decode — String Values
**Function:** `TestLazyColumnDecode_StringValue`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** StringValue() triggers full decode from rawEncoding on first call; subsequent calls
use the decoded dictionary without re-decoding.
**Spec invariant:** NOTE-001 — lazy column decode: values on first accessor call.

---

## READER-TEST-003: Lazy Column Decode — Uint64 Values
**Function:** `TestLazyColumnDecode_Uint64Value`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** Uint64Value() triggers full decode from rawEncoding on first call.
**Spec invariant:** NOTE-001 — lazy column decode: values on first accessor call.

---

## READER-TEST-004: ParseBlockFromBytes Lazy Column Registration
**Function:** `TestParseBlockFromBytes_LazyColumnsRegistered`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** After ParseBlockFromBytes with wantColumns, non-wanted columns are registered
lazily (rawEncoding set, no decode yet).
**Spec invariant:** NOTE-001 — lazy registration for columns not in wantColumns.

---

## READER-TEST-005: Concurrent IsPresent Safety
**Function:** `TestColumn_IsPresent_Concurrent`
**File:** `internal/modules/blockio/reader/concurrent_column_test.go`
**What it tests:** Concurrent calls to IsPresent() on the same Column are race-free; decodeOnce
(via decodeNow) ensures exactly one full decode; decoded atomic.Bool signals completion.
**Spec invariant:** NOTE-CONC-001 — decoded atomic.Bool is the cross-goroutine signal; NOTE-001 lazy decode must be concurrent-safe.

---

## READER-TEST-006: Concurrent expandDenseIdx Safety
**Function:** `TestColumn_expandDenseIdx_Concurrent`
**File:** `internal/modules/blockio/reader/concurrent_column_test.go`
**What it tests:** Concurrent calls to expandDenseIdx on the same Column are race-free.
**Spec invariant:** NOTE-001 — lazy decode must be concurrent-safe.

---

## READER-TEST-007: V14 Footer Constants
**Function:** `TestV14FooterConstants`
**File:** `internal/modules/blockio/reader/format_v14_test.go`
**What it tests:** FooterV7Version == 7 and FooterV7Size == 18; asserts the actual footer bytes
written to a V14 file match these constants.
**Spec invariant:** SPEC-V14-001 — V14 footer version must be 7, size 18 bytes.

---

## READER-TEST-008: V14 Column Decode Integrity
**Function:** `TestV14ColumnDecodeIntegrity`
**File:** `internal/modules/blockio/reader/format_v14_test.go`
**What it tests:** End-to-end: write V14 file, read it back, verify all column values match originals.
**Spec invariant:** SPEC-V14-001 — V14 per-column snappy encoding must round-trip correctly.

---

## READER-TEST-009: Bloom Access Without Block Read
**Function:** `TestBloomAccessNoBlockRead`
**File:** `internal/modules/blockio/reader/format_v14_test.go`
**What it tests:** Accessing sketch bloom data does not trigger a block read (bloom is in sketch section).
**Spec invariant:** SPEC-ROOT-013 — each section independently readable; bloom check must not load blocks.

---

## READER-TEST-010: FileSketchSummary Marshal Round-Trip
**Function:** `TestFileSketchSummary_MarshalRoundTrip`
**File:** `internal/modules/blockio/reader/file_sketch_summary_test.go`
**What it tests:** FileSketchSummary marshals and unmarshals correctly; all sketch fields preserved.
**Spec invariant:** Reader must be able to serialize sketch summaries for external callers.

---

## READER-TEST-011: FileSketchSummaryRaw Round-Trip
**Function:** `TestFileSketchSummaryRaw_RoundTrip`
**File:** `internal/modules/blockio/reader/file_sketch_summary_test.go`
**What it tests:** FileSketchSummaryRaw returns bytes that unmarshal to the original FileSketchSummary.
**Spec invariant:** The raw bytes API must be a faithful serialization of the computed summary.

---

## READER-TEST-012: Column Selective Read
**Function:** `TestColumnSelectiveRead`
**File:** `internal/modules/blockio/reader/format_v14_test.go`
**What it tests:** ReadGroupColumnar with a column filter only decodes requested columns.
**Spec invariant:** NOTE-001 — lazy decode: columns not in wantColumns are never decoded.

---

## READER-TEST-013: Lazy Column Compressed Before First Access
**Function:** `TestLazyColumn_CompressedBeforeFirstAccess`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** After GetBlockWithBytes with wantColumns, non-wanted columns have
compressedEncoding set (ColumnIsCompressed==true, IsDecoded==false). After the first
StringValue() call, the column is fully decoded and compressedEncoding is nil.
**Spec invariant:** SPEC-V14-002 — decompression deferred to first column access.

---

## READER-TEST-014: Lazy Column Never Decompressed If Unaccessed
**Function:** `TestLazyColumn_NeverDecompressedIfUnaccessed`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** A lazily-registered column that is never read remains in compressed
state (ColumnIsCompressed==true, IsDecoded==false) for the lifetime of the block.
**Spec invariant:** SPEC-V14-002 — non-wanted columns pay zero CPU/memory cost if never accessed.

---

## READER-TEST-015: Corrupt Compressed Column — Graceful Failure
**Function:** `TestLazyColumn_CorruptCompressedGraceful`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** A column whose compressedEncoding holds invalid snappy bytes does not
panic on first access. Value accessors return zero/false; IsPresent returns false; column
is not left in compressed state after the failed attempt.
**Spec invariant:** SPEC-V14-002 — decompression failure must be graceful (no panic); SPEC-ROOT-010 error observability.

---

## READER-TEST-016: Concurrent Lazy Decompression — Race Safety
**Function:** `TestLazyColumn_ConcurrentDecompress`
**File:** `internal/modules/blockio/reader/column_test.go`
**What it tests:** Ten goroutines concurrently call StringValue() on the same lazily-registered
V14 column. All goroutines see identical decoded values; no data race is reported by -race.
decompressOnce ensures exactly one snappy decode; decodeOnce serializes full column decode;
decoded atomic.Bool signals completion to concurrent callers.
**Spec invariant:** NOTE-CONC-001 — decoded atomic.Bool is the cross-goroutine signal; SPEC-V14-002 decompression is concurrent-safe.

---

## READER-TEST-017: Removed InlineBytes Kinds Rejected
**Function:** `TestReadColumnEncoding_RejectsRemovedInlineBytesKinds`
**File:** `internal/modules/blockio/reader/encoding_removed_kinds_test.go`
**What it tests:** A column blob naming `shared.KindInlineBytes` (3) or
`shared.KindSparseInlineBytes` (4) now errors at decode time (`"column encoding: unknown kind
%d"`) instead of decoding — issue #490, task A-14/#108 removed these reader-only legacy decode
arms since the current writer never emits them and the project-wide stored-data wipe retires any
file that could still name them.
**Spec invariant:** reader NOTE-490 (issue #490).

---

## READER-TEST-018: Legacy VectorF32 Decode Removed
**File:** `internal/modules/blockio/reader/legacy_vectorf32_test.go` — **DELETED (issue #490, task
A-6/#100, 2026-07-07)**
**What changed:** this file's entire test suite exercised `decodeVectorF32`/`Column.VectorF32Value`
(deleted). A block still physically carrying a VectorF32 column now hard-errors at decode time
instead of decoding gracefully — see writer NOTE-480's addendum for the full deliberate
behavior-change rationale. No replacement negative-case test was added under this file's own
convention since the removed dispatch arm falls through to the same pre-existing "unknown kind"
error path documented by READER-TEST-017 (same failure mode, different removed kind).
**Spec invariant:** writer/NOTES.md NOTE-480 (addendum, issue #490).
