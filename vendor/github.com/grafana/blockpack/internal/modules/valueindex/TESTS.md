# valueindex — Test Specifications

This document defines the required tests for the `internal/modules/valueindex` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`). IDs are assigned in
ascending order and never reused or renumbered.

Next free ID: **TEST-VI-13**.

---

## TEST-VI-1: BucketFileIterator single-block walk
*Added: 2026-07-02*

**Scenario:** A `BucketFileIterator` over a single-block `BucketFile` yields every group in
that block's stored order; an empty file is immediately exhausted.

**Setup:** `TestBucketFileIterator_SingleBlock` constructs a one-block `BucketFile` with
several groups and drains the iterator, asserting the yielded sequence matches the block's
`Groups` slice exactly. `TestBucketFileIterator_SingleBlock_EmptyFile` constructs a
`BucketFile` with zero blocks/groups and asserts the iterator reports exhausted immediately.

**Assertions:** Yielded group order matches input order; empty file exhausts with no panic
and no spurious yield.

**Spec invariants tested:** SPEC-VI-1.

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestBucketFileIterator_SingleBlock`,
`TestBucketFileIterator_SingleBlock_EmptyFile`.

---

## TEST-VI-2: BucketFileIterator cross-block ordering, including empty middle blocks
*Added: 2026-07-02*

**Scenario:** A `BucketFileIterator` walking a multi-block file trusts block order without
re-sorting, and transparently skips an empty block in the middle of the sequence rather than
treating it as end-of-iteration.

**Setup:** `TestBucketFileIterator_CrossBlockOrder` constructs a multi-block `BucketFile`
whose blocks are already globally ordered (per SPEC-VI-1) and asserts the iterator's yielded
sequence is the concatenation of each block's groups in block order, with no re-sort applied.
`TestBucketFileIterator_CrossBlockOrder_SkipsEmptyMiddleBlock` inserts an empty `BucketBlock`
between two non-empty blocks and asserts the iterator advances past it and continues yielding
groups from the following block.

**Assertions:** Cross-block order is preserved as-is (no re-sort); an empty middle block does
not terminate iteration early.

**Spec invariants tested:** SPEC-VI-1.

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestBucketFileIterator_CrossBlockOrder`,
`TestBucketFileIterator_CrossBlockOrder_SkipsEmptyMiddleBlock`.

---

## TEST-VI-3: StreamCompactBucketFiles matches non-streaming path on disjoint keys
*Added: 2026-07-02*

**Scenario:** For inputs with no overlapping `(time_sec, value)` keys across files, the
streaming path's output is equivalent to the non-streaming `CompactBucketFiles` path.

**Setup:** `TestStreamCompactBucketFiles_MatchesNonOverlapping` builds multiple input
`BucketFile`s with disjoint keys, runs both `CompactBucketFiles` (old) and
`StreamCompactBucketFiles` (new) over equivalent inputs, and compares the resulting groups,
refs, and spans for equality (order-independent comparison — group/ref/span sets, not byte
identity, since output block cutting and string table interning order may legitimately
differ).

**Assertions:** The two paths produce the same logical set of groups/refs/spans for
non-overlapping input.

**Spec invariants tested:** SPEC-VI-2.

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestStreamCompactBucketFiles_MatchesNonOverlapping`.

---

## TEST-VI-4: StreamCompactBucketFiles unions refs and spans on overlapping keys
*Added: 2026-07-02*

**Scenario:** Overlapping input across files is merged with the same union rules as
`MergeBucketFiles`: different-source entries under the same `(time_sec, value)` key have
their refs unioned; same-`(sourcePath, page, TraceID)` entries with different row indexes have
their span indexes unioned.

**Setup:** `TestStreamCompactBucketFiles_UnionsRefsAndSpans` ports the scenarios of
`TestMergeBucketFilesUnionsRefs` (different-source, same-key → refs union) and
`TestCompactBucketFiles_MergesGroups` (same source/block/trace, different row index → span
indexes union) through the streaming path.

**Assertions:** Ref union and span-index union behavior is identical to the non-streaming
path for both scenarios.

**Spec invariants tested:** SPEC-VI-2.

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestStreamCompactBucketFiles_UnionsRefsAndSpans`.

---

## TEST-VI-5: StreamCompactBucketFiles output block cutting and per-block metadata
*Added: 2026-07-02*

**Scenario:** Merged output exceeding `groupsPerBlock` is cut into `ceil(N/groupsPerBlock)`
blocks with correctly recomputed per-block min/max time/value metadata, and no group is split
across a block boundary.

**Setup:** `TestStreamCompactBucketFiles_GroupsPerBlockCutting` merges 3 input files whose
combined group count N exceeds a configured `groupsPerBlock`, and inspects the resulting
output file's block structure.

**Assertions:** Block count equals `ceil(N/groupsPerBlock)`; each block's recomputed min/max
time/value metadata matches its actual group contents; every group appears in exactly one
block (no splits, no duplication, no loss).

**Spec invariants tested:** SPEC-VI-2.

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestStreamCompactBucketFiles_GroupsPerBlockCutting`.

---

## TEST-VI-6: StreamCompactBucketFiles edge cases
*Added: 2026-07-02*

**Scenario:** Table/subtest covering degenerate inputs: empty iterator set, a context that is
already canceled at call time, and an all-refs-dead retention filter.

**Setup:** `TestStreamCompactBucketFiles_EdgeCases` runs subtests for: (a) no iterators
supplied, (b) a pre-canceled `context.Context`, and (c) a `RefChecker` that reports every ref
as dead (mirroring the "all refs dead" pre-filter edge case, see NOTE-VI-046).

**Assertions:** (a) empty iterators → `output` is never called, no error; (b) canceled
context → returns `ctx.Err()` promptly, `output` is never called; (c) all-refs-dead → behaves
identically to the empty-iterators case (no output call, no error).

**Spec invariants tested:** SPEC-VI-2 (output-called-at-most-once / not-at-all semantics).

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestStreamCompactBucketFiles_EdgeCases`.

---

## TEST-VI-7: TimeSec is floored to the minute, including across a minute boundary
*Added: 2026-07-02*

**Scenario:** `buildSpanStartSecByRef` floors `span:start` to whole 60-second (minute)
boundaries, not whole seconds — verified both for sub-minute spans (which collapse into a
single bucket) and for spans straddling a minute boundary (which must land in distinct,
correctly-floored buckets, proving floor behavior rather than incidental coarsening).

**Setup:**
- `TestExtractValueIndexEntries_TimeSec` (updated for this change) writes spans starting at
  2s, 3s, and 4s — all inside epoch minute 0 — via `writeBlockpackForExtract`, and asserts via
  `collectEntries` that every resulting `byCol["span:name"]` entry's `TimeSec` is exactly `0`
  (all three sub-minute spans collapse to one bucket). This does not conflict with
  `TestExtractValueIndexEntries_TimeColumnsTruncatedToMillis`, which asserts an unrelated,
  separate truncation (`truncateTimeValueToMillis`, NOTE-VI-027) on the same fixture's values.
- `TestExtractValueIndexEntries_TimeSec_MinuteBoundary` (new) builds a dedicated blockpack
  (bypassing `writeBlockpackForExtractCfg`'s shared sub-minute fixture) with three spans at
  58s, 61s, and 122s (`58_000_000_000`/`61_000_000_000`/`122_000_000_000` ns), one span per
  `AddTracesData` call, and asserts via `collectEntries` that `byCol["span:name"]`'s `TimeSec`
  values are exactly `{0, 60, 120}` — the pre-fix per-second `buildSpanStartSecByRef` would
  have produced `{58, 61, 122}` instead, so this is the actual regression test proving
  minute-flooring (not just some coarsening) across a boundary.

**Assertions:** Sub-minute spans within the same minute collapse to one `TimeSec` bucket
(value `0` for the first-minute case); spans in different minutes floor to their own minute's
start-of-minute second value (`0`, `60`, `120` respectively), never the raw per-second value.

**Spec invariants tested:** SPEC-VI-4.

Back-ref: `valueindex_extract_test.go:TestExtractValueIndexEntries_TimeSec`,
`valueindex_extract_test.go:TestExtractValueIndexEntries_TimeSec_MinuteBoundary`.

**Not affected by this change (confirmed, no update needed):**
`valueindex_e2e_test.go`'s `TestValueIndexE2E_TimeRangeFilename` and
`TestValueIndexE2E_CountOverTime` construct `TimeSec` values directly via
`valueindex.Writer.AddEntryV4` and never call `ExtractValueIndexEntries`/
`buildSpanStartSecByRef`, so this write-side truncation change has no effect on either test.

---

## TEST-VI-8: NewDiskBucketFileIterator basic walk (single-block, cross-block, empty file)
*Added: 2026-07-03*

**Scenario:** `diskBucketFileIterator` (the disk-backed `GroupIterator`) yields the same group
sequence as the in-memory `BucketFileIterator` over equivalent input, for single-block,
multi-block, and zero-block files — mirroring TEST-VI-1/TEST-VI-2's existing in-memory
coverage at the new disk-backed construction path.

**Setup:** `TestNewDiskBucketFileIterator_SingleBlock`, `_CrossBlockOrder`, and `_EmptyFile`
(`disk_iterator_test.go`) each encode a fixture `BucketFile` to bytes via `EncodeBucketFile`,
write it to a file under `t.TempDir()`, construct a `diskBucketFileIterator` via
`NewDiskBucketFileIterator`, and drain it via `Peek`/`Advance(ctx)`.

**Assertions:** The yielded group sequence is identical to the in-memory iterator's sequence
over the same input; an empty (zero-block) file constructs successfully but is immediately
exhausted (`Peek()` returns `(nil, false)` right away, matching `NewBucketFileIterator`'s
existing empty-file behavior).

**Spec invariants tested:** SPEC-VI-1 (ordering is preserved regardless of storage backing).

Back-ref: `internal/modules/valueindex/disk_iterator_test.go:TestNewDiskBucketFileIterator_SingleBlock`,
`TestNewDiskBucketFileIterator_CrossBlockOrder`, `TestNewDiskBucketFileIterator_EmptyFile`.

---

## TEST-VI-9: NewDiskBucketFileIterator corruption handling (nil-interface trap, legacy skip, corrupt-block abort)
*Added: 2026-07-03*

**Scenario:** The corruption-handling split documented in `NOTES.md` NOTE-VI-053: a header
magic mismatch is treated as a legacy file (skip, not abort, via a genuine nil `GroupIterator`
interface value — never a typed-nil pointer leaking through the interface, the nil-interface
trap this repo watches for as a recurring bug class); any other decode failure (including a
corrupted block payload discovered lazily during `Advance`) is a real error that aborts.

**Setup:**
- `TestNewDiskBucketFileIterator_LegacyFileReturnsTrueNilInterface` constructs against a file
  with a bad header magic and asserts the returned `GroupIterator == nil` compares true (only
  possible for a genuine nil interface, not a typed-nil struct pointer).
- `TestNewDiskBucketFileIterator_HeaderMagicMismatchSkipsNotAborts` asserts the same bad-magic
  case returns `(nil, nil)` — no error — confirming "skip" semantics, not "abort" semantics.
- `TestNewDiskBucketFileIterator_CorruptBlockAborts` corrupts a compressed block's payload
  (header/footer/string-table/block-index all valid) and asserts `Advance(ctx)` crossing into
  that block sets a non-nil `Err()`; also exercises Edge Case 5 (calling `Advance(ctx)` again
  on an already-errored iterator is a no-op — `Err()` is unchanged, no panic, no further I/O).

**Assertions:** Legacy header-magic mismatch → `(nil, nil)`, no error, true nil interface;
any other decode failure (including a lazily-discovered corrupt block) → non-nil error,
surfaced via the constructor's error return (construction-time failures) or `Err()`
(mid-iteration failures); an already-errored iterator's `Advance` is a safe no-op.

**Spec invariants tested:** SPEC-VI-5 (`Err()` surfaces all fallible `Advance` work; `Peek`
stays I/O-free and error-free even on the corrupted-but-not-yet-discovered path).

Back-ref: `internal/modules/valueindex/disk_iterator_test.go:TestNewDiskBucketFileIterator_LegacyFileReturnsTrueNilInterface`,
`TestNewDiskBucketFileIterator_HeaderMagicMismatchSkipsNotAborts`,
`TestNewDiskBucketFileIterator_CorruptBlockAborts`.

---

## TEST-VI-10: filterDeadRefsBlock matches whole-file filtering exactly
*Added: 2026-07-03*

**Scenario:** The per-block retention-filter refactor (NOTE-VI-053, `bucketmerge.go`'s
`filterDeadRefs` now calls `filterDeadRefsBlock` once per block with one shared cache) must
produce byte-identical retain/drop decisions and identical `checker.IsLive` call counts as the
original whole-file evaluation — including when a single `SourceID` is repeated across
multiple blocks in one file (the case that actually exercises the persistent `live` cache's
reuse behavior).

**Setup:** `TestFilterDeadRefsBlock_MatchesWholeFileFiltering` (`disk_iterator_test.go`)
builds a multi-block `BucketFile` with a mix of live/dead `SourceID`s spread across blocks,
some repeated across blocks, then runs both the whole-file `filterDeadRefs` and the new
per-block `filterDeadRefsBlock` (called once per block with a shared `live` map) against a
call-counting fake `RefChecker`.

**Assertions:** Retained/dropped results are byte-identical between the two approaches;
`checker.IsLive` is called the same number of times by both (at most once per distinct
`SourceID` per file, regardless of block granularity).

**Spec invariants tested:** none new — this is a behavior-preservation proof for an internal
refactor, not a new invariant (see NOTES.md NOTE-VI-053's "filterDeadRefs refactor" section).

Back-ref: `internal/modules/valueindex/disk_iterator_test.go:TestFilterDeadRefsBlock_MatchesWholeFileFiltering`.

---

## TEST-VI-11: StreamCompactBucketFiles disk-backed output matches in-memory output
*Added: 2026-07-03*

**Scenario:** The output-side disk-staging change (SPEC-VI-2's Addendum, NOTE-VI-053 — a local
temp file + buffered writer instead of an in-memory `body []byte` accumulator) must produce
byte-identical output to the original in-memory assembly path, for the same logical input.

**Setup:** `TestStreamCompactBucketFiles_DiskBackedMatchesInMemoryOutput`
(`stream_compaction_test.go`) runs the same fixture inputs through `StreamCompactBucketFiles`
and compares the resulting output file's bytes against a golden output captured from the
pre-disk-staging (in-memory `body []byte`) implementation.

**Assertions:** The two outputs are byte-identical.

**Spec invariants tested:** SPEC-VI-2 (merge-semantics equivalence, including the Addendum
noting the callback signature change from `func([]byte) error` to `func(path string) error`).

Back-ref: `internal/modules/valueindex/stream_compaction_test.go:TestStreamCompactBucketFiles_DiskBackedMatchesInMemoryOutput`.

---

## TEST-VI-12: SweepOrphanedMergeTempFiles removes only its own naming convention
*Added: 2026-07-03*

**Scenario:** The startup sweep (NOTE-VI-053) must remove only `vi-merge-*.tmp` files, leaving
`runspill.go`'s `vi-run-*.tmp` files and any unrelated file untouched, and must tolerate a
missing/unreadable target directory without erroring.

**Setup:**
- `TestSweepOrphanedMergeTempFiles_RemovesOnlyMatchingPrefix` creates `vi-merge-*.tmp`,
  `vi-run-*.tmp`, and an unrelated file in one directory, runs the sweep against it, and
  asserts only the `vi-merge-*.tmp` files were removed.
- `TestSweepOrphanedMergeTempFiles_ToleratesMissingDir` points the sweep at a nonexistent
  directory and asserts `(0, nil)`, not an error.
- `TestSweepOrphanedMergeTempFiles_ExportedWrapperUsesOSTempDir` confirms the exported
  `SweepOrphanedMergeTempFiles()` wrapper delegates to `os.TempDir()` (the production target),
  distinct from the parameterized inner implementation the other two tests exercise directly
  for testability.

**Assertions:** Only matching-prefix files are removed; a missing directory is tolerated, not
an error; the exported wrapper targets the real process temp directory.

**Spec invariants tested:** none new — this documents `SweepOrphanedMergeTempFiles`'s
contract, described in `NOTES.md` NOTE-VI-053, not a formal SPECS.md invariant.

Back-ref: `internal/modules/valueindex/temp_cleanup_test.go:TestSweepOrphanedMergeTempFiles_RemovesOnlyMatchingPrefix`,
`TestSweepOrphanedMergeTempFiles_ToleratesMissingDir`,
`TestSweepOrphanedMergeTempFiles_ExportedWrapperUsesOSTempDir`.
