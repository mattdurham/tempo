# valueindex — Test Specifications

This document defines the required tests for the `internal/modules/valueindex` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`). IDs are assigned in
ascending order and never reused or renumbered.

Next free ID: **TEST-VI-23**.

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

---

## TEST-VI-13: TestWriter_ReturnsErrorOnLegacyV1EncodeAttempt
*Added: 2026-07-07*

**Scenario:** issue #490, task A-8/#102 — write support for V1 (pre-BlockRef) entries is
retired; `writer.go`'s `assemble()` must return a typed error rather than silently encoding a
V1-shaped VINX section.

**Setup:** An `AddEntry()`-only (v1 API) batch — at least one entry, none carrying a v2+
`BlockRef`/`SpanID` (`seen && !anyBlockRef`) — flushed via the writer.

**Assertions:** `Flush()` returns a non-nil error matching
`"valueindex: legacy pre-BlockRef entries unsupported — data was not fully migrated as assumed
(see NOTES.md NOTE-VI-014)"`. A zero-entry batch is unaffected — it still succeeds via the
empty-file encoding path, now tagged `shared.ValueIndexEntriesVersion` (V2) rather than V1
(task #116's fix).

**Spec invariants tested:** NOTE-VI-014 (addendum).

Back-ref: `internal/modules/valueindex/writer_test.go:TestWriter_ReturnsErrorOnLegacyV1EncodeAttempt`.

---

## TEST-VI-14: TestDecodeTraceGroups_ImplausibleGroupCountRejected (rewritten against v2 decoder)
*Added: 2026-07-07*

**Scenario:** issue #490, task A-2/#97 — the pre-allocation `groupCount` bounds check for
`DecodeTraceGroups` must still be exercised after the legacy v1 flat-blob decode path was
removed (NOTE-VI-079). This test previously built its corrupted-`groupCount` fixture using the
legacy v1 encoding; it is rewritten to call `decodeTraceBlockBody` (the v2 per-block decoder)
directly with a corrupted `groupCount`, preserving the same regression coverage against the
current-format code path.

**Setup:** A hand-built v2 per-block payload with a `groupCount` field set to an implausible
value relative to the remaining payload bytes.

**Assertions:** `decodeTraceBlockBody` returns a non-nil error; does not attempt an
oversized allocation.

**Spec invariants tested:** NOTE-VI-079, SPEC-ROOT-001.

Back-ref: `internal/modules/valueindex/traceindex_test.go:TestDecodeTraceGroups_ImplausibleGroupCountRejected`.

**Also superseded by NOTE-VI-079 (issue #490, task A-2/#97):** `TestTraceV2_LegacyBlobStillDecodes`
and its `makeLegacyTraceBlob` fixture (`traceindexquery_test.go`) — deleted, no replacement
needed (the legacy blob format itself is retired, not merely a decode variant to keep testing).
`TestFilenameV1Compatibility` (`filename_v2_test.go`) — superseded by
`TestParseFilenameV2_RejectsV1Filename` (issue #490, task A-5/#99, see NOTES.md NOTE-VI-037
addendum). `TestDiscoverIndexFiles_V1FilenameAlwaysIncluded` (`discovery_test.go`) — superseded
by `TestDiscoverIndexFiles_V1FilenameSkipped` (same task).

---

## TEST-VI-15: `ReadBucketFileMetadata` matches `disk_iterator.go`'s prior behavior
*Added: 2026-07-07*

**Scenario:** The extracted, store-agnostic `ReadBucketFileMetadata` (SPEC-VI-8) must decode a
v2 `BucketGroup` file's footer, string table, and block directory identically to
`disk_iterator.go`'s pre-extraction `readBucketFileMetadata`, now that the latter is a thin
`*os.File`-adapter wrapper over the former.

**Setup:** `TestReadBucketFileMetadata_MatchesDiskIteratorBehavior`
(`bucketfile_metadata_test.go`) builds a multi-block `BucketFile`, writes it to a temp file,
and compares `ReadBucketFileMetadata`'s output (via a `RangedSource` wrapping the file) against
the pre-existing disk-iterator code path's own decode of the same file.

**Assertions:** decoded footer, block directory, and string table are identical between the two
call paths; no regression in `NewDiskBucketFileIterator`'s own signature/behavior.

## TEST-VI-16: `blockExcludedByValue` — ruling-14 scoped value-range pruning
*Added: 2026-07-07*
*Updated: 2026-07-07 — added the eq-branch comparator-disagreement case (see below).*

**Scenario:** `blockExcludedByValue` (SPEC-VI-9) must prune correctly for equality predicates
(any column type) and range/between predicates on non-numeric types, and must NEVER prune a
range/between predicate on a numeric type — the deliberate capability boundary ruling 14
establishes. Two DISTINCT regression tests are required to cover the comparator-choice
question for numeric types, because the eq-predicate branch and the range/between branches
reach the comparator differently (see the correction below).

**Setup (table-driven, `blockexcluded_test.go`):**
- `TestBlockExcludedByValue_Equality` — string/bytes equality predicate outside a block's
  `[minValue, maxValue]` is excluded; inside is not.
- `TestBlockExcludedByValue_EqualityNumeric` — equality prune also applies to numeric column
  types (self-consistent regardless of byte-order semantics). Uses values kept below 256 so
  LE byte order happens to match numeric order, keeping the test's own reasoning simple — this
  test does NOT exercise the case where `compareCanonicalBytes` and `compareCanonical`
  disagree; that is `TestBlockExcludedByValue_EqualityNumericDisagreement`'s job (see below).
- `TestBlockExcludedByValue_EqualityNumericDisagreement` — **the required regression test for
  the ONE branch where the `compareCanonicalBytes`-vs-`compareCanonical` choice is actually
  live for a numeric column type: `eqPredicate` has no `isNumericColType` gate** (per ruling
  14, equality is sound with either comparator applied consistently, but only if
  `compareCanonicalBytes` — the comparator the write side actually used to build the directory
  bounds — really is the one used). `minValue`/`maxValue` are laid out exactly as write-time
  `compareCanonicalBytes` would order them for a block containing both `uint64(255)` and
  `uint64(256)` (`canon(256)`'s bytes lexicographically precede `canon(255)`'s — the classic
  disagreement case, NOTE-VI-011); an eq predicate for `256` (a value genuinely present in such
  a block) must NOT be excluded. Added by a reviewer finding (task #138) after
  `TestBlockExcludedByValue_NumericRangeNeverPruned`'s own doc comment was found to
  incorrectly claim this disagreement case was already covered by it — a mutation test (eq
  branch temporarily swapped to `compareCanonical`) confirmed the full suite passed without
  this new test, proving the gap was real, not just theoretical. `TestBlockExcludedByValue_
  EqualityNumeric`'s doc comment was corrected in the same pass to point at this test instead
  of misattributing the coverage to `TestBlockExcludedByValue_NumericRangeNeverPruned`.
- `TestBlockExcludedByValue_RangeNonNumeric` / `TestBlockExcludedByValue_BetweenNonNumeric` —
  range/between predicates on string/bytes/UUID types prune correctly using
  `compareCanonicalBytes`.
- `TestBlockExcludedByValue_NumericRangeNeverPruned` — the required regression test for the
  range/between branches: a numeric-typed (e.g. `uint64`) range/between predicate over a block
  whose directory bounds would, under a naive lex-byte read, appear to exclude the predicate's
  target value (the classic `uint64(255)` vs `uint64(256)` LE-byte-ordering disagreement,
  NOTE-VI-011) must NOT be pruned — asserts `blockExcludedByValue` returns `false`
  unconditionally for numeric range/between, proving no false-negative prune occurs. **Note:**
  this test proves the `isNumericColType` gate exists and short-circuits before either
  comparator is ever called for a numeric range/between predicate — it does NOT, by itself,
  prove anything about comparator correctness on the eq-predicate branch (that is
  `TestBlockExcludedByValue_EqualityNumericDisagreement`'s exclusive job, per the correction
  above).
- `TestBlockExcludedByValue_UnprunablePredicatesNeverExclude` — neq/regex/nil predicates always
  return `false` (cannot decide, don't prune; fall through to `matchGroupsInBlock`'s per-group
  TimeSec check + `pred.Match` — no bloom filter is consulted anywhere in this call chain,
  NOTE-VI-082).

**Assertions:** every table case matches SPEC-VI-9's scope table exactly; the numeric-range
regression case in particular must never report `true` (excluded) regardless of directory bound
values, since doing so would be a silent false-negative correctness bug, not merely a missed
optimization; the eq-branch disagreement case must never report `true` (excluded) for a value
genuinely present in the block, confirmed to fail (catching the regression) under a mutation
test that swaps the eq branch's comparator to `compareCanonical`.

## TEST-VI-17: `QueryBucketFileRanged` — partial-read parity with `QueryBucketFiles` and pruning behavior
*Added: 2026-07-07*

**Scenario:** `QueryBucketFileRanged` (SPEC-VI-10) must (a) prune at the file level using only a
footer read, (b) prune at the block level using directory metadata without reading pruned
blocks' bodies, (c) produce results identical to `QueryBucketFiles` for the same file/predicate/
time-range, (d) treat a non-bucket-file as a skip rather than an error, (e) support nil
(match-all) predicates, and (f) respect `ctx` cancellation at function entry (before any read),
after the metadata read (before the per-block loop), and once per directory entry within the
per-block loop.

**Setup (`bucketquery_ranged_test.go`), using a `countingRangedSource` (or equivalent) fake that
records exact `(off, len)` `ReadAt` ranges, not just call counts:**
- `TestQueryBucketFileRanged_TimeExcludedFile_FooterOnly` — a file whose footer time range
  doesn't overlap the query window issues exactly one `ReadAt` (the footer) and returns
  `(nil, nil)`.
- `TestQueryBucketFileRanged_ValueExcludedByAllBlocks_FooterAndDirOnly` — every block excluded
  by `blockExcludedByValue` issues footer + directory reads only, zero block-body reads.
- `TestQueryBucketFileRanged_MatchesQueryBucketFiles_MultiBlock` — a multi-block file queried
  both ways (whole-file `QueryBucketFiles` vs. ranged `QueryBucketFileRanged`) produces
  byte-identical `[]LookupResult` sets.
- `TestQueryBucketFileRanged_NotABucketFile_SkippedNotError` — bad-magic/too-short input returns
  `(nil, nil)`, not an error (NOTE-VI-083's `ErrNotBucketFile` wrapping enables this).
- `TestQueryBucketFileRanged_TooShortToHoldFooter_SkippedNotError` — data shorter than a fixed
  footer is classified the same way as a bad-magic footer (both routes through
  `ErrNotBucketFile`).
- `TestQueryBucketFileRanged_NilPredicate_MatchAll` — a nil predicate matches every group
  (mirrors `QueryBucketFiles`' own nil-predicate contract).
- `TestQueryBucketFileRanged_ContextCancelledStopsEarly` — a `ctx` canceled before the call is
  checked at function entry, before the footer read: it returns a wrapped `ctx.Err()` and issues
  zero `ReadAt` calls, not just an early-but-nonzero-cost cancellation.

**Assertions:** each test's named behavior holds exactly; existing `QueryBucketFiles` tests
remain green, unmodified in their own assertions, confirming the `matchGroupsInBlock` extraction
(NOTE-VI-081) introduced no behavior change to the pre-existing whole-file path.

## TEST-VI-18: `QueryBucketFileRanged` file-level prune matches `QueryBucketFiles` at the SPEC-VI-4 minute-flooring edge
*Added: 2026-07-07*

**Scenario:** required B-4 review sign-off regression proving `QueryBucketFileRanged`'s new
file-level footer prune (which `QueryBucketFiles` has no equivalent of — it never checks
file-level bounds, only per-block) applies SPEC-VI-4's no-internal-flooring, caller-trusts
contract identically to `QueryBucketFiles`, rather than silently diverging with some
ranged-path-only flooring rule.

**Setup:** `TestQueryBucketFileRanged_MinuteFlooringParity_FileLevel`
(`bucketquery_ranged_test.go`) builds a single-block fixture with a write-side-floored
`TimeSec=960` (representing a true event time anywhere in `[960,1019]`), then queries both
`QueryBucketFiles` and `QueryBucketFileRanged` with (a) a raw, un-floored `minTS=995` — strictly
between the floored `TimeSec` and the next minute boundary, the exact edge a caller who fails
to floor could trip — and (b) a correctly-floored `minTS=960`.

**Assertions:** for both `minTS` values, `QueryBucketFileRanged`'s result equals
`QueryBucketFiles`' result exactly (`require.Equal(t, whole, ranged, ...)`); case (a) excludes
the entry in both paths (proving neither path silently includes it via some different flooring
rule); case (b) includes the entry in both paths (proving the parity holds on the
"should include" side too, not just the exclusion side).

## TEST-VI-19: `QueryBucketFileRanged` per-block prune matches `QueryBucketFiles` at the SPEC-VI-4 minute-flooring edge
*Added: 2026-07-07*

**Scenario:** the per-block analog of TEST-VI-18 — isolates the dir-level time check
(`d.MaxTimeSec < minTS || d.MinTimeSec > maxTS`) from the file-level check by using a 2-block
fixture whose overall footer time range overlaps the query window (so the file-level prune
never fires), proving the per-block prune independently applies the same
no-internal-flooring, caller-trusts contract `QueryBucketFiles` already has per block.

**Setup:** `TestQueryBucketFileRanged_MinuteFlooringParity_BlockLevel`
(`bucketquery_ranged_test.go`) builds two blocks — one with `TimeSec=960` (floored; a raw
`minTS=995` falls strictly after this), one with `TimeSec=1020` (a different, later minute
boundary, not excluded by `minTS=995`) — and compares `QueryBucketFileRanged` against
`QueryBucketFiles` at the same window-edge `minTS` values as TEST-VI-18.

## TEST-VI-20: `readBucketFileTail` rejects overflowing footer offsets (NOTE-VI-046, ranged path parity)
*Added: 2026-07-07*

**Scenario:** the shared metadata helper (`readBucketFileTail`, SPEC-VI-8) must reject a corrupt
footer whose `StringTableOff`/`BlockIndexOff` are individually validated against the file size
*before* being summed with their paired length — the overflow-safe pattern `DecodeBucketFile`
already established (`bucketfile.go:DecodeBucketFile`, NOTE-VI-046). A naive `off+len > size`
check is insufficient: a huge `off` paired with a small `len` can wrap `uint64` and slip back
below `size`, passing a sum-first check while `off` itself is nonsensical.

**Setup:** `TestReadBucketFileTail_OverflowingFooterOffsetsRejected`
(`bucketfile_metadata_test.go`) constructs a `BucketFooter` with `StringTableOff =
math.MaxUint64-5, StringTableLen = 10` against a 100-byte `countingRangedSource` and calls
`readBucketFileTail` directly.

**Assertions:** the call returns a non-nil error; `src.reads` is empty — the bounds check runs
before any `ReadAt` is issued, so a corrupt footer cannot even trigger a wasted string-table/
block-index fetch, let alone a negative-offset `ReadAt` or an oversized allocation.

## TEST-VI-21: block-directory `CompOff`/`CompLen` bounds check rejects corrupt entries on both the ranged and disk read paths (NOTE-VI-046)
*Added: 2026-07-07*

**Scenario:** `readBucketFileTail` validates every decoded `BlockDirEntry`'s `CompOff`/`CompLen`
against the string-table offset (the same "region that can legally contain block bodies" bound
`DecodeBucketFile` already checks via `end > strOff`, `bucketfile.go:DecodeBucketFile`) before
returning the directory to either consumer — `bucketquery_ranged.go`'s `readAndDecodeBlockRanged`
(ranged/S3 path) and `disk_iterator.go`'s `decodeBlockAt` (disk/compaction path) — so neither ever
sees an unvalidated entry, closing a possible large-allocation DoS vector on a corrupted or
truncated directory entry.

**Setup, ranged path (`bucketquery_ranged_test.go`)**, using `blockDirEntryByteOffsets`
(`bucketfile_metadata_test.go`) to locate and overwrite a directory entry's `CompOff`/`CompLen`
bytes on an otherwise-valid encoded file:
- `TestQueryBucketFileRanged_CorruptDirEntryCompLenRejected` — `CompLen` corrupted to
  `math.MaxUint32`.
- `TestQueryBucketFileRanged_CorruptDirEntryOffsetOverflowRejected` — `CompOff` corrupted to
  `math.MaxUint64-5` with `CompLen = 10`, the overflow-wraps-below-bound case.

**Setup, disk path (`disk_iterator_test.go`)**, the same two corruption shapes applied to a
file passed to `NewDiskBucketFileIterator`:
- `TestNewDiskBucketFileIterator_CorruptDirEntryCompLenRejected`
- `TestNewDiskBucketFileIterator_CorruptDirEntryOffsetOverflowRejected`

**Assertions:** every case returns a non-nil error (construction fails for the disk path;
`QueryBucketFileRanged` returns an error for the ranged path) rather than panicking or attempting
an oversized allocation; the ranged-path tests additionally assert, via `countingRangedSource`,
that no `ReadAt` is ever issued for the bogus entry's original `CompOff` (CompLen case) or with
the corrupted `CompLen` as its length (overflow case).

**Assertions:** `QueryBucketFileRanged`'s per-block dir-level prune produces results identical
to `QueryBucketFiles`' per-block check at every tested `minTS`, confirming the dir-level check
is the algebraic negation of `BucketBlock.OverlapsTimeRange` applied to the same fields and
comparators, with no additional or missing flooring logic introduced by the ranged path.

---

## TEST-VI-22: Mandatory real-write-path end-to-end round trip for any new VI-consuming index-path feature (issue #489, task #12 post-mortem)

**Policy, not a single test case.** Any new feature that consumes `valueindex.LookupResult`/`VILookupResult` for candidate discovery, joining, or identity purposes MUST ship with at least one end-to-end test that exercises the REAL write path — `Writer.AddEntryV2`/`AddEntryV4` (or the higher-level `WriteValueIndexL0`) producing the actual index bytes a query then reads back — not a hand-constructed `LookupResult`/`VILookupResult` fixture literal.

**Why this is now mandatory, not a nice-to-have.** Issue #489's entire D1-D7 test suite (dozens of tests, "9/9 gates passing") missed a CRITICAL silent-wrong-answer defect (task #12: `ExecuteStructuralFromIndex`'s L-match join keyed on `SpanID`, always zero for attribute entries against the real write path, NOTE-VI-094) because every fixture in that suite hand-constructed its `LookupResult`/`VILookupResult` values directly, and every hand-constructed fixture happened to set `SpanID` to a real value — an assumption the real write path never actually satisfies for attribute columns. The defect was caught only by coder-d3's first test that went through the genuine `WriteValueIndexL0` → search-VI → engine round trip. A fixture-only test suite structurally cannot catch a write-path-vs-fixture mismatch of this shape, no matter how many fixture-based scenarios it adds.

**Scope:** applies to any future feature (not just #489's structural queries) built on top of a `ValueIndexSource`/`LookupResult`/`VILookupResult`-consuming discovery or join mechanism — count/rate metrics, search, structural, or any not-yet-designed VI-driven engine.

**Setup:** `CreateBlock` (or equivalent) → write real spans → `WriteValueIndexL0` (the real write path, not a hand-built `LookupResult`) → run the actual VI query function that produces `LookupResult`/`VILookupResult` values → feed those real results into the feature under test.

**Assertion:** the feature under test produces the expected non-empty match set from realistically-written data — specifically exercising at least one ORDINARY attribute-column predicate (not only the `span:id`/`trace:id` sentinel columns), since those are the columns where `SpanID` is unpopulated and any accidental SpanID-keyed join would silently fail.

See EX-36 (`internal/modules/executor/TESTS.md`) for D4/D6's own instance of this new test class. Issue #489, task #12.
