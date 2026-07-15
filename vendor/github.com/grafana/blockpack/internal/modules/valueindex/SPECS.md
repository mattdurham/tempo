# valueindex — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valueindex` package. It complements `NOTES.md` (design rationale) and
`TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VI-N` (file-scoped per
SPEC-ROOT-009 — distinct from the `NOTE-VI-N` numbering in `NOTES.md`, which is shared/global
across the whole value-index pipeline's NOTES.md files by established convention). IDs are
assigned in ascending order and never reused or renumbered; superseded entries are marked
`[SUPERSEDED by SPEC-VI-N]` rather than deleted.

Next free ID: **SPEC-VI-19**.

---

## SPEC-VI-1: Cross-block global ordering invariant for BucketFile
*Added: 2026-07-02*

**Invariant:** Blocks within a single `BucketFile` are globally ordered by
`(TimeSec ASC, CanonicalValue ASC)` end-to-end, not just internally sorted within each block.

`SplitIntoBlocks` (`bucketmerge.go:145-176`) is the only function in the codebase that
partitions groups into multiple blocks; it globally sorts the full flattened group set across
every existing block before cutting fixed-size output blocks sequentially. Consequently,
block N's last group is always `<=` block N+1's first group by construction — there is no
code path that can produce overlapping or out-of-order blocks in this format.

**Consequence:** A per-file iterator (`BucketFileIterator`) may trust a plain sequential
block-by-block walk to yield fully sorted output for that file, with no cross-block
lookahead or re-sort required.

**Caveat:** This is an emergent property of `SplitIntoBlocks`'s implementation, not an
independently enforced/validated invariant — `DecodeBucketFile` does not assert group
ordering on read. It holds for every `BucketFile` produced by this codebase's writer
(`writerImpl.assembleBucket`) and compactor (`MergeBucketFiles`, invoked today via
`StreamCompactBucketFiles`) paths, both of which route through `SplitIntoBlocks`. **[Updated
2026-07-07, issue #490, task A-4/#98]** the non-streaming `CompactBucketFiles` this sentence
originally named is deleted (see `NOTES.md` NOTE-VI-053 addendum) — zero remaining production
callers. It is not verified against untrusted or
hand-corrupted input.

**Addendum (2026-07-12, scan-fallback removal plan, Phase 1): promoted to a decode-time-asserted
invariant.** The preceding Caveat's "not independently enforced/validated... does not assert
group ordering on read" no longer holds. `decodeBlockIndex` (the shared choke point both
`DecodeBucketFile` and `ReadBucketFileMetadata`/`readBucketFileTail`, SPEC-VI-8, route through)
now calls `validateBlockDirectoryOrder(dir []BlockDirEntry) error`, which asserts
`dir[i].MinTimeSec` is non-decreasing across `i` and returns an error satisfying
`errors.Is(err, ErrBlockDirectoryOutOfOrder)` on violation — a decode error, never a panic, on
both the in-memory and ranged read paths alike since they share this one choke point.

This promotion is not cosmetic: it is the load-bearing correctness precondition for the
newest-first early-stopping resolution introduced in the same phase
(`QueryBucketFileRangedNewestFirst`, `matchGroupsInBlockReverse`, `bucketquery_ranged.go`/
`bucketquery.go`) — early-stopping walks blocks in reverse and stops once enough matches are
found, so a corrupted or hand-built file that violates this ordering would previously have
caused early-stopping to silently return the wrong (non-newest) top-N results rather than an
error. Every future newest-first/early-stopping consumer may now depend on this ordering as a
validated guarantee rather than re-verifying it independently.

**Exemption (binding):** a block encoded with `MinTimeSec == 0 && MaxTimeSec == 0`
(`ComputeBlockMeta`'s explicit empty-block encoding — e.g. `filterDeadRefs` dropping every ref
in one block but not its neighbors) carries no real chronological position and is exempt from
this check in both directions; `EncodeBucketFile` supports such a block appearing anywhere in
the block list without corrupting the file, so the invariant is "MinTimeSec is non-decreasing
across all non-exempt blocks," not "strictly non-decreasing, no exceptions."

Pinned by `TestDecodeBucketFile_RejectsOutOfOrderBlockDirectory` (`bucketfile_test.go`), which
builds a real file via `SplitIntoBlocks` then performs a targeted byte-level swap of two on-disk
block-directory entries to produce a genuine out-of-order file and asserts decode fails with
`errors.Is(err, ErrBlockDirectoryOutOfOrder)`. Mutation-verified (temporarily disabling
`validateBlockDirectoryOrder`'s check reproduces a red run of this test; restoring it goes green
again).

Back-refs: `internal/modules/valueindex/bucketmerge.go:SplitIntoBlocks`,
`internal/modules/valueindex/stream_compaction.go:BucketFileIterator`,
`internal/modules/valueindex/bucketfile.go:decodeBlockIndex,validateBlockDirectoryOrder,
ErrBlockDirectoryOutOfOrder` (the decode-time assertion), `bucketfile_test.go:
TestDecodeBucketFile_RejectsOutOfOrderBlockDirectory` (the regression test).

---

## SPEC-VI-2: StreamCompactBucketFiles contract
*Added: 2026-07-02*

**Contract:** `StreamCompactBucketFiles` performs a heap-based k-way merge across N
already-decoded, already-filtered `BucketFileIterator`s (relying on SPEC-VI-1's ordering
guarantee for correctness), producing merge semantics equivalent to `MergeBucketFiles` —
the same union-refs-by-`(sourcePath, page)` and union-spans-by-`TraceID` rules — without ever
materializing the full merged group set in memory.

**Peak memory bound:** bounded by the K input files (already decoded by the caller before
being wrapped in iterators) plus one in-progress output block (at most `groupsPerBlock`
groups) plus a transient per-key merge buffer sized to however many of the K files currently
share the smallest key. This replaces `MergeBucketFiles`+`SplitIntoBlocks`'s map-of-maps
(`groups map[string]*groupAcc` with nested `refAcc`/`spanAcc`) for this call path, which
previously materialized the entire merged output before any output block was cut.

**Output semantics:** `output` is called at most once, with the fully assembled output file
bytes, or not at all if there is nothing to emit (e.g. all iterators empty, or all refs
filtered as dead by the caller's retention check before iterators were constructed).

`groupsPerBlock <= 0` defaults to `shared.ValueIndexBucketGroupsPerBlock`.

**Addendum (2026-07-03):** Since the disk-streaming redesign (`GroupIterator`, SPEC-VI-5),
"K input files (already decoded by the caller...)" in the Peak memory bound paragraph above no
longer holds universally — a caller may instead supply `GroupIterator`s backed by
`valueindex.NewDiskBucketFileIterator`, which are *not* fully decoded up front; each such
iterator's own resident footprint is at most one block at a time, not one whole file. The
peak-memory bound this contract guarantees is therefore now: K open iterators (each
contributing at most one file's worth of memory for `BucketFileIterator`, or at most one
block's worth for a disk-backed `GroupIterator`) plus one in-progress output block plus the
per-key merge buffer — `StreamCompactBucketFiles`'s own merge algorithm is unchanged either
way; only the per-iterator memory contribution differs by iterator implementation. The
output side is also now disk-staged (a local temp file, not an in-memory `[]byte`
accumulator) — see SPEC-VI-5 and `valueindex/NOTES.md` NOTE-VI-053 — so the callback signature
is `func(path string) error`, not `func(data []byte) error`; this is a caller-visible but
mechanical signature change, not a semantic one (`output`'s at-most-once, exhaustive-empty-set
semantics above are unchanged).

**Addendum (2026-07-06, NOTE-VI-077, issue #482):** `StreamCompactBucketFiles` now takes a
`maxOutputBytes int64` argument and MAY emit **more than one** output file. When
`maxOutputBytes > 0`, once the projected finalized size of the in-progress output file
(compressed body + string table + block index + footer) meets or exceeds the cap *at a block
boundary*, the current file is finalized and handed to `output`, and a fresh output file is
started. `output` is therefore now called **once per emitted file**, in emission order, rather
than at most once — but its per-file contract is unchanged (each invocation receives the path
to one fully assembled, self-contained BucketGroup file). The at-most-once guarantee now
applies per emitted file, and the "not at all if there is nothing to emit" guarantee still
holds for the whole call (an empty merge produces zero files).

The split lands only at block boundaries — never mid-block — because a block's groups reference
the current file's string table by interned index, so the effective rotation granularity is
one block (`ValueIndexBucketGroupsPerBlock`) and a file may overshoot the cap by at most one
block's serialized size (the cap is approximate, matching the flat-VINX path). `maxOutputBytes
<= 0` preserves the original single-file behavior.

**Addendum (2026-07-14, issue #501): peak memory bound revised, precisely.**
`StreamCompactBucketFiles`' peak memory is bounded by (a) the K input files' currently-decoded
state (one block each, unchanged), (b) one in-progress output block, and (c) a transient per-key
merge-union buffer. Every OUTPUT `BucketGroup`, once written (`assembleBucketWithCap`, writer.go)
or merged (`mergeGroupsAtKey`), is capped at `shared.ValueIndexBucketGroupMaxSpanRefs` span-ref
entries — this bounds decode-side cost (`decodeBucketBlock`) unconditionally, and prevents a hot
key's group from growing without bound across compaction generations (issue #501's core bug:
pre-fix, nothing ever split a group once formed). **It does NOT, by itself, bound the transient
union-building buffer inside `collectContributionsAtKey`/`mergeGroupsAtKey` to a fixed
constant** — that buffer's real size is K × (each contributing iterator's own real total fan-in
for the key), and the coalescing behavior (`collectContributionsAtKey`, SPEC-VI-3 amended below)
deliberately does not cap an iterator's own total, since doing so would prevent sibling groups
from ever re-consolidating. Real-world risk is mitigated by three complementary factors, not a
single unconditional bound: (1) this cap preventing unbounded-forever single-group growth, (2)
`valueindexcompactor`'s `CompactMaxInputFiles`/`CompactBatchBytes` bounding K, and (3) retention
filtering keeping a key's real live fan-in from growing without bound over time in practice. This
supersedes a less precise "K × `ValueIndexBucketGroupMaxSpanRefs`" characterization of the
merge-buffer bound that existed only in this issue's own planning draft, never previously
committed here. See `NOTES.md` NOTE-VI-111 for the full residual-risk writeup and the real-data
evidence (tenant 11638 histogram) that informed the final cap constant.

Back-ref: `internal/modules/valueindex/stream_compaction.go:StreamCompactBucketFiles,
mergeGroupsAtKey, collectContributionsAtKey`. See `NOTES.md` NOTE-VI-111, NOTE-VI-112 (why the
regression test for this proves an output-shape postcondition rather than a peak-heap bound).

**Addendum (2026-07-15, issue #503): the "K × fan-in, unbounded" transient union-buffer risk
above is now CLOSED, not merely mitigated — with one honestly-scoped exception.**
`mergeGroupsAtKey`'s union-building internals (`keymerge_spill.go`) were rewritten from a
map-of-maps held for the whole key to a disk-spill design (SPEC-VI-18): contributing
`(ref, span)` pairs accumulate into a flat in-memory buffer that spills to a sorted temp chunk
once its estimated size crosses `shared.ValueIndexMergeBufferSpillBytes` (mirroring
`runspill.go`'s external sort-merge shape, NOTE-VI-026), and a streaming k-way merge
(`reduceKeySpillRecords`) reduces the spilled chunks plus the in-memory tail back into output
groups, feeding completed refs into a `streamingSplitPacker` (SPEC-VI-16's greedy-first-fit
packing, ported to an incremental shape) as soon as each ref is finished, rather than
materializing the full merged result first.

**Revised peak memory bound, mutation-tested (NOTES.md NOTE-VI-113, TESTS.md new entries):**
bounded by (a) O(one spill-chunk's worth of records, itself bounded by
`shared.ValueIndexMergeBufferSpillBytes`/estimated-record-size) — **mutation-confirmed
load-bearing** — plus (c) O(`maxSpansPerGroup`, one sibling group being incrementally packed) —
**mutation-confirmed load-bearing** — **independent of K (contributing file/iterator count) and
independent of the key's cumulative real total fan-in.** This closes the "K × fan-in" gap this
Addendum's 2026-07-14 text left open.

**Honest exception, NOT closed by this task:** the design's memory-bound argument also leans on
a third term — (b) one ref's own per-TraceID union is bounded by `shared.MaxSpans` (a real but
*derived, cross-package* consequence of `blockio/shared`'s row-count limit at
`blockio/shared/constants.go:573`, not a `valueindex`-native invariant; pinned by
`TestBucketBlockRef_CannotAddressABlockWithSpanCountAboveMaxSpans` — the real cross-package
decode-time enforcement test, TESTS.md TEST-VI-36). Mutation testing found this
specific term is **not provably load-bearing** against a "shared, never-reset union map" class of
bug: `SpanIndexes` values are `uint16`, so any map keyed by them is information-theoretically
capped at 65,536 entries regardless of fixture scale — far below `shared.MaxSpans` (1,000,000) —
so no amount of fixture scaling could make that mutation violate this specific bound. That class
of bug is a correctness defect (wrong unions), not a memory-bound violation, and is independently
guarded by `TestReduceKeySpillRecords_UnionsSpanIndexesAcrossChunks`'s own mutation check
(TESTS.md). Term (b) is retained here as a true structural fact (real, worth stating), not as
this task's own proven regression guard.

Back-ref: `internal/modules/valueindex/keymerge_spill.go` (`keySpillRecord`,
`accumulateAndSpillKeyRecords`, `reduceKeySpillRecords`, `streamingSplitPacker`),
`internal/modules/valueindex/stream_compaction.go:mergeGroupsAtKey,
mergeGroupsAtKeyWithSpillThreshold`. See SPEC-VI-18, NOTES.md NOTE-VI-113. Issue #503.

---

## SPEC-VI-3: Per-file key uniqueness precondition for the heap-merge
*Added: 2026-07-02*

**Precondition:** In addition to SPEC-VI-1's ordering guarantee, `(TimeSec,
CanonicalValue)` keys are unique **within** one `BucketFile` — no single `BucketFile`
(across any of its blocks) may contain two separate `BucketGroup`s sharing the same key.

Every producer path guarantees this today by deduping into a single group per key (via a
map-keyed accumulator) before ever calling `SplitIntoBlocks`/`EncodeBucketFile`:
`writerImpl.assembleBucket` (writer.go), `MergeBucketFiles` (bucketmerge.go), and
`StreamCompactBucketFiles` itself (its own heap-merge output is one group per key by
construction).

**Consequence:** `BucketFileIterator`/`StreamCompactBucketFiles`'s heap-based k-way merge
does **not** defend against or detect a violation of this precondition. The heap-drain
"coalesce all roots sharing the current min key" step only merges groups that are
simultaneously parked at the heap root during one outer-loop pass; if a single input file
ever contained two same-key groups in different blocks, only the first occurrence would be
folded into the current merge round, and the second would surface as a spurious second
output group for the same key on a later outer-loop iteration — a silent
duplicate-key/corrupted-output failure mode, not a panic or error.

**Status:** Currently unreachable given every existing producer's construction, so not an
active bug — recorded here so a future change to the writer or a new file-producing path
does not silently violate it.

Back-refs: `internal/modules/valueindex/stream_compaction.go:StreamCompactBucketFiles`,
`internal/modules/valueindex/writer.go:assembleBucket`,
`internal/modules/valueindex/bucketmerge.go:MergeBucketFiles`.

**Amended (2026-07-14, issue #501): the precondition above no longer holds unconditionally.**
`(TimeSec, CanonicalValue)` keys are no longer guaranteed unique within a single input file. As
of issue #501, `assembleBucketWithCap` (writer.go) and `mergeGroupsAtKey`/
`splitBucketGroupBySpanCap` (stream_compaction.go, bucketfile.go — contract in SPEC-VI-16)
deliberately emit multiple sibling `BucketGroup`s sharing one key when that key's accumulated
span-ref count would exceed `shared.ValueIndexBucketGroupMaxSpanRefs`. Every sibling's `Refs` may
reference the same underlying data block (`SourceID`+`Ref`) as another sibling, but their `Spans`
subsets are always disjoint by construction — no span is ever duplicated or dropped across
siblings (proven by `TestSplitBucketGroupBySpanCap`'s union-equality assertions and
`TestSplitSiblingGroups_QueryEquivalentToUnsplitGroup`'s end-to-end query-equivalence check,
`TESTS.md` TEST-VI-26/TEST-VI-27). `collectContributionsAtKey`'s combined pop-advance-repush loop
(no longer the old two-phase collect-then-advance split this entry's original "Consequence"
paragraph above described) merges consecutive same-key groups from ONE iterator within a single
merge pass, so sibling groups re-consolidate (down to the cap, never below it) across successive
compaction generations rather than proliferating indefinitely — see `TESTS.md` TEST-VI-28. The
query path (`matchGroupsInBlock`/`matchGroupsInBlockReverse`) already tolerates multiple same-key
groups with zero changes (plain union of matches, no dedup risk since disjoint by construction).
The original "Consequence"/"Status" paragraphs above describing this as unreachable/latent are
superseded for the specific violation shape this amendment covers (deliberate, capped,
re-consolidating sibling emission) — they remain accurate for any OTHER, non-#501 way a future
producer might violate per-file key uniqueness (e.g. a hand-corrupted file, or a new producer
that emits same-key groups without going through `splitBucketGroupBySpanCap`), which the
heap-merge still does not defend against or detect.

Back-refs (amendment): `internal/modules/valueindex/writer.go:assembleBucketWithCap`,
`internal/modules/valueindex/stream_compaction.go:mergeGroupsAtKey, collectContributionsAtKey`,
`internal/modules/valueindex/bucketfile.go:splitBucketGroupBySpanCap`. See SPEC-VI-16 (the
cap/split mechanism's own contract) and `NOTES.md` NOTE-VI-111 (design rationale, real-data
evidence, residual-risk writeup).

---

## SPEC-VI-4: TimeSec is minute-floored at write time; comparing consumers must match alignment
*Added: 2026-07-02*

**Invariant:** `ValueIndexEntry.TimeSec` (and therefore `BucketGroup.TimeSec`, the
`(TimeSec, CanonicalValue)` merge/sort key governed by SPEC-VI-1/SPEC-VI-3) is floored to
whole 60-second (minute) boundaries at write time: `TimeSec = (startNano / 1_000_000_000) /
60 * 60`. It is not per-second resolution.

**Consequence for consumers:** Any consumer that compares `TimeSec` against an
externally-derived time bound (e.g. a query window's start/end) MUST floor that bound to the
same 60-second alignment before comparing, or it risks false-negative exclusion of in-range
entries. Flooring is monotonic and one-directional — a *lower* (or equally-floored) bound
never excludes an entry that a non-floored bound would have included, but a bound that is
*not* floored to the same granularity as `TimeSec` can sit strictly between an entry's true
event time and that entry's floored `TimeSec`, causing an exact-inclusion filter (e.g.
`bucketquery.go:LookupValue`'s `TimeSec < minTS || TimeSec > maxTS` check) to silently drop a
genuinely in-range entry. Only the **lower** bound requires this treatment; an upper bound
needs no equivalent widening because flooring only ever reduces `TimeSec` relative to the true
event second, so `TimeSec <= true_event_sec <= upperBound` is preserved regardless of the
upper bound's own alignment.

**Where the flooring responsibility sits (binding — applies to every consumer, including
`QueryBucketFileRanged`, SPEC-VI-10):** this package's own query functions
(`LookupValue`, `QueryBucketFiles`, and `QueryBucketFileRanged`) perform **no internal
flooring** — confirmed by direct read, no flooring call exists anywhere in `bucketquery.go`,
`bucketquery_ranged.go`, `predicate.go`, or `bucketfile.go`. They are pure raw-value
comparators by design and contract. The flooring responsibility sits entirely with the
**external caller**, ultimately tempo-mrd's `nanoWindowToSec` (see the cross-repo dependency
note below) — this package trusts that any `minSec`/`minTS` it receives has already been
floored to the same 60-second alignment as write-side `TimeSec`. `QueryBucketFileRanged`
deliberately places this responsibility identically to `QueryBucketFiles`, not differently:
both its file-level prune (`footer.OverlapsTimeRange`) and its per-block prune (the algebraic
negation of `BucketBlock.OverlapsTimeRange` applied to a `BlockDirEntry`'s `MinTimeSec`/
`MaxTimeSec`) compare the caller's raw, unfloored-by-this-package bound directly against
write-side-floored stored values — the same comparator, the same fields, the same lack of
internal flooring `QueryBucketFiles` already has. The file-level check is a non-lossy superset
short-circuit over the per-block checks (if the file's own aggregate range doesn't overlap,
no block's narrower range can either), not a new semantic requiring separate reasoning.
Pinned by `TESTS.md` TEST-VI-18 (file-level) and TEST-VI-19 (block-level), both of which prove
`QueryBucketFileRanged` matches `QueryBucketFiles` exactly at the window edge where a caller's
failure to floor would matter, rather than silently diverging by applying some
ranged-path-only flooring rule.

**Cross-repo dependency (external, informational — not enforced by this repo's tests):**
tempo-mrd's index-driven TraceQL search path
(`tempodb/encoding/vblockpack/value_index_query.go:nanoWindowToSec`) converts a query's
`[startNano, endNano]` window into `[minSec, maxSec]` and passes `minSec` through to
`bucketquery.go:LookupValue`'s exact-inclusion filter via `vibuilder.BuildSource`. Per this
invariant, `nanoWindowToSec`'s `minSec` MUST floor to the same 60-second alignment as this
package's write-side `TimeSec` truncation, or a genuinely in-range span whose minute-floored
`TimeSec` falls before a non-aligned `minSec` is silently dropped from search results with no
fallback (the index path reports "coverage found" and answers the query incompletely). This is
a hard correctness coupling between the two repos, not an independent optimization — the two
sides must not disagree on granularity, and a deploy that ships this repo's truncation without
tempo-mrd's matching widening (or with a version-skew gap between them) reintroduces the
false-negative bug. `QueryBucketFileRanged` (SPEC-VI-10) inherits this same cross-repo coupling
unchanged, since it performs no flooring of its own either.

Back-refs: `valueindex_extract.go:buildSpanStartSecByRef` (this repo — the write-side
truncation), `bucketquery_ranged.go`'s two `SPEC-VI-4`-tagged comparison sites (file-level and
block-level prune) and, as an external cross-repo back-ref, tempo-mrd's
`tempodb/encoding/vblockpack/value_index_query.go:nanoWindowToSec` (the read-side bound that
must match). See `NOTES.md` NOTE-VI-051 for the design rationale and cardinality motivation.

---

## SPEC-VI-5: GroupIterator's Peek/Advance/Err contract
*Added: 2026-07-03*

**Invariant:** Any `GroupIterator` implementation must guarantee `Peek()` never performs I/O
or returns an error — all fallible work happens in `Advance(ctx)`, surfaced via `Err()`. A
caller must check `Err()` after any `Advance` call and after popping an iterator that reported
`Peek() == (nil, false)`, since an errored iterator is indistinguishable from an exhausted one
via `Peek` alone.

**Rationale:** `GroupIterator` (the interface `StreamCompactBucketFiles`' k-way merge
requires — `Peek`, `Advance(ctx)`, `StringTable`, `Err`, `Close`) is implemented by both
`BucketFileIterator` (whole-file, in-memory decode — `Advance` never fails, `Err` is always
nil) and `diskBucketFileIterator` (lazy, block-at-a-time decode from a local temp file —
`Advance` can fail: local disk I/O errors, corrupt block payloads, or a retention-checker call
it triggers that itself performs network I/O). Keeping `Peek()` itself I/O-free and
error-free — by decoding the "current" block eagerly at construction or at the `Advance` call
that crosses into it, never inside `Peek()` — lets every existing caller of `Peek()` (the
merge/heap-ordering code in `bucketIteratorHeap.Less`/`StreamCompactBucketFiles`'s merge loop)
keep exactly the same contract it had before disk-backed iterators existed; only the code that
already calls `Advance`/pops from the heap needs to additionally check `Err()`.

**Consequence:** `Peek() == (nil, false)` is ambiguous on its own — it means either "this
iterator is genuinely exhausted, safe to drop from the merge" or "this iterator hit an error
partway through and must not be silently treated as exhausted" (which would silently drop that
file's remaining, un-merged data with no error surfaced). Every site that pops an iterator
after seeing `Peek() == (nil, false)` must check `Err()` before concluding "exhausted" rather
than "errored."

Back-ref: `internal/modules/valueindex/stream_compaction.go:GroupIterator`.

---

## SPEC-VI-6: `MergeTraceGroups` — `RefChecker`/`context.Context` signature, in-memory-only v1 scope
*Added: 2026-07-04*

**Contract:** `MergeTraceGroups(ctx context.Context, checker RefChecker, inputs
...[]TraceGroup) ([]TraceGroup, error)`. This widens the function's original retention-check
parameter from a bespoke `SourceExists func(sourceRef string) bool` to `RefChecker` — the same
interface (`IsLive(ctx, sourceRef) (bool, error)`) every other compaction code path in this
package already uses (`StreamCompactBucketFiles`, `newCachingRefChecker`), threading a
`context.Context` through for the first time so `RefChecker.IsLive` can be called correctly.
`checker == nil` skips the retention drop entirely (keeps all spans) — the same nil-means-
disabled convention `StreamCompactBucketFiles` uses.

**This was a free, non-breaking signature change at the time it was made**: `MergeTraceGroups`
had zero callers anywhere in the repo before `valueindexcompactor`'s `mergeTraceLevel`
(`internal/modules/valueindexcompactor/traceindex_dispatch.go`) became its first real caller in
the same change — there were no existing call sites to migrate.

**Merge semantics (unchanged by the signature widening):** groups inputs by `TraceID`, dedups
`(TraceID, SpanID)` pairs (first occurrence wins across the flattened input order), drops a
span whose `checker.IsLive` reports false, drops a group entirely if every span was dropped,
takes the minimum `TimeSec` across all contributing groups for a given `TraceID`. Output is
sorted `(TraceID ASC, TimeSec ASC)` — TraceID leads because it is the trace-by-id point-lookup
key, making each v2 block's `[minTraceID, maxTraceID]` a tight, seekable pruning bound (issue
#476, NOTE-VI-075; changed from the original `(TimeSec, TraceID)` flat-blob ordering).

**v1 scope boundary — in-memory only, not disk-streaming:** unlike `StreamCompactBucketFiles`
(SPEC-VI-2), `MergeTraceGroups` requires every input file's `TraceGroup`s fully decoded into
memory as `[]TraceGroup` slices before merging — there is no k-way-merge disk-backed iterator
for `TraceGroup` files analogous to `NewDiskBucketFileIterator`/`GroupIterator`. This is a
deliberate, stated v1 scope decision, not an oversight: trace-index files are expected to stay
substantially smaller than `BucketGroup` attribute-value files (compact per-trace direct
pointers vs. full value postings with per-block metadata), so whole-batch in-memory merging is
expected to be acceptable at expected volumes. **Follow-up condition (mirrors the disk-
streaming precedent SPEC-VI-2/NOTE-VI-052 already established for `BucketGroup` files):**
revisit only if post-deployment telemetry shows trace-index file sizes or merge batch sizes
growing large enough to make whole-batch in-memory decoding a real memory-pressure risk — do
not build a streaming variant preemptively.

**Update (issue #476, NOTE-VI-075):** the v2 batched TraceGroup on-disk format now bounds the
READ path cost independently of output-file size — the querier ranged-reads only the footer +
block directory + the surviving block(s), never the whole file — so a single large compacted
output file is no longer a read-path liability (this was the concern behind the original
"revisit if files grow large" clause). The compactor merge remains in-memory-only per the v1
scope boundary above; that decision is about compactor MEMORY, which v2 batching does not
change (the merge still decodes every input into memory before re-encoding into blocks).

Back-refs: `internal/modules/valueindex/traceindex.go:MergeTraceGroups`,
`internal/modules/valueindexcompactor/traceindex_dispatch.go:mergeTraceLevel` (the sole
caller). See `NOTES.md` NOTE-VI-038 (Addendum, 2026-07-04) and NOTE-VI-064.

---

## SPEC-VI-7: `RangedSource` — minimal partial-read surface for a v2 BucketGroup file
*Added: 2026-07-07*

**Contract:** `RangedSource` is the minimal read surface a v2 `BucketGroup` file needs for
partial (ranged) decoding:

```go
type RangedSource interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64) (int, error)
}
```

`Size` returns the total byte length of the underlying file/object. `ReadAt` fills `p` from the
source starting at `off`, following `io.ReaderAt` semantics — a short read returns a non-nil
error; EOF is `io.EOF`. Implementations are expected to surface a not-found condition (the
object no longer exists) in a form recognizable via `errors.Is` by the specific sentinel their
call site expects (e.g. `vibuilder.ErrFileNotFound`) — `RangedSource` itself defines no such
sentinel, since "not found" is a property of the concrete backend (local disk vs. object
storage), not of this interface.

**Known implementations:** a local `*os.File` (`disk_iterator.go`'s compaction path) and a
per-key object-storage adapter (`vibuilder.storeRangedSource`, issue #488 B-5) — both satisfy
this interface without either needing to know about the other's backend.

**Consequence:** any new backend wanting to use `ReadBucketFileMetadata` (SPEC-VI-8) or
`QueryBucketFileRanged` (SPEC-VI-10) needs only implement these two methods — no dependency on
`os.File`, `FileStore`, or any other concrete storage type.

Back-ref: `internal/modules/valueindex/bucketfile_metadata.go:RangedSource`.

---

## SPEC-VI-8: `ReadBucketFileMetadata` — shared eager metadata decode for `RangedSource`
*Added: 2026-07-07*

**Contract:** `ReadBucketFileMetadata(src RangedSource) (BucketFooter, []BlockDirEntry,
*StringTable, error)` reads and decodes `src`'s footer, string table, and block directory — the
full eager, bounded metadata a ranged reader needs before any block can be addressed or the
file pruned by time range. **Precondition:** `src`'s header magic must already have been
validated by the caller — this function only reads the tail of the file (footer, string table,
block index), never the header, and does not itself detect "this isn't a BucketGroup file at
all" via the header.

**Footer-level not-a-bucket-file detection:** a footer that is too short (`size <
bucketFooterSize`) or fails magic validation is reported via an error satisfying
`errors.Is(err, ErrNotBucketFile)` — the same sentinel `DecodeBucketFooter` uses (both branches,
per the hardening documented in `NOTES.md` NOTE-VI-083) — letting a caller that has only a
`RangedSource` (no prior whole-file decode) classify skip-vs-abort with one check, mirroring
`QueryBucketFiles`' existing whole-file classification (`bucketquery.go:QueryBucketFiles`'s
`ErrNotBucketFile`-skip branch).

**Two-phase decomposition:** internally split into `readBucketFileFooter` (footer only, plus
the source's total size) and `readBucketFileTail` (string table + block directory, given an
already-decoded footer and size) so a caller that must inspect the footer before deciding
whether to fetch the rest of the metadata at all (`QueryBucketFileRanged`'s file-level time
prune, SPEC-VI-10) does not pay for a redundant footer read — only `ReadBucketFileMetadata`'s
own combined call re-derives the footer, and does so exactly once.

**Bounds validation (binding, NOTE-VI-046):** `readBucketFileTail` validates every untrusted
offset/length it decodes using the same overflow-safe, per-term-before-sum pattern
`DecodeBucketFile` already established (`bucketfile.go:DecodeBucketFile`) — never a naive
`off+len > bound` sum-first check, which a corrupt huge `off` paired with a small `len` can wrap
past. Two bounds are checked:
1. The footer's `StringTableOff`/`StringTableLen` and `BlockIndexOff`/`BlockIndexLen`, each
   individually against `size`, before their sums are checked against `size`.
2. Every decoded `BlockDirEntry`'s `CompOff`/`CompLen`, each individually against
   `StringTableOff` (the offset at which the string table begins — block bodies always live in
   `[headerLen, StringTableOff)` by construction, the same "region that can legally contain
   block bodies" bound `DecodeBucketFile` checks via `end > strOff`), before their sum is
   checked against `StringTableOff`.
Both checks fail closed with a descriptive error, never a panic or an unbounded allocation. This
matters more here than in `DecodeBucketFile`: this helper is reachable from
`QueryBucketFileRanged` on object-storage bucket files fetched over the network
(`bucketquery_ranged.go:QueryBucketFileRanged`), not only from `disk_iterator.go`'s trusted local
compaction temp files — a corrupted or truncated directory entry (bit-rot, a partial write,
storage-layer inconsistency) must never drive a `make([]byte, CompLen)` allocation sized directly
from untrusted wire bytes. Because the check runs once here, both consumers
(`bucketquery_ranged.go`'s `readAndDecodeBlockRanged` and `disk_iterator.go`'s `decodeBlockAt`)
receive only already-validated directory entries and need no bounds check of their own. Pinned by
`TESTS.md` TEST-VI-20 (footer) and TEST-VI-21 (block directory).

**Shared consumers:** `disk_iterator.go`'s `readBucketFileMetadata` (the local-`*os.File`,
already-header-validated compaction path) is now a thin adapter over this function, preserving
its own signature/behavior unchanged; `vibuilder`'s ranged query path (issue #488, B-5) is the
other consumer, via `QueryBucketFileRanged`. Both consume one binary-format parser instead of
two independently-maintained copies of the same offset arithmetic.

Back-ref: `internal/modules/valueindex/bucketfile_metadata.go:ReadBucketFileMetadata,readBucketFileFooter,readBucketFileTail`,
`internal/modules/valueindex/disk_iterator.go:readBucketFileMetadata` (the adapter).

---

## SPEC-VI-9: `blockExcludedByValue` — value-range prune, ruling-14 numeric-range capability boundary
*Added: 2026-07-07*

**Contract:** `blockExcludedByValue(pred Predicate, minValue, maxValue []byte) bool` reports
whether every value in `[minValue, maxValue]` (a `BlockDirEntry`'s per-block value range) is
excluded by `pred`, letting a ranged reader (`QueryBucketFileRanged`, SPEC-VI-10) skip a block's
body `ReadAt` entirely, using only information already present in the block directory without
reading or decoding the block body at all — no bloom filter is consulted anywhere in the
`matchGroupsInBlock` call chain either read path uses (see `NOTES.md` NOTE-VI-082).

**Comparator requirement (binding):** the containment check MUST use `compareCanonicalBytes`
(plain lexicographic, with length-prefix tiebreak) — never the numeric-decode-aware
`compareCanonical`. `BlockDirEntry.MinValue`/`MaxValue` are themselves computed at write time
using `compareCanonicalBytes` (`bucketfile.go:ComputeBlockMeta`); checking containment with a
different comparator than the one that built the bounds risks a **false-negative prune** —
silently dropping a block that actually contains a match, a correctness regression, not a style
choice.

**Scope — this is a genuine, deliberate capability boundary, not a bug:**

| Predicate kind | Column type | Value-range prune applied? |
|---|---|---|
| Equality | any | Yes — self-consistency holds regardless of whether byte order carries numeric meaning |
| Range / Between | non-numeric (string, bytes, UUID, RangeString, RangeBytes) | Yes — `compareCanonical` and `compareCanonicalBytes` already agree for these types |
| Range / Between | numeric (uint64, int64, float64, and their Range* variants) | **No** — falls through to `matchGroupsInBlock`'s per-group TimeSec check + `pred.Match` only, exactly matching pre-#488 behavior (no bloom filter is consulted anywhere in this call chain — NOTE-VI-082) |
| neq / regex / nil predicate | any | No — returns `false` ("cannot decide, don't prune"); falls through to `matchGroupsInBlock`'s per-group TimeSec check + `pred.Match` (no bloom filter is consulted anywhere in this call chain — NOTE-VI-082) |

**Why numeric range/between gets no prune:** persisted `MinValue`/`MaxValue` are lex-byte
extremes of little-endian-encoded numbers, which carry no numeric meaning (LE encoding is not
lex-order-preserving — the classic `uint64(255)` vs `uint64(256)` byte-ordering disagreement,
see `NOTES.md` NOTE-VI-011). Directory extremes therefore cannot soundly bound a numeric range.
**This is a real, currently-unclosed gap in the on-disk format** — not something papered over
with an unsound comparator substitution. A future writer-side change to persist
order-preserving numeric bounds in the block directory could close this gap but is explicitly
out of scope for issue #488 (a format change, flagged as a candidate follow-up issue).

**Consequence for callers:** a numeric-typed range/between query gets exactly the same
block-level pruning today as it did before `QueryBucketFileRanged` existed (time-range pruning
plus `matchGroupsInBlock`'s per-group TimeSec check + `pred.Match`; no bloom filter exists
anywhere in this call chain — NOTE-VI-082) — `blockExcludedByValue` never makes numeric range
queries *worse*, it simply does not make them *better* the way it does for equality and
non-numeric range queries.

Back-ref: `internal/modules/valueindex/predicate.go:blockExcludedByValue,isNumericColType`.
See `NOTES.md` NOTE-VI-011 (the LE-byte-ordering root cause) and NOTE-VI-081 (this feature's
overall design rationale).

---

## SPEC-VI-10: `QueryBucketFileRanged` — partial-read query contract, shared matching with `QueryBucketFiles`
*Added: 2026-07-07*

**Contract:** `QueryBucketFileRanged(ctx context.Context, src RangedSource, pred Predicate,
timeRange *[2]uint64) ([]LookupResult, error)` performs a ranged, partial read of one v2
`BucketGroup` file against `pred`/`timeRange`, issuing only the `ReadAt` calls needed to prune
and decode surviving blocks:

1. Footer `ReadAt` (via `ReadBucketFileMetadata`'s footer-only phase, SPEC-VI-8) — a
   footer-magic mismatch (`errors.Is(err, ErrNotBucketFile)`) is treated as **"not a bucket
   file"** and returns `(nil, nil)`, not an error — the per-file analog of `QueryBucketFiles`'
   own `ErrNotBucketFile`-skip (`bucketquery.go:121`). Any other footer decode failure is a
   genuine error on a real v2 file and is returned (caller falls back to a full scan, per
   NOTE-VI-046's silent-under-count concern).
2. **File-level time prune:** if the footer's own time range does not overlap
   `[minTS, maxTS]`, return `(nil, nil)` — costs exactly one `ReadAt` (the footer) and nothing
   more. **No internal flooring is applied to `minTS`/`maxTS` here** — this check
   (`footer.OverlapsTimeRange`) compares the caller's raw bound directly against write-side-
   floored stored values, identically to how `QueryBucketFiles`/`LookupValue` already do (per
   SPEC-VI-4's binding "where the flooring responsibility sits" clause: the caller, ultimately
   tempo-mrd's `nanoWindowToSec`, is responsible for flooring before this package is ever
   reached). This is a non-lossy superset short-circuit over step 4's per-block check, not an
   independent semantic requiring its own flooring reasoning. Pinned by `TESTS.md` TEST-VI-18.
3. Block-directory `ReadAt` (via `ReadBucketFileMetadata`'s remaining phase) for the block
   directory and string table.
4. **Per-block prune**, for each directory entry: dir-level time-range check (`d.MaxTimeSec <
   minTS || d.MinTimeSec > maxTS` — the algebraic negation of `BucketBlock.OverlapsTimeRange`,
   applied to the same `MinTimeSec`/`MaxTimeSec` fields and comparators `QueryBucketFiles`
   already uses per block, with the same no-internal-flooring, caller-trusts contract as step 2
   above), then `blockExcludedByValue` (SPEC-VI-9) if `pred != nil`. Pinned by `TESTS.md`
   TEST-VI-19.
5. Block-body `ReadAt` — one per surviving block only, via `readAndDecodeBlockRanged`. The
   entry's `CompOff`/`CompLen` were already bounds-checked against `StringTableOff` by step 3's
   `readBucketFileTail` call (SPEC-VI-8's binding bounds-validation clause, NOTE-VI-046) — this
   step never sees an unvalidated entry.
6. **Group/predicate matching** delegated to `matchGroupsInBlock` — the exact same function
   `QueryBucketFiles` calls for its own per-block matching (extracted from `QueryBucketFiles`'
   inner loop specifically so the two read paths cannot silently diverge on group-level
   matching semantics). `matchGroupsInBlock`'s own contract is exactly two checks per group:
   `g.TimeSec` within `[minTS, maxTS]`, then `pred.Match(g.CanonicalValue)` if `pred != nil` —
   **no bloom-filter check is part of this contract**. [Oracle-verified 2026-07-07 by direct
   read of `QueryBucketFiles`, `matchGroupsInBlock`, and `QueryBucketFileRanged`: no call to
   `BucketBlock.MayContainValue` exists in this call chain in either read path. An earlier
   design description (`plan.md`/task-breakdown.md §B-4) referenced a "bloom check (post-read)"
   step; this was a stale premise that never matched the actual `QueryBucketFiles`
   implementation. `QueryBucketFileRanged` correctly does not add one either — see NOTES.md
   NOTE-VI-082.]
7. `ctx` is checked for cancellation at three points, each guarding the reads that follow it: once
   at function entry (before the footer read), once after the metadata read (string table + block
   directory) completes and before the per-block loop starts, and once per directory entry, at the
   top of the per-block loop (before either prune check runs) — not just once per surviving entry.
   A `ctx` already canceled on entry costs zero `ReadAt` calls. Pinned by `TESTS.md` TEST-VI-17's
   `TestQueryBucketFileRanged_ContextCancelledStopsEarly` case.

**Parity guarantee:** because both `QueryBucketFiles` and `QueryBucketFileRanged` call the
identical `matchGroupsInBlock`, the two read paths are structurally incapable of diverging on
which groups/spans a given block yields for a given predicate and time range — the only
difference between them is which blocks get their bodies read at all (whole-file decode vs.
directory-pruned partial read). This parity guarantee extends to the SPEC-VI-4 flooring
question above: `TESTS.md` TEST-VI-18/19 assert `QueryBucketFileRanged` produces results
byte-identical to `QueryBucketFiles` at the exact window edge where a caller's failure to floor
would matter, proving the two paths agree rather than one silently applying a different
flooring rule than the other.

Back-refs: `internal/modules/valueindex/bucketquery_ranged.go:QueryBucketFileRanged,readAndDecodeBlockRanged`
(its two `SPEC-VI-4`-tagged comparison sites are the file-level and per-block time-prune checks
in steps 2 and 4 above), `internal/modules/valueindex/bucketquery.go:QueryBucketFiles,matchGroupsInBlock`
(the shared helper). See `NOTES.md` NOTE-VI-081/082/083 and `SPECS.md` SPEC-VI-4.

---

## SPEC-VI-11: `ColumnPolicy` — root-package allow-list-with-hard-exclusion column indexing policy (#496)
*Added: 2026-07-10*

**Contract:** `blockpack.ColumnPolicy{Allow map[string]struct{}, AlwaysExclude
map[string]struct{}, Enabled bool}` controls which columns `WriteValueIndexL0`/
`extractBlockColumns`/`ExtractValueIndexEntries` (root package, `valueindex_extract.go`/
`valueindex_l0write.go`) persist as standard per-column value-index entries. It is an
allow-list-with-hard-exclusion, not a bare denylist — the caller only needs to know the small,
curated set of columns it wants indexed, not a block's full column universe (a bare denylist
would require enumerating every column name in the block to exclude the ones not wanted,
which the caller cannot know ahead of extraction without a redundant pre-pass).

**`(p ColumnPolicy) Allowed(name string) bool`:** `!p.Enabled` → `true` (disabled policy
indexes everything — see the R12 zero-value clause below). Otherwise: `p.AlwaysExclude[name]`
present → `false` unconditionally (AlwaysExclude wins over Allow — a column mistakenly placed
in both a caller's dedicated list AND `AlwaysExclude` is still excluded). Otherwise:
`p.Allow[name]` presence determines the result.

**Zero-value / R12 safety-valve semantics (binding):** `ColumnPolicy{}` (`Enabled: false`)
indexes EVERY column — byte-for-byte identical to the pre-#496 nil-denylist behavior,
INCLUDING `HardExcludedColumns` NOT being enforced. This is deliberate: disabling the #496
feature disables its ENTIRE policy layer, not merely the dedicated-list half of it — a
`ColumnPolicy{Enabled: false}` is not "index everything except the 4 hard-excluded columns," it
is genuinely "index everything," matching today's production behavior exactly.

**`HardExcludedColumns` (binding, permanent regardless of Enabled/Allow):**
`{"span:id", "span:parent_id", "trace:id", "span:start"}` — permanently excluded from the
standard per-column value index whenever `Enabled=true`, regardless of dedicated-list
membership or usage-triggered backfill status (#496 R2; history in NOTES.md's cross-reference
to `valueindexconsumer/NOTES.md`'s R10 entry). `trace:id` is already excluded independently via
`WriteValueIndexL0`'s pre-existing `TraceGroup`-routing special case
(`valueindexconsumer/SPECS.md` SPEC-VI-4's precedent); the other three are not excluded
anywhere else in the codebase and rely entirely on this set for their exclusion.

**`BuildColumnPolicy(enabled bool, dedicatedList, triggeredColumns []string) ColumnPolicy`:**
computes the effective policy a forward-write-path caller should use — `Allow` is the union of
`dedicatedList` and `triggeredColumns`; `AlwaysExclude` is always `HardExcludedColumns`. A
column is indexed iff it is in `dedicatedList` OR `triggeredColumns`, AND is NOT in
`HardExcludedColumns`.

**Enforcement points (both, binding — both must apply the SAME policy for a given write to be
correct):**
1. `extractBlockColumns`'s per-column loop (`valueindex_extract.go`) — `!policy.Allowed(colKey.
   Name)` skips the column before any value is yielded.
2. `WriteValueIndexL0`'s yield callback (`valueindex_l0write.go`) — a SECOND `!policy.Allowed
   (e.ColName)` check, defensive/redundant with (1) for `WriteValueIndexL0`'s own callers but
   necessary because `ExtractValueIndexEntriesForColumns` (SPEC-VI-11's own allowlist wrapper,
   below) deliberately runs extraction with a DISABLED policy internally, so `WriteValueIndexL0`
   itself is the only enforcement point for callers that go through that wrapper.

**`ExtractValueIndexEntriesForColumns(r, allowlist, yield)` — the backfill engine's allowlist
wrapper (plan.md Section 4.5):** a thin filter around `ExtractValueIndexEntries` that reuses the
exact same per-block walk and column-value decoding, filtering at the YIELD boundary to only
pass through entries for columns named in `allowlist`. Internally calls `ExtractValueIndexEntries`
with a DISABLED (`ColumnPolicy{}`) policy so structural columns (`span:id`, `trace:id`, etc.)
still flow through the underlying extraction unfiltered — the allowlist filter, not
`ColumnPolicy`, is what scopes the output to one column. This keeps `extractBlockColumns`'s
policy semantics and every EXISTING caller (`WriteValueIndexL0`, `valueindexconsumer`)
completely unchanged; the backfill engine's "index only this one triggered column against
history" requirement is satisfied without touching extraction internals at all.

**Package-placement note (see NOTES.md's cross-reference and `viusage/NOTES.md` NOTE-VIUSAGE-7
for the full reasoning):** `ColumnPolicy` lives in the ROOT `blockpack` package
(`valueindex_policy.go`), not `internal/modules/viusage` as originally proposed in plan.md
Section 4.6 — moved during implementation to avoid a `blockpack ↔ viusage` import cycle once
`BackfillEngine` needed to import root `blockpack` for `*Reader`/`ObjectPutter`/
`ExtractValueIndexEntriesForColumns`. `BackfillEngine` itself was subsequently relocated the
same direction, out of `viusage` entirely and into root's own `valueindex_backfill.go`, for the
identical import-cycle reason (see NOTE-VIUSAGE-7's addendum) — so it is no longer accurate to
describe it as `viusage.BackfillEngine`. `viusage.DefaultDedicatedColumns` (the actual dedicated-
column LIST, distinct from the policy MECHANISM) correctly stayed in `viusage`
(`viusage/SPECS.md` SPEC-VIUSAGE-7) — only the policy/enforcement TYPE (and, later,
`BackfillEngine`) moved here.

Back-refs: `valueindex_policy.go:ColumnPolicy,HardExcludedColumns,Allowed,BuildColumnPolicy`
(root package), `valueindex_extract.go:extractBlockColumns,ExtractValueIndexEntries,
ExtractValueIndexEntriesForColumns` (root package), `valueindex_l0write.go:WriteValueIndexL0`
(root package). See `NOTES.md`'s #496 cross-reference entry, `viusage/SPECS.md`
SPEC-VIUSAGE-6/7, and `valueindexconsumer/SPECS.md` SPEC-VI-4 (the `trace:id` exclusion
precedent this generalizes).

---

## SPEC-VI-12: Early-stopping top-N-by-recency parity contract (plan-scan-fallback.md Phase 8)
*Added: 2026-07-12*

**Status: drafted incrementally, per plan-scan-fallback.md Phase 8's explicit sequencing** — this
entry was opened once Phase 2's `BuildSourceBounded` existed to back-reference, and is amended
(new back-refs added, never existing text weakened) as each phase lands its own early-stopping
function. As of this update, Phases 2-6/6b have landed and are back-referenced below; Phase 7
(removal of `RecentFirstBudget`/`DispatchBoundedRecentFirst`, task #190) is still in progress and
does not affect this entry's own claims — Phase 8's final sign-off (task #191, including the
mutation sweep and the "`MostRecent`'s existing topK path is unaffected" check) still awaits it.

**Contract:** for any query shape early-stopping resolution supports, the BOUNDED
(`limit`-threaded, early-stopping) resolution's returned result set is **exactly** the top-`limit`
entries by recency (newest-first) that an UNBOUNDED resolution over the same inputs would have
produced — never a same-size-but-silently-wrong subset. This is a genuinely new correctness claim,
distinct from SPEC-VI-1: SPEC-VI-1 governs only the on-disk chronological block-directory ordering
that every early-stopping consumer depends on as a *precondition*; SPEC-VI-12 is the correctness
claim about the *resolution algorithms* built on top of that precondition (the union/merge/anchor
logic deciding which entries survive into the bounded output, and in what order).

**Explicit scope exclusion (binding, confirmed against the landed Phase 6 mechanism):** this
contract does **not** cover structural index-driven query answering under tempo's default
per-block (block-sharded) dispatch. This is stronger than "no bounded answer" — per tempo's
`tempodb/encoding/vblockpack/value_index_structural_query.go` (external, cross-repo, confirmed by
direct read, 2026-07-12), `blockpack.ExecuteStructuralFromIndex` has no per-block
ownership/sourceRef restriction at all (Option A's multi-file trace materialization means a
structural match's spans can legitimately live in a different block than "this" one), so under
per-block dispatch N blocks overlapping one time slice would each independently return the SAME
complete answer rather than a partition of it — a correctness/cost defect that exists regardless
of early-stopping. `tryStructuralIndexFetch` (tempo-side) therefore only ever attempts the
index-driven answer when `indexOnly` is true — i.e. only inside a genuine `DispatchTimeSliced` job
(`structural_sharder.go`'s `structuralTimeSlicedJobsFunc`) — and tempo's own comment there states
this is now "a PERMANENT, correctness-required boundary, not an interim gate pending a future fix,"
explicitly because "a future early-stopping resolution change would make duplicate per-block jobs
non-deterministic at the limit boundary, strictly worse than today's wasteful-but-deterministic
duplication." Phase 5's `chooseDiscoverySeed` recency-ordering fix makes structural's OWN existing
early-stopping mechanism (`evalOneStructuralCandidateTrace`'s limit check) newest-first-correct
*when reached via `DispatchTimeSliced`* — SPEC-VI-12's parity contract DOES apply there — but
per-block dispatch itself is out of scope permanently, not pending a future extension.

**Back-refs (current):**
- `internal/modules/vibuilder/builder.go:BuildSourceBounded` (Phase 2, single-leaf early-stopping
  index resolution). Mirrors `BuildSource` but threads `limit` through to an early-stopping,
  newest-first resolution for the single-leaf case; a multi-leaf query (AND or OR — `collectLeaves`
  flattens both) falls through to the ordinary unbounded `lookupColumn` per leaf rather than
  silently under-reporting, so a not-yet-early-stopped shape still gets a correct answer.
  `limit <= 0` delegates to `BuildSource` directly (no early-stopping code path exercised). `disc`
  is type-asserted against `FileDiscovererNewestFirst`; a discoverer without newest-first support
  falls back to the unbounded path too — never a correctness violation, only a missed optimization.
  Watermark gating (#496 R7) and the match-all path are unchanged and out of scope for
  early-stopping (a match-all query has no selectivity concept to bound).
- `internal/modules/executor/metrics_trace.go:ViUnionNewestFirst` (Phase 3, multi-leaf OR
  early-stopping). A k-way heap merge (`container/heap`) over N per-leaf newest-first-ordered
  `VILookupResult` slices (Phase 2's `BuildSourceBounded` output per leaf), deduplicating by the
  same `(SourceRef, span key)` identity rule `viSpanCmp`/`viSortDedup`/`viUnionSorted` use, stopping
  once the deduplicated output reaches `limit`. Deliberately NOT a repeated pairwise
  `viUnionSorted` (which is key-sorted, not time-sorted, and would defeat early-stopping).
  Correctness of WHICH elements are included does not depend on the input sets actually being
  sorted newest-first — the heap visits every element of every set exactly once regardless of
  order — but this contract's top-limit-by-recency guarantee specifically DOES depend on it: only
  when every input set is genuinely newest-first (as `BuildSourceBounded`'s output is) does the
  merged output represent the true newest-limit union rather than an arbitrary same-size subset.
  `limit <= 0` means unbounded (every element of every set is eventually visited).
- `internal/modules/executor/metrics_trace_bounded.go:ViIntersectNewestFirstAnchored` (Phase 4,
  multi-leaf AND early-stopping, "anchor+confirm" design — the hardest phase, per this file's own
  package doc comment naming the landmine it avoids: independently early-stopping each AND leaf to
  its own newest-N and then intersecting can silently under-report, because a true intersection
  match can lie outside one leaf's own truncated top-N when that leaf is materially less selective
  than the intersection as a whole). Resolves ONE leaf (the anchor, chosen by the caller —
  `vibuilder.BuildSourceBounded` — for being cheap to over-fetch) newest-first and confirms each
  candidate against the remaining leaf(s) via a caller-supplied `confirmFn`, collecting up to
  `limit` confirmed matches and stopping `confirmFn` calls entirely once `limit` is reached. Returns
  `needMore=true` when `anchorResults` was exhausted before `limit` confirmed matches were found —
  the caller's signal to pull a wider anchor batch and retry; this function has no I/O of its own
  and cannot pull more data itself. Supporting primitives sharing this file/back-ref:
  `ViSpanIdentityKey`/`ViMembershipSet`/`ViConfirmAllSets` (the O(1) confirm-membership mechanism,
  reusing the SAME `(SourceRef, span key)` identity rule as `ViUnionNewestFirst`) and
  `viIntersectOrdered` (the eval-time, order-preserving multi-set intersection counterpart to
  `ViUnionNewestFirst`, for a flat multi-leaf AND already resolved into independently-correct
  per-leaf sets — preserves `sets[0]`'s own order rather than destroying it via a key-sorted
  merge-join).
- `internal/modules/executor/structural_index.go:chooseDiscoverySeed,groupVILookupResultsByTrace`
  (Phase 5, structural candidate ordering). `groupVILookupResultsByTrace` returns
  `map[[16]byte]traceSpanSet` — `traceSpanSet` wraps the existing per-trace span-address set
  together with a new per-trace max-recency (`maxTimeSec`) field so the two pieces of state cannot
  drift apart. `chooseDiscoverySeed` sorts candidate TraceIDs by `maxTimeSec` DESCENDING (was
  lexicographic-by-TraceID-bytes only), with lexicographic kept as a same-recency tiebreaker for
  determinism only. Structural's OWN early-stopping mechanism was already correct before this phase
  (`evalOneStructuralCandidateTrace` already checked `limit` and signaled done, the caller already
  broke on it) — the ONLY defect this phase fixes is feeding candidates in the WRONG order, so a
  limited query could stop after confirming lexicographically-early (not most-recent) matches.
  `intersectTraceIDSets`/`chooseCandidateTraceIDs` needed no change (already order-preserving /
  already call `chooseDiscoverySeed`). Per this entry's scope-exclusion clause above, this fix's
  benefit is reachable only via `DispatchTimeSliced` (`indexOnly=true`), never per-block dispatch.

Back-ref: `plan-scan-fallback.md` Phase 8 section (the mutation-sweep list and end-to-end oracle
test design — single-leaf, multi-leaf OR, multi-leaf AND, structural 2-node, each run through both
the old unbounded path as oracle and the new bounded path, exact top-limit equality required —
this entry formalizes). External cross-repo back-ref (informational, tempo-side, not enforced by
this repo's own tests): `tempo/tempodb/encoding/vblockpack/value_index_structural_query.go` (the
per-block-dispatch permanent-exclusion mechanism this entry's scope clause cites) and
`backend_block.go` (formerly the querier-owned `RecentFirstBudget` policy variable — confirmed
retired by Phase 7, see this entry's own Sign-off section below).

**Sign-off (Phase 8 final integration pass, 2026-07-12).** Confirmed directly, not merely
reported:

- **Phase 7 (task #190) landed cleanly in both repos.** `RecentFirstBudget` and
  `DispatchBoundedRecentFirst` have zero remaining live-code references in either
  `blockpack` or `tempo/tempodb` — every remaining hit (`matchall_query.go`,
  `structural_parity_golden_test.go`, `queryplan_test.go`, tempo's `backend_block.go`/
  `slice_errors.go`/`value_index_query.go`) is a comment describing the retired mechanism
  historically, not live code. `queryplan.DispatchStrategy` is now a genuine 2-value enum
  (`DispatchBlockSharded`, `DispatchTimeSliced` only); `SelectSearchStrategy`'s
  `LowSelectivity`/`UnknownSelectivity`-with-limit cases now report `DispatchBlockSharded`
  (early-stopping index resolution, Phases 2-5, now serves the case the third dispatch value
  used to exist for). Pinned by `queryplan_test.go:
  TestSelectSearchStrategy_NoLongerReturnsDispatchBoundedRecentFirst`. `go build ./...` is clean.
- **Full four-shape top-N-parity coverage exists, assembled from multiple test files rather
  than one single combined oracle (an accurate correction to this entry's earlier Back-ref
  paragraph, which described the design intent, not literally one file):**
  - Single-leaf + multi-leaf OR: tempo's `tempodb/encoding/vblockpack/phase8_oracle_test.go:
    TestPhase8_ExactOracleParity_SingleLeafAndOR` — a real `tempodb`-level `Fetch` call chain,
    a deliberately 3-way-partitioned fixture (so OR's union genuinely excludes a non-trivial
    "neither" subset), asserting exact top-N-by-recency trace-identity equality between the
    unbounded oracle path (`MaxTraces=0`) and the bounded path (`MaxTraces=N`). This file's own
    doc comment explicitly scoped itself to "non-AND... AND and structural added during the
    final Phase 8 integration pass" — the two bullets below are that addition, confirmed to
    already exist rather than still pending.
  - Multi-leaf AND: `internal/modules/vibuilder/builder_bounded_and_test.go` —
    `TestBuildSourceBoundedMultiLeafAND_AnchorConfirm_DoesNotUnderReport` (reproduces the
    landmine this whole phase exists to avoid: independently early-stopping each AND leaf and
    intersecting can silently under-report), `TestBuildSourceBoundedMultiLeafAND_ExactOracleMatch`,
    `TestBuildSourceBoundedMultiLeafAND_WrongAnchorStillCorrect` (robustness: correctness must not
    depend on the caller picking the "right" anchor leaf). Real end-to-end coverage in tempo:
    `tempodb/encoding/vblockpack/fetch_bounded_and_integration_test.go:TestFetch_MultiLeafAND_WithLimit_ExactNewestLimitAnswer_EndToEnd`.
  - Structural: `internal/modules/executor/structural_index_seed_ordering_internal_test.go:
    TestChooseDiscoverySeed_NewestFirst_ExactOracleMatch` (Phase 5, real write-path fixture,
    mutation-verified) plus tempo's `structural_indexonly_guard_test.go:
    TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch` (pins this entry's own
    permanent per-block-dispatch scope exclusion — confirming the exclusion is enforced, not
    merely documented).
- **`MostRecent`'s existing topK path (`SPEC-STREAM-7`/`SPEC-STREAM-8`, unrelated to and
  explicitly not touched by any phase of this plan) confirmed still passes unchanged** — pinned
  by `api_test.go:TestQueryTraceQL_MostRecent` (run directly, `go test -run
  'TestQueryTraceQL_MostRecent' -v .`, confirmed passing), replacing this bullet's earlier
  "corroborated by the clean build" phrasing, which cited no specific test.
- **Clarification (binding, requested by team-lead): `boundedAuthorized` was NOT removed by
  Phase 7 and is NOT the same thing as `RecentFirstBudget`.** tempo's `tryIndexFetch` (`tempodb/
  encoding/vblockpack/value_index_query.go:377`) still carries its own `boundedAuthorized bool`
  parameter, UNCHANGED by Phase 7 — it is the real gate `SPEC-VI-12`'s early-stopping resolution
  depends on: `boundedAuthorized` selects whether `tryIndexFetch` calls
  `BuildValueIndexSourceBounded` (the early-stopping path, Phases 2-4) or the ordinary
  `BuildValueIndexSource`. What Phase 7 actually removed is a DIFFERENT, similarly-named
  parameter: `declineOutcomeBounded` (a separate, downstream function) USED TO carry its own
  `boundedAuthorized` parameter, which it used to conditionally relay a routine decline to the
  now-deleted `RecentFirstBudget` raw-block-scan path. That relay parameter is what Phase 7
  dropped — confirmed by direct read of `declineOutcomeBounded`'s current doc comment (lines
  504-519 of `value_index_query.go`), which states this distinction explicitly: a decline
  reaching `declineOutcomeBounded` today means the bounded early-stopping resolution was already
  attempted upstream (via `tryIndexFetch`'s own still-live `boundedAuthorized` gate) and still
  found no coverage, so `declineOutcomeBounded` now unconditionally returns `ErrSearchNoCoverage`
  for a routine (non-`indexOnly`) decline, with nothing left to conditionally relay to.

Phase 8 (task #191) is complete as of this sign-off.

---

## SPEC-VI-13: `TruncateTimeValueToMillis` — the ONE shared write/read truncation function for span:start/span:duration (task #203, CRITICAL)
*Added: 2026-07-12*

**Contract:** `TruncateTimeValueToMillis(nanos uint64) uint64` returns `nanos / 1_000_000` — the
millisecond-truncation NOTE-VI-027 (issue #415) already documented as a deliberate, one-sided
write-time decision for `span:start`/`span:end`/`span:duration`'s stored value-index canonical
value. This function is the SINGLE authoritative implementation of that division: root
`valueindex_extract.go`'s `truncateTimeValueToMillis` (write side) and
`internal/modules/vibuilder/builder.go`'s `intOrDedicatedColType` (read side, gated by
`dedicatedColumnOverride.truncateMillis`) both call it directly rather than each dividing by
`1_000_000` independently.

**Why this matters as a binding contract, not merely a helper:** before task #203, only the write
side truncated; the read side (query-literal predicate construction) did not. A query's
raw-nanosecond literal compared against a millisecond-scale stored value is wrong by a factor of
10^6 — for realistic sub-second durations this makes the comparison wrong for virtually every
query, not merely imprecise at the margins. Both call sites MUST divide by the exact same factor
for a query threshold to mean the same thing as the value it is compared against.

**Placement rationale:** `valueindex` is a leaf package both root `blockpack`
(`valueindex_extract.go`) and `internal/modules/vibuilder` already import, in either direction,
with no cycle — making a single shared Go function possible here, unlike NOTE-VI-051's own
`span:start`-second-floor duplication across the blockpack/tempo-mrd repo boundary (a genuinely
separate Go module on the other side, where no single function can be shared and independent,
documented, coordinated duplication remains the only option).

**Non-goal:** this function does not, and cannot, restore sub-millisecond precision to a
value-index-backed duration/start comparison — that loss is NOTE-VI-027's own accepted trade-off
(a ~1000x cardinality reduction), unchanged by task #203; this function only ensures both sides of
the comparison lose precision the SAME way instead of one side losing it and the other not.

Back-refs: `internal/modules/valueindex/hash.go:TruncateTimeValueToMillis`, root
`valueindex_extract.go:truncateTimeValueToMillis` (delegates), `internal/modules/vibuilder/
builder.go:intOrDedicatedColType` (delegates). See `vibuilder/NOTES.md` NOTE-VI-107 for the full
discovery narrative and `vibuilder/SPECS.md` SPEC-VB-6 for the binding read-side contract.

---

## SPEC-VI-14: `TraceGroupIterator` / `NewDiskTraceGroupFileIterator` — lazy, block-at-a-time disk iteration over a v2 "VTG2" TraceGroup file (issue #500)
*Added: 2026-07-14*

**Contract:** `NewDiskTraceGroupFileIterator(ctx, path, checker) (TraceGroupIterator, error)`
(`disk_trace_iterator.go`) mirrors `NewDiskBucketFileIterator`'s (SPEC-VI-7's disk-iterator
sibling) construction/decode/ownership contract exactly, for the TraceGroup format: eagerly
decodes header magic, footer, string table, and block directory (bounded cost regardless of
file size), then decodes forward until it finds a block with at least one (optionally
retention-filtered) group. `(nil, nil)` — never a typed-nil pointer wrapped in the interface —
signals a bad-magic (not v2 "VTG2") file, the same skip-don't-abort signal
`NewDiskBucketFileIterator` uses for a legacy pre-v2 file. Peak decoded memory per input file is
bounded to one block, verified by `TestNewDiskTraceGroupFileIterator_CorruptBlockLazyAborts`
(TEST-VI-23) and `TestStreamCompactTraceGroups_MemoryBoundedRegardlessOfInputCount` (TEST-VI-25).

**`TraceGroupIterator` is intentionally NOT `GroupIterator`.** Read while implementing this
task: forcing `TraceGroup` through `GroupIterator`'s `BucketGroup`-typed `Peek()` would need a
lossy adapter, for no reuse benefit, because of two real differences:

1. Merge key shape: `BucketGroup`'s merge key `(TimeSec, CanonicalValue)` is also each input
   file's own per-group uniqueness key (SPEC-VI-3); `TraceGroup`'s merge key is `TraceID` alone
   (`TimeSec` is deliberately excluded — `MergeTraceGroups`, `traceindex.go`, collapses every
   group sharing a `TraceID` regardless of `TimeSec` into one output group). A single input file
   can hold multiple consecutive groups sharing one `TraceID`, which SPEC-VI-3's per-file
   uniqueness assumption forbids for `BucketGroup`.
2. No `StringTable()` method: unlike `BucketBlockRef.SourceID` (a `uint16` meaningful only
   relative to its own file's `StringTable`), `SpanEntry.SourceRef` is already resolved to a
   plain string at decode time (`decodeTraceGroupAt` calls `table.Lookup` once, eagerly) — a
   decoded `TraceGroup` carries no further table-dependent state for a `StringTable()` accessor
   to expose.

`TraceGroupIterator`'s method set is therefore `Peek() (*TraceGroup, bool)`, `Advance(ctx)`,
`Err() error`, `Close() error` — no `StringTable()`. `diskTraceGroupFileIterator` also
implements the existing `StatsProvider` interface (`stream_compaction.go`) unmodified — no
TraceGroup-specific stats interface was needed.

Back-refs: `internal/modules/valueindex/disk_trace_iterator.go:TraceGroupIterator,
NewDiskTraceGroupFileIterator, diskTraceGroupFileIterator, filterDeadSpansBlock,
countTraceSpans`. Tests: `disk_trace_iterator_test.go` (TEST-VI-23).

---

## SPEC-VI-15: `StreamCompactTraceGroups` — heap-based k-way streaming merge for TraceGroup files (issue #500)
*Added: 2026-07-14*

**Contract:** `StreamCompactTraceGroups(ctx, iterators []TraceGroupIterator, groupsPerBlock int,
maxOutputBytes int64, tmpDir string, output func(path string) error) error`
(`stream_trace_compaction.go`) is the TraceGroup-format sibling of `StreamCompactBucketFiles`
(SPEC-VI-2): a heap-based k-way merge across N already-decoded, already-filtered
`TraceGroupIterator`s, emitting merged `TraceGroup`s in `(TraceID ASC, TimeSec ASC)` order, cut
into blocks of at most `groupsPerBlock` groups (`groupsPerBlock <= 0` defaults to
`shared.ValueIndexTraceGroupsPerBlock`). Output is staged through a local temp file exactly as
`StreamCompactBucketFiles` does — a `bufio.Writer` over a fresh `os.CreateTemp(tmpDir,
"vi-merge-out-*.tmp")` file written block-by-block, removed via defer after `output` returns —
and supports the same `maxOutputBytes` block-boundary-only rotation (NOTE-VI-077's role, mirrored
by `projectedTraceFileSize`).

**`tmpDir` is an explicit parameter, unlike `StreamCompactBucketFiles`' hardcoded
`os.CreateTemp("", ...)`.** The input-staging side (`writeLocalTempInput`,
`valueindexcompactor/diskstage.go`) already took a `dir` parameter; this output-side path did
not, until this task added it — so a future caller can point local staging at a specific mount
path (e.g. a PVC) instead of always assuming the container's default `/tmp`. Pass `""` to fall
back to `os.TempDir()`, matching `os.CreateTemp`'s own empty-string convention. See NOTE-VI-109
for why this matters: an `emptyDir`-based fix was considered and rejected (node-local disk risk
with `value-index-compactor`'s 20 replicas potentially co-scheduled) — the actual disk-
provisioning mechanism is a separate, ongoing infra decision, and this parameter exists so this
package's code does not need to change again once that decision lands.

**Merge semantics match `MergeTraceGroups` exactly** (verified by the equivalence test,
`TestStreamCompactTraceGroups_MatchesMergeTraceGroups`, TEST-VI-24): `TimeSec` of a merged group
is the minimum across every contributing group; spans are deduplicated by `SpanID` with
first-occurrence-wins semantics, where "first" means the same order `MergeTraceGroups` itself
processes inputs — ascending original-input-slice index, then each input's own natural
`(TraceID, TimeSec)`-sorted encounter order.

**Collection loop is a single combined phase, not `stream_compaction.go`'s two-phase split.**
`collectContributionsAtKey`/`advanceContributors` (SPEC-VI-3) rely on the merge key being unique
per input file, which does not hold for `TraceGroup` (see SPEC-VI-14). This file's
`collectTraceContributionsAtKey` instead pops the heap root, takes its group as a contribution,
advances that one iterator immediately, and re-pushes it onto the heap if it is still live —
including when it is STILL at the same `TraceID` (a different `TimeSec`), which the same
while-loop condition then pops again in the same call. Contributions are ordered `(priority ASC)`
via `traceIterEntry.priority` (each iterator's original slice index), making the collected order
exactly match `MergeTraceGroups`' own input-order iteration without a separate sort step.

**Addendum (2026-07-14, issue #501): this contrast is now historical, not live.** The
distinguishing claim above ("not `stream_compaction.go`'s two-phase split") described
`stream_compaction.go`'s `collectContributionsAtKey`/`advanceContributors` as they existed at
SPEC-VI-15's original writing (2026-07-14). Issue #501 (later the same day) changed
`collectContributionsAtKey` to the SAME combined pop-advance-repush shape described in this
entry, for an analogous reason — `splitBucketGroupBySpanCap` (SPEC-VI-16) can now emit multiple
same-key sibling `BucketGroup`s within one file, so BucketGroup's own per-file key uniqueness
assumption no longer holds unconditionally either (SPEC-VI-3, amended). `advanceContributors`
was deleted outright as part of that change — it is no longer live code, and this entry's
original paragraph should be read as "distinct from `stream_compaction.go`'s PRE-#501 two-phase
split," not as describing `stream_compaction.go`'s current behavior. See `NOTES.md` NOTE-VI-111
for the #501-side design rationale.

**Output tail assembly reuses `traceindex.go`'s existing helpers unmodified** —
`encodeTraceBlock`, `traceBlockTimeRange`, `EncodeStringTable`, `appendTraceBlockIndex` — rather
than duplicating or refactoring them; only the footer-byte-layout (`writeTraceFileTail`) is
newly written, mirroring `encodeTraceGroups`'s own footer assembly byte-for-byte but writing
incrementally to a `bufio.Writer` instead of one in-memory `[]byte`, exactly as
`writeBucketFileTail` (`bucketfile.go`) does for the BucketGroup format. No changes were made to
`traceindex.go` itself.

Back-refs: `internal/modules/valueindex/stream_trace_compaction.go:StreamCompactTraceGroups,
traceIteratorHeap, collectTraceContributionsAtKey, mergeTraceGroupsAtKey, traceStreamWriter,
writeTraceFileTail`. Tests: `stream_trace_compaction_test.go` (TEST-VI-24),
`stream_trace_compaction_scaling_test.go` (TEST-VI-25).

---

## SPEC-VI-16: `bucketGroupSpanCount`/`splitBucketGroupBySpanCap` — per-`BucketGroup` span-ref cap and sibling-splitting mechanism (issue #501)
*Added: 2026-07-14*

**Contract:** `bucketGroupSpanCount(g *BucketGroup) int` returns the total number of `(ref,
traceID)` span entries in `g` — the cheap size proxy this cap uses (`sum of len(r.Spans) across
g.Refs`), never a byte-size estimate. `splitBucketGroupBySpanCap(g BucketGroup, maxSpans int)
[]BucketGroup` splits `g` into one or more sibling `BucketGroup`s, each with
`bucketGroupSpanCount <= maxSpans`, every sibling sharing `g`'s exact `TimeSec`/`CanonicalValue`.
`maxSpans <= 0` is a no-op (mirrors `SplitIntoBlocks`' own `groupsPerBlock <= 0` convention):
returns `[]BucketGroup{g}` unchanged, no copy performed.

**Packing strategy (binding, not an implementation detail free to change without re-verifying
`TestSplitBucketGroupBySpanCap`):** greedy first-fit by `Refs` order — whole refs are appended to
the current sibling until the next ref would exceed `maxSpans`, then the sibling is cut. A single
`BucketBlockRef` whose own `Spans` slice already exceeds `maxSpans` is itself split across
siblings (same `SourceID`/`Ref`, disjoint `Spans` subsets) — this is the specific shape that
caused issue #501's OOM (one hot value referencing one data block via many distinct traces, not
many distinct refs). Neither `Refs` order nor any `Spans` slice's internal order is ever
reshuffled, so a caller relying on `sortBucketBlock`'s pre-existing ordering guarantee sees no
extra work.

**No-loss/no-duplication guarantee (binding):** the union of every returned sibling's `Spans`,
keyed by `(SourceID, Ref, TraceID, SpanIndexes)`, is exactly equal to the input `g`'s own spans —
zero spans dropped, zero duplicated, regardless of which packing branch (whole-ref vs.
split-single-ref) produced a given sibling. Pinned by `TESTS.md` TEST-VI-26.

**Enforcement points (both, binding — both apply the SAME cap constant,
`shared.ValueIndexBucketGroupMaxSpanRefs`, for a given group's siblings to correctly
re-consolidate across write→merge→merge generations):**
1. **Write time** (`writer.go:assembleBucketWithCap`, called by the production
   `FlushBucket`/`flushBucketWithCap` path): a `groupBuild`'s `spanCount` is checked before every
   genuinely new `(ref, traceID)` pair is added (never on an additional `SpanIndexes` append to an
   already-existing `SpanRef`, which does not grow `spanCount`); once `spanCount >=
   maxSpansPerGroup`, the current group is sealed into `sealedGroups` and a fresh `groupBuild` is
   started for the same key, with its own fresh `spanIdx`/`refIdx` maps (`spanKeyLocal2` is scoped
   per-`groupBuild` instance, not per-file, specifically so a sealed sibling's slot indices can
   never collide with the new sibling's).
2. **Merge time** (`stream_compaction.go:mergeGroupsAtKey`): the existing union-building loop
   (unbounded per this function's own signature, but now itself bounded by the write-time
   enforcement point above applied recursively across generations — SPEC-VI-2's Addendum) is
   unchanged; only the tail changed — instead of returning one `BucketGroup`, it calls
   `splitBucketGroupBySpanCap(out, maxSpansPerGroup)` and returns the result directly, so
   `mergeGroupsAtKey`'s own return type is `[]BucketGroup`, not `BucketGroup`.

**Caller-visible consequence (see SPEC-VI-3's amendment):** `StreamCompactBucketFiles`'s main
loop now runs its add+rotate step (`addMergedGroupAndMaybeRotate`) once per sibling a merged key
produces, not once per key — extracted specifically to keep this branching out of the main loop's
own cyclomatic complexity.

Back-refs: `internal/modules/valueindex/bucketfile.go:bucketGroupSpanCount,
splitBucketGroupBySpanCap`, `internal/modules/valueindex/writer.go:assembleBucketWithCap,
flushBucketWithCap`, `internal/modules/valueindex/stream_compaction.go:mergeGroupsAtKey,
addMergedGroupAndMaybeRotate`, `internal/modules/blockio/shared/constants.go:
ValueIndexBucketGroupMaxSpanRefs`. See SPEC-VI-2 (Addendum), SPEC-VI-3 (Amended), `NOTES.md`
NOTE-VI-111, `TESTS.md` TEST-VI-26/TEST-VI-27.

---

## SPEC-VI-17: `ValueIndexBucketBlockMaxBytes` — byte-size-aware early block cutting in `streamOutputWriter` (Approach B, issue #501)
*Added: 2026-07-14*

**Contract:** `streamOutputWriter.add` (`stream_compaction.go`) cuts the current output block
once EITHER `len(w.pending) >= w.groupsPerBlock` OR `w.pendingBytes >= w.maxBlockBytes` is true —
whichever threshold is reached first. `maxBlockBytes` is threaded through `newStreamOutputWriter`
and defaults to `shared.ValueIndexBucketBlockMaxBytes` (16 MiB) at both of
`StreamCompactBucketFiles`'s production call sites (`newBucketOutputFile`). `pendingBytes` is a
running, cheap, **directionally-correct, not exact** estimate
(`estimateBucketGroupBytes`: `len(CanonicalValue) + len(Refs)*32 + bucketGroupSpanCount(g)*24`),
reset to `0` alongside `w.pending`'s reallocation (`w.pending = make([]BucketGroup, 0,
w.groupsPerBlock)`, inside `streamOutputWriter.flush`) whenever a block is flushed — never a full
per-`add`-call re-encode.

**Relationship to SPEC-VI-16's per-group cap (binding — this mechanism is now secondary
hardening, not the primary defense against one group dominating a block):** since SPEC-VI-16
caps any individual group's own span-ref count, a single pathologically large group can no longer
by itself dominate a block's decoded bytes the way it could pre-#501. `ValueIndexBucketBlockMaxBytes`
protects against the OTHER shape — many individually-capped-but-still-moderately-sized groups
landing in one block and collectively exceeding a healthy per-block byte budget — its original
role from the #501 brainstorm, now narrower in scope than originally conceived because SPEC-VI-16
already closed the "one huge group" case.

**Not blocked on the evidence-gathering step that sized `ValueIndexBucketGroupMaxSpanRefs`** — 16
MiB is a reasonable round-number default for this secondary hardening measure, independent of the
per-tenant span-count histogram that determined the primary cap's value (`NOTES.md` NOTE-VI-111).

Back-refs: `internal/modules/valueindex/stream_compaction.go:streamOutputWriter,
newStreamOutputWriter, estimateBucketGroupBytes`, `internal/modules/blockio/shared/constants.go:
ValueIndexBucketBlockMaxBytes`. See `TESTS.md` TEST-VI-29.

---

## SPEC-VI-18: Per-key merge-buffer disk-spill mechanism — transient, invisible to output shape (issue #503)
*Added: 2026-07-15*

**Contract:** `mergeGroupsAtKey`'s transient per-key union-building buffer (SPEC-VI-2's Addendum,
2026-07-15) is bounded by spilling to disk once its estimated size crosses
`shared.ValueIndexMergeBufferSpillBytes` (4 MiB, real-data-confirmed against two S3 samples —
see `blockio/shared/constants.go`'s own comment on the constant). Spilling is **purely a
transient memory-management technique, invisible to output-group semantics** — it changes HOW
the per-key union is built, never WHAT gets split/emitted. `splitBucketGroupBySpanCap`'s
SPEC-VI-16 re-consolidation guarantee is preserved by construction, not by argument: the
streaming incremental packer (`streamingSplitPacker`) is a proven-equivalent, byte-for-byte
behavioral port of `splitBucketGroupBySpanCap`'s own greedy-first-fit logic
(`TestStreamingSplitPacker_MatchesSplitBucketGroupBySpanCap`, TESTS.md), so a spilled key and a
never-spilled key with identical contributions produce byte-identical output
(`TestMergeGroupsAtKey_SpillPathEquivalence`, TESTS.md).

**Spill record format** (`keySpillRecord`, `keymerge_spill.go`): a flat, ephemeral,
process-internal wire record — `sourceID[2] + BlockRef[5] + traceID[16] + idx_count[2] +
idx[2]*idx_count` — one record per contributed `(ref, span)` pair, sorted by `(sourceID,
ref.PageNum, TraceID)` (the same total order `mergeGroupsAtKey`'s own final sort has always
produced). Spill files are created via `writeKeySpillChunk` (`os.CreateTemp(tmpDir,
"vi-keymerge-*.tmp")`, mirroring `runspill.go`'s `writeRun`) and fully consumed within the SAME
`mergeGroupsAtKey` call that created them — this format therefore has NO versioning/compat
concerns, unlike `runspill.go`'s `rawEntry` format (contrast `spillV2Flag`/`spillV4Flag`).
`tmpDir` must be the caller-supplied PVC-backed scratch directory, never a silent fallback to
`os.TempDir()` — a nonexistent/unwritable `tmpDir` returns a clear error and aborts the whole
key (Edge Case 3, plan.md), rather than defeating the reason `tmpDir` threading exists.

**Dedup rule across a spill boundary (correctness-critical, deliberately DIFFERENT from
`runspill.go`'s own rule):** `reduceKeySpillRecords`'s streaming k-way merge UNIONS
`SpanIndexes` for a duplicate `(sourceID, ref, TraceID)` triple across chunks/tail — a
`map[uint16]struct{}` scoped to ONLY the current TraceID being built, freed the instant the
TraceID changes — never collapses to a single ("first-wins") entry the way `runspill.go`'s
`sameEntry`/`mergeRuns` dedup does. `TestReduceKeySpillRecords_UnionsSpanIndexesAcrossChunks` and
`TestMergeGroupsAtKey_UnionDedupNotFirstWinsAcrossSpillBoundary` (TESTS.md) pin this rule with a
mutation check confirming a naive first-wins port would be caught.

**Memory-bound formula (mutation-tested, NOTE-VI-113):** peak per-key merge-buffer memory is
bounded by O(one spill-chunk's records, itself bounded by
`ValueIndexMergeBufferSpillBytes`/estimated-record-size — **mutation-confirmed load-bearing**)
plus O(`maxSpansPerGroup`, one sibling group being incrementally packed — **mutation-confirmed
load-bearing**) — independent of K and of the key's cumulative real fan-in. A third term, one
ref's own per-TraceID union bounded by `shared.MaxSpans` (`blockio/shared/constants.go:573` —
**cited by name, a derived cross-package consequence of the base blockio format's row-count
limit, NOT re-derived or independently enforced by `valueindex`** —
`TestBucketBlockRef_CannotAddressABlockWithSpanCountAboveMaxSpans` pins the CURRENT real-world
coupling via the actual decode-time enforcement, TESTS.md TEST-VI-36),
is a true structural fact but is honestly **not** mutation-confirmed load-bearing against a
"shared, never-reset union map" bug class — `SpanIndexes` being `uint16` caps any such map at
65,536 entries regardless of fixture scale, far below `shared.MaxSpans` — so that class of bug
manifests as a correctness defect (covered by the dedup-rule tests above), not a provable memory
violation via this term. See SPEC-VI-2's Addendum for the full honest framing.

**Cleanup (every exit path, including error/cancellation):** every spill chunk created for a key
is removed via a `defer` registered immediately after `accumulateAndSpillKeyRecords` returns —
success or error — so a partial failure partway through accumulation (string-table overflow, a
disk write error, or `ctx` cancellation) never leaves an orphaned `vi-keymerge-*.tmp` file.
`ctx.Err()` is checked once per ref boundary inside the accumulation loop, so a genuinely huge
hot-key spill can still be cancelled promptly rather than running to completion after
cancellation was requested. `sweepOrphanedMergeTempFilesIn` (`temp_cleanup.go`) additionally
globs `vi-keymerge-*.tmp` as a backstop against a crash mid-key-spill.

**Non-goal:** `stream_trace_compaction.go`/`collectTraceContributionsAtKey`/
`mergeTraceGroupsAtKey` are explicitly NOT touched by this spec or its implementation — per
SPEC-VI-14's "mirror, don't share" precedent, TraceGroup's own transient-buffer risk (if any) is
deferred to a separate follow-up issue pending its own real-data evidence.

Back-refs: `internal/modules/valueindex/keymerge_spill.go` (`keySpillRecord`,
`writeKeySpillRecord`, `readKeySpillRecord`, `keySpillChunk`, `writeKeySpillChunk`,
`keySpillChunkReader`, `estimateKeySpillRecordBytes`, `streamingSplitPacker`,
`reduceKeySpillRecords`), `internal/modules/valueindex/stream_compaction.go:mergeGroupsAtKey,
mergeGroupsAtKeyWithSpillThreshold, accumulateAndSpillKeyRecords`,
`internal/modules/valueindex/temp_cleanup.go:sweepOrphanedMergeTempFilesIn`,
`internal/modules/blockio/shared/constants.go:ValueIndexMergeBufferSpillBytes, MaxSpans`. See
SPEC-VI-2 (Addendum), SPEC-VI-16, SPEC-VI-14, `NOTES.md` NOTE-VI-113, `TESTS.md` (new entries,
Phases 2-8). Issue #503.
