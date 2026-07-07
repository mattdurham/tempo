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

Next free ID: **SPEC-VI-11**.

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

Back-refs: `internal/modules/valueindex/bucketmerge.go:SplitIntoBlocks`,
`internal/modules/valueindex/stream_compaction.go:BucketFileIterator`.

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

Back-ref: `internal/modules/valueindex/stream_compaction.go:StreamCompactBucketFiles`.

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
