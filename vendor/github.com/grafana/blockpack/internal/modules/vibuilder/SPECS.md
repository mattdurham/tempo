# vibuilder — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/vibuilder` package. It complements `NOTES.md` (design rationale) and
`TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

This file is created for the first time as part of issue #488 (read-path modernization, task
B-7) — prior to this, `vibuilder` had only a `NOTES.md`.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VB-N` (file-scoped per
SPEC-ROOT-009 — **deliberately distinct from `valueindex/SPECS.md`'s own `SPEC-VI-N` counter**,
even though `vibuilder` orchestrates `valueindex` types; reusing `SPEC-VI-N` here would collide
with that file's own module-local sequence, mirroring the precedent of `valuecounts` having its
own `SPEC-VC-N` prefix distinct from neighboring modules). IDs are assigned in ascending order
and never reused or renumbered; superseded entries are marked `[SUPERSEDED by SPEC-VB-N]` rather
than deleted. `vibuilder/NOTES.md`'s own entries continue to use the separate, shared `NOTE-VI-N`
counter (spanning `valueindex`/`valueindexcompactor`/`valueindexconsumer`/`executor`/`vibuilder`)
— this SPEC-VB-N convention applies only to this file and to `TESTS.md`'s parallel `TEST-VB-N`.

Next free ID: **SPEC-VB-8** (corrected — SPEC-VB-7 itself was already assigned/present below;
this counter had gone stale).

---

## SPEC-VB-1: `storeRangedSource` — `FileStore`→`RangedSource` per-key adapter
*Added: 2026-07-07*

**Contract:** `storeRangedSource` adapts vibuilder's `FileStore` (keyed by string) to
`valueindex.RangedSource` (no key parameter) for exactly one key, so
`valueindex.QueryBucketFileRanged` can be handed a plain `RangedSource` without the `valueindex`
package ever needing to know about `vibuilder`'s `FileStore` shape (issue #488, B-5).

```go
type storeRangedSource struct {
	store FileStore
	key   string
	size  int64
	sized bool
}
```

**`Size` caching invariant:** `Size()` is answered from the underlying `store.Size(key)` at most
once per `storeRangedSource` instance — the result is cached in `size`/`sized` and reused for
every subsequent call. This requires a pointer receiver, not a plain immutable value, because
both `QueryBucketFileRanged`'s own footer read (which calls `Size` to bound its footer `ReadAt`)
and `queryKeysRanged`'s separate `FilesRead`/`BytesRead` observability bookkeeping (NOTE-VI-039)
need the object's size, and an object-storage backend's `Size` is typically its own network
round trip (e.g. an S3 `HEAD`). Answering it twice per file would double that specific cost,
working directly against the I/O reduction issue #488 exists to deliver.

**`ReadAt` is a pure passthrough:** `storeRangedSource.ReadAt(p, off)` calls
`s.store.ReadAt(s.key, p, off)` with no caching or transformation — every ranged read
`QueryBucketFileRanged` issues (footer, string table, block directory, surviving block bodies)
becomes exactly one `FileStore.ReadAt` call at the same offset and length.

**Precondition:** one `storeRangedSource` instance is scoped to exactly one key for exactly one
`QueryBucketFileRanged` call — it is not intended to be reused across different keys or
concurrent callers of the same key (no internal synchronization).

Back-ref: `internal/modules/vibuilder/builder.go:storeRangedSource`. See `valueindex/SPECS.md`
SPEC-VI-7 (`RangedSource`'s own contract, which this type satisfies).

---

## SPEC-VB-2: `lookupColumn`/`lookupColumnAll` ranged-read I/O-reduction contract
*Added: 2026-07-07*

**Contract:** both `lookupColumn` (single-column, single-predicate path) and `lookupColumnAll`
(match-all-with-column-list path, issue #488 B-5 scope correction — both functions rewired, not
just `lookupColumn`) resolve a column's matching entries via `queryKeysRanged`, which:

1. Runs `valueindex.QueryBucketFileRanged` against every discovered key in parallel, bounded by
   `downloadConcurrency` (mirroring the pre-ranged-read `downloadAll`'s fan-out bound exactly —
   no change to the concurrency model itself, only to what each unit of work does per key).
2. Wraps each key in a fresh `storeRangedSource` (SPEC-VB-1).
3. **Preserves the existing `ErrFileNotFound` 404-skip semantics (NOTE-VI-041) exactly:** a
   per-key `ErrFileNotFound` (surfaced through `QueryBucketFileRanged`'s wrapped error chain via
   `errors.Is`, from either the adapter's `Size` or `ReadAt`) is treated as an empty miss and
   skipped — the compactor's write-then-delete cycle plus a stale file-listing cache can name a
   key that no longer exists; an absent file holds no postings, so dropping it cannot
   under-count. Any other per-key error aborts the whole query so the caller falls back to a
   full scan.
4. **`bytesRead` accounting reports full object size, NOT wire bytes — read this bullet before
   using this counter to measure #488's I/O-reduction impact.** `bytesRead` for a surviving key
   is that key's full object size, obtained via `storeRangedSource`'s cached `Size()` — this
   matches the pre-ranged-read contract's observability semantics *exactly* (bit-for-bit the same
   value `downloadAll` used to report), and is deliberately **NOT** the (typically far smaller)
   number of bytes `QueryBucketFileRanged`'s `ReadAt` calls actually transfer underneath.
   **This will surprise anyone who tries to observe #488's improvement by watching this counter
   — it will not move, by design, even on a query that now transfers a small fraction of a
   file's bytes.** Why: `NOTE-VI-039`'s "what did it cost to build this source" observability
   contract predates and is orthogonal to this I/O-reduction work; changing what `bytesRead`
   measures would silently break that contract's meaning for existing consumers (tempo's
   `blockpackBlock.tryIndexFetch` span stamping) without those consumers opting in. **The real
   wire-byte reduction is real and significant, but is only visible in store-side transfer
   metrics** (e.g. S3/minio request-count and byte-count counters at the storage backend), not in
   this application-level stat. **Recorded candidate follow-up (not attempted in #488, flagged
   for the issue's closing comment):** add a separate, additive ranged-wire-bytes counter (e.g.
   `ValueIndexBuildStats.RangedBytesRead` or similar) that sums the actual `ReadAt` lengths
   `QueryBucketFileRanged` issues, so #488's win becomes observable at the application layer too
   — out of scope here because it requires plumbing a new counter through `QueryBucketFileRanged`
   → `queryKeysRanged` → `BuildSource` → `SliceValueIndexSource`, a wider surface than this
   already-large rewire's blast radius.
5. Bounded-concurrency behavior for `lookupColumnAll`'s per-type-bucket loop is unchanged
   (`leafConcurrency`, NOTE-VI-050) — `queryKeysRanged`'s own internal fan-out is nested inside
   it exactly as `downloadAll`'s was before this rewire, preserving the documented worst-case
   `leafConcurrency * downloadConcurrency` concurrent-goroutine bound.

**I/O-reduction consequence (the point of #488):** a column whose predicate/time-window prunes
most of a value-index file's blocks (via `valueindex`'s file-level and block-level pruning,
`valueindex/SPECS.md` SPEC-VI-9/10) never pays for the pruned blocks' bytes over the network —
only the footer, string table, block directory, and surviving block bodies are fetched, in place
of the previous whole-file download. **This win is real at the network/storage-backend level even
though `bytesRead` (point 4 above) does not reflect it.**

**Explicitly NOT changed by this contract:** result content and ordering (pinned exactly by the
`TestLookupColumn_ParityWithRangedPath` golden-value test, `TESTS.md` TEST-VB-1); the 404-skip
vs. abort-on-real-error classification (NOTE-VI-041); the coverage contract (NOTE-VI-036); the
concurrency bounds (NOTE-VI-049/050); the meaning of `bytesRead` (point 4 above, NOTE-VI-039).

Back-ref: `internal/modules/vibuilder/builder.go:lookupColumn,lookupColumnAll,queryKeysRanged,storeRangedSource`.
See `NOTES.md` (`vibuilder`) NOTE-VI-084 for the B-5 rewire's design rationale and NOTE-VI-039's
addendum, and `valueindex/SPECS.md` SPEC-VI-10 (`QueryBucketFileRanged`'s own contract, which
this function calls per key).

---

## SPEC-VB-3: `LeafIndexable` — per-leaf value-index shape resolvability, independent of data presence
*Added: 2026-07-07 (issue #487, task T5b)*

**Contract:** `LeafIndexable(n *vm.RangeNode) bool` reports whether a single leaf has a shape
`buildPredicate` can represent as a `valueindex.Predicate` at all — single-value equality,
range/between, or regex — using the exact same decision `buildPredicate` makes for real
index-source construction (`LeafIndexable` is a thin wrapper: `_, _, ok := buildPredicate(&leaf{node:
n}); return ok`), so this can never drift from what `BuildSource` itself would do for the same leaf.

**Rules:**
- `n == nil` returns `false`.
- Returns `false` for: multi-value leaves (equality with more than one value), a `RequirePresent`
  leaf, and an empty leaf (no `Values`, `Min`/`Max`, or `Pattern`) — none of these have a
  `valueindex.Predicate` representation.
- Returns `true` for: a single-value equality leaf, a one- or two-sided range/between leaf, and a
  regex leaf with a non-empty `Pattern`.
- Performs no discovery/download I/O and is independent of whether any value-index files
  currently exist for the leaf's column — this is a SHAPE question ("could the index represent
  this leaf at all"), not a data-presence question ("does coverage exist right now"). Contrast
  with `BuildSource`'s own per-leaf handling, which additionally requires actual file discovery
  to succeed before a leaf counts as covered.

**Why exposed publicly:** `queryplan.AllLeavesIndexable` (queryplan's own `SPEC-QP-5`) needs to
apply this exact per-leaf shape decision to every leaf in a program, not just the "at least one"
condition `BuildSource`'s own `ok` return satisfies. Exposing `LeafIndexable` lets `queryplan`
reuse `buildPredicate`'s rules verbatim rather than re-deriving them and risking drift between
the two decisions.

Back-ref: `internal/modules/vibuilder/builder.go:LeafIndexable,buildPredicate`. See `NOTES.md`
NOTE-VI-085. Test: `leaf_indexable_test.go`. Issue #487.

## SPEC-VB-4: `ColumnWatermark`/`BuildSource`'s R7 coverage gate — both `src.Add` call sites (#496)
*Added: 2026-07-10*

**Contract:** `ColumnWatermark{Triggered bool, Done bool, WatermarkSec uint64}` is the
query-time-relevant coverage state for one non-dedicated, usage-triggered column mid-backfill
(#496 R7). `(w ColumnWatermark) CoversRange(minSec, maxSec uint64) bool`: `w.Done` → `true`
unconditionally; `!w.Triggered` → `false` unconditionally (never indexed, no coverage);
otherwise → `minSec >= w.WatermarkSec` (in-progress, newest-to-oldest fill: covered range is
`[WatermarkSec, now]`; only the query's OLDEST bound can create a coverage gap). The boundary
`minSec == WatermarkSec` covers (inclusive, not a strict `>`).

**This logic must stay identical to `viusage.BackfillState.CoversRange`
(`viusage/SPECS.md` SPEC-VIUSAGE-2)** — both express the same contract for the same underlying
registry state, via two independently-defined value types (see this file's own `watermark.go`
doc comment, and `viusage/NOTES.md` NOTE-VIUSAGE-7, for why they cannot share one type without
an import cycle: `vibuilder.BuildSource` is the actual consumer of this gate, and root
`blockpack` already imports `vibuilder`; `ColumnWatermark` cannot live in root without
`vibuilder` needing to import root back, and cannot live in `viusage` either since `vibuilder`
must not depend on `internal/modules/viusage` — this package's own dependency graph must stay a
leaf).

**Enforcement (`BuildSource(ctx, disc, store, prog, minSec, maxSec, watermarks
map[string]ColumnWatermark) (*executor.SliceValueIndexSource, bool, error)`) — the ENTIRE R7
enforcement point, at this file's `src.AddLeaf` call site, no other change to `BuildSource`'s
decision logic:**

```go
if wm, ok := watermarks[w.col]; ok && !wm.CoversRange(minSec, maxSec) {
    continue // leave uncovered; the executor's existing decline/fallback path fires
}
src.AddLeaf(w.idx, w.col, w.colType, results)
```

1. The leaf-predicate loop (per-leaf `RangeNode` resolution) — the sole remaining gate site.

**Correction (task #211, 2026-07-12): the "match-all/`Columns`-list branch" second gate site
described in the original version of this entry no longer exists.** That branch (`Nodes` empty,
`Columns` populated — the shape this entry originally attributed to `{} | rate()`) was removed
entirely: `internal/modules/executor/metrics_trace.go`'s `viMatchSpans` (the only consumer of a
source this package builds) already declines unconditionally whenever `Predicates.Nodes` is
empty, regardless of `Columns` — and, per direct investigation, this shape is NEVER produced by
the real compiler for a genuine match-all (`{} | rate()` compiles with `Predicates == nil`
entirely; `Nodes: nil, Columns: [...]` only ever arises from a compile-time decline, e.g. task
#210's zero-node-operand AND/OR guard). `BuildSource`/`BuildSourceBounded` now decline this
shape immediately, before ever consulting `watermarks` or performing any I/O — see this
package's own `builder.go` file doc comment for the full argument. `buildSourceBoundedColumnsOnly`
(the `BuildSourceBounded` twin of this now-removed branch) was deleted along with it.

`watermarks == nil` (the common case — dedicated columns, and every caller not participating
in #496's usage-triggered backfill) → both gates degrade to a no-op (`ok` is always `false` for
a nil map lookup) — `BuildSource` behaves EXACTLY as it did before #496, byte-for-byte, for
every existing caller that passes `nil`. A column present in `watermarks` whose `CoversRange`
is `false` is treated identically to "no VI files discovered at all for that column" — reusing
100% of the existing, already-tested decline/fallback machinery rather than inventing a second
decline path.

**`ok`'s return-value contract under the gate (binding — a second, independent finding from
the same mutation-check exercise that validated the gate itself, not merely a code-quality
nit): `BuildSource`'s returned `added`/`ok` must reflect whether a leaf was ACTUALLY `Add`ed,
never merely whether a leaf had a buildable predicate.** Before this fix, the leaf-predicate
branch set `added = true` whenever `len(work) > 0` (i.e., at least one leaf resolved to a
buildable predicate) — independent of whether the watermark gate above then skipped every
single one of those leaves' `src.Add` calls. That bug would have silently reintroduced a
false-complete answer through a SECOND path even with the `CoversRange` gate itself correctly
in place: the caller would see `ok=true` and a `SliceValueIndexSource` with zero covered
columns, mistaking "the index has no idea about any of these columns" for "the index confirms
zero matches" — the same class of bug R7 exists to prevent, just one layer further out. Fixed
via an `atomic.Bool anyLeafAdded`, set only inside the actual `src.Add` call (concurrent
leaves each run in their own `errgroup.Go` goroutine, hence the atomic rather than a plain
bool), with `added = true` set from `anyLeafAdded.Load()` after `g.Wait()`, not from
`len(work) > 0`. Discovered during A5's own mandatory mutation-check step (plan.md 4.8 step
3): removing the `CoversRange` gate alone was not sufficient to reproduce a clean pass/fail
signal until this second bug was ALSO fixed, because the stale `len(work) > 0` tracking would
have masked the adversarial test's intended failure mode on a naive first attempt. See
`NOTES.md` for the full writeup of this second finding.

Back-refs: `internal/modules/vibuilder/watermark.go:ColumnWatermark,CoversRange`,
`internal/modules/vibuilder/builder.go:anyLeafAdded`,
`internal/modules/vibuilder/builder.go:BuildSource` (sole remaining gate site, task #211). See `viusage/SPECS.md`
SPEC-VIUSAGE-2 (the duplicated-logic twin) and `NOTES.md` (this file) for the import-cycle
placement rationale.

## SPEC-VB-5: `LeafColumns` — per-leaf `{Column, ColType, Indexable}` enumeration, one entry per leaf occurrence (#496)
*Added: 2026-07-10*

**Contract:** `LeafColumns(prog *vm.Program) []LeafColumnInfo` enumerates every leaf's
`{Column string, ColType modules_shared.ColumnType, Indexable bool}` in `prog`'s predicate
tree, reusing `LeafIndexable`/`buildPredicate`'s exact per-leaf shape decision verbatim (never
independently re-derived — same reuse discipline `LeafIndexable`'s own doc comment already
establishes, SPEC-VB-3). Added for #496 (blockpack/#496)'s tempo-side B1 usage-recording
hook, which needs per-leaf `{column, indexable}` visibility that `AllLeavesIndexable`'s
aggregate boolean cannot provide — R3's decline-reason distinction requires knowing WHICH
column a leaf named and WHETHER its shape was indexable, not just whether every leaf in the
program collectively was.

**One entry PER LEAF OCCURRENCE, deliberately NOT deduplicated by column name (binding).** A
column referenced by two leaves (e.g. two comparisons against the same attribute) produces
TWO entries — mirroring `collectLeaves`' own per-leaf granularity, since that is exactly the
leaf set `LeafIndexable`/`buildPredicate` themselves operate over. **De-duplication, if a
caller needs it, is the CALLER's responsibility, not this function's** — #496's own
usage-recording hook needs to count "one distinct query referenced column X" (a per-query,
per-column decision, R3's specific usage-counting semantics), which is a policy choice
specific to that one consumer, not a property `LeafColumns` itself should bake in. Keeping
`LeafColumns` a faithful, unopinionated mirror of the raw leaf tree avoids assuming any one
consumer's counting rule is the only valid one — a hypothetical future caller might
legitimately want per-leaf (not per-column) granularity for a different purpose (e.g.
counting distinct predicate SHAPES against a column).

**`ColType` is exposed, not just the `Indexable` boolean**, because #496's usage registry
keys entries by `(Tenant, ColumnHash, ColumnType)` (`viusage/SPECS.md` SPEC-VIUSAGE-1) — a
caller recording a use needs the SAME resolved type `buildPredicate` uses, not merely a shape
verdict. `ColType` is the zero `modules_shared.ColumnType` when `Indexable` is `false`
(`buildPredicate` never resolves a type for a shape it rejects) or for a match-all leaf (no
predicate to type-check against).

**`Nodes`-empty/`Columns`-populated case:** a program with `prog.Predicates.Nodes` empty and
`.Columns` populated returns one entry per listed column with `Indexable: true` and a zero
`ColType` — mirrors `AllLeavesIndexable`'s own handling of this shape. **Correction (task #211,
2026-07-12):** this entry originally called this shape "match-all" (e.g. `{} | rate()`) and
cited `BuildSource`'s `lookupColumnAll` path as never rejecting it. Neither claim holds: a
genuine `{} | rate()` compiles with `prog.Predicates == nil` entirely (not this shape at all),
and `BuildSource`/`BuildSourceBounded` no longer call `lookupColumnAll` for this shape — they
decline it immediately (see SPEC-VB-4's own correction and `builder.go`'s package doc comment).
`LeafColumns` still reports `Indexable: true` here regardless, because its own caller (#496's
usage-recording hook) tracks "this query referenced column X" for backfill-triggering purposes,
a deliberately different and narrower question than "can this be answered from the index" —
unaffected by `BuildSource`'s own decline for this shape.

**Empty case:** a program referencing nothing at all (no `Nodes`, no `Columns`, or `prog ==
nil`/`prog.Predicates == nil`) returns `nil`.

**Root re-export:** `blockpack.LeafColumns`/`blockpack.LeafColumnInfo` (`valueindex_query.go`)
are a thin function wrapper and type alias respectively, mirroring `ColumnWatermark`'s own
re-export pattern (SPEC-VB-4) — root already imports `vibuilder` for
`BuildSource`/`ColumnWatermark`, so this adds no new dependency direction.

Back-refs: `internal/modules/vibuilder/builder.go:LeafColumns,LeafColumnInfo` (reuses
`collectLeaves`, `buildPredicate` — see SPEC-VB-3), `valueindex_query.go:LeafColumns,
LeafColumnInfo` (root re-export). See `viusage/SPECS.md` SPEC-VIUSAGE-1 (the
`(Tenant, ColumnHash, ColumnType)` key `ColType` serves) for the intended #496 B1 consumer.

## SPEC-VB-6: `valueAsColType` is column-name-aware — `dedicatedNumericColumnTypes` overrides the Int64 default for span:start/span:duration (task #203, CRITICAL)
*Added: 2026-07-12*

**Contract (binding — this is a correctness contract, not an optimization):** `valueAsColType(col
string, v vm.Value)` and its Int/Duration helper `intOrDedicatedColType(col string, d int64)` MUST
resolve a leaf's value-index column type and comparison value from the SPECIFIC column being
queried (`col`, always `n.Column`/`leaf.col` — never resolved independent of the column name), not
from the TraceQL literal's own type alone. Before this fix, every `vm.TypeInt`/`vm.TypeDuration`
literal mapped unconditionally to `modules_shared.ColumnTypeInt64` regardless of `col` — wrong for
`span:start`/`span:duration`, whose real on-disk value-index type is `ColumnTypeUint64`
(`writer_block.go`'s `feedSpanTiming`/`applySpanStart`/`applySpanDuration`, confirmed against a real
`CreateBlock`+`Reader` round trip). This made every `{duration > Xms}`/`{start > X}` value-index
lookup search the WRONG type-bucket directory, which never has any files for these columns —
`BuildSource`'s "Add even when empty" contract (NOTE-VI-036) then made this indistinguishable from
"the index confirms zero real matches," so the query always silently succeeded empty: never an
error, never the real matches, for every value-index-backed duration/start comparison in
production (a silent wrong-answer bug, not a decline).

**Two stacked corrections, both required (fixing only the first is NOT sufficient for correct
results — see NOTE-VI-107 for why):**
1. **Type-bucket correction:** `dedicatedNumericColumnTypes` maps `span:start`/`span:duration` to
   `ColumnTypeUint64`; every other column (not present in the map, or an attribute) keeps the
   unchanged `ColumnTypeInt64` default — zero behavior change for any column this bug did not
   affect.
2. **Unit correction, CORRECTED by task #204 (see SPEC-VB-7 — this bullet originally described
   task #203's own buggy fix; kept here, struck through in spirit, purely so this history is
   traceable):** ~~for a column whose override entry sets `truncateMillis`, the raw-nanosecond
   literal is divided via `valueindex.TruncateTimeValueToMillis` before being cast to `uint64`,
   and the original comparison operator is reused unchanged.~~ That approach is only
   mathematically sound for two of five comparison operators at exactly one threshold alignment
   each (SPEC-VB-7's decidability rule) — for every other (operator, threshold) pair it produced a
   silent wrong answer (a false negative for `>`, a false positive for `>=`/`<=`, both reproduced
   live and documented in SPEC-VB-7). The correct behavior — decide per-operator whether the
   comparison is even answerable from a millisecond bucket at all, and decline rather than guess
   when it is not — is now implemented by `decidableTimeBucketThreshold` and specified in
   SPEC-VB-7.

**A negative literal against a Uint64-backed column is left unindexable (`ok=false`), never
wrapped via `uint64(d)`** — the real column can never hold a negative value, so no real row could
satisfy such a comparison against a wrapped, huge threshold; the caller falls back to a full block
scan instead of risking a wrong predicate.

**`LeafIndexable` must pass `col: n.Column` when constructing its probe `leaf`** (not a zero-value
`leaf{node: n}`) — its own contract (SPEC-VB-3) requires it make "the exact same decision"
`buildPredicate` makes for the same leaf when driven through `collectLeaves` (which always sets
`col: n.Column`); omitting `col` here would silently skip the override for the equality-leaf branch
and drift from that guarantee specifically for the negative-literal edge case above.

**Audited and found NOT affected (every other intrinsic/dedicated numeric column):** `span:kind`
and `span:status` are genuinely `Int64` on disk (`feedSpanKind`/`feedSpanStatus`); every int-valued
attribute (including #496's `DefaultDedicatedColumns`, e.g. `span.http.response.status_code`) is
always `Int64` because OTLP's `AnyValue` always represents integer attribute values as `int64` —
there is no write path that stores an attribute as `Uint64`, and none of these columns go through
`truncateTimeValueToMillis` either (name-gated to `span:start`/`span:end`/`span:duration` only).
`span:end` has NO real on-disk column at all (NOTE-399: synthesized from `span:start` +
`span:duration` on read, only its range-index min/max is fed) — never written to the value index in
any type bucket, so it carries neither risk, though it remains subject to the separate,
already-documented "no VI files ever written for this column" gap (NOTE-VI-036), unrelated to this
fix.

**Regression tests (real write+read round trip, not hand-built structs):**
`TestBuildSource_RangePredicateBuilds`/`_SpanStart` (`builder_test.go`, this package) pin the fix at
the `vibuilder` unit level with fixtures pre-truncated to milliseconds (mirroring the real write
path exactly, not the raw nanosecond span value). The end-to-end pin lives in tempo:
`tempodb/encoding/vblockpack/duration_intrinsic_divergence_local_test.go`'s
`TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage`,
`TestFetch_DurationComparisons_SameClassification_FullCoverage`, and
`TestFetch_SpanStartComparison_FindsRealUint64MillisecondTruncatedCoverage` — all three go through
a real `CreateBlock` write + real `blockpackBlock.Fetch` read, and were mutation-verified (reverting
this fix reproduces each test's original failure mode; restoring it passes them again).

Back-refs: `internal/modules/vibuilder/builder.go:valueAsColType,intOrDedicatedColType,
dedicatedNumericColumnTypes,dedicatedColumnOverride,LeafIndexable`. See NOTE-VI-107 (this file's
NOTES.md) for the millisecond-truncation half of the fix, and
`internal/modules/valueindex/hash.go:TruncateTimeValueToMillis` (SPEC-VI, `valueindex/SPECS.md`)
for the shared truncation function both the write and read sides now call. **See SPEC-VB-7 for the
task #204 correction to this SPEC's original unit-correction bullet (point 2 above) — floor-
truncating both sides and reusing the operator unchanged, as originally specified here, is a wrong-
answer bug at almost every non-degenerate threshold.**

## SPEC-VB-7: Millisecond-bucket comparison decidability — `decidableTimeBucketThreshold` (task #204, CRITICAL — corrects SPEC-VB-6 point 2 / NOTE-VI-107)
*Added: 2026-07-12*

**Supersedes SPEC-VB-6's original point 2.** Task #203's unit correction floor-truncated BOTH
the stored bucket and the query threshold to the same millisecond granularity and reused the
original comparison operator unchanged. That is mathematically sound for exactly two of the five
comparison operators, at exactly one threshold alignment each — for every other (operator,
threshold) combination it silently produced a wrong answer. Both directions were reproduced live
with real tests during #203's own holistic review: a false negative (`{duration > 1ms}` silently
dropping a real 1.5ms span) and a false positive (`{duration >= 1.6ms}` incorrectly matching that
same 1.5ms span).

**Contract (binding — this is a correctness contract, not an optimization):** for span:start /
span:duration (any column whose `dedicatedColumnOverride.truncateMillis` is set), a range-bound
Int/Duration literal MUST be resolved through `decidableTimeBucketThreshold(nanos uint64, op
timeCompareOp) (bucket uint64, ok bool)` before a `valueindex.RangePredicate` or
`BetweenPredicate` is built against it. `ok=false` means the comparison is genuinely
UNDECIDABLE from the stored millisecond bucket alone — the caller (`intOrDedicatedColType`) MUST
leave the leaf unindexable (`ok=false` propagates through `valueAsRangeColType` →
`buildRangePredicate`/`buildPredicate` → `BuildSource`, leaving the column uncovered, the exact
same "decline, fall back to a full scan" convention `LeafIndexable` already uses for every other
unsupported predicate shape). **Never build a predicate from a bucket value when
`decidableTimeBucketThreshold` reports `ok=false`** — that is precisely task #203's bug.

**The decidability rule** (derived from first principles: a stored bucket B represents an unknown
real value in `[B*1e6, (B+1)*1e6 - 1]` nanoseconds; let `Tq = T div 1_000_000`, `Tr = T mod
1_000_000` for a raw-nanosecond threshold T):

| Operator | Decidable iff | Decidable comparison (unchanged op, threshold=Tq) | Practical frequency |
|---|---|---|---|
| `>=` | `Tr == 0` (T is millisecond-aligned) | `B >= Tq` | common — round-ms thresholds |
| `<`  | `Tr == 0` | `B < Tq` | common — round-ms thresholds |
| `>`  | `Tr == 999_999` (T is 1ns below the next ms) | `B > Tq` | vanishingly rare in practice |
| `<=` | `Tr == 999_999` | `B <= Tq` | vanishingly rare in practice |
| `==` | never — no value of Tr makes it decidable | (always decline) | never |

**Why `>=`/`<` and `>`/`<=` are decidable at OPPOSITE alignments, not the same one:** a
millisecond-aligned threshold sits exactly on a bucket's LOWER edge. `>=`/`<` treat that edge
inclusively/exclusively in a way that cleanly partitions every bucket (the aligned bucket itself
resolves cleanly). `>`/`<=` treat that same edge the other way — the aligned bucket's lower-edge
value itself disagrees with the rest of the bucket, so alignment at the LOWER edge is exactly the
one case that stays ambiguous for `>`/`<=`; they are only decidable at the UPPER edge instead
(`Tr == 999_999`). Rounding direction cannot rescue both operator pairs at once — adjusting which
edge is "inclusive" only swaps which pair is decidable at which alignment; it can never eliminate
the single-bucket gap for both pairs simultaneously. This is a genuine, permanent information
loss from the write-side truncation (NOTE-VI-027), not a rounding bug.

**Equality is unconditionally undecidable, regardless of alignment:** the true-match region for
`==` is a single exact nanosecond value, which a 1,000,000-wide bucket interval can never
represent exactly (unlike `>=`/`<`/`>`/`<=`, whose true/false regions are half-open intervals that
CAN align with a bucket boundary). `buildPredicate`'s equality branch always resolves `ok=false`
for span:start/span:duration.

**Between (two-sided range) predicates:** `buildRangePredicate`'s two-sided branch resolves each
bound independently through `decidableTimeBucketThreshold` under its OWN operator (derived from
`MinInclusive`/`MaxInclusive`), then normalizes into `NewBetweenPredicate`'s inclusive-inclusive
shape via `betweenTimeBucketBounds` (a GT lower bound becomes `bucket+1`; an LT upper bound
becomes `bucket-1`).

**Correction (task #206 holistic review — this paragraph originally described the two bounds'
decidable alignment backwards):** the lower bound's exclusive operator is `>` (GT, decidable
only at the rare `Tr==999_999` alignment per the table above), but the upper bound's exclusive
operator is `<` (LT, decidable at the COMMON `Tr==0` alignment, same as `>=`) — NOT the same
rare alignment as the lower bound. The two bounds' exclusive operators are decidable at
OPPOSITE alignments, exactly like the standalone one-sided case above; there is nothing special
about being the second bound of a `between` that changes which alignment its own operator needs.
The original wording ("declines whenever EITHER bound uses an exclusive comparison at a
non-`Tr==999_999` threshold") incorrectly applied the lower bound's rare-alignment requirement to
the upper bound too. The code itself (`betweenTimeBucketBounds`'s callers each pass the bound's
own correct `timeCompareOp` to `decidableTimeBucketThreshold` independently) was never affected by
this — only this prose description was backwards.

**Reachability (task #206 investigation, CRITICAL finding): this two-sided branch
(`n.Min != nil && n.Max != nil`) is DEAD CODE for every real TraceQL query today.**
`traceql_compiler.go`'s `extractTraceQLNodes` ALWAYS decomposes an AND of two range comparisons
on the same column (e.g. `{ duration > 1ms && duration < 2ms }`) into TWO SEPARATE top-level leaf
`RangeNode`s (one `Min`-only, one `Max`-only) — confirmed by direct inspection of the compiled
program's `Predicates.Nodes` for representative queries, not assumed. There is no TraceQL syntax
and no compiler code path that produces a single `RangeNode` with both `Min` and `Max` set
simultaneously; `buildRangePredicate`'s `case n.Min != nil && n.Max != nil` therefore has no live
caller. It is kept rather than deleted because it is harmless, independently correct (per the
corrected alignment description above), and cheap to keep as defensive coverage should a future
compiler change ever produce a genuine combined bound. **There is currently NO test at all —
neither a real-query end-to-end test nor even a direct unit-level call with a hand-built
`*vm.RangeNode` carrying both `Min` and `Max` — exercising this branch** (confirmed by searching
`internal/modules/vibuilder/*_test.go` for any construction combining both fields); it is untested,
not merely "tested only synthetically." See `internal/modules/executor/metrics_trace.go`'s `SliceValueIndexSource
.AddLeaf`/`LookupLeaf` (issue #206) for the actual, reachable same-column-AND bug this
investigation was chasing — a same-column AND range query is real and common, it just resolves via
TWO leaves, not this branch.

**Regression tests (real write-path, task #204):**
- `internal/modules/vibuilder/decidability_test.go`:
  `TestDecidableTimeBucketThreshold_MatchesBruteForceGroundTruth` brute-forces every one of the
  1,000,000 real nanosecond values a bucket could represent for a sweep of (bucket, remainder,
  operator) combinations and asserts `decidableTimeBucketThreshold`'s verdict matches the ground
  truth exactly — not merely recorded expected outputs.
  `TestDecidableTimeBucketThreshold_TruthTable` pins the concrete named cases from this table,
  including both original bug reports.
- `valueindex_boundary_decidability_test.go` (root package): end-to-end, through the REAL write
  path (`blockpack.NewWriter` → `ExtractValueIndexEntries` → `valueindex.Writer.FlushBucket` →
  `vibuilder.BuildSource`), with five real spans at exact durations (1.000/1.400/1.500/1.999/2.000
  ms) straddling the 1ms/2ms bucket boundaries. Covers a decidable and a non-decidable threshold
  for every operator (`>`, `>=`, `<`, `<=`, `==`), including both original bug reports verbatim
  (`{duration > 1ms}` and `{duration >= 1.6ms}`).
- Mutation-verified: temporarily reverting `decidableTimeBucketThreshold` to task #203's original
  "floor both sides, keep the operator" behavior reproduces BOTH documented wrong-answer shapes
  exactly — the `>1ms` case drops the real 1.4/1.5/1.999ms spans (false negative) and the
  `>=1.6ms` case incorrectly includes the real 1.0/1.4/1.5ms spans (false positive). Restoring the
  fix passes all cases again.

Back-refs: `internal/modules/vibuilder/builder.go:decidableTimeBucketThreshold,timeCompareOp,
intOrDedicatedColType,valueAsRangeColType,buildRangePredicate,betweenTimeBucketBounds`.
