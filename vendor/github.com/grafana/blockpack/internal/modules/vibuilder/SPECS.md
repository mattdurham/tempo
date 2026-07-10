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

Next free ID: **SPEC-VB-6**.

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
enforcement point, at BOTH of this file's `src.Add` call sites, no other change to `BuildSource`'s
decision logic:**

```go
if wm, ok := watermarks[w.col]; ok && !wm.CoversRange(minSec, maxSec) {
    continue // leave uncovered; the executor's existing decline/fallback path fires
}
src.Add(w.col, w.colType, results)
```

1. The leaf-predicate loop (per-leaf `RangeNode` resolution).
2. The match-all/`Columns`-list branch (`{} | rate()`-shaped queries) — a match-all query over
   a partially-backfilled column has the IDENTICAL partial-coverage risk as a leaf predicate
   and must be gated too; this is not an optional second application, both sites are required.

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
`internal/modules/vibuilder/builder.go:BuildSource` (both gate sites). See `viusage/SPECS.md`
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

**Match-all case:** a match-all query (`prog.Predicates.Nodes` empty, `.Columns` populated —
e.g. `{} | rate()`) returns one entry per listed column with `Indexable: true` and a zero
`ColType` — mirrors `AllLeavesIndexable`'s own match-all handling (`BuildSource`'s
`lookupColumnAll` path never rejects a column's shape in this case, but a match-all leaf has
no predicate to resolve a type from).

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
