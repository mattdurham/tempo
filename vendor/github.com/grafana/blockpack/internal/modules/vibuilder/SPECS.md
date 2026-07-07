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

Next free ID: **SPEC-VB-3**.

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
