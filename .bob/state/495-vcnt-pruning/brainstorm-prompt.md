Task: grafana/blockpack #495 — tempo-side VCNT listing-time pruning.

## Background

Follow-up to #494 (VCNT v2 filenames + time-cluster-based compaction, shipped in
blockpack@d2324a29..67c49cf1 / tempo@4d738a44..23a75f0f4). VCNT filenames now carry a genuine
wall-clock time range (`L<level>-<minSec>-<maxSec>-<id>.vcnt`), but two tempo-side consumers
that list/fetch VCNT files still treat filenames as opaque strings and fetch every file
unconditionally, applying the query window only after full decode:

1. `modules/frontend/vcnt_fetch.go`'s `fetchVCNTSection` — lists via `backend.RawReader.Find()`,
   no filename-based filtering, applies the window only after `blockpack.ClassifyProgramVCNTWithDetail()`.
2. `tempodb/encoding/vblockpack/cube_backfill.go`'s `buildVCNTSection` — lists via
   `valueIndexStore.List()`; its own comment explicitly documents today's contract ("VCNT
   filenames... carry no embedded time range... so all of a column's .vcnt files are fetched")
   — this comment is stale as of #494 and needs updating regardless of whether pruning lands.

## Work required (from the issue)

1. Add filename parsing (blockpack's current VCNT v2 parsing API — confirm exact function name
   post-#494/#496, since blockpack's own API surface changed twice since the issue was written;
   check for `VCNTParseFilenameV2` or whatever superseded it) to both listing paths.
2. Skip files whose declared `[WallMinSec, WallMaxSec]` range doesn't overlap the query window,
   BEFORE issuing the S3 GET — mirroring how VI's file discovery (`internal/modules/valueindex/
   filecache.go`'s `FilesForTimeRange`/`IsInTimeRange`) already does this.
3. Update `cube_backfill.go`'s stale comment either way (even if pruning turns out infeasible
   for some reason, the comment must not keep asserting a now-false claim).

## Critical backward-compatibility note (from the issue, do not skip)

Every VCNT file only reliably carries a v2 range once the full data wipe from #494 has actually
happened in each environment (existing v1 files are never rewritten in place, and #494's own
design explicitly left old-style files as permanent stragglers if they're never touched by a
merge). This pruning logic MUST treat a v1-shaped filename (2-dash, no embedded range) OR a
parse failure as "unknown range, always fetch, never skip" — never as an error, never as
something to drop. This exactly mirrors how VI's own file discovery already handles the
old-vs-new filename distinction (blockpack's `internal/modules/valueindex/discovery.go` was
found, during #494's own planning, to have FULLY REMOVED its v1 fallback — silently dropping
old files from discovery forever, per NOTE-VI-030). #495 explicitly does NOT want that shortcut
— unknown-range files must still be considered, just not pruned.

## Requirements for this brainstorm

1. Confirm blockpack's CURRENT public API for parsing VCNT v2 filenames (things have shipped
   twice since #495 was filed — #494 itself, then #496's own changes touched adjacent VI/vibuilder
   code and might have touched exported VCNT-adjacent symbols too, verify directly rather than
   trust the issue text's exact function names).
2. Investigate whether tempo's own VCNT L0 writer (`tempodb/encoding/vblockpack/vcntwriter.go`,
   per #494's own history) is ALREADY emitting v2-ranged filenames (it should be, per #494's
   own Part B — verify this landed and is live, not just planned) — this determines whether
   "old-style stragglers" are truly a permanent-until-wipe concern or a rapidly-shrinking
   population, which affects how much engineering effort the "unknown range" fallback path
   deserves versus how much it's a rare edge case.
3. Design the exact pruning insertion point for BOTH `fetchVCNTSection` and `buildVCNTSection`
   — trace the current code (post-#494/#496 changes) to find precisely where the file list is
   built and where the query's time window is known, and confirm the two are available together
   at a point before any S3 GET is issued.
4. Identify whether VI's `FilesForTimeRange`/`IsInTimeRange` pattern can be reused/mirrored
   directly, or whether VCNT's semantics differ enough (e.g. VCNT records are aggregates over a
   window, not point-in-time spans) that the overlap check needs different boundary semantics —
   investigate concretely rather than assume symmetry with VI.
5. Consider whether this pruning belongs in a shared helper (both call sites do conceptually the
   same thing: list → parse-and-filter → fetch) or whether the two call sites' surrounding code
   structure makes a shared helper awkward — look at both functions directly before deciding.
6. Identify risks: could pruning ever cause a FALSE NEGATIVE (skipping a file that should have
   been included)? This is the single most important correctness question — get the overlap-check
   boundary conditions right (inclusive/exclusive edges) and verify against #494's actual
   `IsInTimeRange` semantics for VI, which the plan for #494 established as
   `WallMaxSec >= queryMinSec && WallMinSec <= queryMaxSec`.

## Constraints

- This is a tempo-only task. blockpack's #494/#496 already shipped the filename format and
  parsing primitives — no blockpack changes should be needed unless investigation reveals a
  genuine gap (e.g. a needed parsing function doesn't actually exist as a public/importable
  symbol from tempo). If so, flag it explicitly rather than silently deciding to add blockpack
  code as part of this "tempo-only" task.
- No new backward-compat design needed beyond what's already established: treat unparseable/
  old-style filenames as "unknown, always include," consistent with #494's own compactor
  behavior for stragglers.
- Use .bob/state/495-vcnt-pruning/ for all state files for this task (NOT the bare .bob/state/
  filenames like brainstorm.md/plan.md/review.md, which already contain unrelated prior work
  in this repo from earlier sessions — do not overwrite those).
