# pgcatalog — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/pgcatalog` package. It complements `NOTES.md` (design rationale), per
root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-PGCATALOG-N`, independent
of every other module's own numbering (`SPEC-VI-N`, `SPEC-VC-N`, `SPEC-COLMANIFEST-N`, etc.) —
this module is a genuinely separate, shared concern (issue #522) spanning VI, VCNT, and cube.
IDs are assigned in ascending order and never reused or renumbered.

---

## SPEC-PGCATALOG-1: `blockpack_file_catalog` — one shared table, discriminated by `subsystem`

One Postgres table, `blockpack_file_catalog`, covers VI, VCNT, and cube physical-object
bookkeeping, discriminated by a `subsystem` column (`'vi'` | `'vcnt'` | `'cube'`). It is
deliberately NOT merged with tempo's own `file_catalog` table (different repo/Postgres-ownership
boundary — two tables system-wide).

Each row is one physical object, at one subsystem-defined merge `level`, for one
`(tenant, resource_id)` pair (`resource_id` is `colHash+colType` for VI, `colHash` for VCNT,
`cubeID` for cube). `object_key` is globally UNIQUE across all subsystems and is the table's
`ON CONFLICT` idempotency target (SPEC-PGCATALOG-2).

Back-ref: `internal/modules/pgcatalog/schema.sql`.

## SPEC-PGCATALOG-2: `Store.Insert` — idempotent on `object_key`

`Insert(ctx, row) error` inserts row, using `ON CONFLICT (object_key) DO NOTHING`. A second
`Insert` call for a `Row` whose `ObjectKey` already has a committed row is a silent no-op, never
a duplicate-key error. This exists specifically so a compaction job retried after a crash
between writing its merge-output object and reporting job success (plan.md Section E) can safely
re-`Insert` the identical row without special-casing "did my previous attempt already commit
this."

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:Insert`.

## SPEC-PGCATALOG-3: `Store.MarkCompacted` — never touches storage, only the catalog

`MarkCompacted(ctx, objectKeys) error` sets `compacted_at = now()` on every row in `objectKeys`
whose `compacted_at` is still `NULL`. It never deletes or otherwise touches the underlying
storage object — those objects remain physically present (and still safely readable by any
in-flight query) until a separate reaper process deletes them after a grace window, using
`ListCompactedOlderThan` (SPEC-PGCATALOG-5) to find them. A no-op (not an error) on an empty
`objectKeys` slice.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:MarkCompacted`.

## SPEC-PGCATALOG-4: `Store.ListCandidates` — live rows for one `(subsystem, tenant, resource_id)`

`ListCandidates(ctx, subsystem, tenant, resourceID) ([]Row, error)` returns every row matching
all three key fields with `compacted_at IS NULL AND deleted_at IS NULL`, ordered by `level` then
`object_key`. This ordering is deliberate: a candidate-selection query groups consecutive rows by
`(tenant, resource_id, level)` in Go (plan.md Section 1.2/2.2/3.2) and needs same-level rows
adjacent and in a stable order to do so correctly.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:ListCandidates`.

## SPEC-PGCATALOG-5: `Store.ListCompactedOlderThan` — the reaper's candidate set

`ListCompactedOlderThan(ctx, cutoff) ([]Row, error)` returns every row with a non-NULL
`compacted_at` strictly before `cutoff` and `deleted_at IS NULL` — never a row that is still live
(`compacted_at IS NULL`), regardless of how far in the future `cutoff` is. Callers pass
`cutoff = now() - gracePeriod` (30 minutes in the reaper's real usage, plan.md Section C) to
find objects safe to physically delete.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:ListCompactedOlderThan`.

## SPEC-PGCATALOG-6: `Store.DeleteRow` — hard delete, idempotent, only after the object is confirmed gone

`DeleteRow(ctx, rowID) error` hard-deletes the row identified by `rowID`. Idempotent: deleting an
already-gone `rowID` is a no-op, never an error (mirrors the reaper's own claim/lease-free,
safe-to-retry design, plan.md Section C). Callers (backend-worker's catalog_reap handler) must
only call this after the row's underlying object has been physically deleted from storage — there
is no audit value in retaining a row once its object is confirmed gone, unlike `backend_jobs`'
terminal rows.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:DeleteRow`.

## SPEC-PGCATALOG-7: `Store.ListCompactedKeys` — the mandatory VCNT read-path filter's query

`ListCompactedKeys(ctx, keys) (map[string]struct{}, error)` returns the subset of `keys` whose
row has a non-NULL `compacted_at`, regardless of subsystem (`object_key` is globally unique,
SPEC-PGCATALOG-1). A key with no row at all is never included (fail-open: "unknown" is not
"compacted"). This is the actual fix for NOTE-VC-009 (plan.md Section F/Phase 2.1): VCNT's
`valuecounts.Compact` sums per key with no source-identity dedup, so a caller building a VCNT
query-time section (`buildVCNTSection`) must exclude every key this returns from its candidate
set BEFORE downloading and summing it — a compacted-but-undeleted source coexisting with its
merged replacement for up to the reaper's 30-minute grace window would otherwise be summed twice.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:ListCompactedKeys`, `cube_backfill_runner.go:buildVCNTSection`.

## SPEC-PGCATALOG-8: `Store.ListLiveKeys` — every live row for `(subsystem, tenant)`, across all `resource_id`s

`ListLiveKeys(ctx, subsystem, tenant) ([]Row, error)` returns every live (not compacted, not
deleted) row for the given `(subsystem, tenant)`, unscoped by `resource_id` — unlike
`ListCandidates` (`SPEC-PGCATALOG-4`), which requires one and is shaped for compaction's own
pairwise candidate grouping. This is `catalog_reconcile`'s own candidate set for detecting a
catalog row whose backing object has vanished out-of-band (files fall out of retention or
otherwise disappear independent of this system's own compaction/reap actions) — see
`compactionworker`'s `SPEC-COMPACTIONWORKER-8`.

Back-ref: `internal/modules/pgcatalog/pgcatalog.go:ListLiveKeys`, `internal/modules/compactionworker/catalog_reconcile.go:pruneVanishedRows`.
