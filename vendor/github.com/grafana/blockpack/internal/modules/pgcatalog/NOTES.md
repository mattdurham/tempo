# pgcatalog — Design Notes

This document records the design decisions and rationale behind
`internal/modules/pgcatalog` (issue #522: unify compaction around pairwise merges + a global
Postgres file catalog). Entries are append-only and dated; never delete or rewrite an existing
entry — add an `*Addendum (date):*` note if a decision is later corrected or superseded.

## ID convention

Entries use the module-local prefix `NOTE-PGCATALOG-N`, independent of every other module's own
numbering. This module is new, shared infrastructure for VI/VCNT/cube, not a fourth participant
in any of their existing numbering spaces.

---

## NOTE-PGCATALOG-1 — Why one shared table with a discriminator, not three per-subsystem tables

Date: 2026-07-20

VI, VCNT, and cube each independently accumulate small compacted-merge output files that need
the same lifecycle: track a live object, mark it superseded the moment a merge output replaces
it, and physically delete it only after a grace window (so in-flight queries reading the old
object don't race a delete). Rather than three near-identical tables, `blockpack_file_catalog`
uses one table with a `subsystem` discriminator column — `backend_jobs.job_type` already proves
this pattern works cleanly for heterogeneous shapes sharing one table, in this exact codebase
(plan.md Section B). One schema, one reaper query shape, one set of indexes, one migration file,
at the cost of a `resource_id` column whose meaning (`colHash+colType` for VI, `colHash` for
VCNT, `cubeID` for cube) is subsystem-dependent and opaque to this package by design — pgcatalog
never interprets `resource_id`, only stores and filters on it as an opaque string.

**Why not merged with tempo's own `file_catalog`.** Different repo, different Postgres-ownership
boundary (blockpack's own module vs. tempo's `tempodb/encoding/vblockpack/schema/`) — two tables
system-wide is the accepted, deliberate outcome (plan.md Section B), not an oversight.

**Why `object_key` is globally UNIQUE across all subsystems, not just unique per-subsystem.**
`Store.Insert`'s idempotency (SPEC-PGCATALOG-2) needs exactly one `ON CONFLICT` target column,
and real object-storage keys already encode the tenant/subsystem/resource path components as a
prefix, so a global uniqueness constraint costs nothing in practice while keeping `Insert`'s
conflict target unambiguous without also needing `subsystem` in the constraint.

## NOTE-PGCATALOG-2 — Why this package never touches object storage

Date: 2026-07-20

`Store` only ever reads/writes Postgres rows. Every method's caller is responsible for the
actual object-storage `Put`/no-delete sequencing described in plan.md Section E: write the
merge-output object first, `Insert` its catalog row, then `MarkCompacted` the input rows' catalog
rows — in that order, with the object write happening before any catalog mutation. A crash
between any of these steps leaves the catalog in a recoverable state (worst case: an output
object exists in storage with no catalog row yet), closed by a separate `catalog_reconcile` job
(plan.md Section C), never a correctness problem for this package itself — only a visibility
delay resolved one layer up. Keeping this package storage-agnostic also means it has zero
dependency on any subsystem's own storage client (`valueindexcompactor.IndexStore`,
`cube.ObjectStore`, etc.) — it is a true leaf package.

## NOTE-PGCATALOG-3 — Why `Store` is a concrete Postgres-only type, not an interface with a blob-backed alternative

Date: 2026-07-20

Unlike `viusage`/`cube`/`colhashmanifest` (each of which has a legacy blob-backed implementation
predating their Postgres one, and therefore define `EntryStore`/`Store` as an interface both
backends satisfy), `blockpack_file_catalog` has no blob-backed predecessor — it is new,
Postgres-only infrastructure from day one (issue #522). `pgcatalog.Store` is therefore a plain
concrete struct wrapping a `*pgxpool.Pool`, with no interface indirection to support a second
backend that will never exist.

Back-refs: `internal/modules/pgcatalog/pgcatalog.go`, `internal/modules/pgcatalog/schema.sql`.
See SPECS.md SPEC-PGCATALOG-1 through 5. Issue #522.
