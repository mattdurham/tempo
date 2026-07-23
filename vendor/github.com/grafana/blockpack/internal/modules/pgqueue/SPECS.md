# pgqueue — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/pgqueue` package. It complements `NOTES.md` (design rationale), per root
`SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-PGQUEUE-N`. IDs are assigned
in ascending order and never reused or renumbered.

---

## SPEC-PGQUEUE-1: `compaction_jobs` — one shared table, generic across `job_type`

One Postgres table, `compaction_jobs`, covers every compaction-worker job type
(`vi_compaction` | `vcnt_compaction` | `cube_compaction` | `catalog_reconcile` | `catalog_reap`).
`Claim` has **no `job_type` filter** — a caller claims the oldest claimable row of ANY type and
dispatches on the returned `JobType`, mirroring the "any pod, any job" philosophy (issue #522
Section G.2).

Back-ref: `internal/modules/pgqueue/schema.sql`.

## SPEC-PGQUEUE-2: `Store.Insert` — idempotent on `dedup_key`

`Insert(ctx, jobType, subsystem, tenant, dedupKey, detail) error` inserts a pending row, using
`ON CONFLICT (dedup_key) WHERE status != 'succeeded' DO NOTHING`. A second `Insert` call for the
same `dedupKey` while any non-`succeeded` row exists (`pending`/`claimed`/`running`/`failed`) is a
silent no-op, never a duplicate-key error. Not scoped to `pending`/`claimed`/`running` alone — see
`SPEC-PGQUEUE-6` for why `failed` must be included too.

Back-ref: `internal/modules/pgqueue/store.go:Insert`.

## SPEC-PGQUEUE-3: `Store.Claim` — `SELECT ... FOR UPDATE SKIP LOCKED`, self-healing lease expiry

`Claim(ctx, workerID) (*Job, error)` atomically claims and returns the oldest claimable row, or
`(nil, nil)` if none exists — never an error for "nothing to claim". A row is claimable if it is
`pending`, or `claimed`/`running` with an expired lease (crashed-worker self-heal), or `failed`
with a due retry. `SKIP LOCKED` guarantees N concurrent callers never claim the same row.

Back-ref: `internal/modules/pgqueue/store.go:Claim`.

## SPEC-PGQUEUE-4: `Store.Fail` — never permanently fails, always schedules a retry

`Fail(ctx, id, errMsg) error` records the failure and unconditionally schedules a retry via
exponential backoff (1 minute base, doubling, capped at 30 minutes) — there is no terminal
"gave up" state. A persistently-failing job is retried forever, bounded only by the backoff cap
on how much worker capacity it can consume.

Back-ref: `internal/modules/pgqueue/store.go:Fail,backoffDuration`.

## SPEC-PGQUEUE-5: `Store.RenewLease` — extends a claimed job's lease, no-op once terminal

`RenewLease(ctx, id) error` extends `lease_expires_at` by another 30 minutes from the CURRENT
`now()`, not the original claim time. The `status IN ('claimed','running')` guard makes this a
no-op once the job has reached a terminal state — callers are expected to invoke this
periodically (well under the 30-minute lease TTL) for the duration of active processing.

Back-ref: `internal/modules/pgqueue/store.go:RenewLease`.

## SPEC-PGQUEUE-6: The dedup-active predicate must cover every non-terminal status, including `failed`

`idx_compaction_jobs_dedup_active`'s `WHERE` predicate is `status != 'succeeded'`, not
`status IN ('pending','claimed','running')` — `failed` must be included. A `failed` row is NEVER
terminal (`SPEC-PGQUEUE-4`: `Fail` unconditionally schedules a retry) and still represents the
exact same candidate as any fresh `Insert` attempt with the identical `dedup_key`. A caller with
no memory of what it already enqueued (`compactionplanner`'s own periodic re-planning pass, which
just calls `Insert` every tick and relies entirely on this index to deduplicate) can otherwise
create a SECOND row sharing one `dedup_key` with the first, once the first transitions to
`failed`.

That collision is not merely a wasted duplicate: the first `Claim` attempt on EITHER row that
tries to transition it to `claimed` violates this same unique index (both rows still sharing one
`dedup_key`, both non-`succeeded`), and `Claim`'s own query always selects the single oldest
claimable row (`ORDER BY created_at ... LIMIT 1`) — so that one failing `UPDATE` permanently jams
the ENTIRE queue behind it, since nothing else ever gets a turn. Found live on
tempo-dev-test-03, 2026-07-21: a large backlog of legitimately claimable work sat completely
unprocessed behind exactly one such poisoned row.

Schema migration note: `schema.sql` `DROP INDEX IF EXISTS` + unconditional `CREATE UNIQUE INDEX`
(not `CREATE ... IF NOT EXISTS`) for this specific index — an index's `WHERE` predicate is part of
its identity to Postgres, but `IF NOT EXISTS` only checks the NAME, so a predicate change would
otherwise never apply to an already-provisioned live database.

Back-ref: `internal/modules/pgqueue/schema.sql`, `internal/modules/pgqueue/store.go:insertJobSQL`,
`store_test.go:TestStore_Insert_DedupKeyPreventsDoubleEnqueue_AfterFailure`.

## SPEC-PGQUEUE-7: `ViBackfillDetail` carries two coexisting vi_backfill job shapes (issue #532)

Retired by issue #533 — every vi_backfill job is now block-shaped by construction
(`ViBackfillDetail` no longer carries column identity at all). See SPEC-PGQUEUE-8/9.

## SPEC-PGQUEUE-8: vi_backfill column identity lives in `vi_backfill_job_columns`, not the parent job (issue #533)

Column identity (`column_hash`/`column_type`/`column_name`) no longer lives on the parent
`compaction_jobs` row at all — it's membership rows in the child table
`vi_backfill_job_columns`, keyed `UNIQUE(job_id, column_hash, column_type)`. This is what lets N
columns triggered for the SAME physical block collapse onto exactly ONE parent job row (the
`INSERT ... ON CONFLICT (dedup_key) ... DO UPDATE` get-or-create idiom) instead of N independent,
independently-claimable rows for the identical block — the root fix for the live "N concurrent
claims/downloads of the same block" incident.

**Contract:** the `UNIQUE(job_id, column_hash, column_type)` index makes child-row registration
idempotent per (job, column) pair — a repeat `InsertViBackfillBlocks` call for a column already
registered on a block's job is a silent no-op on the child insert (`ON CONFLICT ... DO NOTHING`),
mirroring `Store.Insert`'s own idempotent-dedup convention at the parent-row level.
`MissingViBackfillBlocks`/`ViBackfillGapRanges` are now JOINs against this child table (filtered
on `column_hash`/`column_type`, `MissingViBackfillBlocks` status-agnostic on the child row exactly
like its #532 predecessor was on the parent row) rather than filters against parent-row columns
that no longer exist.

Back-ref: `internal/modules/pgqueue/schema.sql` (`vi_backfill_job_columns`,
`idx_vi_backfill_job_columns_lookup`, `idx_vi_backfill_job_columns_pending`),
`internal/modules/pgqueue/store.go` (`ViBackfillPendingColumns`, `ViBackfillMarkColumnDone`,
`ViBackfillMarkAllColumnsResolved`, `viBackfillExistingBlockKeysSQL`, `viBackfillGapRangesSQL`).
See NOTES.md NOTE-PGQUEUE-VI-BLOCK-2.

## SPEC-PGQUEUE-9: `ViBackfillFinalize` — parent-row lock is the race-closing serialization point, not a child-row lock (issue #533)

Both the get-or-create insert side (`insertViBackfillBlockChunk`'s round trip 1, a genuine
`ON CONFLICT ... DO UPDATE` — not `DO NOTHING` — so it always takes a real row-level lock for the
whole transaction) and the finalize side (`ViBackfillFinalize`'s own `SELECT ... FOR UPDATE`) lock
the IDENTICAL parent `compaction_jobs` row via Postgres's normal row-level locking. This is
deliberate: a lock on already-EXISTING child rows cannot, by itself, prevent a brand-new child-row
`INSERT` from landing in the gap between a "read the pending-column list" step and a "flip to
succeeded" step, because the new row doesn't exist yet to be locked. Locking the shared PARENT row
closes that gap, since both sides of the race contend for the same lock.

**Contract:** `ViBackfillFinalize` returns `(false, nil)` — never an error — when pending children
remain; the caller (`compactionworker.processViBackfillJob`) must reprocess those columns and call
`ViBackfillFinalize` again, bounded by `viBackfillFinalizeMaxAttempts`. If a column arrives for a
block whose job has ALREADY finalized to `succeeded` (excluded from the dedup conflict target),
`InsertViBackfillBlocks` self-corrects by creating a genuine NEW parent job instead — a column is
never silently dropped in either race outcome.

Back-ref: `store.go:ViBackfillFinalize,insertViBackfillBlockChunk`,
`vi_backfill_race_test.go:TestStore_ViBackfillRace_ColumnInsertDuringFinalize_NeverDropsColumn`.
See NOTES.md NOTE-PGQUEUE-VI-BLOCK-2.
