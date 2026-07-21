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
