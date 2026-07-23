# pgqueue — Design Notes

This document records the design decisions and rationale behind `internal/modules/pgqueue`
(issue #522 Section G.2). Entries are append-only and dated.

## ID convention

Entries use the module-local prefix `NOTE-PGQUEUE-N`.

---

## NOTE-PGQUEUE-1 — Near-verbatim port of tempo's `jobstore.go`, not a novel design

Date: 2026-07-20

**Decision:** `compaction_jobs`'s schema and `Store`'s `SELECT ... FOR UPDATE SKIP LOCKED` claim
pattern are a near-verbatim port of tempo's own already-proven
`tempodb/encoding/vblockpack/jobstore` package (`backend_jobs` table). blockpack cannot import
tempo's package across the repo/module boundary, so this is a fresh implementation of the
identical design, not a shared dependency and not a novel one either.

**Why generic across `job_type` from day one (unlike tempo's own `jobstore.Claim`, which takes a
`jobType` parameter):** issue #522 Section G.2 explicitly settles the "any pod, any job" claim
philosophy for this new table from the start, rather than retrofitting it later the way tempo's
own backend-worker evolved toward the same end state incrementally (`tryClaimPostgresJob` trying
several job types in sequence). Since `compaction-worker` has no reason to prefer one job type
over another (Section G.4), a single filterless `Claim` is simpler than tempo's own
type-then-fallback loop and equally correct.

**Why no `Subsystem`-scoped claim:** `Subsystem` is stored on every row (redundant with
`job_type`'s own prefix) purely for cheap indexing/reporting — mirrors
`blockpack_file_catalog`'s own identical `subsystem` column convention — never used as a `Claim`
filter, consistent with the generic-claim design above.

Back-refs: `internal/modules/pgqueue/schema.sql`, `internal/modules/pgqueue/store.go`. See
SPECS.md SPEC-PGQUEUE-1 through 5. Issue #522.

## NOTE-PGQUEUE-2 — Dedup-active predicate excluded `failed`, permanently jamming the queue (found live, 2026-07-21)

`idx_compaction_jobs_dedup_active`'s original predicate (`status IN ('pending','claimed','running')`)
looked reasonable in isolation — those are exactly the "someone's already working on this" states.
The gap only became visible under sustained live load: `compactionplanner`'s periodic re-planning
pass calls `Insert` every poll tick with no memory of what it already enqueued, relying entirely on
this index to deduplicate. Once a row transitioned to `failed` (which `SPEC-PGQUEUE-4` establishes
is NEVER terminal — a retry is always scheduled), it stopped counting as "active," so the very next
planning tick could insert a second row with the identical `dedup_key`.

The failure mode wasn't obvious from the symptom: both `compaction-planner` and `compaction-worker`
appeared healthy (low CPU, clean logs, no crash loops, `pg_stat_activity` showing live claim/insert
queries executing every few seconds), yet `succeeded`/`failed` counts hadn't moved in 20+ minutes.
Root-caused by manually re-running `Claim`'s exact SQL by hand against the live database, which
surfaced the actual Postgres error the Go client was silently absorbing into a generic "claim
error" retry loop: `duplicate key value violates unique constraint "idx_compaction_jobs_dedup_active"`.
Since `Claim` always selects the single oldest claimable row, and that specific row's `UPDATE`
failed on every single attempt, the entire queue was jammed behind it — a large, otherwise
perfectly claimable backlog sat completely idle the whole time.

Fixed by widening the predicate to `status != 'succeeded'` (SPEC-PGQUEUE-6) — `failed` is the only
status the original predicate missed, since `succeeded` is the sole true terminal state a fresh
`Insert` should ever be allowed to reuse a `dedup_key` past.

Back-refs: `internal/modules/pgqueue/schema.sql`, `internal/modules/pgqueue/store.go:insertJobSQL`.
Issue #522.

---

## NOTE-PGQUEUE-VI-BLOCK-1: vi_backfill Moves From Per-Window To Per-Block Jobs (Issue #532)

*Added: 2026-07-22*

**Problem:** Issue #529's per-1-minute-window model (`WindowSpec`/`WindowsForRetention`/
`InsertViBackfillWindows`/`MissingViBackfillWindows`, all now deleted) assigned exactly one job
per synthetic wall-clock minute. Trace blocks don't align to 1-minute boundaries, so a single
block routinely fell inside MULTIPLE window jobs — even within one column's own backfill, the
same block got listed and re-fetched by several jobs.

**Decision:** One vi_backfill job per (column, ACTUAL trace block) instead of per (column,
synthetic window). `pgqueue.BlockSpec` (object key + real MinSec/MaxSec + claim priority)
replaces `WindowSpec`; `InsertViBackfillBlocks`/`MissingViBackfillBlocks` replace
`InsertViBackfillWindows`/`MissingViBackfillWindows`, mirroring their exact idempotency
contract (status-agnostic existence check before insert, since the dedup unique index is
scoped to `status != 'succeeded'`). `viBackfillBlockDedupKey` replaces `viBackfillDedupKey`,
keyed by the block's object key instead of a window-end timestamp.

**`ViBackfillDetail` carries BOTH shapes** (see its own doc comment): `BlockObjectKey` empty
means the OLD window-shaped job; non-empty means the NEW block-shaped job.
`WindowStartSec`/`WindowEndSec` are populated for BOTH shapes — for a new-model row, from the
block's own real `MinSec`/`MaxSec` (not a synthetic slice) — so `ViBackfillGapRanges` (the
query-time coverage-check primitive tempo's own `ErrSliceIndexCoverageGap`-style checks
depend on) keeps answering correctly across both shapes without ever needing to know which one
produced a given row. This was the key insight that let the migration avoid a second,
parallel gap-tracking query: only the JOB-CREATION side needed to change; the
QUERY-TIME-COVERAGE side did not.

**Migration/transition (the highest-risk part — a live cluster carries hundreds of thousands
of in-flight OLD window-shaped rows at ship time):**

- OLD rows are NEVER migrated into the new shape. They keep draining via
  `compactionworker.processViBackfillJob`'s unchanged window/listing branch
  (`detail.BlockObjectKey == ""`) until they naturally complete — see
  `compactionworker/NOTES.md`'s own entry for the dispatch-side detail.
- `compactionplanner`'s trailing top-up and gap-heal (`plan_vi_backfill.go`) and the root
  `Postgres.InsertViBackfillHistory` reactive first-trigger BOTH switched fully to
  block-enumeration — no code path creates new window-shaped rows after this ships.
- The old window-generation primitives were deleted outright (no back-compat shim, no
  deprecated-but-kept wrapper) — this project's own "no backwards compat ever" convention
  applies to internal primitives just as much as public API; only the RUNTIME DATA (already-
  inserted Postgres rows) needs backward compatibility, not the code that used to create it.

**Schema:** `block_object_key TEXT NULL` (additive `ALTER TABLE ADD COLUMN IF NOT EXISTS`,
NULL for every OLD row and every non-vi_backfill job type) + `idx_vi_backfill_all_blocks`
(new, `CREATE INDEX CONCURRENTLY`, mirroring `idx_vi_backfill_all_windows`'s own
non-status-scoped existence-check role, now scoped to `block_object_key IS NOT NULL`).

Back-ref: `internal/modules/pgqueue/details.go` (`ViBackfillDetail`), `store.go` (`BlockSpec`,
`InsertViBackfillBlocks`, `MissingViBackfillBlocks`, `viBackfillBlockDedupKey`),
`schema.sql`. See `internal/modules/compactionplanner/NOTES.md` and
`internal/modules/compactionworker/NOTES.md` for the caller-side halves of this migration.

## NOTE-PGQUEUE-VI-BLOCK-2: vi_backfill Moves From Per-Column-Per-Block To Per-Block-With-Column-Membership (Issue #533)

*Added: 2026-07-23*

**Problem:** #532's per-(column, block) job shape still let N independently-triggered columns for
the SAME physical trace block create N independent parent rows — each independently claimable,
each independently downloading and decoding that block from scratch. This was the direct root
cause of a live incident: N concurrent workers claiming N jobs for one block simultaneously,
downloading it N times, exhausting local disk ("no space left on device") and OOMKilling the pod.
Issue #530's shared cache mitigated re-fetches ACROSS separately-triggered jobs over time, but did
nothing to stop N jobs from being claimed and started concurrently in the first place.

**Decision:** One vi_backfill job per BLOCK ONLY. Column identity moves off the parent
`compaction_jobs` row entirely into a new child table, `vi_backfill_job_columns` — column
membership, not job identity. `viBackfillBlockDedupKey` drops its column parameters (now keyed
purely by `(tenant, block_object_key)`), so `InsertViBackfillBlocks`' own get-or-create insert
collapses every column triggered for the same block onto exactly one parent row, regardless of how
many columns are outstanding for it. `compactionworker.processViBackfillJob` loops the job's
pending columns (`ViBackfillPendingColumns`) against the SAME staged/cached block, processing them
sequentially through the existing, unchanged `BackfillEngine.Run`/`blockOnlyFetcher` machinery —
one download now serves however many columns share that block, not one download per column.

**Why not a child-row lock (the issue's own literal text, corrected here):** the issue as
originally written proposed locking the child rows to close the race between a new column's
membership INSERT and the worker's own finalize-to-succeeded transaction. This does not work: a
row-level lock can only be held on a row that ALREADY EXISTS. A brand-new child-row `INSERT`
landing in the gap between the worker's "read the pending-column list" step and its "flip parent to
succeeded" step is not blocked by any lock on the OTHER, already-existing child rows — there is
nothing to lock yet for the new row. Locking the shared PARENT row instead (SPEC-PGQUEUE-9) closes
this gap correctly, because both the insert side and the finalize side contend for the identical
row lock regardless of how many child rows exist or don't yet exist.

**Why not Postgres advisory locks:** this table's own `simple_protocol`/pgbouncer transaction-
pooling comment (see `insertWithPriority`'s doc comment) already documents the constraint this
would violate. `pg_advisory_lock`/`pg_advisory_unlock` are session-scoped and unsafe under
transaction-mode pooling (a session-scoped lock can leak across pooled connections that don't
correspond to a stable backend session). `pg_advisory_xact_lock` is transaction-scoped and would
work under pooling, but requires holding an open transaction across the ENTIRE column-processing
loop's slow I/O (block download + extraction, potentially seconds per column) — the exact
long-held-transaction anti-pattern this codebase's own index-rebuild-locking incident
(`idx_compaction_jobs_claimable_priority`'s schema.sql comment) already taught it to avoid at the
table level. A plain row-level lock, held only for the duration of a single fast INSERT or a single
fast finalize check (never across the slow per-column extraction work), avoids both problems.

**Migration:** clean cut, no coexistence, no per-row migration — unlike #532's own transition
(which drained OLD window-shaped rows to exhaustion alongside NEW block-shaped ones),
issue #533 clears ALL existing vi_backfill rows outright via a one-time, manually-run `DELETE`
(rollout artifact immediately below). This project's "no backwards compat ever" convention applies
here too: the OLD per-column-per-block dispatch branch is deleted outright in
`compactionworker/vi_backfill.go`, not kept as a second, unreachable-in-practice code path drained
to exhaustion. Self-heal (`TestPlanViBackfill_SelfHealsFromTruncatedJobTable`,
`compactionplanner`) proves the very next planner tick and/or reactive trigger fully reconstructs
correct jobs+columns from `blockpack_file_catalog`/`viusage_entries` with zero manual data
migration needed.

**Deferred:** multi-column single-pass extraction. `ExtractValueIndexEntriesForColumns` (root
`blockpack`) already supports extracting several columns from one decoded block in a single pass
via its own allowlist parameter — issue #533 does NOT use this; `processViBackfillPendingColumns`
still calls the existing single-column `BackfillEngine.Run` once per pending column, sequentially,
reusing 100% of already-tested machinery. The column loop already gets the incident-fixing
property (one download, one decode-and-cache-population per block) from issue #530's shared cache
alone — collapsing to a true single-pass multi-column extraction is a pure CPU-efficiency
follow-on, explicitly out of scope here, with zero changes to root `blockpack`.

Back-ref: `internal/modules/pgqueue/schema.sql` (`vi_backfill_job_columns` and its 2 indexes),
`store.go` (`viBackfillBlockDedupKey`, `insertViBackfillBlockChunk`, `ViBackfillPendingColumns`,
`ViBackfillMarkColumnDone`, `ViBackfillMarkAllColumnsResolved`, `ViBackfillFinalize`),
`vi_backfill_race_test.go`. See `internal/modules/compactionworker/NOTES.md` and
`internal/modules/compactionplanner/SPECS.md` for the caller-side halves of this migration.

---

**ROLLOUT ARTIFACT (issue #533) — manual, sign-off-gated operator action, NOT executed by any
workflow or agent:**

This is documentation only. No agent or automated workflow may run this against any real
database. It requires separate, explicit human sign-off at actual deploy time, after confirming:

- The new schema (`vi_backfill_job_columns` table + dropped `column_hash`/`column_type` columns
  on `compaction_jobs`, Step 1.1) has already been applied cluster-wide — every pod's
  `ApplyCompactionJobsSchema` call, which runs automatically at process startup — BEFORE running
  this `DELETE`, so `vi_backfill_job_columns` already exists to receive the very next tick's fresh
  inserts.
- The new compaction-worker/compaction-planner images are deployed and healthy, NOT before —
  running this against a cluster still serving OLD-shaped rows through the (now-deleted) OLD
  dispatch branch would strand those in-flight jobs mid-drain.

```sql
-- ROLLOUT STEP (issue #533) -- requires separate, explicit user sign-off at actual deploy
-- time. Run manually against the target cluster's Postgres AFTER the new schema (Step 1.1's
-- vi_backfill_job_columns table + dropped column_hash/column_type) has been applied and the
-- new compaction-worker/compaction-planner images are deployed and healthy, NOT before --
-- running this against a cluster still serving OLD-shaped rows through the (now-deleted)
-- OLD dispatch branch would strand those in-flight jobs mid-drain.
--
-- Clears BOTH pre-existing vi_backfill job shapes: #529's original window-shaped rows (if any
-- still remain undrained) AND #532's per-column-per-block rows. Cascades to
-- vi_backfill_job_columns automatically via its ON DELETE CASCADE foreign key -- no separate
-- child-table cleanup statement is needed.
--
-- Self-heal verification (already proven by test -- see
-- TestPlanViBackfill_SelfHealsFromTruncatedJobTable): the very next compaction-planner poll
-- tick and/or the next reactive InsertViBackfillHistory trigger reconstructs the full correct
-- (block, column-membership) set from blockpack_file_catalog + viusage_entries with zero
-- further manual intervention.
DELETE FROM compaction_jobs WHERE job_type = 'vi_backfill';
```

**This statement is destructive and intentional, not a bug** — it discards ALL prior vi_backfill
job history (including already-succeeded rows), by design, per this issue's own accepted "clean
cut, no per-row migration" scope. `TestPlanViBackfill_SelfHealsFromTruncatedJobTable`
(`compactionplanner`) is the proof this is safe to run — it is NOT a substitute for actually
running it against the real target cluster with the operator's own explicit go-ahead.
