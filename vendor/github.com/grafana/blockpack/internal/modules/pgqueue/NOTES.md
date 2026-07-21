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
