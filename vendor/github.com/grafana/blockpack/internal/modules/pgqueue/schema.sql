-- schema.sql — compaction_jobs' Postgres schema (issue #522 Section G.2). A
-- near-verbatim port of tempo's own proven jobstore.go SKIP LOCKED design
-- (backend_jobs table) -- blockpack cannot import tempo's package across the
-- repo/module boundary, so the SQL shape is re-implemented here rather than
-- shared, but the design itself is not novel.
--
-- Deliberately generic across job_type from day one (the "any pod, any job"
-- claim philosophy): ClaimSQL has no job_type filter, so a worker claims the
-- oldest claimable row of ANY type and dispatches on the returned job_type.
CREATE TABLE IF NOT EXISTS compaction_jobs (
    id               UUID PRIMARY KEY,
    job_type         TEXT NOT NULL,   -- 'vi_compaction' | 'vcnt_compaction' |
                                       -- 'cube_compaction' | 'catalog_reconcile' |
                                       -- 'catalog_reap'
    subsystem        TEXT NOT NULL,   -- 'vi' | 'vcnt' | 'cube' (redundant with
                                       -- job_type's prefix, kept as its own column
                                       -- for cheap indexing, mirrors
                                       -- blockpack_file_catalog's own convention)
    tenant           TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    detail           JSONB NOT NULL,   -- e.g. {input_object_keys: [...]}
    dedup_key        TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at       TIMESTAMPTZ NULL,
    claimed_by       TEXT NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    started_at       TIMESTAMPTZ NULL,
    finished_at      TIMESTAMPTZ NULL,
    retries          INT NOT NULL DEFAULT 0,
    last_error       TEXT NULL,
    next_retry_at    TIMESTAMPTZ NULL,
    -- priority (issue #529): claim ordering tiebreaker, ASC (lower = claimed first). Every
    -- job type except vi_backfill leaves this at its DEFAULT 0, preserving today's exact
    -- FIFO-by-created_at behavior for them (0 always ties on created_at). vi_backfill's bulk
    -- 1-minute-window jobs assign an ordinal value per window (0 = newest window in the batch,
    -- increasing for older ones) so its own backlog is claimed newest-first without ever
    -- starving other job types: any priority-0 row of ANY type is at least as eligible as an
    -- older vi_backfill window.
    priority         BIGINT NOT NULL DEFAULT 0,
    -- window_start_sec/window_end_sec (issue #529): populated ONLY for vi_backfill rows (NULL for
    -- every other job_type), denormalized OUT of detail's JSONB blob into real, indexed columns.
    -- detail remains the source of truth for the full ViBackfillDetail struct; these are a
    -- query-optimization-only denormalized copy. column_hash/column_type used to live here too
    -- (issue #529) but moved off the parent row entirely into vi_backfill_job_columns (issue
    -- #533) -- column identity is now membership, not job identity; see that table's own comment
    -- below.
    window_start_sec BIGINT NULL,
    window_end_sec   BIGINT NULL,
    -- block_object_key (issue #532): populated ONLY for NEW-model vi_backfill rows -- one job
    -- per (column, actual trace block) instead of one job per synthetic 1-minute wall-clock
    -- window. NULL for every other job_type AND for OLD-model vi_backfill rows (the two shapes
    -- coexist during the transition -- see pgqueue/NOTES.md and ViBackfillDetail's own doc
    -- comment). window_start_sec/window_end_sec are still populated for NEW-model rows too
    -- (from the block's own real MinSec/MaxSec), so ViBackfillGapRanges' existing time-range
    -- coverage query works unchanged across both shapes without needing to know which one
    -- produced a given row.
    block_object_key TEXT NULL
);

-- ALTER TABLE ... ADD COLUMN IF NOT EXISTS (issue #529): CREATE TABLE IF NOT EXISTS above is a
-- no-op against an already-live compaction_jobs table -- it does NOT retrofit newly-added
-- columns onto an existing table, so every column added after this table's original rollout
-- needs its own explicit, idempotent ALTER TABLE here (mirrors the same fix already applied for
-- pgcatalog's blockpack_file_catalog.meta column gap).
ALTER TABLE compaction_jobs ADD COLUMN IF NOT EXISTS priority BIGINT NOT NULL DEFAULT 0;
ALTER TABLE compaction_jobs ADD COLUMN IF NOT EXISTS window_start_sec BIGINT NULL;
ALTER TABLE compaction_jobs ADD COLUMN IF NOT EXISTS window_end_sec BIGINT NULL;
ALTER TABLE compaction_jobs ADD COLUMN IF NOT EXISTS block_object_key TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_claimable
    ON compaction_jobs (job_type, status, created_at)
    WHERE status = 'pending';

-- DROP+recreate CONCURRENTLY: see idx_compaction_jobs_dedup_active's own comment below for why
-- "IF NOT EXISTS" alone can't apply a definition change to an already-existing index. CONCURRENTLY
-- on both statements (issue #529 incident, 2026-07-23): a plain (non-concurrent) DROP+CREATE
-- takes a lock that blocks EVERY other write against compaction_jobs for as long as the rebuild
-- takes -- harmless at this table's original, small size, but once the #529 bulk-insert grew it
-- past a million rows, a routine compaction-worker pod restart's schema-apply (re-running this
-- exact statement on every single startup) stalled the ENTIRE fleet's claim/complete throughput
-- for several seconds at a time. CONCURRENTLY costs more total rebuild time and cannot run inside
-- an explicit transaction block (fine here -- pgschema.ApplyStatements executes each statement as
-- its own autocommitted Exec call, never wrapped in BEGIN/COMMIT) but never blocks concurrent
-- reads or writes.
--
-- status != 'succeeded' (issue #529 incident, 2026-07-23): originally scoped to status = 'pending'
-- only, matching this index's own name -- but Claim's real query (claimJobSQL below) is a 4-way OR
-- across pending, lease-expired claimed/running, and retry-due failed rows, and Postgres can't use
-- a partial index scoped to just ONE of those branches to satisfy an ORDER BY + LIMIT 1 across all
-- four without risking a wrong answer, so it fell back to a full parallel sequential scan --
-- correct, but a 1.24M-row table turns "correct" into a 3.6-SECOND claim attempt, repeated by
-- every worker, every claim. status != 'succeeded' covers all four OR branches in one partial
-- index (a succeeded row is the only status this claim query never wants), restoring the
-- ORDER-BY-driven index scan's early-exit behavior regardless of total table size.
DROP INDEX CONCURRENTLY IF EXISTS idx_compaction_jobs_claimable_priority;
CREATE INDEX CONCURRENTLY idx_compaction_jobs_claimable_priority
    ON compaction_jobs (priority, created_at)
    WHERE status != 'succeeded';

-- DROP+recreate CONCURRENTLY rather than plain CREATE ... IF NOT EXISTS: an index's WHERE
-- predicate is part of its identity to Postgres, but "IF NOT EXISTS" only
-- checks the NAME -- a schema.sql change to this predicate would otherwise
-- silently never apply to an already-existing live database, leaving it
-- permanently running the stale definition. CONCURRENTLY (issue #529 incident, 2026-07-23): see
-- idx_compaction_jobs_claimable_priority's own comment above for why a non-concurrent rebuild on
-- this table's current size blocks the whole fleet, not just this index's own readers.
--
-- status != 'succeeded' (not just pending/claimed/running): a 'failed' row is
-- NEVER terminal (pgqueue.Store.Fail unconditionally schedules a retry, see
-- its own doc comment) and still represents the exact same candidate pair
-- until it succeeds or its inputs are otherwise excluded from candidacy --
-- compaction-planner's own periodic re-planning pass has no memory of what
-- it already inserted, so a 'failed' row not counting as "active" let a
-- fresh insert create a SECOND row with the identical dedup_key. The
-- resulting collision wasn't just a wasted duplicate: the FIRST claim
-- attempt on EITHER row that tried to transition it to 'claimed' violated
-- this same unique index (both rows sharing one key), and since Claim's own
-- query always selects the single oldest claimable row, that one failing
-- UPDATE permanently jammed the entire queue behind it -- found live on
-- tempo-dev-test-03, 2026-07-21.
DROP INDEX CONCURRENTLY IF EXISTS idx_compaction_jobs_dedup_active;
CREATE UNIQUE INDEX CONCURRENTLY idx_compaction_jobs_dedup_active
    ON compaction_jobs (dedup_key)
    WHERE status != 'succeeded';

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_tenant_type_status
    ON compaction_jobs (tenant, job_type, status);

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_lease_expiry
    ON compaction_jobs (lease_expires_at)
    WHERE status IN ('claimed', 'running');

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_retry_due
    ON compaction_jobs (next_retry_at)
    WHERE status = 'failed' AND next_retry_at IS NOT NULL;

-- issue #533: column_hash/column_type move off the parent row entirely into
-- vi_backfill_job_columns (column membership, not job identity). These 3 indexes all
-- reference column_hash/column_type directly and must be dropped before the columns
-- themselves can be dropped. None are recreated in the new column-less shape --
-- MissingViBackfillBlocks/ViBackfillGapRanges become JOINs against the new child table's
-- own idx_vi_backfill_job_columns_lookup index instead (see below).
DROP INDEX CONCURRENTLY IF EXISTS idx_vi_backfill_coverage_gap;
DROP INDEX CONCURRENTLY IF EXISTS idx_vi_backfill_all_windows;
DROP INDEX CONCURRENTLY IF EXISTS idx_vi_backfill_all_blocks;

-- DROP COLUMN is a fast, metadata-only operation in Postgres (marks the column dropped in
-- the catalog; does not rewrite existing rows) -- NOT a repeat of this table's documented
-- index-rebuild-locking incident (see idx_compaction_jobs_claimable_priority's own comment).
ALTER TABLE compaction_jobs DROP COLUMN IF EXISTS column_hash;
ALTER TABLE compaction_jobs DROP COLUMN IF EXISTS column_type;

-- vi_backfill_job_columns (issue #533): column membership + per-column status for a
-- block-shaped vi_backfill job. ON DELETE CASCADE: the live rollout's `DELETE FROM
-- compaction_jobs WHERE job_type = 'vi_backfill'` step (see NOTES.md's own rollout entry)
-- cascades to this table automatically, no separate cleanup needed.
CREATE TABLE IF NOT EXISTS vi_backfill_job_columns (
    id           BIGSERIAL PRIMARY KEY,
    job_id       UUID NOT NULL REFERENCES compaction_jobs(id) ON DELETE CASCADE,
    column_hash  TEXT NOT NULL,
    column_type  TEXT NOT NULL,
    column_name  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    succeeded_at TIMESTAMPTZ NULL,
    UNIQUE (job_id, column_hash, column_type)
);

-- idx_vi_backfill_job_columns_lookup: backs both the rewritten ViBackfillGapRanges (filters
-- column_hash/column_type/status, joins job_id -> compaction_jobs.id) and the rewritten
-- MissingViBackfillBlocks (filters column_hash/column_type only, status-agnostic --
-- Postgres can still use this index's leading columns even when the status predicate is
-- omitted, at the cost of scanning slightly more rows within one (column_hash, column_type)
-- group).
DROP INDEX CONCURRENTLY IF EXISTS idx_vi_backfill_job_columns_lookup;
CREATE INDEX CONCURRENTLY idx_vi_backfill_job_columns_lookup
    ON vi_backfill_job_columns (column_hash, column_type, status, job_id);

-- idx_vi_backfill_job_columns_pending: backs ViBackfillFinalize's "any pending children left
-- for this job?" check and ViBackfillPendingColumns' per-job pending-column lookup. Partial on
-- status != 'succeeded' -- mirrors idx_compaction_jobs_dedup_active's own "succeeded is the
-- only status this query never wants" rationale.
DROP INDEX CONCURRENTLY IF EXISTS idx_vi_backfill_job_columns_pending;
CREATE INDEX CONCURRENTLY idx_vi_backfill_job_columns_pending
    ON vi_backfill_job_columns (job_id)
    WHERE status != 'succeeded';
