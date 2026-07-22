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
    -- column_hash/column_type/window_start_sec/window_end_sec (issue #529): populated ONLY for
    -- vi_backfill rows (NULL for every other job_type), denormalized OUT of detail's JSONB blob
    -- into real, indexed columns specifically so the query-time coverage check
    -- (ViBackfillCoverageGap) can efficiently answer "does any non-succeeded window overlap
    -- [minSec, maxSec] for this column" without a JSONB field-extraction scan across
    -- potentially tens of thousands of rows per column. detail remains the source of truth for
    -- the full ViBackfillDetail struct; these are a query-optimization-only denormalized copy.
    column_hash      TEXT NULL,
    column_type      TEXT NULL,
    window_start_sec BIGINT NULL,
    window_end_sec   BIGINT NULL
);

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_claimable
    ON compaction_jobs (job_type, status, created_at)
    WHERE status = 'pending';

-- DROP+recreate: see idx_compaction_jobs_dedup_active's own comment below for why "IF NOT
-- EXISTS" alone can't apply a definition change to an already-existing index.
DROP INDEX IF EXISTS idx_compaction_jobs_claimable_priority;
CREATE INDEX idx_compaction_jobs_claimable_priority
    ON compaction_jobs (priority, created_at)
    WHERE status = 'pending';

-- DROP+recreate rather than plain CREATE ... IF NOT EXISTS: an index's WHERE
-- predicate is part of its identity to Postgres, but "IF NOT EXISTS" only
-- checks the NAME -- a schema.sql change to this predicate would otherwise
-- silently never apply to an already-existing live database, leaving it
-- permanently running the stale definition. Cheap and safe to redo on every
-- startup (rebuilds fast at this table's realistic size; touches no data).
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
DROP INDEX IF EXISTS idx_compaction_jobs_dedup_active;
CREATE UNIQUE INDEX idx_compaction_jobs_dedup_active
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

-- idx_vi_backfill_coverage_gap (issue #529): backs ViBackfillCoverageGap's "does any
-- non-succeeded vi_backfill window overlap [minSec, maxSec] for this column" query. Scoped to
-- status != 'succeeded' specifically because that is the ONLY subset this query ever needs
-- (and, in steady state, a small one -- most windows behind the priority-ordered claim frontier
-- succeed quickly; the non-succeeded set concentrates on the still-catching-up historical tail
-- and any genuinely stuck/retrying windows).
DROP INDEX IF EXISTS idx_vi_backfill_coverage_gap;
CREATE INDEX idx_vi_backfill_coverage_gap
    ON compaction_jobs (tenant, column_hash, column_type, window_end_sec)
    WHERE job_type = 'vi_backfill' AND status != 'succeeded';
