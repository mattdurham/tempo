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
    next_retry_at    TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_compaction_jobs_claimable
    ON compaction_jobs (job_type, status, created_at)
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
