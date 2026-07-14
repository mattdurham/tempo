-- backend_jobs.sql — the FIRST schema file in this repo applied by a real
-- migration runner (see migrate.go), not a "reviewed artifact only" file
-- like ../schema/registries.sql/file_catalog.sql.
-- Durable job queue for vi_backfill/cube_backfill (Phase 1 scope only --
-- compaction/retention/redaction stay on the existing in-memory work.Work +
-- local-disk/backend-blob flush path, and keep dispatching via the existing
-- gRPC Next()/UpdateJob() pair).
CREATE TABLE IF NOT EXISTS backend_jobs (
    id               UUID PRIMARY KEY,
    job_type         TEXT NOT NULL,                     -- 'vi_backfill' | 'cube_backfill'
    tenant           TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',    -- pending|claimed|running|succeeded|failed
    detail           JSONB NOT NULL,                     -- {column_hash,column_name,column_type} or {cube_id,window_minutes}
    -- Idempotency key: job_type||tenant||col_hash||col_type for vi_backfill,
    -- job_type||tenant||cube_id for cube_backfill. The job_type prefix is
    -- defense-in-depth against a future collision between the two job types'
    -- id formats -- today's formats can never collide, but nothing guarantees
    -- that stays true forever. Enforced unique-while-non-terminal by the
    -- partial index below, NOT by a table constraint (Postgres doesn't
    -- support conditional UNIQUE constraints).
    dedup_key        TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at       TIMESTAMPTZ NULL,
    claimed_by       TEXT NULL,                          -- worker_id (os.Hostname(), mirrors BackendWorker.workerID)
    lease_expires_at TIMESTAMPTZ NULL,                    -- mirrors viusage's R8 lease semantics
    started_at       TIMESTAMPTZ NULL,
    finished_at      TIMESTAMPTZ NULL,
    retries          INT NOT NULL DEFAULT 0,
    last_error       TEXT NULL,
    next_retry_at    TIMESTAMPTZ NULL                    -- NULL until a failure schedules a retry
);

-- Claim query's index: workers scan for the oldest pending/ready-to-retry job of their type.
CREATE INDEX IF NOT EXISTS idx_backend_jobs_claimable
    ON backend_jobs (job_type, status, created_at)
    WHERE status = 'pending';

-- Dedup enforcement: at most one non-terminal row per dedup_key, ever. A terminal row
-- (succeeded/failed with retries exhausted) frees the key for a brand-new job.
CREATE UNIQUE INDEX IF NOT EXISTS idx_backend_jobs_dedup_active
    ON backend_jobs (dedup_key)
    WHERE status IN ('pending', 'claimed', 'running');

-- Operational visibility (StatusHandler-equivalent queries, ad-hoc debugging).
CREATE INDEX IF NOT EXISTS idx_backend_jobs_tenant_type_status
    ON backend_jobs (tenant, job_type, status);

-- Lease-expiry sweep (crashed-worker self-heal, mirrors viusage's R8).
CREATE INDEX IF NOT EXISTS idx_backend_jobs_lease_expiry
    ON backend_jobs (lease_expires_at)
    WHERE status IN ('claimed', 'running');

-- Retry sweep: jobs that failed and are scheduled for a future retry attempt.
CREATE INDEX IF NOT EXISTS idx_backend_jobs_retry_due
    ON backend_jobs (next_retry_at)
    WHERE status = 'failed' AND next_retry_at IS NOT NULL;
