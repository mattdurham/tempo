-- schema.sql — blockpack_file_catalog's Postgres schema (issue #522, plan.md
-- Section B). One shared table, discriminated by `subsystem`, for VI/VCNT/cube
-- physical-object bookkeeping: NOT merged with tempo's own `file_catalog`
-- (different repo/Postgres-ownership boundary; two tables system-wide).
--
-- `object_key` is UNIQUE: Store.Insert relies on this for its
-- ON CONFLICT (object_key) DO NOTHING idempotency (a merge job retried after a
-- crash between writing its output object and reporting job success must not
-- fail with a duplicate-key error on re-insert -- see plan.md Section E).
CREATE TABLE IF NOT EXISTS blockpack_file_catalog (
    row_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subsystem     TEXT   NOT NULL,          -- 'vi' | 'vcnt' | 'cube'
    tenant        TEXT   NOT NULL,
    resource_id   TEXT   NOT NULL,          -- colHash+colType (VI), colHash (VCNT), cubeID (cube)
    object_key    TEXT   NOT NULL UNIQUE,   -- full object-storage key
    level         INT    NOT NULL DEFAULT 0,  -- merge-depth
    min_sec       BIGINT NOT NULL,
    max_sec       BIGINT NOT NULL,
    size_bytes    BIGINT NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    compacted_at  TIMESTAMPTZ NULL,   -- set the moment this file is superseded by a
                                       -- merge output; NULL = live and eligible
    deleted_at    TIMESTAMPTZ NULL    -- set the moment the reaper physically deletes
                                       -- the object; row is then eligible for hard
                                       -- delete from Postgres itself
);

CREATE INDEX IF NOT EXISTS idx_bfc_candidacy
    ON blockpack_file_catalog (subsystem, tenant, resource_id, level)
    WHERE compacted_at IS NULL AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_bfc_reap
    ON blockpack_file_catalog (compacted_at)
    WHERE compacted_at IS NOT NULL AND deleted_at IS NULL;
