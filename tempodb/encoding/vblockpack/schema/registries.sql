-- registries.sql — reviewed artifact only. NOT applied to any live Postgres
-- instance by this repo. Tests (Part 5) apply this verbatim against an
-- ephemeral testcontainers-go Postgres instance only.
--
-- viusage: one row per (tenant, col_hash, col_type). Replaces the shared
-- <tenant>/viusage/index.json blob's per-tenant contention point.
CREATE TABLE IF NOT EXISTS viusage_entries (
    tenant              TEXT    NOT NULL,
    col_hash            TEXT    NOT NULL,
    col_type            TEXT    NOT NULL,
    column_name         TEXT    NOT NULL,
    first_seen_sec       BIGINT  NOT NULL DEFAULT 0,
    created_at           BIGINT  NOT NULL DEFAULT 0,
    lease_owner_id        TEXT    NOT NULL DEFAULT '',
    lease_expires_at       BIGINT  NOT NULL DEFAULT 0,
    watermark_sec         BIGINT  NOT NULL DEFAULT 0,
    window_start_sec       BIGINT  NOT NULL DEFAULT 0,
    window_end_sec         BIGINT  NOT NULL DEFAULT 0,
    triggered             BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_in_progress    BOOLEAN NOT NULL DEFAULT FALSE,
    done                  BOOLEAN NOT NULL DEFAULT FALSE,
    last_catalog_row_id     BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, col_hash, col_type)
);

-- viusage: the "list of queries" — genuinely unbounded, append-only, DECOUPLED
-- from the hot-path entries row above (resolves the brainstorm's Q2 in favor of
-- the "expanded reading": the user's own phrasing lists "backfill state AND the
-- list of queries" as two distinct things; with the repeated-use ring removed by
-- Part 0, there is no remaining hot-path reason to bound or co-locate this data).
-- Never read by RecordUseAndMaybeTrigger's trigger logic (which no longer needs
-- to count anything) — write-only from the application's perspective, an
-- observability/audit capability only. Postgres-backend-only: the blob-backed
-- (S3/Local/GCS/Azure) path gets NO equivalent (never had one; not a regression).
CREATE TABLE IF NOT EXISTS viusage_query_log (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant      TEXT NOT NULL,
    col_hash    TEXT NOT NULL,
    col_type    TEXT NOT NULL,
    queried_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_viusage_query_log_lookup
    ON viusage_query_log (tenant, col_hash, col_type, queried_at DESC);

-- cube: NO LONGER DEFINED HERE (issue #504, 2026-07-15). Tempo's own local
-- pgCubeEntryStore/cube_entries table (this section used to define it) was deleted --
-- cube's Postgres-backed registry is now blockpack's own native implementation
-- (blockpack.NewPgCubeRegistry / cube.PgEntryStore, issue #506), which owns its own
-- schema (applied via blockpack.ApplyCubeSchema, see blockpack's internal/modules/cube/
-- pg_entry_store.go / schema.sql) entirely independently of this file. viusage's tables
-- above are unaffected -- #504's scope is cube-only.
