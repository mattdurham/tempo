-- schema.sql — viusage's Postgres schema, ported from tempo's
-- tempodb/encoding/vblockpack/schema/registries.sql (viusage_entries section
-- only). viusage_query_log is deliberately EXCLUDED (Open Decision D2, plan
-- .bob/state/plan.md): it is Postgres-backend-only schema with no
-- corresponding blockpack read/write code today, and porting a table nothing
-- writes to would be dead weight and a false signal that it's load-bearing.
--
-- viusage: one row per (tenant, col_hash, col_type). Replaces the shared
-- <tenant>/viusage/index.json blob's per-tenant contention point.
CREATE TABLE IF NOT EXISTS viusage_entries (
    tenant                TEXT    NOT NULL,
    col_hash              TEXT    NOT NULL,
    col_type              TEXT    NOT NULL,
    column_name           TEXT    NOT NULL,
    first_seen_sec        BIGINT  NOT NULL DEFAULT 0,
    created_at            BIGINT  NOT NULL DEFAULT 0,
    lease_owner_id        TEXT    NOT NULL DEFAULT '',
    lease_expires_at      BIGINT  NOT NULL DEFAULT 0,
    watermark_sec         BIGINT  NOT NULL DEFAULT 0,
    window_start_sec      BIGINT  NOT NULL DEFAULT 0,
    window_end_sec        BIGINT  NOT NULL DEFAULT 0,
    triggered             BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_in_progress  BOOLEAN NOT NULL DEFAULT FALSE,
    done                  BOOLEAN NOT NULL DEFAULT FALSE,
    last_catalog_row_id   BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, col_hash, col_type)
);
