-- file_catalog.sql — reviewed artifact only. NOT applied to any live Postgres
-- instance by this repo. Tests (Part 5) apply this verbatim against an
-- ephemeral testcontainers-go Postgres instance only.
--
-- Populated by a background reconciliation loop inside backend-scheduler
-- (see modules/backendscheduler/filecatalog) against its already-live,
-- already-maintained per-tenant BlockMetas snapshot. Read by
-- catalogBlockFetcher via a persisted per-column cursor
-- (BackfillState.LastCatalogRowID) instead of live-listing S3/local storage
-- on every backfill run.
CREATE TABLE IF NOT EXISTS file_catalog (
    row_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant      TEXT   NOT NULL,
    block_id    TEXT   NOT NULL,   -- uuid.UUID.String()
    block_ref   TEXT   NOT NULL,   -- "<tenant>/<block_id>/data.blockpack" (blockObjectKey format, reused verbatim)
    start_sec   BIGINT NOT NULL,
    end_sec     BIGINT NOT NULL,
    size_bytes  BIGINT NOT NULL DEFAULT 0,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ NULL,   -- soft delete; set by the lister's reconciliation pass
    UNIQUE (tenant, block_id)
);

-- Primary access pattern: cursor-based incremental discovery per tenant, live rows only.
CREATE INDEX IF NOT EXISTS idx_file_catalog_tenant_rowid_live
    ON file_catalog (tenant, row_id)
    WHERE deleted_at IS NULL;
