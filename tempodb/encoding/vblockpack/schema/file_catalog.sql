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

-- compaction_level/compacted_at (#522 Phase 0.2): added so file_catalog can
-- become the primary source for vblockpack-encoded tenants' compaction
-- candidate selection (Phase 4c), mirroring blockpack_file_catalog's
-- level/compacted_at columns. deleted_at keeps its existing "vanished from
-- filesystem/blocklist" meaning; compacted_at is distinct -- set the moment a
-- row is superseded by a pairwise merge output, while the row (and its
-- object) is still physically present during the reaper's grace window.
ALTER TABLE file_catalog ADD COLUMN IF NOT EXISTS compaction_level INT NOT NULL DEFAULT 0;
ALTER TABLE file_catalog ADD COLUMN IF NOT EXISTS compacted_at TIMESTAMPTZ NULL;

-- tenant_redaction_state (#522 Phase 0.2/4c): mirrors backend-scheduler's
-- existing in-memory work.Interface TenantPending state into Postgres, so
-- job-planner's trace-compaction candidate query (which has no access to
-- that in-memory state) can exclude any tenant with a redaction batch in
-- flight via a simple anti-join, without job-planner gaining a dependency on
-- backend-scheduler's work-queue internals.
CREATE TABLE IF NOT EXISTS tenant_redaction_state (
    tenant      TEXT PRIMARY KEY,
    pending     BOOLEAN NOT NULL DEFAULT FALSE,
    batch_id    TEXT NULL,
    started_at  TIMESTAMPTZ NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
