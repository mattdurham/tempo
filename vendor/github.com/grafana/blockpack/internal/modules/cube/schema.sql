-- schema.sql — cube's Postgres schema, ported from tempo's
-- tempodb/encoding/vblockpack/schema/registries.sql (cube_entries section only).
--
-- cube: one row per active cube. dimensions/filters/agg_attrs/watermarks are
-- variable-shape/nested (Filters especially — ColumnFilter has its own sub-
-- structure) and bounded in practice (small watermark maps keyed by resolution
-- level) — stored as JSONB rather than fully normalized into child tables, a
-- deliberate simplicity choice for this data's actual shape and access pattern
-- (loaded whole per cube, never queried by sub-field).
CREATE TABLE IF NOT EXISTS cube_entries (
    cube_id      TEXT    NOT NULL PRIMARY KEY,
    tenant       TEXT    NOT NULL,
    dimensions   JSONB   NOT NULL DEFAULT '[]',
    filters      JSONB   NOT NULL DEFAULT '[]',
    agg_attrs    JSONB   NOT NULL DEFAULT '[]',
    resolution   INT     NOT NULL DEFAULT 1,
    created_at   BIGINT  NOT NULL DEFAULT 0,
    watermarks   JSONB   NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_cube_entries_tenant ON cube_entries (tenant);
