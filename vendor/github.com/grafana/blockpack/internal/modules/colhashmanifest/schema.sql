-- schema.sql — colhashmanifest's Postgres schema: a generic key/blob table,
-- NOT a normalized per-Entry table (Open Decision D1, approved by
-- spec-oracle-506). Store is genuinely object-storage-shaped
-- (Get(ctx,key)([]byte,error) / Put(ctx,key,data)error), not row-oriented
-- like cube/viusage's EntryStore -- RecordColumn/Load's business logic
-- already lives entirely in manifest.go and calls Store.Get/Put directly, so
-- a plain key/blob mirror of the blob-backed path is trivially, provably
-- identical in behavior (Get returns exactly what Put last stored).
CREATE TABLE IF NOT EXISTS column_manifest_blobs (
    key        TEXT   NOT NULL PRIMARY KEY,
    data       BYTEA  NOT NULL,
    updated_at BIGINT NOT NULL DEFAULT 0
);
