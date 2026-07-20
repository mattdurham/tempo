package vblockpack

// file_catalog_schema_test.go — proves schema/file_catalog.sql's #522 Phase
// 0.2 additions (compaction_level, compacted_at, tenant_redaction_state) are
// valid, executable SQL and idempotent, using the same real-Postgres
// testcontainer infra as pg_testutil_test.go's own smoke test.

import (
	"context"
	"testing"
)

func TestFileCatalogSchema_CompactionColumnsExist(t *testing.T) {
	pool := newTestPostgresPool(t)

	rows, err := pool.Query(context.Background(), `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'file_catalog'
		  AND column_name IN ('compaction_level', 'compacted_at')
	`)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	defer rows.Close()

	found := map[string]struct {
		dataType   string
		nullable   string
		defaultVal *string
	}{}
	for rows.Next() {
		var name, dataType, nullable string
		var defaultVal *string
		if err := rows.Scan(&name, &dataType, &nullable, &defaultVal); err != nil {
			t.Fatalf("scanning row: %v", err)
		}
		found[name] = struct {
			dataType   string
			nullable   string
			defaultVal *string
		}{dataType, nullable, defaultVal}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating rows: %v", err)
	}

	level, ok := found["compaction_level"]
	if !ok {
		t.Fatal("expected compaction_level column to exist on file_catalog")
	}
	if level.dataType != "integer" || level.nullable != "NO" || level.defaultVal == nil {
		t.Fatalf("compaction_level column has unexpected shape: %+v", level)
	}

	compactedAt, ok := found["compacted_at"]
	if !ok {
		t.Fatal("expected compacted_at column to exist on file_catalog")
	}
	if compactedAt.dataType != "timestamp with time zone" || compactedAt.nullable != "YES" {
		t.Fatalf("compacted_at column has unexpected shape: %+v", compactedAt)
	}
}

func TestFileCatalogSchema_TenantRedactionStateTableExists(t *testing.T) {
	pool := newTestPostgresPool(t)

	var tableCount int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'tenant_redaction_state'
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	if tableCount != 1 {
		t.Fatalf("expected tenant_redaction_state table to exist, found %d", tableCount)
	}

	_, err = pool.Exec(context.Background(), `
		INSERT INTO tenant_redaction_state (tenant, pending, batch_id, started_at)
		VALUES ('tenant-a', TRUE, 'batch-1', now())
	`)
	if err != nil {
		t.Fatalf("inserting into tenant_redaction_state: %v", err)
	}

	var pending bool
	err = pool.QueryRow(context.Background(),
		`SELECT pending FROM tenant_redaction_state WHERE tenant = 'tenant-a'`).Scan(&pending)
	if err != nil {
		t.Fatalf("querying tenant_redaction_state: %v", err)
	}
	if !pending {
		t.Fatal("expected pending to be TRUE")
	}
}

func TestFileCatalogSchema_ApplyIsIdempotent(t *testing.T) {
	pool := newTestPostgresPool(t)

	// newTestPostgresPool already applied schema/file_catalog.sql once.
	// Re-applying it must be a safe no-op -- proves the ALTER TABLE ... ADD
	// COLUMN IF NOT EXISTS and CREATE TABLE IF NOT EXISTS statements are
	// genuinely idempotent, not just "happens to work on an empty database".
	applySchema(context.Background(), t, pool, "schema/file_catalog.sql")
}
