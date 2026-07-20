package schema

import (
	"context"
	"testing"
)

func TestApplyFileCatalog_CreatesFileCatalogAndTenantRedactionStateTables(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := ApplyFileCatalog(ctx, pool); err != nil {
		t.Fatalf("ApplyFileCatalog: %v", err)
	}

	var tableCount int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('file_catalog', 'tenant_redaction_state')
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	if tableCount != 2 {
		t.Fatalf("expected both file_catalog and tenant_redaction_state to exist, found %d", tableCount)
	}

	var columnCount int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'file_catalog'
		  AND column_name IN ('compaction_level', 'compacted_at')
	`).Scan(&columnCount)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	if columnCount != 2 {
		t.Fatalf("expected file_catalog.compaction_level/compacted_at (issue #522 #143) to exist, found %d", columnCount)
	}
}

func TestApplyFileCatalog_IsIdempotent(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := ApplyFileCatalog(ctx, pool); err != nil {
		t.Fatalf("ApplyFileCatalog (first): %v", err)
	}
	if err := ApplyFileCatalog(ctx, pool); err != nil {
		t.Fatalf("ApplyFileCatalog (second, must be a safe no-op): %v", err)
	}
}
