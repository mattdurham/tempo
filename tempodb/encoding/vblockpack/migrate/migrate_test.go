package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSplitStatements_HandlesLineCommentsWithSemicolons(t *testing.T) {
	sql := `
-- a comment; with a semicolon in it, not a statement boundary
CREATE TABLE IF NOT EXISTS foo (id INT); -- another comment; also with one
CREATE INDEX IF NOT EXISTS idx_foo ON foo (id);
`
	got := SplitStatements(sql)
	want := []string{
		"CREATE TABLE IF NOT EXISTS foo (id INT)",
		"CREATE INDEX IF NOT EXISTS idx_foo ON foo (id)",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d statements, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statement %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSplitStatements_SkipsBlankStatements(t *testing.T) {
	got := SplitStatements("  ;;\n-- only a comment\n  ;")
	if len(got) != 0 {
		t.Fatalf("expected 0 statements from all-blank/comment input, got %v", got)
	}
}

func indexExists(ctx context.Context, t *testing.T, pool *pgxpool.Pool, indexName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)`, indexName).Scan(&exists)
	if err != nil {
		t.Fatalf("querying pg_indexes for %s: %v", indexName, err)
	}
	return exists
}

func TestApply_CreatesBackendJobsTable(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := Apply(ctx, pool); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var tableExists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'backend_jobs'
		)
	`).Scan(&tableExists)
	if err != nil {
		t.Fatalf("querying information_schema.tables: %v", err)
	}
	if !tableExists {
		t.Fatalf("expected backend_jobs table to exist after Apply, with no other schema files applied")
	}

	for _, idx := range []string{
		"idx_backend_jobs_claimable",
		"idx_backend_jobs_dedup_active",
		"idx_backend_jobs_tenant_type_status",
		"idx_backend_jobs_lease_expiry",
		"idx_backend_jobs_retry_due",
	} {
		if !indexExists(ctx, t, pool, idx) {
			t.Fatalf("expected index %s to exist after Apply", idx)
		}
	}
}

func TestApply_IdempotentOnAlreadyMigratedDatabase(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := Apply(ctx, pool); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := Apply(ctx, pool); err != nil {
		t.Fatalf("second Apply (expected no-op) failed: %v", err)
	}
}

func TestApply_DedupIndexRejectsSecondActiveRowSameKey(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := Apply(ctx, pool); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// md5(...)::uuid avoids depending on the pgcrypto extension being
	// installed (gen_random_uuid() would require it).
	const insertSQL = `
		INSERT INTO backend_jobs (id, job_type, tenant, status, detail, dedup_key)
		VALUES (md5(random()::text || clock_timestamp()::text)::uuid, 'vi_backfill', 'tenant-a', $1, '{}', 'dedup-key-a')
	`

	if _, err := pool.Exec(ctx, insertSQL, "pending"); err != nil {
		t.Fatalf("first insert (pending): %v", err)
	}

	_, err := pool.Exec(ctx, insertSQL, "pending")
	if err == nil {
		t.Fatalf("expected second INSERT with same dedup_key while first is still pending to fail with a unique violation, got no error")
	}
	if !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
		t.Fatalf("expected a unique-violation error, got: %v", err)
	}
}

// TestApply_DedupIndexAllowsTwoTerminalRowsSameKey is the sibling of
// TestApply_DedupIndexRejectsSecondActiveRowSameKey: two rows sharing the same
// dedup_key are allowed once both are in a TERMINAL status (here,
// 'succeeded'), since the partial unique index only covers
// pending/claimed/running. This is the mutation-test guard for this phase --
// see the plan's Phase 0 mutation-test note: a corrupted, non-partial (plain)
// unique index over dedup_key would WRONGLY reject this case.
func TestApply_DedupIndexAllowsTwoTerminalRowsSameKey(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	if err := Apply(ctx, pool); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	const insertSQL = `
		INSERT INTO backend_jobs (id, job_type, tenant, status, detail, dedup_key)
		VALUES (md5(random()::text || clock_timestamp()::text)::uuid, 'vi_backfill', 'tenant-a', 'succeeded', '{}', 'dedup-key-terminal')
	`

	if _, err := pool.Exec(ctx, insertSQL); err != nil {
		t.Fatalf("first insert (succeeded): %v", err)
	}
	if _, err := pool.Exec(ctx, insertSQL); err != nil {
		t.Fatalf("second insert with same dedup_key, both terminal ('succeeded'), should be allowed by the partial index but failed: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = 'dedup-key-terminal'`).Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 terminal rows with the same dedup_key to coexist, found %d", count)
	}
}
