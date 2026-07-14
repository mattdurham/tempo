package migrate

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed backend_jobs.sql
var backendJobsSchema string

// SplitStatements splits sql into individual executable statements, stripping
// `--` line comments first so a comment containing an ordinary English
// semicolon is never mistaken for a statement boundary. Extracted from
// ../pg_testutil_test.go's original applySchema/stripSQLLineComments, which
// this package's schema files and registries.sql/file_catalog.sql all rely on.
func SplitStatements(sql string) []string {
	var stmts []string
	for _, stmt := range strings.Split(stripSQLLineComments(sql), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmts = append(stmts, stmt)
	}
	return stmts
}

func stripSQLLineComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx != -1 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

// ApplyStatements executes every statement in sql (as produced by
// SplitStatements) against pool, in order. Shared by Apply (backend_jobs.sql)
// and by this repo's own test infra for registries.sql/file_catalog.sql,
// which need the identical comment/semicolon splitting behavior.
func ApplyStatements(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	for _, stmt := range SplitStatements(sql) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: apply statement: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}

// Apply runs the embedded backend_jobs schema against pool. Every statement is
// written with IF NOT EXISTS (tables/indexes), making a repeated Apply call on
// an already-migrated database a safe no-op -- this is the entire "migration"
// mechanism for Phase 1: no version table, no up/down pairs, no migration
// history tracking. Sufficient because there is exactly one schema and it only
// ever grows additively for the foreseeable future.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ApplyStatements(ctx, pool, backendJobsSchema); err != nil {
		return fmt.Errorf("apply backend_jobs schema: %w", err)
	}
	return nil
}
