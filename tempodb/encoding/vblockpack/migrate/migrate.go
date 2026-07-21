package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SplitStatements splits sql into individual executable statements, stripping
// `--` line comments first so a comment containing an ordinary English
// semicolon is never mistaken for a statement boundary. Extracted from
// ../pg_testutil_test.go's original applySchema/stripSQLLineComments, which
// this package's schema files and file_catalog.sql both rely on.
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
// SplitStatements) against pool, in order. Shared by this repo's own test infra
// for file_catalog.sql and schema/schema.go's ApplyFileCatalog, both of which
// need the identical comment/semicolon splitting behavior. (Apply, the former
// backend_jobs-specific wrapper around this function, was removed 2026-07-21
// along with backend_jobs itself, once vi_backfill/cube_backfill -- backend_jobs'
// only two job types -- retired entirely in favor of blockpack's own
// compaction_jobs queue.)
func ApplyStatements(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	for _, stmt := range SplitStatements(sql) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: apply statement: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}
