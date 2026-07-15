// Package pgschema provides schema-application plumbing shared by every
// Postgres-backed module in this repo (cube, viusage, colhashmanifest).
// Ported verbatim from tempo's tempodb/encoding/vblockpack/migrate/migrate.go
// SplitStatements/ApplyStatements. This is schema-application mechanics, not
// business logic -- the one kind of shared helper this effort's own
// "mirror each module's own shape, don't force a generic abstraction"
// convention explicitly permits.
package pgschema

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SplitStatements splits sql into individual executable statements, stripping
// `--` line comments first so a comment containing an ordinary English
// semicolon is never mistaken for a statement boundary.
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
// SplitStatements) against pool, in order.
func ApplyStatements(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	for _, stmt := range SplitStatements(sql) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgschema: apply statement: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}
