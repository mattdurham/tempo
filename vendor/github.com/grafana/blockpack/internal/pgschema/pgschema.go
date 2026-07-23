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
	"hash/fnv"
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

// ApplyStatements executes every statement in sql (as produced by SplitStatements) against pool,
// in order, after first winning a NON-BLOCKING session-level advisory lock keyed by a hash of sql
// itself (issue #529 incident, 2026-07-23): every Postgres-backed component calls this at its own
// startup, so a rolling deploy or any coincidental restart of several replicas at once runs the
// SAME DROP+CREATE INDEX CONCURRENTLY/ALTER TABLE statements from many sessions simultaneously.
// A caller that loses the race (pg_try_advisory_lock returns false) skips applying entirely and
// returns success -- whichever caller DID win is applying the identical target schema, so its
// completion (or a later restart's own retry, if it crashes first) is sufficient; this caller
// doesn't need to wait for it.
//
// A BLOCKING pg_advisory_lock here (the first version of this fix, reverted) does not work:
// CREATE INDEX CONCURRENTLY must wait for every OTHER session's open transaction to advance past
// its own snapshot before it can finish, and a session merely blocked waiting to acquire a
// blocking advisory lock still counts as "open" for that purpose -- so the loser sessions block
// the winner's CONCURRENTLY build, which never finishes to release the lock the losers are
// waiting on. Confirmed live in this repo's own tests: pg_advisory_lock (blocking) deadlocks
// under this exact schema shape; pg_try_advisory_lock (non-blocking) does not, because a losing
// caller never sits in an open transaction at all -- it returns immediately.
func ApplyStatements(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgschema: acquire connection: %w", err)
	}
	defer conn.Release()

	lockKey := advisoryLockKey(sql)
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockKey).Scan(&acquired); err != nil {
		return fmt.Errorf("pgschema: try advisory lock: %w", err)
	}
	if !acquired {
		return nil
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockKey) }()

	for _, stmt := range SplitStatements(sql) {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pgschema: apply statement: %w\nstatement: %s", err, stmt)
		}
	}
	return nil
}

// advisoryLockKey derives a stable bigint lock key from sql's own content -- different modules'
// schemas (different sql content) get different keys and apply concurrently with each other;
// concurrent callers applying the SAME module's schema always collide on the same key and
// serialize, which is exactly the property ApplyStatements needs.
func advisoryLockKey(sql string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return int64(h.Sum64()) //nolint:gosec // G115: intentional truncation, only used as an opaque lock key
}
