package vblockpack

// pg_testutil_test.go — shared ephemeral-Postgres test infrastructure for this
// package's pg_entrystore_test.go and vi_backfill_catalog_test.go (2026-07-11), and
// (2026-07-15, issue #504) every cube test that now needs a real Postgres-backed
// cube registry.
//
// newTestPostgresPool starts a real, throwaway PostgreSQL container per test run
// (spun up and torn down via t.Cleanup, never a persistent or shared instance --
// this is exactly the "team's own ephemeral testcontainers-go test infra"
// exception carved out by the standing infrastructure checkpoint, not a
// violation of it) and applies schema/registries.sql + schema/file_catalog.sql
// verbatim -- this is the test that proves those schema files are actually
// valid, executable SQL, not just reviewed prose. It also applies blockpack's OWN
// cube schema (blockpack.ApplyCubeSchema) -- cube's Postgres-backed registry moved
// to blockpack's native implementation (issue #506), which owns the cube_entries
// table definition now; schema/registries.sql's own cube_entries section was
// removed (issue #504) once tempo's local pgCubeEntryStore was deleted, so this is
// the only remaining source of that table for tests in this package.

import (
	"context"
	"os"
	"strings"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// newTestPostgresPool starts an ephemeral Postgres container, applies this
// package's schema files, and returns a connected pool. Skips the calling test
// (does not fail the suite) if Docker is unavailable in the environment.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("viusage_test"),
		tcpostgres.WithUsername("viusage_test"),
		tcpostgres.WithPassword("viusage_test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable in this environment, skipping Postgres-backed test: %v", err)
		}
		t.Fatalf("starting postgres testcontainer: %v", err)
	}
	t.Cleanup(func() {
		if termErr := container.Terminate(context.Background()); termErr != nil {
			t.Logf("terminating postgres testcontainer: %v", termErr)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting postgres connection string: %v", err)
	}

	// MaxConns is set well above pgxpool's small default (4, or NumCPU if higher)
	// -- this test infra backs concurrency tests (e.g. the required
	// ConcurrentTriggersConvergeOnOneWinner regression guard) whose whole point
	// is genuine concurrent transactions; a too-small pool would silently
	// serialize most "concurrent" calls through pool-exhaustion queuing,
	// masking exactly the race condition those tests exist to catch.
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing postgres connection string: %v", err)
	}
	poolCfg.MaxConns = 50

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("connecting to postgres testcontainer: %v", err)
	}
	t.Cleanup(pool.Close)

	applySchema(ctx, t, pool, "schema/registries.sql")
	applySchema(ctx, t, pool, "schema/file_catalog.sql")
	if err := blockpack.ApplyCubeSchema(ctx, pool); err != nil {
		t.Fatalf("applying blockpack cube schema: %v", err)
	}

	return pool
}

// applySchema executes path's SQL statements verbatim against pool, via the
// shared migrate.ApplyStatements helper (2026-07-14) -- the comment-strip/
// semicolon-split logic used to live here as a private copy, but is now
// shared with tempodb/encoding/vblockpack/migrate's own Apply(), which needs
// the identical behavior for backend_jobs.sql. The comment-strip step is
// load-bearing, not cosmetic: this package's schema files' comments use
// semicolons as ordinary English punctuation (e.g. "...as two distinct
// things; with the repeated-use ring removed..."), which a naive split-on-";"
// over the raw file text would misread as a statement boundary, slicing a
// CREATE TABLE statement in half mid-comment.
func applySchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading schema file %s: %v", path, err)
	}
	if err := migrate.ApplyStatements(ctx, pool, string(data)); err != nil {
		t.Fatalf("applying schema from %s: %v", path, err)
	}
}

// TestNewTestPostgresPool_SmokeTest is the "does the environment even support
// this" check (plan.md Part 5.2 step 1): confirms a container actually starts
// and both schema files apply without error, before any real EntryStore logic
// depends on this helper.
func TestNewTestPostgresPool_SmokeTest(t *testing.T) {
	pool := newTestPostgresPool(t)

	// cube_entries is now created by blockpack.ApplyCubeSchema (issue #504/#506), not by
	// this package's own schema/registries.sql -- tempo's local pgCubeEntryStore/cube_entries
	// definition was deleted once cube moved to blockpack's own native Postgres-backed
	// registry (blockpack.NewPgCubeRegistry, which owns this table's schema entirely now).
	// newTestPostgresPool applies both, so it's still expected to exist here.
	var tableCount int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name IN ('viusage_entries', 'viusage_query_log', 'file_catalog', 'cube_entries')
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	if tableCount != 4 {
		t.Fatalf("expected all 4 schema tables to exist after applySchema, found %d", tableCount)
	}
}

// isDockerUnavailable reports whether err looks like "no Docker daemon
// reachable" rather than some other, real container-startup failure --
// matching the standard testcontainers-go pattern of skipping (not failing)
// the suite in a sandbox without Docker.
func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, marker := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"is the docker daemon running",
		"no such host",
		"connect: connection refused",
		"context deadline exceeded", // Docker socket entirely unreachable
	} {
		if strings.Contains(strings.ToLower(msg), strings.ToLower(marker)) {
			return true
		}
	}
	return false
}
