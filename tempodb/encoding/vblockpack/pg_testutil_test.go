package vblockpack

// pg_testutil_test.go — shared ephemeral-Postgres test infrastructure for this
// package's vi_backfill_catalog_test.go (2026-07-11), and (2026-07-15, issue #504) every cube
// test that now needs a real Postgres-backed cube registry.
//
// newTestPostgresPool starts a real, throwaway PostgreSQL container per test run
// (spun up and torn down via t.Cleanup, never a persistent or shared instance --
// this is exactly the "team's own ephemeral testcontainers-go test infra"
// exception carved out by the standing infrastructure checkpoint, not a
// violation of it) and applies schema/file_catalog.sql verbatim -- this is the test that proves
// that schema file is actually valid, executable SQL, not just reviewed prose. It also applies
// blockpack's own cube, viusage, and blockpack_file_catalog schemas (blockpack.ApplyCubeSchema/
// ApplyViUsageSchema/ApplyFileCatalogSchema) -- both cube's AND viusage's Postgres-backed
// registries moved to blockpack's native implementation (issue #506): cube's move (and its own
// schema/registries.sql cube_entries section removal) happened first (issue #504); viusage's
// production call sites (vi_usage_hook.go/vi_backfill.go) were only swapped over to
// blockpack.NewPgViUsageEntryStore in issue #522's own bootstrap-ordering fix -- tempo's local
// pg_entrystore.go/schema/registries.sql (viusage_entries section) are deleted entirely now,
// this test infra applies blockpack's native schema instead. viusage_query_log is deliberately
// NOT created anywhere anymore (matches blockpack's own schema.sql, which excludes it for the
// same reason: no production code writes to it, tempo's own grep-confirmed zero writers either).

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

	applySchema(ctx, t, pool, "schema/file_catalog.sql")
	if err := blockpack.ApplyCubeSchema(ctx, pool); err != nil {
		t.Fatalf("applying blockpack cube schema: %v", err)
	}
	if err := blockpack.ApplyViUsageSchema(ctx, pool); err != nil {
		t.Fatalf("applying blockpack viusage schema: %v", err)
	}
	// blockpack_file_catalog schema (issue #522): cube_query_path.go's mandatory
	// Phase 3.4 compacted-exclusion filter (pgcatalog.NewStore(q.pgPool).ListCompactedKeys)
	// depends on this table existing, mirroring the same gap fixed in tempodb.go's New().
	if err := blockpack.ApplyFileCatalogSchema(ctx, pool); err != nil {
		t.Fatalf("applying blockpack_file_catalog schema: %v", err)
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

	// cube_entries and viusage_entries are both now created by blockpack's own native schema
	// (blockpack.ApplyCubeSchema/ApplyViUsageSchema, issue #504/#506/#522) -- tempo's local
	// pgCubeEntryStore/pgViUsageEntryStore and their schema/registries.sql definitions were both
	// deleted once their registries moved to blockpack's own native Postgres-backed
	// implementations. viusage_query_log is deliberately excluded, matching blockpack's own
	// schema.sql (no production writer anywhere, tempo-side or blockpack-side).
	var tableCount int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name IN ('viusage_entries', 'file_catalog', 'cube_entries')
	`).Scan(&tableCount)
	if err != nil {
		t.Fatalf("querying information_schema: %v", err)
	}
	if tableCount != 3 {
		t.Fatalf("expected all 3 schema tables to exist after applySchema, found %d", tableCount)
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
