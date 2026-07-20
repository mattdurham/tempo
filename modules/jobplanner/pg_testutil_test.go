package jobplanner

// pg_testutil_test.go -- this package's own copy of the
// newTestPostgresPool/isDockerUnavailable helpers duplicated across
// tempodb/encoding/vblockpack/jobstore, .../migrate, and modules/backendworker
// (unexported test helpers cannot be imported across packages). Applies
// backend_jobs.sql plus viusage_entries/cube_entries/file_catalog/
// blockpack_file_catalog -- planVi/planCube/planCatalogReconcile's own e2e
// tests need all five tables, unlike jobstore's copy, which only needs
// backend_jobs.

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

// fileCatalogSchemaPath is tempo's own file_catalog.sql (NOT
// blockpack_file_catalog, which blockpack.ApplyFileCatalogSchema below
// applies) -- needed by the "trace" subsystem's catalogReconcileSubsystemQueries
// entry.
const fileCatalogSchemaPath = "../../tempodb/encoding/vblockpack/schema/file_catalog.sql"

// newTestPostgresPool starts an ephemeral Postgres container, applies
// backend_jobs.sql plus the viusage/cube/file_catalog/blockpack_file_catalog
// schemas, and returns a connected pool. Skips the calling test (does not
// fail the suite) if Docker is unavailable.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("jobplanner_test"),
		tcpostgres.WithUsername("jobplanner_test"),
		tcpostgres.WithPassword("jobplanner_test"),
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

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to postgres testcontainer: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := migrate.Apply(ctx, pool); err != nil {
		t.Fatalf("applying backend_jobs migration: %v", err)
	}
	if err := blockpack.ApplyViUsageSchema(ctx, pool); err != nil {
		t.Fatalf("applying viusage schema: %v", err)
	}
	if err := blockpack.ApplyCubeSchema(ctx, pool); err != nil {
		t.Fatalf("applying cube schema: %v", err)
	}
	if err := blockpack.ApplyFileCatalogSchema(ctx, pool); err != nil {
		t.Fatalf("applying blockpack_file_catalog schema: %v", err)
	}
	applySchemaFile(ctx, t, pool, fileCatalogSchemaPath)

	return pool
}

// applySchemaFile executes path's SQL statements verbatim against pool, split
// on bare top-level semicolons after stripping `--` line comments first
// (mirrors modules/backendscheduler/filecatalog's identical helper -- that
// package's file_catalog.sql schema file's comments use semicolons as
// ordinary English punctuation, which a naive split-on-";" over the raw file
// text would misread as a statement boundary).
func applySchemaFile(ctx context.Context, t *testing.T, pool *pgxpool.Pool, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading schema file %s: %v", path, err)
	}
	for _, stmt := range strings.Split(stripSQLLineComments(string(data)), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("applying schema statement from %s: %v\nstatement: %s", path, err, stmt)
		}
	}
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

func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, marker := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"is the docker daemon running",
		"no such host",
		"connect: connection refused",
		"context deadline exceeded",
	} {
		if strings.Contains(strings.ToLower(msg), strings.ToLower(marker)) {
			return true
		}
	}
	return false
}
