package filecatalog

// pg_testutil_test.go — this package's own small copy of
// tempodb/encoding/vblockpack/pg_testutil_test.go's newTestPostgresPool
// helper. Duplicated deliberately, not imported: Go test helpers are private
// to their own package's test binary (an unexported _test.go symbol cannot be
// imported across packages at all, cycle or not), and this package sits below
// tempodb/encoding/vblockpack in the module's layering, so a real (non-test)
// dependency in either direction would be the wrong shape anyway. This copy
// only needs the file_catalog.sql schema (not registries.sql -- this package
// never touches viusage_entries/cube_entries).

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const fileCatalogSchemaPath = "../../../tempodb/encoding/vblockpack/schema/file_catalog.sql"

// newTestPostgresPool starts an ephemeral Postgres container, applies
// file_catalog.sql, and returns a connected pool. Skips the calling test if
// Docker is unavailable in the environment.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("filecatalog_test"),
		tcpostgres.WithUsername("filecatalog_test"),
		tcpostgres.WithPassword("filecatalog_test"),
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

	applySchema(ctx, t, pool, fileCatalogSchemaPath)

	return pool
}

// applySchema executes path's SQL statements verbatim against pool, split on
// bare top-level semicolons after stripping `--` line comments first (this
// package's schema file's comments use semicolons as ordinary English
// punctuation, which a naive split-on-";" over the raw file text would
// misread as a statement boundary).
func applySchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool, path string) {
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
