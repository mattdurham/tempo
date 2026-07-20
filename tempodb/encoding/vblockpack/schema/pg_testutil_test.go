package schema

// pg_testutil_test.go — this package's own small copy of
// ../pg_testutil_test.go's newTestPostgresPool helper, mirroring the same
// "duplicated deliberately, not imported" convention already established by
// ../migrate/pg_testutil_test.go and modules/backendscheduler/filecatalog/
// pg_testutil_test.go (unexported test helpers cannot be imported across
// packages). Like ../migrate's own copy, this applies NO schema at all --
// ApplyFileCatalog itself is what this package's tests exercise, so the
// container must start with a genuinely empty database.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// newTestPostgresPool starts an ephemeral, schema-empty Postgres container and
// returns a connected pool. Skips the calling test (does not fail the suite)
// if Docker is unavailable in the environment.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("schema_test"),
		tcpostgres.WithUsername("schema_test"),
		tcpostgres.WithPassword("schema_test"),
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

	return pool
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
