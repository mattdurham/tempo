package jobstore

// pg_testutil_test.go -- this package's own copy of
// ../pg_testutil_test.go's newTestPostgresPool helper (unexported test
// helpers cannot be imported across packages, mirrors
// ../migrate/pg_testutil_test.go's own duplication for the same reason).
// Applies backend_jobs.sql via migrate.Apply -- NOT registries.sql/
// file_catalog.sql, which this package has no dependency on.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// newTestPostgresPool starts an ephemeral Postgres container, applies
// backend_jobs.sql via migrate.Apply, and returns a connected pool. Skips the
// calling test (does not fail the suite) if Docker is unavailable.
func newTestPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("jobstore_test"),
		tcpostgres.WithUsername("jobstore_test"),
		tcpostgres.WithPassword("jobstore_test"),
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

	// MaxConns is set well above pgxpool's small default -- this test infra
	// backs the required concurrent-claim regression guard, whose whole point
	// is genuine concurrent transactions; a too-small pool would silently
	// serialize "concurrent" calls through pool-exhaustion queuing, masking
	// exactly the race condition that test exists to catch. Mirrors
	// ../pg_testutil_test.go's MaxConns=50 convention exactly.
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

	if err := migrate.Apply(ctx, pool); err != nil {
		t.Fatalf("applying backend_jobs migration: %v", err)
	}

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
