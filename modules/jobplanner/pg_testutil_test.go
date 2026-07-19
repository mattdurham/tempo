package jobplanner

// pg_testutil_test.go -- this package's own copy of the
// newTestPostgresPool/isDockerUnavailable helpers duplicated across
// tempodb/encoding/vblockpack/jobstore, .../migrate, and modules/backendworker
// (unexported test helpers cannot be imported across packages). Applies
// backend_jobs.sql plus viusage_entries/cube_entries -- planVi/planCube's own
// e2e tests need all three tables, unlike jobstore's copy, which only needs
// backend_jobs.

import (
	"context"
	"strings"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
)

// newTestPostgresPool starts an ephemeral Postgres container, applies
// backend_jobs.sql plus the viusage/cube schemas, and returns a connected
// pool. Skips the calling test (does not fail the suite) if Docker is
// unavailable.
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
