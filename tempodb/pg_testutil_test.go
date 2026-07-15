package tempodb

// pg_testutil_test.go — ephemeral-Postgres helper for this package's own tests
// (2026-07-15, issue #504 follow-up). New() now calls blockpack.ApplyCubeSchema
// against cfg.Postgres whenever it's configured, so any test that sets
// cfg.Postgres needs a real, connectable pool -- a *postgres.Config with an
// empty/unreachable DSN (previously safe, since pgxpool.New doesn't connect
// eagerly) now fails New() outright once schema application tries to actually
// use the connection. Mirrors tempodb/encoding/vblockpack/pg_testutil_test.go's
// pattern; skips (not fails) when Docker is unavailable.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func newTestPostgresPool(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("tempodb_test"),
		tcpostgres.WithUsername("tempodb_test"),
		tcpostgres.WithPassword("tempodb_test"),
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
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parsing postgres connection string: %v", err)
	}
	return dsn
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
