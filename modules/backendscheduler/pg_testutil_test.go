package backendscheduler

// pg_testutil_test.go — this package's own small copy of
// tempodb/encoding/vblockpack/pg_testutil_test.go's newTestPostgresPool
// helper, mirroring the same "duplicated deliberately, not imported"
// convention already established by
// modules/backendscheduler/filecatalog/pg_testutil_test.go. This copy
// applies NO schema at all: BackendScheduler.New's own cfg.Postgres != nil
// path is what's supposed to apply the file_catalog migration via
// schema.ApplyFileCatalog, so the container must start with a genuinely
// empty database for that to be provable.

import (
	"context"
	"strings"
	"testing"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// newTestPostgresDSN starts an ephemeral, schema-empty Postgres container and
// returns its connection DSN. Skips the calling test (does not fail the
// suite) if Docker is unavailable in the environment.
func newTestPostgresDSN(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("backendscheduler_test"),
		tcpostgres.WithUsername("backendscheduler_test"),
		tcpostgres.WithPassword("backendscheduler_test"),
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
