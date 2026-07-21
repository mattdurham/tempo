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
	"path"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
)

// newTestReaderWriter constructs a real *readerWriter (via New(), the same
// production factory every caller uses) over a local backend, optionally with
// a real Postgres pool wired in (cfg.Postgres) -- shared by any test in this
// package that needs a genuine *readerWriter rather than a fake.
func newTestReaderWriter(t *testing.T, version string, withPostgres bool) *readerWriter {
	t.Helper()
	tempDir := t.TempDir()
	cfg := &Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: path.Join(tempDir, "traces")},
		Block: &common.BlockConfig{
			BloomFP:             .01,
			BloomShardSizeBytes: 100_000,
			Version:             version,
		},
		WAL:           &wal.Config{Filepath: path.Join(tempDir, "wal")},
		BlocklistPoll: 0,
		Search: &SearchConfig{
			ChunkSizeBytes:  1_000_000,
			ReadBufferCount: 8, ReadBufferSizeBytes: 4 * 1024 * 1024,
		},
	}
	if withPostgres {
		cfg.Postgres = &postgres.Config{DSN: newTestPostgresPool(t)}
	}

	r, _, _, err := New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)
	rw, ok := r.(*readerWriter)
	require.True(t, ok, "New() must return a *readerWriter under the Reader interface")
	return rw
}

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
