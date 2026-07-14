package backendscheduler

// backend_jobs_migration_test.go — #181 Phase 0: proves BackendScheduler.New's
// existing cfg.Postgres != nil block (backendscheduler.go) applies the
// backend_jobs migration (tempodb/encoding/vblockpack/migrate.Apply) as a
// side effect of construction, with the exact same "warn, don't crash"
// failure posture as the neighboring filecatalog.NewLister construction.

import (
	"context"
	"flag"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/postgres"
)

func TestBackendScheduler_New_AppliesBackendJobsMigration(t *testing.T) {
	dsn := newTestPostgresDSN(t)

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	cfg.LocalWorkPath = t.TempDir()
	cfg.Postgres = &postgres.Config{DSN: dsn}

	ctx, cancel := context.WithCancel(context.Background())
	store, rr, ww := newStore(ctx, t, t.TempDir())
	defer func() {
		cancel()
		store.Shutdown()
	}()

	limits, err := overrides.NewOverrides(overrides.Config{Defaults: overrides.Overrides{}}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	_, err = New(cfg, nil, store, limits, rr, ww)
	require.NoError(t, err)

	verifyPool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer verifyPool.Close()

	var tableExists bool
	err = verifyPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'backend_jobs'
		)
	`).Scan(&tableExists)
	require.NoError(t, err)
	require.True(t, tableExists, "expected backend_jobs table to exist after BackendScheduler construction with cfg.Postgres configured")
}
