package backendworker

// viusage_schema_bootstrap_test.go — regression guard for issue #522's viusage_entries
// bootstrap-ordering fix, backend-worker's own wiring point (New(), backendworker.go).
// Mirrors tempodb/viusage_schema_bootstrap_test.go's exact shape: a genuinely fresh Postgres
// testcontainer with NO separate schema-apply step, constructed via the real production path
// (New(), not a test helper that pre-applies schema), proving New() alone leaves
// blockpack.NewPgViUsageEntryStore(w.pgPool) -- the exact construction
// NewViBackfillDepsWithPgRegistry (vi_backfill.go) uses against this same pool -- usable
// immediately.

import (
	"context"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/postgres"
)

func TestNew_AppliesViUsageSchema_PgViUsageEntryStoreUsableImmediately(t *testing.T) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("backendworker_viusage_bootstrap_test"),
		tcpostgres.WithUsername("backendworker_viusage_bootstrap_test"),
		tcpostgres.WithPassword("backendworker_viusage_bootstrap_test"),
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
	require.NoError(t, err)

	limitCfg := overridesConfigForTest(t)
	workerCfg, schedulerClientCfg, overridesSvc, _, store := setupDependencies(ctx, t, limitCfg)
	workerCfg.Postgres = &postgres.Config{DSN: dsn}

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, nil)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NotNil(t, w.pgPool, "New() must have configured pgPool given a non-nil cfg.Postgres")

	entryStore := blockpack.NewPgViUsageEntryStore(w.pgPool)
	entry, err := entryStore.UpsertEntry(ctx, "tenant-a", "col-hash-a", "string",
		func() blockpack.Entry {
			return blockpack.Entry{Tenant: "tenant-a", ColumnHash: "col-hash-a", ColumnType: "string", ColumnName: "span.name"}
		},
		func(_ *blockpack.Entry) error { return nil },
	)
	require.NoError(t, err, "viusage_entries must already exist after New() alone, with no separate schema-apply step")
	require.Equal(t, "span.name", entry.ColumnName)
}
