package app

// job_planner_schema_bootstrap_test.go — regression guard for issue #522's schema-bootstrap-
// ordering fix, job-planner's own wiring point (initJobPlanner, job_planner.go). Mirrors
// tempodb/viusage_schema_bootstrap_test.go's exact shape: a genuinely fresh Postgres
// testcontainer with NO separate schema-apply step, constructed via the real production path
// (initJobPlanner, not a test helper that pre-applies schema), proving initJobPlanner's own
// five Apply calls actually leave every table it depends on usable immediately -- this was
// previously ZERO test coverage of any kind (no test anywhere constructed *App and called
// initJobPlanner).

import (
	"context"
	"strings"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func isDockerUnavailableForJobPlannerTest(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"cannot connect to the docker daemon",
		"docker daemon",
		"is the docker daemon running",
		"no such host",
		"connect: connection refused",
		"context deadline exceeded",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func TestInitJobPlanner_AppliesAllSchemas_UsableImmediately(t *testing.T) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("job_planner_schema_bootstrap_test"),
		tcpostgres.WithUsername("job_planner_schema_bootstrap_test"),
		tcpostgres.WithPassword("job_planner_schema_bootstrap_test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailableForJobPlannerTest(err) {
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

	a := &App{cfg: Config{}}
	a.cfg.StorageConfig.Trace.Postgres = &postgres.Config{DSN: dsn}
	a.cfg.StorageConfig.Trace.Block = &common.BlockConfig{}
	a.cfg.StorageConfig.Trace.Block.Blockpack.JobPlanner = common.JobPlannerConfig{Enabled: true}

	svc, err := a.initJobPlanner()
	require.NoError(t, err, "initJobPlanner must apply every schema it depends on without error")
	require.NotNil(t, svc)

	// initJobPlanner's own schema-apply calls run synchronously before it ever returns the
	// IdleService -- reopening a brand new pool against the SAME dsn and exercising every table
	// it wired proves the schema really landed in Postgres, not merely that no error was
	// returned from a call this test can't otherwise observe.
	verifyPool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(verifyPool.Close)

	viStore := blockpack.NewPgViUsageEntryStore(verifyPool)
	_, err = viStore.UpsertEntry(ctx, "tenant-a", "col-hash-a", "string",
		func() blockpack.Entry {
			return blockpack.Entry{Tenant: "tenant-a", ColumnHash: "col-hash-a", ColumnType: "string", ColumnName: "span.name"}
		},
		func(_ *blockpack.Entry) error { return nil },
	)
	require.NoError(t, err, "viusage_entries must already exist after initJobPlanner alone")

	var backendJobsCount, fileCatalogCount, cubeEntriesCount, blockpackFileCatalogCount int
	require.NoError(t, verifyPool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'backend_jobs'`,
	).Scan(&backendJobsCount))
	require.Equal(t, 1, backendJobsCount, "backend_jobs must exist after initJobPlanner alone")

	require.NoError(t, verifyPool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'file_catalog'`,
	).Scan(&fileCatalogCount))
	require.Equal(t, 1, fileCatalogCount, "file_catalog must exist after initJobPlanner alone")

	require.NoError(t, verifyPool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'cube_entries'`,
	).Scan(&cubeEntriesCount))
	require.Equal(t, 1, cubeEntriesCount, "cube_entries must exist after initJobPlanner alone")

	require.NoError(t, verifyPool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'blockpack_file_catalog'`,
	).Scan(&blockpackFileCatalogCount))
	require.Equal(t, 1, blockpackFileCatalogCount, "blockpack_file_catalog must exist after initJobPlanner alone")
}
