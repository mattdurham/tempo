package app

// job_planner.go — issue #518: the job-planner Tempo module target
// (-target=job-planner). A poll loop that chain-enqueues the next
// bounded-window vi_backfill/cube_backfill job for columns/cubes already
// triggered at least once with more history left to backfill. See
// modules/jobplanner/service.go for the poll loop itself; this file is pure
// wiring, modeled on initValueIndexConsumer's enabled-check/hard-fail-on-
// missing-postgres shape (value_index.go).
//
// Deliberately does NOT depend on the Store module (unlike
// initValueIndexCompactor's {Store, Server}): Store transitively constructs
// the full S3/GCS/Azure trace storage stack via tempo_storage.NewStore, which
// job-planner never needs -- it only ever reads/writes Postgres rows, never
// object storage. Module deps are {Server} only (see modules.go).

import (
	"context"
	"errors"
	"fmt"

	"github.com/grafana/dskit/services"

	blockpack "github.com/grafana/blockpack"

	"github.com/grafana/tempo/modules/jobplanner"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/migrate"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/schema"
)

func (t *App) initJobPlanner() (services.Service, error) {
	cfg := t.cfg.StorageConfig.Trace.Block.Blockpack.JobPlanner
	if !cfg.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	// job-planner has no purpose without Postgres -- its entire job is
	// reading/writing Postgres rows (viusage_entries/cube_entries/backend_jobs).
	pgCfg := t.cfg.StorageConfig.Trace.Postgres
	if pgCfg == nil {
		return nil, errors.New("job-planner: postgres is not configured; job-planner has no purpose without it")
	}

	pgPool, err := postgres.NewPool(context.Background(), pgCfg)
	if err != nil {
		return nil, fmt.Errorf("job-planner: create postgres pool: %w", err)
	}

	// job-planner runs as its own standalone module target (-target=job-planner), with its own
	// independent pgPool -- NOT the same pool tempodb.go/backendscheduler.go migrate, so it can't
	// rely on either of those processes having started first against a shared Postgres instance.
	// job-planner reads/writes backend_jobs (jobstore), file_catalog (tenant_redaction_state's
	// schema file), viusage_entries (plan_vi.go), and cube_entries (plan_cube.go) directly, so it
	// must apply every schema itself, idempotently, mirroring backend-scheduler's own posture
	// exactly. blockpack.Postgres.ApplySchemas covers viusage/cube/file_catalog/compaction_jobs/
	// column_manifest in one call.
	if err := migrate.Apply(context.Background(), pgPool); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("job-planner: applying backend_jobs postgres schema: %w", err)
	}
	if err := schema.ApplyFileCatalog(context.Background(), pgPool); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("job-planner: applying file_catalog postgres schema: %w", err)
	}
	if err := blockpack.NewPostgresFromPool(pgPool).ApplySchemas(context.Background()); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("job-planner: applying blockpack postgres schemas: %w", err)
	}

	svc := jobplanner.New(pgPool, cfg)

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(_ error) error {
			pgPool.Close()
			return nil
		},
	), nil
}
