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

	"github.com/grafana/tempo/modules/jobplanner"
	"github.com/grafana/tempo/modules/postgres"
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

	svc := jobplanner.New(pgPool, cfg)

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(_ error) error {
			pgPool.Close()
			return nil
		},
	), nil
}
