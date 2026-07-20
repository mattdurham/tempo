package jobplanner

// plan_catalog_reconcile.go — issue #522 Section C's catalog-sync mechanism,
// now trace/span-only (#154, per revision note pivot #4): job-planner's own
// poll loop periodically enumerates known tenants from file_catalog's own
// DISTINCT tenant (self-referential, no storage.Store needed) and inserts
// one catalog_reconcile job per tenant. This preserves job-planner's
// "Postgres-only, zero S3 client" design principle completely: it never
// touches object storage itself, only ever asks Postgres "what tenants do I
// already know about," and hands the actual storage-comparison work to
// backend-worker's processCatalogReconcileJobPostgres handler (#145).
//
// VI/VCNT/cube's own catalog-sync now lives entirely in blockpack's
// compaction-planner (plan.md Section G.3) against blockpack_file_catalog --
// deleted from here outright, not left dormant, since compaction-planner
// already ported this same enumeration verbatim.
//
// Runs on its own slower ticker (Config.CatalogPollInterval, default 5m,
// matching filecatalog.Lister's own CatalogListInterval default) rather than
// the main 60s PollInterval -- reconciliation has no tight latency
// requirement (see service.go's Run).

import (
	"context"
	"fmt"
)

// catalogReconcileInserter is the minimal seam planCatalogReconcileSubsystem
// needs -- lets unit tests exercise the enumeration-to-Insert-call mapping
// against a fake, without a real Postgres connection. *jobstore.Store
// satisfies this in production.
type catalogReconcileInserter interface {
	InsertCatalogReconcile(ctx context.Context, subsystem, tenant string) error
}

// catalogReconcileSubsystemQueries maps each subsystem to the query that
// finds every tenant job-planner already knows about for it, entirely from
// tables it already has Postgres access to. Trace/span only (#154) -- VI/
// VCNT/cube's entries moved to blockpack's own compaction-planner.
var catalogReconcileSubsystemQueries = map[string]string{
	"trace": `SELECT DISTINCT tenant FROM file_catalog`,
}

// planCatalogReconcile runs every subsystem's tenant-enumeration query
// against the real database and chain-enqueues one catalog_reconcile job per
// (subsystem, tenant) pair found. A single subsystem's query/insert failure
// is recorded but does not prevent the remaining subsystems in this tick from
// being planned.
func (s *Service) planCatalogReconcile(ctx context.Context) error {
	var firstErr error
	for subsystem, query := range catalogReconcileSubsystemQueries {
		if err := s.planCatalogReconcileSubsystem(ctx, subsystem, query); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *Service) planCatalogReconcileSubsystem(ctx context.Context, subsystem, query string) error {
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("jobplanner: query tenants for subsystem %q: %w", subsystem, err)
	}
	defer rows.Close()

	var firstErr error
	for rows.Next() {
		var tenant string
		if scanErr := rows.Scan(&tenant); scanErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("jobplanner: scan tenant for subsystem %q: %w", subsystem, scanErr)
			}
			continue
		}
		if insErr := planCatalogReconcilePair(ctx, s.jobStore, subsystem, tenant); insErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf(
					"jobplanner: insert catalog_reconcile for subsystem %q tenant %q: %w", subsystem, tenant, insErr,
				)
			}
			continue
		}
		metricCatalogReconcilesPlanned.Inc()
	}
	if rows.Err() != nil {
		return fmt.Errorf("jobplanner: iterate tenants for subsystem %q: %w", subsystem, rows.Err())
	}
	return firstErr
}

// planCatalogReconcilePair enqueues one catalog_reconcile job for
// (subsystem, tenant), factored out from planCatalogReconcileSubsystem so
// unit tests can exercise this single mapping against a fake inserter.
func planCatalogReconcilePair(ctx context.Context, inserter catalogReconcileInserter, subsystem, tenant string) error {
	return inserter.InsertCatalogReconcile(ctx, subsystem, tenant)
}
