package backendworker

// catalog_reconcile.go — issue #522 Section C's processCatalogReconcileJobPostgres
// handler, now trace/span-only (#154, per revision note pivot #4): claims a
// catalog_reconcile job and reconciles "trace" by reusing
// filecatalog.Lister.RunOnce's exact existing upsert-live/soft-delete-vanished
// logic against file_catalog, scoped to this job's single tenant instead of
// every tenant on a ticker -- this is filecatalog.Lister's ticker-driven
// reconciliation, job-triggered instead (fully replaces filecatalog.Lister
// once proven; its ticker/construction in backendscheduler.go is left in
// place for now, flagged for a later cleanup task rather than deleted here).
//
// VI/VCNT/cube's own self-healing reconcile logic (against
// blockpack_file_catalog) now lives entirely in blockpack's
// compaction-worker (plan.md Section G.4) -- deleted from here outright, not
// left dormant.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/grafana/tempo/modules/backendscheduler/filecatalog"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

func (w *BackendWorker) processCatalogReconcileJobPostgres(ctx context.Context, job *jobstore.Job) error {
	if job.Tenant == "" {
		return fmt.Errorf("catalog_reconcile job missing tenant")
	}
	var detail jobstore.CatalogReconcileDetail
	if err := json.Unmarshal(job.Detail, &detail); err != nil {
		return fmt.Errorf("catalog_reconcile: unmarshal detail: %w", err)
	}

	switch detail.Subsystem {
	case "trace":
		return w.reconcileTraceFileCatalog(ctx, detail.Tenant)
	default:
		return fmt.Errorf("catalog_reconcile: unknown subsystem %q", detail.Subsystem)
	}
}

// reconcileTraceFileCatalog scopes filecatalog.Lister.RunOnce to exactly
// this job's tenant (its tenants func returns a single-element slice), so
// nothing about Lister's own already-tested upsert/soft-delete logic is
// duplicated or reimplemented here.
func (w *BackendWorker) reconcileTraceFileCatalog(ctx context.Context, tenant string) error {
	if w.pgPool == nil {
		return fmt.Errorf("catalog_reconcile trace: postgres not configured on worker")
	}
	lister := filecatalog.NewLister(w.pgPool, w.store.BlockMetas, func() []string { return []string{tenant} })
	return lister.RunOnce(ctx)
}

