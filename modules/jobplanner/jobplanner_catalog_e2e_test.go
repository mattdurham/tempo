package jobplanner

// jobplanner_catalog_e2e_test.go — real-Postgres proof that
// catalogReconcileSubsystemQueries's SQL is correct against the real
// file_catalog/viusage_entries/cube_entries/blockpack_file_catalog schemas
// (issue #522), mirroring jobplanner_e2e_test.go's rationale for
// planVi/planCube. catalog_reap's own e2e coverage was removed outright by
// #154 along with the rest of tempo's now-redundant reap mechanism.

import (
	"context"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestPlanCatalogReconcile_EnumeratesOneJobPerRealTraceTenant seeds a trace
// tenant into file_catalog and proves planCatalogReconcile finds it via
// catalogReconcileSubsystemQueries's real SQL, not just the fake-inserter
// mapping plan_catalog_reconcile_test.go already covers. Also seeds
// viusage_entries/cube_entries/blockpack_file_catalog rows for vi/cube/vcnt
// tenants and proves NONE of them produce a catalog_reconcile job (#154,
// per revision note pivot #4: vi/vcnt/cube catalog-sync moved entirely to
// blockpack's own compaction-planner) -- a regression here would mean
// tempo's job-planner silently resumed double-planning reconcile work that
// compaction-planner already owns.
func TestPlanCatalogReconcile_EnumeratesOneJobPerRealTraceTenant(t *testing.T) {
	pool := newTestPostgresPool(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO file_catalog (tenant, block_id, block_ref, start_sec, end_sec)
		VALUES ('trace-tenant', 'block-1', 'trace-tenant/block-1/data.blockpack', 100, 200)`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO viusage_entries (tenant, col_hash, col_type, column_name)
		VALUES ('vi-tenant', 'hash1', 'string', 'span.name')`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO cube_entries (cube_id, tenant)
		VALUES ('cube-1', 'cube-tenant')`)
	require.NoError(t, err)

	fileCatalogStore := blockpack.NewFileCatalogStore(pool)
	require.NoError(t, fileCatalogStore.Insert(ctx, blockpack.FileCatalogRow{
		Subsystem: "vcnt", Tenant: "vcnt-tenant", ResourceID: "colhash-1",
		ObjectKey: "vcnt-tenant/vcnt/colhash-1/l0/a.vcnt", Level: 0, MinSec: 100, MaxSec: 200,
	}))

	s := New(pool, common.JobPlannerConfig{})
	require.NoError(t, s.planCatalogReconcile(ctx))

	for _, tc := range []struct {
		subsystem, tenant string
		wantJob           bool
	}{
		{"trace", "trace-tenant", true},
		{"vi", "vi-tenant", false},
		{"cube", "cube-tenant", false},
		{"vcnt", "vcnt-tenant", false},
	} {
		var count int
		row := pool.QueryRow(
			ctx, `SELECT count(*) FROM backend_jobs WHERE dedup_key = $1`,
			"catalog_reconcile|"+tc.subsystem+"|"+tc.tenant,
		)
		require.NoError(t, row.Scan(&count))
		wantCount := 0
		if tc.wantJob {
			wantCount = 1
		}
		require.Equal(
			t,
			wantCount,
			count,
			"catalog_reconcile job presence for subsystem=%s tenant=%s",
			tc.subsystem,
			tc.tenant,
		)
	}
}
