package jobplanner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCatalogReconcileInserter is a pure in-memory catalogReconcileInserter
// for unit tests that need no real Postgres connection.
type fakeCatalogReconcileInserter struct {
	calls []struct{ subsystem, tenant string }
	err   error
}

func (f *fakeCatalogReconcileInserter) InsertCatalogReconcile(_ context.Context, subsystem, tenant string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, struct{ subsystem, tenant string }{subsystem, tenant})
	return nil
}

// TestPlanCatalogReconcilePair_InsertsSubsystemAndTenant pins the
// enumeration-row-to-Insert-call mapping.
func TestPlanCatalogReconcilePair_InsertsSubsystemAndTenant(t *testing.T) {
	inserter := &fakeCatalogReconcileInserter{}

	err := planCatalogReconcilePair(context.Background(), inserter, "trace", "tenant-a")
	require.NoError(t, err)

	require.Len(t, inserter.calls, 1)
	assert.Equal(t, "trace", inserter.calls[0].subsystem)
	assert.Equal(t, "tenant-a", inserter.calls[0].tenant)
}

// TestPlanCatalogReconcilePair_PropagatesInserterError proves a real insert
// failure surfaces to the caller rather than being silently swallowed.
func TestPlanCatalogReconcilePair_PropagatesInserterError(t *testing.T) {
	inserter := &fakeCatalogReconcileInserter{err: errors.New("boom")}
	err := planCatalogReconcilePair(context.Background(), inserter, "trace", "tenant-a")
	require.Error(t, err)
}

// TestCatalogReconcileSubsystemQueries_CoversTraceOnly pins issue #522
// Section C's now trace/span-only subsystem set (#154, per revision note
// pivot #4 -- VI/VCNT/cube moved to blockpack's own compaction-planner) -- a
// query map missing "trace" would silently stop reconciling it forever, with
// no compiler or test failure to catch it.
func TestCatalogReconcileSubsystemQueries_CoversTraceOnly(t *testing.T) {
	query, ok := catalogReconcileSubsystemQueries["trace"]
	require.True(t, ok, "missing tenant-enumeration query for subsystem %q", "trace")
	require.NotEmpty(t, query)
	assert.Len(t, catalogReconcileSubsystemQueries, 1, "vi/vcnt/cube must be removed, not left dormant")
}
