package jobplanner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
)

// fakeCubeInserter is a pure in-memory cubeBackfillInserter for unit tests
// that need no real Postgres connection.
type fakeCubeInserter struct {
	calls []jobstore.CubeBackfillDetail
	err   error
}

func (f *fakeCubeInserter) InsertCubeBackfill(_ context.Context, _ string, d jobstore.CubeBackfillDetail) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, d)
	return nil
}

// TestPlanCubeRow_InsertsWithConfiguredWindow pins the query-row-to-Insert-call
// mapping: the resulting CubeBackfillDetail must carry the cube identity plus
// cfg.CubeWindowMinutes, not the dead math.MaxUint32 literal Correction 1 found.
func TestPlanCubeRow_InsertsWithConfiguredWindow(t *testing.T) {
	inserter := &fakeCubeInserter{}
	cfg := common.JobPlannerConfig{CubeWindowMinutes: 1440}

	err := planCubeRow(context.Background(), inserter, cfg, "tenant-a", "cube-1")
	require.NoError(t, err)

	require.Len(t, inserter.calls, 1)
	assert.Equal(t, jobstore.CubeBackfillDetail{CubeID: "cube-1", WindowMinutes: 1440}, inserter.calls[0])
}

// TestPlanCubeRow_PropagatesInserterError proves a real insert failure
// surfaces to the caller rather than being silently swallowed.
func TestPlanCubeRow_PropagatesInserterError(t *testing.T) {
	inserter := &fakeCubeInserter{err: errors.New("boom")}
	err := planCubeRow(context.Background(), inserter, common.JobPlannerConfig{}, "tenant-a", "cube-1")
	require.Error(t, err)
}
