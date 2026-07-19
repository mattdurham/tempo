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

// fakeViInserter is a pure in-memory viBackfillInserter for unit tests that
// need no real Postgres connection.
type fakeViInserter struct {
	calls []jobstore.ViBackfillDetail
	err   error
}

func (f *fakeViInserter) InsertViBackfill(_ context.Context, _ string, d jobstore.ViBackfillDetail) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, d)
	return nil
}

// TestPlanViRow_InsertsWithConfiguredWindow pins the query-row-to-Insert-call
// mapping: the resulting ViBackfillDetail must carry the row's column
// identity plus cfg.ViWindowSeconds, not zero/unbounded.
func TestPlanViRow_InsertsWithConfiguredWindow(t *testing.T) {
	inserter := &fakeViInserter{}
	cfg := common.JobPlannerConfig{ViWindowSeconds: 21600}
	row := viUsageRow{Tenant: "tenant-a", ColHash: "hash1", ColType: "string", ColumnName: "span.custom.attr"}

	err := planViRow(context.Background(), inserter, cfg, row)
	require.NoError(t, err)

	require.Len(t, inserter.calls, 1)
	assert.Equal(t, jobstore.ViBackfillDetail{
		ColumnHash: "hash1", ColumnName: "span.custom.attr", ColumnType: "string", WindowSeconds: 21600,
	}, inserter.calls[0])
}

// TestPlanViRow_PropagatesInserterError proves a real insert failure surfaces
// to the caller rather than being silently swallowed.
func TestPlanViRow_PropagatesInserterError(t *testing.T) {
	inserter := &fakeViInserter{err: errors.New("boom")}
	err := planViRow(context.Background(), inserter, common.JobPlannerConfig{}, viUsageRow{Tenant: "t"})
	require.Error(t, err)
}
