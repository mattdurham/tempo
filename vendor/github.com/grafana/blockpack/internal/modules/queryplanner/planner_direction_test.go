package queryplanner_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// stubIndexer implements BlockIndexer with a fixed block count and no real data.
// Used to test PlanWithOptions direction ordering without needing a real file.
type stubIndexer struct {
	// metas overrides per-block metadata (keyed by block index).
	// Blocks not in the map return a zero BlockMeta.
	metas map[int]shared.BlockMeta
	// tsIndex, when non-nil, is returned by BlocksInTimeRange (simulates TS index present).
	tsIndex    []int
	blockCount int
}

func (s *stubIndexer) BlockCount() int { return s.blockCount }
func (s *stubIndexer) BlockMeta(blockIdx int) shared.BlockMeta {
	if s.metas != nil {
		return s.metas[blockIdx]
	}
	return shared.BlockMeta{}
}

func (s *stubIndexer) ReadBlocks(blockIndices []int) (map[int][]byte, error) {
	result := make(map[int][]byte, len(blockIndices))
	for _, idx := range blockIndices {
		result[idx] = nil
	}
	return result, nil
}
func (s *stubIndexer) RangeColumnType(_ string) (shared.ColumnType, bool) { return 0, false }
func (s *stubIndexer) BlocksForRange(_ string, _ shared.RangeValueKey) ([]int, error) {
	return nil, nil
}

func (s *stubIndexer) BlocksForRangeInterval(_ string, _, _ shared.RangeValueKey) ([]int, error) {
	return nil, nil
}

// BlocksInTimeRange returns the pre-configured tsIndex slice (nil = no TS index).
func (s *stubIndexer) BlocksInTimeRange(_, _ uint64) []int             { return s.tsIndex }
func (s *stubIndexer) ColumnSketch(_ string) queryplanner.ColumnSketch { return nil }

func TestPlanWithOptions_ForwardOrder(t *testing.T) {
	t.Parallel()
	stub := &stubIndexer{blockCount: 5}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{Direction: queryplanner.Forward})
	require.Equal(t, []int{0, 1, 2, 3, 4}, plan.SelectedBlocks)
	require.Equal(t, queryplanner.Forward, plan.Direction)
	require.Equal(t, 0, plan.Limit)
}

func TestPlanWithOptions_BackwardOrder(t *testing.T) {
	t.Parallel()
	stub := &stubIndexer{blockCount: 5}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(
		nil,
		queryplanner.TimeRange{},
		queryplanner.PlanOptions{Direction: queryplanner.Backward, Limit: 3},
	)
	require.Equal(t, []int{4, 3, 2, 1, 0}, plan.SelectedBlocks)
	require.Equal(t, queryplanner.Backward, plan.Direction)
	require.Equal(t, 3, plan.Limit)
}

func TestPlanWithOptions_BackwardSingleBlock(t *testing.T) {
	t.Parallel()
	stub := &stubIndexer{blockCount: 1}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{Direction: queryplanner.Backward})
	require.Equal(t, []int{0}, plan.SelectedBlocks)
}

func TestPlanWithOptions_BackwardEmpty(t *testing.T) {
	t.Parallel()
	stub := &stubIndexer{blockCount: 0}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{Direction: queryplanner.Backward})
	require.Empty(t, plan.SelectedBlocks)
	require.Equal(t, queryplanner.Backward, plan.Direction)
}

// TestPlanWithOptions_ForwardByTimestamp verifies that when blocks have real MinStart
// timestamps, SelectedBlocks is sorted oldest-first (ascending MinStart).
func TestPlanWithOptions_ForwardByTimestamp(t *testing.T) {
	t.Parallel()
	// Blocks written in reverse time order: block 0 is newest, block 2 is oldest.
	stub := &stubIndexer{
		blockCount: 3,
		metas: map[int]shared.BlockMeta{
			0: {MinStart: 300},
			1: {MinStart: 100},
			2: {MinStart: 200},
		},
	}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{Direction: queryplanner.Forward})
	// Expect oldest-first: block 1 (100), block 2 (200), block 0 (300).
	require.Equal(t, []int{1, 2, 0}, plan.SelectedBlocks)
}

// TestPlanWithOptions_BackwardByTimestamp verifies newest-first (descending MinStart).
func TestPlanWithOptions_BackwardByTimestamp(t *testing.T) {
	t.Parallel()
	stub := &stubIndexer{
		blockCount: 3,
		metas: map[int]shared.BlockMeta{
			0: {MinStart: 300},
			1: {MinStart: 100},
			2: {MinStart: 200},
		},
	}
	p := queryplanner.NewPlanner(stub)
	plan := p.PlanWithOptions(nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{Direction: queryplanner.Backward})
	// Expect newest-first: block 0 (300), block 2 (200), block 1 (100).
	require.Equal(t, []int{0, 2, 1}, plan.SelectedBlocks)
}

// TestPlan_TSIndexPruning verifies that BlocksInTimeRange is used for time pruning
// when the TS index is present (non-nil return), and that blocks outside the range
// are counted in PrunedByTime.
func TestPlan_TSIndexPruning(t *testing.T) {
	t.Parallel()
	// TS index reports only blocks 1 and 3 overlap the query window.
	stub := &stubIndexer{
		blockCount: 5,
		tsIndex:    []int{1, 3}, // blocks 0, 2, 4 are outside the time range
	}
	p := queryplanner.NewPlanner(stub)
	plan := p.Plan(nil, queryplanner.TimeRange{MinNano: 100, MaxNano: 200})
	require.Equal(t, []int{1, 3}, plan.SelectedBlocks)
	require.Equal(t, 3, plan.PrunedByTime) // blocks 0, 2, 4 pruned
}

// TestPlan_TSIndexAbsentFallback verifies that when BlocksInTimeRange returns nil
// (no TS index), the planner falls back to BlockMeta scan.
func TestPlan_TSIndexAbsentFallback(t *testing.T) {
	t.Parallel()
	// tsIndex is nil → fallback path. Block 1 has unknown time (always kept).
	// Block 0 is outside the range; block 2 overlaps.
	stub := &stubIndexer{
		blockCount: 3,
		tsIndex:    nil, // no TS index → triggers fallback
		metas: map[int]shared.BlockMeta{
			0: {MinStart: 500, MaxStart: 600}, // outside [100, 200]
			1: {MinStart: 0, MaxStart: 0},     // unknown time → always kept
			2: {MinStart: 100, MaxStart: 150}, // overlaps
		},
	}
	p := queryplanner.NewPlanner(stub)
	plan := p.Plan(nil, queryplanner.TimeRange{MinNano: 100, MaxNano: 200})
	require.Equal(t, []int{1, 2}, plan.SelectedBlocks)
	require.Equal(t, 1, plan.PrunedByTime) // block 0 pruned
}
