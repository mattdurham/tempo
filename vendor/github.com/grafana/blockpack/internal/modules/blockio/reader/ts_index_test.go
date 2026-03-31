package reader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlocksInTimeRange_NilEntries(t *testing.T) {
	t.Parallel()
	r := &Reader{}
	got := r.BlocksInTimeRange(100, 200)
	require.Nil(t, got)
}

func TestBlocksInTimeRange_AllMatch(t *testing.T) {
	t.Parallel()
	r := &Reader{tsEntries: []tsIndexEntry{
		{minTS: 100, maxTS: 200, blockID: 0},
		{minTS: 300, maxTS: 400, blockID: 1},
		{minTS: 500, maxTS: 600, blockID: 2},
	}}
	got := r.BlocksInTimeRange(50, 650)
	require.Equal(t, []int{0, 1, 2}, got)
}

func TestBlocksInTimeRange_PartialMatch(t *testing.T) {
	t.Parallel()
	r := &Reader{tsEntries: []tsIndexEntry{
		{minTS: 100, maxTS: 200, blockID: 0},
		{minTS: 300, maxTS: 400, blockID: 1},
		{minTS: 500, maxTS: 600, blockID: 2},
	}}
	// [250,450]: block 0 ends at 200 < 250 (no overlap); block 2 starts at 500 > 450 (no overlap).
	got := r.BlocksInTimeRange(250, 450)
	require.Equal(t, []int{1}, got)
}

func TestBlocksInTimeRange_NoneMatch(t *testing.T) {
	t.Parallel()
	r := &Reader{tsEntries: []tsIndexEntry{
		{minTS: 100, maxTS: 200, blockID: 0},
		{minTS: 300, maxTS: 400, blockID: 1},
	}}
	got := r.BlocksInTimeRange(500, 600)
	require.Empty(t, got)
}

func TestBlocksInTimeRange_ZeroTimeBlockAlwaysIncluded(t *testing.T) {
	t.Parallel()
	r := &Reader{tsEntries: []tsIndexEntry{
		{minTS: 0, maxTS: 0, blockID: 3}, // unknown time
		{minTS: 100, maxTS: 200, blockID: 0},
	}}
	// Block 0 overlaps [150, 300]; block 3 is always included.
	got := r.BlocksInTimeRange(150, 300)
	require.ElementsMatch(t, []int{0, 3}, got)
}

func TestBlocksInTimeRange_ExactBoundary(t *testing.T) {
	t.Parallel()
	r := &Reader{tsEntries: []tsIndexEntry{
		{minTS: 100, maxTS: 200, blockID: 0},
		{minTS: 300, maxTS: 400, blockID: 1},
	}}
	// Query window ends exactly at block 1 minTS.
	got := r.BlocksInTimeRange(200, 300)
	// Block 0: maxTS=200 >= 200 and minTS=100 <= 300 → overlaps.
	// Block 1: maxTS=400 >= 200 and minTS=300 <= 300 → overlaps.
	require.Equal(t, []int{0, 1}, got)
}
