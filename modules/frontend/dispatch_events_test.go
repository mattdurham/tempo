package frontend

// dispatch_events_test.go — TDD coverage for issue #493's sampleAdvancementEvents (R2(c)'s exact
// cap-and-sample algorithm), tested in isolation before any real dispatch fixture: the sampling
// algorithm itself has no dependency on real data.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSampleAdvancementEvents_UnderCap_ReturnsAllUnchanged(t *testing.T) {
	points := make([]advancementPoint, 5)
	for i := range points {
		points[i] = advancementPoint{jobs: i, completedThroughSeconds: uint32(i)} //nolint:gosec // test data
	}
	got := sampleAdvancementEvents(points, 20)
	assert.Equal(t, points, got)
}

// TestSampleAdvancementEvents_OverCap_KeepsFirstAndShouldQuitAndEvenlyStridesRemainder is R2(c)'s
// own exact test requirement: a 37-point fixture with a ShouldQuit trigger at index 22, capped at
// 20 -- assert result length == 20, index 0 kept, the ShouldQuit-marked point survives, and the
// remaining 18 are evenly strided across the other 35 points (not clustered at the start).
func TestSampleAdvancementEvents_OverCap_KeepsFirstAndShouldQuitAndEvenlyStridesRemainder(t *testing.T) {
	const n = 37
	const shouldQuitAt = 22
	const capN = 20

	points := make([]advancementPoint, n)
	for i := range points {
		points[i] = advancementPoint{jobs: i, completedThroughSeconds: uint32(i)} //nolint:gosec // test data
	}
	points[shouldQuitAt].isShouldQuitTrigger = true

	got := sampleAdvancementEvents(points, capN)
	require.Len(t, got, capN)

	assert.Equal(t, points[0], got[0], "the first genuine advancement point must always survive sampling")

	foundShouldQuit := false
	for _, p := range got {
		if p.isShouldQuitTrigger {
			foundShouldQuit = true
			assert.Equal(t, uint32(shouldQuitAt), p.completedThroughSeconds)
		}
	}
	assert.True(t, foundShouldQuit, "the ShouldQuit-trigger point must survive sampling regardless of its position")

	// Not clustered at the start: the LAST kept point's index (by completedThroughSeconds, which
	// mirrors each point's original index in this fixture) must be well past the cap's own size --
	// a naive "keep the first 20" truncation would top out at index 19.
	last := got[len(got)-1]
	assert.Greater(t, last.completedThroughSeconds, uint32(capN-1),
		"sampling must reach late-query advancement points, not bias toward the first N")

	// Every kept point's index must be strictly increasing (chronological order preserved).
	for i := 1; i < len(got); i++ {
		assert.Greater(t, got[i].completedThroughSeconds, got[i-1].completedThroughSeconds)
	}
}

func TestSampleAdvancementEvents_OverCap_NoShouldQuitTrigger(t *testing.T) {
	const n = 30
	const capN = 20
	points := make([]advancementPoint, n)
	for i := range points {
		points[i] = advancementPoint{jobs: i, completedThroughSeconds: uint32(i)} //nolint:gosec // test data
	}

	got := sampleAdvancementEvents(points, capN)
	require.Len(t, got, capN)
	assert.Equal(t, points[0], got[0])
	for _, p := range got {
		assert.False(t, p.isShouldQuitTrigger)
	}
}

func TestSampleAdvancementEvents_ZeroOrNegativeCap_ReturnsInputUnchanged(t *testing.T) {
	points := []advancementPoint{{jobs: 1}, {jobs: 2}}
	assert.Equal(t, points, sampleAdvancementEvents(points, 0))
	assert.Equal(t, points, sampleAdvancementEvents(points, -1))
}

func TestSampleAdvancementEvents_EmptyInput(t *testing.T) {
	assert.Empty(t, sampleAdvancementEvents(nil, 20))
}

// TestSampleAdvancementEvents_LargeScale_ReachesTrueTail is the go-presubmit MEDIUM finding's own
// regression guard: at a 1000-point scale (the empirically-verified size at which the OLD
// `step := len(candidates)/remaining` truncating-stride formula left the LAST ~52 of 999
// candidate points -- roughly a 10-shard-wide blind spot at defaultMostRecentShards=200 --
// permanently unreachable), assert the sampled result actually reaches the TRUE final index
// (len(points)-1), not just "something greater than capN-1" (TestSampleAdvancementEvents_
// OverCap_KeepsFirstAndShouldQuitAndEvenlyStridesRemainder's own 37-point fixture is too small
// for that weaker bound to distinguish a 1-index gap from a genuine ~52-index dead zone -- this
// test exists specifically because that one couldn't catch the bug at the scale where it
// actually mattered in production). No ShouldQuit trigger in this fixture -- the interesting late
// point here is reached purely by the even-sampling stride itself, proving the fix does not rely
// on the ShouldQuit special-case to reach the tail.
func TestSampleAdvancementEvents_LargeScale_ReachesTrueTail(t *testing.T) {
	const n = 1000
	const capN = 20

	points := make([]advancementPoint, n)
	for i := range points {
		points[i] = advancementPoint{jobs: i, completedThroughSeconds: uint32(i)} //nolint:gosec // test data
	}

	got := sampleAdvancementEvents(points, capN)
	require.Len(t, got, capN)

	last := got[len(got)-1]
	assert.Equal(t, uint32(n-1), last.completedThroughSeconds,
		"the true final advancement point (index %d) must be reachable -- the old truncating "+
			"stride formula left roughly the last 52 of 999 candidates permanently unreachable at "+
			"this scale, which this exact assertion would have caught", n-1)

	// Every kept point's index must be strictly increasing (chronological order preserved) and
	// the whole result must stay within the cap.
	for i := 1; i < len(got); i++ {
		assert.Greater(t, got[i].completedThroughSeconds, got[i-1].completedThroughSeconds)
	}
}
