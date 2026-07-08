package vblockpack

import "testing"

// TestCubeTierToLevel_MapsFilenameTierToRollupLevelValue pins cubeTierToLevel's contract: a
// merged filename's tier prefix (0/1/2, from "L0"/"L1"/"L2") maps to the actual
// blockpack.CubeFileInfo.Level value (1/60/1440), not the raw tier digit.
func TestCubeTierToLevel_MapsFilenameTierToRollupLevelValue(t *testing.T) {
	cases := []struct {
		tier      uint64
		wantLevel uint32
		wantOK    bool
	}{
		{0, cubeLevelL0, true},
		{1, cubeLevelL1, true},
		{2, cubeLevelL2, true},
		{3, 0, false},
	}
	for _, tc := range cases {
		gotLevel, gotOK := cubeTierToLevel(tc.tier)
		if gotOK != tc.wantOK || gotLevel != tc.wantLevel {
			t.Fatalf("cubeTierToLevel(%d) = (%d, %v), want (%d, %v)", tc.tier, gotLevel, gotOK, tc.wantLevel, tc.wantOK)
		}
	}
}

// TestCubeTierToLevel_L1TierNeverCollidesWithRollupL0Value (mutation-verification required) pins
// the specific collision that caused the real bug this function fixes: a re-listed L1 rollup
// file's filename tier digit is "1" (from "L1-..."), which numerically equals cubeLevelL0's value
// (1) if passed through unmapped — causing the file to be misclassified as an evictable L0 file
// and, once past retention and covered by its own just-written L1 watermark, deleted outright by
// EvictAgedL0. Mutation-verified: reverting cubeTierToLevel to `return uint32(tier), true` (the
// original buggy behavior) must fail this test.
func TestCubeTierToLevel_L1TierNeverCollidesWithRollupL0Value(t *testing.T) {
	level, ok := cubeTierToLevel(1)
	if !ok {
		t.Fatalf("expected tier 1 to be recognized")
	}
	if level == cubeLevelL0 {
		t.Fatalf("tier 1 (L1 rollup file) must not map to cubeLevelL0's value (%d); got %d", cubeLevelL0, level)
	}
	if level != cubeLevelL1 {
		t.Fatalf("expected tier 1 to map to cubeLevelL1 (%d), got %d", cubeLevelL1, level)
	}
}
