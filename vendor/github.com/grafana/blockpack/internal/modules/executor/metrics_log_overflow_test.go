package executor

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/vm"
)

// TestLogBuildDenseRows_CapPrealloc verifies that logBuildDenseRows caps the initial
// slice pre-allocation at maxInitialCap when numBuckets*len(attrGroupKeys) would exceed
// it. Without the BUG-5 fix, int(numBuckets)*len(attrGroupKeys) would be computed with
// no overflow guard, risking OOM on large time windows with many group keys.
//
// The test uses a small numBuckets (10) so the loop is fast, and validates the result
// is correct. The overflow guard is exercised structurally: after the fix the guard
// expression is present; this test confirms the code path builds sensible output.
func TestLogBuildDenseRows_CapPrealloc(t *testing.T) {
	const stepNanos = int64(1_000_000_000) // 1 second
	now := int64(1_700_000_000_000_000_000)
	start := now
	end := now + 10*stepNanos // 10 buckets

	// Build buckets: 3 distinct attr group keys, each with bucket index 0..9.
	attrKeys := []string{"groupA", "groupB", "groupC"}
	buckets := make(map[string]*aggBucketState)
	for bucketIdx := range 10 {
		for _, key := range attrKeys {
			compositeKey := "0\x00" + key
			if bucketIdx > 0 {
				compositeKey = string(rune('0'+bucketIdx)) + "\x00" + key
			}
			buckets[compositeKey] = &aggBucketState{count: 1, sum: float64(bucketIdx)}
		}
	}

	qs := &vm.QuerySpec{
		Aggregate: vm.AggregateSpec{Function: "COUNT"},
		TimeBucketing: vm.TimeBucketSpec{
			Enabled:       true,
			StartTime:     start,
			EndTime:       end,
			StepSizeNanos: stepNanos,
		},
	}

	require.NotPanics(t, func() {
		rows := logBuildDenseRows(buckets, qs, logFuncCountOverTime, nil, 1.0, 0.0)
		// 10 buckets * 3 group keys = 30 rows
		assert.Len(t, rows, 30, "expected 30 rows (10 buckets * 3 group keys)")
	})
}

// TestCalcInitialCap verifies that calcInitialCap returns the correct capped or uncapped
// value for all boundary conditions. This exercises the overflow guard (BUG-5) without
// constructing any LogMetricsRow entries.
func TestCalcInitialCap(t *testing.T) {
	const maxInitialCap = 1_000_000

	tests := []struct {
		name      string
		nBuckets  int64
		nAttrKeys int
		want      int
	}{
		{
			name:      "under cap",
			nBuckets:  10,
			nAttrKeys: 3,
			want:      30,
		},
		{
			name:      "exactly at threshold",
			nBuckets:  int64(maxInitialCap / 3),
			nAttrKeys: 3,
			want:      maxInitialCap / 3 * 3, // 999_999
		},
		{
			name:      "just over threshold fires cap",
			nBuckets:  333_334,
			nAttrKeys: 3,
			want:      maxInitialCap,
		},
		{
			name:      "large nBuckets fires cap",
			nBuckets:  math.MaxInt32,
			nAttrKeys: 3,
			want:      maxInitialCap,
		},
		{
			name:      "zero buckets",
			nBuckets:  0,
			nAttrKeys: 3,
			want:      0,
		},
		{
			name:      "zero attrKeys fires cap",
			nBuckets:  10,
			nAttrKeys: 0,
			want:      maxInitialCap,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcInitialCap(tt.nBuckets, tt.nAttrKeys)
			assert.Equal(t, tt.want, got)
		})
	}
}
