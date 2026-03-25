package writer

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestFindBucket_Generic_Int64 exercises the consolidated generic findBucket
// function (DUP-01 refactor) with int64 boundaries, covering the common cases:
// value in the first bucket, last bucket, a middle bucket, and edge values.
func TestFindBucket_Generic_Int64(t *testing.T) {
	t.Parallel()

	// bounds: sentinel-low, 10, 20, 30 — three buckets: [lo,10), [10,20), [20,30)
	bounds := []int64{0, 10, 20, 30}

	cases := []struct {
		v    int64
		want uint16
	}{
		{v: 0, want: 0},  // first bucket
		{v: 5, want: 0},  // first bucket
		{v: 10, want: 1}, // second bucket (bounds[2]=20 > 10, lo stops at 2, idx=1)
		{v: 15, want: 1}, // second bucket
		{v: 20, want: 2}, // third bucket
		{v: 25, want: 2}, // third bucket
		{v: 30, want: 2}, // clamped to last bucket (idx >= len-1)
		{v: 99, want: 2}, // beyond max, clamped
	}

	for _, tc := range cases {
		got := findBucket(tc.v, bounds)
		if got != tc.want {
			t.Errorf("findBucket(%d, %v) = %d, want %d", tc.v, bounds, got, tc.want)
		}
	}
}

// TestTryApplyExactValues_Uint64_HighBit verifies BUG-02: boundaries for
// ColumnTypeRangeUint64 must be sorted in unsigned order.
// Values straddling 2^63 (0x7FFF... and 0x8000...) were previously sorted
// as int64, making the high-bit value appear negative and therefore first,
// inverting the order and breaking binary-search block pruning.
func TestTryApplyExactValues_Uint64_HighBit(t *testing.T) {
	t.Parallel()

	const lo = uint64(math.MaxInt64)     // 0x7FFFFFFFFFFFFFFF
	const hi = uint64(math.MaxInt64) + 1 // 0x8000000000000000

	encKey := func(v uint64) string {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		return string(b[:])
	}

	loKey := encKey(lo)
	hiKey := encKey(hi)

	cd := &rangeColumnData{
		colType: shared.ColumnTypeRangeUint64,
		blocks: []blockRange{
			{minKey: loKey, maxKey: loKey, blockID: 1},
			{minKey: hiKey, maxKey: hiKey, blockID: 2},
		},
	}

	ok := tryApplyExactValues(cd)
	if !ok {
		t.Fatal("tryApplyExactValues returned false; expected exact-value path to succeed")
	}

	if len(cd.boundaries) != 2 {
		t.Fatalf("expected 2 boundaries, got %d", len(cd.boundaries))
	}

	// Reinterpret stored int64 bits as uint64 — the wire format stores bits,
	// not signed magnitudes. Unsigned order must be preserved.
	b0 := uint64(cd.boundaries[0]) //nolint:gosec
	b1 := uint64(cd.boundaries[1]) //nolint:gosec
	if b0 >= b1 {
		t.Errorf("boundaries out of unsigned order: boundaries[0]=0x%016X >= boundaries[1]=0x%016X (BUG-02)",
			b0, b1)
	}
	if b0 != lo {
		t.Errorf("boundaries[0] = 0x%016X, want 0x%016X", b0, lo)
	}
	if b1 != hi {
		t.Errorf("boundaries[1] = 0x%016X, want 0x%016X", b1, hi)
	}
}
