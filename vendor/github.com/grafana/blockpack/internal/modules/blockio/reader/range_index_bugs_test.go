package reader_test

// Tests for BUG-07: compareRangeKey float64 NaN handling.

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// encodeFloat64Key encodes a float64 as an 8-byte little-endian string key,
// mirroring the private encodeFloat64Key in range_index.go.
func encodeFloat64Key(v float64) string {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	return string(buf[:])
}

// TestCompareRangeKey_Float64_NaN verifies that compareRangeKey handles NaN
// inputs without returning 0 (equal), which would corrupt binary search.
// BUG-07: IEEE 754 NaN comparisons all return false, so the old manual < / >
// branches both failed, causing the function to fall through to "return 0".
func TestCompareRangeKey_Float64_NaN(t *testing.T) {
	nanKey := encodeFloat64Key(math.NaN())
	normalKey := encodeFloat64Key(1.0)
	negInfKey := encodeFloat64Key(math.Inf(-1))

	t.Run("NaN vs normal: consistent non-zero", func(t *testing.T) {
		result := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, nanKey, normalKey)
		assert.NotEqual(t, 0, result,
			"compareRangeKey(NaN, 1.0) must not return 0 — would corrupt binary search")
	})

	t.Run("normal vs NaN: consistent non-zero", func(t *testing.T) {
		result := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, normalKey, nanKey)
		assert.NotEqual(t, 0, result,
			"compareRangeKey(1.0, NaN) must not return 0 — would corrupt binary search")
	})

	t.Run("NaN vs NaN: stable (equal to itself)", func(t *testing.T) {
		result := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, nanKey, nanKey)
		assert.Equal(t, 0, result,
			"compareRangeKey(NaN, NaN) should be 0 (stable total order)")
	})

	t.Run("NaN vs NegInf: NaN sorts below NegInf per cmp.Compare", func(t *testing.T) {
		result := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, nanKey, negInfKey)
		assert.Less(t, result, 0,
			"cmp.Compare treats NaN as less than any non-NaN, so NaN < -Inf expected")
	})

	t.Run("antisymmetry: sign flips when args reversed", func(t *testing.T) {
		ab := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, nanKey, normalKey)
		ba := reader.CompareRangeKeyExported(shared.ColumnTypeRangeFloat64, normalKey, nanKey)
		assert.True(t, (ab < 0 && ba > 0) || (ab > 0 && ba < 0),
			"compareRangeKey must be antisymmetric: sign(a,b) == -sign(b,a)")
	})
}

// TestDecodeFloat64Key_ShortKey_ReturnsNaN verifies BUG-08: decodeFloat64Key must
// return math.NaN() for short keys, not 0.0 (which is a valid float64 value).
// Returning 0 silently treats malformed data as a valid zero-value, corrupting
// binary search results in compareRangeKey.
func TestDecodeFloat64Key_ShortKey_ReturnsNaN(t *testing.T) {
	got := reader.DecodeFloat64KeyExported("")
	assert.True(t, math.IsNaN(got),
		"decodeFloat64Key(\"\") must return NaN, got %v", got)

	got = reader.DecodeFloat64KeyExported("short")
	assert.True(t, math.IsNaN(got),
		"decodeFloat64Key(\"short\") must return NaN, got %v", got)
}

// TestDecodeInt64Key_ShortKey_ReturnsSentinel verifies BUG-08: decodeInt64Key must
// return math.MinInt64 (not 0) for short keys, so malformed keys sort below all
// valid values rather than silently appearing as zero.
func TestDecodeInt64Key_ShortKey_ReturnsSentinel(t *testing.T) {
	got := reader.DecodeInt64KeyExported("")
	assert.Equal(t, int64(math.MinInt64), got,
		"decodeInt64Key(\"\") must return math.MinInt64 sentinel")

	got = reader.DecodeInt64KeyExported("short")
	assert.Equal(t, int64(math.MinInt64), got,
		"decodeInt64Key(\"short\") must return math.MinInt64 sentinel")
}

// TestDecodeUint64Key_ShortKey_ReturnsSentinel verifies BUG-08: decodeUint64Key
// already returns 0 for short keys, which is the correct minimum sentinel for
// uint64. This test confirms the existing behavior is correct and intentional.
func TestDecodeUint64Key_ShortKey_ReturnsSentinel(t *testing.T) {
	got := reader.DecodeUint64KeyExported("")
	assert.Equal(t, uint64(0), got,
		"decodeUint64Key(\"\") must return 0 (minimum uint64 sentinel)")

	got = reader.DecodeUint64KeyExported("short")
	assert.Equal(t, uint64(0), got,
		"decodeUint64Key(\"short\") must return 0 (minimum uint64 sentinel)")
}
