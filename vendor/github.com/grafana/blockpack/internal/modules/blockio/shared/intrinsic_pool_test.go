package shared_test

// NOTE-011: Pool correctness tests for AcquireIntrinsicBuf / ReleaseIntrinsicBuf.
// SHARED-24: TestAcquireIntrinsicBuf_NonNil
// SHARED-25: TestReleaseIntrinsicBuf_ResetsLen
// SHARED-26: TestReleaseIntrinsicBuf_CapGuard
// SHARED-27: TestIntrinsicBufPool_DecodeCorrectness
// SHARED-28: TestDecodeFlatPage_BytesAreCopied

import (
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestAcquireIntrinsicBuf_NonNil verifies AcquireIntrinsicBuf returns a non-nil *[]byte.
func TestAcquireIntrinsicBuf_NonNil(t *testing.T) {
	bp := shared.AcquireIntrinsicBuf()
	require.NotNil(t, bp, "AcquireIntrinsicBuf must return non-nil pointer")
	require.NotNil(t, *bp, "dereferenced buffer must be non-nil")
	shared.ReleaseIntrinsicBuf(bp)
}

// TestReleaseIntrinsicBuf_ResetsLen verifies ReleaseIntrinsicBuf resets the length to 0.
func TestReleaseIntrinsicBuf_ResetsLen(t *testing.T) {
	bp := shared.AcquireIntrinsicBuf()
	*bp = append(*bp, 1, 2, 3)
	require.Equal(t, 3, len(*bp))
	shared.ReleaseIntrinsicBuf(bp)

	bp2 := shared.AcquireIntrinsicBuf()
	assert.Equal(t, 0, len(*bp2), "released buffer must have length 0 after reuse")
	shared.ReleaseIntrinsicBuf(bp2)
}

// TestReleaseIntrinsicBuf_CapGuard verifies ReleaseIntrinsicBuf replaces oversized buffers
// (> intrinsicBufMaxCap) with a fresh 64KB buffer instead of returning them to the pool.
func TestReleaseIntrinsicBuf_CapGuard(t *testing.T) {
	bp := shared.AcquireIntrinsicBuf()
	// Simulate an oversized buffer by assigning a large slice.
	oversized := make([]byte, 0, 5*1024*1024) // 5MB > 4MB cap guard
	*bp = oversized
	assert.Greater(t, cap(*bp), 4*1024*1024, "pre-condition: buffer must be oversized")
	shared.ReleaseIntrinsicBuf(bp)

	// After ReleaseIntrinsicBuf the pool discards the oversized backing array and
	// replaces it with a fresh 64KB buffer. Acquire again to verify.
	bp2 := shared.AcquireIntrinsicBuf()
	assert.Equal(t, 0, len(*bp2), "reacquired buffer must have length 0")
	// Cap should be the default (oversized backing array was discarded).
	assert.LessOrEqual(t, cap(*bp2), 4*1024*1024, "reacquired buffer must not retain oversized capacity")
	shared.ReleaseIntrinsicBuf(bp2)
}

// TestIntrinsicBufPool_DecodeCorrectness verifies that using AcquireIntrinsicBuf as a
// snappy decode target produces correct output — the pool buffer mechanics do not corrupt
// decoded data.
func TestIntrinsicBufPool_DecodeCorrectness(t *testing.T) {
	original := []byte("hello snappy pool correctness test data 1234567890")
	compressed := snappy.Encode(nil, original)

	bp := shared.AcquireIntrinsicBuf()
	decoded, err := snappy.Decode(*bp, compressed)
	require.NoError(t, err)
	*bp = decoded

	assert.Equal(t, original, *bp, "decoded data must match original")
	shared.ReleaseIntrinsicBuf(bp)
}

// TestDecodeFlatPage_BytesAreCopied verifies that BytesValues returned by DecodeFlatPage
// are independent copies (not sub-slices of raw), so modifying raw does not corrupt them.
// This is the pool safety invariant: NOTE-011.
func TestDecodeFlatPage_BytesAreCopied(t *testing.T) {
	// Build a minimal flat-bytes page blob manually.
	// Wire format for bytes column:
	//   per row: vLen[2 LE] + value[vLen]
	//   refs: rowCount × (blockW + rowW) bytes (1+1 = 2 bytes each here)
	const rowCount = 2
	const blockW = 1
	const rowW = 1

	payload := []byte{
		// Row 0: "ab" (vLen=2)
		0x02, 0x00, 'a', 'b',
		// Row 1: "cd" (vLen=2)
		0x02, 0x00, 'c', 'd',
		// Refs: (blockIdx=0, rowIdx=0), (blockIdx=0, rowIdx=1)
		0x00, 0x00,
		0x00, 0x01,
	}

	col, err := shared.DecodeFlatPage(payload, blockW, rowW, rowCount, shared.ColumnTypeBytes)
	require.NoError(t, err)
	require.Equal(t, 2, len(col.BytesValues))
	assert.Equal(t, []byte("ab"), col.BytesValues[0])
	assert.Equal(t, []byte("cd"), col.BytesValues[1])

	// Mutate payload to verify BytesValues are independent copies.
	payload[2] = 'Z' // was 'a'
	assert.Equal(t, []byte("ab"), col.BytesValues[0],
		"BytesValues must be a copy — mutation of raw must not affect col.BytesValues")
}
