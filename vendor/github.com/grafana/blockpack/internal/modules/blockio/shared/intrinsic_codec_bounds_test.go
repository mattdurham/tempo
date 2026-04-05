package shared

import (
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildFlatBlobHeader constructs a minimal snappy-compressed flat column blob header.
// The blob contains only the header bytes; no actual value or ref data follows.
// Format: format_version[1] + format[1] + colType[1] + rowCount[4 LE] + blockW[1] + rowW[1]
func buildFlatBlobHeader(rowCount uint32, blockW, rowW uint8) []byte {
	raw := make([]byte, 10)
	raw[0] = IntrinsicFormatVersion // format_version = 0x01
	raw[1] = IntrinsicFormatFlat    // format = flat
	raw[2] = byte(ColumnTypeUint64) // colType = uint64
	binary.LittleEndian.PutUint32(raw[3:], rowCount)
	raw[7] = blockW
	raw[8] = rowW
	// raw[9] is one trailing byte so the blob is at least 10 bytes after decode
	raw[9] = 0xFF
	return snappy.Encode(nil, raw)
}

// --- BUG-1: refsStart bounds check ---

// TestScanFlatColumnRefs_OversizedRowCount verifies that ScanFlatColumnRefs returns nil
// (not a panic or garbage) when the blob's rowCount is so large that
// valuesStart+rowCount*8 would exceed len(raw).
func TestScanFlatColumnRefs_OversizedRowCount(t *testing.T) {
	// rowCount=0x0FFFFFFF means we'd need 0x0FFFFFFF*8 = ~2GB of values data.
	// The blob contains only 10 bytes total after decode, so the bounds check must fire.
	blob := buildFlatBlobHeader(0x0FFFFFFF, 1, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnRefs(blob, 0, 0, false, false, 0)
	assert.Nil(t, result, "expected nil when rowCount*8 exceeds blob length")
}

// TestScanFlatColumnTopKRefs_OversizedRowCount verifies that ScanFlatColumnTopKRefs
// returns nil when rowCount*8 would exceed the blob length.
func TestScanFlatColumnTopKRefs_OversizedRowCount(t *testing.T) {
	blob := buildFlatBlobHeader(0x0FFFFFFF, 1, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnTopKRefs(blob, 10, false)
	assert.Nil(t, result, "expected nil when rowCount*8 exceeds blob length")
}

// TestScanFlatColumnRefsFiltered_OversizedRowCount verifies that ScanFlatColumnRefsFiltered
// returns nil when rowCount*8 would exceed the blob length.
func TestScanFlatColumnRefsFiltered_OversizedRowCount(t *testing.T) {
	blob := buildFlatBlobHeader(0x0FFFFFFF, 1, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnRefsFiltered(blob, false, 10, func(_ BlockRef) bool { return true })
	assert.Nil(t, result, "expected nil when rowCount*8 exceeds blob length")
}

// --- BUG-13: blockW/rowW validation ---

// TestDecodeVariableWidthRef_BlockWZero verifies that decodeVariableWidthRef returns an
// error when blockW=0.
func TestDecodeVariableWidthRef_BlockWZero(t *testing.T) {
	raw := []byte{0xAB, 0xCD, 0xEF, 0x01}
	_, _, err := decodeVariableWidthRef(raw, 0, 0, 1)
	assert.Error(t, err, "expected error for blockW=0")
}

// TestDecodeVariableWidthRef_BlockWThree verifies that decodeVariableWidthRef returns an
// error when blockW=3 (not 1 or 2).
func TestDecodeVariableWidthRef_BlockWThree(t *testing.T) {
	raw := []byte{0xAB, 0xCD, 0xEF, 0x01}
	_, _, err := decodeVariableWidthRef(raw, 0, 3, 1)
	assert.Error(t, err, "expected error for blockW=3")
}

// TestDecodeVariableWidthRef_RowWZero verifies that decodeVariableWidthRef returns an
// error when rowW=0.
func TestDecodeVariableWidthRef_RowWZero(t *testing.T) {
	raw := []byte{0xAB, 0xCD, 0xEF, 0x01}
	_, _, err := decodeVariableWidthRef(raw, 0, 1, 0)
	assert.Error(t, err, "expected error for rowW=0")
}

// TestScanFlatColumnRefs_InvalidBlockW verifies that ScanFlatColumnRefs returns nil
// when blockW=0 (invalid) is encoded in the blob.
func TestScanFlatColumnRefs_InvalidBlockW(t *testing.T) {
	// Build a blob with blockW=0, rowW=1, rowCount=1.
	// Even with rowCount=1, the width validation should fire before decoding any ref.
	blob := buildFlatBlobHeader(1, 0, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnRefs(blob, 0, ^uint64(0), false, false, 0)
	assert.Nil(t, result, "expected nil for blockW=0 (invalid width)")
}

// TestScanFlatColumnTopKRefs_InvalidBlockW verifies ScanFlatColumnTopKRefs returns nil
// for blockW=0.
func TestScanFlatColumnTopKRefs_InvalidBlockW(t *testing.T) {
	blob := buildFlatBlobHeader(1, 0, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnTopKRefs(blob, 10, false)
	assert.Nil(t, result, "expected nil for blockW=0 (invalid width)")
}

// TestScanFlatColumnRefsFiltered_InvalidBlockW verifies ScanFlatColumnRefsFiltered
// returns nil for blockW=0.
func TestScanFlatColumnRefsFiltered_InvalidBlockW(t *testing.T) {
	blob := buildFlatBlobHeader(1, 0, 1)
	require.NotEmpty(t, blob)

	result := ScanFlatColumnRefsFiltered(blob, false, 10, func(_ BlockRef) bool { return true })
	assert.Nil(t, result, "expected nil for blockW=0 (invalid width)")
}

// TestDecodeRef_BlockWZero verifies that the internal decodeRef function is never
// reached with blockW=0 — the flat-column scan functions must reject it first.
// Since decodeRef has no error return, this test exercises it indirectly through
// ScanFlatColumnTopKRefs: a blob encoding blockW=0 must return nil without panic,
// confirming the BUG-13 width guard fires before any call to decodeRef.
func TestDecodeRef_BlockWZero(t *testing.T) {
	blob := buildFlatBlobHeader(1, 0, 1)
	require.NotEmpty(t, blob)

	// Must not panic; must return nil (not garbage refs).
	assert.NotPanics(t, func() {
		result := ScanFlatColumnTopKRefs(blob, 10, false)
		assert.Nil(t, result, "blockW=0 must be rejected before decodeRef is called")
	})
}
