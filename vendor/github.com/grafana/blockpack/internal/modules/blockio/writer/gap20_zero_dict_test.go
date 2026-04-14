package writer_test

// GAP-20: Zero-entry dict column — verify that encodeDictColumn handles a dictAccum
// with zero entries without panicking, and that the resulting blob decodes correctly
// to an empty IntrinsicColumn.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// TestGAP20_ZeroEntryDictColumn_EncodeDecodeRoundTrip verifies that encodeDictColumn
// on a zero-entry dictAccum produces a non-nil blob that decodes to an empty column.
func TestGAP20_ZeroEntryDictColumn_EncodeDecodeRoundTrip(t *testing.T) {
	accum := writer.NewDictAccumForTest(shared.ColumnTypeString)

	blob, err := writer.EncodeDictColumnForTest(accum)
	require.NoError(t, err)
	require.NotNil(t, blob, "zero-entry encodeDictColumn must return non-nil blob")
	assert.Greater(t, len(blob), 0, "blob must be non-empty (snappy header present)")

	col, err := shared.DecodeIntrinsicColumnBlob(blob)
	require.NoError(t, err)
	require.NotNil(t, col)
	assert.Equal(t, uint32(0), col.Count, "decoded column must have Count==0")
	assert.Empty(t, col.BlockRefs, "decoded column must have no BlockRefs")
}

// TestGAP20_ZeroEntryDictColumn_ComputeMinMax verifies that computeMinMax returns
// empty strings for a column with no entries (guards against index-out-of-bounds).
func TestGAP20_ZeroEntryDictColumn_ComputeMinMax(t *testing.T) {
	a := writer.NewIntrinsicAccumulatorForTest()
	// Don't feed anything — no column exists, computeMinMax must return ("", "").
	minVal, maxVal := a.ComputeMinMaxForTest("span:name")
	assert.Equal(t, "", minVal)
	assert.Equal(t, "", maxVal)
}

// TestGAP20_ZeroEntryDictColumn_OverCap verifies that overCap returns false for
// an empty accumulator (zero entries in all columns).
func TestGAP20_ZeroEntryDictColumn_OverCap(t *testing.T) {
	a := writer.NewIntrinsicAccumulatorForTest()
	assert.False(t, a.OverCapForTest(), "empty accumulator must not be over cap")
}
