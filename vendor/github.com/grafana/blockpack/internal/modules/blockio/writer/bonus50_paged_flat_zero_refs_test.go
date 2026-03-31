package writer_test

// Bonus #50: encodePagedFlatColumn with zero refs — verify no panic and that the
// resulting blob decodes correctly to an empty IntrinsicColumn.
//
// When len(c.refs) == 0, pageCount == 0, the page loop is skipped, and the function
// produces a paged blob with an empty TOC. decodePagedColumnBlob handles zero pages
// by iterating zero times and returning an empty merged column.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// TestEncodePagedFlatColumn_ZeroRefs verifies that encodePagedFlatColumn does not
// panic when the flatAccum has zero refs, and produces a decodable blob.
func TestEncodePagedFlatColumn_ZeroRefs(t *testing.T) {
	c := writer.NewFlatAccumForTest(shared.ColumnTypeUint64)
	// Do not add any values — zero refs.

	blob, err := writer.EncodePagedFlatColumnForTest(c)
	require.NoError(t, err)
	require.NotNil(t, blob, "zero-refs encodePagedFlatColumn must return non-nil blob")
	assert.Greater(t, len(blob), 0, "blob must be non-empty")

	// First byte must be IntrinsicPagedVersion (0x02).
	assert.Equal(t, shared.IntrinsicPagedVersion, blob[0], "blob must start with paged sentinel byte")

	// Decoding must succeed and return an empty column.
	col, err := shared.DecodeIntrinsicColumnBlob(blob)
	require.NoError(t, err)
	require.NotNil(t, col)
	assert.Equal(t, uint32(0), col.Count, "decoded column must have Count==0")
	assert.Empty(t, col.BlockRefs, "decoded column must have no BlockRefs")
}
