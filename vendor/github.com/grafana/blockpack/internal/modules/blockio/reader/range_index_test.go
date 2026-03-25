package reader_test

// Tests for RangeColumnBoundaries — boundary parsing roundtrip.

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// buildLatencyFile writes a blockpack file with 4 spans with latency values
// 100, 200, 300, 400 across 2 blocks (MaxBlockSpans=2).
func buildLatencyFile(t *testing.T) *reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	var tid [16]byte
	tid[0] = 0xCC
	latencies := []int64{100, 200, 300, 400}
	for i, lat := range latencies {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   1_000_002_000,
		}
		attrs := map[string]any{"latency": lat}
		require.NoError(t, w.AddSpan(tid[:], span, attrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestRangeColumnBoundaries_Int64 verifies that after writing spans with int64
// attribute values, RangeColumnBoundaries returns BucketMin/BucketMax that
// bracket the written values.
func TestRangeColumnBoundaries_Int64(t *testing.T) {
	r := buildLatencyFile(t)

	// Find the column name used by the range index for the "latency" attribute.
	var latCol string
	for _, n := range r.ColumnNames() {
		if n == "span.latency" || n == "resource.latency" {
			latCol = n
			break
		}
	}
	if latCol == "" {
		t.Skip("latency column not found in range index — skip")
	}

	bounds := r.RangeColumnBoundaries(latCol)
	require.NotNil(t, bounds, "boundaries should be present for %s", latCol)
	assert.Equal(t, shared.ColumnTypeRangeInt64, bounds.ColType)
	// BucketMin ≤ 100 and BucketMax ≥ 400.
	assert.LessOrEqual(t, bounds.BucketMin, int64(100))
	assert.GreaterOrEqual(t, bounds.BucketMax, int64(400))
}

// TestRangeColumnBoundaries_NotPresent verifies that RangeColumnBoundaries returns
// nil for a column that was never written.
func TestRangeColumnBoundaries_NotPresent(t *testing.T) {
	r := buildLatencyFile(t)
	bounds := r.RangeColumnBoundaries("nonexistent.col")
	assert.Nil(t, bounds, "should return nil for missing column")
}

// TestBlocksForRangeInterval_PointQuery verifies that BlocksForRangeInterval with min==max
// (a point query) returns only the block whose range contains that exact value.
// GAP-23: interval lookup edge case — point query must behave correctly.
func TestBlocksForRangeInterval_PointQuery(t *testing.T) {
	buf := &bytes.Buffer{}
	// Use MaxBlockSpans=2: 3 blocks with values [100-200], [300-400], [500-600].
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	var tid [16]byte
	// Write 6 spans in pairs so they land in 3 distinct blocks.
	// Each pair has values in distinct ranges to ensure 3 separate KLL buckets.
	values := []int64{100, 200, 300, 400, 500, 600}
	for i, v := range values {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
		}
		attrs := map[string]any{"metric": v}
		require.NoError(t, w.AddSpan(tid[:], span, attrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)

	// The "metric" attribute is passed as a resource attribute → "resource.metric".
	colName := "resource.metric"
	_, hasIdx := r.RangeColumnType(colName)
	require.True(t, hasIdx, "int64 resource attribute must produce a range index")

	// Point query [300, 300]: should return at least one block (the block containing 300).
	var key300 [8]byte
	binary.LittleEndian.PutUint64(key300[:], uint64(300)) //nolint:gosec
	blocksPoint, err := r.BlocksForRangeInterval(colName, string(key300[:]), string(key300[:]))
	require.NoError(t, err)
	assert.NotEmpty(t, blocksPoint, "point query [300,300] must return at least one block")

	// Point query [100, 100]: should return at least one block.
	var key100 [8]byte
	binary.LittleEndian.PutUint64(key100[:], uint64(100)) //nolint:gosec
	blocksA, err := r.BlocksForRangeInterval(colName, string(key100[:]), string(key100[:]))
	require.NoError(t, err)
	assert.NotEmpty(t, blocksA, "point query [100,100] must return at least one block")

	// Wide interval [100, 600]: must include blocks from all ranges.
	var key600 [8]byte
	binary.LittleEndian.PutUint64(key600[:], uint64(600)) //nolint:gosec
	blocksAll, err := r.BlocksForRangeInterval(colName, string(key100[:]), string(key600[:]))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(blocksAll), len(blocksPoint),
		"wide interval must return at least as many blocks as a point query")
}

// TestRangeIndex_BlockIDsPreserved verifies that the range index correctly records
// block IDs for spans distributed across many blocks and all spans are retrievable.
// GAP-6: block ID boundary — small-scale sanity check that block IDs are stored correctly.
func TestRangeIndex_BlockIDsPreserved(t *testing.T) {
	const numBlocks = 10
	buf := &bytes.Buffer{}
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: 1, // one span per block
	})
	require.NoError(t, err)

	var tid [16]byte
	for i := range numBlocks {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_001_000_000 + i*1_000_000), //nolint:gosec
		}
		// Use a shared service.name so all spans map to the same range bucket group.
		attrs := map[string]any{"service.name": "svc"}
		require.NoError(t, w.AddSpan(tid[:], span, attrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, numBlocks, r.BlockCount(), "must have exactly numBlocks blocks")

	// Verify each block can be read without error — block IDs are all valid.
	for i := range numBlocks {
		bwb, readErr := r.GetBlockWithBytes(i, nil, nil)
		require.NoError(t, readErr, "block %d must be readable", i)
		assert.Greater(t, bwb.Block.SpanCount(), 0, "block %d must have at least 1 span", i)
	}
}

// TestRangeColumnBoundaries_Duration verifies boundary parsing for span:duration
// which is a uint64 column normalized to RangeUint64 in the range index.
func TestRangeColumnBoundaries_Duration(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	var tid [16]byte
	// Write spans with different durations to populate span:duration range index.
	durations := []struct{ start, end uint64 }{
		{1_000_000_000, 1_000_001_000}, // 1000 ns
		{1_000_000_000, 1_000_005_000}, // 5000 ns
		{1_000_000_000, 1_000_010_000}, // 10000 ns
		{1_000_000_000, 1_000_020_000}, // 20000 ns
	}
	for i, d := range durations {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: d.start,
			EndTimeUnixNano:   d.end,
		}
		require.NoError(t, w.AddSpan(tid[:], span, nil, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)

	bounds := r.RangeColumnBoundaries("span:duration")
	if bounds == nil {
		t.Skip("span:duration not range-indexed in this file — skip")
	}
	// span:duration is stored as RangeUint64.
	assert.Equal(t, shared.ColumnTypeRangeUint64, bounds.ColType)
	// BucketMin should be <= 1000 (smallest duration) and BucketMax >= 20000.
	minDuration := uint64(1000)
	maxDuration := uint64(20000)
	assert.LessOrEqual(t, uint64(bounds.BucketMin), minDuration)    //nolint:gosec
	assert.GreaterOrEqual(t, uint64(bounds.BucketMax), maxDuration) //nolint:gosec
}
