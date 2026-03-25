package reader_test

// Tests for synthesizeSpanEnd (intrinsic_reader.go).
//
// synthesizeSpanEnd builds a span:end intrinsic column by joining span:start and
// span:duration on BlockRef. If a span has start but no duration (or vice versa),
// it is silently skipped. These tests exercise that partial-join path, plus the
// happy path to verify the synthesis is correct.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// writeFileWithSpans writes a blockpack file with the given spans and returns
// a Reader over the result.
func writeFileWithSpans(t *testing.T, spans []spanSpec) *reader.Reader {
	t.Helper()
	var buf bytes.Buffer
	cfg := writer.Config{OutputStream: &buf}
	w, err := writer.NewWriterWithConfig(cfg)
	require.NoError(t, err)

	for i, s := range spans {
		tid := [16]byte{byte(i + 1)}
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0},
			Name:              s.name,
			StartTimeUnixNano: s.startNano,
			EndTimeUnixNano:   s.endNano,
		}
		err = w.AddSpan(tid[:], span, map[string]any{"service.name": "svc"}, "", nil, "")
		require.NoError(t, err)
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

type spanSpec struct {
	name      string
	startNano uint64
	endNano   uint64
}

// TestSynthesizeSpanEnd_HappyPath verifies that span:end is correctly synthesized
// from span:start + span:duration (EndTimeUnixNano - StartTimeUnixNano).
func TestSynthesizeSpanEnd_HappyPath(t *testing.T) {
	start := uint64(1_000_000_000)
	end := uint64(2_000_000_000)
	r := writeFileWithSpans(t, []spanSpec{
		{name: "op", startNano: start, endNano: end},
	})

	require.True(t, r.HasIntrinsicSection(), "file must have intrinsic section")

	// span:end must not be stored directly — it is synthesized.
	_, storedDirectly := r.IntrinsicColumnMeta("span:end")
	assert.False(t, storedDirectly, "span:end must not be stored directly in intrinsic section")

	endCol, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	require.NotNil(t, endCol, "synthesized span:end must be non-nil")
	assert.Equal(t, "span:end", endCol.Name)
	require.Greater(t, len(endCol.Uint64Values), 0, "must have at least one end value")

	// The synthesized end value must equal start + duration.
	// Duration = EndTimeUnixNano - StartTimeUnixNano.
	wantEnd := end // EndTimeUnixNano is stored as span:start + span:duration
	assert.Equal(t, wantEnd, endCol.Uint64Values[0])
}

// TestSynthesizeSpanEnd_Caching verifies that the synthesized span:end column is
// returned from cache on subsequent calls (no re-computation).
func TestSynthesizeSpanEnd_Caching(t *testing.T) {
	r := writeFileWithSpans(t, []spanSpec{
		{name: "op", startNano: 100, endNano: 200},
	})

	col1, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	require.NotNil(t, col1)

	col2, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	// Same pointer — returned from cache.
	assert.Same(t, col1, col2, "second call must return cached pointer")
}

// TestSynthesizeSpanEnd_NoIntrinsicSection verifies that GetIntrinsicColumn returns
// (nil, nil) for a column name that does not exist in the intrinsic index.
func TestSynthesizeSpanEnd_NoIntrinsicSection(t *testing.T) {
	r := writeFileWithSpans(t, []spanSpec{
		{name: "op", startNano: 100, endNano: 200},
	})

	col, err := r.GetIntrinsicColumn("nonexistent:column")
	assert.NoError(t, err)
	assert.Nil(t, col, "missing column must return nil, nil")
}

// makeTestReader builds a minimal Reader whose intrinsicIndex and intrinsicDecoded
// are populated synthetically, bypassing all file I/O. This allows injecting
// mismatched or partial IntrinsicColumn data to exercise synthesizeSpanEnd paths.
func makeTestReader(t *testing.T, cols map[string]*shared.IntrinsicColumn) *reader.Reader {
	t.Helper()
	// Write a throwaway file just to get a valid Reader structure.
	var buf bytes.Buffer
	cfg := writer.Config{OutputStream: &buf}
	w, err := writer.NewWriterWithConfig(cfg)
	require.NoError(t, err)
	tid := [16]byte{0xAA}
	span := &tracev1.Span{
		TraceId:           tid[:],
		SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Name:              "base",
		StartTimeUnixNano: 1_000,
		EndTimeUnixNano:   2_000,
	}
	require.NoError(t, w.AddSpan(tid[:], span, map[string]any{"service.name": "svc"}, "", nil, ""))
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := reader.NewReaderFromProvider(&memProvider{data: buf.Bytes()})
	require.NoError(t, err)

	// Build a synthetic intrinsic index so HasIntrinsicSection() == true and
	// GetIntrinsicColumn resolves against our injected decoded cache.
	index := make(map[string]shared.IntrinsicColMeta, len(cols))
	for name := range cols {
		index[name] = shared.IntrinsicColMeta{Name: name}
	}
	reader.SetIntrinsicIndexForTest(r, index)
	reader.SetIntrinsicDecodedForTest(r, cols)
	return r
}

// TestSynthesizeSpanEnd_MissingDuration verifies that synthesizeSpanEnd returns
// (nil, nil) when span:duration is absent from the intrinsic section.
// This exercises the early-exit path at intrinsic_reader.go:172-174.
func TestSynthesizeSpanEnd_MissingDuration(t *testing.T) {
	startCol := &shared.IntrinsicColumn{
		Name:         "span:start",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        1,
		Uint64Values: []uint64{1_000_000_000},
		BlockRefs:    []shared.BlockRef{{BlockIdx: 0, RowIdx: 0}},
	}
	// Only inject span:start — span:duration is absent.
	r := makeTestReader(t, map[string]*shared.IntrinsicColumn{
		"span:start": startCol,
	})

	endCol, err := r.GetIntrinsicColumn("span:end")
	assert.NoError(t, err)
	assert.Nil(t, endCol, "synthesizeSpanEnd must return nil when span:duration is missing")
}

// TestSynthesizeSpanEnd_MissingStart verifies that synthesizeSpanEnd returns
// (nil, nil) when span:start is absent from the intrinsic section.
// This exercises the early-exit path at intrinsic_reader.go:168-170.
func TestSynthesizeSpanEnd_MissingStart(t *testing.T) {
	durCol := &shared.IntrinsicColumn{
		Name:         "span:duration",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        1,
		Uint64Values: []uint64{500_000_000},
		BlockRefs:    []shared.BlockRef{{BlockIdx: 0, RowIdx: 0}},
	}
	// Only inject span:duration — span:start is absent.
	r := makeTestReader(t, map[string]*shared.IntrinsicColumn{
		"span:duration": durCol,
	})

	endCol, err := r.GetIntrinsicColumn("span:end")
	assert.NoError(t, err)
	assert.Nil(t, endCol, "synthesizeSpanEnd must return nil when span:start is missing")
}

// TestSynthesizeSpanEnd_PartialJoin verifies that spans with a start BlockRef that
// has no matching duration BlockRef are silently skipped, while spans with both
// start and duration are correctly synthesized.
// This exercises the partial-join skip path at intrinsic_reader.go:200-203.
func TestSynthesizeSpanEnd_PartialJoin(t *testing.T) {
	// Two spans:
	//   Span A: BlockRef{0,0} — has both start (1s) and duration (0.5s) → end = 1.5s
	//   Span B: BlockRef{0,1} — has start (2s) but NO duration → must be skipped
	startCol := &shared.IntrinsicColumn{
		Name:         "span:start",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        2,
		Uint64Values: []uint64{1_000_000_000, 2_000_000_000},
		BlockRefs: []shared.BlockRef{
			{BlockIdx: 0, RowIdx: 0}, // span A
			{BlockIdx: 0, RowIdx: 1}, // span B — no matching duration
		},
	}
	durCol := &shared.IntrinsicColumn{
		Name:         "span:duration",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        1,
		Uint64Values: []uint64{500_000_000},
		BlockRefs: []shared.BlockRef{
			{BlockIdx: 0, RowIdx: 0}, // span A only
		},
	}

	r := makeTestReader(t, map[string]*shared.IntrinsicColumn{
		"span:start":    startCol,
		"span:duration": durCol,
	})

	endCol, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	require.NotNil(t, endCol, "synthesized span:end must not be nil when at least one pair exists")

	// Only span A should be in the result — span B is skipped.
	assert.Equal(t, 1, len(endCol.Uint64Values), "only the span with a matching duration must appear")
	assert.Equal(t, uint64(1_500_000_000), endCol.Uint64Values[0], "end = start + duration")
	assert.Equal(t, shared.BlockRef{BlockIdx: 0, RowIdx: 0}, endCol.BlockRefs[0])
}

// TestSynthesizeSpanEnd_AllMissingDurations verifies that synthesizeSpanEnd returns
// a non-nil but empty column when every span:start has no matching span:duration.
func TestSynthesizeSpanEnd_AllMissingDurations(t *testing.T) {
	startCol := &shared.IntrinsicColumn{
		Name:         "span:start",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        2,
		Uint64Values: []uint64{1_000_000_000, 2_000_000_000},
		BlockRefs:    []shared.BlockRef{{BlockIdx: 0, RowIdx: 0}, {BlockIdx: 0, RowIdx: 1}},
	}
	durCol := &shared.IntrinsicColumn{
		Name:         "span:duration",
		Type:         shared.ColumnTypeUint64,
		Format:       shared.IntrinsicFormatFlat,
		Count:        1,
		Uint64Values: []uint64{999_000_000},
		BlockRefs:    []shared.BlockRef{{BlockIdx: 1, RowIdx: 99}}, // no overlap with startCol refs
	}

	r := makeTestReader(t, map[string]*shared.IntrinsicColumn{
		"span:start":    startCol,
		"span:duration": durCol,
	})

	endCol, err := r.GetIntrinsicColumn("span:end")
	require.NoError(t, err)
	// synthesizeSpanEnd returns a non-nil column even when all spans are skipped.
	require.NotNil(t, endCol)
	assert.Equal(t, 0, len(endCol.Uint64Values), "all spans skipped → empty values")
	assert.Equal(t, uint32(0), endCol.Count)
}
