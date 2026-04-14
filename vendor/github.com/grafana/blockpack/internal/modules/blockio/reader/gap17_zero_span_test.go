package reader_test

// GAP-17: Zero-span block — verify that a Reader whose blockMetas slice contains
// an entry with SpanCount==0 does not panic and reports correct metadata.
//
// The Writer never produces a zero-span block through normal operation, but
// externally-produced or corrupted files might. This test uses SetBlockMetasForTest
// (export_test.go) to inject a synthetic zero-span entry.

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

// writeMinimalSingleSpanFile writes a one-span blockpack file and returns its bytes.
func writeMinimalSingleSpanFile(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	cfg := writer.Config{OutputStream: &buf}
	w, err := writer.NewWriterWithConfig(cfg)
	require.NoError(t, err)

	tid := [16]byte{0xAA}
	span := &tracev1.Span{
		TraceId:           tid[:],
		SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Name:              "op.test",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
	}
	err = w.AddSpan(tid[:], span, map[string]any{"service.name": "svc"}, "", nil, "")
	require.NoError(t, err)
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TestGAP17_ZeroSpanBlock_BlockCount verifies that injecting a zero-span BlockMeta
// alongside a real one is reflected by BlockCount without panicking.
func TestGAP17_ZeroSpanBlock_BlockCount(t *testing.T) {
	data := writeMinimalSingleSpanFile(t)
	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	require.Equal(t, 1, r.BlockCount())

	realMeta := r.BlockMeta(0)
	zeroSpanMeta := shared.BlockMeta{
		Offset:    realMeta.Offset,
		Length:    realMeta.Length,
		MinStart:  0,
		MaxStart:  0,
		SpanCount: 0,
	}
	reader.SetBlockMetasForTest(r, []shared.BlockMeta{zeroSpanMeta, realMeta})

	assert.Equal(t, 2, r.BlockCount(), "BlockCount must reflect injected metas")
}

// TestGAP17_ZeroSpanBlock_MetaFields verifies that BlockMeta returns the correct
// fields for a zero-span entry without panicking.
func TestGAP17_ZeroSpanBlock_MetaFields(t *testing.T) {
	data := writeMinimalSingleSpanFile(t)
	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)

	zeroMeta := shared.BlockMeta{SpanCount: 0, MinStart: 100, MaxStart: 200}
	reader.SetBlockMetasForTest(r, []shared.BlockMeta{zeroMeta})

	got := r.BlockMeta(0)
	assert.Equal(t, uint32(0), got.SpanCount)
	assert.Equal(t, uint64(100), got.MinStart)
	assert.Equal(t, uint64(200), got.MaxStart)
}

// TestGAP17_ZeroSpanBlock_NewBlockForParsing verifies that constructing a Block
// from a zero-span BlockMeta does not panic and returns SpanCount==0 with no columns.
func TestGAP17_ZeroSpanBlock_NewBlockForParsing(t *testing.T) {
	meta := shared.BlockMeta{SpanCount: 0}
	blk := reader.NewBlockForParsing(meta)
	require.NotNil(t, blk)
	assert.Equal(t, 0, blk.SpanCount(), "zero-span block must report SpanCount==0")
	assert.Nil(t, blk.GetColumn("span:name"), "zero-span block must have no columns")
}
