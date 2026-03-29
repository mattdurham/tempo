package writer

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestFlatAccumFeedAndEncode verifies that feeding uint64 values into a flat accumulator
// and encoding produces sorted values with matching blockRefs.
func TestFlatAccumFeedAndEncode(t *testing.T) {
	a := newIntrinsicAccumulator()

	// Feed rows out of order to verify sort.
	a.feedUint64("span:duration", shared.ColumnTypeUint64, 300, 0, 2)
	a.feedUint64("span:duration", shared.ColumnTypeUint64, 100, 0, 0)
	a.feedUint64("span:duration", shared.ColumnTypeUint64, 200, 0, 1)

	blob, err := a.encodeColumn("span:duration")
	if err != nil {
		t.Fatalf("encodeColumn: %v", err)
	}
	if len(blob) == 0 {
		t.Fatal("blob is empty")
	}

	// Decode and verify sorted order.
	col, err := decodeIntrinsicColumnBlob(blob)
	if err != nil {
		t.Fatalf("decodeIntrinsicColumnBlob: %v", err)
	}
	if col.Count != 3 {
		t.Errorf("Count = %d, want 3", col.Count)
	}
	// Values must be sorted ascending.
	if col.Uint64Values[0] != 100 || col.Uint64Values[1] != 200 || col.Uint64Values[2] != 300 {
		t.Errorf("values not sorted: %v", col.Uint64Values)
	}
	// Block refs must match sorted order.
	if col.BlockRefs[0].RowIdx != 0 || col.BlockRefs[1].RowIdx != 1 || col.BlockRefs[2].RowIdx != 2 {
		t.Errorf("block refs not aligned: %v", col.BlockRefs)
	}
}

// TestDictAccumFeedAndEncode verifies that feeding string values into a dict accumulator
// and encoding produces a correct dictionary.
func TestDictAccumFeedAndEncode(t *testing.T) {
	a := newIntrinsicAccumulator()

	a.feedString("span:name", shared.ColumnTypeString, "GET /foo", 0, 0)
	a.feedString("span:name", shared.ColumnTypeString, "POST /bar", 0, 1)
	a.feedString("span:name", shared.ColumnTypeString, "GET /foo", 1, 0) // duplicate

	blob, err := a.encodeColumn("span:name")
	if err != nil {
		t.Fatalf("encodeColumn: %v", err)
	}

	col, err := decodeIntrinsicColumnBlob(blob)
	if err != nil {
		t.Fatalf("decodeIntrinsicColumnBlob: %v", err)
	}
	// 2 unique values.
	if len(col.DictEntries) != 2 {
		t.Errorf("DictEntries len = %d, want 2", len(col.DictEntries))
	}
	// "GET /foo" has 2 refs; "POST /bar" has 1 ref.
	var getFooRefs, postBarRefs int
	for _, e := range col.DictEntries {
		if e.Value == "GET /foo" {
			getFooRefs = len(e.BlockRefs)
		}
		if e.Value == "POST /bar" {
			postBarRefs = len(e.BlockRefs)
		}
	}
	if getFooRefs != 2 {
		t.Errorf("GET /foo refs = %d, want 2", getFooRefs)
	}
	if postBarRefs != 1 {
		t.Errorf("POST /bar refs = %d, want 1", postBarRefs)
	}
}

// TestBytesAccumFeedAndEncode verifies bytes (trace:id, span:id) columns.
func TestBytesAccumFeedAndEncode(t *testing.T) {
	a := newIntrinsicAccumulator()

	id1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	id2 := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15} // lexicographically smaller

	a.feedBytes("trace:id", shared.ColumnTypeBytes, id1, 0, 0)
	a.feedBytes("trace:id", shared.ColumnTypeBytes, id2, 0, 1)

	blob, err := a.encodeColumn("trace:id")
	if err != nil {
		t.Fatalf("encodeColumn: %v", err)
	}

	col, err := decodeIntrinsicColumnBlob(blob)
	if err != nil {
		t.Fatalf("decodeIntrinsicColumnBlob: %v", err)
	}
	if col.Count != 2 {
		t.Errorf("Count = %d, want 2", col.Count)
	}
	// id2 sorts before id1 (lexicographic bytes).
	if string(col.BytesValues[0]) != string(id2) {
		t.Errorf("first value not id2")
	}
}

// TestMaxIntrinsicRowsCap verifies that the accumulator tracks total rows
// and triggers the cap flag at MaxIntrinsicRows.
func TestMaxIntrinsicRowsCap(t *testing.T) {
	a := newIntrinsicAccumulator()
	for i := range shared.MaxIntrinsicRows + 1 {
		a.feedUint64("span:duration", shared.ColumnTypeUint64, uint64(i), 0, i%65535)
	}
	if !a.overCap() {
		t.Error("overCap() should be true after exceeding MaxIntrinsicRows")
	}
}

// TestAccumEncodeTOC verifies that the TOC round-trips correctly.
func TestAccumEncodeTOC(t *testing.T) {
	entries := []shared.IntrinsicColMeta{
		{
			Name: "span:duration", Type: shared.ColumnTypeUint64, Format: shared.IntrinsicFormatFlat,
			Offset: 1000, Length: 500, Count: 200, Min: "\x00\x00\x00\x00\x00\x00\x00\x00", Max: "\xff\xff\xff\xff\xff\xff\xff\xff",
		},
		{
			Name: "span:name", Type: shared.ColumnTypeString, Format: shared.IntrinsicFormatDict,
			Offset: 1500, Length: 300, Count: 200, Min: "GET /", Max: "POST /",
		},
	}
	blob, err := encodeTOC(entries)
	if err != nil {
		t.Fatalf("encodeTOC: %v", err)
	}
	decoded, err := decodeTOC(blob)
	if err != nil {
		t.Fatalf("decodeTOC: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded len = %d, want 2", len(decoded))
	}
	if decoded[0].Name != "span:duration" || decoded[0].Offset != 1000 || decoded[0].Count != 200 {
		t.Errorf("first entry mismatch: %+v", decoded[0])
	}
	if decoded[1].Name != "span:name" || decoded[1].Min != "GET /" {
		t.Errorf("second entry mismatch: %+v", decoded[1])
	}
}

// TestWriterAccumulatesIntrinsics verifies that writing spans populates the accumulator.
func TestWriterAccumulatesIntrinsics(t *testing.T) {
	span := &tracev1.Span{
		TraceId:           bytes.Repeat([]byte{1}, 16),
		SpanId:            bytes.Repeat([]byte{2}, 8),
		Name:              "GET /hello",
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: 1000,
		EndTimeUnixNano:   2000,
	}

	// Use MaxBlockSpans=1 and MaxBufferedSpans=1 to trigger auto-flush immediately.
	w2, err := NewWriterWithConfig(Config{OutputStream: io.Discard, MaxBlockSpans: 1, MaxBufferedSpans: 1})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}
	// First span fills the buffer and triggers auto-flush (MaxBufferedSpans=1).
	if err := w2.AddSpan(span.TraceId, span, nil, "", nil, ""); err != nil {
		t.Fatalf("AddSpan: %v", err)
	}
	// After auto-flush, intrinsicAccum should have been created by buildAndWriteBlock.
	if w2.intrinsicAccum == nil {
		t.Error("intrinsicAccum should be non-nil after spans trigger block builds")
	}
}

// TestWriterFlushWritesIntrinsicSection verifies that Flush writes a v4 footer and
// a readable intrinsic section when spans have been added.
func TestWriterFlushWritesIntrinsicSection(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriterWithConfig(Config{OutputStream: &buf, MaxBlockSpans: 100})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}

	span := &tracev1.Span{
		TraceId:           bytes.Repeat([]byte{0xAB}, 16),
		SpanId:            bytes.Repeat([]byte{0xCD}, 8),
		Name:              "my-operation",
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_500_000_000,
	}
	if addErr := w.AddSpan(span.TraceId, span, nil, "", nil, ""); addErr != nil {
		t.Fatalf("AddSpan: %v", addErr)
	}
	if _, flushErr := w.Flush(); flushErr != nil {
		t.Fatalf("Flush: %v", flushErr)
	}

	// Read the footer from the end of the buffer.
	data := buf.Bytes()
	footerStart := len(data) - int(shared.FooterV4Size)
	if footerStart < 0 {
		t.Fatalf("file too small: %d bytes", len(data))
	}
	version := binary.LittleEndian.Uint16(data[footerStart:])
	if version != shared.FooterV4Version {
		t.Errorf("footer version = %d, want %d", version, shared.FooterV4Version)
	}
	// intrinsicIndexOffset is at footerStart+22 (after version[2]+headerOffset[8]+compactOffset[8]+compactLen[4])
	intrinsicOffset := binary.LittleEndian.Uint64(data[footerStart+22:])
	intrinsicLen := binary.LittleEndian.Uint32(data[footerStart+30:])
	if intrinsicLen == 0 {
		t.Error("intrinsicIndexLen = 0, want > 0")
	}

	// Decode the TOC.
	if intrinsicOffset > uint64(len(data)) || uint64(intrinsicLen) > uint64(len(data))-intrinsicOffset {
		t.Fatalf("TOC out of bounds: offset=%d len=%d fileLen=%d", intrinsicOffset, intrinsicLen, len(data))
	}
	tocBlob := data[intrinsicOffset : intrinsicOffset+uint64(intrinsicLen)]
	entries, err := decodeTOC(tocBlob)
	if err != nil {
		t.Fatalf("decodeTOC: %v", err)
	}
	if len(entries) == 0 {
		t.Error("TOC has 0 entries, want > 0")
	}

	// span:duration must be present.
	var foundDur bool
	for _, e := range entries {
		if e.Name == "span:duration" {
			foundDur = true
			if e.Count != 1 {
				t.Errorf("span:duration Count = %d, want 1", e.Count)
			}
		}
	}
	if !foundDur {
		t.Error("span:duration not in TOC")
	}
}
