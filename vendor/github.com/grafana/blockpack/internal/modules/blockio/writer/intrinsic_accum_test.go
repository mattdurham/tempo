package writer

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// intrinsicTestProvider is a minimal in-memory ReaderProvider for intrinsic_accum_test.go.
type intrinsicTestProvider struct{ data []byte }

func (p *intrinsicTestProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *intrinsicTestProvider) ReadAt(b []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	return n, nil
}

// openWriterTestReader opens a reader from raw bytes written by the writer.
func openWriterTestReader(t *testing.T, data []byte) (*modules_reader.Reader, error) {
	t.Helper()
	return modules_reader.NewReaderFromProvider(&intrinsicTestProvider{data: data})
}

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
	// After auto-flush, intrinsicAccum should have been populated by flushBlocks.
	if w2.intrinsicAccum == nil {
		t.Error("intrinsicAccum should be non-nil after spans trigger block builds")
	}
}

// TestWriterFlushWritesIntrinsicSection verifies that Flush writes a V14 footer and
// readable intrinsic column entries when spans have been added.
// V14 format: intrinsic columns are name-keyed entries in the section directory.
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

	// Verify V14 footer (FooterV7): last 18 bytes = magic[4]+version[2]+dir_offset[8]+dir_len[4].
	data := buf.Bytes()
	footerStart := len(data) - int(shared.FooterV7Size)
	if footerStart < 0 {
		t.Fatalf("file too small: %d bytes", len(data))
	}
	magic := binary.LittleEndian.Uint32(data[footerStart:])
	if magic != shared.MagicNumber {
		t.Errorf("footer magic = 0x%08X, want 0x%08X", magic, shared.MagicNumber)
	}
	version := binary.LittleEndian.Uint16(data[footerStart+4:])
	if version != shared.FooterV7Version {
		t.Errorf("footer version = %d, want %d", version, shared.FooterV7Version)
	}

	// Open via Reader and check that HasIntrinsicSection returns true.
	r, openErr := openWriterTestReader(t, data)
	if openErr != nil {
		t.Fatalf("open reader: %v", openErr)
	}
	if !r.HasIntrinsicSection() {
		t.Error("HasIntrinsicSection() = false, want true after Flush with spans")
	}
	names := r.IntrinsicColumnNames()
	if len(names) == 0 {
		t.Error("IntrinsicColumnNames() is empty, want > 0 entries")
	}

	// span:duration must be present.
	col, colErr := r.GetIntrinsicColumn("span:duration")
	if colErr != nil {
		t.Fatalf("GetIntrinsicColumn(span:duration): %v", colErr)
	}
	if col == nil {
		t.Error("span:duration not in intrinsic section")
	} else if col.Count != 1 {
		t.Errorf("span:duration Count = %d, want 1", col.Count)
	}
}
