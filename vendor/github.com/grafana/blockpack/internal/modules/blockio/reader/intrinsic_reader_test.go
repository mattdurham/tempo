package reader_test

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// intrinsicMemProvider is a minimal ReaderProvider backed by a byte slice, for tests.
type intrinsicMemProvider struct{ data []byte }

func (m *intrinsicMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *intrinsicMemProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// newIntrinsicTestProvider wraps data in an intrinsicMemProvider.
func newIntrinsicTestProvider(data []byte) rw.ReaderProvider {
	return &intrinsicMemProvider{data: data}
}

// buildIntrinsicTestFile writes a minimal blockpack file with one span and returns the raw bytes.
func buildIntrinsicTestFile(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 100})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}
	span := &tracev1.Span{
		TraceId:           bytes.Repeat([]byte{0x01}, 16),
		SpanId:            bytes.Repeat([]byte{0x02}, 8),
		Name:              "test-op",
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
	}
	if err := w.AddSpan(span.TraceId, span, nil, "", nil, ""); err != nil {
		t.Fatalf("AddSpan: %v", err)
	}
	if _, err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// TestReaderParsesV4FooterAndTOC verifies that NewReaderFromProvider reads the
// v4 footer and populates the intrinsic index.
func TestReaderParsesV4FooterAndTOC(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Fatal("HasIntrinsicSection() = false, want true")
	}
	names := r.IntrinsicColumnNames()
	if len(names) == 0 {
		t.Error("IntrinsicColumnNames() is empty after parsing v4 footer")
	}
}

// TestGetIntrinsicColumnSpanDuration verifies that GetIntrinsicColumn returns
// a decoded column with the expected value and block ref.
func TestGetIntrinsicColumnSpanDuration(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	col, err := r.GetIntrinsicColumn("span:duration")
	if err != nil {
		t.Fatalf("GetIntrinsicColumn: %v", err)
	}
	if col == nil {
		t.Fatal("GetIntrinsicColumn returned nil")
	}
	if col.Count != 1 {
		t.Errorf("Count = %d, want 1", col.Count)
	}
	// Expected duration: 2_000_000_000 - 1_000_000_000 = 1_000_000_000 ns
	expectedDur := uint64(1_000_000_000)
	if len(col.Uint64Values) == 0 || col.Uint64Values[0] != expectedDur {
		t.Errorf("duration = %v, want [%d]", col.Uint64Values, expectedDur)
	}
	// BlockRef must point to block 0, row 0.
	if len(col.BlockRefs) == 0 || col.BlockRefs[0].BlockIdx != 0 || col.BlockRefs[0].RowIdx != 0 {
		t.Errorf("BlockRefs = %v, want [{BlockIdx:0, RowIdx:0}]", col.BlockRefs)
	}
}

// TestGetIntrinsicColumnSpanName verifies a dict column round-trip.
func TestGetIntrinsicColumnSpanName(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	col, err := r.GetIntrinsicColumn("span:name")
	if err != nil {
		t.Fatalf("GetIntrinsicColumn: %v", err)
	}
	if col == nil {
		t.Fatal("GetIntrinsicColumn returned nil")
	}
	if len(col.DictEntries) != 1 || col.DictEntries[0].Value != "test-op" {
		t.Errorf("DictEntries = %v, want [{Value:test-op}]", col.DictEntries)
	}
}

// TestGetIntrinsicColumnMissing verifies nil return for unknown column names.
func TestGetIntrinsicColumnMissing(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, _ := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	col, err := r.GetIntrinsicColumn("span.http.method") // dynamic attr, not intrinsic
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if col != nil {
		t.Errorf("expected nil for unknown column, got %+v", col)
	}
}

// TestReaderV3FooterBackwardCompat verifies that a file with v3 footer is still readable.
// This tests that v4 reader handles older files without intrinsic section.
func TestReaderV3FooterBackwardCompat(t *testing.T) {
	// Since we can't easily produce v3 files after the footer version bump, skip for now.
	t.Skip("TODO: add v3 footer backward compat test once test infra supports it")
}

// TestIntrinsicColumnsFullRoundTrip writes a file with multiple spans across multiple
// blocks and verifies all intrinsic columns round-trip correctly via the reader.
func TestIntrinsicColumnsFullRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	// MaxBlockSpans=2 so spans split across 2 blocks.
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 2})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}

	traceID := bytes.Repeat([]byte{0xAB}, 16)
	spans := []struct {
		name               string
		startNano, endNano uint64
	}{
		{"op-A", 1_000_000_000, 1_100_000_000}, // duration 100ms
		{"op-B", 2_000_000_000, 2_200_000_000}, // duration 200ms
		{"op-A", 3_000_000_000, 3_150_000_000}, // duration 150ms (op-A second occurrence)
		{"op-C", 4_000_000_000, 4_500_000_000}, // duration 500ms
	}
	for i, s := range spans {
		spanID := bytes.Repeat([]byte{byte(i + 1)}, 8)
		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              s.name,
			Kind:              tracev1.Span_SPAN_KIND_CLIENT,
			StartTimeUnixNano: s.startNano,
			EndTimeUnixNano:   s.endNano,
		}
		if addErr := w.AddSpan(traceID, span, nil, "", nil, ""); addErr != nil {
			t.Fatalf("AddSpan %d: %v", i, addErr)
		}
	}
	if _, flushErr := w.Flush(); flushErr != nil {
		t.Fatalf("Flush: %v", flushErr)
	}
	data := buf.Bytes()

	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}

	if !r.HasIntrinsicSection() {
		t.Fatal("HasIntrinsicSection() = false")
	}

	// span:name dict has 3 unique values (op-A, op-B, op-C); op-A appears 2 times.
	nameCol, err := r.GetIntrinsicColumn("span:name")
	if err != nil {
		t.Fatalf("GetIntrinsicColumn(span:name): %v", err)
	}
	if nameCol == nil {
		t.Fatal("GetIntrinsicColumn(span:name) returned nil")
	}
	if len(nameCol.DictEntries) != 3 {
		t.Errorf("span:name dict size = %d, want 3", len(nameCol.DictEntries))
	}
	opAIdx := -1
	for i, e := range nameCol.DictEntries {
		if e.Value == "op-A" {
			opAIdx = i
		}
	}
	if opAIdx == -1 {
		t.Error("op-A not found in span:name dict")
	} else if len(nameCol.DictEntries[opAIdx].BlockRefs) != 2 {
		t.Errorf("op-A refs = %d, want 2", len(nameCol.DictEntries[opAIdx].BlockRefs))
	}

	// span:duration: 4 entries, sorted ascending.
	durCol, err := r.GetIntrinsicColumn("span:duration")
	if err != nil {
		t.Fatalf("GetIntrinsicColumn(span:duration): %v", err)
	}
	if durCol == nil {
		t.Fatal("GetIntrinsicColumn(span:duration) returned nil")
	}
	if durCol.Count != 4 {
		t.Errorf("span:duration Count = %d, want 4", durCol.Count)
	}
	for i := 1; i < len(durCol.Uint64Values); i++ {
		if durCol.Uint64Values[i] < durCol.Uint64Values[i-1] {
			t.Errorf("span:duration not sorted at index %d: %d > %d",
				i, durCol.Uint64Values[i-1], durCol.Uint64Values[i])
		}
	}
}
