package reader_test

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// buildMultiBlockFile writes a file with many spans across multiple blocks so that
// intrinsic columns have enough rows to potentially span multiple pages.
// Returns raw file bytes.
func buildMultiBlockFile(t *testing.T, spansPerBlock, blockCount int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: spansPerBlock,
	})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}

	total := spansPerBlock * blockCount
	for i := range total {
		traceID := make([]byte, 16)
		traceID[0] = byte(i % 256)
		traceID[1] = byte(i / 256)
		spanID := make([]byte, 8)
		spanID[0] = byte(i % 256)

		sp := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "op",
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(2_000_000_000 + i*1000), //nolint:gosec
		}
		if err := w.AddSpan(sp.TraceId, sp, nil, "", nil, ""); err != nil {
			t.Fatalf("AddSpan: %v", err)
		}
	}
	if _, err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// TestGetIntrinsicColumnForRefs_ReturnsFullColumn verifies that GetIntrinsicColumnForRefs
// decodes all rows (page-skipping was removed in NOTE-007; all pages are always decoded).
// The refFilter parameter is retained for API compatibility but no longer skips pages.
func TestGetIntrinsicColumnForRefs_ReturnsFullColumn(t *testing.T) {
	// To force multiple pages we need >IntrinsicPageSize rows (10,000).
	// Use MaxBlockSpans=10001 to get one block with >10K spans → 2+ pages.
	// Lower MaxIntrinsicRows temporarily to allow the test to proceed.
	origMax := shared.MaxIntrinsicRows
	shared.MaxIntrinsicRows = 100_000
	defer func() { shared.MaxIntrinsicRows = origMax }()

	data := buildMultiBlockFile(t, 10001, 1)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	// Get the full column to find a ref we can use.
	full, err := r.GetIntrinsicColumn("span:duration")
	if err != nil || full == nil {
		t.Skip("span:duration column not available")
	}
	if len(full.BlockRefs) == 0 {
		t.Skip("column has no refs")
	}

	// Pick just one ref.
	selectedRef := full.BlockRefs[0]
	refs := []shared.BlockRef{selectedRef}

	col, err := r.GetIntrinsicColumnForRefs("span:duration", refs)
	if err != nil {
		t.Fatalf("GetIntrinsicColumnForRefs: %v", err)
	}

	// NOTE-007: RefBloom and page-skipping were removed. All pages are decoded;
	// GetIntrinsicColumnForRefs returns the full column regardless of ref count.
	if col == nil {
		t.Fatal("result is nil, want non-nil column")
	}
	if col.Count != full.Count {
		t.Errorf("expected full column count %d, got %d (NOTE-007: page-skipping removed)", full.Count, col.Count)
	}
}

// TestGetIntrinsicColumnForRefs_NilRefs_ReturnsNil verifies that nil refs returns (nil, nil).
func TestGetIntrinsicColumnForRefs_NilRefs_ReturnsNil(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}

	col, err := r.GetIntrinsicColumnForRefs("span:duration", nil)
	if err != nil {
		t.Fatalf("GetIntrinsicColumnForRefs(nil): unexpected error: %v", err)
	}
	if col != nil {
		t.Errorf("GetIntrinsicColumnForRefs(nil) = non-nil, want nil")
	}
}

// TestGetIntrinsicColumnForRefs_ColumnNotPresent verifies that a missing column returns (nil, nil).
func TestGetIntrinsicColumnForRefs_ColumnNotPresent(t *testing.T) {
	data := buildIntrinsicTestFile(t)
	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}

	ref := shared.BlockRef{BlockIdx: 0, RowIdx: 0}
	col, err := r.GetIntrinsicColumnForRefs("nonexistent:column", []shared.BlockRef{ref})
	if err != nil {
		t.Fatalf("GetIntrinsicColumnForRefs: unexpected error: %v", err)
	}
	if col != nil {
		t.Errorf("GetIntrinsicColumnForRefs for missing column = non-nil, want nil")
	}
}

// TestGetIntrinsicColumnForRefs_NoIntrinsicSection verifies that files without an
// intrinsic section return (nil, nil).
func TestGetIntrinsicColumnForRefs_NoIntrinsicSection(t *testing.T) {
	// Build a file with no spans (empty file has no intrinsic section).
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 100})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}
	if _, flushErr := w.Flush(); flushErr != nil {
		t.Fatalf("Flush: %v", flushErr)
	}
	data := buf.Bytes()

	r, err := reader.NewReaderFromProvider(newIntrinsicTestProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}

	ref := shared.BlockRef{BlockIdx: 0, RowIdx: 0}
	col, err := r.GetIntrinsicColumnForRefs("span:duration", []shared.BlockRef{ref})
	if err != nil {
		t.Fatalf("GetIntrinsicColumnForRefs on empty file: unexpected error: %v", err)
	}
	if col != nil {
		t.Errorf("GetIntrinsicColumnForRefs on empty file = non-nil, want nil")
	}
}
