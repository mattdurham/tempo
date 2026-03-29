package blockio_test

// BenchmarkSpanFieldsAdapter measures the per-span field-access cost for both the
// fast O(1) block-column path and the O(N) intrinsic-fallback path (loadIntrinsicCache).
//
// Context: PR #172 removed intrinsic columns from block payloads, leaving them only in
// the intrinsic section (sorted by value, not by BlockRef). Every GetField call on a
// matched span then hit loadIntrinsicCache, which scans ALL N entries in each intrinsic
// column linearly until finding the matching (blockIdx, rowIdx) key. At production scale
// this became the #1 hotspot at 62.71% flat CPU.
//
// With dual storage (restored after PR #172 rollback), new files store intrinsic columns
// in block payloads alongside user attributes. GetField finds them via modulesGetValue
// (O(1) row index access) before reaching the intrinsic fallback. loadIntrinsicCache
// is kept only as a backward-compatibility safety net for files written during the
// regression window.
//
// Run with:
//
//	go test -bench=BenchmarkSpanFieldsAdapter -benchmem ./internal/modules/blockio/

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

const (
	spanFieldsBenchMaxBlockSpans = 50   // MaxBlockSpans → ~40 blocks for 2000 spans
	spanFieldsBenchTotalSpans    = 2000 // ~40 blocks × 50 spans
)

// spanFieldsBenchMemProvider is a minimal in-memory ByteRangeProvider.
type spanFieldsBenchMemProvider struct{ data []byte }

func (m *spanFieldsBenchMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *spanFieldsBenchMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// buildSpanFieldsBenchFile writes a blockpack file with spanFieldsBenchTotalSpans spans
// across multiple blocks and returns the raw bytes. Each span has a few span and resource
// attributes alongside the intrinsic columns (trace:id, span:id, span:name, span:kind,
// span:start, span:end, span:duration) stored via dual storage.
func buildSpanFieldsBenchFile(b *testing.B) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: spanFieldsBenchMaxBlockSpans,
	})
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	serviceNames := []string{"auth-service", "query-service", "ingester", "distributor", "compactor"}
	spanNames := []string{"HTTP GET", "HTTP POST", "gRPC Query", "Write", "Flush"}

	traceID := [16]byte{0xCA, 0xFE}
	for i := range spanFieldsBenchTotalSpans {
		spanID := []byte{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0x01} //nolint:gosec
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              spanNames[i%len(spanNames)],
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(i) * 1_000_000,      //nolint:gosec
			EndTimeUnixNano:   uint64(i)*1_000_000 + 2000, //nolint:gosec
		}
		resourceAttrs := map[string]any{
			"service.name": serviceNames[i%len(serviceNames)],
			"env":          "prod",
		}
		if err := w.AddSpan(traceID[:], span, resourceAttrs, "", nil, ""); err != nil {
			b.Fatalf("AddSpan %d: %v", i, err)
		}
	}
	if _, err := w.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// getFirstBlock parses the first block from data via GetBlockWithBytes, which calls
// BuildIterFields internally so the fast IterFields path is available.
func getFirstBlock(b *testing.B, data []byte) (*modules_reader.Reader, *modules_reader.Block) {
	b.Helper()
	r, err := modules_reader.NewReaderFromProvider(&spanFieldsBenchMemProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	if r.BlockCount() == 0 {
		b.Fatal("expected at least 1 block")
	}
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	if err != nil {
		b.Fatalf("GetBlockWithBytes: %v", err)
	}
	return r, bwb.Block
}

// intrinsicFieldNames is the set of field names exercised by the benchmarks.
// These are the columns that were removed from block payloads in PR #172,
// causing every GetField call to fall through to the O(N) loadIntrinsicCache path.
var intrinsicFieldNames = []string{
	"span:name",
	"span:kind",
	"span:duration",
	"resource.service.name",
	"span:start",
}

// BenchmarkSpanFieldsAdapter measures the per-span GetField cost for intrinsic columns.
//
// Sub-benchmarks:
//   - BlockColumn: adapter with reader=nil (block columns present via dual storage).
//     GetField finds the column via modulesLookupColumn and reads the value in O(1).
//     This is the expected hot path for new files.
//   - IntrinsicFallback: adapter with reader set but block columns also present.
//     GetField first finds the value from block columns (O(1)), so intrinsicCache
//     is never populated. This verifies the dual-storage fast path also holds when
//     a reader is present (the common case in production after the rollback).
func BenchmarkSpanFieldsAdapter(b *testing.B) {
	data := buildSpanFieldsBenchFile(b)

	b.Run("BlockColumn", func(b *testing.B) {
		// Block columns present (dual storage), reader=nil.
		// GetField resolves each intrinsic field in O(1) via modulesLookupColumn +
		// modulesGetValue. No intrinsic section scan. This is the O(1) baseline that
		// should be maintained as intrinsic column storage evolves.
		_, block := getFirstBlock(b, data)
		adapter := modules_blockio.NewSpanFieldsAdapter(block, 0)
		defer modules_blockio.ReleaseSpanFieldsAdapter(adapter)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, name := range intrinsicFieldNames {
				_, _ = adapter.GetField(name)
			}
		}
	})

	b.Run("IntrinsicFallback", func(b *testing.B) {
		// Block columns present AND reader is set — simulates the production path
		// where NewSpanFieldsAdapterWithReader is called (e.g., from api.go).
		// With dual storage, block columns are found before the intrinsic fallback,
		// so intrinsicCache remains nil and loadIntrinsicCache is never called.
		// This sub-benchmark guards against regressions where the fallback path is
		// accidentally triggered even when block columns are available.
		r, block := getFirstBlock(b, data)
		adapter := modules_blockio.NewSpanFieldsAdapterWithReader(block, r, 0, 0)
		defer modules_blockio.ReleaseSpanFieldsAdapter(adapter)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, name := range intrinsicFieldNames {
				_, _ = adapter.GetField(name)
			}
		}
	})
}
