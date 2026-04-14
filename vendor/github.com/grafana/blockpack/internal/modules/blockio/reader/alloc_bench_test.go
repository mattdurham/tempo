package reader_test

// NOTE: Allocation regression benchmarks — any increase in allocs/op here signals a
// regression in the pool-based optimisations implemented in Fix A and Fix B.
// Run with: go test -bench=BenchmarkParseBlockFromBytes_AllocCount -benchmem -count=3 ./internal/modules/blockio/reader/

import (
	"bytes"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// buildBenchFile creates an in-memory blockpack file with the given number of spans
// spread across blocks of maxPerBlock spans. Returns the raw bytes.
func buildBenchFile(b *testing.B, numSpans, maxPerBlock int) []byte {
	b.Helper()

	var buf bytes.Buffer
	cfg := writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: maxPerBlock,
	}
	w, err := writer.NewWriterWithConfig(cfg)
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	attrs := []*commonv1.KeyValue{
		{
			Key:   "http.method",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
		},
		{
			Key:   "http.status_code",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}},
		},
		{
			Key:   "http.url",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "/api/v1/traces"}},
		},
	}

	traceID := [16]byte{0xAB, 0xCD, 0xEF}
	for i := range numSpans {
		spanID := []byte{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 1}
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              "bench.op",
			StartTimeUnixNano: uint64(i) * 1_000_000,
			EndTimeUnixNano:   uint64(i)*1_000_000 + 500_000,
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Attributes:        attrs,
		}
		resourceAttrs := map[string]any{
			"service.name": "bench-svc",
			"host.name":    "host-01",
		}
		if err := w.AddSpan(traceID[:], span, resourceAttrs, "", nil, ""); err != nil {
			b.Fatalf("AddSpan: %v", err)
		}
	}

	if _, err := w.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// BenchmarkParseBlockFromBytes_AllocCount measures allocations per parse call.
// It contains two sub-benchmarks:
//   - Baseline: calls ParseBlockFromBytes (non-pooled path) — always allocates a fresh intern map.
//   - WithIntern: calls ParseBlockFromBytesWithIntern with a pooled map (Fix A regression guard).
//     Target: 0 intern-map allocs per call in the WithIntern sub-benchmark.
func BenchmarkParseBlockFromBytes_AllocCount(b *testing.B) {
	data := buildBenchFile(b, 200, 0) // single block, ~200 spans

	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	if r.BlockCount() == 0 {
		b.Fatal("expected at least 1 block")
	}

	// Fetch block bytes once; we re-parse them in the loop to measure pure parse allocs.
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	if err != nil {
		b.Fatalf("GetBlockWithBytes: %v", err)
	}
	rawBytes := bwb.RawBytes
	meta := r.BlockMeta(0)

	// Baseline: non-pooled path — ParseBlockFromBytes always allocates its own intern map.
	b.Run("Baseline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			parsed, pErr := r.ParseBlockFromBytes(rawBytes, nil, meta)
			if pErr != nil {
				b.Fatalf("ParseBlockFromBytes: %v", pErr)
			}
			_ = parsed
		}
	})

	// WithIntern: pooled path — Fix A regression guard.
	// The intern map is acquired once outside the loop (simulating scanBlocks acquire/release
	// per block) and reused across iterations. allocs/op should show 0 intern-map allocations.
	b.Run("WithIntern", func(b *testing.B) {
		internPtr := reader.AcquireInternMap()
		defer reader.ReleaseInternMap(internPtr)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			clear(*internPtr)
			parsed, pErr := r.ParseBlockFromBytesWithIntern(rawBytes, nil, meta, *internPtr)
			if pErr != nil {
				b.Fatalf("ParseBlockFromBytesWithIntern: %v", pErr)
			}
			_ = parsed
		}
	})
}

// BenchmarkCollectScanBlocks_AllocCount measures allocations when fetching all blocks
// sequentially via GetBlockWithBytes — a proxy for the per-block allocation overhead
// that Fix A (intern map pool) and Fix B (present-rows scratch pool) target.
// NOTE: regression guard — allocs/op should decrease as pool fixes are applied.
// This benchmark does NOT run query execution; it measures block parsing only.
func BenchmarkCollectScanBlocks_AllocCount(b *testing.B) {
	const totalSpans = 2000
	const maxPerBlock = 200 // ~10 blocks

	data := buildBenchFile(b, totalSpans, maxPerBlock)

	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	if r.BlockCount() == 0 {
		b.Fatal("expected at least 1 block")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for i := range r.BlockCount() {
			bwb, bErr := r.GetBlockWithBytes(i, nil, nil)
			if bErr != nil {
				b.Fatalf("GetBlockWithBytes block %d: %v", i, bErr)
			}
			_ = bwb.Block.SpanCount()
		}
	}
}
