package reader_test

// BenchmarkIntrinsicReverseLookup demonstrates the O(N) scan cost of reverse lookups
// (ref→value) through the intrinsic TOC section vs the O(1) direct block column access.
//
// Context: PR #172 moved intrinsic columns out of block payloads into the intrinsic
// section exclusively. The intrinsic section is sorted by VALUE, not by BlockRef. A
// reverse lookup for a known (blockIdx, rowIdx) must scan all N entries because pages
// partition by value and page-level min/max cannot skip pages. With 2.8M spans × 11
// columns × 14 files this caused O(8.6B) operations per query.
//
// Run with:
//
//	go test -bench=BenchmarkIntrinsicReverseLookup -benchmem ./internal/modules/blockio/reader/

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/rw"
)

const (
	benchSpansPerBlock = 50   // MaxBlockSpans → ~20 blocks for 1000 spans
	benchTotalSpans    = 1000 // 20 blocks × 50 spans
	benchLookupRefs    = 10   // refs to resolve per benchmark iteration
)

// buildReverseLookupBenchFile writes a file with benchTotalSpans spans spread across
// blocks of benchSpansPerBlock spans each. Returns raw bytes.
func buildReverseLookupBenchFile(b *testing.B) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: benchSpansPerBlock,
	})
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	traceID := [16]byte{0xDE, 0xAD, 0xBE, 0xEF}
	serviceNames := []string{"auth-service", "query-service", "ingester", "distributor", "compactor"}
	spanNames := []string{"HTTP GET", "HTTP POST", "gRPC Query", "Write", "Flush"}

	for i := range benchTotalSpans {
		spanID := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0, 0, 0, 0, 0x01}
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              spanNames[i%len(spanNames)],
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(i) * 1_000_000,
			EndTimeUnixNano:   uint64(i)*1_000_000 + uint64(i%500)*1_000,
		}
		resourceAttrs := map[string]any{
			"service.name": serviceNames[i%len(serviceNames)],
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

type reverseBenchMemProvider struct{ data []byte }

func (m *reverseBenchMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *reverseBenchMemProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// pickSpreadRefs returns benchLookupRefs refs spread evenly across available blocks.
func pickSpreadRefs(blockCount int) []shared.BlockRef {
	refs := make([]shared.BlockRef, benchLookupRefs)
	for i := range benchLookupRefs {
		blockIdx := (i * blockCount) / benchLookupRefs
		refs[i] = shared.BlockRef{
			BlockIdx: uint16(blockIdx), //nolint:gosec
			RowIdx:   uint16(i % benchSpansPerBlock),
		}
	}
	return refs
}

// intrinsicReverseLookup performs a ref→value scan through all intrinsic columns for
// the given ref. This is the O(N) path that existed in PR #172's exclusive-intrinsic
// model: for each column, scan all BlockRefs linearly until the target is found.
// Returns the number of entries scanned across all columns.
func intrinsicReverseLookup(r *reader.Reader, ref shared.BlockRef) int {
	key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
	scanned := 0
	for _, colName := range r.IntrinsicColumnNames() {
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			continue
		}
		switch col.Format {
		case shared.IntrinsicFormatFlat:
			for _, r := range col.BlockRefs {
				scanned++
				if uint32(r.BlockIdx)<<16|uint32(r.RowIdx) == key {
					break
				}
			}
		case shared.IntrinsicFormatDict:
			for _, entry := range col.DictEntries {
				for _, r := range entry.BlockRefs {
					scanned++
					if uint32(r.BlockIdx)<<16|uint32(r.RowIdx) == key {
						break
					}
				}
			}
		}
	}
	return scanned
}

// BenchmarkIntrinsicReverseLookup_ONCost measures the O(N) scan cost of resolving
// benchLookupRefs refs through the intrinsic section — the regression introduced by
// PR #172 when it removed intrinsic columns from block payloads.
//
// At the benchmark's small scale (1000 spans / 20 blocks) the IntrinsicScan may appear
// faster due to CPU cache effects. The regression only becomes severe at production scale:
// 2.8M spans × 11 intrinsic columns × 14 files → O(8.6B) operations per query.
// This benchmark documents the algorithmic characteristic, not just the wall-clock cost.
//
// Sub-benchmarks:
//   - IntrinsicScan:        O(N) scan through all entries in each intrinsic column per ref
//   - BlockColumn:          O(1) row access via block column payload (includes I/O parse cost)
//   - BlockColumnPreloaded: O(1) row access with blocks pre-parsed (pure row access cost)
func BenchmarkIntrinsicReverseLookup_ONCost(b *testing.B) {
	data := buildReverseLookupBenchFile(b)
	r, err := reader.NewReaderFromProvider(&reverseBenchMemProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	if r.BlockCount() == 0 {
		b.Fatal("expected at least 1 block")
	}

	refs := pickSpreadRefs(r.BlockCount())

	// IntrinsicScan: simulates the PR #172 regression — each ref→value lookup
	// scans all N entries in each intrinsic column because the section is sorted
	// by value, not by BlockRef.
	b.Run("IntrinsicScan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			total := 0
			for _, ref := range refs {
				total += intrinsicReverseLookup(r, ref)
			}
			_ = total
		}
	})

	// BlockColumn: O(1) direct access via block column payload — the dual-storage path.
	// Includes the GetBlockWithBytes I/O + parse cost per ref, matching realistic usage
	// where the result-materialization loop reads a block and immediately accesses rows.
	b.Run("BlockColumn", func(b *testing.B) {
		wantCols := map[string]struct{}{
			"span:name":             {},
			"span:kind":             {},
			"span:duration":         {},
			"span:status":           {},
			"resource.service.name": {},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, ref := range refs {
				bwb, bErr := r.GetBlockWithBytes(int(ref.BlockIdx), wantCols, nil)
				if bErr != nil {
					b.Fatalf("GetBlockWithBytes: %v", bErr)
				}
				// O(1) column access by row index — no scan needed.
				if col := bwb.Block.GetColumn("span:duration"); col != nil {
					_, _ = col.Uint64Value(int(ref.RowIdx))
				}
				if col := bwb.Block.GetColumn("span:name"); col != nil {
					_, _ = col.StringValue(int(ref.RowIdx))
				}
			}
		}
	})

	// BlockColumnPreloaded: pure O(1) row access cost with blocks already parsed.
	// Pre-parses all referenced blocks outside the loop so the measured work is only
	// the column map lookup + row index access — no I/O or decompression overhead.
	b.Run("BlockColumnPreloaded", func(b *testing.B) {
		wantCols := map[string]struct{}{
			"span:name":             {},
			"span:kind":             {},
			"span:duration":         {},
			"span:status":           {},
			"resource.service.name": {},
		}
		// Pre-parse all blocks referenced by our refs.
		blocks := make(map[uint16]*reader.BlockWithBytes)
		for _, ref := range refs {
			if _, ok := blocks[ref.BlockIdx]; ok {
				continue
			}
			bwb, bErr := r.GetBlockWithBytes(int(ref.BlockIdx), wantCols, nil)
			if bErr != nil {
				b.Fatalf("GetBlockWithBytes: %v", bErr)
			}
			blocks[ref.BlockIdx] = bwb
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for _, ref := range refs {
				bwb := blocks[ref.BlockIdx]
				// O(1) column access by row index — no scan, no I/O.
				if col := bwb.Block.GetColumn("span:duration"); col != nil {
					_, _ = col.Uint64Value(int(ref.RowIdx))
				}
				if col := bwb.Block.GetColumn("span:name"); col != nil {
					_, _ = col.StringValue(int(ref.RowIdx))
				}
			}
		}
	})

	// BuildRefMap: measures the cost of constructing a ref→bytes map from an
	// intrinsic column (the pattern used in api.go's buildIntrinsicBytesMap for
	// trace:id and span:id lookups during result materialization).
	//
	// At production scale (2.8M spans, 11 intrinsic columns, 14 files), a 2.8M-entry
	// map is built per-query per-file. This sub-benchmark documents the allocation and
	// CPU cost of that map construction so regressions in map-building overhead are
	// caught before they reach production.
	b.Run("BuildRefMap", func(b *testing.B) {
		col, err := r.GetIntrinsicColumn("trace:id")
		if err != nil || col == nil {
			b.Skip("trace:id intrinsic column not available")
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			m := make(map[uint32][]byte, len(col.BlockRefs))
			for i, ref := range col.BlockRefs {
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
				m[key] = col.BytesValues[i]
			}
			_ = m
		}
	})
}
