package blockio_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	blockpack "github.com/grafana/blockpack"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// allocMemProvider is an in-memory ByteRangeProvider for test readers.
type allocMemProvider struct{ data []byte }

func (m *allocMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *allocMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, fmt.Errorf("ReadAt: offset %d out of range [0, %d]", off, len(m.data))
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// buildAllocTestBlock writes a small blockpack file with a few spans (each with
// several resource and span attributes) and returns a fully parsed Block with
// BuildIterFields() already called.
func buildAllocTestBlock(t *testing.T) *modules_reader.Block {
	t.Helper()

	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 0,
	})
	require.NoError(t, err)

	traceID := [16]byte{0x01, 0x02, 0x03}
	for i := range 5 {
		spanID := make([]byte, 8)
		spanID[0] = byte(i + 1)
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              "op.test",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
			Attributes: []*commonv1.KeyValue{
				{Key: "http.method", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}}},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}}},
				{Key: "span.kind", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "server"}}},
			},
		}
		err = w.AddSpan(
			traceID[:], span,
			map[string]any{"service.name": "alloc-test-svc", "env": "test"},
			"", nil, "",
		)
		require.NoError(t, err)
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&allocMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, 1, r.BlockCount(), "expected 1 block in test file")

	// GetBlockWithBytes calls BuildIterFields internally (added in this PR).
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	return bwb.Block
}

// TestIterateFieldsNoSeenMapAllocsAfterBuildIterFields verifies that IterateFields on a block
// that has called BuildIterFields does not allocate the per-call seen-map.
// NOTE-049: pre-computed column iteration eliminates the per-span make(map[string]struct{}) alloc.
// Remaining allocs are numeric value boxing (int64/uint64 → any); these are separate from
// the seen-map fix and are addressed by a future Approach D change.
func TestIterateFieldsNoSeenMapAllocsAfterBuildIterFields(t *testing.T) {
	block := buildAllocTestBlock(t)
	require.NotNil(t, block.IterFields(), "BuildIterFields must produce non-nil slice")

	// Count allocs with the fast pre-computed path.
	adapter := modules_blockio.NewSpanFieldsAdapter(block, 0)
	defer modules_blockio.ReleaseSpanFieldsAdapter(adapter)

	fastAllocs := testing.AllocsPerRun(100, func() {
		adapter.IterateFields(func(_ string, _ any) bool { return true })
	})

	// The only allocs should be numeric value boxing (int64/uint64 → any),
	// NOT the per-call seen-map. Verify that iterFields is populated (fast path active).
	require.NotNil(t, block.IterFields())

	// No more than one alloc per column — no seen-map overhead.
	// With dual storage, intrinsic columns (trace:id, span:id, span:name, span:kind,
	// span:start, span:end, span:duration) are now in block columns alongside user attrs.
	// Column count: 7 intrinsics + 3 span attrs + 2 resource attrs = ~12 + buffer = ≤14.
	const maxExpected = 14.0
	if fastAllocs > maxExpected {
		t.Errorf(
			"IterateFields after BuildIterFields: got %.1f allocs, want ≤%.0f (seen-map must not allocate)",
			fastAllocs, maxExpected,
		)
	}
}

// TestSpanMatchCloneAllocsPerSpan verifies that Clone allocates significantly fewer
// objects per call after switching from map[string]any to pooled []kvField.
// NOTE-ALLOC-2: map[string]any replaced with pooled []kvField slice.
func TestSpanMatchCloneAllocsPerSpan(t *testing.T) {
	block := buildAllocTestBlock(t)

	adapter := modules_blockio.NewSpanFieldsAdapter(block, 0)
	defer modules_blockio.ReleaseSpanFieldsAdapter(adapter)
	sm := blockpack.SpanMatch{
		Fields:  adapter,
		TraceID: "abc",
		SpanID:  "def",
	}

	// Warm the kvFieldSlicePool with one call.
	warmup := sm.Clone()
	_ = warmup

	allocs := testing.AllocsPerRun(100, func() {
		cloned := sm.Clone()
		_ = cloned
	})
	// After fix: allocs = N_columns (value boxing into any) + 1 (owned []kvField copy)
	// + 1 (materializedSpanFields struct) = ~N+2 allocs.
	// With dual storage, N increased by 7 intrinsic columns now in block payload.
	// Set ceiling at 18 to tolerate minor GC/runtime variation across versions.
	const maxExpected = 18.0
	if allocs > maxExpected {
		t.Errorf("Clone: got %.1f allocs, want ≤%.0f", allocs, maxExpected)
	}
}

// TestNewSpanFieldsAdapterPooled verifies that a pooled get+release round-trip
// does not allocate after the pool is warmed.
// NOTE-ALLOC-4: modulesSpanFieldsAdapter is pooled via sync.Pool.
func TestNewSpanFieldsAdapterPooled(t *testing.T) {
	block := buildAllocTestBlock(t)

	// Warm the pool: get one adapter and return it, so the pool has a free entry.
	warmup := modules_blockio.NewSpanFieldsAdapter(block, 0)
	modules_blockio.ReleaseSpanFieldsAdapter(warmup)

	// NOTE: sync.Pool may drop entries at GC. AllocsPerRun triggers GC between runs,
	// so a strictly-zero assertion would be flaky. Allow ≤1 to tolerate a cold-pool
	// allocation on the first iteration after a GC sweep.
	allocs := testing.AllocsPerRun(100, func() {
		a := modules_blockio.NewSpanFieldsAdapter(block, 0)
		modules_blockio.ReleaseSpanFieldsAdapter(a)
	})
	if allocs > 1 {
		t.Errorf("pooled adapter get+release: got %.1f allocs, want ≤1", allocs)
	}
}

// TestBuildIterFieldsDedupMatchesGetColumn verifies that when multiple columns share
// the same name (different types), BuildIterFields selects the same column as GetColumn,
// ensuring consistent IterateFields semantics regardless of map iteration order.
func TestBuildIterFieldsDedupMatchesGetColumn(t *testing.T) {
	block := buildAllocTestBlock(t)
	entries := block.IterFields()
	require.NotNil(t, entries, "BuildIterFields must be called during parse")

	for _, entry := range entries {
		canonical := block.GetColumn(entry.Name)
		require.NotNil(t, canonical, "GetColumn returned nil for name %q present in iterFields", entry.Name)
		require.Equal(t, canonical, entry.Col,
			"BuildIterFields selected a different *Column for %q than GetColumn (tie-breaking mismatch)", entry.Name)
	}
}

// BenchmarkExtractIDsHex measures the allocation profile of hex.EncodeToString,
// which replaces fmt.Sprintf("%x", v) for trace/span ID encoding.
func BenchmarkExtractIDsHex(b *testing.B) {
	traceIDBytes := make([]byte, 16)
	spanIDBytes := make([]byte, 8)
	for i := range traceIDBytes {
		traceIDBytes[i] = byte(i)
	}
	for i := range spanIDBytes {
		spanIDBytes[i] = byte(i + 16)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = hex.EncodeToString(traceIDBytes)
		_ = hex.EncodeToString(spanIDBytes)
	}
}
