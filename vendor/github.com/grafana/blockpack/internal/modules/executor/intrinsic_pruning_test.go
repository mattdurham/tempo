package executor

import (
	"bytes"
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// bytesProvider is a minimal in-memory ReaderProvider for tests.
type bytesProvider struct{ data []byte }

func (b *bytesProvider) Size() (int64, error) { return int64(len(b.data)), nil }
func (b *bytesProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(b.data)) {
		return 0, bytes.ErrTooLarge
	}
	return copy(p, b.data[off:]), nil
}

func newBytesProvider(data []byte) *bytesProvider { return &bytesProvider{data: data} }

// compileFilter is a test helper that parses and compiles a TraceQL filter string.
func compileFilter(t *testing.T, query string) (*vm.Program, bool) {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	if err != nil {
		t.Logf("skip %q: ParseTraceQL: %v", query, err)
		return nil, false
	}
	filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
	if !ok {
		t.Logf("skip %q: expected FilterExpression, got %T", query, parsed)
		return nil, false
	}
	prog, err := vm.CompileTraceQLFilter(filterExpr)
	if err != nil {
		t.Logf("skip %q: CompileTraceQLFilter: %v", query, err)
		return nil, false
	}
	return prog, true
}

// buildFileWithDurations creates a file where spans have distinct durations across blocks.
// Block 0: 3 spans with durations 100, 200, 300 ns
// Block 1: 3 spans with durations 1000, 2000, 3000 ns
func buildFileWithDurations(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 3})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}

	makeSpan := func(traceID byte, dur uint64) *tracev1.Span {
		return &tracev1.Span{
			TraceId:           bytes.Repeat([]byte{traceID}, 16),
			SpanId:            bytes.Repeat([]byte{traceID + 0x10}, 8),
			Name:              "op",
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   1_000_000_000 + dur,
		}
	}

	for _, sp := range []*tracev1.Span{
		makeSpan(1, 100), makeSpan(2, 200), makeSpan(3, 300), // block 0
		makeSpan(4, 1000), makeSpan(5, 2000), makeSpan(6, 3000), // block 1
	} {
		_ = w.AddSpan(sp.TraceId, sp, nil, "", nil, "")
	}
	_, _ = w.Flush()
	return buf.Bytes()
}

// TestIntrinsicOnlyQueryPrunesBlocks verifies that a query on span:duration
// with the intrinsic section present correctly returns results.
// Query: { span:duration > 500 } — should only match block 1 spans.
func TestIntrinsicOnlyQueryPrunesBlocks(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	prog, ok := compileFilter(t, `{ span:duration > 500 }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	e := New()
	rows, _, err := e.Collect(r, prog, CollectOptions{})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// 3 spans in block 1 match duration > 500.
	if len(rows) != 3 {
		t.Errorf("matched %d rows, want 3", len(rows))
	}
}

// TestBlocksFromIntrinsicTOC_FlatDurationRange verifies that a range predicate on span:duration
// prunes blocks whose max duration falls below the query threshold.
// Block 0: durations [100, 200, 300] ns — all below 500 ns, should be pruned.
// Block 1: durations [1000, 2000, 3000] ns — all above 500 ns, should be kept.
func TestBlocksFromIntrinsicTOC_FlatDurationRange(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	prog, ok := compileFilter(t, `{ span:duration > 500 }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	// Only block 1 has spans with duration > 500ns.
	if len(result) != 1 || result[0] != 1 {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want [1]", result)
	}
}

// TestBlocksFromIntrinsicTOC_FlatDurationRangeAllMatch verifies that when all blocks
// may match (query range overlaps all block values), nil is returned (no pruning).
func TestBlocksFromIntrinsicTOC_FlatDurationRangeAllMatch(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	// duration < 5000ns — spans in both blocks qualify. All blocks survive, so nil is returned.
	prog, ok := compileFilter(t, `{ span:duration < 5000 }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	// All blocks survive pruning — function returns nil (no pruning applied).
	if result != nil {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want nil (no pruning when all blocks match)", result)
	}
}

// TestBlocksFromIntrinsicTOC_FlatDurationNoMatch verifies that when no blocks
// can possibly match, an empty slice is returned.
func TestBlocksFromIntrinsicTOC_FlatDurationNoMatch(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	// duration > 10000ns — no span in the file has duration this large.
	prog, ok := compileFilter(t, `{ span:duration > 10000 }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	if len(result) != 0 {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want [] (no matching blocks)", result)
	}
}

// TestBlocksFromIntrinsicTOC_NonIntrinsicQuery verifies that queries on non-intrinsic
// columns (dynamic user attributes) return nil (no pruning via intrinsic section).
func TestBlocksFromIntrinsicTOC_NonIntrinsicQuery(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}

	// Query references a dynamic attribute — not in the intrinsic column set.
	prog, ok := compileFilter(t, `{ span.http.method = "GET" }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	if result != nil {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want nil for non-intrinsic query", result)
	}
}

// TestBlocksFromIntrinsicTOC_DictServiceName builds a file with service.name values
// in two blocks and verifies that dict-based pruning eliminates the wrong block.
func TestBlocksFromIntrinsicTOC_DictServiceName(t *testing.T) {
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 2})
	if err != nil {
		t.Fatalf("NewWriterWithConfig: %v", err)
	}

	makeSpan := func(traceID byte) *tracev1.Span {
		return &tracev1.Span{
			TraceId:           bytes.Repeat([]byte{traceID}, 16),
			SpanId:            bytes.Repeat([]byte{traceID + 0x10}, 8),
			Name:              "op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
		}
	}

	resourceA := map[string]any{"service.name": "alpha"}
	resourceB := map[string]any{"service.name": "beta"}

	// Block 0: 2 spans with service.name="alpha"
	// Block 1: 2 spans with service.name="beta"
	for _, sp := range []*tracev1.Span{makeSpan(1), makeSpan(2)} {
		_ = w.AddSpan(sp.TraceId, sp, resourceA, "", nil, "")
	}
	for _, sp := range []*tracev1.Span{makeSpan(3), makeSpan(4)} {
		_ = w.AddSpan(sp.TraceId, sp, resourceB, "", nil, "")
	}
	_, _ = w.Flush()

	r, err := reader.NewReaderFromProvider(newBytesProvider(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	prog, ok := compileFilter(t, `{ resource.service.name = "alpha" }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	// Only block 0 has service.name="alpha".
	if len(result) != 1 || result[0] != 0 {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want [0] (only alpha block)", result)
	}
}

// TestBlocksFromIntrinsicTOC_MixedQuery verifies that a mixed query (intrinsic AND
// non-intrinsic columns) still uses the intrinsic column for pruning.
func TestBlocksFromIntrinsicTOC_MixedQuery(t *testing.T) {
	data := buildFileWithDurations(t)
	r, err := reader.NewReaderFromProvider(newBytesProvider(data))
	if err != nil {
		t.Fatalf("NewReaderFromProvider: %v", err)
	}
	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section")
	}

	// Mixed: span:duration is intrinsic (can prune), span.http.method is not (ignored for pruning).
	prog, ok := compileFilter(t, `{ span:duration > 500 && span.http.method = "GET" }`)
	if !ok {
		t.Fatal("failed to compile filter")
	}

	result := BlocksFromIntrinsicTOC(r, prog)
	// The intrinsic predicate prunes block 0; block 1 is kept.
	if len(result) != 1 || result[0] != 1 {
		t.Errorf("BlocksFromIntrinsicTOC = %v, want [1]", result)
	}
}

// TestProgramIsIntrinsicOnly verifies detection of intrinsic-only programs.
func TestProgramIsIntrinsicOnly(t *testing.T) {
	cases := []struct {
		query    string
		wantTrue bool
	}{
		{`{ span:duration > 100ms }`, true},
		{`{ span:name = "foo" }`, true},
		{`{ span:kind = server }`, true},
		{`{ span.http.method = "GET" }`, false},                          // dynamic attribute
		{`{ resource.service.name = "svc" }`, true},                      // practically intrinsic
		{`{ span:duration > 100ms && span.http.method = "GET" }`, false}, // mixed
	}
	for _, tc := range cases {
		prog, ok := compileFilter(t, tc.query)
		if !ok {
			continue
		}
		got := ProgramIsIntrinsicOnly(prog)
		if got != tc.wantTrue {
			t.Errorf("ProgramIsIntrinsicOnly(%q) = %v, want %v", tc.query, got, tc.wantTrue)
		}
	}
}
