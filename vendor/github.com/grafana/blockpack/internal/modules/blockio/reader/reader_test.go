package reader_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// ---- memProvider: in-memory ReaderProvider for tests ----

type memProvider struct{ data []byte }

func (m *memProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *memProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// ---- helpers ----

func mustNewWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *writer.Writer {
	t.Helper()
	cfg := writer.Config{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	}
	w, err := writer.NewWriterWithConfig(cfg)
	require.NoError(t, err)
	return w
}

func flushToBuffer(t *testing.T, _ *bytes.Buffer, w *writer.Writer) {
	t.Helper()
	_, err := w.Flush()
	require.NoError(t, err)
}

func openReader(t *testing.T, data []byte) *reader.Reader {
	t.Helper()
	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	return r
}

func makeSpan(
	traceID [16]byte,
	spanID []byte,
	name string,
	startNano, endNano uint64,
	kind tracev1.Span_SpanKind,
	attrs []*commonv1.KeyValue,
) *tracev1.Span {
	return &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            spanID,
		Name:              name,
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   endNano,
		Kind:              kind,
		Attributes:        attrs,
	}
}

func stringAttr(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

func int64Attr(key string, val int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}},
	}
}

func float64Attr(key string, val float64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: val}},
	}
}

func boolAttr(key string, val bool) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: val}},
	}
}

func bytesAttr(key string, val []byte) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: val}},
	}
}

func addSpanToWriter(
	t *testing.T,
	w *writer.Writer,
	traceID [16]byte,
	span *tracev1.Span,
	resourceAttrs map[string]any,
) {
	t.Helper()
	err := w.AddSpan(traceID[:], span, resourceAttrs, "", nil, "")
	require.NoError(t, err)
}

// fixedSpanID returns a deterministic 8-byte span ID based on the given seed.
func fixedSpanID(seed byte) []byte {
	return []byte{seed, seed + 1, seed + 2, seed + 3, seed + 4, seed + 5, seed + 6, seed + 7}
}

// ---- RT-01: Basic round-trip — single block ----

func TestRoundTrip_Basic(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0) // default block size, all spans fit in one block

	traceID1 := [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	traceID2 := [16]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}

	names := []string{
		"op.alpha", "op.beta", "op.gamma", "op.delta", "op.epsilon",
		"op.zeta", "op.eta", "op.theta", "op.iota", "op.kappa",
	}

	for i := range 10 {
		tid := traceID1
		if i >= 5 {
			tid = traceID2
		}
		span := makeSpan(
			tid,
			fixedSpanID(byte(i)),
			names[i],
			uint64(1_000_000_000+i*1000),
			uint64(1_000_002_000+i*1000),
			tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{stringAttr("http.method", "GET")},
		)
		addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "test-svc"})
	}

	flushToBuffer(t, &buf, w)
	data := buf.Bytes()

	r := openReader(t, data)
	assert.Greater(t, r.BlockCount(), 0, "should have at least one block")

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb)

	// span:name and resource.service.name are now intrinsic-only columns.
	// Verify their presence via the intrinsic section rather than block columns.
	require.True(t, r.HasIntrinsicSection(), "intrinsic section must be present")
	_, hasSpanName := r.IntrinsicColumnMeta("span:name")
	require.True(t, hasSpanName, "span:name must be present in intrinsic section")
	_, hasSvcName := r.IntrinsicColumnMeta("resource.service.name")
	require.True(t, hasSvcName, "resource.service.name must be present in intrinsic section")
}

// ---- RT-02: Empty flush ----

func TestRoundTrip_EmptyFlush(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	flushToBuffer(t, &buf, w)
	data := buf.Bytes()

	assert.NotEmpty(t, data, "empty file must still have footer/header bytes")

	r := openReader(t, data)
	assert.Equal(t, 0, r.BlockCount(), "empty flush must produce zero blocks")
}

// ---- RT-03: Multi-block ----

func TestRoundTrip_MultiBlock(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 2000
	w := mustNewWriter(t, &buf, maxPerBlock)

	const totalSpans = 5000
	traceID := [16]byte{0xAB, 0xCD}

	for i := range totalSpans {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i%256)),
			"op.load",
			uint64(i*1000),
			uint64(i*1000+500),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "bulk-svc"})
	}

	flushToBuffer(t, &buf, w)
	r := openReader(t, buf.Bytes())

	assert.GreaterOrEqual(t, r.BlockCount(), 2, "5000 spans with maxPerBlock=2000 needs at least 2 blocks")

	var total int
	for i := range r.BlockCount() {
		bwb, err := r.GetBlockWithBytes(i, nil, nil)
		require.NoError(t, err)
		total += bwb.Block.SpanCount()
	}
	assert.Equal(t, totalSpans, total, "total span count across all blocks must equal 5000")
}

// ---- RT-04: All column types ----

func TestRoundTrip_AllColumnTypes(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x10}
	span := makeSpan(
		traceID,
		fixedSpanID(1),
		"all-types",
		1_000_000_000,
		2_000_000_000,
		tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{
			stringAttr("custom.str", "hello"),
			int64Attr("custom.int", -42),
			float64Attr("custom.float", 3.14159),
			boolAttr("custom.bool", true),
			bytesAttr("custom.bytes", []byte{0x01, 0x02, 0x03}),
		},
	)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "type-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	blk := bwb.Block

	// String column.
	strCol := blk.GetColumn("span.custom.str")
	require.NotNil(t, strCol, "span.custom.str must be present")
	v, ok := strCol.StringValue(0)
	assert.True(t, ok)
	assert.Equal(t, "hello", v)

	// Int64 column.
	intCol := blk.GetColumn("span.custom.int")
	require.NotNil(t, intCol, "span.custom.int must be present")
	iv, ok := intCol.Int64Value(0)
	assert.True(t, ok)
	assert.Equal(t, int64(-42), iv)

	// Float64 column.
	fltCol := blk.GetColumn("span.custom.float")
	require.NotNil(t, fltCol, "span.custom.float must be present")
	fv, ok := fltCol.Float64Value(0)
	assert.True(t, ok)
	assert.Equal(t, 3.14159, fv)

	// Bool column.
	boolCol := blk.GetColumn("span.custom.bool")
	require.NotNil(t, boolCol, "span.custom.bool must be present")
	bv, ok := boolCol.BoolValue(0)
	assert.True(t, ok)
	assert.True(t, bv)

	// Bytes column.
	bytCol := blk.GetColumn("span.custom.bytes")
	require.NotNil(t, bytCol, "span.custom.bytes must be present")
	byt, ok := bytCol.BytesValue(0)
	assert.True(t, ok)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, byt)
}

// ---- Trace index tests ----

func TestRoundTrip_TraceIndex(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceA := [16]byte{0xAA}
	traceB := [16]byte{0xBB}
	unknownTrace := [16]byte{0xFF}

	for i := range 3 {
		span := makeSpan(traceA, fixedSpanID(byte(i)), "span-a", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceA, span, map[string]any{"service.name": "svc-a"})
	}
	for i := range 2 {
		span := makeSpan(traceB, fixedSpanID(byte(10+i)), "span-b", 3000, 4000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceB, span, map[string]any{"service.name": "svc-b"})
	}

	flushToBuffer(t, &buf, w)
	r := openReader(t, buf.Bytes())

	blocksA := r.BlocksForTraceID(traceA)
	assert.NotEmpty(t, blocksA, "trace A must appear in at least one block")

	blocksB := r.BlocksForTraceID(traceB)
	assert.NotEmpty(t, blocksB, "trace B must appear in at least one block")

	blocksUnknown := r.BlocksForTraceID(unknownTrace)
	assert.Empty(t, blocksUnknown, "unknown trace must return empty")
}

// ---- Provider tracking tests ----

func TestProvider_Tracking(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x11}
	for i := range 5 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "track-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)

	r, err := reader.NewReaderFromProvider(tracker)
	require.NoError(t, err)
	require.Greater(t, r.BlockCount(), 0)

	// Opening the reader already causes footer + header + metadata reads.
	assert.Greater(t, tracker.IOOps(), int64(0), "opening reader must issue I/O")
	assert.Greater(t, tracker.BytesRead(), int64(0), "opening reader must read bytes")

	// Reading a block issues at least one more I/O.
	opsBefore := tracker.IOOps()
	_, err = r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	assert.Greater(t, tracker.IOOps(), opsBefore, "GetBlockWithBytes must issue at least 1 I/O")
}

// ---- RangeCachingProvider test ----

func TestProvider_RangeCaching(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x22}
	for i := range 3 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "cache-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)
	cache := rw.NewRangeCachingProvider(tracker)

	// First read: goes to underlying.
	p1 := make([]byte, 22)
	_, err := cache.ReadAt(p1, int64(len(buf.Bytes())-22), rw.DataTypeFooter)
	require.NoError(t, err)
	opsAfterFirst := tracker.IOOps()
	assert.Equal(t, int64(1), opsAfterFirst)

	// Second read of the same range: served from cache, tracker unchanged.
	p2 := make([]byte, 22)
	_, err = cache.ReadAt(p2, int64(len(buf.Bytes())-22), rw.DataTypeFooter)
	require.NoError(t, err)
	assert.Equal(t, opsAfterFirst, tracker.IOOps(), "second read of cached range must not issue I/O")
	assert.Equal(t, p1, p2, "cached bytes must match original")
}

// ---- CoalesceBlocks tests ----

func TestCoalesceBlocks_Adjacent(t *testing.T) {
	// Build metas for 3 adjacent blocks with no gap.
	metas := []shared.BlockMeta{
		{Offset: 0, Length: 1000},
		{Offset: 1000, Length: 1000},
		{Offset: 2000, Length: 1000},
	}

	blockOrder := []int{0, 1, 2}
	coalesced := reader.CoalesceBlocks(metas, blockOrder, shared.AggressiveCoalesceConfig)

	assert.Equal(t, 1, len(coalesced), "adjacent blocks must coalesce into a single read")
	assert.Equal(t, 3, len(coalesced[0].BlockIDs), "merged read must contain all 3 block IDs")
}

func TestCoalesceBlocks_LargeGap(t *testing.T) {
	const tenMB = 10 * 1024 * 1024
	metas := []shared.BlockMeta{
		{Offset: 0, Length: 1000},
		{Offset: tenMB, Length: 1000},
	}

	blockOrder := []int{0, 1}
	coalesced := reader.CoalesceBlocks(metas, blockOrder, shared.AggressiveCoalesceConfig)

	assert.Equal(t, 2, len(coalesced), "blocks 10 MB apart must not be coalesced (gap > 4 MB limit)")
}

func TestCoalesceBlocks_MaxReadBytesLimitsCoalesce(t *testing.T) {
	// 4 adjacent 3 MB blocks.  AggressiveCoalesceConfig caps at 8 MB, so the
	// first two blocks (6 MB) coalesce but the third would push the total to
	// 9 MB and must start a new read.
	const threeMB = 3 * 1024 * 1024
	metas := []shared.BlockMeta{
		{Offset: 0, Length: threeMB},
		{Offset: threeMB, Length: threeMB},
		{Offset: 2 * threeMB, Length: threeMB},
		{Offset: 3 * threeMB, Length: threeMB},
	}

	coalesced := reader.CoalesceBlocks(metas, []int{0, 1, 2, 3}, shared.AggressiveCoalesceConfig)

	require.Equal(t, 2, len(coalesced), "4×3 MB blocks must split into two 6 MB reads under the 8 MB cap")
	assert.Equal(t, int64(2*threeMB), coalesced[0].Length)
	assert.Equal(t, int64(2*threeMB), coalesced[1].Length)
}

func TestCoalesceBlocks_OversizedBlockReadWhole(t *testing.T) {
	// A single block larger than MaxReadBytes must be read whole; the cap
	// must not prevent the next block from starting its own read.
	const tenMB = 10 * 1024 * 1024
	const oneMB = 1 * 1024 * 1024
	metas := []shared.BlockMeta{
		{Offset: 0, Length: tenMB},     // exceeds 8 MB cap alone
		{Offset: tenMB, Length: oneMB}, // must start a fresh read
	}

	coalesced := reader.CoalesceBlocks(metas, []int{0, 1}, shared.AggressiveCoalesceConfig)

	require.Equal(t, 2, len(coalesced), "oversized block must not pull adjacent block into the same read")
	assert.Equal(t, int64(tenMB), coalesced[0].Length)
	assert.Equal(t, int64(oneMB), coalesced[1].Length)
}

// ---- GetBlockWithBytes single I/O test ----

func TestGetBlockWithBytes_SingleIO(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x33}
	for i := range 10 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "io-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)

	r, err := reader.NewReaderFromProvider(tracker)
	require.NoError(t, err)
	require.Equal(t, 1, r.BlockCount())

	// Reset tracking after open so we measure only GetBlockWithBytes.
	tracker.Reset()

	_, err = r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, int64(1), tracker.IOOps(), "GetBlockWithBytes must issue exactly 1 I/O for the block data")
}

// ---- AddColumnsToBlock incremental load ----

func TestAddColumnsToBlock(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x44}
	span := makeSpan(
		traceID,
		fixedSpanID(1),
		"op",
		1000, 2000,
		tracev1.Span_SPAN_KIND_INTERNAL,
		[]*commonv1.KeyValue{
			stringAttr("col.a", "val-a"),
			stringAttr("col.b", "val-b"),
			stringAttr("col.c", "val-c"),
		},
	)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "add-col-svc"})
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)
	r, err := reader.NewReaderFromProvider(tracker)
	require.NoError(t, err)

	// Request only "span.col.a".
	wantA := map[string]struct{}{"span.col.a": {}}
	bwb, err := r.GetBlockWithBytes(0, wantA, nil)
	require.NoError(t, err)
	require.NotNil(t, bwb.Block.GetColumn("span.col.a"))
	// With lazy registration, col.b is present (lazily registered) but not yet fully decoded.
	require.NotNil(t, bwb.Block.GetColumn("span.col.b"), "col.b must be lazily registered")

	// Reset tracker to verify no new I/O on AddColumnsToBlock.
	tracker.Reset()

	addWant := map[string]struct{}{"span.col.b": {}, "span.col.c": {}}
	err = r.AddColumnsToBlock(bwb, addWant)
	require.NoError(t, err)

	assert.NotNil(t, bwb.Block.GetColumn("span.col.b"), "col.b must be present after add")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.c"), "col.c must be present after add")
	assert.Equal(t, int64(0), tracker.IOOps(), "AddColumnsToBlock must not issue any I/O")
}

// ---- Block reuse test (READ-06) ----

func TestBlockReuse(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 5
	w := mustNewWriter(t, &buf, maxPerBlock)

	traceID := [16]byte{0x55}
	for i := range 10 {
		name := "op-one"
		if i >= maxPerBlock {
			name = "op-two"
		}
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			name,
			uint64(i*1000),
			uint64(i*1000+500),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "reuse-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 2, r.BlockCount())

	// span:name is now stored in the intrinsic section only, not in block columns.
	// Verify span:name values for each block via the intrinsic section.
	require.True(t, r.HasIntrinsicSection(), "intrinsic section must be present")

	// Block 0 should have "op-one" spans; block 1 should have "op-two" spans.
	// Verify at least one span:name is resolvable in each block.
	name0, ok0 := r.IntrinsicDictStringAt("span:name", 0, 0)
	assert.True(t, ok0, "span:name at block 0 row 0 must be present")
	assert.Equal(t, "op-one", name0, "block 0 must contain op-one spans")

	name1, ok1 := r.IntrinsicDictStringAt("span:name", 1, 0)
	assert.True(t, ok1, "span:name at block 1 row 0 must be present")
	assert.Equal(t, "op-two", name1, "block 1 must contain op-two spans")
}

// ---- Column filtering: only requested columns decoded ----

func TestGetBlockWithBytes_ColumnFiltering(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x66}
	attrs := make([]*commonv1.KeyValue, 0, 5)
	for i := range 5 {
		attrs = append(attrs, stringAttr("col."+string(rune('a'+i)), "v"))
	}
	span := makeSpan(traceID, fixedSpanID(1), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, attrs)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "filter-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())

	// Request only 3 attribute columns.
	want := map[string]struct{}{
		"span.col.a": {},
		"span.col.b": {},
		"span.col.c": {},
	}
	bwb, err := r.GetBlockWithBytes(0, want, nil)
	require.NoError(t, err)

	assert.NotNil(t, bwb.Block.GetColumn("span.col.a"), "requested column must be present")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.b"), "requested column must be present")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.c"), "requested column must be present")
	// With lazy registration, unrequested columns are lazily present (not absent).
	assert.NotNil(t, bwb.Block.GetColumn("span.col.d"), "unrequested column lazily registered")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.e"), "unrequested column lazily registered")
}

// ---- Full block read: nil want returns all columns ----

func TestGetBlockWithBytes_NilWant(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x77}
	span := makeSpan(
		traceID, fixedSpanID(1), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL,
		[]*commonv1.KeyValue{
			stringAttr("colx.1", "v1"),
			stringAttr("colx.2", "v2"),
			stringAttr("colx.3", "v3"),
		},
	)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "full-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	// All 3 attribute columns must be present as block columns.
	assert.NotNil(t, bwb.Block.GetColumn("span.colx.1"))
	assert.NotNil(t, bwb.Block.GetColumn("span.colx.2"))
	assert.NotNil(t, bwb.Block.GetColumn("span.colx.3"))
	// span:name is now an intrinsic-only column — verify via intrinsic section.
	_, hasSpanName := r.IntrinsicColumnMeta("span:name")
	assert.True(t, hasSpanName, "span:name must be present in intrinsic section")
}

// ---- Block meta tests ----

func TestBlockMeta_Timestamps(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0x88}
	starts := []uint64{100, 300, 200, 500, 400}
	for i, st := range starts {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", st, st+100, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "meta-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	meta := r.BlockMeta(0)
	assert.Equal(t, uint64(100), meta.MinStart)
	assert.Equal(t, uint64(500), meta.MaxStart)
}

func TestBlockMeta_TraceIDRange(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traces := [][16]byte{
		{0x10, 0x00},
		{0x30, 0x00},
		{0x20, 0x00},
	}
	for i, tid := range traces {
		span := makeSpan(tid, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "range-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	meta := r.BlockMeta(0)
	// V13+ block index no longer stores MinTraceID/MaxTraceID — always zero.
	assert.Equal(t, byte(0x00), meta.MinTraceID[0])
	assert.Equal(t, byte(0x00), meta.MaxTraceID[0])
}

// ---- TBI tests: trace block index ----

func TestTraceBlockIndex_SingleBlock(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xA1}
	for i := range 3 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "tbi-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	blocks := r.BlocksForTraceID(traceID)
	require.NotEmpty(t, blocks)
	assert.Contains(t, blocks, 0)
}

func TestTraceBlockIndex_SplitBlocks(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 3
	w := mustNewWriter(t, &buf, maxPerBlock)

	traceID := [16]byte{0xA2}
	for i := range 6 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op",
			uint64(i*1000),
			uint64(i*1000+500),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "split-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 2, r.BlockCount())

	blocks := r.BlocksForTraceID(traceID)
	assert.Equal(t, 2, len(blocks), "trace split across 2 blocks must appear in 2 blocks")
}

func TestTraceBlockIndex_MultipleTraces(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	tid1 := [16]byte{0xB1}
	tid2 := [16]byte{0xB2}
	tid3 := [16]byte{0xB3}
	unknown := [16]byte{0xFF}

	for i := range 2 {
		span := makeSpan(tid1, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, tid1, span, map[string]any{"service.name": "multi-svc"})
	}
	for i := range 2 {
		span := makeSpan(tid2, fixedSpanID(byte(10+i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, tid2, span, map[string]any{"service.name": "multi-svc"})
	}
	for i := range 2 {
		span := makeSpan(tid3, fixedSpanID(byte(20+i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, tid3, span, map[string]any{"service.name": "multi-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())

	assert.NotEmpty(t, r.BlocksForTraceID(tid1))
	assert.NotEmpty(t, r.BlocksForTraceID(tid2))
	assert.NotEmpty(t, r.BlocksForTraceID(tid3))
	assert.Empty(t, r.BlocksForTraceID(unknown))
}

// ---- Intrinsic fields round-trip ----

func TestRoundTrip_IntrinsicFields(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
	}
	spanID := []byte{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8}
	parentID := []byte{0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8}

	span := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            spanID,
		ParentSpanId:      parentID,
		Name:              "my.operation",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Status: &tracev1.Status{
			Code:    tracev1.Status_STATUS_CODE_OK,
			Message: "all good",
		},
	}
	err := w.AddSpan(traceID[:], span, map[string]any{"service.name": "intrinsic-svc"}, "", nil, "")
	require.NoError(t, err)
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	// All intrinsic columns are now stored in the intrinsic section only.
	require.True(t, r.HasIntrinsicSection(), "intrinsic section must be present")

	// span:name (dict string)
	name, ok := r.IntrinsicDictStringAt("span:name", 0, 0)
	assert.True(t, ok, "span:name must be present in intrinsic section")
	assert.Equal(t, "my.operation", name)

	// span:start (flat uint64)
	st, ok := r.IntrinsicUint64At("span:start", 0, 0)
	assert.True(t, ok, "span:start must be present in intrinsic section")
	assert.Equal(t, uint64(1_000_000_000), st)

	// span:end (synthesized: start + duration)
	et, ok := r.IntrinsicUint64At("span:end", 0, 0)
	assert.True(t, ok, "span:end must be present in intrinsic section")
	assert.Equal(t, uint64(2_000_000_000), et)

	// span:duration = end - start = 1_000_000_000 (flat uint64)
	dur, ok := r.IntrinsicUint64At("span:duration", 0, 0)
	assert.True(t, ok, "span:duration must be present in intrinsic section")
	assert.Equal(t, uint64(1_000_000_000), dur)

	// span:kind (dict int64)
	kind, ok := r.IntrinsicDictInt64At("span:kind", 0, 0)
	assert.True(t, ok, "span:kind must be present in intrinsic section")
	assert.Equal(t, int64(tracev1.Span_SPAN_KIND_SERVER), kind)

	// span:status (dict int64)
	sc, ok := r.IntrinsicDictInt64At("span:status", 0, 0)
	assert.True(t, ok, "span:status must be present in intrinsic section")
	assert.Equal(t, int64(tracev1.Status_STATUS_CODE_OK), sc)

	// span:status_message (dict string)
	sm, ok := r.IntrinsicDictStringAt("span:status_message", 0, 0)
	assert.True(t, ok, "span:status_message must be present in intrinsic section")
	assert.Equal(t, "all good", sm)

	// trace:id (flat bytes)
	tidVal, ok := r.IntrinsicBytesAt("trace:id", 0, 0)
	assert.True(t, ok, "trace:id must be present in intrinsic section")
	assert.Equal(t, traceID[:], tidVal)

	// span:id (flat bytes)
	sidVal, ok := r.IntrinsicBytesAt("span:id", 0, 0)
	assert.True(t, ok, "span:id must be present in intrinsic section")
	assert.Equal(t, spanID, sidVal)

	// span:parent_id (flat bytes)
	pidVal, ok := r.IntrinsicBytesAt("span:parent_id", 0, 0)
	assert.True(t, ok, "span:parent_id must be present in intrinsic section")
	assert.Equal(t, parentID, pidVal)
}

// ---- Null / absent values (RT-04) ----

func TestRoundTrip_NullValues(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xCC}
	for i := range 5 {
		attrs := []*commonv1.KeyValue(nil)
		if i%2 == 0 {
			attrs = []*commonv1.KeyValue{stringAttr("sparse.attr", "value")}
		}
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op",
			uint64(i*1000),
			uint64(i*1000+100),
			tracev1.Span_SPAN_KIND_INTERNAL,
			attrs,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "null-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	blk := bwb.Block

	col := blk.GetColumn("span.sparse.attr")
	require.NotNil(t, col, "span.sparse.attr must be present")

	// The writer sorts spans, so we cannot rely on row order matching insertion order.
	// Instead check that exactly 3 rows are present (rows for i=0,2,4).
	presentCount := 0
	for i := range blk.SpanCount() {
		_, ok := col.StringValue(i)
		if ok {
			presentCount++
		}
	}
	assert.Equal(t, 3, presentCount, "exactly 3 rows should have the sparse attribute")
}

// ---- All-null column (RT-05) ----

func TestRoundTrip_AllNullColumn(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xDD}
	for i := range 3 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "absent-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	// Column that was never added must be absent.
	assert.Nil(t, bwb.Block.GetColumn("absent.attr"), "column never written must be nil")
}

// ---- Range index (BlocksForRange) tests ----

// encodeInt64Key returns the 8-byte LE encoding of an int64 value as a string key,
// matching the boundary key format used by the range index (SPECS §5.2.1).
func encodeInt64Key(v int64) string {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
	return string(tmp[:])
}

// encodeUint64Key returns the 8-byte LE encoding of a uint64 value as a string key.
func encodeUint64Key(v uint64) string {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return string(tmp[:])
}

// TestBlocksForRange_Int64 verifies bucket-range coverage for an int64 attribute.
// Four spans with values 10, 20, 30, 40 each become their own block (maxBlockSpans=1).
// With 4 unique samples KLL returns boundaries [10, 20, 30, 40] → 3 buckets:
//
//	bucket 0: lower=enc(10), value 10  (10 < 20)
//	bucket 1: lower=enc(20), value 20  (20 < 30)
//	bucket 2: lower=enc(30), values 30 and 40  (boundary value maps to last bucket = len-2)
func TestBlocksForRange_Int64(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 1) // maxBlockSpans=1 → one span per block

	traceID := [16]byte{0xE0}
	for i, val := range []int64{10, 20, 30, 40} {
		span := makeSpan(
			traceID, fixedSpanID(byte(i)), "op",
			uint64(1000+i*1000), uint64(2000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL,
			[]*commonv1.KeyValue{int64Attr("ded.num", val)},
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "ded-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 4, r.BlockCount())

	// With boundaries [10,20,30,40]: bucket0=[10,20), bucket1=[20,30), bucket2=[30,∞).
	// Range lookup: pass the actual query value; reader finds the correct bucket.
	bucket0, err := r.BlocksForRange("span.ded.num", encodeInt64Key(10))
	require.NoError(t, err)
	bucket1, err := r.BlocksForRange("span.ded.num", encodeInt64Key(20))
	require.NoError(t, err)
	bucket2, err := r.BlocksForRange("span.ded.num", encodeInt64Key(30))
	require.NoError(t, err)
	// Value 40 is also in [30,∞) — should return the same 2 blocks as bucket2.
	bucket2also, err := r.BlocksForRange("span.ded.num", encodeInt64Key(40))
	require.NoError(t, err)
	// Value 5 is below all lower boundaries — should return nil.
	belowAll, err := r.BlocksForRange("span.ded.num", encodeInt64Key(5))
	require.NoError(t, err)

	// NOTE-38: With exact-value index (cardinality=4 ≤ threshold), each value
	// gets its own entry — zero false positives. Each lookup returns exactly 1 block.
	assert.Len(t, bucket0, 1, "value=10 must return exactly 1 block")
	assert.Len(t, bucket1, 1, "value=20 must return exactly 1 block")
	assert.Len(t, bucket2, 1, "value=30 must return exactly 1 block")
	assert.Len(t, bucket2also, 1, "value=40 must return exactly 1 block")
	assert.Nil(t, belowAll, "value below all boundaries must return nil")

	// All 4 blocks appear exactly once across all four lookups.
	all := append(append(append(bucket0, bucket1...), bucket2...), bucket2also...)
	require.Len(t, all, 4)
	seen := make(map[int]struct{}, 4)
	for _, id := range all {
		seen[id] = struct{}{}
	}
	assert.Len(t, seen, 4, "all 4 block IDs must be distinct")
}

// TestBlocksForRange_Bool_NotIndexed verifies that Bool columns are excluded
// from the range index (no RangeBool type exists).
func TestBlocksForRange_Bool_NotIndexed(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xE1}
	span := makeSpan(
		traceID, fixedSpanID(1), "op", 1000, 2000,
		tracev1.Span_SPAN_KIND_INTERNAL,
		[]*commonv1.KeyValue{boolAttr("flag.ok", true)},
	)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "bool-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	_, err := r.BlocksForRange("span.flag.ok", "\x00")
	assert.Error(t, err, "Bool column must not appear in the range index")
}

// TestBlocksForRange_TraceID_NotIndexed verifies that trace:id is excluded
// from the range index even though it is a data column.
func TestBlocksForRange_TraceID_NotIndexed(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xE2, 0x01}
	span := makeSpan(
		traceID, fixedSpanID(1), "op", 1000, 2000,
		tracev1.Span_SPAN_KIND_INTERNAL, nil,
	)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "tid-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	_, err := r.BlocksForRange("trace:id", "\x00\x00")
	assert.Error(t, err, "trace:id must not appear in the range index")
}

// TestBlocksForRange_StringColumn verifies that string span attributes are
// range-bucketed in the range index using boundary-prefix keys (SPECS §5.2.1).
// Three distinct values "dev", "prod", "staging" → sorted boundaries ["dev","prod","staging"],
// 2 buckets whose keys are the lower boundary of each bucket range:
//
//	key "dev":  bucket 0 — contains "dev"
//	key "prod": bucket 1 — contains "prod" and "staging" (boundary value → last bucket)
func TestBlocksForRange_StringColumn(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xE3}
	for i, env := range []string{"prod", "staging", "dev"} {
		span := makeSpan(
			traceID, fixedSpanID(byte(i)), "op",
			uint64(1000+i*1000), uint64(2000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL,
			[]*commonv1.KeyValue{stringAttr("deploy.env", env)},
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "str-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	// Range lookup: pass the actual query value; reader finds the correct bucket.
	// Boundaries ["dev","prod","staging"] → buckets: ["dev","prod") and ["prod",∞).
	bucketDev, err := r.BlocksForRange("span.deploy.env", "dev")
	require.NoError(t, err)
	bucketProd, err := r.BlocksForRange("span.deploy.env", "prod")
	require.NoError(t, err)
	// "staging" sorts into ["prod",∞) — same bucket as "prod".
	bucketStaging, err := r.BlocksForRange("span.deploy.env", "staging")
	require.NoError(t, err)
	// "aaa" < "dev" — below all lower boundaries.
	belowAll, err := r.BlocksForRange("span.deploy.env", "aaa")
	require.NoError(t, err)

	assert.Contains(t, bucketDev, 0, "bucket ['dev','prod') must contain block 0")
	assert.Contains(t, bucketProd, 0, "bucket ['prod',∞) must contain block 0 (values=prod,staging)")
	assert.Contains(t, bucketStaging, 0, "'staging' sorts into ['prod',∞) and must return block 0")
	assert.Nil(t, belowAll, "'aaa' is below all lower boundaries, must return nil")
}

// TestBlocksForRange_UnknownColumn verifies that querying a column that was
// never written returns an error.
func TestBlocksForRange_UnknownColumn(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xE4}
	span := makeSpan(traceID, fixedSpanID(1), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "unk-svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	_, err := r.BlocksForRange("span.unknown.attr", "\x00\x00")
	assert.Error(t, err, "unknown column must return an error")
}

// TestBlocksForRange_IntrinsicColumns verifies that span intrinsics
// (span:start, span:duration, span:name) appear in the range index.
// Two spans have distinct names and distinct timestamps ensuring 2-sample KLL sketches
// that produce range buckets rather than exact-value fallback.
func TestBlocksForRange_IntrinsicColumns(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xE5}
	// Use distinct names and distinct durations so each KLL sketch has ≥2 unique samples.
	for i, name := range []string{"op.alpha", "op.beta"} {
		span := makeSpan(
			traceID, fixedSpanID(byte(i)), name,
			uint64(1000+i*5000), uint64(2000+i*5000+i*3000),
			tracev1.Span_SPAN_KIND_INTERNAL, nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "intr-svc"})
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	// span:start is a uint64 intrinsic — must be in the range index.
	// KLL bounds for values [1000, 6000] → 1 bucket with lower boundary key = enc(1000).
	startBlocks, err := r.BlocksForRange("span:start", encodeUint64Key(1000))
	require.NoError(t, err, "span:start must be in the range index")
	assert.Contains(t, startBlocks, 0, "span:start bucket 0 must include block 0")

	// span:name is a string intrinsic — must be in the range index.
	// With 2 distinct names ["op.alpha","op.beta"] there is 1 bucket: ["op.alpha",∞).
	// Both "op.alpha" and "op.beta" resolve to this bucket via range lookup.
	nameBlocksAlpha, err := r.BlocksForRange("span:name", "op.alpha")
	require.NoError(t, err, "span:name must be in the range index")
	nameBlocksBeta, err := r.BlocksForRange("span:name", "op.beta")
	require.NoError(t, err)
	// "abc" < "op.alpha" — below all lower boundaries.
	nameBlocksBelow, err := r.BlocksForRange("span:name", "abc")
	require.NoError(t, err)
	assert.Contains(t, nameBlocksAlpha, 0, "span:name 'op.alpha' must include block 0")
	assert.Contains(t, nameBlocksBeta, 0, "'op.beta' sorts into ['op.alpha',∞) and must include block 0")
	assert.Nil(t, nameBlocksBelow, "'abc' is below all boundaries, must return nil")
}

// ---- Range interval (BlocksForRangeInterval) tests ----

// TestBlocksForRangeInterval_StringColumn verifies that BlocksForRangeInterval
// returns blocks from all buckets whose lower boundary falls within [min, max].
// NOTE-011: interval matching for case-insensitive regex prefix lookups.
func TestBlocksForRangeInterval_StringColumn(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 5) // 5 spans per block

	// Write 3 blocks with distinct service names so each gets its own bucket.
	services := []string{"alpha-svc", "beta-svc", "gamma-svc"}
	for blk, svc := range services {
		for i := range 5 {
			idx := blk*5 + i
			traceID := [16]byte{0xE5, byte(idx)}
			span := makeSpan(
				traceID, fixedSpanID(byte(idx)), "op",
				uint64(1000+idx*1000), uint64(2000+idx*1000),
				tracev1.Span_SPAN_KIND_INTERNAL, nil,
			)
			addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": svc})
		}
	}
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 2, "need multiple blocks for interval test")

	_, hasIndex := r.RangeColumnType("resource.service.name")
	if !hasIndex {
		t.Skip("range index not present for resource.service.name")
	}

	// Interval [alpha, beta\xff]: should include alpha-svc and beta-svc buckets, not gamma-svc.
	blocks, err := r.BlocksForRangeInterval("resource.service.name", "alpha", "beta\xff")
	require.NoError(t, err)
	require.NotEmpty(t, blocks, "interval must return at least one block")

	// Interval covering everything: [a, z].
	allBlocks, err := r.BlocksForRangeInterval("resource.service.name", "a", "z")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(allBlocks), len(blocks),
		"wider interval must return at least as many blocks")

	// Interval below all boundaries: should return nil or empty.
	none, err := r.BlocksForRangeInterval("resource.service.name", "000", "111")
	require.NoError(t, err)
	assert.Empty(t, none, "interval below all boundaries must return empty")
}

// ---- Lean reader (NewLeanReaderFromProvider) tests ----

// TestLeanReader_ThreeIO verifies that NewLeanReaderFromProvider opens a V14 file with
// the expected I/O operations: footer + section directory + 6 section reads.
func TestLeanReader_ThreeIO(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xF1, 0x00}
	for i := range 3 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op",
			uint64(1000+i*1000),
			uint64(2000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "lean-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)

	r, err := reader.NewLeanReaderFromProvider(tracker)
	require.NoError(t, err)

	// Writer produces V14 (FooterV7) files. NewLeanReaderFromProvider reads:
	// 1 (footer) + 1 (section directory) + 1 (block_index, the only section loaded eagerly)
	// + M peek reads per name-keyed intrinsic column blob to populate IntrinsicColMeta at open.
	// The remaining sections (trace_index, range_index, ts_index, sketch_index, file_bloom)
	// are loaded lazily on first access.
	openOps := tracker.IOOps()
	assert.GreaterOrEqual(
		t,
		openOps,
		int64(3),
		"NewLeanReaderFromProvider: must read at least 3 I/Os for V14 footer+dir+block_index",
	)

	// TraceEntries triggers lazy loading of the trace index section (1 additional I/O).
	entries := r.TraceEntries(traceID)
	assert.NotEmpty(t, entries, "lean reader must return trace entries via trace index section")
	assert.Equal(t, openOps+1, tracker.IOOps(), "TraceEntries: exactly 1 additional I/O to load trace index section")

	// A second TraceEntries call must NOT trigger additional I/O (trace index already loaded).
	_ = r.TraceEntries(traceID)
	assert.Equal(t, openOps+1, tracker.IOOps(), "second TraceEntries: no additional I/O (trace index already loaded)")
}

// TestLeanReader_LazyTraceIndex verifies that after the trace index is loaded (1 I/O),
// a bloom-miss lookup does NOT trigger any further I/O.
func TestLeanReader_LazyTraceIndex(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	traceID := [16]byte{0xF1, 0x10}
	for i := range 2 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op",
			uint64(1000+i*100),
			uint64(2000+i*100),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "lazy-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	tracker := rw.NewTrackingReaderProvider(mem)

	r, err := reader.NewLeanReaderFromProvider(tracker)
	require.NoError(t, err)

	// For V14, the trace index section is loaded lazily on first access. Trigger it now.
	absent := [16]byte{0xDE, 0xAD, 0xBE, 0xEF}
	blocks := r.BlocksForTraceID(absent)
	assert.Nil(t, blocks, "absent trace ID must return nil")

	// After the trace index is loaded, a second bloom-miss lookup must not trigger any
	// additional I/O (the trace section is already loaded via sync.Once).
	afterFirstOps := tracker.IOOps()
	blocks2 := r.BlocksForTraceID(absent)
	assert.Nil(t, blocks2, "absent trace ID must still return nil")
	assert.Equal(t, afterFirstOps, tracker.IOOps(), "second bloom miss must not trigger additional I/O")
}

// TestLeanReader_TraceEntriesAndBlock verifies that a lean reader can look up
// trace entries and read a block correctly from the compact block table.
func TestLeanReader_TraceEntriesAndBlock(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 2
	w := mustNewWriter(t, &buf, maxPerBlock)

	traceA := [16]byte{0xF2, 0x00}
	traceB := [16]byte{0xF3, 0x00}
	unknown := [16]byte{0xFF, 0xFF}

	for i := range 4 {
		span := makeSpan(
			traceA,
			fixedSpanID(byte(i)),
			"op-a",
			uint64(1000+i*100),
			uint64(2000+i*100),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceA, span, map[string]any{"service.name": "lean-svc"})
	}
	for i := range 2 {
		span := makeSpan(
			traceB,
			fixedSpanID(byte(10+i)),
			"op-b",
			uint64(5000+i*100),
			uint64(6000+i*100),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceB, span, map[string]any{"service.name": "lean-svc"})
	}
	flushToBuffer(t, &buf, w)

	mem := &memProvider{data: buf.Bytes()}
	r, err := reader.NewLeanReaderFromProvider(mem)
	require.NoError(t, err)

	// Trace A spans 2 blocks, trace B in 1 block.
	entriesA := r.TraceEntries(traceA)
	assert.GreaterOrEqual(t, len(entriesA), 1, "traceA must have at least one entry")

	entriesB := r.TraceEntries(traceB)
	assert.NotEmpty(t, entriesB, "traceB must have entries")

	assert.Nil(t, r.TraceEntries(unknown), "unknown trace must return nil")

	// Verify GetBlockWithBytes works for lean reader blocks.
	for _, e := range entriesA {
		bwb, err := r.GetBlockWithBytes(e.BlockID, nil, nil)
		require.NoError(t, err, "GetBlockWithBytes must work for lean reader block %d", e.BlockID)
		require.NotNil(t, bwb)
		assert.Greater(t, bwb.Block.SpanCount(), 0)
	}
}

// TestLeanReader_CompactTraceIndex verifies that the compact trace index is
// consistent with the main trace index from a full reader.
func TestLeanReader_CompactTraceIndex(t *testing.T) {
	var buf bytes.Buffer
	const maxPerBlock = 3
	w := mustNewWriter(t, &buf, maxPerBlock)

	traceID := [16]byte{0xF4}
	for i := range 6 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			"op",
			uint64(i*1000),
			uint64(i*1000+500),
			tracev1.Span_SPAN_KIND_INTERNAL,
			nil,
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "cmp-svc"})
	}
	flushToBuffer(t, &buf, w)

	data := buf.Bytes()

	// Full reader.
	full := openReader(t, data)
	fullEntries := full.TraceEntries(traceID)

	// Lean reader.
	lean, err := reader.NewLeanReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	leanEntries := lean.TraceEntries(traceID)

	require.Len(t, leanEntries, len(fullEntries), "lean and full readers must return the same number of trace entries")

	// Both readers must agree on which blocks contain the trace.
	fullBlocks := make(map[int]struct{})
	for _, e := range fullEntries {
		fullBlocks[e.BlockID] = struct{}{}
	}
	for _, e := range leanEntries {
		_, ok := fullBlocks[e.BlockID]
		require.True(t, ok, "lean entry block %d not found in full reader", e.BlockID)
	}
}

// ---- RT-MC-01: Metadata compression round-trip (V12) ----

func TestRoundTrip_MetadataCompression(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 5) // MaxBlockSpans=5, forces 3 blocks for 13 spans

	traceID := [16]byte{0xAA, 0xBB, 0xCC, 0xDD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}
	for i := range 13 {
		span := makeSpan(
			traceID,
			fixedSpanID(byte(i)),
			fmt.Sprintf("op.%d", i),
			uint64(1_000_000_000+i*1_000_000),
			uint64(1_001_000_000+i*1_000_000),
			tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{
				stringAttr("http.method", "GET"),
				int64Attr("http.status_code", int64(200+i%4)),
			},
		)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "svc-test"})
	}

	_, err := w.Flush()
	require.NoError(t, err)

	data := buf.Bytes()

	r := openReader(t, data)
	assert.Equal(t, 3, r.BlockCount(), "expected 3 blocks for 13 spans with MaxBlockSpans=5")

	// Verify trace block index round-tripped: traceID appears in all 3 blocks.
	entries := r.TraceEntries(traceID)
	assert.Equal(t, 3, len(entries), "expected traceID to appear in all 3 blocks")

	// Verify block metadata round-tripped: check MinStart/MaxStart of first block.
	meta0 := r.BlockMeta(0)
	assert.Greater(t, meta0.MaxStart, meta0.MinStart, "MaxStart must exceed MinStart")
}

// ---- Security: decompression-bomb guard ----

func TestDecompressionBombGuard(t *testing.T) {
	// Craft a snappy payload whose varint header claims a decoded length that
	// exceeds MaxMetadataSize, then verify the reader rejects it before allocating.
	//
	// The snappy block format stores the uncompressed length as a little-endian
	// varint prefix. binary.AppendUvarint encodes in the same format snappy expects.
	fakeHeader := binary.AppendUvarint(nil, shared.MaxMetadataSize+1)
	// Append a single zero byte so the slice is non-empty after the varint.
	craftedPayload := append(fakeHeader, 0x00)

	t.Run("V3_metadata", func(t *testing.T) {
		// Build a minimal V3 bomb file from scratch (no dependency on writer output format).
		// Layout: [metadata blob] [file header (22 bytes)] [V3 footer (22 bytes)]
		// The compact index is empty (compactLen=0), which is valid for V3 format.
		out := make([]byte, 0, len(craftedPayload)+44)

		metadataOffset := uint64(len(out))
		out = append(out, craftedPayload...)

		headerOffset := uint64(len(out))
		var hdrBuf [22]byte
		binary.LittleEndian.PutUint32(hdrBuf[0:], shared.MagicNumber)
		hdrBuf[4] = shared.VersionV13
		binary.LittleEndian.PutUint64(hdrBuf[5:], metadataOffset)
		binary.LittleEndian.PutUint64(hdrBuf[13:], uint64(len(craftedPayload)))
		hdrBuf[21] = shared.SignalTypeTrace
		out = append(out, hdrBuf[:]...)

		compactOffset := uint64(len(out)) // empty compact index at end of header
		var footerBuf [22]byte
		binary.LittleEndian.PutUint16(footerBuf[0:], shared.FooterV3Version)
		binary.LittleEndian.PutUint64(footerBuf[2:], headerOffset)
		binary.LittleEndian.PutUint64(footerBuf[10:], compactOffset)
		binary.LittleEndian.PutUint32(footerBuf[18:], 0) // compactLen=0
		out = append(out, footerBuf[:]...)

		_, openErr := reader.NewReaderFromProvider(&memProvider{data: out})
		require.Error(t, openErr)
		assert.Contains(t, openErr.Error(), "snappy decoded size",
			"expected decompression-bomb guard error, got: %v", openErr)
	})

	t.Run("V14_section_directory", func(t *testing.T) {
		// Build a minimal V14 bomb file: crafted section directory + V7 footer.
		// Layout: [crafted_dir] [V7 footer (18 bytes)]
		// The crafted payload IS the section directory — the reader will reject
		// it before parsing entries because decoded size > MaxMetadataSize.
		dirOffset := uint64(0)
		out := make([]byte, 0, len(craftedPayload)+18)
		out = append(out, craftedPayload...)

		var footer [18]byte
		binary.LittleEndian.PutUint32(footer[0:], shared.MagicNumber)
		binary.LittleEndian.PutUint16(footer[4:], shared.FooterV7Version)
		binary.LittleEndian.PutUint64(footer[6:], dirOffset)
		binary.LittleEndian.PutUint32(footer[14:], uint32(len(craftedPayload))) //nolint:gosec // test only
		out = append(out, footer[:]...)

		_, openErr := reader.NewReaderFromProvider(&memProvider{data: out})
		require.Error(t, openErr)
		assert.Contains(t, openErr.Error(), "snappy decoded size",
			"expected decompression-bomb guard error for V14 section directory, got: %v", openErr)
	})
}

// ---- BENCH-R-08: Metadata open latency ----

func BenchmarkReaderOpen_V13(b *testing.B) {
	// Generate a file with many blocks to stress the metadata section.
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  &buf,
		MaxBlockSpans: 100,
	})
	if err != nil {
		b.Fatal(err)
	}

	traceID := [16]byte{0x01}
	for i := range 1000 {
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            fixedSpanID(byte(i % 256)),
			Name:              fmt.Sprintf("bench.op.%d", i%50),
			StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000),
			EndTimeUnixNano:   uint64(1_001_000_000 + i*1_000_000),
			Attributes: []*commonv1.KeyValue{
				stringAttr("http.method", "GET"),
				int64Attr("http.status_code", int64(200)),
			},
		}
		if addErr := w.AddSpan(traceID[:], span, map[string]any{
			"service.name": fmt.Sprintf("svc-%d", i%10),
		}, "", nil, ""); addErr != nil {
			b.Fatal(addErr)
		}
	}
	if _, err := w.Flush(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		r, openErr := reader.NewReaderFromProvider(&memProvider{data: data})
		if openErr != nil {
			b.Fatal(openErr)
		}
		_ = r.BlockCount()
	}

	b.StopTimer()
	b.ReportMetric(float64(len(data)), "file_bytes")
}

// ---- TIDBLOOM-01: Trace ID bloom — present traces must pass bloom check ----

// TestTraceIDBloom_PresentTraces writes a file with N known trace IDs,
// opens it with NewLeanReaderFromProvider, and verifies that BlocksForTraceID
// returns non-nil for every inserted trace ID.
func TestTraceIDBloom_PresentTraces(t *testing.T) {
	const numTraces = 50

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 1000)

	traceIDs := make([][16]byte, numTraces)
	for i := range numTraces {
		traceIDs[i][0] = byte(i)
		traceIDs[i][15] = byte(i + 1)
	}

	for i, tid := range traceIDs {
		span := makeSpan(tid, fixedSpanID(byte(i)), "op", uint64(i+1)*1000, uint64(i+2)*1000, 0, nil)
		addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	prov := &memProvider{data: buf.Bytes()}
	r, err := reader.NewLeanReaderFromProvider(prov)
	require.NoError(t, err)

	// All inserted trace IDs must return at least one block.
	for i, tid := range traceIDs {
		blocks := r.BlocksForTraceID(tid)
		assert.NotNil(t, blocks, "trace %d must be found (not pruned by bloom)", i)
		assert.NotEmpty(t, blocks, "trace %d must have block entries", i)
	}
}

// ---- TIDBLOOM-02: Trace ID bloom — absent trace must be pruned ----

// TestTraceIDBloom_AbsentTrace verifies that a trace ID never written to the file
// returns nil from BlocksForTraceIDCompact (bloom correctly prunes it).
func TestTraceIDBloom_AbsentTrace(t *testing.T) {
	const numTraces = 100

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 1000)

	for i := range numTraces {
		var tid [16]byte
		tid[0] = byte(i)
		tid[1] = 0xAA
		span := makeSpan(tid, fixedSpanID(byte(i)), "op", uint64(i+1)*1000, uint64(i+2)*1000, 0, nil)
		addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	prov := &memProvider{data: buf.Bytes()}
	r, err := reader.NewLeanReaderFromProvider(prov)
	require.NoError(t, err)

	// A trace ID with byte pattern that was never written must not be found.
	// The all-0xFF trace ID is guaranteed absent.
	absentID := [16]byte{
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	}
	blocks := r.BlocksForTraceID(absentID)
	assert.Nil(t, blocks, "absent trace ID must return nil (pruned by bloom or hash miss)")
}

// ---- TIDBLOOM-03: LeanReader with bloom filter — 2 I/O path uses bloom ----

// TestTraceIDBloom_LeanReader verifies that NewLeanReaderFromProvider produces a reader
// that correctly handles the bloom-enabled compact index (version 2).
func TestTraceIDBloom_LeanReader(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 500)

	traceID := [16]byte{0xDE, 0xAD, 0xBE, 0xEF}
	span := makeSpan(traceID, fixedSpanID(1), "root", 1000, 2000, 0, nil)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "svc"})
	flushToBuffer(t, &buf, w)

	prov := &memProvider{data: buf.Bytes()}
	r, err := reader.NewLeanReaderFromProvider(prov)
	require.NoError(t, err)

	// Present trace must be found.
	blocks := r.BlocksForTraceID(traceID)
	assert.NotNil(t, blocks)

	// Absent trace must not be found.
	var absent [16]byte
	absent[0] = 0xFF
	absent[15] = 0xFF
	blocksAbsent := r.BlocksForTraceID(absent)
	assert.Nil(t, blocksAbsent)
}

// ---- GAP-9: ReadGroup I/O failure propagation ----

// failingReaderProvider wraps a memProvider and returns ErrUnexpectedEOF on the Nth call and all subsequent calls.
type failingReaderProvider struct {
	inner   memProvider
	failOnN int32
	n       int32
}

func (f *failingReaderProvider) Size() (int64, error) { return int64(len(f.inner.data)), nil }

func (f *failingReaderProvider) ReadAt(p []byte, off int64, dt rw.DataType) (int, error) {
	count := f.n + 1
	f.n = count
	if count >= f.failOnN {
		return 0, fmt.Errorf("injected I/O failure on read %d: %w", count, io.ErrUnexpectedEOF)
	}
	return f.inner.ReadAt(p, off, dt)
}

// TestReadGroup_IOFailure verifies that Reader.ReadGroup propagates I/O errors from
// the underlying provider rather than returning partial results or panicking.
// GAP-9: ReadGroup I/O failure.
func TestReadGroup_IOFailure(t *testing.T) {
	// Build a valid 3-span file with one block.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	traceID := [16]byte{0xC1}
	for i := range 3 {
		span := makeSpan(traceID, fixedSpanID(byte(i)), "op",
			uint64(1000+i*1000), uint64(2000+i*1000),
			tracev1.Span_SPAN_KIND_INTERNAL, nil)
		addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "fail-svc"})
	}
	flushToBuffer(t, &buf, w)

	goodBytes := buf.Bytes()

	// First, count how many reads initialization requires by using a counting provider.
	countingMem := &memProvider{data: goodBytes}
	countTracker := rw.NewTrackingReaderProvider(countingMem)
	initReader, err := reader.NewReaderFromProvider(countTracker)
	require.NoError(t, err)
	initReads := countTracker.IOOps()
	require.Greater(t, initReads, int64(0), "initialization must perform at least one read")
	_ = initReader

	// Now open a reader from a failingProvider that fails AFTER initialization reads.
	// The initReads+1 th call will be the first block read → fail there.
	fp := &failingReaderProvider{
		inner:   memProvider{data: goodBytes},
		failOnN: int32(initReads) + 1, //nolint:gosec // safe: initReads bounded by small constant
	}
	r, openErr := reader.NewReaderFromProvider(fp)
	require.NoError(t, openErr, "reader initialization must succeed (failure injected after init)")
	require.Greater(t, r.BlockCount(), 0, "file must have at least 1 block")

	// Construct a CoalescedRead for block 0.
	groups := r.CoalescedGroups([]int{0})
	require.NotEmpty(t, groups, "must have at least one coalesced group")

	// ReadGroup must return an error (the injected failure).
	_, readErr := r.ReadGroup(groups[0])
	require.Error(t, readErr, "ReadGroup must propagate the injected I/O error")
}

// ---- GAP-16: Zero-block file ----

// TestReader_ZeroBlockFile verifies that flushing an empty writer produces a valid
// zero-block file: NewReaderFromProvider must succeed and BlockCount must be 0.
func TestReader_ZeroBlockFile(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	flushToBuffer(t, &buf, w)

	data := buf.Bytes()
	assert.NotEmpty(t, data, "empty flush must still write footer/header bytes")

	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err, "opening a zero-block file must succeed")
	require.Equal(t, 0, r.BlockCount(), "zero-block file must report 0 blocks")

	// GetBlockWithBytes with out-of-range index must return an error, not panic.
	_, err = r.GetBlockWithBytes(0, nil, nil)
	assert.Error(t, err, "GetBlockWithBytes on empty file must return an error")
}

// ---- GAP-13: Footer version mismatch ----

// TestReader_FooterVersionMismatch verifies that a file with a corrupted footer
// version is rejected by NewReaderFromProvider with an error.
func TestReader_FooterVersionMismatch(t *testing.T) {
	t.Parallel()

	// Build a valid 1-span file.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	traceID := [16]byte{0xC0, 0xFF}
	span := makeSpan(traceID, fixedSpanID(1), "op", 1000, 2000, tracev1.Span_SPAN_KIND_INTERNAL, nil)
	addSpanToWriter(t, w, traceID, span, map[string]any{"service.name": "ver-svc"})
	flushToBuffer(t, &buf, w)

	good := buf.Bytes()

	// The writer now writes a V7 footer (18 bytes at the end of the file).
	// Corrupt the version bytes in all known footer sizes so that all footer-format
	// detections fail: V7 (18 bytes), V4 (34 bytes), and V3 (22 bytes).
	corrupt := make([]byte, len(good))
	copy(corrupt, good)
	n := len(corrupt)

	// Overwrite V7 version field at footerStart+4 with an invalid version (99).
	binary.LittleEndian.PutUint16(corrupt[n-int(shared.FooterV7Size)+4:], 99)
	// Overwrite V4 version field (bytes n-34 and n-33) with an invalid version (99).
	if n >= int(shared.FooterV4Size) {
		binary.LittleEndian.PutUint16(corrupt[n-int(shared.FooterV4Size):], 99)
	}
	// Overwrite V3 version field (bytes n-22 and n-21) with an invalid version (99).
	if n >= int(shared.FooterV3Size) {
		binary.LittleEndian.PutUint16(corrupt[n-int(shared.FooterV3Size):], 99)
	}

	// Clear reader caches so the corrupted file is not served from a prior parse.
	reader.ClearCaches()

	_, err := reader.NewReaderFromProvider(&memProvider{data: corrupt})
	require.Error(t, err, "corrupted footer version must cause an error")
}

// ---- INTCACHE-01: parsedIntrinsicCache — second reader hits process-level cache ----

// TestParsedIntrinsicCache_SecondReaderHitsCache verifies that the process-level
// parsedIntrinsicCache returns the same decoded column pointer on second access,
// avoiding redundant DecodeIntrinsicColumnBlob calls. (GAP-51 / bonus)
func TestParsedIntrinsicCache_SecondReaderHitsCache(t *testing.T) {
	// Clear caches before and after to avoid cross-test pollution.
	reader.ClearCaches()
	t.Cleanup(reader.ClearCaches)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	var tid [16]byte
	tid[0] = 0x42
	span := makeSpan(tid, fixedSpanID(1), "cached-op", 1_000_000_000, 2_000_000_000, 0, nil)
	addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "cache-svc"})
	flushToBuffer(t, &buf, w)

	data := buf.Bytes()

	// First reader: populates the process-level cache.
	r1, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: "test-cache-file"},
	)
	require.NoError(t, err)
	require.True(t, r1.HasIntrinsicSection(), "file must have an intrinsic section")

	col1, err := r1.GetIntrinsicColumn("span:name")
	require.NoError(t, err)
	require.NotNil(t, col1, "span:name must be present")

	// Second reader with same FileID: must return identical decoded pointer from cache.
	r2, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: "test-cache-file"},
	)
	require.NoError(t, err)

	col2, err := r2.GetIntrinsicColumn("span:name")
	require.NoError(t, err)
	require.NotNil(t, col2, "span:name must be present from cache")

	// Same pointer means the cache was hit — no re-decoding occurred.
	assert.Same(t, col1, col2, "second reader must return the cached pointer (no re-decode)")
}

// TestParsedIntrinsicCache_DifferentFileIDs verifies that two readers with different
// FileIDs do NOT share cached results — each gets its own decoded column.
func TestParsedIntrinsicCache_DifferentFileIDs(t *testing.T) {
	reader.ClearCaches()
	t.Cleanup(reader.ClearCaches)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	var tid [16]byte
	tid[0] = 0x43
	span := makeSpan(tid, fixedSpanID(2), "isolated-op", 1_000_000_000, 2_000_000_000, 0, nil)
	addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "isolation-svc"})
	flushToBuffer(t, &buf, w)

	data := buf.Bytes()

	rA, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: "file-A"},
	)
	require.NoError(t, err)

	rB, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: "file-B"},
	)
	require.NoError(t, err)

	colA, err := rA.GetIntrinsicColumn("span:name")
	require.NoError(t, err)
	require.NotNil(t, colA)

	colB, err := rB.GetIntrinsicColumn("span:name")
	require.NoError(t, err)
	require.NotNil(t, colB)

	// Different FileIDs must produce separate cache entries (different pointers).
	assert.NotSame(t, colA, colB, "different FileIDs must use separate cache entries")
}

// TestParsedIntrinsicCache_HitCorrectness verifies that the process-level
// parsedIntrinsicCache returns data equivalent to a fresh decode on a cache hit.
//
// Two correctness properties are checked:
//  1. Value equality: the cache-hit column has the same Name, Count, Uint64Values,
//     and BlockRefs as the column from the cache-miss decode.
//  2. Shared-pointer semantics: the cache stores and returns the same *IntrinsicColumn
//     pointer (no defensive copy). Callers must treat the returned value as immutable.
//     This test documents and asserts that contract — mutating the slice would corrupt
//     the cache, so callers MUST NOT modify the returned column.
func TestParsedIntrinsicCache_HitCorrectness(t *testing.T) {
	reader.ClearCaches()
	t.Cleanup(reader.ClearCaches)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	var tid [16]byte
	tid[0] = 0x77
	span := makeSpan(tid, fixedSpanID(7), "correctness-op", 1_000_000_000, 3_000_000_000, 0, nil)
	addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "hit-svc"})
	flushToBuffer(t, &buf, w)

	data := buf.Bytes()
	const fileID = "test-hit-correctness"

	// First reader: cache miss — performs real decode.
	r1, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: fileID},
	)
	require.NoError(t, err)
	require.True(t, r1.HasIntrinsicSection())

	col1, err := r1.GetIntrinsicColumn("span:start")
	require.NoError(t, err)
	require.NotNil(t, col1, "span:start must exist")

	// Second reader with same FileID: cache hit — returns from parsedIntrinsicCache.
	r2, err := reader.NewReaderFromProviderWithOptions(
		&memProvider{data: data},
		reader.Options{FileID: fileID},
	)
	require.NoError(t, err)

	col2, err := r2.GetIntrinsicColumn("span:start")
	require.NoError(t, err)
	require.NotNil(t, col2, "span:start must be present from cache")

	// Property 1: value equality — cache-hit data must equal cache-miss data.
	assert.Equal(t, col1.Name, col2.Name, "Name must match")
	assert.Equal(t, col1.Count, col2.Count, "Count must match")
	assert.Equal(t, col1.Type, col2.Type, "Type must match")
	assert.Equal(t, col1.Uint64Values, col2.Uint64Values, "Uint64Values must match")
	assert.Equal(t, col1.BlockRefs, col2.BlockRefs, "BlockRefs must match")

	// Property 2: shared-pointer semantics — same pointer = no re-decode, no copy.
	// The returned column is immutable by contract; callers must not mutate it.
	assert.Same(t, col1, col2, "cache hit must return the identical pointer (shared immutable)")
}

// ---- TIDBLOOM-04: Compact index version 2 — bloom data preserved across write/read ----

// TestTraceIDBloom_VersionV2 writes a file and verifies the compact index contains
// a valid version-2 header with a non-zero bloom filter.
func TestTraceIDBloom_VersionV2(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 1000)

	for i := range 20 {
		var tid [16]byte
		tid[0] = byte(i + 1)
		span := makeSpan(tid, fixedSpanID(byte(i)), "op", uint64(i+1)*100, uint64(i+2)*100, 0, nil)
		addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "svc"})
	}
	flushToBuffer(t, &buf, w)

	// Both full and lean readers must find all traces (regression: bloom must have no false negatives).
	data := buf.Bytes()
	prov := &memProvider{data: data}
	rFull, err := reader.NewReaderFromProvider(prov)
	require.NoError(t, err)
	rLean, err := reader.NewLeanReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)

	for i := range 20 {
		var tid [16]byte
		tid[0] = byte(i + 1)
		bFull := rFull.BlocksForTraceID(tid)
		bLean := rLean.BlocksForTraceID(tid)
		assert.NotEmpty(t, bFull, "full reader: trace %d not found", i)
		assert.NotEmpty(t, bLean, "lean reader: trace %d not found", i)
		assert.Equal(t, bFull, bLean, "full and lean must agree for trace %d", i)
	}
}

// TestGetBlockWithBytes_NonNilSecondPassColsReturnsError verifies that GetBlockWithBytes
// returns an error (not a panic) when secondPassCols is non-nil (BUG-4 / BUG-12 guard).
func TestGetBlockWithBytes_NonNilSecondPassColsReturnsError(t *testing.T) {
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)

	var tid [16]byte
	tid[0] = 0x01
	span := makeSpan(tid, fixedSpanID(1), "op.test", 100, 200, 0, nil)
	addSpanToWriter(t, w, tid, span, map[string]any{"service.name": "svc"})
	flushToBuffer(t, &buf, w)

	r := openReader(t, buf.Bytes())

	secondPass := map[string]struct{}{"span:name": {}}
	_, err := r.GetBlockWithBytes(0, nil, secondPass)
	require.Error(t, err)
	require.Contains(t, err.Error(), "secondPassCols must be nil")
}
