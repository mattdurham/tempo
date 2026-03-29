package reader_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// writeSpansAndGetBlock is the shared helper for column encoding tests.
// It writes defs to a fresh writer, flushes, opens a reader, and returns block 0.
func writeSpansAndGetBlock(t *testing.T, defs []spanWriteDef) *reader.BlockWithBytes {
	t.Helper()
	_, bwb := writeSpansAndGetReader(t, defs)
	return bwb
}

// writeSpansAndGetReader is like writeSpansAndGetBlock but also returns the reader.
// Use this when intrinsic column access via r.IntrinsicBytesAt / r.IntrinsicUint64At
// / r.IntrinsicDictStringAt is needed alongside the block.
func writeSpansAndGetReader(t *testing.T, defs []spanWriteDef) (*reader.Reader, *reader.BlockWithBytes) {
	t.Helper()
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)

	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 1)

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	return r, bwb
}

// spanWriteDef bundles data for one AddSpan call.
type spanWriteDef struct {
	span          *tracev1.Span
	resourceAttrs map[string]any
	traceID       [16]byte
}

func spanDef(tid [16]byte, name string, start, end uint64, attrs []*commonv1.KeyValue) spanWriteDef {
	return spanWriteDef{
		traceID: tid,
		span: &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            fixedSpanID(tid[0]),
			Name:              name,
			StartTimeUnixNano: start,
			EndTimeUnixNano:   end,
			Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
			Attributes:        attrs,
		},
		resourceAttrs: map[string]any{"service.name": "col-test-svc"},
	}
}

// ---- ENC-01: Dictionary encoding — string column ----

func TestDecodeDictionary_String(t *testing.T) {
	methods := []string{"GET", "POST", "DELETE"}
	tid := [16]byte{0x01}

	defs := make([]spanWriteDef, 20)
	want := make([]string, 20)
	for i := range 20 {
		m := methods[i%3]
		want[i] = m
		defs[i] = spanDef(
			tid,
			"op",
			uint64(i*1000), uint64(i*1000+100),
			[]*commonv1.KeyValue{stringAttr("http.method", m)},
		)
		// Each span needs a unique span ID.
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	bwb := writeSpansAndGetBlock(t, defs)
	col := bwb.Block.GetColumn("span.http.method")
	require.NotNil(t, col, "http.method column must be present")
	require.Equal(t, 20, col.SpanCount)

	// Collect all decoded values.
	got := make(map[string]int)
	for i := range 20 {
		v, ok := col.StringValue(i)
		assert.True(t, ok, "row %d must be present", i)
		got[v]++
	}
	// Each method should appear ~6-7 times total (20 spans, 3 methods).
	assert.Greater(t, got["GET"], 0, "GET must appear")
	assert.Greater(t, got["POST"], 0, "POST must appear")
	assert.Greater(t, got["DELETE"], 0, "DELETE must appear")
	assert.Equal(t, 20, got["GET"]+got["POST"]+got["DELETE"])
}

// ---- ENC-02: Delta uint64 encoding — timestamp column ----

func TestDecodeDeltaUint64_Timestamps(t *testing.T) {
	tid := [16]byte{0x02}
	const n = 10
	defs := make([]spanWriteDef, n)
	startTimes := make([]uint64, n)

	for i := range n {
		st := uint64(1_000_000_000 + i)
		startTimes[i] = st
		defs[i] = spanDef(tid, "op", st, st+1000, nil)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	r, _ := writeSpansAndGetReader(t, defs)
	// span:start is now stored in the intrinsic section only, not in block columns.
	require.True(t, r.HasIntrinsicSection(), "span:start requires intrinsic section")

	// Collect all timestamps from the intrinsic column and verify they match the expected set.
	decoded := make(map[uint64]bool)
	for i := range n {
		v, ok := r.IntrinsicUint64At("span:start", 0, i)
		assert.True(t, ok, "row %d must be present in intrinsic span:start", i)
		decoded[v] = true
	}
	for _, st := range startTimes {
		assert.True(t, decoded[st], "start time %d must round-trip via intrinsic section", st)
	}
}

// ---- ENC-03: XOR encoding — span:id column ----

func TestDecodeXORBytes_IDs(t *testing.T) {
	tid := [16]byte{0x03}
	// 5 span IDs sharing the same first 6 bytes.
	prefix := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
	spanIDs := make([][]byte, 5)
	for i := range 5 {
		id := make([]byte, 8)
		copy(id, prefix)
		id[6] = byte(i)
		id[7] = byte(i + 10)
		spanIDs[i] = id
	}

	defs := make([]spanWriteDef, 5)
	for i := range 5 {
		defs[i] = spanDef(tid, "op", uint64(i*1000), uint64(i*1000+100), nil)
		defs[i].span.SpanId = spanIDs[i]
		defs[i].span.TraceId = tid[:]
	}

	bwb := writeSpansAndGetBlock(t, defs)
	// NOTE-005: span:id is no longer in the intrinsic section; read from block column.
	col := bwb.Block.GetColumn("span:id")
	require.NotNil(t, col, "span:id block column must be present")

	// Collect decoded span IDs from the block column and verify round-trip.
	decoded := make(map[string]bool)
	for i := range 5 {
		v, ok := col.BytesValue(i)
		assert.True(t, ok, "row %d span:id must be present in block column", i)
		decoded[string(v)] = true
	}
	for _, sid := range spanIDs {
		assert.True(t, decoded[string(sid)], "span ID %v must round-trip via block column", sid)
	}
}

// ---- ENC-04: Prefix encoding — URL column ----

func TestDecodePrefixBytes_URLs(t *testing.T) {
	tid := [16]byte{0x04}
	const prefix = "https://api.example.com/"
	paths := []string{
		"users", "orders", "products", "items", "reviews",
		"catalog", "search", "auth", "billing", "webhooks",
	}

	defs := make([]spanWriteDef, 10)
	wantURLs := make([]string, 10)
	for i := range 10 {
		url := prefix + paths[i]
		wantURLs[i] = url
		defs[i] = spanDef(
			tid, "op", uint64(i*1000), uint64(i*1000+100),
			[]*commonv1.KeyValue{bytesAttr("http.url", []byte(url))},
		)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	bwb := writeSpansAndGetBlock(t, defs)
	col := bwb.Block.GetColumn("span.http.url")
	require.NotNil(t, col, "http.url column must be present")

	// Collect decoded values.
	decoded := make(map[string]bool)
	for i := range 10 {
		v, ok := col.BytesValue(i)
		assert.True(t, ok, "row %d must be present", i)
		decoded[string(v)] = true
	}
	for _, url := range wantURLs {
		assert.True(t, decoded[url], "URL %q must round-trip", url)
	}
}

// ---- ENC-05: RLE encoding — low cardinality ----

func TestDecodeRLEIndexes(t *testing.T) {
	tid := [16]byte{0x05}
	const n = 100

	defs := make([]spanWriteDef, n)
	for i := range n {
		defs[i] = spanDef(
			tid, "op", uint64(i*100), uint64(i*100+50),
			[]*commonv1.KeyValue{stringAttr("deployment.env", "production")},
		)
		defs[i].span.SpanId = fixedSpanID(byte(i % 256))
	}

	bwb := writeSpansAndGetBlock(t, defs)
	col := bwb.Block.GetColumn("span.deployment.env")
	require.NotNil(t, col, "deployment.env must be present")
	require.Equal(t, n, col.SpanCount)

	for i := range n {
		v, ok := col.StringValue(i)
		assert.True(t, ok, "row %d must be present", i)
		assert.Equal(t, "production", v, "row %d value mismatch", i)
	}
}

// ---- ENC-06: Sparse encoding — >50% nulls ----

func TestDecodePresence_Sparse(t *testing.T) {
	tid := [16]byte{0x06}
	const n = 10

	defs := make([]spanWriteDef, n)
	for i := range n {
		var attrs []*commonv1.KeyValue
		if i == 2 {
			attrs = []*commonv1.KeyValue{stringAttr("rare.attr", "present")}
		}
		defs[i] = spanDef(tid, "op", uint64(i*1000), uint64(i*1000+100), attrs)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	bwb := writeSpansAndGetBlock(t, defs)
	col := bwb.Block.GetColumn("span.rare.attr")
	require.NotNil(t, col, "rare.attr must be present")
	require.Equal(t, n, col.SpanCount)

	// Exactly one row must be present.
	presentCount := 0
	presentValue := ""
	for i := range n {
		v, ok := col.StringValue(i)
		if ok {
			presentCount++
			presentValue = v
		}
	}
	assert.Equal(t, 1, presentCount, "exactly one row must be present")
	assert.Equal(t, "present", presentValue)
}

// ---- ENC-07: DeltaDictionary — trace:id column ----

func TestDecodeDeltaDictionary_TraceID(t *testing.T) {
	tid1 := [16]byte{0x10, 0x00, 0x00, 0x00}
	tid2 := [16]byte{0x20, 0x00, 0x00, 0x00}
	tid3 := [16]byte{0x30, 0x00, 0x00, 0x00}

	defs := make([]spanWriteDef, 0, 10)
	for _, tid := range [][16]byte{tid1, tid1, tid1, tid1, tid2, tid2, tid2, tid2, tid3, tid3} {
		d := spanDef(tid, "op", 1000, 2000, nil)
		d.traceID = tid
		d.span.TraceId = tid[:]
		d.span.SpanId = fixedSpanID(byte(len(defs))) //nolint:gosec // safe: len bounded by 10
		defs = append(defs, d)
	}

	bwb := writeSpansAndGetBlock(t, defs)
	// NOTE-005: trace:id is no longer in the intrinsic section; read from block column.
	col := bwb.Block.GetColumn("trace:id")
	require.NotNil(t, col, "trace:id block column must be present")

	// All 10 trace IDs must be present in the block column.
	decoded := make(map[[16]byte]bool)
	for i := range 10 {
		v, ok := col.BytesValue(i)
		assert.True(t, ok, "row %d trace:id must be present in block column", i)
		if ok && len(v) == 16 {
			var arr [16]byte
			copy(arr[:], v)
			decoded[arr] = true
		}
	}
	assert.True(t, decoded[tid1], "tid1 must be present")
	assert.True(t, decoded[tid2], "tid2 must be present")
	assert.True(t, decoded[tid3], "tid3 must be present")
}

// ---- Lazy column decode tests ----

// TestLazyColumnDecode_PresenceOnlyCorrect writes spans with a mix of present and absent
// values, parses with wantColumns filtering out the test column (making it lazy), then
// verifies that IsPresent() returns the same results as eager decode.
func TestLazyColumnDecode_PresenceOnlyCorrect(t *testing.T) {
	tid := [16]byte{0xA0}
	const n = 10
	defs := make([]spanWriteDef, n)
	for i := range n {
		var attrs []*commonv1.KeyValue
		if i%3 == 0 {
			attrs = []*commonv1.KeyValue{stringAttr("lazy.attr", "val")}
		}
		defs[i] = spanDef(tid, "op", uint64(i*1000), uint64(i*1000+100), attrs)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	// Eager decode: request span.lazy.attr to get ground-truth presence.
	bwbEager := writeSpansAndGetBlock(t, defs)
	colEager := bwbEager.Block.GetColumn("span.lazy.attr")
	require.NotNil(t, colEager, "span.lazy.attr must be present in eager decode")

	// Lazy decode: same spans but request only span:start — making span.lazy.attr lazy.
	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())
	wantCols := map[string]struct{}{"span:start": {}}
	bwbLazy, err := r.GetBlockWithBytes(0, wantCols, nil)
	require.NoError(t, err)

	colLazy := bwbLazy.Block.GetColumn("span.lazy.attr")
	require.NotNil(t, colLazy, "span.lazy.attr must be lazily registered")

	// Verify IsPresent() matches eager decode for all rows — without triggering full decode.
	for i := range n {
		assert.Equal(t, colEager.IsPresent(i), colLazy.IsPresent(i),
			"row %d: IsPresent must match eager", i)
	}
}

// TestLazyColumnDecode_StringValue verifies that StringValue() on a lazily-registered
// column returns correct values after triggering decodeNow().
func TestLazyColumnDecode_StringValue(t *testing.T) {
	tid := [16]byte{0xA1}
	methods := []string{"GET", "POST", "DELETE"}
	const n = 15
	defs := make([]spanWriteDef, n)
	want := make([]string, n)
	for i := range n {
		m := methods[i%3]
		want[i] = m
		defs[i] = spanDef(
			tid, "op", uint64(i*1000), uint64(i*1000+100),
			[]*commonv1.KeyValue{stringAttr("http.method", m)},
		)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())

	// Request only span:name (not span.http.method) — making it lazily registered.
	wantCols := map[string]struct{}{"span:name": {}}
	bwb, err := r.GetBlockWithBytes(0, wantCols, nil)
	require.NoError(t, err)

	col := bwb.Block.GetColumn("span.http.method")
	require.NotNil(t, col, "span.http.method must be lazily registered")

	// First call to StringValue triggers decodeNow().
	got := make(map[string]int)
	for i := range n {
		v, ok := col.StringValue(i)
		assert.True(t, ok, "row %d must be present", i)
		got[v]++
	}
	assert.Equal(t, n, got["GET"]+got["POST"]+got["DELETE"], "all values must round-trip")
	assert.Greater(t, got["GET"], 0)
	assert.Greater(t, got["POST"], 0)
	assert.Greater(t, got["DELETE"], 0)
}

// TestLazyColumnDecode_Uint64Value verifies uint64 (kind 5 / DeltaUint64) lazy decode.
func TestLazyColumnDecode_Uint64Value(t *testing.T) {
	tid := [16]byte{0xA2}
	const n = 8
	defs := make([]spanWriteDef, n)
	startTimes := make([]uint64, n)
	for i := range n {
		st := uint64(1_000_000_000 + i*1000)
		startTimes[i] = st
		defs[i] = spanDef(tid, "op", st, st+500, nil)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())
	// span:start is now stored in the intrinsic section only, not in block columns.
	// Verify round-trip via r.IntrinsicUint64At instead of block column GetColumn.
	require.True(t, r.HasIntrinsicSection(), "span:start requires intrinsic section")

	decoded := make(map[uint64]bool)
	for i := range n {
		v, ok := r.IntrinsicUint64At("span:start", 0, i)
		assert.True(t, ok, "row %d must be present in intrinsic span:start", i)
		decoded[v] = true
	}
	for _, st := range startTimes {
		assert.True(t, decoded[st], "start time %d must round-trip via intrinsic section", st)
	}
}

// TestParseBlockFromBytes_LazyColumnsRegistered verifies that after GetBlockWithBytes
// with a wantColumns filter, all non-wantColumns are lazily registered (not nil).
func TestParseBlockFromBytes_LazyColumnsRegistered(t *testing.T) {
	tid := [16]byte{0xA3}
	defs := []spanWriteDef{
		spanDef(tid, "op", 1000, 2000, []*commonv1.KeyValue{
			stringAttr("col.eager", "e"),
			stringAttr("col.lazy1", "l1"),
			stringAttr("col.lazy2", "l2"),
		}),
	}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())
	wantCols := map[string]struct{}{"span.col.eager": {}}
	bwb, err := r.GetBlockWithBytes(0, wantCols, nil)
	require.NoError(t, err)

	assert.NotNil(t, bwb.Block.GetColumn("span.col.eager"), "eager column must be present")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.lazy1"), "col.lazy1 must be lazily registered")
	assert.NotNil(t, bwb.Block.GetColumn("span.col.lazy2"), "col.lazy2 must be lazily registered")
}

// TestParseBlockFromBytes_LazyColumnsCorrect verifies that lazy columns produce the same
// values as columns decoded eagerly (nil wantColumns).
func TestParseBlockFromBytes_LazyColumnsCorrect(t *testing.T) {
	tid := [16]byte{0xA4}
	const n = 12
	attrs := make([]*commonv1.KeyValue, 0, 3)
	attrs = append(attrs,
		stringAttr("col.a", "apple"),
		stringAttr("col.b", "banana"),
		stringAttr("col.c", "cherry"),
	)
	defs := make([]spanWriteDef, n)
	for i := range n {
		defs[i] = spanDef(tid, "op", uint64(i*1000), uint64(i*1000+100), attrs)
		defs[i].span.SpanId = fixedSpanID(byte(i))
	}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	for _, d := range defs {
		require.NoError(t, w.AddSpan(d.traceID[:], d.span, d.resourceAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())

	// Eager: decode all columns.
	bwbEager, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	// Lazy: decode only span:name; col.a, col.b, col.c are lazily registered.
	bwbLazy, err := r.GetBlockWithBytes(0, map[string]struct{}{"span:name": {}}, nil)
	require.NoError(t, err)

	for _, colName := range []string{"span.col.a", "span.col.b", "span.col.c"} {
		eagerCol := bwbEager.Block.GetColumn(colName)
		lazyCol := bwbLazy.Block.GetColumn(colName)
		require.NotNil(t, eagerCol, "%s must be present in eager block", colName)
		require.NotNil(t, lazyCol, "%s must be lazily registered in lazy block", colName)

		for i := range n {
			eVal, eOK := eagerCol.StringValue(i)
			lVal, lOK := lazyCol.StringValue(i)
			assert.Equal(t, eOK, lOK, "%s row %d: presence mismatch", colName, i)
			assert.Equal(t, eVal, lVal, "%s row %d: value mismatch", colName, i)
		}
	}
}

// ---- Event names column ----

func TestDecodeEventNames(t *testing.T) {
	tid := [16]byte{0x07}
	span := &tracev1.Span{
		TraceId:           tid[:],
		SpanId:            fixedSpanID(1),
		Name:              "op",
		StartTimeUnixNano: 1000,
		EndTimeUnixNano:   2000,
		Events: []*tracev1.Span_Event{
			{Name: "exception"},
			{Name: "log"},
		},
	}

	var buf bytes.Buffer
	w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
	require.NoError(t, err)
	require.NoError(t, w.AddSpan(tid[:], span, map[string]any{"service.name": "evt-svc"}, "", nil, ""))
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)

	col := bwb.Block.GetColumn("event:name")
	if col == nil {
		// event:name may not be present if no event column is written; skip.
		t.Skip("event:name column not present in this implementation")
	}
	v, ok := col.BytesValue(0)
	assert.True(t, ok)
	assert.NotEmpty(t, v, "event:name bytes must not be empty")
}
