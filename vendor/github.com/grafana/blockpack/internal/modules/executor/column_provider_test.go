package executor

// Internal tests for blockColumnProvider — covers scan methods not exercised by executor_test.go.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/vm"
)

// --- test helpers ---

type memProv struct{ data []byte }

func (m *memProv) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *memProv) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

func buildBlock(t *testing.T, spans []spanDef) *modules_reader.Block {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 0,
	})
	require.NoError(t, err)

	traceID := [16]byte{0x01}
	for i, s := range spans {
		sp := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              s.name,
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Attributes:        s.attrs,
		}
		require.NoError(t, w.AddSpan(traceID[:], sp, s.resAttrs, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&memProv{data: buf.Bytes()})
	require.NoError(t, err)
	require.Greater(t, r.BlockCount(), 0)
	// Load all columns via GetBlockWithBytes
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	return bwb.Block
}

type spanDef struct {
	resAttrs map[string]any
	name     string
	attrs    []*commonv1.KeyValue
}

func kv(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   k,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}},
	}
}

func collectRows(t *testing.T, rs vm.RowSet) []int {
	t.Helper()
	return rs.ToSlice()
}

// --- tests ---

func TestBlockColumnProvider_GetRowCount(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	assert.Equal(t, 3, p.GetRowCount())
}

func TestBlockColumnProvider_ScanLessThan(t *testing.T) {
	// Use a user attribute column (span.label) — intrinsic columns (span:name) are no longer
	// in block columns and are served from the intrinsic section only.
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("label", "aardvark")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("label", "badger")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("label", "cat")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanLessThan("span.label", "badger")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 1) // only "aardvark" < "badger"
}

func TestBlockColumnProvider_ScanLessThanOrEqual(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("label", "aardvark")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("label", "badger")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("label", "cat")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanLessThanOrEqual("span.label", "badger")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 2) // "aardvark" and "badger" <= "badger"
}

func TestBlockColumnProvider_ScanGreaterThanOrEqual(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("label", "aardvark")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("label", "badger")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("label", "cat")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanGreaterThanOrEqual("span.label", "badger")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 2) // "badger" and "cat" >= "badger"
}

func TestBlockColumnProvider_ScanIsNull(t *testing.T) {
	// All spans lack "span.missing" column, so all should be null.
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanIsNull("span.missing")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 2)
}

func TestBlockColumnProvider_ScanIsNotNull(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("env", "prod")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	// User attributes in span scope use dot syntax: "span.env"
	rs, err := p.ScanIsNotNull("span.env")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 1)
}

func TestBlockColumnProvider_ScanIsNotNull_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanIsNotNull("span.nonexistent")
	require.NoError(t, err)
	assert.Empty(t, collectRows(t, rs))
}

func TestBlockColumnProvider_ScanRegex(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("op", "http.get")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("op", "grpc.call")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("op", "http.post")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanRegex("span.op", "^http\\..*")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 2)
}

func TestBlockColumnProvider_ScanRegex_InvalidPattern(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("op", "x")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	_, err := p.ScanRegex("span.op", "[invalid")
	assert.Error(t, err)
}

func TestBlockColumnProvider_ScanRegex_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanRegex("span.nonexistent", ".*")
	require.NoError(t, err)
	assert.Empty(t, collectRows(t, rs))
}

func TestBlockColumnProvider_ScanRegexNotMatch(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("op", "http.get")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("op", "grpc.call")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("op", "http.post")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanRegexNotMatch("span.op", "^http\\..*")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 1) // only "grpc.call" doesn't match
}

func TestBlockColumnProvider_ScanRegexNotMatch_InvalidPattern(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	_, err := p.ScanRegexNotMatch("span:name", "[invalid")
	assert.Error(t, err)
}

func TestBlockColumnProvider_ScanContains(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", attrs: []*commonv1.KeyValue{kv("svc", "checkout-service")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", attrs: []*commonv1.KeyValue{kv("svc", "auth-service")}, resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", attrs: []*commonv1.KeyValue{kv("svc", "checkout-processor")}, resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanContains("span.svc", "checkout")
	require.NoError(t, err)
	rows := collectRows(t, rs)
	assert.Len(t, rows, 2)
}

func TestBlockColumnProvider_ScanContains_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs, err := p.ScanContains("span.nonexistent", "foo")
	require.NoError(t, err)
	assert.Empty(t, collectRows(t, rs))
}

func TestBlockColumnProvider_FullScan(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	rs := p.FullScan()
	rows := collectRows(t, rs)
	assert.Equal(t, []int{0, 1, 2}, rows)
}

func TestBlockColumnProvider_Complement(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s4", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)

	// Complement of {1, 3} in a 4-row block = {0, 2}
	included := newRowSet()
	included.Add(1)
	included.Add(3)

	comp := p.Complement(included)
	rows := collectRows(t, comp)
	assert.Equal(t, []int{0, 2}, rows)
}

func TestBlockColumnProvider_Complement_EmptyInput(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	comp := p.Complement(newRowSet())
	rows := collectRows(t, comp)
	assert.Equal(t, []int{0, 1}, rows)
}

func TestBlockColumnProvider_GetValue(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{
			name:     "span-a",
			attrs:    []*commonv1.KeyValue{kv("region", "us-east")},
			resAttrs: map[string]any{"service.name": "svc"},
		},
		{name: "span-b", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)

	// Row 0 has the attribute
	val, present, err := p.GetValue("span.region", 0)
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, "us-east", val)

	// Row 1 is missing the attribute
	val2, present2, err2 := p.GetValue("span.region", 1)
	require.NoError(t, err2)
	assert.False(t, present2)
	assert.Nil(t, val2)

	// Nonexistent column
	val3, present3, err3 := p.GetValue("span.nonexistent", 0)
	require.NoError(t, err3)
	assert.False(t, present3)
	assert.Nil(t, val3)
}

func TestBlockColumnProvider_StreamFullScan(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	var rows []int
	n, err := p.StreamFullScan(func(rowIdx int) bool {
		rows = append(rows, rowIdx)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []int{0, 1}, rows)
}

func TestBlockColumnProvider_StreamFullScan_EarlyStop(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s3", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	var rows []int
	_, err := p.StreamFullScan(func(rowIdx int) bool {
		rows = append(rows, rowIdx)
		return rowIdx < 1 // stop after 2nd row
	})
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestBlockColumnProvider_StreamScanGreaterThanOrEqual_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	n, err := p.StreamScanGreaterThanOrEqual("span.nonexistent", "x", func(_ int) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestBlockColumnProvider_StreamScanIsNull_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s1", resAttrs: map[string]any{"service.name": "svc"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc"}},
	})
	p := newBlockColumnProvider(block)
	var rows []int
	n, err := p.StreamScanIsNull("span.nonexistent", func(rowIdx int) bool {
		rows = append(rows, rowIdx)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []int{0, 1}, rows)
}

// --- StreamScanEqualAny tests ---

func TestBlockColumnProvider_StreamScanEqualAny_StringDictFastPath(t *testing.T) {
	// Use resource.region (a user resource attribute) — resource.service.name is also
	// present in block columns (dual-storage per NOTE-050), but resource.region keeps
	// this test independent of intrinsic column behavior.
	block := buildBlock(t, []spanDef{
		{name: "s0", resAttrs: map[string]any{"service.name": "svc", "region": "us-a"}},
		{name: "s1", resAttrs: map[string]any{"service.name": "svc", "region": "eu-b"}},
		{name: "s2", resAttrs: map[string]any{"service.name": "svc", "region": "ap-c"}},
		{name: "s3", resAttrs: map[string]any{"service.name": "svc", "region": "us-a"}},
	})
	p := newBlockColumnProvider(block)
	var rows []int
	n, err := p.StreamScanEqualAny("resource.region", []any{"us-a", "ap-c"}, func(rowIdx int) bool {
		rows = append(rows, rowIdx)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 3, n, "us-a (x2) + ap-c (x1) = 3 matches")
	assert.Len(t, rows, 3, "must return 3 rows")
	// Verify matched region values — order depends on internal block sort.
	col := block.GetColumn("resource.region")
	require.NotNil(t, col)
	for _, rowIdx := range rows {
		v, ok := col.StringValue(rowIdx)
		require.True(t, ok)
		assert.True(t, v == "us-a" || v == "ap-c", "matched row %d has value %q, expected us-a or ap-c", rowIdx, v)
	}
}

func TestBlockColumnProvider_StreamScanEqualAny_SingleValue_DelegatesToScanEqual(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s0", resAttrs: map[string]any{"service.name": "svc", "region": "us-a"}},
		{name: "s1", resAttrs: map[string]any{"service.name": "svc", "region": "eu-b"}},
	})
	p := newBlockColumnProvider(block)
	var rows []int
	n, err := p.StreamScanEqualAny("resource.region", []any{"us-a"}, func(rowIdx int) bool {
		rows = append(rows, rowIdx)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, rows, 1)
}

func TestBlockColumnProvider_StreamScanEqualAny_EmptyValues(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s0", resAttrs: map[string]any{"service.name": "svc-a"}},
	})
	p := newBlockColumnProvider(block)
	n, err := p.StreamScanEqualAny("resource.service.name", []any{}, func(_ int) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestBlockColumnProvider_StreamScanEqualAny_MissingColumn(t *testing.T) {
	block := buildBlock(t, []spanDef{
		{name: "s0", resAttrs: map[string]any{"service.name": "svc-a"}},
	})
	p := newBlockColumnProvider(block)
	n, err := p.StreamScanEqualAny("span.nonexistent", []any{"x"}, func(_ int) bool { return true })
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
