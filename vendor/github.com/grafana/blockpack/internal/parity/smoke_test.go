package parity_test

// PST — Parity Smoke Tests.
// End-to-end field-coverage check: write OTLP spans covering all intrinsic fields and all
// attribute types, read them back via the blockpack reader, and assert every column value
// matches the written input. This is CI-gated (ci.yml + Makefile ci target).
//
// Also includes black-box public API surface checks verifying that the modules/rw extraction
// migration left all public aliases wired up correctly.

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	blockpack "github.com/grafana/blockpack"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// parityMemProvider is an in-memory ByteProvider for tests.
type parityMemProvider struct{ data []byte }

func (p *parityMemProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *parityMemProvider) ReadAt(buf []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off+int64(len(buf)) > int64(len(p.data)) {
		return 0, fmt.Errorf("parityMemProvider.ReadAt: range [%d, %d) out of bounds (size=%d)",
			off, off+int64(len(buf)), len(p.data))
	}
	return copy(buf, p.data[off:]), nil
}

// Compile-time check: parityMemProvider must satisfy the internal ReaderProvider interface.
var _ modules_rw.ReaderProvider = (*parityMemProvider)(nil)

// PST-01: Column parity — intrinsics and all attribute types.
//
// TestParitySmokeTest is the CI-gated parity check. It writes a span with all
// intrinsic fields that are captured as block columns and all five attribute value
// types (string, int64, float64, bool, bytes) for span, resource, and scope
// namespaces. Reads back via the blockpack reader and asserts every column value
// matches the written input. Assertions are extended as reader support for additional
// columns (event:name, link:trace_id) is implemented.
func TestParitySmokeTest(t *testing.T) {
	var (
		traceID = [16]byte{
			0x01,
			0x02,
			0x03,
			0x04,
			0x05,
			0x06,
			0x07,
			0x08,
			0x09,
			0x0A,
			0x0B,
			0x0C,
			0x0D,
			0x0E,
			0x0F,
			0x10,
		}
		spanID   = []byte{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8}
		parentID = []byte{0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8}
	)

	span := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            spanID,
		ParentSpanId:      parentID,
		Name:              "parity.smoke.operation",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   3_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		TraceState:        "vendor=value",
		Status: &tracev1.Status{
			Code:    tracev1.Status_STATUS_CODE_ERROR,
			Message: "something went wrong",
		},
		Attributes: []*commonv1.KeyValue{
			{Key: "parity.str", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}}},
			{Key: "parity.int", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: -42}}},
			{
				Key:   "parity.float",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14159}},
			},
			{Key: "parity.bool", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}},
			{
				Key: "parity.bytes",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
				},
			},
		},
	}

	resourceAttrs := map[string]any{
		"service.name": "parity-svc",
		"res.int":      int64(99),
		"res.float":    1.23,
		"res.bool":     false,
		"res.bytes":    []byte{0x01, 0x02, 0x03},
	}

	scopeAttrs := map[string]any{
		"version": "v1.0.0",
		"weight":  int64(7),
		"ratio":   0.42,
		"enabled": true,
		"sig":     []byte{0xCA, 0xFE},
	}

	const resSchemaURL = "https://opentelemetry.io/schemas/1.21.0"
	const scopeSchemaURL = "https://opentelemetry.io/schemas/1.21.0/scope"

	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream: &buf,
	})
	require.NoError(t, err)
	require.NoError(t, w.AddSpan(traceID[:], span, resourceAttrs, resSchemaURL, scopeAttrs, scopeSchemaURL))
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&parityMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	require.Equal(t, 1, r.BlockCount(), "one flush must produce exactly one block")

	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block
	require.Equal(t, 1, b.SpanCount(), "block must contain exactly one span")

	// --- Intrinsic field assertions ---
	// These fields are stored in the intrinsic section only, not as block columns.
	// Use GetIntrinsicColumn to read them and look up (blockIdx=0, rowIdx=0).

	t.Run("trace:id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("trace:id")
		require.NoError(t, err)
		require.NotNil(t, col, "trace:id must be in intrinsic section")
		v := intrinsicBytesForRow(col, 0, 0)
		assert.Equal(t, traceID[:], v)
	})

	t.Run("span:id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:id")
		require.NoError(t, err)
		require.NotNil(t, col, "span:id must be in intrinsic section")
		v := intrinsicBytesForRow(col, 0, 0)
		assert.Equal(t, spanID, v)
	})

	t.Run("span:parent_id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:parent_id")
		require.NoError(t, err)
		require.NotNil(t, col, "span:parent_id must be in intrinsic section")
		v := intrinsicBytesForRow(col, 0, 0)
		assert.Equal(t, parentID, v)
	})

	t.Run("span:name", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:name")
		require.NoError(t, err)
		require.NotNil(t, col, "span:name must be in intrinsic section")
		v, ok := intrinsicDictStringForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, "parity.smoke.operation", v)
	})

	t.Run("span:kind", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:kind")
		require.NoError(t, err)
		require.NotNil(t, col, "span:kind must be in intrinsic section")
		v, ok := intrinsicDictInt64ForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, int64(tracev1.Span_SPAN_KIND_SERVER), v)
	})

	t.Run("span:start", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:start")
		require.NoError(t, err)
		require.NotNil(t, col, "span:start must be in intrinsic section")
		v, ok := intrinsicUint64ForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(1_000_000_000), v)
	})

	t.Run("span:end", func(t *testing.T) {
		// span:end is synthesized from span:start + span:duration on read.
		col, err := r.GetIntrinsicColumn("span:end")
		require.NoError(t, err)
		require.NotNil(t, col, "span:end must be synthesized from intrinsic section")
		v, ok := intrinsicUint64ForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(3_000_000_000), v)
	})

	t.Run("span:duration", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:duration")
		require.NoError(t, err)
		require.NotNil(t, col, "span:duration must be in intrinsic section")
		v, ok := intrinsicUint64ForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(2_000_000_000), v) // 3e9 - 1e9
	})

	t.Run("span:status", func(t *testing.T) {
		// span:status is now intrinsic-only; no longer written as a block column.
		col, err := r.GetIntrinsicColumn("span:status")
		require.NoError(t, err)
		require.NotNil(t, col, "span:status must be in intrinsic section for non-zero status code")
		v, ok := intrinsicDictInt64ForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, int64(tracev1.Status_STATUS_CODE_ERROR), v)
	})

	t.Run("span:status_message", func(t *testing.T) {
		// span:status_message is now intrinsic-only; no longer written as a block column.
		col, err := r.GetIntrinsicColumn("span:status_message")
		require.NoError(t, err)
		require.NotNil(t, col, "span:status_message must be in intrinsic section for non-empty message")
		v, ok := intrinsicDictStringForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, "something went wrong", v)
	})

	t.Run("trace:state", func(t *testing.T) {
		col := b.GetColumn("trace:state")
		require.NotNil(t, col, "trace:state must be present for non-empty state")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "vendor=value", v)
	})

	t.Run("resource:schema_url", func(t *testing.T) {
		col := b.GetColumn("resource:schema_url")
		require.NotNil(t, col, "resource:schema_url must be present")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, resSchemaURL, v)
	})

	t.Run("scope:schema_url", func(t *testing.T) {
		col := b.GetColumn("scope:schema_url")
		require.NotNil(t, col, "scope:schema_url must be present")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, scopeSchemaURL, v)
	})

	// --- Span attribute assertions ---

	t.Run("span.parity.str (string)", func(t *testing.T) {
		col := b.GetColumn("span.parity.str")
		require.NotNil(t, col, "span.parity.str must be present")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "hello", v)
	})

	t.Run("span.parity.int (int64)", func(t *testing.T) {
		col := b.GetColumn("span.parity.int")
		require.NotNil(t, col, "span.parity.int must be present")
		v, ok := col.Int64Value(0)
		assert.True(t, ok)
		assert.Equal(t, int64(-42), v)
	})

	t.Run("span.parity.float (float64)", func(t *testing.T) {
		col := b.GetColumn("span.parity.float")
		require.NotNil(t, col, "span.parity.float must be present")
		v, ok := col.Float64Value(0)
		assert.True(t, ok)
		assert.Equal(t, 3.14159, v)
	})

	t.Run("span.parity.bool (bool/true)", func(t *testing.T) {
		col := b.GetColumn("span.parity.bool")
		require.NotNil(t, col, "span.parity.bool must be present")
		v, ok := col.BoolValue(0)
		assert.True(t, ok)
		assert.True(t, v)
	})

	t.Run("span.parity.bytes (bytes)", func(t *testing.T) {
		col := b.GetColumn("span.parity.bytes")
		require.NotNil(t, col, "span.parity.bytes must be present")
		v, ok := col.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, v)
	})

	// --- Resource attribute assertions ---

	t.Run("resource.service.name (string)", func(t *testing.T) {
		// resource.service.name is now intrinsic-only; no longer written as a block column.
		col, err := r.GetIntrinsicColumn("resource.service.name")
		require.NoError(t, err)
		require.NotNil(t, col, "resource.service.name must be in intrinsic section")
		v, ok := intrinsicDictStringForRow(col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, "parity-svc", v)
	})

	t.Run("resource.res.int (int64)", func(t *testing.T) {
		col := b.GetColumn("resource.res.int")
		require.NotNil(t, col, "resource.res.int must be present")
		v, ok := col.Int64Value(0)
		assert.True(t, ok)
		assert.Equal(t, int64(99), v)
	})

	t.Run("resource.res.float (float64)", func(t *testing.T) {
		col := b.GetColumn("resource.res.float")
		require.NotNil(t, col, "resource.res.float must be present")
		v, ok := col.Float64Value(0)
		assert.True(t, ok)
		assert.Equal(t, 1.23, v)
	})

	t.Run("resource.res.bool (bool/false)", func(t *testing.T) {
		col := b.GetColumn("resource.res.bool")
		require.NotNil(t, col, "resource.res.bool must be present")
		v, ok := col.BoolValue(0)
		assert.True(t, ok)
		assert.False(t, v)
	})

	t.Run("resource.res.bytes (bytes)", func(t *testing.T) {
		col := b.GetColumn("resource.res.bytes")
		require.NotNil(t, col, "resource.res.bytes must be present")
		v, ok := col.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, v)
	})

	// --- Scope attribute assertions ---

	t.Run("scope.version (string)", func(t *testing.T) {
		col := b.GetColumn("scope.version")
		require.NotNil(t, col, "scope.version must be present")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "v1.0.0", v)
	})

	t.Run("scope.weight (int64)", func(t *testing.T) {
		col := b.GetColumn("scope.weight")
		require.NotNil(t, col, "scope.weight must be present")
		v, ok := col.Int64Value(0)
		assert.True(t, ok)
		assert.Equal(t, int64(7), v)
	})

	t.Run("scope.ratio (float64)", func(t *testing.T) {
		col := b.GetColumn("scope.ratio")
		require.NotNil(t, col, "scope.ratio must be present")
		v, ok := col.Float64Value(0)
		assert.True(t, ok)
		assert.Equal(t, 0.42, v)
	})

	t.Run("scope.enabled (bool/true)", func(t *testing.T) {
		col := b.GetColumn("scope.enabled")
		require.NotNil(t, col, "scope.enabled must be present")
		v, ok := col.BoolValue(0)
		assert.True(t, ok)
		assert.True(t, v)
	})

	t.Run("scope.sig (bytes)", func(t *testing.T) {
		col := b.GetColumn("scope.sig")
		require.NotNil(t, col, "scope.sig must be present")
		v, ok := col.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, []byte{0xCA, 0xFE}, v)
	})
}

// intrinsicBytesForRow returns the bytes value from a flat IntrinsicColumn for
// the given (blockIdx, rowIdx). Returns nil if not found.
func intrinsicBytesForRow(col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) []byte {
	for i, ref := range col.BlockRefs {
		if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
			return col.BytesValues[i]
		}
	}
	return nil
}

// intrinsicUint64ForRow returns the uint64 value from a flat IntrinsicColumn for
// the given (blockIdx, rowIdx). Returns 0, false if not found.
func intrinsicUint64ForRow(col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (uint64, bool) {
	for i, ref := range col.BlockRefs {
		if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
			return col.Uint64Values[i], true
		}
	}
	return 0, false
}

// intrinsicDictStringForRow returns the string value from a dict IntrinsicColumn for
// the given (blockIdx, rowIdx). Returns "", false if not found.
func intrinsicDictStringForRow(col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (string, bool) {
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
				return entry.Value, true
			}
		}
	}
	return "", false
}

// intrinsicDictInt64ForRow returns the int64 value from a dict IntrinsicColumn for
// the given (blockIdx, rowIdx). Returns 0, false if not found.
func intrinsicDictInt64ForRow(col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (int64, bool) {
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
				return entry.Int64Val, true
			}
		}
	}
	return 0, false
}

// --- Public API surface checks ---

// smokeReaderProvider is a minimal in-memory implementation of blockpack.ReaderProvider.
// Its presence is a compile-time assertion that the public interface is satisfied.
type smokeReaderProvider struct{ data []byte }

func (s *smokeReaderProvider) Size() (int64, error) { return int64(len(s.data)), nil }

func (s *smokeReaderProvider) ReadAt(p []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 || off > int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[off:])
	return n, nil
}

// Compile-time check: smokeReaderProvider must satisfy the public ReaderProvider interface.
var _ blockpack.ReaderProvider = (*smokeReaderProvider)(nil)

// TestDataTypeConstantsAreNonZero verifies that all public DataType constants
// exported from the blockpack package are non-zero (not DataTypeUnknown).
// A zero-value constant would indicate a broken alias or iota misconfiguration.
func TestDataTypeConstantsAreNonZero(t *testing.T) {
	constants := []blockpack.DataType{
		blockpack.DataTypeFooter,
		blockpack.DataTypeHeader,
		blockpack.DataTypeMetadata,
		blockpack.DataTypeTraceBloomFilter,
		blockpack.DataTypeTimestampIndex,
		blockpack.DataTypeBlock,
	}
	for _, dt := range constants {
		assert.NotZero(t, dt, "DataType constant must not be DataTypeUnknown (zero)")
	}
}

// TestReaderProviderInterfaceSatisfaction verifies that a struct implementing
// blockpack.ReaderProvider compiles and can be passed to blockpack.NewReaderFromProvider.
// The empty provider triggers a format parse error (not a panic or type assertion failure),
// confirming the interface wiring is correct.
func TestReaderProviderInterfaceSatisfaction(t *testing.T) {
	provider := &smokeReaderProvider{data: []byte{}}
	_, err := blockpack.NewReaderFromProvider(provider)
	// Empty data is not a valid blockpack file — a parse error is expected and correct.
	// A nil-pointer panic or interface mismatch would indicate a broken alias.
	require.Error(t, err, "empty provider must return a parse error, not panic")
}

// PST-05: SelectColumns parity — exact, extra-filtered, none-missing.
//
// These three tests verify that QueryOptions.SelectColumns enforces field
// selectivity correctly:
//   - PST-05a: only the requested columns appear in results (no extras)
//   - PST-05b: all requested columns appear in results (none missing)
//   - PST-05c: nil SelectColumns returns all columns written (backward compat)
//
// This is the correctness guarantee that prevents callers from receiving
// unexpected columns when they specify an explicit selection set.

func buildParityFile(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{OutputStream: &buf})
	require.NoError(t, err)
	traceID := [16]byte{0x10, 0x20}
	span := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7},
		Name:              "parity.select",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		Attributes: []*commonv1.KeyValue{
			{Key: "http.method", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}}},
			{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}}},
			{
				Key:   "db.system",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "postgres"}},
			},
		},
	}
	require.NoError(t, w.AddSpan(traceID[:], span, map[string]any{"service.name": "parity-select-svc"}, "", nil, ""))
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// PST-05a: No extra columns — only requested columns appear in results.
func TestSelectColumns_NoExtraColumns(t *testing.T) {
	data := buildParityFile(t)
	r, err := blockpack.NewReaderFromProvider(&smokeReaderProvider{data: data})
	require.NoError(t, err)

	results, _, err := blockpack.QueryTraceQL(r, `{ span.http.method = "GET" }`, blockpack.QueryOptions{
		SelectColumns: []string{"span.http.method", "resource.service.name"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, results, "query must return at least one match")

	for _, m := range results {
		if m.Fields == nil {
			continue
		}
		m.Fields.IterateFields(func(name string, _ any) bool {
			assert.True(t, name == "span.http.method" || name == "resource.service.name",
				"unexpected column %q returned — SelectColumns must filter to exactly the requested set", name)
			return true
		})
	}
}

// PST-05b: No missing columns — all requested columns are present in results.
func TestSelectColumns_NoMissingColumns(t *testing.T) {
	data := buildParityFile(t)
	r, err := blockpack.NewReaderFromProvider(&smokeReaderProvider{data: data})
	require.NoError(t, err)

	results, _, err := blockpack.QueryTraceQL(r, `{ span.http.method = "GET" }`, blockpack.QueryOptions{
		SelectColumns: []string{"span.http.method", "resource.service.name"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	for _, m := range results {
		if m.Fields == nil {
			continue
		}
		_, hasMethod := m.Fields.GetField("span.http.method")
		_, hasSvc := m.Fields.GetField("resource.service.name")
		assert.True(t, hasMethod, "span.http.method must be present in result fields")
		assert.True(t, hasSvc, "resource.service.name must be present in result fields")
	}
}

// PST-05c: nil SelectColumns returns all written columns (backward compat).
func TestSelectColumns_NilReturnsAll(t *testing.T) {
	data := buildParityFile(t)
	r, err := blockpack.NewReaderFromProvider(&smokeReaderProvider{data: data})
	require.NoError(t, err)

	results, _, err := blockpack.QueryTraceQL(r, `{ span.http.method = "GET" }`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	for _, m := range results {
		if m.Fields == nil {
			continue
		}
		var names []string
		m.Fields.IterateFields(func(name string, _ any) bool {
			names = append(names, name)
			return true
		})
		assert.Contains(
			t,
			names,
			"span.http.method",
			"nil SelectColumns must return all columns including span.http.method",
		)
		assert.Contains(
			t,
			names,
			"span.http.status_code",
			"nil SelectColumns must return all columns including span.http.status_code",
		)
		assert.Contains(
			t,
			names,
			"span.db.system",
			"nil SelectColumns must return all columns including span.db.system",
		)
	}
}
