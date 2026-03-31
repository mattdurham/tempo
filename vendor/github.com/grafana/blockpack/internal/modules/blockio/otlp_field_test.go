package blockio_test

// OFT — OTLP Full-Field Tests.
// These tests write a span with every OTLP field type populated and verify
// that each field round-trips correctly through the modules writer and reader.
// They serve as a completeness check: if a new OTLP field is added, a test
// here must fail or be extended to cover it.

import (
	"bytes"
	"testing"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// intrinsicBytesAt returns the bytes value for an intrinsic flat column at (blockIdx, rowIdx).
func intrinsicBytesAt(t *testing.T, col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) ([]byte, bool) {
	t.Helper()
	for i, ref := range col.BlockRefs {
		if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx && i < len(col.BytesValues) {
			return col.BytesValues[i], true
		}
	}
	return nil, false
}

// intrinsicUint64At returns the uint64 value for an intrinsic flat column at (blockIdx, rowIdx).
func intrinsicUint64At(t *testing.T, col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (uint64, bool) {
	t.Helper()
	for i, ref := range col.BlockRefs {
		if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx && i < len(col.Uint64Values) {
			return col.Uint64Values[i], true
		}
	}
	return 0, false
}

// intrinsicDictStringAt returns the string value for an intrinsic dict column at (blockIdx, rowIdx).
func intrinsicDictStringAt(t *testing.T, col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (string, bool) {
	t.Helper()
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
				return entry.Value, true
			}
		}
	}
	return "", false
}

// intrinsicDictInt64At returns the int64 value for an intrinsic dict column at (blockIdx, rowIdx).
// Int64 dict entries use Int64Val (not Value string).
func intrinsicDictInt64At(t *testing.T, col *modules_shared.IntrinsicColumn, blockIdx, rowIdx uint16) (int64, bool) {
	t.Helper()
	for _, entry := range col.DictEntries {
		for _, ref := range entry.BlockRefs {
			if ref.BlockIdx == blockIdx && ref.RowIdx == rowIdx {
				return entry.Int64Val, true
			}
		}
	}
	return 0, false
}

// ---- OFT-01: All intrinsic fields ----

// TestOTLPField_Intrinsics writes a span with every OTLP intrinsic field set
// and verifies each corresponding column is present and holds the correct value.
func TestOTLPField_Intrinsics(t *testing.T) {
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
		spanID       = []byte{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8}
		parentSpanID = []byte{0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8}
	)

	span := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              "full.coverage.operation",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   3_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		TraceState:        "vendor=value",
		Status: &tracev1.Status{
			Code:    tracev1.Status_STATUS_CODE_ERROR,
			Message: "something went wrong",
		},
	}

	const resSchema = "https://opentelemetry.io/schemas/1.21.0"
	const scopeSchema = "https://opentelemetry.io/schemas/1.21.0/scope"

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan(
		traceID[:], span,
		map[string]any{"service.name": "coverage-svc"},
		resSchema, nil, scopeSchema,
	))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	require.Equal(t, 1, r.BlockCount())
	// Intrinsic columns are now stored exclusively in the intrinsic section (not block columns).
	// Block columns contain only user attributes. Use r.GetIntrinsicColumn for intrinsic fields.
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block
	require.Equal(t, 1, b.SpanCount())

	getFlat := func(t *testing.T, name string) *modules_shared.IntrinsicColumn {
		t.Helper()
		col, err := r.GetIntrinsicColumn(name)
		require.NoError(t, err, "GetIntrinsicColumn %q", name)
		require.NotNil(t, col, "intrinsic column %q must be present", name)
		return col
	}
	getDict := func(t *testing.T, name string) *modules_shared.IntrinsicColumn {
		t.Helper()
		col, err := r.GetIntrinsicColumn(name)
		require.NoError(t, err, "GetIntrinsicColumn %q", name)
		require.NotNil(t, col, "intrinsic column %q must be present", name)
		return col
	}

	// NOTE-005: trace:id, span:id, span:parent_id are no longer stored in the intrinsic
	// section. They are stored only in block column payloads (dual-storage preserved for
	// block payloads; intrinsic accumulator no longer feeds them).
	t.Run("trace:id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("trace:id")
		require.NoError(t, err)
		assert.Nil(t, col, "trace:id must NOT be in intrinsic section (NOTE-005)")
		bcol := b.GetColumn("trace:id")
		require.NotNil(t, bcol, "trace:id must be present as a block column")
		v, ok := bcol.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, traceID[:], v)
	})

	t.Run("span:id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:id")
		require.NoError(t, err)
		assert.Nil(t, col, "span:id must NOT be in intrinsic section (NOTE-005)")
		bcol := b.GetColumn("span:id")
		require.NotNil(t, bcol, "span:id must be present as a block column")
		v, ok := bcol.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, spanID, v)
	})

	t.Run("span:parent_id", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:parent_id")
		require.NoError(t, err)
		assert.Nil(t, col, "span:parent_id must NOT be in intrinsic section (NOTE-005)")
		bcol := b.GetColumn("span:parent_id")
		require.NotNil(t, bcol, "span:parent_id must be present as a block column")
		v, ok := bcol.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, parentSpanID, v)
	})

	t.Run("span:name", func(t *testing.T) {
		col := getDict(t, "span:name")
		v, ok := intrinsicDictStringAt(t, col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, "full.coverage.operation", v)
	})

	t.Run("span:kind", func(t *testing.T) {
		col := getDict(t, "span:kind")
		v, ok := intrinsicDictInt64At(t, col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, int64(tracev1.Span_SPAN_KIND_SERVER), v)
	})

	t.Run("span:start", func(t *testing.T) {
		col := getFlat(t, "span:start")
		v, ok := intrinsicUint64At(t, col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(1_000_000_000), v)
	})

	t.Run("span:end", func(t *testing.T) {
		// span:end is derived from span:start + span:duration (both in intrinsic section).
		startCol := getFlat(t, "span:start")
		durCol := getFlat(t, "span:duration")
		start, okS := intrinsicUint64At(t, startCol, 0, 0)
		dur, okD := intrinsicUint64At(t, durCol, 0, 0)
		assert.True(t, okS && okD, "span:start and span:duration must be present")
		assert.Equal(t, uint64(3_000_000_000), start+dur)
	})

	t.Run("span:duration", func(t *testing.T) {
		col := getFlat(t, "span:duration")
		v, ok := intrinsicUint64At(t, col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(2_000_000_000), v) // 3e9 - 1e9
	})

	t.Run("span:status (non-zero code)", func(t *testing.T) {
		col := getDict(t, "span:status")
		v, ok := intrinsicDictInt64At(t, col, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, int64(tracev1.Status_STATUS_CODE_ERROR), v)
	})

	// NOTE-005: span:status_message is no longer stored in the intrinsic section.
	t.Run("span:status_message (non-empty)", func(t *testing.T) {
		col, err := r.GetIntrinsicColumn("span:status_message")
		require.NoError(t, err)
		assert.Nil(t, col, "span:status_message must NOT be in intrinsic section (NOTE-005)")
		bcol := b.GetColumn("span:status_message")
		require.NotNil(t, bcol, "span:status_message must be present as a block column")
		v, ok := bcol.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "something went wrong", v)
	})

	t.Run("trace:state (non-empty)", func(t *testing.T) {
		col := b.GetColumn("trace:state")
		require.NotNil(t, col, "trace:state must be present for non-empty state")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "vendor=value", v)
	})

	t.Run("resource:schema_url (non-empty)", func(t *testing.T) {
		col := b.GetColumn("resource:schema_url")
		require.NotNil(t, col, "resource:schema_url must be present for non-empty URL")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, resSchema, v)
	})

	t.Run("scope:schema_url (non-empty)", func(t *testing.T) {
		col := b.GetColumn("scope:schema_url")
		require.NotNil(t, col, "scope:schema_url must be present for non-empty URL")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, scopeSchema, v)
	})
}

// ---- OFT-02: Conditional intrinsics absent when zero/empty ----

// TestOTLPField_ConditionalIntrinsicsAbsent verifies that span:status,
// span:status_message, and trace:state are NOT written when zero/empty.
func TestOTLPField_ConditionalIntrinsicsAbsent(t *testing.T) {
	span := &tracev1.Span{
		TraceId:           []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "op",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
		// Status.Code == 0 (unset), Status.Message == "", TraceState == "".
	}

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan([]byte{0x01}, span, map[string]any{"service.name": "svc"}, "", nil, ""))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

	assert.Nil(t, b.GetColumn("span:status"), "span:status must be absent for zero status code")
	assert.Nil(t, b.GetColumn("span:status_message"), "span:status_message must be absent for empty message")
	assert.Nil(t, b.GetColumn("trace:state"), "trace:state must be absent for empty state")
	assert.Nil(t, b.GetColumn("resource:schema_url"), "resource:schema_url must be absent when empty")
	assert.Nil(t, b.GetColumn("scope:schema_url"), "scope:schema_url must be absent when empty")
}

// ---- OFT-03: All attribute value types ----

// TestOTLPField_AllAttrTypes writes spans with span attributes of every OTLP
// value type and verifies each round-trips through the correct column type.
func TestOTLPField_AllAttrTypes(t *testing.T) {
	traceID := []byte{0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "attr.types",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		Attributes: []*commonv1.KeyValue{
			{Key: "attr.str", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello"}}},
			{Key: "attr.int", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: -42}}},
			{Key: "attr.float", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14159}}},
			{Key: "attr.bool", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}},
			{
				Key: "attr.bytes",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
				},
			},
		},
	}

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan(traceID, span, map[string]any{"service.name": "attr-svc"}, "", nil, ""))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

	t.Run("span.attr.str (string)", func(t *testing.T) {
		col := b.GetColumn("span.attr.str")
		require.NotNil(t, col, "span.attr.str must be present")
		v, ok := col.StringValue(0)
		assert.True(t, ok)
		assert.Equal(t, "hello", v)
	})

	t.Run("span.attr.int (int64)", func(t *testing.T) {
		col := b.GetColumn("span.attr.int")
		require.NotNil(t, col, "span.attr.int must be present")
		v, ok := col.Int64Value(0)
		assert.True(t, ok)
		assert.Equal(t, int64(-42), v)
	})

	t.Run("span.attr.float (float64)", func(t *testing.T) {
		col := b.GetColumn("span.attr.float")
		require.NotNil(t, col, "span.attr.float must be present")
		v, ok := col.Float64Value(0)
		assert.True(t, ok)
		assert.Equal(t, 3.14159, v)
	})

	t.Run("span.attr.bool (bool)", func(t *testing.T) {
		col := b.GetColumn("span.attr.bool")
		require.NotNil(t, col, "span.attr.bool must be present")
		v, ok := col.BoolValue(0)
		assert.True(t, ok)
		assert.True(t, v)
	})

	t.Run("span.attr.bytes (bytes)", func(t *testing.T) {
		col := b.GetColumn("span.attr.bytes")
		require.NotNil(t, col, "span.attr.bytes must be present")
		v, ok := col.BytesValue(0)
		assert.True(t, ok)
		assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, v)
	})
}

// ---- OFT-04: Resource attribute value types ----

// TestOTLPField_ResourceAttrTypes writes resource attributes of every value type
// via the map[string]any path and verifies each round-trips correctly.
func TestOTLPField_ResourceAttrTypes(t *testing.T) {
	traceID := []byte{0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "res.attr.types",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
	}

	resourceAttrs := map[string]any{
		"service.name": "res-attr-svc",
		"res.int":      int64(99),
		"res.float":    1.23,
		"res.bool":     false,
		"res.bytes":    []byte{0x01, 0x02, 0x03},
	}

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan(traceID, span, resourceAttrs, "", nil, ""))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

	t.Run("resource.service.name (string)", func(t *testing.T) {
		// resource.service.name is intrinsic-only (no block column); read from intrinsic section.
		intrCol, err := r.GetIntrinsicColumn("resource.service.name")
		require.NoError(t, err)
		require.NotNil(t, intrCol, "resource.service.name must be in intrinsic section")
		v, ok := intrinsicDictStringAt(t, intrCol, 0, 0)
		assert.True(t, ok)
		assert.Equal(t, "res-attr-svc", v)
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
}

// ---- OFT-05: Scope attribute types ----

// TestOTLPField_ScopeAttrs writes scope attributes via the scopeAttrs parameter
// and verifies they appear under the "scope." column prefix.
func TestOTLPField_ScopeAttrs(t *testing.T) {
	traceID := []byte{0x05, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "scope.attr.test",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
	}

	scopeAttrs := map[string]any{
		"version": "v1.0.0",
		"weight":  int64(7),
	}

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan(traceID, span, map[string]any{"service.name": "scope-svc"}, "", scopeAttrs, ""))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

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
}

// ---- OFT-06: Events and links ----

// TestOTLPField_EventsAndLinks writes a span with events and links and verifies
// the relevant columns are present (or skips if not yet implemented).
func TestOTLPField_EventsAndLinks(t *testing.T) {
	traceID := []byte{0x06, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	linkTraceID := []byte{0xFF, 0xFE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}
	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            []byte{0x01, 0, 0, 0, 0, 0, 0, 0},
		Name:              "events.links.test",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Kind:              tracev1.Span_SPAN_KIND_CLIENT,
		Events: []*tracev1.Span_Event{
			{Name: "exception", TimeUnixNano: 1_500_000_000},
			{Name: "checkpoint"},
		},
		Links: []*tracev1.Span_Link{
			{
				TraceId: linkTraceID,
				SpanId:  []byte{0xFE, 0, 0, 0, 0, 0, 0, 0x02},
			},
		},
	}

	var buf bytes.Buffer
	w := mustNewModulesWriter(t, &buf, 0)
	require.NoError(t, w.AddSpan(traceID, span, map[string]any{"service.name": "evt-svc"}, "", nil, ""))
	mustFlushModulesWriter(t, w)

	r := openModulesReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

	t.Run("event:name (blob of null-separated names)", func(t *testing.T) {
		col := b.GetColumn("event:name")
		if col == nil {
			t.Skip("event:name column not yet implemented as a block column")
		}
		v, ok := col.BytesValue(0)
		assert.True(t, ok, "event:name row 0 must be present")
		s := string(v)
		assert.Contains(t, s, "exception", "event:name blob must contain 'exception'")
		assert.Contains(t, s, "checkpoint", "event:name blob must contain 'checkpoint'")
	})

	t.Run("link:trace_id (concatenated 16-byte IDs)", func(t *testing.T) {
		col := b.GetColumn("link:trace_id")
		if col == nil {
			t.Skip("link:trace_id column not yet implemented as a block column")
		}
		v, ok := col.BytesValue(0)
		assert.True(t, ok, "link:trace_id row 0 must be present")
		assert.Equal(t, len(linkTraceID), len(v), "link:trace_id must contain one 16-byte trace ID")
		assert.Equal(t, linkTraceID, v[:16])
	})
}
