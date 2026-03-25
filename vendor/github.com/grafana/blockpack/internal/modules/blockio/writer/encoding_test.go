package writer_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// makeAttrKV creates a single KeyValue with a string value.
func makeAttrKV(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

// makeIntAttrKV creates a single KeyValue with an int64 value.
func makeIntAttrKV(key string, val int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}},
	}
}

// makeBoolAttrKV creates a single KeyValue with a bool value.
func makeBoolAttrKV(key string, val bool) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: val}},
	}
}

// makeFloatAttrKV creates a single KeyValue with a float64 value.
func makeFloatAttrKV(key string, val float64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: val}},
	}
}

// TestEncoding_DeltaUint64_SmallRange verifies that timestamp columns within a narrow window
// flush without error (delta uint64 encoding is selected automatically) (ENC-03).
func TestEncoding_DeltaUint64_SmallRange(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 1000)

	// All start times within a 60-second window (in nanoseconds).
	baseTime := uint64(1_700_000_000_000_000_000)
	for i := range 100 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "ts-op",
			StartTimeUnixNano: baseTime + uint64(i)*600_000_000, // spread over ~60s
			EndTimeUnixNano:   baseTime + uint64(i)*600_000_000 + 1_000_000,
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_XOR_SpanID verifies that sequential 8-byte span IDs flush without error
// (XOR encoding is selected for span:id) (ENC-04).
func TestEncoding_XOR_SpanID(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 100 {
		spanID := make([]byte, 8)
		// Shared prefix: first 6 bytes identical, last 2 bytes differ.
		spanID[0] = 0xAB
		spanID[1] = 0xCD
		spanID[2] = 0xEF
		spanID[3] = 0x01
		spanID[4] = 0x23
		spanID[5] = 0x45
		spanID[6] = byte(i)
		spanID[7] = byte(i >> 8)
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            spanID,
			Name:              "xor-op",
			StartTimeUnixNano: 1_000_000_000 + uint64(i)*1000,
			EndTimeUnixNano:   2_000_000_000 + uint64(i)*1000,
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_Prefix_URL verifies that URL attributes with a common prefix flush without error
// (prefix encoding is selected) (ENC-05).
func TestEncoding_Prefix_URL(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 100 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "url-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeAttrKV("http.url", fmt.Sprintf("https://api.example.com/v1/items/%d", i)),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_Dictionary_String verifies that a low-cardinality string column flushes without error
// (dictionary encoding is selected) (ENC-01).
func TestEncoding_Dictionary_String(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	methods := []string{"GET", "POST", "DELETE", "PUT", "PATCH"}
	for i := range 200 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i%255) + 1),
			SpanId:            makeSpanID(byte(i%255) + 1),
			Name:              methods[i%len(methods)],
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeAttrKV("http.method", methods[i%len(methods)]),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_Bool verifies that boolean attributes flush without error.
func TestEncoding_Bool(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 100 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i%255) + 1),
			SpanId:            makeSpanID(byte(i%255) + 1),
			Name:              "bool-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeBoolAttrKV("cache.hit", i%2 == 0),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_Float64 verifies that float64 attributes flush without error.
func TestEncoding_Float64(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 50 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "float-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeFloatAttrKV("latency.ms", float64(i)*0.5+1.23),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_NullHandling verifies that sparse columns (>50% null ratio) flush without error (ENC-02 / ENC-06).
func TestEncoding_NullHandling(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// 100 spans: 50 have "rare.field", 50 do not.
	for i := range 100 {
		attrs := []*commonv1.KeyValue{}
		if i%2 == 0 {
			attrs = append(attrs, makeAttrKV("rare.field", "value"))
		}
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i%255) + 1),
			SpanId:            makeSpanID(byte(i%255) + 1),
			Name:              "sparse-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes:        attrs,
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_RLE_SingleValue verifies that a column with a single repeated value flushes
// without error (RLE encoding is selected) (ENC-05).
func TestEncoding_RLE_SingleValue(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 100 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i%255) + 1),
			SpanId:            makeSpanID(byte(i%255) + 1),
			Name:              "rle-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeAttrKV("deployment.env", "production"),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_DeltaDictionary_TraceID verifies that trace IDs from a small set of traces
// flush without error (DeltaDictionary encoding is selected for trace:id) (ENC-07).
func TestEncoding_DeltaDictionary_TraceID(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// 10 spans from 3 different traces.
	traceIDs := [][]byte{makeTraceID(10), makeTraceID(20), makeTraceID(30)}
	distribution := []int{4, 4, 2}
	row := 0
	for traceIdx, count := range distribution {
		for j := range count {
			span := &tracev1.Span{
				TraceId:           traceIDs[traceIdx],
				SpanId:            makeSpanID(byte(row + 1)),
				Name:              fmt.Sprintf("op-%d-%d", traceIdx, j),
				StartTimeUnixNano: uint64(row) * 1_000,
				EndTimeUnixNano:   uint64(row)*1_000 + 500,
			}
			td := makeTracesData("svc", []*tracev1.Span{span})
			require.NoError(t, w.AddTracesData(td))
			row++
		}
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_SpanEvents_InlineBytes verifies that span events (array columns) flush
// without error (inline bytes encoding) (ENC-08).
func TestEncoding_SpanEvents_InlineBytes(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 5 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "event-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Events: []*tracev1.Span_Event{
				{Name: "exception", TimeUnixNano: 1_100_000_000},
				{Name: "log", TimeUnixNano: 1_200_000_000},
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_MultipleBlocksPreserveEncoding verifies that encoding selections are consistent
// across block boundaries when MaxBlockSpans forces multiple blocks.
func TestEncoding_MultipleBlocksPreserveEncoding(t *testing.T) {
	var buf bytes.Buffer
	// Small MaxBlockSpans to force multiple blocks.
	w := newTestWriter(t, &buf, 10)

	methods := []string{"GET", "POST", "DELETE"}
	for i := range 30 {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i%255) + 1),
			SpanId:            makeSpanID(byte(i%255) + 1),
			Name:              "multi-block-op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000),
			EndTimeUnixNano:   uint64(1_000_000_000 + i*1000 + 500),
			Attributes: []*commonv1.KeyValue{
				makeAttrKV("http.method", methods[i%len(methods)]),
				makeAttrKV("http.url", fmt.Sprintf("https://api.example.com/items/%d", i)),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_Int64WithNegatives verifies that int64 columns with negative values flush without error.
func TestEncoding_Int64WithNegatives(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	values := []int64{-5, 0, 3, -100, 50, 42, -999, 1}
	for i, v := range values {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "int-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeIntAttrKV("latency", v),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_SpanLinks verifies that span links (link trace IDs) flush without error.
func TestEncoding_SpanLinks(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 5 {
		linkedTraceID := makeTraceID(byte(i + 100))
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "linked-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Links: []*tracev1.Span_Link{
				{TraceId: linkedTraceID, SpanId: makeSpanID(byte(i + 200))},
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_SparseHighNullRatio verifies that a column with >50% null ratio uses sparse encoding
// and flushes without error (ENC-06).
func TestEncoding_SparseHighNullRatio(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	// 10 spans: only span 2 has "rare.attr".
	for i := range 10 {
		attrs := []*commonv1.KeyValue{}
		if i == 2 {
			attrs = append(attrs, makeAttrKV("rare.attr", "present"))
		}
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "sparse-high-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes:        attrs,
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_UUIDAutoDetection verifies that a string column of UUIDs is stored without error (ENC-09).
func TestEncoding_UUIDAutoDetection(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	uuids := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b811-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b812-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b814-9dad-11d1-80b4-00c04fd430c8",
	}
	for i, u := range uuids {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "uuid-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes: []*commonv1.KeyValue{
				makeAttrKV("request.id", u),
			},
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}

// TestEncoding_UUIDRoundtrip verifies that UUID string attributes survive the write→read roundtrip:
// the reader must return the original UUID-formatted string (not raw bytes) and the column type
// must be ColumnTypeUUID so the distinction from raw bytes columns is preserved (ENC-09).
func TestEncoding_UUIDRoundtrip(t *testing.T) {
	const uuidVal = "550e8400-e29b-41d4-a716-446655440000"
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i, u := range []string{
		uuidVal,
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"6ba7b811-9dad-11d1-80b4-00c04fd430c8",
	} {
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "uuid-rt",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes:        []*commonv1.KeyValue{makeAttrKV("request.id", u)},
		}
		require.NoError(t, w.AddTracesData(makeTracesData("svc", []*tracev1.Span{span})))
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openWriterTestReader(t, buf.Bytes())
	bwb, err := r.GetBlockWithBytes(0, nil, nil)
	require.NoError(t, err)
	b := bwb.Block

	// The column must exist and be typed as UUID.
	col := b.GetColumn("span.request.id")
	require.NotNil(t, col, "span.request.id column must be present")
	require.Equal(t, shared.ColumnTypeUUID, col.Type, "UUID-valued string columns must be stored as ColumnTypeUUID")

	// StringValue must return the original UUID-formatted string, not raw bytes.
	// Row order may vary due to MinHash sorting; check that uuidVal appears somewhere in the column.
	found := false
	for i := range col.SpanCount {
		v, ok := col.StringValue(i)
		if ok && v == uuidVal {
			found = true
			break
		}
	}
	require.True(t, found, "UUID value %q must appear in span.request.id column", uuidVal)
}

// TestEncoding_MixedPresence verifies that a mix of present and absent values across many columns flushes correctly.
func TestEncoding_MixedPresence(t *testing.T) {
	var buf bytes.Buffer
	w := newTestWriter(t, &buf, 0)

	for i := range 20 {
		attrs := []*commonv1.KeyValue{}
		// Even spans get attribute A, odd spans get attribute B.
		if i%2 == 0 {
			attrs = append(attrs, makeAttrKV("attr.a", fmt.Sprintf("val-a-%d", i)))
		} else {
			attrs = append(attrs, makeAttrKV("attr.b", fmt.Sprintf("val-b-%d", i)))
		}
		span := &tracev1.Span{
			TraceId:           makeTraceID(byte(i + 1)),
			SpanId:            makeSpanID(byte(i + 1)),
			Name:              "mixed-op",
			StartTimeUnixNano: 1_000_000_000,
			EndTimeUnixNano:   2_000_000_000,
			Attributes:        attrs,
		}
		td := makeTracesData("svc", []*tracev1.Span{span})
		require.NoError(t, w.AddTracesData(td))
	}

	_, err := w.Flush()
	require.NoError(t, err)
}
