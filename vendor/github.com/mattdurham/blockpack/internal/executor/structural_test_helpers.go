package executor

import (
	"encoding/hex"
	"fmt"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// testSpan represents a span in a test trace with configurable structure and attributes.
// Used for building test traces with known parent-child relationships.
type testSpan struct {
	SpanID   string                 // Hex-encoded span ID (16 hex chars = 8 bytes)
	ParentID string                 // Hex-encoded parent span ID (empty for root spans)
	Name     string                 // Span name
	Attrs    map[string]interface{} // Span attributes (supports string, int64, bool, float64)
}

// makeTestTrace builds a test trace with known structure from a slice of testSpan definitions.
// Returns a slice of BlockpackSpanMatch that can be used in structural query tests.
//
// The traceID should be a 32-character hex string (16 bytes).
// Each span's SpanID should be a 16-character hex string (8 bytes).
// Parent-child relationships are established via ParentID field.
//
// Example: Building a linear chain A → B → C
//
//	spans := []testSpan{
//	    {SpanID: "1111111111111111", ParentID: "", Name: "A"},
//	    {SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "B"},
//	    {SpanID: "3333333333333333", ParentID: "2222222222222222", Name: "C"},
//	}
//	matches := makeTestTrace("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", spans)
//
// Example: Building a tree A(B, C) with B(D)
//
//	spans := []testSpan{
//	    {SpanID: "1111111111111111", ParentID: "", Name: "A"},
//	    {SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "B"},
//	    {SpanID: "3333333333333333", ParentID: "1111111111111111", Name: "C"},
//	    {SpanID: "4444444444444444", ParentID: "2222222222222222", Name: "D"},
//	}
//	matches := makeTestTrace("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", spans)
func makeTestTrace(traceID string, spans []testSpan) []BlockpackSpanMatch {
	matches := make([]BlockpackSpanMatch, 0, len(spans))

	for _, span := range spans {
		fields := make(map[string]any)

		// Add intrinsic fields
		fields["trace:id"] = traceID
		fields["span:id"] = span.SpanID

		if span.ParentID != "" {
			fields["span:parent_id"] = span.ParentID
		}

		if span.Name != "" {
			fields["span:name"] = span.Name
		}

		// Add custom attributes
		for k, v := range span.Attrs {
			fields[k] = v
		}

		matches = append(matches, BlockpackSpanMatch{
			TraceID: traceID,
			SpanID:  span.SpanID,
			Fields:  NewMapSpanFields(fields),
		})
	}

	return matches
}

// makeTestTraceOTLP builds an OTLP TracesData structure for testing.
// This is useful when you need to write test data to blockpack format.
// Returns a TracesData that can be passed to blockpackio.Writer.AddTracesData().
//
// The traceID should be a 32-character hex string (16 bytes).
// Each span's SpanID should be a 16-character hex string (8 bytes).
//
// Example:
//
//	spans := []testSpan{
//	    {SpanID: "1111111111111111", ParentID: "", Name: "root"},
//	    {SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "child"},
//	}
//	traces := makeTestTraceOTLP("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", spans)
//	writer := blockpackio.NewWriter(1024)
//	writer.AddTracesData(traces)
func makeTestTraceOTLP(traceID string, spans []testSpan) *tracev1.TracesData {
	traceIDBytes, err := hex.DecodeString(traceID)
	if err != nil || len(traceIDBytes) != 16 {
		panic(fmt.Sprintf("invalid traceID: must be 32 hex chars, got %q", traceID))
	}

	otlpSpans := make([]*tracev1.Span, 0, len(spans))

	for _, span := range spans {
		spanIDBytes, err := hex.DecodeString(span.SpanID)
		if err != nil || len(spanIDBytes) != 8 {
			panic(fmt.Sprintf("invalid spanID %q: must be 16 hex chars", span.SpanID))
		}

		otlpSpan := &tracev1.Span{
			TraceId:           traceIDBytes,
			SpanId:            spanIDBytes,
			Name:              span.Name,
			StartTimeUnixNano: 1000000, // Default start time
			EndTimeUnixNano:   2000000, // Default end time
		}

		if span.ParentID != "" {
			parentIDBytes, err := hex.DecodeString(span.ParentID)
			if err != nil || len(parentIDBytes) != 8 {
				panic(fmt.Sprintf("invalid parentID %q: must be 16 hex chars", span.ParentID))
			}
			otlpSpan.ParentSpanId = parentIDBytes
		}

		// Convert attributes to OTLP format
		if len(span.Attrs) > 0 {
			otlpSpan.Attributes = make([]*commonv1.KeyValue, 0, len(span.Attrs))
			for k, v := range span.Attrs {
				kv := &commonv1.KeyValue{Key: k}
				switch val := v.(type) {
				case string:
					kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}}
				case int64:
					kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}}
				case int:
					kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(val)}}
				case bool:
					kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: val}}
				case float64:
					kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: val}}
				default:
					panic(fmt.Sprintf("unsupported attribute type for key %q: %T", k, v))
				}
				otlpSpan.Attributes = append(otlpSpan.Attributes, kv)
			}
		}

		otlpSpans = append(otlpSpans, otlpSpan)
	}

	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-service"}}},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: otlpSpans,
					},
				},
			},
		},
	}
}

// extractSpanIDs extracts span IDs from a slice of BlockpackSpanMatch.
// Returns a slice of span IDs in the same order as the input matches.
// Useful for validating query results contain the expected spans.
//
// Example:
//
//	matches := makeTestTrace(traceID, spans)
//	spanIDs := extractSpanIDs(matches)
//	require.ElementsMatch(t, []string{"1111111111111111", "2222222222222222"}, spanIDs)
func extractSpanIDs(matches []BlockpackSpanMatch) []string {
	ids := make([]string, 0, len(matches))
	for _, match := range matches {
		ids = append(ids, match.SpanID)
	}
	return ids
}

// filterSpans filters a slice of BlockpackSpanMatch to only include spans with IDs in the given list.
// Returns a new slice containing only the matching spans, in the order they appear in the input.
// Useful for extracting a subset of spans for validation.
//
// Example:
//
//	allSpans := makeTestTrace(traceID, allTestSpans)
//	childSpans := filterSpans(allSpans, []string{"2222222222222222", "3333333333333333"})
//	require.Len(t, childSpans, 2)
func filterSpans(matches []BlockpackSpanMatch, spanIDs []string) []BlockpackSpanMatch {
	idSet := make(map[string]struct{}, len(spanIDs))
	for _, id := range spanIDs {
		idSet[id] = struct{}{}
	}

	filtered := make([]BlockpackSpanMatch, 0, len(spanIDs))
	for _, match := range matches {
		if _, exists := idSet[match.SpanID]; exists {
			filtered = append(filtered, match)
		}
	}
	return filtered
}

// extractSpanNames extracts span names from a slice of BlockpackSpanMatch.
// Returns a slice of span names in the same order as the input matches.
// Returns empty string for spans without a name field.
// Useful for validating query results contain spans with expected names.
//
// Example:
//
//	matches := makeTestTrace(traceID, spans)
//	names := extractSpanNames(matches)
//	require.Equal(t, []string{"A", "B", "C"}, names)
func extractSpanNames(matches []BlockpackSpanMatch) []string {
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		if name, ok := match.Fields.GetField("span:name"); ok {
			if nameStr, isStr := name.(string); isStr {
				names = append(names, nameStr)
			} else {
				names = append(names, "")
			}
		} else {
			names = append(names, "")
		}
	}
	return names
}

// getSpanAttribute retrieves an attribute value from a BlockpackSpanMatch.
// Returns the attribute value and true if found, nil and false otherwise.
// Useful for validating specific attribute values in test results.
//
// Example:
//
//	match := matches[0]
//	val, ok := getSpanAttribute(match, "http.method")
//	require.True(t, ok)
//	require.Equal(t, "GET", val)
func getSpanAttribute(match BlockpackSpanMatch, attrName string) (interface{}, bool) {
	return match.Fields.GetField(attrName)
}

// TestTraceFixtures provides common trace structures for testing.
// Each fixture returns a traceID and slice of testSpan definitions.
type TestTraceFixtures struct{}

// LinearChain returns a linear trace: A → B → C → D
// Each span has exactly one parent (except root A).
func (TestTraceFixtures) LinearChain() (string, []testSpan) {
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
	spans := []testSpan{
		{SpanID: "1111111111111111", ParentID: "", Name: "A"},
		{SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "B"},
		{SpanID: "3333333333333333", ParentID: "2222222222222222", Name: "C"},
		{SpanID: "4444444444444444", ParentID: "3333333333333333", Name: "D"},
	}
	return traceID, spans
}

// Tree returns a tree structure: A has children B and C, B has child D
// Root A → [B, C], B → D
func (TestTraceFixtures) Tree() (string, []testSpan) {
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2"
	spans := []testSpan{
		{SpanID: "1111111111111111", ParentID: "", Name: "A"},
		{SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "B"},
		{SpanID: "3333333333333333", ParentID: "1111111111111111", Name: "C"},
		{SpanID: "4444444444444444", ParentID: "2222222222222222", Name: "D"},
	}
	return traceID, spans
}

// MultipleRoots returns a trace with multiple root spans: A(B), C(D)
// Root A → B, Root C → D
// Note: This is technically invalid in OpenTelemetry (traces should have one root),
// but useful for testing edge cases.
func (TestTraceFixtures) MultipleRoots() (string, []testSpan) {
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3"
	spans := []testSpan{
		{SpanID: "1111111111111111", ParentID: "", Name: "A"},
		{SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "B"},
		{SpanID: "3333333333333333", ParentID: "", Name: "C"},
		{SpanID: "4444444444444444", ParentID: "3333333333333333", Name: "D"},
	}
	return traceID, spans
}

// Complex returns a complex trace structure with various patterns:
// Root → [Branch1, Branch2], Branch1 → [Leaf1, Leaf2], Branch2 → Leaf3
func (TestTraceFixtures) Complex() (string, []testSpan) {
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4"
	spans := []testSpan{
		{SpanID: "1111111111111111", ParentID: "", Name: "Root"},
		{SpanID: "2222222222222222", ParentID: "1111111111111111", Name: "Branch1"},
		{SpanID: "3333333333333333", ParentID: "1111111111111111", Name: "Branch2"},
		{SpanID: "4444444444444444", ParentID: "2222222222222222", Name: "Leaf1"},
		{SpanID: "5555555555555555", ParentID: "2222222222222222", Name: "Leaf2"},
		{SpanID: "6666666666666666", ParentID: "3333333333333333", Name: "Leaf3"},
	}
	return traceID, spans
}

// WithAttributes returns a simple trace with spans containing various attribute types.
// Useful for testing attribute filtering and access.
func (TestTraceFixtures) WithAttributes() (string, []testSpan) {
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa5"
	spans := []testSpan{
		{
			SpanID:   "1111111111111111",
			ParentID: "",
			Name:     "Root",
			Attrs: map[string]interface{}{
				"http.method":      "GET",
				"http.status_code": int64(200),
				"custom.flag":      true,
				"custom.duration":  float64(123.45),
			},
		},
		{
			SpanID:   "2222222222222222",
			ParentID: "1111111111111111",
			Name:     "Child",
			Attrs: map[string]interface{}{
				"db.system":    "postgresql",
				"db.statement": "SELECT * FROM users",
				"db.rows":      int64(42),
			},
		},
	}
	return traceID, spans
}
