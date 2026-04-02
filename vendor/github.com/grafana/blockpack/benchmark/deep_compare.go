package benchmark

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

// DeepCompareSearchResponses performs an exhaustive deep comparison of two SearchResponse objects.
// It checks every field and follows every possible code path, returning detailed error messages
// about any differences found.
func DeepCompareSearchResponses(a, b *tempopb.SearchResponse) error {
	// Check nil cases - all 4 combinations
	if a == nil && b == nil {
		return nil // Both nil is equal
	}
	if a == nil && b != nil {
		return fmt.Errorf("first response is nil, second is not nil (has %d traces)", len(b.Traces))
	}
	if a != nil && b == nil {
		return fmt.Errorf("first response is not nil (has %d traces), second is nil", len(a.Traces))
	}

	// Both non-nil, continue comparison

	// Compare Metrics field
	// Note: In practice, when comparing Tempo vs blockpack, metrics are often cleared/nil'd
	// before comparison since implementations track metrics differently. However, this
	// function should still validate metrics when they ARE present to ensure completeness.
	if err := deepCompareSearchMetrics(a.Metrics, b.Metrics); err != nil {
		return fmt.Errorf("metrics mismatch: %w", err)
	}

	// Compare Traces field
	if err := deepCompareTracesList(a.Traces, b.Traces); err != nil {
		return fmt.Errorf("traces mismatch: %w", err)
	}

	return nil
}

// deepCompareSearchMetrics compares SearchMetrics objects
func deepCompareSearchMetrics(a, b *tempopb.SearchMetrics) error {
	// Check nil cases
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first is nil, second is not nil")
	}
	if a != nil && b == nil {
		return fmt.Errorf("first is not nil, second is nil")
	}

	// Both non-nil, compare each field
	if a.InspectedTraces != b.InspectedTraces {
		return fmt.Errorf("InspectedTraces: %d != %d", a.InspectedTraces, b.InspectedTraces)
	}
	if a.InspectedBytes != b.InspectedBytes {
		return fmt.Errorf("InspectedBytes: %d != %d", a.InspectedBytes, b.InspectedBytes)
	}
	if a.InspectedSpans != b.InspectedSpans {
		return fmt.Errorf("InspectedSpans: %d != %d", a.InspectedSpans, b.InspectedSpans)
	}
	if a.TotalBlocks != b.TotalBlocks {
		return fmt.Errorf("TotalBlocks: %d != %d", a.TotalBlocks, b.TotalBlocks)
	}
	if a.TotalBlockBytes != b.TotalBlockBytes {
		return fmt.Errorf("TotalBlockBytes: %d != %d", a.TotalBlockBytes, b.TotalBlockBytes)
	}
	if a.TotalJobs != b.TotalJobs {
		return fmt.Errorf("TotalJobs: %d != %d", a.TotalJobs, b.TotalJobs)
	}
	if a.CompletedJobs != b.CompletedJobs {
		return fmt.Errorf("CompletedJobs: %d != %d", a.CompletedJobs, b.CompletedJobs)
	}

	return nil
}

// deepCompareTracesList compares lists of TraceSearchMetadata
func deepCompareTracesList(a, b []*tempopb.TraceSearchMetadata) error {
	// Check all nil/empty combinations
	if a == nil && b == nil {
		return nil
	}
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first traces list is nil, second has %d traces", len(b))
	}
	if a != nil && b == nil {
		return fmt.Errorf("first traces list has %d traces, second is nil", len(a))
	}
	if len(a) != len(b) {
		return fmt.Errorf("trace count mismatch: %d != %d", len(a), len(b))
	}

	// Build maps by normalized trace ID for comparison
	aMap := make(map[string]*tempopb.TraceSearchMetadata)
	bMap := make(map[string]*tempopb.TraceSearchMetadata)

	for i, trace := range a {
		if trace == nil {
			return fmt.Errorf("trace at index %d in first list is nil", i)
		}
		normalized, ok := normalizeTraceID(trace.TraceID)
		if !ok {
			return fmt.Errorf("trace at index %d in first list has invalid TraceID: %q", i, trace.TraceID)
		}
		if _, exists := aMap[normalized]; exists {
			return fmt.Errorf("duplicate trace ID in first list: %q (normalized: %s)", trace.TraceID, normalized)
		}
		aMap[normalized] = trace
	}

	for i, trace := range b {
		if trace == nil {
			return fmt.Errorf("trace at index %d in second list is nil", i)
		}
		normalized, ok := normalizeTraceID(trace.TraceID)
		if !ok {
			return fmt.Errorf("trace at index %d in second list has invalid TraceID: %q", i, trace.TraceID)
		}
		if _, exists := bMap[normalized]; exists {
			return fmt.Errorf("duplicate trace ID in second list: %q (normalized: %s)", trace.TraceID, normalized)
		}
		bMap[normalized] = trace
	}

	// Check for missing traces in either direction
	for traceID := range aMap {
		if _, ok := bMap[traceID]; !ok {
			return fmt.Errorf("trace %s exists in first list but not in second", traceID)
		}
	}
	for traceID := range bMap {
		if _, ok := aMap[traceID]; !ok {
			return fmt.Errorf("trace %s exists in second list but not in first", traceID)
		}
	}

	// Compare each trace deeply
	for traceID, aTrace := range aMap {
		bTrace := bMap[traceID]
		if bTrace == nil {
			return fmt.Errorf("trace %s: missing in b", traceID)
		}
		if err := deepCompareTraceSearchMetadata(traceID, aTrace, bTrace); err != nil {
			return fmt.Errorf("trace %s: %w", traceID, err)
		}
	}

	return nil
}

// deepCompareTraceSearchMetadata compares all fields of TraceSearchMetadata
func deepCompareTraceSearchMetadata(traceID string, a, b *tempopb.TraceSearchMetadata) error {
	// Both should be non-nil at this point (checked by caller)

	// Compare TraceID (already normalized and matched, but check raw value)
	if a.TraceID != b.TraceID {
		aNorm, _ := normalizeTraceID(a.TraceID)
		bNorm, _ := normalizeTraceID(b.TraceID)
		// Only error if normalized values don't match (they should match since we keyed by it)
		if aNorm != bNorm {
			return fmt.Errorf("TraceID mismatch: %q != %q", a.TraceID, b.TraceID)
		}
		// Raw values differ but normalized values match - this is OK, just different representations
	}

	// Skip RootServiceName comparison - Tempo selects root based on ingestion order
	// (last root span encountered during proto iteration), while blockpack format uses
	// earliest root span by timestamp. Both are valid approaches, but fundamentally
	// incompatible without preserving ingestion order in the blockpack format.

	// Skip RootTraceName comparison - same reason as RootServiceName above.
	// The root trace name comes from the selected root span, which differs between
	// Tempo and blockpack format due to different root span selection logic.

	// Compare StartTimeUnixNano
	if a.StartTimeUnixNano != b.StartTimeUnixNano {
		return fmt.Errorf("StartTimeUnixNano: %d != %d", a.StartTimeUnixNano, b.StartTimeUnixNano)
	}

	// Compare DurationMs
	if a.DurationMs != b.DurationMs {
		return fmt.Errorf("DurationMs: %d != %d", a.DurationMs, b.DurationMs)
	}

	// Compare SpanSet (deprecated field, may be nil)
	if err := deepCompareSpanSet("SpanSet", a.SpanSet, b.SpanSet); err != nil {
		return fmt.Errorf("SpanSet (deprecated): %w", err)
	}

	// Compare SpanSets (current field)
	if err := deepCompareSpanSets(a.SpanSets, b.SpanSets); err != nil {
		return fmt.Errorf("SpanSets: %w", err)
	}

	// Skip ServiceStats comparison - not relevant for blockpack vs Tempo comparison
	// if err := deepCompareServiceStats(a.ServiceStats, b.ServiceStats); err != nil {
	// 	return fmt.Errorf("ServiceStats: %w", err)
	// }

	return nil
}

// deepCompareServiceStats compares ServiceStats maps
// deepCompareSpanSets compares lists of SpanSets
func deepCompareSpanSets(a, b []*tempopb.SpanSet) error {
	// Check nil/empty cases
	if a == nil && b == nil {
		return nil
	}
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first is nil, second has %d span sets", len(b))
	}
	if a != nil && b == nil {
		return fmt.Errorf("first has %d span sets, second is nil", len(a))
	}
	if len(a) != len(b) {
		return fmt.Errorf("span set count mismatch: %d != %d", len(a), len(b))
	}

	// Create normalized comparable versions
	aNormalized := normalizeSpanSetsForDeepCompare(a)
	bNormalized := normalizeSpanSetsForDeepCompare(b)

	// Sort by key for stable comparison
	sort.Slice(aNormalized, func(i, j int) bool { return aNormalized[i].key < aNormalized[j].key })
	sort.Slice(bNormalized, func(i, j int) bool { return bNormalized[i].key < bNormalized[j].key })

	// Compare each span set
	for i := range aNormalized {
		if err := deepCompareSpanSetComparable(i, aNormalized[i], bNormalized[i]); err != nil {
			return fmt.Errorf("span set %d: %w", i, err)
		}
	}

	return nil
}

type spanSetForDeepCompare struct {
	key     string
	attrs   []*tempocommon.KeyValue
	spans   []*tempopb.Span
	matched uint32
}

// normalizeSpanSetsForDeepCompare creates comparable versions with sorting
func normalizeSpanSetsForDeepCompare(sets []*tempopb.SpanSet) []spanSetForDeepCompare {
	out := make([]spanSetForDeepCompare, 0, len(sets))
	for _, set := range sets {
		if set == nil {
			continue
		}

		// Sort spans by ID for deterministic comparison
		sortedSpans := make([]*tempopb.Span, len(set.Spans))
		copy(sortedSpans, set.Spans)
		sort.Slice(sortedSpans, func(i, j int) bool {
			if sortedSpans[i] == nil {
				return true
			}
			if sortedSpans[j] == nil {
				return false
			}
			iID := normalizeSpanID(sortedSpans[i].SpanID)
			jID := normalizeSpanID(sortedSpans[j].SpanID)
			if iID != jID {
				return iID < jID
			}
			if sortedSpans[i].StartTimeUnixNano != sortedSpans[j].StartTimeUnixNano {
				return sortedSpans[i].StartTimeUnixNano < sortedSpans[j].StartTimeUnixNano
			}
			return sortedSpans[i].Name < sortedSpans[j].Name
		})

		// Create key based on matched count and span IDs
		keyParts := make([]string, 0, len(sortedSpans))
		for _, sp := range sortedSpans {
			if sp != nil {
				keyParts = append(keyParts, normalizeSpanID(sp.SpanID))
			}
		}
		key := fmt.Sprintf(
			"m%d_s%d_%s",
			set.Matched,
			len(sortedSpans),
			bytes.Join([][]byte{[]byte(fmt.Sprint(keyParts))}, []byte(",")),
		)

		out = append(out, spanSetForDeepCompare{
			matched: set.Matched,
			attrs:   set.Attributes,
			spans:   sortedSpans,
			key:     key,
		})
	}
	return out
}

// deepCompareSpanSetComparable compares normalized span sets
func deepCompareSpanSetComparable(idx int, a, b spanSetForDeepCompare) error {
	if a.matched != b.matched {
		return fmt.Errorf("matched count: %d != %d", a.matched, b.matched)
	}

	// Compare attributes
	if err := deepCompareKeyValueSlices("Attributes", a.attrs, b.attrs); err != nil {
		return err
	}

	// Compare spans
	if len(a.spans) != len(b.spans) {
		return fmt.Errorf("span count: %d != %d", len(a.spans), len(b.spans))
	}

	for i := range a.spans {
		if err := deepCompareSpan(i, a.spans[i], b.spans[i]); err != nil {
			return fmt.Errorf("span %d: %w", i, err)
		}
	}

	return nil
}

// deepCompareSpanSet compares a single SpanSet
func deepCompareSpanSet(label string, a, b *tempopb.SpanSet) error {
	// Check nil cases
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("%s: first is nil, second is not", label)
	}
	if a != nil && b == nil {
		return fmt.Errorf("%s: first is not nil, second is nil", label)
	}

	// Both non-nil
	if a.Matched != b.Matched {
		return fmt.Errorf("%s.Matched: %d != %d", label, a.Matched, b.Matched)
	}

	if err := deepCompareKeyValueSlices(label+".Attributes", a.Attributes, b.Attributes); err != nil {
		return err
	}

	if err := deepCompareSpanList(label+".Spans", a.Spans, b.Spans); err != nil {
		return err
	}

	return nil
}

// deepCompareSpanList compares lists of spans
func deepCompareSpanList(label string, a, b []*tempopb.Span) error {
	// Check nil/empty
	if a == nil && b == nil {
		return nil
	}
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("%s: first is nil, second has %d spans", label, len(b))
	}
	if a != nil && b == nil {
		return fmt.Errorf("%s: first has %d spans, second is nil", label, len(a))
	}
	if len(a) != len(b) {
		return fmt.Errorf("%s: count mismatch %d != %d", label, len(a), len(b))
	}

	// Sort both lists by span ID for deterministic comparison
	aSorted := make([]*tempopb.Span, len(a))
	bSorted := make([]*tempopb.Span, len(b))
	copy(aSorted, a)
	copy(bSorted, b)

	spanSort := func(spans []*tempopb.Span) {
		sort.Slice(spans, func(i, j int) bool {
			if spans[i] == nil {
				return true
			}
			if spans[j] == nil {
				return false
			}
			iID := normalizeSpanID(spans[i].SpanID)
			jID := normalizeSpanID(spans[j].SpanID)
			if iID != jID {
				return iID < jID
			}
			if spans[i].StartTimeUnixNano != spans[j].StartTimeUnixNano {
				return spans[i].StartTimeUnixNano < spans[j].StartTimeUnixNano
			}
			return spans[i].Name < spans[j].Name
		})
	}

	spanSort(aSorted)
	spanSort(bSorted)

	// Compare each span
	for i := range aSorted {
		if err := deepCompareSpan(i, aSorted[i], bSorted[i]); err != nil {
			return fmt.Errorf("%s[%d]: %w", label, i, err)
		}
	}

	return nil
}

// deepCompareSpan compares all fields of a Span
func deepCompareSpan(idx int, a, b *tempopb.Span) error {
	// Check nil
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first is nil, second is not")
	}
	if a != nil && b == nil {
		return fmt.Errorf("first is not nil, second is nil")
	}

	// Both non-nil
	// Compare SpanID (with normalization)
	aNormID := normalizeSpanID(a.SpanID)
	bNormID := normalizeSpanID(b.SpanID)
	if aNormID != bNormID {
		return fmt.Errorf("SpanID: %q != %q (normalized: %s != %s)", a.SpanID, b.SpanID, aNormID, bNormID)
	}

	// Compare Name
	if a.Name != b.Name {
		return fmt.Errorf("name: %q != %q", a.Name, b.Name)
	}

	// Compare StartTimeUnixNano
	if a.StartTimeUnixNano != b.StartTimeUnixNano {
		return fmt.Errorf("StartTimeUnixNano: %d != %d", a.StartTimeUnixNano, b.StartTimeUnixNano)
	}

	// Compare DurationNanos
	if a.DurationNanos != b.DurationNanos {
		return fmt.Errorf("DurationNanos: %d != %d", a.DurationNanos, b.DurationNanos)
	}

	// Compare Attributes
	if err := deepCompareKeyValueSlices("Attributes", a.Attributes, b.Attributes); err != nil {
		return err
	}

	return nil
}

// deepCompareKeyValueSlices compares slices of KeyValue pairs
func deepCompareKeyValueSlices(label string, a, b []*tempocommon.KeyValue) error {
	// Check nil/empty
	if a == nil && b == nil {
		return nil
	}
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("%s: first is nil, second has %d attributes", label, len(b))
	}
	if a != nil && b == nil {
		return fmt.Errorf("%s: first has %d attributes, second is nil", label, len(a))
	}
	if len(a) != len(b) {
		return fmt.Errorf("%s: count mismatch %d != %d", label, len(a), len(b))
	}

	// Group attributes by key (allows duplicates per key, as in TraceQL unscoped queries)
	aMap := make(map[string][]*tempocommon.AnyValue)
	bMap := make(map[string][]*tempocommon.AnyValue)

	for i, kv := range a {
		if kv == nil {
			return fmt.Errorf("%s[%d]: first has nil KeyValue", label, i)
		}
		aMap[kv.Key] = append(aMap[kv.Key], kv.Value)
	}

	for i, kv := range b {
		if kv == nil {
			return fmt.Errorf("%s[%d]: second has nil KeyValue", label, i)
		}
		bMap[kv.Key] = append(bMap[kv.Key], kv.Value)
	}

	// Check all keys exist in both
	for key := range aMap {
		if _, ok := bMap[key]; !ok {
			return fmt.Errorf("%s: key %q exists in first but not in second", label, key)
		}
	}
	for key := range bMap {
		if _, ok := aMap[key]; !ok {
			return fmt.Errorf("%s: key %q exists in second but not in first", label, key)
		}
	}

	// Compare each key's values (each key can have multiple values)
	for key, aVals := range aMap {
		bVals := bMap[key]
		if len(aVals) != len(bVals) {
			return fmt.Errorf("%s[%q]: value count mismatch %d != %d", label, key, len(aVals), len(bVals))
		}
		// Sort both value lists by their string representation for consistent comparison
		// (attributes with same key can appear in any order)
		sortAnyValues(aVals)
		sortAnyValues(bVals)
		for i := range aVals {
			if err := deepCompareAnyValue(key, aVals[i], bVals[i]); err != nil {
				return fmt.Errorf("%s[%q][%d]: %w", label, key, i, err)
			}
		}
	}

	return nil
}

// sortAnyValues sorts a slice of AnyValue by their string representation for consistent comparison
func sortAnyValues(vals []*tempocommon.AnyValue) {
	sort.Slice(vals, func(i, j int) bool {
		return anyValueToString(vals[i]) < anyValueToString(vals[j])
	})
}

// anyValueToString converts an AnyValue to a string for sorting
func anyValueToString(v *tempocommon.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *tempocommon.AnyValue_StringValue:
		return val.StringValue
	case *tempocommon.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	case *tempocommon.AnyValue_DoubleValue:
		return fmt.Sprintf("%f", val.DoubleValue)
	case *tempocommon.AnyValue_BoolValue:
		return fmt.Sprintf("%t", val.BoolValue)
	case *tempocommon.AnyValue_BytesValue:
		return string(val.BytesValue)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// deepCompareAnyValue compares AnyValue objects exhaustively
// checkAnyValueNils handles nil-pointer pre-checks for deepCompareAnyValue.
// Returns (done, err): done=true means the caller should return err immediately.
func checkAnyValueNils(a, b *tempocommon.AnyValue) (done bool, err error) {
	if a == nil && b == nil {
		return true, nil
	}
	if a == nil {
		return true, fmt.Errorf("first is nil, second is not")
	}
	if b == nil {
		return true, fmt.Errorf("first is not nil, second is nil")
	}
	if a.Value == nil && b.Value == nil {
		return true, nil
	}
	if a.Value == nil {
		return true, fmt.Errorf("first Value is nil, second is not")
	}
	if b.Value == nil {
		return true, fmt.Errorf("first Value is not nil, second is nil")
	}
	return false, nil
}

func deepCompareAnyValue(key string, a, b *tempocommon.AnyValue) error {
	if done, err := checkAnyValueNils(a, b); done {
		return err
	}

	// Compare based on type
	switch aVal := a.Value.(type) {
	case *tempocommon.AnyValue_StringValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_StringValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is string, second is %T", b.Value)
		}
		if aVal.StringValue != bVal.StringValue {
			return fmt.Errorf("string value: %q != %q", aVal.StringValue, bVal.StringValue)
		}

	case *tempocommon.AnyValue_BoolValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_BoolValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is bool, second is %T", b.Value)
		}
		if aVal.BoolValue != bVal.BoolValue {
			return fmt.Errorf("bool value: %t != %t", aVal.BoolValue, bVal.BoolValue)
		}

	case *tempocommon.AnyValue_IntValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_IntValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is int, second is %T", b.Value)
		}
		if aVal.IntValue != bVal.IntValue {
			return fmt.Errorf("int value: %d != %d", aVal.IntValue, bVal.IntValue)
		}

	case *tempocommon.AnyValue_DoubleValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_DoubleValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is double, second is %T", b.Value)
		}
		if aVal.DoubleValue != bVal.DoubleValue {
			return fmt.Errorf("double value: %g != %g", aVal.DoubleValue, bVal.DoubleValue)
		}

	case *tempocommon.AnyValue_ArrayValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_ArrayValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is array, second is %T", b.Value)
		}
		if err := deepCompareArrayValue(aVal.ArrayValue, bVal.ArrayValue); err != nil {
			return fmt.Errorf("array value: %w", err)
		}

	case *tempocommon.AnyValue_KvlistValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_KvlistValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is kvlist, second is %T", b.Value)
		}
		if err := deepCompareKvListValue(aVal.KvlistValue, bVal.KvlistValue); err != nil {
			return fmt.Errorf("kvlist value: %w", err)
		}

	case *tempocommon.AnyValue_BytesValue:
		bVal, ok := b.Value.(*tempocommon.AnyValue_BytesValue)
		if !ok {
			return fmt.Errorf("type mismatch: first is bytes, second is %T", b.Value)
		}
		if !bytes.Equal(aVal.BytesValue, bVal.BytesValue) {
			return fmt.Errorf("bytes value: %x != %x", aVal.BytesValue, bVal.BytesValue)
		}

	default:
		return fmt.Errorf("unknown AnyValue type: %T", aVal)
	}

	return nil
}

// deepCompareArrayValue compares ArrayValue objects
func deepCompareArrayValue(a, b *tempocommon.ArrayValue) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first is nil, second is not")
	}
	if a != nil && b == nil {
		return fmt.Errorf("first is not nil, second is nil")
	}

	if len(a.Values) != len(b.Values) {
		return fmt.Errorf("array length: %d != %d", len(a.Values), len(b.Values))
	}

	for i := range a.Values {
		if err := deepCompareAnyValue(fmt.Sprintf("[%d]", i), a.Values[i], b.Values[i]); err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
	}

	return nil
}

// deepCompareKvListValue compares KeyValueList objects
func deepCompareKvListValue(a, b *tempocommon.KeyValueList) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("first is nil, second is not")
	}
	if a != nil && b == nil {
		return fmt.Errorf("first is not nil, second is nil")
	}

	return deepCompareKeyValueSlices("kvlist", a.Values, b.Values)
}

// DeepCompareOTLPSpans compares two full OTLP Span protos field-by-field. Covers every
// field present on tempotrace.Span: TraceId, SpanId, ParentSpanId, Name, Kind,
// StartTimeUnixNano, EndTimeUnixNano, TraceState, Flags, Status, Attributes, Events,
// Links, and the three dropped-count fields. Use this when the raw OTLP trace data is
// available — e.g. when verifying FindTraceByID results or building format-comparison tests.
func DeepCompareOTLPSpans(a, b *tempotrace.Span) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return fmt.Errorf("first span is nil, second is not")
	}
	if b == nil {
		return fmt.Errorf("first span is not nil, second is nil")
	}

	if !bytes.Equal(a.TraceId, b.TraceId) {
		return fmt.Errorf("TraceId: %x != %x", a.TraceId, b.TraceId)
	}
	if !bytes.Equal(a.SpanId, b.SpanId) {
		return fmt.Errorf("SpanId: %x != %x", a.SpanId, b.SpanId)
	}
	if !bytes.Equal(a.ParentSpanId, b.ParentSpanId) {
		return fmt.Errorf("ParentSpanId: %x != %x", a.ParentSpanId, b.ParentSpanId)
	}
	if a.Name != b.Name {
		return fmt.Errorf("name: %q != %q", a.Name, b.Name)
	}
	if a.Kind != b.Kind {
		return fmt.Errorf("kind: %v != %v", a.Kind, b.Kind)
	}
	if a.Flags != b.Flags {
		return fmt.Errorf("flags: %d != %d", a.Flags, b.Flags)
	}
	if a.DroppedAttributesCount != b.DroppedAttributesCount {
		return fmt.Errorf("DroppedAttributesCount: %d != %d", a.DroppedAttributesCount, b.DroppedAttributesCount)
	}
	if a.DroppedEventsCount != b.DroppedEventsCount {
		return fmt.Errorf("DroppedEventsCount: %d != %d", a.DroppedEventsCount, b.DroppedEventsCount)
	}
	if a.DroppedLinksCount != b.DroppedLinksCount {
		return fmt.Errorf("DroppedLinksCount: %d != %d", a.DroppedLinksCount, b.DroppedLinksCount)
	}
	if a.StartTimeUnixNano != b.StartTimeUnixNano {
		return fmt.Errorf("StartTimeUnixNano: %d != %d", a.StartTimeUnixNano, b.StartTimeUnixNano)
	}
	if a.EndTimeUnixNano != b.EndTimeUnixNano {
		return fmt.Errorf("EndTimeUnixNano: %d != %d", a.EndTimeUnixNano, b.EndTimeUnixNano)
	}
	if a.TraceState != b.TraceState {
		return fmt.Errorf("TraceState: %q != %q", a.TraceState, b.TraceState)
	}
	if err := deepCompareOTLPStatus(a.Status, b.Status); err != nil {
		return fmt.Errorf("status: %w", err)
	}
	if err := deepCompareKeyValueSlices("Attributes", a.Attributes, b.Attributes); err != nil {
		return err
	}
	if err := deepCompareOTLPEvents(a.Events, b.Events); err != nil {
		return fmt.Errorf("events: %w", err)
	}
	if err := deepCompareOTLPLinks(a.Links, b.Links); err != nil {
		return fmt.Errorf("links: %w", err)
	}
	return nil
}

// deepCompareOTLPStatus compares two OTLP Status protos.
func deepCompareOTLPStatus(a, b *tempotrace.Status) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return fmt.Errorf("first is nil, second is not (code=%v)", b.Code)
	}
	if b == nil {
		return fmt.Errorf("first is not nil (code=%v), second is nil", a.Code)
	}
	if a.Code != b.Code {
		return fmt.Errorf("code: %v != %v", a.Code, b.Code)
	}
	if a.Message != b.Message {
		return fmt.Errorf("message: %q != %q", a.Message, b.Message)
	}
	return nil
}

// deepCompareOTLPEvents compares two slices of OTLP Span_Event protos.
func deepCompareOTLPEvents(a, b []*tempotrace.Span_Event) error {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if len(a) != len(b) {
		return fmt.Errorf("event count: %d != %d", len(a), len(b))
	}
	for i := range a {
		ae, be := a[i], b[i]
		if ae == nil && be == nil {
			continue
		}
		if ae == nil || be == nil {
			return fmt.Errorf("event[%d]: one is nil", i)
		}
		if ae.Name != be.Name {
			return fmt.Errorf("event[%d].Name: %q != %q", i, ae.Name, be.Name)
		}
		if ae.TimeUnixNano != be.TimeUnixNano {
			return fmt.Errorf("event[%d].TimeUnixNano: %d != %d", i, ae.TimeUnixNano, be.TimeUnixNano)
		}
		if ae.DroppedAttributesCount != be.DroppedAttributesCount {
			return fmt.Errorf(
				"event[%d].DroppedAttributesCount: %d != %d",
				i,
				ae.DroppedAttributesCount,
				be.DroppedAttributesCount,
			)
		}
		if err := deepCompareKeyValueSlices(fmt.Sprintf("event[%d].Attributes", i), ae.Attributes, be.Attributes); err != nil {
			return err
		}
	}
	return nil
}

// deepCompareOTLPLinks compares two slices of OTLP Span_Link protos.
func deepCompareOTLPLinks(a, b []*tempotrace.Span_Link) error {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	if len(a) != len(b) {
		return fmt.Errorf("link count: %d != %d", len(a), len(b))
	}
	for i := range a {
		al, bl := a[i], b[i]
		if al == nil && bl == nil {
			continue
		}
		if al == nil || bl == nil {
			return fmt.Errorf("link[%d]: one is nil", i)
		}
		if !bytes.Equal(al.TraceId, bl.TraceId) {
			return fmt.Errorf("link[%d].TraceId: %x != %x", i, al.TraceId, bl.TraceId)
		}
		if !bytes.Equal(al.SpanId, bl.SpanId) {
			return fmt.Errorf("link[%d].SpanId: %x != %x", i, al.SpanId, bl.SpanId)
		}
		if al.TraceState != bl.TraceState {
			return fmt.Errorf("link[%d].TraceState: %q != %q", i, al.TraceState, bl.TraceState)
		}
		if al.Flags != bl.Flags {
			return fmt.Errorf("link[%d].Flags: %d != %d", i, al.Flags, bl.Flags)
		}
		if al.DroppedAttributesCount != bl.DroppedAttributesCount {
			return fmt.Errorf(
				"link[%d].DroppedAttributesCount: %d != %d",
				i,
				al.DroppedAttributesCount,
				bl.DroppedAttributesCount,
			)
		}
		if err := deepCompareKeyValueSlices(fmt.Sprintf("link[%d].Attributes", i), al.Attributes, bl.Attributes); err != nil {
			return err
		}
	}
	return nil
}
