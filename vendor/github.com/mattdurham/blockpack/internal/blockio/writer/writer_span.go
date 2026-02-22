package writer

import (
	"fmt"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// AddTracesData ingests normalized traces into the writer.
func (w *Writer) AddTracesData(td *tracev1.TracesData) error {
	return w.AddTracesDataWithRaw(td, td)
}

// AddTracesDataWithRaw ingests normalized traces while preserving raw OTLP resource and scope metadata.
// The raw parameter provides access to original schema URLs and instrumentation scope information
// that are preserved during normalization. Used by internal/otlpconvert for Tempo compatibility.
func (w *Writer) AddTracesDataWithRaw(td *tracev1.TracesData, raw *tracev1.TracesData) error {
	if td == nil || raw == nil {
		return fmt.Errorf("nil traces data")
	}
	if len(td.ResourceSpans) != len(raw.ResourceSpans) {
		return fmt.Errorf("resource span count mismatch: %d != %d", len(td.ResourceSpans), len(raw.ResourceSpans))
	}
	for rIdx, rs := range td.ResourceSpans {
		rawRS := raw.ResourceSpans[rIdx]
		resource := rs.GetResource()
		resAttrs := resource.GetAttributes()
		rawResource := rawRS.GetResource()
		rawResourceSchema := rawRS.GetSchemaUrl()
		if len(rs.ScopeSpans) != len(rawRS.ScopeSpans) {
			return fmt.Errorf("scope span count mismatch: %d != %d", len(rs.ScopeSpans), len(rawRS.ScopeSpans))
		}
		for sIdx, ss := range rs.GetScopeSpans() {
			rawSS := rawRS.ScopeSpans[sIdx]
			rawScope := rawSS.GetScope()
			rawScopeSchema := rawSS.GetSchemaUrl()
			if len(ss.Spans) != len(rawSS.Spans) {
				return fmt.Errorf("span count mismatch: %d != %d", len(ss.Spans), len(rawSS.Spans))
			}
			for _, span := range ss.GetSpans() {
				if err := w.AddSpan(span, resAttrs, rawResource, rawResourceSchema, rawScope, rawScopeSchema); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// AddSpan buffers a single span for writing. This is the low-level API used
// internally by AddTracesData. Most callers should use AddTracesData instead.
func (w *Writer) AddSpan(
	span *tracev1.Span,
	resourceAttrs []*commonv1.KeyValue,
	rawResource *resourcev1.Resource,
	rawResourceSchema string,
	rawScope *commonv1.InstrumentationScope,
	rawScopeSchema string,
) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	// Buffer span for sorting (instead of immediately adding to block)
	buffered := &BufferedSpan{
		Span:           span,
		ResourceAttrs:  resourceAttrs,
		RawResource:    rawResource,
		ResourceSchema: rawResourceSchema,
		RawScope:       rawScope,
		ScopeSchema:    rawScopeSchema,
	}

	w.spanBuffer = append(w.spanBuffer, buffered)
	w.totalSpans++

	return nil
}

// addSpanToBlock adds a buffered span to a block (called during Flush after sorting)
// setSpanIntrinsicFields sets span intrinsic fields (name, kind, status, dropped counts, etc).
func (w *Writer) setSpanIntrinsicFields(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	if err := w.setString(b, "span:name", spanIdx, span.GetName()); err != nil {
		return fmt.Errorf("set span:name: %w", err)
	}
	if err := w.setInt64(b, "span:kind", spanIdx, int64(span.GetKind())); err != nil {
		return fmt.Errorf("set span:kind: %w", err)
	}
	if span.GetStatus() != nil {
		if err := w.setInt64(b, "span:status", spanIdx, int64(span.GetStatus().GetCode())); err != nil {
			return fmt.Errorf("set span:status: %w", err)
		}
		if err := w.setString(b, "span:status_message", spanIdx, span.GetStatus().GetMessage()); err != nil {
			return fmt.Errorf("set span:status_message: %w", err)
		}
	}
	if traceState := span.GetTraceState(); traceState != "" {
		if err := w.setString(b, "span:trace_state", spanIdx, traceState); err != nil {
			return fmt.Errorf("set span:trace_state: %w", err)
		}
	}
	if dropped := span.GetDroppedAttributesCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
			return fmt.Errorf("set span:dropped_attributes_count: %w", err)
		}
	}
	if dropped := span.GetDroppedEventsCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_events_count", spanIdx, uint64(dropped)); err != nil {
			return fmt.Errorf("set span:dropped_events_count: %w", err)
		}
	}
	if dropped := span.GetDroppedLinksCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_links_count", spanIdx, uint64(dropped)); err != nil {
			return fmt.Errorf("set span:dropped_links_count: %w", err)
		}
	}
	return nil
}

// setSpanIdentifiersAndTimestamps sets trace/span IDs, timestamps, and updates block ranges.
func (w *Writer) setSpanIdentifiersAndTimestamps(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	traceID := span.GetTraceId()
	spanID := span.GetSpanId()
	if err := w.setBytes(b, "trace:id", spanIdx, traceID); err != nil {
		return fmt.Errorf("set trace:id: %w", err)
	}
	if err := w.setBytes(b, "span:id", spanIdx, spanID); err != nil {
		return fmt.Errorf("set span:id: %w", err)
	}
	parentID := span.GetParentSpanId()
	if len(parentID) == 0 {
		parentID = nil
	}
	if err := w.setBytes(b, "span:parent_id", spanIdx, parentID); err != nil {
		return fmt.Errorf("set span:parent_id: %w", err)
	}

	// span:start (needed for time-based queries and aggregations)
	start := span.GetStartTimeUnixNano()
	if err := w.setUint64(b, "span:start", spanIdx, start); err != nil {
		return fmt.Errorf("set span:start: %w", err)
	}
	if start < b.minStart {
		b.minStart = start
	}
	if start > b.maxStart {
		b.maxStart = start
	}
	b.updateTraceRange(traceID)

	// span:end (needed for trace duration calculations and compatibility)
	end := span.GetEndTimeUnixNano()
	if err := w.setUint64(b, "span:end", spanIdx, end); err != nil {
		return fmt.Errorf("set span:end: %w", err)
	}

	// span:duration
	//nolint:gosec
	duration := uint64(0)
	if end >= start {
		duration = end - start
	}
	// setUint64 automatically calls recordDedicatedValue, which handles range-bucketed columns
	if err := w.setUint64(b, "span:duration", spanIdx, duration); err != nil {
		return fmt.Errorf("set span:duration: %w", err)
	}

	return nil
}

// setResourceAttributes sets resource attributes for a span.
func (w *Writer) setResourceAttributes(
	b *blockBuilder,
	resourceAttrs []*commonv1.KeyValue,
	rawResource *resourcev1.Resource,
	spanIdx int,
) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	if err := forEachUniqueAttr(resourceAttrs, w.attrKeyScratch, func(attr *commonv1.KeyValue) error {
		colName := "resource." + attr.Key

		// Write attribute value to block column for this span
		if err := w.setFromAnyValue(b, colName, spanIdx, attr.Value); err != nil {
			return fmt.Errorf("set resource attribute %q: %w", attr.Key, err)
		}

		// Track in dedicated index
		if s := attr.GetValue().GetStringValue(); s != "" {
			w.trackDedicatedValue(b, colName, s)
		}

		// Record attribute value for statistics (v10 feature)
		w.recordAttributeForStats(colName, attr.Value)
		return nil
	}); err != nil {
		return err
	}
	if rawResource != nil {
		if dropped := rawResource.GetDroppedAttributesCount(); dropped > 0 {
			if err := w.setUint64(b, "resource:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
				return fmt.Errorf("set resource:dropped_attributes_count: %w", err)
			}
		}
	}
	return nil
}

// setSpanAttributes sets span attributes.
func (w *Writer) setSpanAttributes(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	err := forEachUniqueAttr(span.Attributes, w.attrKeyScratch, func(attr *commonv1.KeyValue) error {
		// All attributes use span.{key} format
		// No conflict with intrinsics because intrinsics use span:{field} format
		full := "span." + attr.Key
		if err := w.setFromAnyValue(b, full, spanIdx, attr.Value); err != nil {
			return fmt.Errorf("set span attribute %q: %w", attr.Key, err)
		}
		if s := attr.GetValue().GetStringValue(); s != "" {
			w.trackDedicatedValue(b, full, s)
		}

		// Record attribute value for statistics (v10 feature)
		w.recordAttributeForStats(full, attr.Value)
		return nil
	})
	return err
}

func hasDuplicateAttrKey(attrs []*commonv1.KeyValue, idx int) bool {
	if idx <= 0 {
		return false
	}
	key := attrs[idx].Key
	for i := 0; i < idx; i++ {
		if attrs[i].Key == key {
			return true
		}
	}
	return false
}

func forEachUniqueAttr(
	attrs []*commonv1.KeyValue,
	scratch map[string]struct{},
	fn func(attr *commonv1.KeyValue) error,
) error {
	const smallAttrDedupThreshold = 8
	if len(attrs) == 0 {
		return nil
	}

	if len(attrs) <= smallAttrDedupThreshold {
		for i, attr := range attrs {
			if hasDuplicateAttrKey(attrs, i) {
				continue
			}
			if err := fn(attr); err != nil {
				return err
			}
		}
		return nil
	}

	for _, attr := range attrs {
		if _, ok := scratch[attr.Key]; ok {
			continue
		}
		scratch[attr.Key] = struct{}{}
		if err := fn(attr); err != nil {
			for k := range scratch {
				delete(scratch, k)
			}
			return err
		}
	}

	for k := range scratch {
		delete(scratch, k)
	}
	return nil
}

// recordAttributeForStats records an attribute value for block statistics collection
func (w *Writer) recordAttributeForStats(attrName string, value *commonv1.AnyValue) {
	if w.statsCollector == nil {
		return // Stats collection not enabled
	}

	if value == nil {
		return
	}

	// Extract the actual value based on type
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		w.statsCollector.recordValue(attrName, v.StringValue)
	case *commonv1.AnyValue_IntValue:
		w.statsCollector.recordValue(attrName, v.IntValue)
	case *commonv1.AnyValue_DoubleValue:
		w.statsCollector.recordValue(attrName, v.DoubleValue)
	case *commonv1.AnyValue_BoolValue:
		w.statsCollector.recordValue(attrName, v.BoolValue)
		// Skip arrays, maps, and other complex types
	}
}

func (w *Writer) addSpanToBlock(b *blockBuilder, buffered *BufferedSpan) error {
	span := buffered.Span
	resourceAttrs := buffered.ResourceAttrs
	rawResource := buffered.RawResource
	rawResourceSchema := buffered.ResourceSchema
	rawScope := buffered.RawScope
	rawScopeSchema := buffered.ScopeSchema

	// Update metric streams with this span
	if err := w.UpdateMetricStreams(span, resourceAttrs); err != nil {
		return fmt.Errorf("failed to update metric streams: %w", err)
	}

	spanIdx := b.spanCount
	b.spanCount++

	// Append NULL for existing columns to maintain row alignment.
	for _, cb := range b.columns {
		cb.appendNull()
	}

	if rawResource == nil {
		rawResource = &resourcev1.Resource{}
	}
	if rawScope == nil {
		rawScope = &commonv1.InstrumentationScope{}
	}

	if err := w.setString(b, "resource:schema_url", spanIdx, rawResourceSchema); err != nil {
		return fmt.Errorf("set resource:schema_url: %w", err)
	}

	if err := w.setString(b, "scope:schema_url", spanIdx, rawScopeSchema); err != nil {
		return fmt.Errorf("set scope:schema_url: %w", err)
	}

	if err := w.setSpanIntrinsicFields(b, span, spanIdx); err != nil {
		return fmt.Errorf("set span intrinsic fields: %w", err)
	}

	if err := w.setSpanIdentifiersAndTimestamps(b, span, spanIdx); err != nil {
		return fmt.Errorf("set span identifiers and timestamps: %w", err)
	}

	// Get or create trace entry for trace-level deduplication
	traceID := span.GetTraceId()
	var traceID16 [16]byte
	copy(traceID16[:], traceID)
	traceIdx, exists := b.traceIndex[traceID16]
	if !exists {
		traceIdx = len(b.traces)
		b.traces = append(b.traces, &traceEntry{
			traceID:        traceID16,
			resourceAttrs:  make(map[string]*columnBuilder),
			resourceSchema: rawResourceSchema,
		})
		b.traceIndex[traceID16] = traceIdx
	}
	b.spanToTrace = append(b.spanToTrace, traceIdx)
	trace := b.traces[traceIdx]
	_ = trace // Keep trace for potential future use

	if err := w.setResourceAttributes(b, resourceAttrs, rawResource, spanIdx); err != nil {
		return fmt.Errorf("set resource attributes: %w", err)
	}

	if err := w.setSpanAttributes(b, span, spanIdx); err != nil {
		return fmt.Errorf("set span attributes: %w", err)
	}

	// Track column names in bloom
	for name := range b.columns {
		addToColumnNameBloom(&b.columnBloom, name)
	}

	if err := w.setInstrumentationColumns(b, spanIdx, rawScope); err != nil {
		return fmt.Errorf("set instrumentation columns: %w", err)
	}
	if err := w.setEventColumns(b, spanIdx, span); err != nil {
		return fmt.Errorf("set event columns: %w", err)
	}
	if err := w.setLinkColumns(b, spanIdx, span); err != nil {
		return fmt.Errorf("set link columns: %w", err)
	}

	return nil
}

func (w *Writer) trackDedicatedValue(b *blockBuilder, colName, val string) {
	// All string columns are now auto-dedicated
	w.recordDedicatedValue(b, colName, StringValueKey(val))
}
