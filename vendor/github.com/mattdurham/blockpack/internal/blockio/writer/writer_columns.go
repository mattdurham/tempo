package writer

import (
	"fmt"

	"github.com/grafana/tempo/pkg/util"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// writer_columns.go contains functions for setting column values and handling different data types.

func (w *Writer) setFromAnyValue(b *blockBuilder, name string, spanIdx int, value *commonv1.AnyValue) error {
	if value == nil {
		return nil
	}
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		if err := w.setString(b, name, spanIdx, v.StringValue); err != nil {
			return fmt.Errorf("set string value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_IntValue:
		if err := w.setInt64(b, name, spanIdx, v.IntValue); err != nil {
			return fmt.Errorf("set int64 value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_DoubleValue:
		if err := w.setFloat64(b, name, spanIdx, v.DoubleValue); err != nil {
			return fmt.Errorf("set float64 value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_BoolValue:
		if err := w.setBool(b, name, spanIdx, v.BoolValue); err != nil {
			return fmt.Errorf("set bool value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_BytesValue:
		if err := w.setBytes(b, name, spanIdx, v.BytesValue); err != nil {
			return fmt.Errorf("set bytes value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_ArrayValue:
		// Encode array to bytes and store
		encoded := EncodeAnyValueArray(v.ArrayValue.Values)
		if err := w.setBytes(b, name, spanIdx, encoded); err != nil {
			return fmt.Errorf("set array value for column %s: %w", name, err)
		}
	case *commonv1.AnyValue_KvlistValue:
		// Encode KVList to bytes and store
		encoded := EncodeKeyValueList(v.KvlistValue)
		if err := w.setBytes(b, name, spanIdx, encoded); err != nil {
			return fmt.Errorf("set kvlist value for column %s: %w", name, err)
		}
	default:
		// Unsupported types are skipped.
		return nil
	}
	return nil
}

func (w *Writer) setInstrumentationColumns(b *blockBuilder, spanIdx int, scope *commonv1.InstrumentationScope) error {
	if scope == nil {
		return nil
	}
	if scope.GetName() != "" {
		if err := w.setBytes(b, "instrumentation:name", spanIdx, EncodeStringArray([]string{scope.GetName()})); err != nil {
			return fmt.Errorf("set instrumentation:name: %w", err)
		}
	}
	if scope.GetVersion() != "" {
		if err := w.setBytes(b, "instrumentation:version", spanIdx, EncodeStringArray([]string{scope.GetVersion()})); err != nil {
			return fmt.Errorf("set instrumentation:version: %w", err)
		}
	}
	if dropped := scope.GetDroppedAttributesCount(); dropped > 0 {
		if err := w.setUint64(b, "instrumentation:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
			return fmt.Errorf("set instrumentation:dropped_attributes_count: %w", err)
		}
	}
	if len(scope.Attributes) == 0 {
		return nil
	}
	attrMap := make(map[string][]*commonv1.AnyValue)
	for _, kv := range scope.Attributes {
		if kv == nil || kv.Value == nil {
			continue
		}
		attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "instrumentation."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return fmt.Errorf("set instrumentation.%s: %w", key, err)
		}
	}
	return nil
}

func (w *Writer) setEventColumns(b *blockBuilder, spanIdx int, span *tracev1.Span) error {
	if span == nil || len(span.Events) == 0 {
		return nil
	}
	eventNames := make([]string, 0, len(span.Events))
	eventTimes := make([]int64, 0, len(span.Events))
	eventDropped := make([]int64, 0, len(span.Events))
	attrMap := make(map[string][]*commonv1.AnyValue)
	start := span.GetStartTimeUnixNano()
	for _, event := range span.Events {
		if event == nil {
			continue
		}
		eventNames = append(eventNames, event.GetName())
		delta := int64(0)
		if event.GetTimeUnixNano() > start {
			delta = int64(event.GetTimeUnixNano() - start) //nolint:gosec
		}
		eventTimes = append(eventTimes, delta)
		eventDropped = append(eventDropped, int64(event.GetDroppedAttributesCount()))
		for _, kv := range event.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
		}
	}
	if len(eventNames) > 0 {
		if err := w.setBytes(b, "event:name", spanIdx, EncodeStringArray(eventNames)); err != nil {
			return fmt.Errorf("set event:name: %w", err)
		}
	}
	if len(eventTimes) > 0 {
		if err := w.setBytes(b, "event:time_since_start", spanIdx, EncodeDurationArray(eventTimes)); err != nil {
			return fmt.Errorf("set event:time_since_start: %w", err)
		}
	}
	if len(eventDropped) > 0 {
		if err := w.setBytes(b, "event:dropped_attributes_count", spanIdx, EncodeInt64Array(eventDropped)); err != nil {
			return fmt.Errorf("set event:dropped_attributes_count: %w", err)
		}
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "event."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return fmt.Errorf("set event.%s: %w", key, err)
		}
	}
	return nil
}

func (w *Writer) setLinkColumns(b *blockBuilder, spanIdx int, span *tracev1.Span) error {
	if span == nil || len(span.Links) == 0 {
		return nil
	}
	linkTraceIDs := make([]string, 0, len(span.Links))
	linkSpanIDs := make([]string, 0, len(span.Links))
	linkTraceStates := make([]string, 0, len(span.Links))
	linkDropped := make([]int64, 0, len(span.Links))
	attrMap := make(map[string][]*commonv1.AnyValue)
	for _, link := range span.Links {
		if link == nil {
			continue
		}
		if len(link.GetTraceId()) > 0 {
			linkTraceIDs = append(linkTraceIDs, util.TraceIDToHexString(link.GetTraceId()))
		}
		if len(link.GetSpanId()) > 0 {
			linkSpanIDs = append(linkSpanIDs, util.SpanIDToHexString(link.GetSpanId()))
		}
		linkTraceStates = append(linkTraceStates, link.GetTraceState())
		linkDropped = append(linkDropped, int64(link.GetDroppedAttributesCount()))
		for _, kv := range link.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
		}
	}
	if len(linkTraceIDs) > 0 {
		if err := w.setBytes(b, "link:trace_id", spanIdx, EncodeStringArray(linkTraceIDs)); err != nil {
			return fmt.Errorf("set link:trace_id: %w", err)
		}
	}
	if len(linkSpanIDs) > 0 {
		if err := w.setBytes(b, "link:span_id", spanIdx, EncodeStringArray(linkSpanIDs)); err != nil {
			return fmt.Errorf("set link:span_id: %w", err)
		}
	}
	if len(linkTraceStates) > 0 {
		if err := w.setBytes(b, "link:trace_state", spanIdx, EncodeStringArray(linkTraceStates)); err != nil {
			return fmt.Errorf("set link:trace_state: %w", err)
		}
	}
	if len(linkDropped) > 0 {
		if err := w.setBytes(b, "link:dropped_attributes_count", spanIdx, EncodeInt64Array(linkDropped)); err != nil {
			return fmt.Errorf("set link:dropped_attributes_count: %w", err)
		}
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "link."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return fmt.Errorf("set link.%s: %w", key, err)
		}
	}
	return nil
}

func (w *Writer) setString(b *blockBuilder, name string, idx int, value string) error {
	// Check if this column has already been detected
	if detected, ok := w.uuidColumns[name]; ok {
		if detected {
			// Convert UUID string to bytes
			uuidBytes, err := parseUUID(value)
			if err != nil {
				// If parsing fails, something's wrong - fall back to string for this column
				w.uuidColumns[name] = false
				if err := w.setStringDirect(b, name, idx, value); err != nil {
					return fmt.Errorf("set string direct for column %s: %w", name, err)
				}
				return nil
			}
			if err := w.setBytes(b, name, idx, uuidBytes[:]); err != nil {
				return fmt.Errorf("set UUID bytes for column %s: %w", name, err)
			}
			return nil
		}
		// Already determined not a UUID column
		if err := w.setStringDirect(b, name, idx, value); err != nil {
			return fmt.Errorf("set string direct for column %s: %w", name, err)
		}
		return nil
	}

	// First value for this column - decide now, but defer detection if value is empty
	// For known UUID columns with empty first values, wait for a valid sample
	if value == "" && knownUUIDColumns[name] {
		// Defer detection until we see a non-empty value
		if err := w.setStringDirect(b, name, idx, value); err != nil {
			return fmt.Errorf("set empty string for column %s: %w", name, err)
		}
		return nil
	}

	// Check if this is a known UUID column OR if first value looks like UUID
	if knownUUIDColumns[name] || isUUID(value) {
		// Verify this is actually a UUID before committing
		if isUUID(value) {
			w.uuidColumns[name] = true
			uuidBytes, err := parseUUID(value)
			if err != nil {
				w.uuidColumns[name] = false
				if err := w.setStringDirect(b, name, idx, value); err != nil {
					return fmt.Errorf("set string direct for column %s: %w", name, err)
				}
				return nil
			}
			if err := w.setBytes(b, name, idx, uuidBytes[:]); err != nil {
				return fmt.Errorf("set UUID bytes for column %s: %w", name, err)
			}
			return nil
		}
		// Known UUID column but value doesn't match - treat as string
		w.uuidColumns[name] = false
	} else {
		// Not a known UUID column and first value doesn't look like UUID
		w.uuidColumns[name] = false
	}

	if err := w.setStringDirect(b, name, idx, value); err != nil {
		return fmt.Errorf("set string direct for column %s: %w", name, err)
	}
	return nil
}

func (w *Writer) setStringDirect(b *blockBuilder, name string, idx int, value string) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeString)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setString(idx, value); err != nil {
		return fmt.Errorf("set string value for column %s: %w", name, err)
	}
	cb.stats.recordString(value)
	w.recordDedicatedValue(b, name, StringValueKey(value))
	return nil
}

func (w *Writer) setInt64(b *blockBuilder, name string, idx int, value int64) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeInt64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setInt64(idx, value); err != nil {
		return fmt.Errorf("set int64 value for column %s: %w", name, err)
	}
	cb.stats.recordInt(value)
	w.recordDedicatedValue(b, name, IntValueKey(value))
	return nil
}

func (w *Writer) setUint64(b *blockBuilder, name string, idx int, value uint64) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeUint64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setUint64(idx, value); err != nil {
		return fmt.Errorf("set uint64 value for column %s: %w", name, err)
	}
	cb.stats.recordUint(value)
	w.recordDedicatedValue(b, name, UintValueKey(value))
	return nil
}

func (w *Writer) setFloat64(b *blockBuilder, name string, idx int, value float64) error {
	// Columns are single-typed per name; mismatched writes are rejected in column builders.
	cb := w.getOrCreateColumn(b, name, ColumnTypeFloat64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setFloat64(idx, value); err != nil {
		return fmt.Errorf("set float64 value for column %s: %w", name, err)
	}
	cb.stats.recordFloat(value)
	w.recordDedicatedValue(b, name, FloatValueKey(value))
	return nil
}

func (w *Writer) setBytes(b *blockBuilder, name string, idx int, value []byte) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeBytes)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setBytes(idx, value); err != nil {
		return fmt.Errorf("set bytes value for column %s: %w", name, err)
	}
	cb.stats.recordBytes(name, value)
	if value != nil {
		w.recordDedicatedValue(b, name, BytesValueKey(value))
	}
	return nil
}

func (w *Writer) setBool(b *blockBuilder, name string, idx int, value bool) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeBool)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setBool(idx, value); err != nil {
		return fmt.Errorf("set bool value for column %s: %w", name, err)
	}
	cb.stats.recordBool(value)
	w.recordDedicatedValue(b, name, BoolValueKey(value))
	return nil
}

func (w *Writer) getOrCreateColumn(b *blockBuilder, name string, typ ColumnType) *columnBuilder {
	if cb, ok := b.columns[name]; ok {
		// Type mismatch: column already exists with different type
		// This can happen when different spans use the same attribute name with different types
		// Skip this value rather than causing an error
		if cb.typ != typ {
			return nil
		}
		return cb
	}
	cb := newColumnBuilder(name, typ, b.spanCount)
	b.columns[name] = cb
	return cb
}

// Flush finalizes all blocks and returns the encoded bytes.
