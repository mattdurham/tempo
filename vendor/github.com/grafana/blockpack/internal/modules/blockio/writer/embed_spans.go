package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"strconv"

	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/embedder"
)

// embedPendingSpans assembles embedding text for each span and calls EmbedBatch once
// for all non-empty texts. Returns a slice of vectors parallel to spans:
// vectors[i] is nil when the span produced no text.
//
// For proto-based spans (rs/ss/span set): field values are extracted directly from
// the OTLP proto attributes — no column decode needed.
// For Tempo-native spans (tempoSpan set): Tempo proto attributes are used.
// For block-based spans (srcBlock set): field values are read from the source Block's
// decoded columns.
//
// The returned slice is always len(spans). Callers check for nil elements.
// A single EmbedBatch call is used for efficiency; the HTTP backend sends all texts
// in one request.
func (w *Writer) embedPendingSpans(spans []pendingSpan) ([][]float32, error) {
	// Phase 1: collect texts and track which span indices have non-empty text.
	texts := make([]string, 0, len(spans))
	indices := make([]int, 0, len(spans)) // maps texts[i] → spans index
	for i := range spans {
		ps := &spans[i]

		var fields map[string]string
		switch {
		case ps.srcBlock != nil:
			fields = extractBlockSpanFields(ps.srcBlock, ps.srcRowIdx)
		case ps.tempoSpan != nil:
			fields = extractTempoProtoFields(ps)
		default:
			fields = extractProtoFields(ps)
		}

		text := w.assembleEmbedText(fields)
		if text == "" {
			continue
		}
		texts = append(texts, text)
		indices = append(indices, i)
	}

	if len(texts) == 0 {
		return make([][]float32, len(spans)), nil
	}

	// Phase 2: embed all texts in a single batch call.
	batchVecs, err := w.cfg.Embedder.EmbedBatch(texts)
	if err != nil {
		return nil, fmt.Errorf("embedder: batch: %w", err)
	}
	if len(batchVecs) != len(texts) {
		return nil, fmt.Errorf("embedder: batch returned %d vectors for %d texts", len(batchVecs), len(texts))
	}

	// Phase 3: scatter batch results back to span positions.
	vecs := make([][]float32, len(spans))
	for j, spanIdx := range indices {
		vecs[spanIdx] = batchVecs[j]
	}
	return vecs, nil
}

// assembleEmbedText converts fields into an embedding text string using the writer's
// EmbeddingFields config or the default AssembleAllFields strategy.
func (w *Writer) assembleEmbedText(fields map[string]string) string {
	if len(w.cfg.EmbeddingFields) == 0 {
		return embedder.AssembleAllFields(fields, embedder.DefaultMaxTextLen)
	}
	return assembleConfiguredFields(fields, w.cfg.EmbeddingFields)
}

// assembleConfiguredFields builds text from a specific list of configured fields.
// Primary-weighted fields are emitted as plain values; context/secondary fields
// are emitted as key=value pairs. Fields not present in the map are skipped.
func assembleConfiguredFields(fields map[string]string, cfg []EmbeddingFieldConfig) string {
	var b []byte
	for _, fc := range cfg {
		v, ok := fields[fc.Name]
		if !ok || v == "" {
			continue
		}
		if len(b) > 0 {
			b = append(b, ' ')
		}
		if fc.Weight == "primary" {
			b = append(b, v...)
		} else {
			b = append(b, fc.Name...)
			b = append(b, '=')
			b = append(b, v...)
		}
	}
	return string(b)
}

// extractProtoFields extracts string-representable field values from an OTLP proto span.
// Numeric values are converted to strings so the embedder can include them.
// Bytes values (trace:id, span:id) are skipped — no semantic text content.
//
//nolint:dupl // intentional mirror of extractTempoProtoFields for OTLP types; different proto types prevent sharing
func extractProtoFields(ps *pendingSpan) map[string]string {
	span := ps.span
	out := make(map[string]string, 32) //nolint:gomnd // reasonable pre-size for average span

	if span.Name != "" {
		out["span:name"] = span.Name
	}
	if span.StartTimeUnixNano > 0 {
		out["span:start"] = strconv.FormatUint(span.StartTimeUnixNano, 10)
	}
	if span.EndTimeUnixNano > 0 {
		out["span:end"] = strconv.FormatUint(span.EndTimeUnixNano, 10)
	}
	if span.EndTimeUnixNano >= span.StartTimeUnixNano && span.EndTimeUnixNano > 0 {
		dur := span.EndTimeUnixNano - span.StartTimeUnixNano
		out["span:duration"] = strconv.FormatUint(dur, 10)
	}
	if span.Status != nil {
		out["span:status"] = strconv.FormatInt(int64(span.Status.Code), 10)
		if span.Status.Message != "" {
			out["span:status_message"] = span.Status.Message
		}
	}

	for _, kv := range span.Attributes {
		if kv == nil || kv.Value == nil {
			continue
		}
		if s := protoAttrString(kv.Value); s != "" {
			out["span."+kv.Key] = s
		}
	}

	if ps.rs != nil && ps.rs.Resource != nil {
		for _, kv := range ps.rs.Resource.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			if s := protoAttrString(kv.Value); s != "" {
				out["resource."+kv.Key] = s
			}
		}
	}

	if ps.ss != nil && ps.ss.Scope != nil {
		for _, kv := range ps.ss.Scope.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			if s := protoAttrString(kv.Value); s != "" {
				out["scope."+kv.Key] = s
			}
		}
	}

	return out
}

// extractTempoProtoFields extracts string-representable field values from a Tempo-native proto span.
//
//nolint:dupl // intentional mirror of extractProtoFields for Tempo types; different proto types prevent sharing
func extractTempoProtoFields(ps *pendingSpan) map[string]string {
	span := ps.tempoSpan
	out := make(map[string]string, 32) //nolint:gomnd // reasonable pre-size for average span

	if span.Name != "" {
		out["span:name"] = span.Name
	}
	if span.StartTimeUnixNano > 0 {
		out["span:start"] = strconv.FormatUint(span.StartTimeUnixNano, 10)
	}
	if span.EndTimeUnixNano > 0 {
		out["span:end"] = strconv.FormatUint(span.EndTimeUnixNano, 10)
	}
	if span.EndTimeUnixNano >= span.StartTimeUnixNano && span.EndTimeUnixNano > 0 {
		dur := span.EndTimeUnixNano - span.StartTimeUnixNano
		out["span:duration"] = strconv.FormatUint(dur, 10)
	}
	if span.Status != nil {
		out["span:status"] = strconv.FormatInt(int64(span.Status.Code), 10)
		if span.Status.Message != "" {
			out["span:status_message"] = span.Status.Message
		}
	}

	for _, kv := range span.Attributes {
		if kv == nil || kv.Value == nil {
			continue
		}
		if s := tempoAttrString(kv.Value); s != "" {
			out["span."+kv.Key] = s
		}
	}

	if ps.tempoRS != nil && ps.tempoRS.Resource != nil {
		for _, kv := range ps.tempoRS.Resource.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			if s := tempoAttrString(kv.Value); s != "" {
				out["resource."+kv.Key] = s
			}
		}
	}

	if ps.tempoSS != nil && ps.tempoSS.Scope != nil {
		for _, kv := range ps.tempoSS.Scope.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			if s := tempoAttrString(kv.Value); s != "" {
				out["scope."+kv.Key] = s
			}
		}
	}

	return out
}

// tempoAttrString extracts a string from a Tempo AnyValue. Returns "" if not string type.
func tempoAttrString(v *tempocommon.AnyValue) string {
	if v == nil {
		return ""
	}
	sv, ok := v.Value.(*tempocommon.AnyValue_StringValue)
	if !ok {
		return ""
	}
	return sv.StringValue
}

// extractBlockSpanFields extracts string-representable field values from a source Block row.
// Only string-typed columns are included; numeric columns are converted via strconv.
// Bytes and vector columns are skipped — no useful text representation.
func extractBlockSpanFields(srcBlock *modules_reader.Block, rowIdx int) map[string]string {
	out := make(map[string]string, 32) //nolint:gomnd // reasonable pre-size for average span
	for colKey, col := range srcBlock.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		name := colKey.Name
		switch colKey.Type {
		case shared.ColumnTypeString, shared.ColumnTypeRangeString, shared.ColumnTypeUUID:
			if v, ok := col.StringValue(rowIdx); ok && v != "" {
				out[name] = v
			}
		case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
			if v, ok := col.Int64Value(rowIdx); ok {
				out[name] = strconv.FormatInt(v, 10)
			}
		case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
			if v, ok := col.Uint64Value(rowIdx); ok {
				out[name] = strconv.FormatUint(v, 10)
			}
		case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
			if v, ok := col.Float64Value(rowIdx); ok {
				out[name] = strconv.FormatFloat(v, 'f', -1, 64)
			}
		case shared.ColumnTypeBool:
			if v, ok := col.BoolValue(rowIdx); ok {
				out[name] = strconv.FormatBool(v)
			}
			// shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes, shared.ColumnTypeVectorF32: skip
		}
	}
	return out
}
