package benchmark

import (
	"fmt"
	"sort"
	"testing"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/stretchr/testify/require"
)

// TestFormatComparisonDeepCompare validates that blockpack and Parquet return identical
// span-level data for all benchmark queries: same traces, same spans, same attribute
// values. Parquet is the ground truth; blockpack must match it exactly.
//
// Excluded from comparison: SpanSet.Matched (metric, allowed to differ),
// attribute order (sorted before comparison), structural queries (SpanMatch.Fields
// is nil for structural queries — ID-only comparison falls back to the existing test).
func TestFormatComparisonDeepCompare(t *testing.T) {
	const testTraceCount = 500
	const testSpansPerTrace = 10

	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(t, testTraceCount, testSpansPerTrace)
	blockpackPath := writeBlockpackFormat(t, otelTraces, "")
	tempoMeta, tempoReader, _, _, cleanup := writeTempoBlock(t, tempoTraces, traceIDs, "")
	defer cleanup()

	for _, query := range benchmarkQueries {
		if query.isStructural {
			continue // structural queries have nil Fields; ID parity covered by TestFormatComparisonCorrectness
		}
		query := query
		t.Run(query.name, func(t *testing.T) {
			parquetResp, err := executeTempoSearchResponse(tempoMeta, tempoReader, query.traceqlQuery)
			require.NoError(t, err, "parquet query failed")

			blockpackResp, err := executeBlockpackSearch(blockpackPath, query.traceqlQuery)
			require.NoError(t, err, "blockpack query failed")

			if err := deepCompareSearchResponses(parquetResp, blockpackResp); err != nil {
				t.Fatalf("deep compare mismatch for query %q: %v", query.name, err)
			}
		})
	}
}

// deepCompareSearchResponses performs a full attribute-level comparison of two
// SearchResponse objects. Parquet is the ground truth; blockpack must match exactly.
//
// Order-independent: traces sorted by ID, spans sorted by ID, attributes compared
// by key using deepCompareKeyValueSlices (type-aware, handles duplicate keys).
// Metrics (SpanSet.Matched) are excluded.
func deepCompareSearchResponses(parquet, blockpack *tempopb.SearchResponse) error {
	pqMap, err := buildSpansByID(parquet)
	if err != nil {
		return fmt.Errorf("parquet response: %w", err)
	}
	bpMap, err := buildSpansByID(blockpack)
	if err != nil {
		return fmt.Errorf("blockpack response: %w", err)
	}

	if len(pqMap) != len(bpMap) {
		return fmt.Errorf("trace count mismatch: parquet=%d blockpack=%d", len(pqMap), len(bpMap))
	}

	traceIDs := make([]string, 0, len(pqMap))
	for tid := range pqMap {
		traceIDs = append(traceIDs, tid)
	}
	sort.Strings(traceIDs)

	for _, traceID := range traceIDs {
		pqSpans := pqMap[traceID]
		bpSpans, ok := bpMap[traceID]
		if !ok {
			return fmt.Errorf("trace %s: present in parquet, missing in blockpack", traceID)
		}
		if err := compareSpanIDMaps(traceID, pqSpans, bpSpans); err != nil {
			return err
		}
	}
	for traceID := range bpMap {
		if _, ok := pqMap[traceID]; !ok {
			return fmt.Errorf("trace %s: present in blockpack, missing in parquet", traceID)
		}
	}
	return nil
}

// buildSpansByID converts a SearchResponse into map[traceID → map[spanID → *tempopb.Span]].
// Flattens all SpanSets entries for a trace into a single span map.
// Errors on invalid TraceIDs, duplicate normalized trace IDs, and duplicate normalized span IDs.
func buildSpansByID(resp *tempopb.SearchResponse) (map[string]map[string]*tempopb.Span, error) {
	out := make(map[string]map[string]*tempopb.Span, len(resp.Traces))
	for _, trace := range resp.Traces {
		if trace == nil {
			continue
		}
		traceID, ok := normalizeTraceID(trace.TraceID)
		if !ok {
			return nil, fmt.Errorf("invalid TraceID %q in response", trace.TraceID)
		}
		if _, dup := out[traceID]; dup {
			return nil, fmt.Errorf("duplicate normalized traceID %q in response", traceID)
		}

		spanSets := trace.SpanSets

		spans := make(map[string]*tempopb.Span)
		for _, ss := range spanSets {
			if ss == nil {
				continue
			}
			for _, s := range ss.Spans {
				if s == nil {
					continue
				}
				sid := normalizeSpanID(s.SpanID)
				if _, dup := spans[sid]; dup {
					return nil, fmt.Errorf("trace %s: duplicate normalized spanID %q", traceID, sid)
				}
				spans[sid] = s
			}
		}
		out[traceID] = spans
	}
	return out, nil
}

// compareSpanIDMaps compares two span maps for a single trace.
func compareSpanIDMaps(traceID string, pq, bp map[string]*tempopb.Span) error {
	if len(pq) != len(bp) {
		return fmt.Errorf("trace %s: span count mismatch: parquet=%d blockpack=%d",
			traceID, len(pq), len(bp))
	}
	spanIDs := make([]string, 0, len(pq))
	for sid := range pq {
		spanIDs = append(spanIDs, sid)
	}
	sort.Strings(spanIDs)

	for _, spanID := range spanIDs {
		pqSpan := pq[spanID]
		bpSpan, ok := bp[spanID]
		if !ok {
			return fmt.Errorf("trace %s span %s: present in parquet, missing in blockpack",
				traceID, spanID)
		}
		if err := compareProtoSpans(traceID, spanID, pqSpan, bpSpan); err != nil {
			return err
		}
	}
	for spanID := range bp {
		if _, ok := pq[spanID]; !ok {
			return fmt.Errorf("trace %s span %s: present in blockpack, missing in parquet",
				traceID, spanID)
		}
	}
	return nil
}

// compareProtoSpans performs an exact comparison of two tempopb.Span values,
// delegating to deepCompareKeyValueSlices for type-aware attribute comparison.
// This correctly handles duplicate attribute keys and preserves AnyValue types
// rather than collapsing them to strings.
//
// executeBlockpackSearch applies queryParquetKeys so both sides carry identical
// attribute sets; any difference here is a bug.
func compareProtoSpans(traceID, spanID string, pq, bp *tempopb.Span) error {
	if pq.Name != bp.Name {
		return fmt.Errorf("trace %s span %s: name: parquet=%q blockpack=%q",
			traceID, spanID, pq.Name, bp.Name)
	}
	if pq.StartTimeUnixNano != bp.StartTimeUnixNano {
		return fmt.Errorf("trace %s span %s: startNano: parquet=%d blockpack=%d",
			traceID, spanID, pq.StartTimeUnixNano, bp.StartTimeUnixNano)
	}
	if pq.DurationNanos != bp.DurationNanos {
		return fmt.Errorf("trace %s span %s: durNano: parquet=%d blockpack=%d",
			traceID, spanID, pq.DurationNanos, bp.DurationNanos)
	}
	label := fmt.Sprintf("trace %s span %s attributes", traceID, spanID)
	return deepCompareKeyValueSlices(label, pq.Attributes, bp.Attributes)
}
