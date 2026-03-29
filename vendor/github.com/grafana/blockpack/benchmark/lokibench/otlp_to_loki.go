package lokibench

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

// OTLPToLokiStreams converts OTLP LogsData to Loki streams for chunk store ingestion.
//
// Mapping:
//   - ResourceLogs.Resource.Attributes → stream label set (non-dotted keys only)
//   - LogRecord.SeverityText (lowercased) → StructuredMetadata "detected_level"
//   - LogRecord.Attributes → StructuredMetadata entries (skip detected_level if
//     already written from SeverityText to avoid duplication)
//   - LogRecord.Body.StringValue → log line
//   - LogRecord.TimeUnixNano → entry timestamp
func OTLPToLokiStreams(ld *logsv1.LogsData) []logproto.Stream {
	// Group entries by stream label string.
	type streamAcc struct {
		entries []logproto.Entry
	}
	streamMap := make(map[string]*streamAcc)

	for _, rl := range ld.ResourceLogs {
		labelStr := buildLokiLabelString(rl.GetResource().GetAttributes())

		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				acc, ok := streamMap[labelStr]
				if !ok {
					acc = &streamAcc{}
					streamMap[labelStr] = acc
				}

				// Build structured metadata: SeverityText → detected_level first,
				// then remaining LogRecord.Attributes.
				var sm []logproto.LabelAdapter
				if sevText := record.SeverityText; sevText != "" {
					sm = append(sm, logproto.LabelAdapter{
						Name:  "detected_level",
						Value: strings.ToLower(sevText),
					})
				}
				for _, attr := range record.Attributes {
					// Skip detected_level — already written from SeverityText above.
					if attr.Key == "detected_level" {
						continue
					}
					if sv, ok2 := attr.Value.GetValue().(*commonv1.AnyValue_StringValue); ok2 {
						sm = append(sm, logproto.LabelAdapter{
							Name:  attr.Key,
							Value: sv.StringValue,
						})
					}
				}

				line := ""
				if record.Body != nil {
					if sv, ok2 := record.Body.GetValue().(*commonv1.AnyValue_StringValue); ok2 {
						line = sv.StringValue
					}
				}

				ts := time.Unix(0, int64(record.TimeUnixNano)) //nolint:gosec

				acc.entries = append(acc.entries, logproto.Entry{
					Timestamp:          ts,
					Line:               line,
					StructuredMetadata: sm,
				})
			}
		}
	}

	streams := make([]logproto.Stream, 0, len(streamMap))
	for labelStr, acc := range streamMap {
		// Loki chunk stores require entries within a stream to be in non-decreasing
		// timestamp order. The generator assigns timestamps non-sequentially
		// (round-robin across resource combos with jitter), so sort before writing.
		sort.Slice(acc.entries, func(i, j int) bool {
			return acc.entries[i].Timestamp.Before(acc.entries[j].Timestamp)
		})
		streams = append(streams, logproto.Stream{
			Labels:  labelStr,
			Entries: acc.entries,
		})
	}
	return streams
}

// buildLokiLabelString converts OTLP resource attributes to a Loki label string.
//
// Only non-dotted, non-internal keys are included (e.g. cluster, env, region).
// Keys with dots (like service.name) or double-underscore prefixes (__loki_labels__)
// are skipped — they are not valid Loki label names.
// Returns a sorted "{k="v", ...}" string compatible with syntax.ParseLabels.
func buildLokiLabelString(attrs []*commonv1.KeyValue) string {
	type kv struct{ k, v string }
	var pairs []kv
	for _, attr := range attrs {
		k := attr.Key
		if strings.Contains(k, ".") || strings.HasPrefix(k, "__") {
			continue
		}
		if sv, ok := attr.Value.GetValue().(*commonv1.AnyValue_StringValue); ok {
			pairs = append(pairs, kv{k, sv.StringValue})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })

	var sb strings.Builder
	sb.WriteString("{")
	for i, p := range pairs {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s=%q", p.k, p.v)
	}
	sb.WriteString("}")
	return sb.String()
}
