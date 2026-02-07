package ondiskio

import (
	"strings"
	"unsafe"

	"github.com/mattdurham/blockpack/internal/arena"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// BufferedSpan holds a span and its metadata for sorting during block creation.
// Spans are buffered during ingestion, then sorted by ServiceName (primary) and
// MinHashSig (secondary) before being written to blocks. This clustering improves
// query performance by reducing the number of blocks that need to be scanned.
type BufferedSpan struct {
	// Original span data
	Span           *tracev1.Span
	ResourceAttrs  []*commonv1.KeyValue
	RawResource    *resourcev1.Resource
	ResourceSchema string
	RawScope       *commonv1.InstrumentationScope
	ScopeSchema    string

	// Sort keys
	ServiceName   string           // Primary sort key
	MinHashSig    MinHashSignature // Secondary sort key
	MinHashPrefix [2]uint64        // Fast path prefix for MinHash comparisons

	// Extracted attributes for quick access
	Attributes map[string]string
}

// ExtractAttributes builds a map of all string attributes from a span.
// Resource attributes are prefixed with "resource.", span attributes with
// "span.", and span intrinsics (name, kind) are added directly.
// This unified map is used for both sorting key computation and MinHash generation.
func ExtractAttributes(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue) map[string]string {
	attrs := make(map[string]string)

	// Resource attributes (service.name is typically here)
	for _, kv := range resourceAttrs {
		key := "resource." + kv.Key
		if val := kv.GetValue().GetStringValue(); val != "" {
			attrs[key] = val
		}
	}

	// Span attributes
	for _, kv := range span.GetAttributes() {
		key := "span." + kv.Key
		if val := kv.GetValue().GetStringValue(); val != "" {
			attrs[key] = val
		}
	}

	// Span intrinsics
	// BOT: Shouldnt we store this as span:name? Intrinsics are always : same for kind below.
	attrs["span.name"] = span.GetName()
	// Note: span.kind is int, convert to string for consistency
	if kind := span.GetKind(); kind != 0 {
		attrs["span.kind"] = span.GetKind().String()
	}

	return attrs
}

// ComputeSortKey computes the sort keys for a buffered span.
// The primary key is the service.name from resource attributes.
// The secondary key is a MinHash signature computed from OTEL semantic fields.
// This method should be called before sorting the span buffer.
func (bs *BufferedSpan) ComputeSortKey(cache *MinHashCache, arena *arena.Arena) {
	bs.Attributes = nil

	// Primary: service.name (resource attribute in OTEL)
	bs.ServiceName = extractServiceName(bs.ResourceAttrs)

	// Secondary: MinHash of OTEL semantic fields (in OTELSemanticFields order)
	tokens := extractOTELTokensFromSpan(bs.Span, bs.ResourceAttrs, arena)
	if cache == nil {
		bs.MinHashSig = ComputeMinHash(tokens)
		bs.MinHashPrefix[0] = bs.MinHashSig[0]
		bs.MinHashPrefix[1] = bs.MinHashSig[1]
		return
	}
	bs.MinHashSig = cache.Compute(tokens)
	bs.MinHashPrefix[0] = bs.MinHashSig[0]
	bs.MinHashPrefix[1] = bs.MinHashSig[1]
}

func extractServiceName(resourceAttrs []*commonv1.KeyValue) string {
	for _, kv := range resourceAttrs {
		if kv.Key != "service.name" {
			continue
		}
		if val := kv.GetValue().GetStringValue(); val != "" {
			return val
		}
	}
	return ""
}

func extractOTELTokensFromSpan(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, arena *arena.Arena) []string {
	tokens := make([]string, 0, len(OTELSemanticFields))

	for _, field := range OTELSemanticFields {
		switch {
		case field == "span.service.name":
			if val := spanStringAttrValue(resourceAttrs, "service.name"); val != "" {
				tokens = append(tokens, buildTokenString(arena, field, val))
			}
		case field == "span.name":
			if name := span.GetName(); name != "" {
				tokens = append(tokens, buildTokenString(arena, field, name))
			}
		case field == "span.kind":
			if kind := span.GetKind(); kind != 0 {
				tokens = append(tokens, buildTokenString(arena, field, kind.String()))
			}
		case strings.HasPrefix(field, "span."):
			key := strings.TrimPrefix(field, "span.")
			// BOT: Why does this use span.GetAttributes? Shouldnt we be using the unified attributes map we built in ExtractAttributes? That way we can support resource attributes as well if they are included in the OTELSemanticFields list.
			if val := spanStringAttrValue(span.GetAttributes(), key); val != "" {
				tokens = append(tokens, buildTokenString(arena, field, val))
			}
		case strings.HasPrefix(field, "resource."):
			key := strings.TrimPrefix(field, "resource.")
			if val := spanStringAttrValue(resourceAttrs, key); val != "" {
				tokens = append(tokens, buildTokenString(arena, field, val))
			}
		}
	}

	return tokens
}

func spanStringAttrValue(attrs []*commonv1.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.Key != key {
			continue
		}
		if val := kv.GetValue().GetStringValue(); val != "" {
			return val
		}
	}
	return ""
}

func buildTokenString(a *arena.Arena, field, value string) string {
	if a == nil {
		return field + ":" + value
	}
	size := len(field) + 1 + len(value)
	if size == 0 {
		return ""
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(a.Alloc(size))), size)
	copy(buf, field)
	buf[len(field)] = ':'
	copy(buf[len(field)+1:], value)
	return unsafe.String(&buf[0], size)
}

// Less compares two buffered spans for sorting.
// Returns true if bs should come before other in sorted order.
// Comparison is done first by ServiceName, then by MinHash signature, then by TraceID.
// This ensures spans from the same service are clustered together, with
// similar spans (same endpoint, method, etc.) adjacent to each other.
// The TraceID tie-breaker ensures deterministic ordering for spans with identical MinHash.
func (bs *BufferedSpan) Less(other *BufferedSpan) bool {
	// Primary: service name
	if bs.ServiceName != other.ServiceName {
		return bs.ServiceName < other.ServiceName
	}

	// Secondary: MinHash signature
	if bs.MinHashPrefix[0] != other.MinHashPrefix[0] {
		return bs.MinHashPrefix[0] < other.MinHashPrefix[0]
	}
	if bs.MinHashPrefix[1] != other.MinHashPrefix[1] {
		return bs.MinHashPrefix[1] < other.MinHashPrefix[1]
	}
	cmp := CompareMinHashSigs(bs.MinHashSig, other.MinHashSig)
	if cmp != 0 {
		return cmp < 0
	}

	// Tertiary: TraceID (for deterministic ordering)
	return compareTraceIDs(bs.Span.TraceId, other.Span.TraceId) < 0
}

// compareTraceIDs compares two trace IDs lexicographically.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareTraceIDs(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	// If all bytes match, shorter slice comes first
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
