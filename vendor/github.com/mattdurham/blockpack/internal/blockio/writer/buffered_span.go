package writer

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
	Span        *tracev1.Span
	RawResource *resourcev1.Resource
	RawScope    *commonv1.InstrumentationScope

	// Extracted attributes for quick access
	Attributes     map[string]string
	ResourceSchema string
	ScopeSchema    string

	// Sort keys
	ServiceName   string // Primary sort key
	ResourceAttrs []*commonv1.KeyValue
	MinHashSig    MinHashSignature // Secondary sort key
	MinHashPrefix [2]uint64        // Fast path prefix for MinHash comparisons
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
	// Validate total size to prevent excessive allocation and overflow
	// Check components separately first to avoid overflow in MaxNameLen*2
	if len(field) > MaxNameLen || len(value) > MaxNameLen {
		// Fallback to safe string concatenation for oversized tokens
		return field + ":" + value
	}
	if size > MaxNameLen*2 {
		// Fallback to safe string concatenation for oversized tokens
		return field + ":" + value
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(a.Alloc(size))), size) //nolint:gosec
	copy(buf, field)
	buf[len(field)] = ':'
	copy(buf[len(field)+1:], value)
	return unsafe.String(&buf[0], size) //nolint:gosec
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
