package executor

import (
	"strings"
	"sync"

	"github.com/mattdurham/blockpack/vm"
)

// Cache for attributePathToColumnName to avoid repeated string operations
// Key: attribute path string, Value: canonical column name string
var attributePathToColumnNameCache sync.Map

func attributePathToColumnName(attrPath string) string {
	// Check cache first
	if cached, ok := attributePathToColumnNameCache.Load(attrPath); ok {
		if result, ok := cached.(string); ok {
			return result
		}
	}

	// Compute result
	result := attributePathToColumnNameUncached(attrPath)

	// Store in cache
	attributePathToColumnNameCache.Store(attrPath, result)

	return result
}

func attributePathToColumnNameUncached(attrPath string) string {
	// Explicit intrinsic syntax with : - pass through
	if strings.Contains(attrPath, ":") {
		return attrPath
	}

	// Handle unscoped span intrinsics (for TraceQL compatibility)
	// These are normalized to span: notation during compilation
	switch attrPath {
	case "name":
		return "span:name"
	case "kind":
		return "span:kind"
	case "status":
		return "span:status"
	case "status_message":
		return "span:status_message"
	case "duration":
		return "span:duration"
	case "start":
		return "span:start"
	case "end":
		return "span:end"
	}

	// Span-scoped paths with . are attributes (not intrinsics)
	// Users MUST use span: to reference intrinsics
	if strings.HasPrefix(attrPath, "span.") {
		// It's an attribute - pass through (uses . syntax)
		return attrPath
	}

	// Resource attributes - pass through
	if strings.HasPrefix(attrPath, "resource.") {
		return attrPath
	}

	// Event attributes - pass through (intrinsics must use event: notation)
	if strings.HasPrefix(attrPath, "event.") {
		return attrPath
	}

	// Link attributes - pass through (intrinsics must use link: notation)
	if strings.HasPrefix(attrPath, "link.") {
		return attrPath
	}

	// Instrumentation attributes - pass through (intrinsics must use instrumentation: notation)
	if strings.HasPrefix(attrPath, "instrumentation.") {
		return attrPath
	}

	// Handle well-known unscoped attributes (TraceQL intrinsic behavior)
	// These attributes can be referenced without a scope prefix and map to resource scope
	if isWellKnownResourceAttribute(attrPath) {
		return "resource." + attrPath
	}

	return attrPath
}

// isWellKnownResourceAttribute checks if an attribute is a well-known resource attribute
// that can be referenced without the "resource." prefix in TraceQL
func isWellKnownResourceAttribute(attrPath string) bool {
	switch attrPath {
	case "service.name",
		"service.namespace",
		"service.instance.id",
		"deployment.environment",
		"cluster",
		"namespace",
		"pod",
		"container",
		"k8s.cluster.name",
		"k8s.namespace.name",
		"k8s.pod.name",
		"k8s.container.name",
		"http.status_code",
		"http.method",
		"http.url",
		"http.target",
		"http.host":
		return true
	default:
		return false
	}
}

// AttributePathToColumnName exposes the internal attribute-to-column mapping.
func AttributePathToColumnName(attrPath string) string {
	return attributePathToColumnName(attrPath)
}

// Cache for GetPossibleColumnNames to avoid repeated slice allocations
// Key: attribute path string, Value: []string of possible column names
var possibleColumnNamesCache sync.Map

// GetPossibleColumnNames returns all possible column names for an attribute path.
// For unscoped attributes, this returns multiple possibilities (resource, span, etc.)
// For scoped attributes, this returns a single column name.
// Results are cached to eliminate 10.2M allocations per query.
func GetPossibleColumnNames(attrPath string) []string {
	// Check cache first
	if cached, ok := possibleColumnNamesCache.Load(attrPath); ok {
		if result, ok := cached.([]string); ok {
			return result
		}
	}

	// Compute result
	result := getPossibleColumnNamesUncached(attrPath)

	// Store in cache for future calls
	possibleColumnNamesCache.Store(attrPath, result)

	return result
}

// getPossibleColumnNamesUncached computes possible column names without caching
func getPossibleColumnNamesUncached(attrPath string) []string {
	attrPath = strings.TrimPrefix(attrPath, vm.UnscopedColumnPrefix)

	// Check if it's a trace-level intrinsic (trace:id, span:id, etc.)
	// These are stored with their exact names, not as attributes
	// But we need to map them to use : syntax for storage
	if isTraceIntrinsic(attrPath) {
		return []string{attributePathToColumnName(attrPath)}
	}

	// If contains : it's already a column name (intrinsic with : syntax)
	// Just return as-is
	if strings.Contains(attrPath, ":") {
		return []string{attrPath}
	}

	// If already scoped, return single column name
	if strings.HasPrefix(attrPath, "resource.") ||
		strings.HasPrefix(attrPath, "span.") ||
		strings.HasPrefix(attrPath, "event.") ||
		strings.HasPrefix(attrPath, "link.") ||
		strings.HasPrefix(attrPath, "instrumentation.") {
		return []string{attributePathToColumnName(attrPath)}
	}

	// For unscoped attributes (no prefix), check multiple scopes
	// In TraceQL, unscoped selectors search across resource, span, and other scopes

	// Check if it's a well-known resource attribute
	if isWellKnownResourceAttribute(attrPath) {
		// Well-known resource attributes can appear in both resource and span scopes
		// Prefer span scope first to match TraceQL behavior (span attributes override resource)
		return []string{
			"span." + attrPath,
			"resource." + attrPath,
		}
	}

	// For other unscoped attributes (e.g., custom attributes),
	// check both resource and span scopes
	if !strings.Contains(attrPath, ".") {
		// Simple attribute name without dots - unlikely in practice
		return []string{
			"resource." + attrPath,
			"span." + attrPath,
		}
	}

	// Attribute with dots (e.g., "http.method", "db.name")
	// These can appear in any scope
	return []string{
		"resource." + attrPath,
		"span." + attrPath,
		"event." + attrPath,
		"link." + attrPath,
	}
}

// isTraceIntrinsic checks if an attribute is an intrinsic field (not an attribute).
// These fields have fixed column names and are not stored under .attr namespaces.
// Intrinsics must use colon notation (e.g., span:id).
func isTraceIntrinsic(attrPath string) bool {
	switch attrPath {
	// Span-level intrinsics (use colon notation)
	case "trace:id",
		"span:id",
		"span:parent_id",
		"span:name",
		"span:status",
		"span:kind",
		"span:status_message",
		"span:start",
		"span:end",
		"span:duration":
		return true
	// Event intrinsics (use colon notation)
	case "event:name",
		"event:time_since_start",
		"event:dropped_attributes_count":
		return true
	// Link intrinsics (use colon notation)
	case "link:trace_id",
		"link:span_id",
		"link:trace_state",
		"link:dropped_attributes_count":
		return true
	// Instrumentation scope intrinsics (use colon notation)
	case "instrumentation:name",
		"instrumentation:version",
		"instrumentation:dropped_attributes_count":
		return true
	default:
		return false
	}
}
