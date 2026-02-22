package tempoapi

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/mattdurham/blockpack/internal/blockio/reader"
)

const (
	maxTagValues    = 10000          // Cardinality limit for tag values
	attrServiceName = "service.name" // Service name attribute
)

// SearchTags returns attribute names available in the blockpack file.
// The scope parameter filters results:
//   - "resource" or "Resource": only attributes starting with "resource."
//   - "span" or "Span": only attributes starting with "span." or "span:"
//   - "" (empty string): all attributes
func SearchTags(path string, scope string, storage Storage) ([]string, error) {
	// Create reader using storage provider
	provider, err := storage.GetProvider(path)
	if err != nil {
		return nil, fmt.Errorf("get storage provider: %w", err)
	}

	// Close provider if closeable
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close()
		}()
	}

	rdr, err := reader.NewReaderFromProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}

	// Collect tag names from dedicated column index
	tagSet := make(map[string]struct{})
	for _, name := range rdr.DedicatedColumnNames() {
		if matchesScope(name, scope) {
			tagSet[name] = struct{}{}
		}
	}

	// Convert to sorted slice
	result := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		result = append(result, tag)
	}
	sort.Strings(result)

	return result, nil
}

// matchesScope checks if an attribute name matches the given scope filter
func matchesScope(name string, scope string) bool {
	switch strings.ToLower(scope) {
	case "resource":
		return strings.HasPrefix(name, "resource.")
	case "span":
		return strings.HasPrefix(name, "span.") || strings.HasPrefix(name, "span:")
	case "":
		return true
	default:
		return true
	}
}

// SearchTagValues returns distinct values for the given attribute name.
// Values are limited to 10,000 entries to prevent excessive memory usage
// on high-cardinality attributes.
// The tag name is normalized (e.g., attrServiceName -> "resource.service.name").
func SearchTagValues(path string, tag string, storage Storage) ([]string, error) {
	// Create reader using storage provider
	provider, err := storage.GetProvider(path)
	if err != nil {
		return nil, fmt.Errorf("get storage provider: %w", err)
	}

	// Close provider if closeable
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close()
		}()
	}

	rdr, err := reader.NewReaderFromProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}

	// Normalize tag name
	normalizedTag := normalizeTagName(tag)

	// Try dedicated column index first
	values := rdr.DedicatedValues(normalizedTag)
	if len(values) == 0 {
		// No dedicated index exists for this tag
		return []string{}, nil
	}

	// Convert DedicatedValueKey to string representations
	result := make([]string, 0, len(values))
	for _, key := range values {
		strValue, err := dedicatedValueToString(key)
		if err != nil {
			continue // Skip values that can't be converted
		}
		result = append(result, strValue)
	}

	// Apply cardinality limit
	if len(result) > maxTagValues {
		result = result[:maxTagValues]
	}

	// Sort results
	sort.Strings(result)

	return result, nil
}

// normalizeTagName applies canonical naming rules for tag lookup
// This matches Tempo's normalization conventions:
//   - attrServiceName -> "resource.service.name"
//   - "service_name" -> "resource.service.name"
func normalizeTagName(tag string) string {
	// Common shortcuts to full canonical names
	switch tag {
	case attrServiceName, "service_name":
		return "resource.service.name"
	case "span.name":
		return "span:name"
	case "span.status":
		return "span:status"
	case "trace.id":
		return "trace:id"
	case "span.id":
		return "span:id"
	}

	// Convert underscores to dots for user attributes
	normalized := strings.ReplaceAll(tag, "_", ".")

	return normalized
}

// dedicatedValueToString converts a DedicatedValueKey to its string representation
func dedicatedValueToString(key reader.DedicatedValueKey) (string, error) {
	switch key.Type() {
	case reader.ColumnTypeString:
		return string(key.Data()), nil
	case reader.ColumnTypeInt64:
		if len(key.Data()) != 8 {
			return "", fmt.Errorf("invalid int64 data length: %d", len(key.Data()))
		}
		// Decode int64 from little-endian bytes
		val := int64(key.Data()[0]) | int64(key.Data()[1])<<8 | int64(key.Data()[2])<<16 | int64(key.Data()[3])<<24 |
			int64(key.Data()[4])<<32 | int64(key.Data()[5])<<40 | int64(key.Data()[6])<<48 | int64(key.Data()[7])<<56
		return strconv.FormatInt(val, 10), nil
	case reader.ColumnTypeUint64:
		if len(key.Data()) != 8 {
			return "", fmt.Errorf("invalid uint64 data length: %d", len(key.Data()))
		}
		// Decode uint64 from little-endian bytes
		val := uint64(
			key.Data()[0],
		) | uint64(
			key.Data()[1],
		)<<8 | uint64(
			key.Data()[2],
		)<<16 | uint64(
			key.Data()[3],
		)<<24 |
			uint64(
				key.Data()[4],
			)<<32 | uint64(
			key.Data()[5],
		)<<40 | uint64(
			key.Data()[6],
		)<<48 | uint64(
			key.Data()[7],
		)<<56
		return strconv.FormatUint(val, 10), nil
	case reader.ColumnTypeBool:
		if len(key.Data()) != 1 {
			return "", fmt.Errorf("invalid bool data length: %d", len(key.Data()))
		}
		if key.Data()[0] == 0 {
			return "false", nil
		}
		return "true", nil
	case reader.ColumnTypeFloat64:
		if len(key.Data()) != 8 {
			return "", fmt.Errorf("invalid float64 data length: %d", len(key.Data()))
		}
		// Decode float64 from little-endian bytes
		bits := uint64(
			key.Data()[0],
		) | uint64(
			key.Data()[1],
		)<<8 | uint64(
			key.Data()[2],
		)<<16 | uint64(
			key.Data()[3],
		)<<24 |
			uint64(
				key.Data()[4],
			)<<32 | uint64(
			key.Data()[5],
		)<<40 | uint64(
			key.Data()[6],
		)<<48 | uint64(
			key.Data()[7],
		)<<56
		// Reinterpret bits as float64
		val := *(*float64)(unsafe.Pointer(&bits)) //nolint:gosec // Reviewed and acceptable
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("unsupported type: %v", key.Type())
	}
}
