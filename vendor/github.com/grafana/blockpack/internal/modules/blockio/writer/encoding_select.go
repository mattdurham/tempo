package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "strings"

// isIDColumn returns true for column names that should use XOR encoding.
// Checks suffixes: ".id", "_id", "-id" and exact names "span:id", "span:parent_id".
// Explicit exclusion: "trace:id" returns false (uses DeltaDictionary).
func isIDColumn(name string) bool {
	if name == traceIDColumnName {
		return false
	}
	if name == "span:id" || name == "span:parent_id" {
		return true
	}
	return strings.HasSuffix(name, ".id") ||
		strings.HasSuffix(name, "_id") ||
		strings.HasSuffix(name, "-id")
}

// isURLColumn returns true for columns that should use prefix encoding.
// Checks suffixes: ".url", ".uri", ".path", ".target" and exact name "http.target".
func isURLColumn(name string) bool {
	if name == "http.target" {
		return true
	}
	return strings.HasSuffix(name, ".url") ||
		strings.HasSuffix(name, ".uri") ||
		strings.HasSuffix(name, ".path") ||
		strings.HasSuffix(name, ".target")
}

// shouldUseDeltaEncoding returns true when DeltaUint64 beats dictionary for uint64.
// Rules from NOTES §6:
//   - range ≤ 65535 → always delta
//   - range ≤ uint32 AND cardinality > deltaCardinalityThreshold → delta
//   - cardinality > 2 for any range → delta
func shouldUseDeltaEncoding(minVal, maxVal uint64, cardinality int) bool {
	if maxVal < minVal {
		return false
	}
	rangeVal := maxVal - minVal
	switch {
	case rangeVal <= deltaRangeThreshold16:
		return true
	case rangeVal <= deltaRangeThreshold32 && cardinality > deltaCardinalityThreshold:
		return true
	case cardinality > 2:
		return true
	default:
		return false
	}
}
