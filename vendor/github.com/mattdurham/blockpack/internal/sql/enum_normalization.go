// Package sql provides SQL query compilation and execution capabilities for TraceQL.
package sql

import "strings"

func normalizeEnumComparisonValue(attrPath string, value interface{}) interface{} {
	literal, ok := value.(string)
	if !ok {
		return value
	}

	if isStatusField(attrPath) {
		if mapped, ok := statusEnumValue(literal); ok {
			return mapped
		}
	}

	if isKindField(attrPath) {
		if mapped, ok := kindEnumValue(literal); ok {
			return mapped
		}
	}

	return value
}

func isStatusField(attrPath string) bool {
	normalized := normalizeFieldName(attrPath)
	return normalized == IntrinsicSpanStatus
}

func isKindField(attrPath string) bool {
	normalized := normalizeFieldName(attrPath)
	return normalized == IntrinsicSpanKind
}

func statusEnumValue(value string) (int64, bool) {
	switch strings.ToLower(value) {
	case "unset":
		return int64(0), true
	case "ok":
		return int64(1), true
	case "error":
		return int64(2), true
	default:
		return 0, false
	}
}

func kindEnumValue(value string) (int64, bool) {
	switch strings.ToLower(value) {
	case "unspecified":
		return int64(0), true
	case "internal":
		return int64(1), true
	case "server":
		return int64(2), true
	case "client":
		return int64(3), true
	case "producer":
		return int64(4), true
	case "consumer":
		return int64(5), true
	default:
		return 0, false
	}
}
