package executor

import "slices"

// IntrinsicFieldNames is the canonical list of intrinsic span fields that should
// always be available in query results. These are core span fields like timestamps,
// IDs, and metadata that aren't part of WHERE clauses.
//
// This list is used across multiple components:
//   - lazy_fields.go: isIntrinsicField() and IterateFields()
//   - blockpack_executor.go: isIntrinsicColumn() and extractResultColumns()
//
// IMPORTANT: This is the single source of truth for intrinsic fields.
// Any additions must be made here and will automatically propagate to all consumers.
var IntrinsicFieldNames = []string{
	"trace:id",
	"span:id",
	"span:parent_id",
	"span:name",
	"span:start",
	"span:end",
	"span:duration",
	"span:status",
	"span:kind",
	"span:status_message",
	"resource.service.name", // Needed for RootServiceName in TraceSearchMetadata
}

// IsIntrinsicField checks if a column name represents an intrinsic span field.
// Intrinsic fields are always available and not filtered by OR projection logic.
func IsIntrinsicField(columnName string) bool {
	return slices.Contains(IntrinsicFieldNames, columnName)
}
