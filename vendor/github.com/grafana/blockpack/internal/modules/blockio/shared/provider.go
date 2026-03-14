package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// SpanFieldsProvider gives access to all attributes for a single span row.
// GetField returns the typed value of a named attribute.
// IterateFields calls fn for every present attribute, stopping if fn returns false.
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}
