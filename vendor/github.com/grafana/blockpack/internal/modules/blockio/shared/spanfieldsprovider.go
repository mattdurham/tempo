package shared

// SpanFieldsProvider is a blockpack data type.
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}
