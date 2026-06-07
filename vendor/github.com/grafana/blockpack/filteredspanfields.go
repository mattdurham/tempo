package blockpack

type filteredSpanFields struct {
	inner   SpanFieldsProvider
	allowed map[string]struct{}
}
