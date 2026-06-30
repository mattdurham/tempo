package otlpconvert

type traceBuilder struct {
	resources    map[string]*resourceBuilder
	resourceKeys []string
}
