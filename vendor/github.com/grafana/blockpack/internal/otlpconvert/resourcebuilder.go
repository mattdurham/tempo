package otlpconvert

import temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"

type resourceBuilder struct {
	resource  *temporesource.Resource
	schemaURL string
	scopes    map[string]*scopeBuilder
	scopeKeys []string
}
