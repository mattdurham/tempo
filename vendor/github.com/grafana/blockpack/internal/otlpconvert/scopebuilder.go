package otlpconvert

import (
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

type scopeBuilder struct {
	scope     *tempocommon.InstrumentationScope
	schemaURL string
	spans     []*tempotrace.Span
}
