package writer

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

type pendingSpan struct {
	rs          *tracev1.ResourceSpans
	ss          *tracev1.ScopeSpans
	span        *tracev1.Span
	tempoRS     *tempotrace.ResourceSpans
	tempoSS     *tempotrace.ScopeSpans
	tempoSpan   *tempotrace.Span
	srcBlock    *modules_reader.Block
	srcReader   *modules_reader.Reader
	svcName     string
	minHashSig  [4]uint64
	srcRowIdx   int
	srcBlockIdx int
	traceID     [16]byte
}
