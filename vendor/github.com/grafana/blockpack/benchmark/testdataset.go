package benchmark

import tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

type TestDataset struct {
	Cleanup       func()
	ParquetPath   string
	Traces        []*tracev1.TracesData
	BlockpackPath string
	TraceIDs      []string
	SpanCount     int
}
