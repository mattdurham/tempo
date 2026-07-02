package otlpconvert

import (
	"bytes"
	"fmt"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// WriteBlockpack encodes OTLP traces into the blockpack format.
func WriteBlockpack(traces []*tracev1.TracesData, maxSpansPerBlock int) ([]byte, error) {
	normalized, err := NormalizeTracesForBlockpack(traces)
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	writer, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxSpansPerBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	for _, trace := range normalized {
		if err := writer.AddTracesData(trace); err != nil {
			return nil, err
		}
	}
	if _, err := writer.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
