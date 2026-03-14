package otlpconvert

import (
	"bytes"
	"fmt"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
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

// WriteBlockpackLogs encodes OTLP logs into the blockpack format.
// Mirrors WriteBlockpack for the log signal path.
func WriteBlockpackLogs(logs []*logsv1.LogsData, maxRecordsPerBlock int) ([]byte, error) {
	normalized, err := NormalizeLogsForBlockpack(logs)
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	writer, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxRecordsPerBlock,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	for _, ld := range normalized {
		if err := writer.AddLogsData(ld); err != nil {
			return nil, err
		}
	}
	if _, err := writer.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
