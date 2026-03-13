package otlpconvert

import (
	"fmt"
	"io"
	"os"

	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ConvertFromProtoFile reads an OTLP protobuf-encoded TracesData file and writes
// blockpack-formatted trace data to the output writer.
// The input file must contain a single wire-encoded tracev1.TracesData message.
func ConvertFromProtoFile(inputPath string, output io.Writer, maxSpansPerBlock int) error {
	//nolint:gosec // inputPath comes from the caller; intentional file read
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("read proto file: %w", err)
	}
	var td tracev1.TracesData
	unmarshalErr := proto.Unmarshal(data, &td)
	if unmarshalErr != nil {
		return fmt.Errorf("unmarshal proto file: %w", unmarshalErr)
	}
	blockData, err := WriteBlockpack([]*tracev1.TracesData{&td}, maxSpansPerBlock)
	if err != nil {
		return fmt.Errorf("convert to blockpack: %w", err)
	}
	if _, err = output.Write(blockData); err != nil {
		return fmt.Errorf("write blockpack output: %w", err)
	}
	return nil
}

// ConvertLogsProtoFile reads an OTLP protobuf-encoded LogsData file and writes
// blockpack-formatted log data to the output writer.
// The input file must contain a single wire-encoded logsv1.LogsData message.
func ConvertLogsProtoFile(inputPath string, output io.Writer, maxRecordsPerBlock int) error {
	//nolint:gosec // inputPath comes from the caller; intentional file read
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return fmt.Errorf("read proto file: %w", err)
	}
	var ld logsv1.LogsData
	if unmarshalErr := proto.Unmarshal(data, &ld); unmarshalErr != nil {
		return fmt.Errorf("unmarshal proto file: %w", unmarshalErr)
	}
	blockData, err := WriteBlockpackLogs([]*logsv1.LogsData{&ld}, maxRecordsPerBlock)
	if err != nil {
		return fmt.Errorf("convert to blockpack: %w", err)
	}
	if _, err = output.Write(blockData); err != nil {
		return fmt.Errorf("write blockpack output: %w", err)
	}
	return nil
}
