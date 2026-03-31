package compaction

import (
	"bytes"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strAttr(v string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}
}

// makeLogRecord creates a log record with the given attributes.
func makeLogRecord(ts uint64, body string, attrs map[string]string) *logsv1.LogRecord {
	kvs := make([]*commonv1.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, &commonv1.KeyValue{Key: k, Value: strAttr(v)})
	}
	return &logsv1.LogRecord{
		TimeUnixNano: ts,
		Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}},
		Attributes:   kvs,
	}
}

// writeLogFile creates a log blockpack file with small block size to force multiple blocks.
func writeLogFile(t *testing.T, records []*logsv1.LogsData) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 2, // tiny blocks to force many blocks
	})
	require.NoError(t, err)
	for _, ld := range records {
		require.NoError(t, w.AddLogsData(ld))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

func wrapInLogsData(rl *logsv1.ResourceLogs) *logsv1.LogsData {
	return &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{rl}}
}

// TestCompactLogFile_GlobalSort verifies that CompactLogFile produces blocks with
// tight label-value boundaries by globally re-sorting.
func TestCompactLogFile_GlobalSort(t *testing.T) {
	// Create records from 3 different streams, interleaved so that without
	// global re-sort they'd end up mixed across blocks.
	stream1 := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
			{Key: "env", Value: strAttr("prod")},
			{Key: "region", Value: strAttr("us-east-1")},
		}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			makeLogRecord(100, "s1-msg1", nil),
			makeLogRecord(200, "s1-msg2", nil),
			makeLogRecord(300, "s1-msg3", nil),
		}}},
	}
	stream2 := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
			{Key: "env", Value: strAttr("staging")},
			{Key: "region", Value: strAttr("eu-west-1")},
		}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			makeLogRecord(150, "s2-msg1", nil),
			makeLogRecord(250, "s2-msg2", nil),
			makeLogRecord(350, "s2-msg3", nil),
		}}},
	}
	stream3 := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
			{Key: "env", Value: strAttr("dev")},
			{Key: "region", Value: strAttr("ap-southeast-1")},
		}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			makeLogRecord(120, "s3-msg1", nil),
			makeLogRecord(220, "s3-msg2", nil),
			makeLogRecord(320, "s3-msg3", nil),
		}}},
	}

	// Write all 9 records interleaved (1 record per LogsData to maximize mixing).
	records := make([]*logsv1.LogsData, 0, 3)
	records = append(records, wrapInLogsData(stream1))
	records = append(records, wrapInLogsData(stream2))
	records = append(records, wrapInLogsData(stream3))
	originalData := writeLogFile(t, records)

	// Compact.
	compacted, err := CompactLogFileBytes(originalData, Config{MaxSpansPerBlock: 3})
	require.NoError(t, err)
	require.NotEmpty(t, compacted)

	// Read compacted file and verify blocks have tight label boundaries.
	r, err := modules_reader.NewReaderFromProvider(&bytesProvider{data: compacted})
	require.NoError(t, err)
	assert.Equal(t, shared.SignalTypeLog, r.SignalType())

	// Count total rows and verify each block has homogeneous resource.env values.
	totalRows := 0
	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		require.NoError(t, getErr)
		require.NotNil(t, bwb)
		block := bwb.Block

		envCol := block.GetColumn("resource.env")
		if envCol == nil {
			continue
		}

		// All rows in this block should have the same env value.
		var blockEnv string
		for rowIdx := range block.SpanCount() {
			totalRows++
			if !envCol.IsPresent(rowIdx) {
				continue
			}
			v, ok := envCol.StringValue(rowIdx)
			if !ok {
				continue
			}
			if blockEnv == "" {
				blockEnv = v
			} else {
				assert.Equal(t, blockEnv, v,
					"block %d has mixed env values: %q and %q", blockIdx, blockEnv, v)
			}
		}
	}
	assert.Equal(t, 9, totalRows, "compacted file should have all 9 rows")
}

// TestCompactLogFile_RejectsTraceFile verifies that CompactLogFile returns an error
// for trace-signal files.
func TestCompactLogFile_RejectsTraceFile(t *testing.T) {
	// Write a trace file (use AddSpan which sets SignalTypeTrace).
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream: &buf,
	})
	require.NoError(t, err)
	_, err = w.Flush()
	require.NoError(t, err)

	_, err = CompactLogFileBytes(buf.Bytes(), Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SignalTypeLog")
}

// TestCompactLogFile_RoundTrip verifies that compaction preserves all data:
// same number of rows, same attribute values.
func TestCompactLogFile_RoundTrip(t *testing.T) {
	rl := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
			{Key: "svc", Value: strAttr("api")},
		}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			makeLogRecord(1000, "hello world", map[string]string{"level": "info"}),
			makeLogRecord(2000, "error occurred", map[string]string{"level": "error"}),
		}}},
	}

	originalData := writeLogFile(t, []*logsv1.LogsData{wrapInLogsData(rl)})
	compacted, err := CompactLogFileBytes(originalData, Config{})
	require.NoError(t, err)

	// Read back compacted.
	r, err := modules_reader.NewReaderFromProvider(&bytesProvider{data: compacted})
	require.NoError(t, err)

	totalRows := 0
	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		require.NoError(t, getErr)
		if bwb != nil {
			totalRows += bwb.Block.SpanCount()
		}
	}
	assert.Equal(t, 2, totalRows, "compacted file should have all 2 rows")
}
