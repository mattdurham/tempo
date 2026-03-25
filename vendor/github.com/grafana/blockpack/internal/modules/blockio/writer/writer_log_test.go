package writer

import (
	"bytes"
	"encoding/binary"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/stretchr/testify/require"
)

// TestSortPendingLogs verifies that sortPendingLogs orders by
// (minHashSig ASC, timestamp ASC). NOTE-37: svcName removed as primary key.
func TestSortPendingLogs(t *testing.T) {
	records := []pendingLogRecord{
		{minHashSig: [4]uint64{300, 0, 0, 0}, timestamp: 200},
		{minHashSig: [4]uint64{100, 0, 0, 0}, timestamp: 300},
		{minHashSig: [4]uint64{100, 0, 0, 0}, timestamp: 100},
		{svcName: "svc-z", minHashSig: [4]uint64{50, 0, 0, 0}, timestamp: 500},
	}
	sortPendingLogs(records)
	// svcName is ignored — sort is purely by minHashSig then timestamp.
	require.Equal(t, uint64(50), records[0].minHashSig[0])
	require.Equal(t, uint64(500), records[0].timestamp)
	require.Equal(t, uint64(100), records[1].minHashSig[0])
	require.Equal(t, uint64(100), records[1].timestamp)
	require.Equal(t, uint64(100), records[2].minHashSig[0])
	require.Equal(t, uint64(300), records[2].timestamp)
	require.Equal(t, uint64(300), records[3].minHashSig[0])
}

// TestComputeMinHashSigFromLog verifies that attribute keys are hashed.
func TestComputeMinHashSigFromLog(t *testing.T) {
	rl := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{Key: "service.name", Value: strAttr("my-svc")},
			},
		},
	}
	sl := &logsv1.ScopeLogs{}
	record := &logsv1.LogRecord{
		Attributes: []*commonv1.KeyValue{
			{Key: "log.level", Value: strAttr("info")},
		},
	}
	plr := pendingLogRecord{rl: rl, sl: sl, record: record}
	computeMinHashSigFromLog(&plr)
	// At least one slot should be non-max (a key was hashed).
	allMax := true
	for _, v := range plr.minHashSig {
		if v != ^uint64(0) {
			allMax = false
		}
	}
	require.False(t, allMax, "expected at least one hash slot to be populated")
}

// TestComputeMinHashSigFromLog_ValueSensitivity verifies that the minhash signature
// is sensitive to attribute values, not just attribute keys.
func TestComputeMinHashSigFromLog_ValueSensitivity(t *testing.T) {
	makeRecord := func(kvs ...[2]string) *logsv1.LogRecord {
		attrs := make([]*commonv1.KeyValue, 0, len(kvs))
		for _, kv := range kvs {
			attrs = append(attrs, &commonv1.KeyValue{Key: kv[0], Value: strAttr(kv[1])})
		}
		return &logsv1.LogRecord{Attributes: attrs}
	}

	makeRL := func(kvs ...[2]string) *logsv1.ResourceLogs {
		attrs := make([]*commonv1.KeyValue, 0, len(kvs))
		for _, kv := range kvs {
			attrs = append(attrs, &commonv1.KeyValue{Key: kv[0], Value: strAttr(kv[1])})
		}
		return &logsv1.ResourceLogs{
			Resource: &resourcev1.Resource{Attributes: attrs},
		}
	}

	sig := func(plr pendingLogRecord) [4]uint64 {
		computeMinHashSigFromLog(&plr)
		return plr.minHashSig
	}

	// Case 1: same keys, different values → different signatures.
	sigA := sig(pendingLogRecord{
		record: makeRecord([2]string{"service.name", "svc-a"}, [2]string{"env", "prod"}),
	})
	sigB := sig(pendingLogRecord{
		record: makeRecord([2]string{"service.name", "svc-b"}, [2]string{"env", "prod"}),
	})
	require.NotEqual(t, sigA, sigB, "different values should produce different signatures")

	// Case 2: same keys, same values → identical signatures.
	sigC := sig(pendingLogRecord{
		record: makeRecord([2]string{"service.name", "svc-a"}, [2]string{"env", "prod"}),
	})
	require.Equal(t, sigA, sigC, "same keys and values should produce identical signatures")

	// Case 3: different keys → different signatures.
	sigD := sig(pendingLogRecord{
		record: makeRecord([2]string{"component", "svc-a"}, [2]string{"env", "prod"}),
	})
	require.NotEqual(t, sigA, sigD, "different keys should produce different signatures")

	// Case 4: no attributes → all max values (signature is the zero value).
	maxSig := [4]uint64{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)}
	sigEmpty := sig(pendingLogRecord{record: &logsv1.LogRecord{}})
	require.Equal(t, maxSig, sigEmpty, "empty attributes should leave signature at max values")

	// Case 5: resource attributes contribute to the signature.
	sigRL1 := sig(pendingLogRecord{
		rl:     makeRL([2]string{"cluster", "us-east"}),
		record: makeRecord([2]string{"service.name", "svc-a"}),
	})
	sigRL2 := sig(pendingLogRecord{
		rl:     makeRL([2]string{"cluster", "eu-west"}),
		record: makeRecord([2]string{"service.name", "svc-a"}),
	})
	require.NotEqual(t, sigRL1, sigRL2, "different resource attribute values should produce different signatures")
}

// TestBuildLogBlock_Intrinsics verifies that all 8 intrinsic columns are written.
func TestBuildLogBlock_Intrinsics(t *testing.T) {
	traceIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	rl := &logsv1.ResourceLogs{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{Key: "service.name", Value: strAttr("test-svc")},
			},
		},
	}
	sl := &logsv1.ScopeLogs{}
	record := &logsv1.LogRecord{
		TimeUnixNano:         1_700_000_000_000_000_000,
		ObservedTimeUnixNano: 1_700_000_000_000_000_001,
		Body:                 &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "hello world"}},
		SeverityNumber:       logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:         "INFO",
		TraceId:              traceIDBytes,
		SpanId:               spanIDBytes,
		Flags:                1,
		Attributes: []*commonv1.KeyValue{
			{Key: "env", Value: strAttr("prod")},
		},
	}
	pending := []pendingLogRecord{
		{rl: rl, sl: sl, record: record, svcName: "test-svc", timestamp: record.TimeUnixNano},
	}
	enc, err := newZstdEncoder()
	require.NoError(t, err)
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)
	require.Equal(t, 1, built.spanCount)
	require.Equal(t, record.TimeUnixNano, built.minStart)
	require.Equal(t, record.TimeUnixNano, built.maxStart)
	require.NotEmpty(t, built.payload)
}

// TestBuildLogBlock_ConditionalIntrinsics verifies that absent optional fields
// (trace_id, span_id, flags, severity) do NOT crash and produce a valid block.
func TestBuildLogBlock_ConditionalIntrinsics(t *testing.T) {
	rl := &logsv1.ResourceLogs{}
	sl := &logsv1.ScopeLogs{}
	record := &logsv1.LogRecord{
		TimeUnixNano: 1_000_000,
		// No TraceId, no SpanId, no Flags, no SeverityNumber, no SeverityText
	}
	pending := []pendingLogRecord{
		{rl: rl, sl: sl, record: record, timestamp: record.TimeUnixNano},
	}
	enc, err := newZstdEncoder()
	require.NoError(t, err)
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)
	require.Equal(t, 1, built.spanCount)
}

// TestFlush_LogFile_SignalTypeByte verifies the output file has version=13 and
// signal byte=0x02 at the correct offsets in the file header.
func TestFlush_LogFile_SignalTypeByte(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriterWithConfig(Config{OutputStream: &buf, MaxBlockSpans: 100})
	require.NoError(t, err)

	record := &logsv1.LogRecord{TimeUnixNano: 1_000_000}
	ld := &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{
		{ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{record}}}},
	}}
	require.NoError(t, w.AddLogsData(ld))
	_, err = w.Flush()
	require.NoError(t, err)

	data := buf.Bytes()
	// The file header is located at headerOffset, which is stored in the footer.
	// Writer now always produces V4 footers (34 bytes).
	// footer layout: version[2] + headerOffset[8] + compactOffset[8] + compactLen[4] + intrinsicOffset[8] + intrinsicLen[4]
	require.GreaterOrEqual(t, len(data), int(shared.FooterV4Size), "file too small") //nolint:gosec
	footerStart := len(data) - int(shared.FooterV4Size)                              //nolint:gosec
	headerOffset := binary.LittleEndian.Uint64(data[footerStart+2 : footerStart+10])
	require.Less(t, int(headerOffset)+22, len(data), "headerOffset out of range") //nolint:gosec

	// File header layout: magic[4] + version[1] + metadataOffset[8] + metadataLen[8] + signalType[1]
	hdrSlice := data[headerOffset:]
	require.GreaterOrEqual(t, len(hdrSlice), 22)
	fileVersion := hdrSlice[4]
	signalType := hdrSlice[21]
	require.Equal(t, shared.VersionV13, fileVersion, "expected version 13 for log file")
	require.Equal(t, shared.SignalTypeLog, signalType, "expected signal type 0x02 for log file")
}

// TestWriter_MixedSignalType_ReturnsError verifies that calling AddLogsData on a Writer
// that already has buffered trace data returns a non-nil error.
func TestWriter_MixedSignalType_ReturnsError(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriterWithConfig(Config{OutputStream: &buf, MaxBlockSpans: 100})
	require.NoError(t, err)

	// Force signalType to SignalTypeTrace and simulate buffered trace data.
	w.signalType = shared.SignalTypeTrace
	w.pending = append(w.pending, pendingSpan{}) // simulate buffered trace data

	ld := &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{}}}
	err = w.AddLogsData(ld)
	require.Error(t, err, "expected error when mixing signal types")
}

func strAttr(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func makeLogPendingWithAttr(key, val string, ts uint64) pendingLogRecord {
	return pendingLogRecord{
		rl: &logsv1.ResourceLogs{},
		sl: &logsv1.ScopeLogs{},
		record: &logsv1.LogRecord{
			TimeUnixNano: ts,
			Attributes: []*commonv1.KeyValue{
				{Key: key, Value: strAttr(val)},
			},
		},
		timestamp: ts,
	}
}

// NOTE-040: numeric override for all-parseable string columns
func TestBuildLogBlock_NumericStringColumn_AllInt(t *testing.T) {
	enc, err := newZstdEncoder()
	require.NoError(t, err)

	pending := []pendingLogRecord{
		makeLogPendingWithAttr("latency_ms", "100", 1000),
		makeLogPendingWithAttr("latency_ms", "200", 2000),
		makeLogPendingWithAttr("latency_ms", "300", 3000),
	}
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)

	mm, ok := built.colMinMax["log.latency_ms"]
	require.True(t, ok, "log.latency_ms must have a range entry")
	require.Equal(t, shared.ColumnTypeRangeInt64, mm.colType,
		"all-int string column must produce ColumnTypeRangeInt64")

	// Verify min/max keys decode to 100 and 300.
	require.Equal(t, 8, len(mm.minKey))
	require.Equal(t, 8, len(mm.maxKey))
	minV := int64(binary.LittleEndian.Uint64([]byte(mm.minKey))) //nolint:gosec
	maxV := int64(binary.LittleEndian.Uint64([]byte(mm.maxKey))) //nolint:gosec
	require.Equal(t, int64(100), minV)
	require.Equal(t, int64(300), maxV)
}

func TestBuildLogBlock_NumericStringColumn_Mixed(t *testing.T) {
	enc, err := newZstdEncoder()
	require.NoError(t, err)

	pending := []pendingLogRecord{
		makeLogPendingWithAttr("latency_ms", "100", 1000),
		makeLogPendingWithAttr("latency_ms", "fast", 2000),
		makeLogPendingWithAttr("latency_ms", "300", 3000),
	}
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)

	mm, ok := built.colMinMax["log.latency_ms"]
	require.True(t, ok)
	isStringType := mm.colType == shared.ColumnTypeString || mm.colType == shared.ColumnTypeRangeString
	require.True(t, isStringType,
		"mixed column must not be promoted to numeric (got colType=%d)", mm.colType)
}

func TestBuildLogBlock_NumericStringColumn_Float(t *testing.T) {
	enc, err := newZstdEncoder()
	require.NoError(t, err)

	pending := []pendingLogRecord{
		makeLogPendingWithAttr("ratio", "0.5", 1000),
		makeLogPendingWithAttr("ratio", "0.75", 2000),
		makeLogPendingWithAttr("ratio", "0.99", 3000),
	}
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)

	mm, ok := built.colMinMax["log.ratio"]
	require.True(t, ok)
	require.Equal(t, shared.ColumnTypeRangeFloat64, mm.colType,
		"all-float (non-int) string column must produce ColumnTypeRangeFloat64")
}

func TestBuildLogBlock_NumericStringColumn_EmptySkipped(t *testing.T) {
	enc, err := newZstdEncoder()
	require.NoError(t, err)

	pending := []pendingLogRecord{
		makeLogPendingWithAttr("code", "200", 1000),
		makeLogPendingWithAttr("code", "200", 2000),
		makeLogPendingWithAttr("code", "", 3000), // empty present value
		makeLogPendingWithAttr("code", "200", 4000),
	}
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)

	mm, ok := built.colMinMax["log.code"]
	require.True(t, ok)
	// Empty strings are skipped; should not mark column as non-numeric.
	require.Equal(t, shared.ColumnTypeRangeInt64, mm.colType,
		"empty strings must not prevent numeric index for otherwise all-int column")
}

func TestBuildLogBlock_NumericStringColumn_OnlyEmpty(t *testing.T) {
	enc, err := newZstdEncoder()
	require.NoError(t, err)

	pending := []pendingLogRecord{
		makeLogPendingWithAttr("tag", "", 1000),
		makeLogPendingWithAttr("tag", "", 2000),
	}
	built, err := buildLogBlock(pending, enc)
	require.NoError(t, err)

	// If the column appears in colMinMax at all, it should remain string (no numeric
	// substitution because colNumericMinMax["log.tag"] will be nil).
	if mm, ok := built.colMinMax["log.tag"]; ok {
		require.Equal(t, shared.ColumnTypeRangeString, mm.colType,
			"column with only empty strings must not be substituted to numeric")
	}
	// It is also acceptable for the column to have no colMinMax entry when all keys
	// are empty (encodeRangeKey returns "" for empty strings).
}

// LOG-08: JSON body extracted into log.{key} ColumnTypeRangeString columns
// TestAddLogRecordFromProto_JSONBodyExtracted verifies that a JSON log body produces
// log.{key} range columns populated with the extracted values.
// SPEC-11.5: auto-parsed body columns
func TestAddLogRecordFromProto_JSONBodyExtracted(t *testing.T) {
	bb := newLogBlockBuilder(4)
	plr := &pendingLogRecord{
		rl: &logsv1.ResourceLogs{},
		sl: &logsv1.ScopeLogs{},
		record: &logsv1.LogRecord{
			TimeUnixNano: 1000,
			Body: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{
					StringValue: `{"level":"error","service":"api"}`,
				},
			},
		},
	}
	bb.addLogRecordFromProto(plr, 0)

	// Verify log.level column was created
	key := shared.ColumnKey{Name: "log.level", Type: shared.ColumnTypeRangeString}
	cb, ok := bb.columns[key]
	require.True(t, ok, "expected log.level column to exist")
	require.Equal(t, 1, cb.rowCount())

	// Verify log.service column was created
	key2 := shared.ColumnKey{Name: "log.service", Type: shared.ColumnTypeRangeString}
	_, ok2 := bb.columns[key2]
	require.True(t, ok2, "expected log.service column to exist")
}

// LOG-09: logfmt body extracted into log.{key} ColumnTypeRangeString columns
// TestAddLogRecordFromProto_LogfmtBodyExtracted verifies logfmt body parsing.
// SPEC-11.5: auto-parsed body columns
func TestAddLogRecordFromProto_LogfmtBodyExtracted(t *testing.T) {
	bb := newLogBlockBuilder(4)
	plr := &pendingLogRecord{
		rl: &logsv1.ResourceLogs{},
		sl: &logsv1.ScopeLogs{},
		record: &logsv1.LogRecord{
			TimeUnixNano: 1000,
			Body: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{
					StringValue: `level=warn msg=timeout duration=150ms`,
				},
			},
		},
	}
	bb.addLogRecordFromProto(plr, 0)

	key := shared.ColumnKey{Name: "log.level", Type: shared.ColumnTypeRangeString}
	cb, ok := bb.columns[key]
	require.True(t, ok, "expected log.level column")
	require.Equal(t, 1, cb.rowCount())
}

// LOG-10: unparseable body produces no extra log.{key} columns (silent failure)
// TestAddLogRecordFromProto_UnparsedBodyNoExtraColumns verifies that unparseable
// bodies do not produce any extra log.{key} columns.
// SPEC-11.5: silent failure semantics
func TestAddLogRecordFromProto_UnparsedBodyNoExtraColumns(t *testing.T) {
	bb := newLogBlockBuilder(4)
	plr := &pendingLogRecord{
		rl: &logsv1.ResourceLogs{},
		sl: &logsv1.ScopeLogs{},
		record: &logsv1.LogRecord{
			TimeUnixNano: 1000,
			Body: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{
					StringValue: "just a plain text log line with no structure",
				},
			},
		},
	}
	// Count columns before
	columnsBefore := len(bb.columns)
	bb.addLogRecordFromProto(plr, 0)
	// Only intrinsic columns; no log.{key} extras
	require.Equal(t, columnsBefore, len(bb.columns),
		"unparseable body should not add extra columns")
}

// LOG-11: nil body produces no extra columns
// TestAddLogRecordFromProto_NilBodyNoExtraColumns verifies nil body path.
func TestAddLogRecordFromProto_NilBodyNoExtraColumns(t *testing.T) {
	bb := newLogBlockBuilder(4)
	plr := &pendingLogRecord{
		rl:     &logsv1.ResourceLogs{},
		sl:     &logsv1.ScopeLogs{},
		record: &logsv1.LogRecord{TimeUnixNano: 1000, Body: nil},
	}
	columnsBefore := len(bb.columns)
	bb.addLogRecordFromProto(plr, 0)
	require.Equal(t, columnsBefore, len(bb.columns))
}
