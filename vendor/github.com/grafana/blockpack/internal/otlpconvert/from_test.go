package otlpconvert_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/blockpack"
	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/blockpack/internal/parquetconv"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// TestConvertFromParquetBlock_Roundtrip builds synthetic OTLP traces, writes them to a
// Tempo vparquet5 block, converts the block to blockpack, and verifies that every span
// is present in the output with its name, service name, and span attributes intact.
func TestConvertFromParquetBlock_Roundtrip(t *testing.T) {
	traces := buildParquetRoundtripTraces()

	// Write to a temp parquet block via the shared conversion helpers.
	tmpDir := t.TempDir()
	tempoTraces, traceIDs, err := otlpconvert.BuildTempoTraces(traces)
	require.NoError(t, err)
	blockPath, _, err := parquetconv.WriteTempoBlock(tempoTraces, traceIDs, tmpDir)
	require.NoError(t, err)

	// Convert parquet → blockpack (write to a temp file).
	outPath := filepath.Join(t.TempDir(), "out.blockpack")
	outFile, err := os.Create(outPath) //nolint:gosec // path is from t.TempDir(), not user input
	require.NoError(t, err)
	require.NoError(t, parquetconv.ConvertFromParquetBlock(blockPath, outFile, 0))
	require.NoError(t, outFile.Close())

	// Read the blockpack back and collect all spans via a match-all TraceQL query.
	f, err := os.Open(outPath) //nolint:gosec // path is from t.TempDir(), not user input
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	info, err := f.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0), "blockpack output must not be empty")

	r, err := blockpack.NewReaderFromProvider(&testFileProvider{f: f})
	require.NoError(t, err)

	type spanRecord struct {
		name        string
		serviceName string
		httpMethod  string
	}
	// Key: "<traceIDHex>:<spanIDHex>" — matches the format stored in SpanMatch.
	got := make(map[string]spanRecord)

	var bpMatches []blockpack.SpanMatch
	bpMatches, err = blockpack.QueryTraceQL(r, "{}", blockpack.QueryOptions{})
	require.NoError(t, err)
	for _, m := range bpMatches {
		rec := spanRecord{}
		if v, ok := m.Fields.GetField("span:name"); ok {
			rec.name = fmt.Sprint(v)
		}
		if v, ok := m.Fields.GetField("resource.service.name"); ok {
			rec.serviceName = fmt.Sprint(v)
		}
		// Span attributes are stored with a "span." prefix in blockpack.
		if v, ok := m.Fields.GetField("span.http.method"); ok {
			rec.httpMethod = fmt.Sprint(v)
		}
		got[m.TraceID+":"+m.SpanID] = rec
	}

	// Build the expected span set from the original OTLP traces.
	expected := make(map[string]spanRecord)
	for _, td := range traces {
		for _, rs := range td.ResourceSpans {
			svcName := ""
			for _, attr := range rs.GetResource().GetAttributes() {
				if attr.Key == "service.name" {
					svcName = attr.Value.GetStringValue()
				}
			}
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					traceHex := hex.EncodeToString(span.TraceId)
					spanHex := hex.EncodeToString(span.SpanId)
					key := traceHex + ":" + spanHex
					httpMethod := ""
					for _, attr := range span.Attributes {
						if attr.Key == "http.method" {
							httpMethod = attr.Value.GetStringValue()
						}
					}
					expected[key] = spanRecord{
						name:        span.Name,
						serviceName: svcName,
						httpMethod:  httpMethod,
					}
				}
			}
		}
	}

	require.Len(t, got, len(expected), "span count mismatch: parquet roundtrip lost or duplicated spans")
	for key, want := range expected {
		have, ok := got[key]
		require.True(t, ok, "span %s not found in blockpack output after parquet roundtrip", key)
		require.Equal(t, want.name, have.name, "span:name mismatch for %s", key)
		require.Equal(t, want.serviceName, have.serviceName, "resource.service.name mismatch for %s", key)
		require.Equal(t, want.httpMethod, have.httpMethod, "span.http.method mismatch for %s", key)
	}
}

// TestConvertFromProtoFile_Roundtrip writes an OTLP TracesData proto to a temp file,
// converts it to blockpack via ConvertFromProtoFile, and verifies every span is present
// with its name, service name, and attributes intact.
func TestConvertFromProtoFile_Roundtrip(t *testing.T) {
	traces := buildParquetRoundtripTraces()

	// Merge all traces into a single TracesData message and marshal to disk.
	merged := &tracev1.TracesData{}
	for _, td := range traces {
		merged.ResourceSpans = append(merged.ResourceSpans, td.ResourceSpans...)
	}
	protoBytes, err := proto.Marshal(merged)
	require.NoError(t, err)

	protoFile := filepath.Join(t.TempDir(), "traces.pb")
	require.NoError(t, os.WriteFile(protoFile, protoBytes, 0o600))

	// Convert proto file → blockpack (write to a temp file).
	outPath := filepath.Join(t.TempDir(), "out.blockpack")
	outFile, err := os.Create(outPath) //nolint:gosec // path is from t.TempDir(), not user input
	require.NoError(t, err)
	require.NoError(t, otlpconvert.ConvertFromProtoFile(protoFile, outFile, 0))
	require.NoError(t, outFile.Close())

	// Read back and collect spans via match-all query.
	f, err := os.Open(outPath) //nolint:gosec // path is from t.TempDir(), not user input
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	info, err := f.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0), "blockpack output must not be empty")

	r, err := blockpack.NewReaderFromProvider(&testFileProvider{f: f})
	require.NoError(t, err)

	type spanRecord struct {
		name        string
		serviceName string
		httpMethod  string
	}
	got := make(map[string]spanRecord)

	var bpMatches []blockpack.SpanMatch
	bpMatches, err = blockpack.QueryTraceQL(r, "{}", blockpack.QueryOptions{})
	require.NoError(t, err)
	for _, m := range bpMatches {
		rec := spanRecord{}
		if v, ok := m.Fields.GetField("span:name"); ok {
			rec.name = fmt.Sprint(v)
		}
		if v, ok := m.Fields.GetField("resource.service.name"); ok {
			rec.serviceName = fmt.Sprint(v)
		}
		if v, ok := m.Fields.GetField("span.http.method"); ok {
			rec.httpMethod = fmt.Sprint(v)
		}
		got[m.TraceID+":"+m.SpanID] = rec
	}

	// Build expected set from the original traces.
	expected := make(map[string]spanRecord)
	for _, td := range traces {
		for _, rs := range td.ResourceSpans {
			svcName := ""
			for _, attr := range rs.GetResource().GetAttributes() {
				if attr.Key == "service.name" {
					svcName = attr.Value.GetStringValue()
				}
			}
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					key := hex.EncodeToString(span.TraceId) + ":" + hex.EncodeToString(span.SpanId)
					httpMethod := ""
					for _, attr := range span.Attributes {
						if attr.Key == "http.method" {
							httpMethod = attr.Value.GetStringValue()
						}
					}
					expected[key] = spanRecord{
						name:        span.Name,
						serviceName: svcName,
						httpMethod:  httpMethod,
					}
				}
			}
		}
	}

	require.Len(t, got, len(expected), "span count mismatch: proto roundtrip lost or duplicated spans")
	for key, want := range expected {
		have, ok := got[key]
		require.True(t, ok, "span %s not found in blockpack output after proto roundtrip", key)
		require.Equal(t, want.name, have.name, "span:name mismatch for %s", key)
		require.Equal(t, want.serviceName, have.serviceName, "resource.service.name mismatch for %s", key)
		require.Equal(t, want.httpMethod, have.httpMethod, "span.http.method mismatch for %s", key)
	}
}

// buildParquetRoundtripTraces returns three deterministic OTLP traces with realistic span data.
// Attribute types are consistent across all spans to prevent normalization type coercion.
func buildParquetRoundtripTraces() []*tracev1.TracesData {
	const baseTimeNs = uint64(1_700_000_000_000_000_000) // 2023-11-14 in nanoseconds

	type traceSpec struct {
		svc     string
		method  string
		traceID []byte
		nSpans  int
	}

	specs := []traceSpec{
		{
			traceID: []byte{0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			svc:     "frontend",
			method:  "GET",
			nSpans:  3,
		},
		{
			traceID: []byte{0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			svc:     "backend",
			method:  "POST",
			nSpans:  2,
		},
		{
			traceID: []byte{0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			svc:     "db-service",
			method:  "GET",
			nSpans:  2,
		},
	}

	out := make([]*tracev1.TracesData, 0, len(specs))
	for _, spec := range specs {
		spans := make([]*tracev1.Span, spec.nSpans)
		for i := range spec.nSpans {
			spanID := make([]byte, 8)
			spanID[0] = byte(i + 1)     //nolint:gosec // test data, not security-sensitive
			spanID[1] = spec.traceID[0] //nolint:gosec
			spans[i] = &tracev1.Span{
				TraceId:           spec.traceID,
				SpanId:            spanID,
				Name:              fmt.Sprintf("%s/op%d", spec.svc, i),
				Kind:              tracev1.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: baseTimeNs + uint64(i)*1_000_000,           //nolint:gosec
				EndTimeUnixNano:   baseTimeNs + uint64(i)*1_000_000 + 500_000, //nolint:gosec
				Attributes: []*commonv1.KeyValue{
					{Key: "http.method", Value: strAttr(spec.method)},
				},
				Status: &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK},
			}
		}
		out = append(out, &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: strAttr(spec.svc)},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{{Spans: spans}},
				},
			},
		})
	}
	return out
}

func strAttr(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

// testFileProvider implements blockpack.ReaderProvider for an *os.File.
type testFileProvider struct {
	f *os.File
}

func (p *testFileProvider) Size() (int64, error) {
	info, err := p.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *testFileProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(buf, off)
}

// TestNormalizeLogsForBlockpack_MixedTypes verifies that mixed int/string values on
// the same key are coerced to string for log attributes.
func TestNormalizeLogsForBlockpack_MixedTypes(t *testing.T) {
	ld1 := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: strAttr("svc-a")},
					},
				},
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 1_000_000,
								Attributes: []*commonv1.KeyValue{
									{Key: "my.key", Value: intAttr(42)},
								},
							},
						},
					},
				},
			},
		},
	}
	ld2 := &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano: 2_000_000,
								Attributes: []*commonv1.KeyValue{
									// Same key "my.key" but string type — mixed!
									{Key: "my.key", Value: strAttr("hello")},
								},
							},
						},
					},
				},
			},
		},
	}
	normalized, err := otlpconvert.NormalizeLogsForBlockpack([]*logsv1.LogsData{ld1, ld2})
	require.NoError(t, err)
	require.Len(t, normalized, 2)
	// Both values should now be strings.
	r0 := normalized[0].ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	r1 := normalized[1].ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	myKey0 := getAttr(r0.Attributes, "my.key")
	myKey1 := getAttr(r1.Attributes, "my.key")
	require.NotNil(t, myKey0)
	require.NotNil(t, myKey1)
	_, ok0 := myKey0.Value.(*commonv1.AnyValue_StringValue)
	_, ok1 := myKey1.Value.(*commonv1.AnyValue_StringValue)
	require.True(t, ok0, "expected string type for my.key in record 0 after normalization")
	require.True(t, ok1, "expected string type for my.key in record 1 after normalization")
}

// TestConvertLogsProtoFile_Roundtrip writes a synthetic LogsData proto to disk,
// converts it to blockpack via ConvertLogsProtoFile, and verifies the signal type
// byte and block count.
func TestConvertLogsProtoFile_Roundtrip(t *testing.T) {
	ld := buildRoundtripLogs()

	// Merge into single LogsData and write to disk.
	merged := &logsv1.LogsData{}
	for _, ld := range ld {
		merged.ResourceLogs = append(merged.ResourceLogs, ld.ResourceLogs...)
	}
	protoBytes, err := proto.Marshal(merged)
	require.NoError(t, err)

	protoFile := filepath.Join(t.TempDir(), "logs.pb")
	require.NoError(t, os.WriteFile(protoFile, protoBytes, 0o600))

	outPath := filepath.Join(t.TempDir(), "out.blockpack")
	outFile, err := os.Create(outPath) //nolint:gosec // path is from t.TempDir()
	require.NoError(t, err)
	require.NoError(t, otlpconvert.ConvertLogsProtoFile(protoFile, outFile, 0))
	require.NoError(t, outFile.Close())

	f, err := os.Open(outPath) //nolint:gosec // path is from t.TempDir()
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	info, err := f.Stat()
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0), "blockpack output must not be empty")

	r, err := blockpack.NewReaderFromProvider(&testFileProvider{f: f})
	require.NoError(t, err)

	require.Equal(t, blockpack.SignalTypeLog, r.SignalType(),
		"expected SignalTypeLog in output file")
	require.Greater(t, r.BlockCount(), 0, "expected at least one block")
	totalRecords := 0
	for i := range r.BlockCount() {
		totalRecords += int(r.BlockMeta(i).SpanCount)
	}
	require.Equal(t, 5, totalRecords, "expected 5 log records (2+3 across 2 services)")
}

func buildRoundtripLogs() []*logsv1.LogsData {
	const baseTimeNs = uint64(1_700_000_000_000_000_000)
	type svcSpec struct {
		svc      string
		nRecords int
	}
	specs := []svcSpec{
		{svc: "frontend", nRecords: 2},
		{svc: "backend", nRecords: 3},
	}
	out := make([]*logsv1.LogsData, 0, len(specs))
	for i, spec := range specs {
		records := make([]*logsv1.LogRecord, spec.nRecords)
		for j := range spec.nRecords {
			records[j] = &logsv1.LogRecord{
				TimeUnixNano: baseTimeNs + uint64(i)*1_000_000 + uint64(j)*1_000, //nolint:gosec
				Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{
					StringValue: fmt.Sprintf("%s/log%d", spec.svc, j),
				}},
				SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
				SeverityText:   "INFO",
				Attributes: []*commonv1.KeyValue{
					{Key: "env", Value: strAttr("test")},
				},
			}
		}
		out = append(out, &logsv1.LogsData{
			ResourceLogs: []*logsv1.ResourceLogs{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{Key: "service.name", Value: strAttr(spec.svc)},
						},
					},
					ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: records}},
				},
			},
		})
	}
	return out
}

func intAttr(i int64) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: i}}
}

func getAttr(attrs []*commonv1.KeyValue, key string) *commonv1.AnyValue {
	for _, a := range attrs {
		if a != nil && a.Key == key {
			return a.Value
		}
	}
	return nil
}
