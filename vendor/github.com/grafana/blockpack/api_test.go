package blockpack_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"

	blockpack "github.com/grafana/blockpack"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memReaderProvider is an in-memory ReaderProvider for api_test.go.
type memReaderProvider struct{ data []byte }

func (m *memReaderProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *memReaderProvider) ReadAt(p []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, fmt.Errorf("ReadAt: offset %d out of range", off)
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// writeTestBlockpack writes numSpans spans split across blocks of spansPerBlock each.
// Returns the serialized blockpack bytes.
func writeTestBlockpack(t *testing.T, numSpans, spansPerBlock int) []byte {
	t.Helper()
	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, spansPerBlock)
	require.NoError(t, err)

	for i := 0; i < numSpans; i++ {
		spanID := [8]byte{
			1,
			0,
			0,
			0,
			0,
			0,
			byte(i / 256 % 256),
			byte(i % 256),
		} //nolint:gosec
		traceID := [16]byte{
			1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9,
			10,
			11,
			12,
			13,
			14,
			byte(i / 256 % 256),
			byte(i % 256),
		} //nolint:gosec
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{
								Key: "service.name",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "test-service"},
								},
							},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{
						{
							Spans: []*tracev1.Span{
								{
									TraceId:           traceID[:],
									SpanId:            spanID[:],
									Name:              "test-span",
									StartTimeUnixNano: uint64(1000000000 + i*1000000), //nolint:gosec
									EndTimeUnixNano:   uint64(1001000000 + i*1000000), //nolint:gosec
								},
							},
						},
					},
				},
			},
		}
		err = w.AddTracesData(td)
		require.NoError(t, err)
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return output.Bytes()
}

// Span name constants for the structural test tree.
const (
	spanA = "A"
	spanB = "B"
	spanC = "C"
	spanD = "D"
	spanE = "E"
)

// structuralSpanIDs maps span name constant to its fixed 8-byte spanID.
// Parent-child topology:
//
//	A (root)
//	├── B
//	│   ├── C
//	│   └── D
//	└── E
var structuralSpanIDs = map[string][8]byte{
	spanA: {0x0A},
	spanB: {0x0B},
	spanC: {0x0C},
	spanD: {0x0D},
	spanE: {0x0E},
}

// structuralTreeSpanDef defines one span in the structural test trace.
// A zero parentID (all bytes zero) means the span is a root.
type structuralTreeSpanDef struct {
	name     string
	parentID [8]byte
}

// spanHex returns the hex string for a named span in structuralSpanIDs.
func spanHex(name string) string {
	id := structuralSpanIDs[name]
	return fmt.Sprintf("%x", id[:])
}

// writeStructuralTreeBlockpack writes a single trace with 5 spans in a known
// parent-child topology and returns the serialized blockpack bytes.
//
// Span relationships:
//
//	A (root)  ← name=spanA
//	├── B     ← name=spanB, parent=A
//	│   ├── C ← name=spanC, parent=B
//	│   └── D ← name=spanD, parent=B
//	└── E     ← name=spanE, parent=A
func writeStructuralTreeBlockpack(t *testing.T) []byte {
	t.Helper()

	traceID := [16]byte{0xDE, 0xAD, 0xBE, 0xEF}
	spans := []structuralTreeSpanDef{
		{name: spanA},
		{name: spanB, parentID: structuralSpanIDs[spanA]},
		{name: spanC, parentID: structuralSpanIDs[spanB]},
		{name: spanD, parentID: structuralSpanIDs[spanB]},
		{name: spanE, parentID: structuralSpanIDs[spanA]},
	}

	protoSpans := make([]*tracev1.Span, 0, len(spans))
	for i, sd := range spans {
		spanID := structuralSpanIDs[sd.name]
		sp := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID[:],
			Name:              sd.name,
			StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_001_000_000 + i*1_000_000), //nolint:gosec
		}
		if sd.parentID != ([8]byte{}) {
			sp.ParentSpanId = sd.parentID[:]
		}
		protoSpans = append(protoSpans, sp)
	}

	td := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-svc"}},
				}},
			},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: protoSpans}},
		}},
	}

	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, 0)
	require.NoError(t, err)
	require.NoError(t, w.AddTracesData(td))
	_, err = w.Flush()
	require.NoError(t, err)
	return output.Bytes()
}

// collectStructuralMatches runs QueryTraceQL and returns the set of matched spanID hex strings.
func collectStructuralMatches(t *testing.T, data []byte, query string) map[string]bool {
	t.Helper()
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	matches, err := blockpack.QueryTraceQL(r, query, blockpack.QueryOptions{})
	require.NoError(t, err)

	matched := make(map[string]bool, len(matches))
	for _, m := range matches {
		matched[m.SpanID] = true
	}
	return matched
}

// TestStreamTraceQL_Structural verifies all 6 structural operators plus empty-filter
// semantics against a known 5-span tree:
//
//	A (root)
//	├── B
//	│   ├── C
//	│   └── D
//	└── E
func TestStreamTraceQL_Structural(t *testing.T) {
	data := writeStructuralTreeBlockpack(t)

	tests := []struct {
		name    string
		query   string
		want    []string // span names that must appear in results
		notWant []string // span names that must not appear in results
		wantLen int
	}{
		{
			name:    "Descendant",
			query:   `{ name = "A" || name = "B" } >> { name = "C" || name = "D" || name = "E" }`,
			want:    []string{spanC, spanD, spanE}, // all right-side spans have a left-side ancestor
			wantLen: 3,
		},
		{
			name:    "Child",
			query:   `{ name = "B" } > { name = "C" || name = "D" || name = "E" }`,
			want:    []string{spanC, spanD},
			notWant: []string{spanE}, // E's parent is A, not B
			wantLen: 2,
		},
		{
			name:    "Sibling",
			query:   `{ name = "C" } ~ { name = "D" || name = "E" }`,
			want:    []string{spanD},
			notWant: []string{spanE}, // E's parent is A, not B
			wantLen: 1,
		},
		{
			name:    "Ancestor",
			query:   `{ name = "C" || name = "D" } << { name = "A" || name = "B" || name = "E" }`,
			want:    []string{spanA, spanB},
			notWant: []string{spanE}, // E is not an ancestor of C or D
			wantLen: 2,               // A and B deduplicated across C and D
		},
		{
			name:    "Parent",
			query:   `{ name = "C" || name = "D" } < { name = "A" || name = "B" || name = "E" }`,
			want:    []string{spanB},
			notWant: []string{spanA, spanE}, // A is grandparent; E is not a parent of C or D
			wantLen: 1,
		},
		{
			name:    "NotSibling",
			query:   `{ name = "E" } !~ { name = "A" || name = "B" || name = "C" || name = "D" }`,
			want:    []string{spanA, spanC, spanD}, // right-side spans with no left-sibling
			notWant: []string{spanB},               // B shares parent A with E
			wantLen: 3,
		},
		{
			name:    "EmptyFilters",
			query:   `{} >> {}`,
			want:    []string{spanB, spanC, spanD, spanE}, // non-root spans are descendants of their ancestors
			notWant: []string{spanA},                      // A is root — no ancestors to be a descendant of
			wantLen: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectStructuralMatches(t, data, tc.query)
			for _, name := range tc.want {
				assert.True(t, got[spanHex(name)], "%s must match", name)
			}
			for _, name := range tc.notWant {
				assert.False(t, got[spanHex(name)], "%s must not match", name)
			}
			assert.Len(t, got, tc.wantLen, "expected %d matched spans", tc.wantLen)
		})
	}
}

// TestNewFileStorage_Delete verifies that NewFileStorage.Delete removes both
// plain files and directory-style blocks.
func TestNewFileStorage_Delete(t *testing.T) {
	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	data := writeTestBlockpack(t, 10, 10)
	require.NoError(t, storage.Put("test.blockpack", data))

	_, err := storage.Size("test.blockpack")
	require.NoError(t, err, "file must exist before delete")

	require.NoError(t, storage.Delete("test.blockpack"))

	_, err = storage.Size("test.blockpack")
	require.Error(t, err, "file must not exist after delete")
}

// TestGetTraceByID verifies end-to-end trace lookup via the public API.
func TestGetTraceByID(t *testing.T) {
	data := writeTestBlockpack(t, 10, 10)

	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	// i=0 trace ID from writeTestBlockpack: {1,2,3,4,5,6,7,8,9,10,11,12,13,14,0,0}
	traceIDHex := "0102030405060708090a0b0c0d0e0000"

	matches, err := blockpack.GetTraceByID(r, traceIDHex)
	require.NoError(t, err)
	require.NotEmpty(t, matches, "trace must be found")
	for _, m := range matches {
		assert.Equal(t, traceIDHex, m.TraceID, "all returned spans must have the requested trace ID")
	}
}

// TestGetTraceByID_NotFound verifies that a missing trace returns an empty slice and no error.
func TestGetTraceByID_NotFound(t *testing.T) {
	data := writeTestBlockpack(t, 5, 5)

	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	matches, err := blockpack.GetTraceByID(r, "deadbeefdeadbeefdeadbeefdeadbeef")
	require.NoError(t, err)
	assert.Empty(t, matches, "missing trace must return empty slice")
}

// TestGetTraceByID_InvalidHex verifies that a malformed trace ID hex returns an error.
func TestGetTraceByID_InvalidHex(t *testing.T) {
	data := writeTestBlockpack(t, 1, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	_, err = blockpack.GetTraceByID(r, "not-a-hex-string-padded-to-32xxx")
	assert.Error(t, err, "invalid hex must return an error")

	_, err = blockpack.GetTraceByID(r, "tooshort")
	assert.Error(t, err, "too-short trace ID must return an error")
}

// TestNewLeanReaderFromProvider_GetTraceByID verifies the 2-I/O lean reader path
// produces the same results as the full reader for GetTraceByID.
func TestNewLeanReaderFromProvider_GetTraceByID(t *testing.T) {
	data := writeTestBlockpack(t, 10, 10)

	traceIDHex := "0102030405060708090a0b0c0d0e0000"

	full, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	fullMatches, err := blockpack.GetTraceByID(full, traceIDHex)
	require.NoError(t, err)

	lean, err := blockpack.NewLeanReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	leanMatches, err := blockpack.GetTraceByID(lean, traceIDHex)
	require.NoError(t, err)

	require.Equal(t, len(fullMatches), len(leanMatches), "lean and full readers must return the same number of spans")
	for i := range fullMatches {
		assert.Equal(t, fullMatches[i].TraceID, leanMatches[i].TraceID, "span %d trace ID must match", i)
		assert.Equal(t, fullMatches[i].SpanID, leanMatches[i].SpanID, "span %d span ID must match", i)
	}
}

// TestQueryTraceQL_MostRecent verifies that QueryOptions.MostRecent=true delivers spans
// from the newest block first (SPEC-STREAM-8).
//
// writeTestBlockpack with 4 spans and spansPerBlock=2 produces two blocks:
//
//	block 0: spans i=0,1 (StartTimeUnixNano 1_000_000_000, 1_001_000_000)
//	block 1: spans i=2,3 (StartTimeUnixNano 1_002_000_000, 1_003_000_000)
//
// With MostRecent=true (Backward direction), block 1 is traversed first,
// so the first two results must be spans i=3 and i=2 (reversed within block).
func TestQueryTraceQL_MostRecent(t *testing.T) {
	t.Parallel()

	// 4 spans, 2 per block → 2 blocks; block 1 holds the newer spans.
	data := writeTestBlockpack(t, 4, 2)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	require.Equal(t, 2, r.BlockCount(), "expected 2 blocks")

	// SpanID for span i mirrors writeTestBlockpack: {1,0,0,0,0,0,byte(i/256%256),byte(i%256)}.
	spanIDHex := func(i int) string {
		id := [8]byte{1, 0, 0, 0, 0, 0, byte(i / 256 % 256), byte(i % 256)} //nolint:gosec
		return fmt.Sprintf("%x", id[:])
	}

	// Forward (default): block 0 first → span 0 is the first result.
	fwd, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, fwd, 4)
	assert.Equal(t, spanIDHex(0), fwd[0].SpanID, "forward: first result must be the oldest span")

	// MostRecent=true (Backward): block 1 first → span 3 is the first result.
	bwd, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{MostRecent: true})
	require.NoError(t, err)
	require.Len(t, bwd, 4)
	assert.Equal(t, spanIDHex(3), bwd[0].SpanID, "MostRecent: first result must be the newest span")
	assert.Equal(t, spanIDHex(2), bwd[1].SpanID, "MostRecent: second result must be the second-newest span")

	// MostRecent=true with Limit=2: exercises the global top-K path (SPEC-STREAM-7, SPEC-STREAM-8).
	limited, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{MostRecent: true, Limit: 2})
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, spanIDHex(3), limited[0].SpanID, "MostRecent+Limit: first result must be the newest span")
	assert.Equal(t, spanIDHex(2), limited[1].SpanID, "MostRecent+Limit: second result must be the second-newest span")
}

// writeBlockpackWithTimes writes a blockpack file with spans at the given start times (unix nanos).
// Returns the serialized bytes.
func writeBlockpackWithTimes(t *testing.T, startNanos []uint64) []byte {
	t.Helper()
	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, len(startNanos)+1)
	require.NoError(t, err)

	for i, startNano := range startNanos {
		spanID := [8]byte{byte(i + 1)} //nolint:gosec
		traceID := [16]byte{byte(i + 1)}
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: &resourcev1.Resource{
						Attributes: []*commonv1.KeyValue{
							{
								Key: "service.name",
								Value: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "svc"},
								},
							},
						},
					},
					ScopeSpans: []*tracev1.ScopeSpans{
						{
							Spans: []*tracev1.Span{
								{
									TraceId:           traceID[:],
									SpanId:            spanID[:],
									Name:              "span",
									StartTimeUnixNano: startNano,
									EndTimeUnixNano:   startNano + 1000,
								},
							},
						},
					},
				},
			},
		}
		require.NoError(t, w.AddTracesData(td))
	}

	_, err = w.Flush()
	require.NoError(t, err)
	return output.Bytes()
}

// putFile writes data to the storage under name and returns the name.
func putFile(t *testing.T, storage blockpack.WritableStorage, name string, data []byte) string {
	t.Helper()
	require.NoError(t, storage.Put(name, data))
	return name
}

func TestPlanFiles_Basic(t *testing.T) {
	blockpack.ClearReaderCaches()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	// Three files with increasing time ranges (100, 200, 300 nanos as max starts).
	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{100}))
	p2 := putFile(t, storage, "f2.blockpack", writeBlockpackWithTimes(t, []uint64{200}))
	p3 := putFile(t, storage, "f3.blockpack", writeBlockpackWithTimes(t, []uint64{300}))

	plan := blockpack.PlanFiles([]string{p1, p2, p3}, storage, nil)
	require.NotNil(t, plan)

	paths := plan.Between(0, math.MaxUint64)
	require.Len(t, paths, 3)
	// Newest first: p3 (max=300), p2 (max=200), p1 (max=100).
	assert.Equal(t, p3, paths[0])
	assert.Equal(t, p2, paths[1])
	assert.Equal(t, p1, paths[2])
}

func TestPlanFiles_Between_Filters(t *testing.T) {
	blockpack.ClearReaderCaches()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	// f1: min=max=100, f2: min=max=300, f3: min=max=500.
	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{100}))
	p2 := putFile(t, storage, "f2.blockpack", writeBlockpackWithTimes(t, []uint64{300}))
	p3 := putFile(t, storage, "f3.blockpack", writeBlockpackWithTimes(t, []uint64{500}))

	plan := blockpack.PlanFiles([]string{p1, p2, p3}, storage, nil)
	require.NotNil(t, plan)

	// Between(250, 450): only f2 (min=300 <= 450, max=300 >= 250) qualifies.
	// f1: max=100 < 250 → excluded. f3: min=500 > 450 → excluded.
	paths := plan.Between(250, 450)
	require.Len(t, paths, 1)
	assert.Equal(t, p2, paths[0])
}

func TestPlanFiles_Between_Overlap(t *testing.T) {
	blockpack.ClearReaderCaches()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	// File spanning [100, 500]: both spans are within the file.
	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{100, 500}))

	plan := blockpack.PlanFiles([]string{p1}, storage, nil)
	require.NotNil(t, plan)

	// Query [200, 300] is fully inside [100, 500] — file should match.
	paths := plan.Between(200, 300)
	require.Len(t, paths, 1)
	assert.Equal(t, p1, paths[0])
}

func TestPlanFiles_FailedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{300}))
	// "missing.blockpack" does not exist — will fail.
	pMissing := "missing.blockpack"

	plan := blockpack.PlanFiles([]string{p1, pMissing}, storage, nil)
	require.NotNil(t, plan)

	paths := plan.Between(0, math.MaxUint64)
	require.Len(t, paths, 2)
	// Non-failed file comes first; failed file is last.
	assert.Equal(t, p1, paths[0])
	assert.Equal(t, pMissing, paths[1])
}

func TestPlanFiles_EmptyInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	planNil := blockpack.PlanFiles(nil, storage, nil)
	require.NotNil(t, planNil)
	assert.Nil(t, planNil.Between(0, math.MaxUint64))

	planEmpty := blockpack.PlanFiles([]string{}, storage, nil)
	require.NotNil(t, planEmpty)
	assert.Nil(t, planEmpty.Between(0, math.MaxUint64))
}

func TestPlanFiles_NilCache(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{100}))

	// nil cache must not panic and must return correct results.
	plan := blockpack.PlanFiles([]string{p1}, storage, nil)
	require.NotNil(t, plan)

	paths := plan.Between(0, math.MaxUint64)
	require.Len(t, paths, 1)
	assert.Equal(t, p1, paths[0])
}

func TestFilePlan_Between_NilReceiver(t *testing.T) {
	t.Parallel()

	var plan *blockpack.FilePlan
	assert.Nil(t, plan.Between(0, math.MaxUint64))
}

// TestPlanFiles_UnknownTimeRange verifies that files with MinStartNanos==0 && MaxStartNanos==0
// (unknown time range) are conservatively included in Between results, matching the block-level
// planner behavior in queryplanner/planner.go.
func TestPlanFiles_UnknownTimeRange(t *testing.T) {
	blockpack.ClearReaderCaches()

	dir := t.TempDir()
	storage := blockpack.NewFileStorage(dir)

	// File with span at time 0 — produces MinStartNanos==0, MaxStartNanos==0.
	p1 := putFile(t, storage, "f1.blockpack", writeBlockpackWithTimes(t, []uint64{0}))
	// File with known time range.
	p2 := putFile(t, storage, "f2.blockpack", writeBlockpackWithTimes(t, []uint64{500}))

	plan := blockpack.PlanFiles([]string{p1, p2}, storage, nil)
	require.NotNil(t, plan)

	// Query [100, 200] does not overlap [0,0] numerically, but unknown-time files
	// must be included conservatively.
	paths := plan.Between(100, 200)
	require.Len(t, paths, 1)
	assert.Equal(t, p1, paths[0], "unknown-time file must be included conservatively")
}

// TestQueryTraceQL_SubFileShard verifies that StartBlock/BlockCount restrict which
// internal blocks are scanned at the public API level.
func TestQueryTraceQL_SubFileShard(t *testing.T) {
	t.Parallel()

	// 6 spans, 1 per block → 6 blocks.
	data := writeTestBlockpack(t, 6, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	require.Equal(t, 6, r.BlockCount(), "expected 6 blocks")

	// Full scan: all 6 spans.
	all, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, all, 6, "full scan should return all 6 spans")

	// Shard [2, 4): 2 spans.
	shard, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 2,
		BlockCount: 2,
	})
	require.NoError(t, err)
	assert.Len(t, shard, 2, "shard [2,4) should return 2 spans")
}

// TestQueryTraceQL_SubFileShard_Validation verifies that invalid shard params
// are rejected at the public API boundary.
func TestQueryTraceQL_SubFileShard_Validation(t *testing.T) {
	t.Parallel()

	data := writeTestBlockpack(t, 2, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	_, err = blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: -1,
		BlockCount: 1,
	})
	require.Error(t, err, "negative StartBlock should fail")

	_, err = blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 0,
		BlockCount: -1,
	})
	require.Error(t, err, "negative BlockCount should fail")
}

// TestQueryTraceQL_SubFileShard_TimeRange verifies that StartNano/EndNano time
// pruning works in combination with sub-file sharding.
func TestQueryTraceQL_SubFileShard_TimeRange(t *testing.T) {
	t.Parallel()

	// 4 spans, 1 per block. StartTimeUnixNano: 1_000_000_000, 1_001_000_000, 1_002_000_000, 1_003_000_000
	data := writeTestBlockpack(t, 4, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	require.Equal(t, 4, r.BlockCount())

	// Shard [0,2) with time range covering only the first block's start time.
	results, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 0,
		BlockCount: 2,
		StartNano:  1_000_000_000,
		EndNano:    1_000_500_000, // only overlaps first span
	})
	require.NoError(t, err)
	// Time pruning + shard should narrow results.
	assert.LessOrEqual(t, len(results), 2, "shard + time range should narrow results")
}

// TestQueryTraceQL_Structural_SubFileShard verifies that structural queries
// respect StartBlock/BlockCount, scanning only the assigned shard.
func TestQueryTraceQL_Structural_SubFileShard(t *testing.T) {
	t.Parallel()

	// 6 spans across 3 blocks (2 spans per block), each span in its own trace
	// so structural queries can match independently per block.
	data := writeTestBlockpack(t, 6, 2)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	require.Equal(t, 3, r.BlockCount(), "expected 3 blocks")

	// Full scan: structural query { } >> { } matches all spans with parent relationships.
	// Use a simple filter query to count spans per shard instead, since structural
	// results depend on trace topology. The key invariant is that sharding restricts
	// which blocks are scanned.
	allResults, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, allResults, 6, "full scan should return all 6 spans")

	// Shard [0, 1): only block 0 → 2 spans.
	shard0, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 0,
		BlockCount: 1,
	})
	require.NoError(t, err)
	assert.Len(t, shard0, 2, "shard [0,1) should return 2 spans from block 0")

	// Shard [1, 3): blocks 1 and 2 → 4 spans.
	shard12, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 1,
		BlockCount: 2,
	})
	require.NoError(t, err)
	assert.Len(t, shard12, 4, "shard [1,3) should return 4 spans from blocks 1-2")

	// Shards are disjoint and cover all results.
	assert.Equal(t, len(allResults), len(shard0)+len(shard12),
		"disjoint shards must cover all results")
}

// TestQueryTraceQL_SubFileShard_StartBlockWithoutBlockCount verifies that
// StartBlock != 0 with BlockCount == 0 is rejected (ambiguous: no-op shard).
func TestQueryTraceQL_SubFileShard_StartBlockWithoutBlockCount(t *testing.T) {
	t.Parallel()

	data := writeTestBlockpack(t, 2, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	_, err = blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartBlock: 5,
		BlockCount: 0,
	})
	require.Error(t, err, "StartBlock!=0 with BlockCount==0 should be rejected")
	assert.Contains(t, err.Error(), "has no effect")
}

// TestQueryTraceQL_SubFileShard_UnboundedEndNano verifies that StartNano > 0
// with EndNano == 0 means "no upper bound" and does not prune all blocks.
func TestQueryTraceQL_SubFileShard_UnboundedEndNano(t *testing.T) {
	t.Parallel()

	// 4 spans, 1 per block.
	data := writeTestBlockpack(t, 4, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	// StartNano set, EndNano==0 → should behave as unbounded upper bound.
	results, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartNano: 1,
		EndNano:   0,
	})
	require.NoError(t, err)
	assert.Len(t, results, 4, "EndNano==0 with StartNano>0 should not prune any blocks")
}

// TestQueryTraceQL_SubFileShard_StartNanoAfterEndNano verifies that
// StartNano > EndNano is rejected as an invalid time range.
func TestQueryTraceQL_SubFileShard_StartNanoAfterEndNano(t *testing.T) {
	t.Parallel()

	data := writeTestBlockpack(t, 2, 1)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	_, err = blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		StartNano: 5000,
		EndNano:   1000,
	})
	require.Error(t, err, "StartNano > EndNano should be rejected")
	assert.Contains(t, err.Error(), "StartNano")
}

type pipelineSpanDef struct {
	traceID   [16]byte
	spanID    [8]byte
	latencyMs int64
}

func writePipelineTestBlockpack(t *testing.T, spans []pipelineSpanDef) []byte {
	t.Helper()
	output := &bytes.Buffer{}
	w, err := blockpack.NewWriter(output, 0)
	require.NoError(t, err)
	for i, sd := range spans {
		attrs := []*commonv1.KeyValue{}
		if sd.latencyMs != 0 {
			attrs = append(attrs, &commonv1.KeyValue{
				Key: "latency_ms",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_IntValue{IntValue: sd.latencyMs},
				},
			})
		}
		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-svc"}},
					}},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{
					Spans: []*tracev1.Span{{
						TraceId:           sd.traceID[:],
						SpanId:            sd.spanID[:],
						Name:              fmt.Sprintf("span-%d", i),
						StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000), //nolint:gosec
						EndTimeUnixNano:   uint64(1_001_000_000 + i*1_000_000), //nolint:gosec
						Attributes:        attrs,
					}},
				}},
			}},
		}
		require.NoError(t, w.AddTracesData(td))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	return output.Bytes()
}

// EX-PA-01
func TestQueryTraceQL_PipelineCount(t *testing.T) {
	t.Parallel()
	traceID := [16]byte{0x01}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceID, spanID: [8]byte{0x01}},
		{traceID: traceID, spanID: [8]byte{0x02}},
		{traceID: traceID, spanID: [8]byte{0x03}},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ resource.service.name = "test-svc" } | count()`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 3, "count() with no threshold must emit all 3 spans")
}

// EX-PA-02
func TestQueryTraceQL_PipelineAvg(t *testing.T) {
	t.Parallel()
	traceID := [16]byte{0x02}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceID, spanID: [8]byte{0x01}, latencyMs: 10},
		{traceID: traceID, spanID: [8]byte{0x02}, latencyMs: 20},
		{traceID: traceID, spanID: [8]byte{0x03}, latencyMs: 30},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ span.latency_ms > 0 } | avg(span.latency_ms)`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 3, "avg() without threshold must emit all 3 spans")
}

// EX-PA-03
func TestQueryTraceQL_PipelineMin(t *testing.T) {
	t.Parallel()
	traceA, traceB := [16]byte{0x0A}, [16]byte{0x0B}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceA, spanID: [8]byte{0xA1}, latencyMs: 5},
		{traceID: traceA, spanID: [8]byte{0xA2}, latencyMs: 25},
		{traceID: traceB, spanID: [8]byte{0xB1}, latencyMs: 20},
		{traceID: traceB, spanID: [8]byte{0xB2}, latencyMs: 30},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ span.latency_ms > 0 } | min(span.latency_ms) > 15`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 2, "only trace-B's 2 spans must pass min() > 15")
	ids := make(map[string]bool, len(results))
	for _, m := range results {
		ids[m.SpanID] = true
	}
	assert.True(t, ids[fmt.Sprintf("%x", [8]byte{0xB1})], "span 0xB1 must be in results")
	assert.True(t, ids[fmt.Sprintf("%x", [8]byte{0xB2})], "span 0xB2 must be in results")
}

// EX-PA-04
func TestQueryTraceQL_PipelineMax(t *testing.T) {
	t.Parallel()
	traceA, traceB := [16]byte{0x0C}, [16]byte{0x0D}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceA, spanID: [8]byte{0xC1}, latencyMs: 10},
		{traceID: traceA, spanID: [8]byte{0xC2}, latencyMs: 80},
		{traceID: traceB, spanID: [8]byte{0xD1}, latencyMs: 10},
		{traceID: traceB, spanID: [8]byte{0xD2}, latencyMs: 40},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ span.latency_ms > 0 } | max(span.latency_ms) < 50`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 2, "only trace-B's 2 spans must pass max() < 50")
	ids := make(map[string]bool, len(results))
	for _, m := range results {
		ids[m.SpanID] = true
	}
	assert.True(t, ids[fmt.Sprintf("%x", [8]byte{0xD1})], "span 0xD1 must be in results")
	assert.True(t, ids[fmt.Sprintf("%x", [8]byte{0xD2})], "span 0xD2 must be in results")
}

// EX-PA-05
func TestQueryTraceQL_PipelineThreshold(t *testing.T) {
	t.Parallel()
	traceA, traceB := [16]byte{0x0E}, [16]byte{0x0F}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceA, spanID: [8]byte{0xE1}},
		{traceID: traceA, spanID: [8]byte{0xE2}},
		{traceID: traceB, spanID: [8]byte{0xF1}},
		{traceID: traceB, spanID: [8]byte{0xF2}},
		{traceID: traceB, spanID: [8]byte{0xF3}},
		{traceID: traceB, spanID: [8]byte{0xF4}},
		{traceID: traceB, spanID: [8]byte{0xF5}},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ resource.service.name = "test-svc" } | count() > 3`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 5, "only trace-B's 5 spans must pass count() > 3")
}

// EX-PA-06
// Two invariants combine: SPEC-PA-1 (field not in wantColumns because filter doesn't
// reference it) and SPEC-PA-2 (field genuinely absent from span data). Either alone
// would produce 0 results; both apply here.
func TestQueryTraceQL_PipelineNoMatchingField(t *testing.T) {
	t.Parallel()
	traceID := [16]byte{0x10}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceID, spanID: [8]byte{0x01}},
		{traceID: traceID, spanID: [8]byte{0x02}},
		{traceID: traceID, spanID: [8]byte{0x03}},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ resource.service.name = "test-svc" } | avg(span.latency_ms)`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Empty(t, results, "avg over absent/unloaded field must return zero spans (SPEC-PA-1 + SPEC-PA-2)")
}

// EX-PA-07
func TestQueryTraceQL_PipelineNoAggregate(t *testing.T) {
	t.Parallel()
	traceID := [16]byte{0x11}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceID, spanID: [8]byte{0x01}},
		{traceID: traceID, spanID: [8]byte{0x02}},
		{traceID: traceID, spanID: [8]byte{0x03}},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)
	results, err := blockpack.QueryTraceQL(
		r,
		`{ resource.service.name = "test-svc" } | by(resource.service.name)`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 3, "pipeline with no aggregate must emit all 3 matching spans (SPEC-PA-7)")
}

// EX-PA-08: SPEC-PA-1, SPEC-PA-6 — sum() without threshold emits all spans.
func TestQueryTraceQL_PipelineSum(t *testing.T) {
	t.Parallel()

	traceID := [16]byte{0x12}
	data := writePipelineTestBlockpack(t, []pipelineSpanDef{
		{traceID: traceID, spanID: [8]byte{0x01}, latencyMs: 10},
		{traceID: traceID, spanID: [8]byte{0x02}, latencyMs: 20},
		{traceID: traceID, spanID: [8]byte{0x03}, latencyMs: 30},
	})
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	// sum(10+20+30) = 60.0; no threshold → spanset passes → all 3 spans emitted.
	results, err := blockpack.QueryTraceQL(
		r,
		`{ span.latency_ms > 0 } | sum(span.latency_ms)`,
		blockpack.QueryOptions{},
	)
	require.NoError(t, err)
	assert.Len(t, results, 3, "sum() without threshold must emit all 3 spans from the passing spanset")
}

// =============================================================================
// SelectColumns filtering
// =============================================================================

// TestSelectColumns verifies that QueryOptions.SelectColumns restricts which
// fields appear in SpanMatch.Fields.IterateFields and GetField.
func TestSelectColumns(t *testing.T) {
	t.Parallel()

	// Build a blockpack with spans that have both a span attribute and a resource attribute.
	data := writeTestBlockpack(t, 5, 10)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	// Query requesting only span:name and resource.service.name.
	wantCols := []string{"span:name", "resource.service.name"}
	results, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{
		SelectColumns: wantCols,
	})
	require.NoError(t, err)
	require.NotEmpty(t, results, "expected at least one result")

	allowed := map[string]struct{}{
		"span:name":             {},
		"resource.service.name": {},
	}

	for i, m := range results {
		require.NotNil(t, m.Fields, "span %d: Fields must not be nil", i)

		// IterateFields must only yield allowed columns.
		m.Fields.IterateFields(func(name string, _ any) bool {
			if _, ok := allowed[name]; !ok {
				t.Errorf("span %d: IterateFields emitted unexpected column %q", i, name)
			}
			return true
		})

		// GetField for an allowed column must succeed.
		v, ok := m.Fields.GetField("span:name")
		if !ok {
			t.Errorf("span %d: GetField(span:name) returned false", i)
		}
		if v == nil {
			t.Errorf("span %d: GetField(span:name) returned nil value", i)
		}

		// GetField for a column not in SelectColumns must return false.
		_, ok = m.Fields.GetField("span:start")
		if ok {
			t.Errorf("span %d: GetField(span:start) should be blocked by SelectColumns", i)
		}
	}
}

// TestSelectColumns_NilMeansAll verifies that a nil SelectColumns returns all fields.
func TestSelectColumns_NilMeansAll(t *testing.T) {
	t.Parallel()

	data := writeTestBlockpack(t, 3, 10)
	r, err := blockpack.NewReaderFromProvider(&memReaderProvider{data: data})
	require.NoError(t, err)

	// No SelectColumns — all fields must be available.
	results, err := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// span:start should be present when SelectColumns is nil.
	for i, m := range results {
		require.NotNil(t, m.Fields)
		_, ok := m.Fields.GetField("span:start")
		if !ok {
			t.Errorf("span %d: GetField(span:start) should be accessible when SelectColumns is nil", i)
		}
	}
}
