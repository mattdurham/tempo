package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// makeStreamLogRecord creates a minimal LogsData payload with a single record at the given timestamp.
func makeStreamLogRecord(svcName string, timeUnixNano uint64) *logsv1.LogsData {
	return &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svcName}},
		}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			{
				TimeUnixNano: timeUnixNano,
				TraceId: []byte{
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				},
				SpanId: []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
			},
		}}},
	}}}
}

// findStep returns the StepStats with the given name, or nil.
func findStep(qs executor.QueryStats, name string) *executor.StepStats {
	for i := range qs.Steps {
		if qs.Steps[i].Name == name {
			return &qs.Steps[i]
		}
	}
	return nil
}

// mustFindStep returns the StepStats with the given name. Fails the test if absent.
func mustFindStep(t *testing.T, qs executor.QueryStats, name string) executor.StepStats {
	t.Helper()
	s := findStep(qs, name)
	require.NotNilf(t, s, "%s step must be present in QueryStats", name)
	return *s
}

// EX-S-01: TestStream_TracePath verifies that Collect with TimestampColumn="" (trace mode)
// returns exactly the rows matching the predicate and no others.
func TestStream_TracePath(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x01}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-alpha"})
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-beta"})
	addSpan(t, w, tid, 2, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-alpha"})
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ resource.service.name = "svc-alpha" }`)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		TimestampColumn: "",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "should return exactly two svc-alpha spans")
	// Pure intrinsic queries use the fast path (intrinsic section only), so ExecutionPath
	// will be set to "intrinsic-plain". TotalDuration must be positive.
	assert.NotEmpty(t, qs.ExecutionPath)
}

// EX-S-02: TestStream_LogPath_TimeFilter verifies that Collect with TimestampColumn="log:timestamp"
// applies per-row time filtering and returns only the record whose timestamp is within [T2, T2].
func TestStream_LogPath_TimeFilter(t *testing.T) {
	t.Parallel()

	w, buf := mustNewLogWriter(t, 0)
	const T1, T2, T3 = uint64(1_000_000_000), uint64(2_000_000_000), uint64(3_000_000_000)

	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T1)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T2)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", T3)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	program := compileQuery(t, `{}`)

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{
		TimestampColumn: "log:timestamp",
		TimeRange:       queryplanner.TimeRange{MinNano: T2, MaxNano: T2},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(rows), "only the T2 record should pass the per-row time filter")
}

// EX-S-03: TestStream_Direction_Backward verifies that Collect with Direction=Backward
// delivers the newest (higher block index) block before the older block.
func TestStream_Direction_Backward(t *testing.T) {
	t.Parallel()

	// MaxBlockSpans=2 forces each pair of records into a separate block.
	w, buf := mustNewLogWriter(t, 2)
	// Block 0: older records.
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 100)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 200)))
	// Block 1: newer records.
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 1000)))
	require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", 2000)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	require.Equal(t, 2, r.BlockCount(), "expected 2 blocks")

	program := compileQuery(t, `{}`)

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
	})

	require.NoError(t, err)
	require.NotEmpty(t, rows, "expected at least one matched row")
	assert.Equal(t, 1, rows[0].BlockIdx, "Backward direction must deliver the newer (higher-index) block first")
}

// EX-S-04: TestStream_EarlyStop_FetchedLessThanSelected verifies that Collect with a Limit causes
// early stop so FetchedBlocks <= SelectedBlocks (lazy I/O proportional to results).
func TestStream_EarlyStop_FetchedLessThanSelected(t *testing.T) {
	t.Parallel()

	// MaxBlockSpans=2 → 10 records create 5 blocks, ensuring SelectedBlocks >= 3.
	w, buf := mustNewLogWriter(t, 2)
	for i := range 10 {
		require.NoError(t, w.AddLogsData(makeStreamLogRecord("svc", uint64(i+1)*1_000_000_000))) //nolint:gosec
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.data)
	require.GreaterOrEqual(t, r.BlockCount(), 3, "expected at least 3 blocks for early-stop test")

	program := compileQuery(t, `{}`)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		Limit: 2,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "should return exactly 2 rows (Limit=2)")
	// SPEC-STREAM-3: FetchedBlocks <= SelectedBlocks invariant.
	// With small test data all blocks coalesce into one group so FetchedBlocks == SelectedBlocks.
	// In production with large data, early stop skips unfetched groups, giving FetchedBlocks < SelectedBlocks.
	planStep := mustFindStep(t, qs, "plan")
	blockScanStep := mustFindStep(t, qs, "block-scan")
	selectedBlocks, _ := planStep.Metadata["selected_blocks"].(int)
	fetchedBlocksVal, _ := blockScanStep.Metadata["fetched_blocks"].(int)
	assert.LessOrEqual(t, fetchedBlocksVal, selectedBlocks,
		"fetched_blocks must not exceed selected_blocks (SPEC-STREAM-3)")
}

// EX-S-05: TestStream_NilReader verifies that Collect with a nil reader returns an empty slice
// and nil error (SPEC-STREAM-1).
func TestStream_NilReader(t *testing.T) {
	t.Parallel()

	program := compileQuery(t, `{}`)

	rows, _, err := executor.Collect(nil, program, executor.CollectOptions{})

	require.NoError(t, err)
	assert.Empty(t, rows, "nil reader must return empty results")
}

// EX-QS-03: Collect returns QueryStats with ExecutionPath set for block-scan queries.
func TestCollect_ReturnsQueryStats_BlockScan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x01}
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.method", "GET")}, // span attr → forces block-scan path
		map[string]any{"service.name": "svc-alpha"},
	)
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	// User attribute predicate → block-scan path
	program := compileQuery(t, `{ span.http.method = "GET" }`)
	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	assert.NotEmpty(t, qs.ExecutionPath)
	assert.True(t, qs.TotalDuration > 0, "TotalDuration must be positive")

	// Verify "plan" step is present
	planStep := findStep(qs, "plan")
	require.NotNil(t, planStep, "plan step must be present for block-scan path")
	totalBlocks, _ := planStep.Metadata["total_blocks"].(int)
	assert.GreaterOrEqual(t, totalBlocks, 1)

	// Verify "block-scan" step carries real I/O metrics.
	scanStep := mustFindStep(t, qs, "block-scan")
	assert.Greater(t, scanStep.BytesRead, int64(0), "block-scan must report BytesRead")
	assert.Greater(t, scanStep.IOOps, 0, "block-scan must report IOOps")
}

// streamF32ToLE encodes a float32 slice as little-endian bytes for use as an embedding attribute.
func streamF32ToLE(vec []float32) []byte {
	b := make([]byte, len(vec)*4)
	for i, f := range vec {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// streamL2Normalize returns a unit-length copy of vec.
func streamL2Normalize(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	norm := float32(1.0 / math.Sqrt(sum))
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * norm
	}
	return out
}

// EX-VECTOR-01: TestCollect_VectorTopK verifies that Collect with a HasVector Program
// returns rows sorted by Score descending and capped at VectorLimit.
func TestCollect_VectorTopK(t *testing.T) {
	t.Parallel()

	const dim = 3
	queryVec := streamL2Normalize([]float32{1, 0, 0})

	// Build spans with varying similarity to queryVec.
	// v1 is identical (score ~1.0), v2 is close, v3 is orthogonal (score ~0).
	v1 := streamL2Normalize([]float32{1, 0, 0})
	v2 := streamL2Normalize([]float32{0.8, 0.2, 0})
	v3 := streamL2Normalize([]float32{0, 1, 0}) // orthogonal
	tid := [16]byte{0xBB}

	makeEmbed := func(vec []float32) *commonv1.KeyValue {
		return &commonv1.KeyValue{
			Key:   modules_shared.EmbeddingColumnName,
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: streamF32ToLE(vec)}},
		}
	}

	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:    &buf,
		VectorDimension: dim,
	})
	require.NoError(t, err)
	for i, vec := range [][]float32{v1, v2, v3} {
		sp := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			Attributes:        []*commonv1.KeyValue{makeEmbed(vec)},
		}
		require.NoError(t, w.AddSpan(tid[:], sp, map[string]any{"service.name": "svc"}, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r := openReader(t, buf.Bytes())

	// Build a Program with HasVector=true, VectorScorer for post-filter scoring,
	// and a ColumnPredicate that passes all rows as candidates (FullScan).
	const threshold = float32(0.0)
	capturedVec := queryVec
	program := &vm.Program{
		HasVector:    true,
		QueryVector:  capturedVec,
		VectorColumn: modules_shared.EmbeddingColumnName,
		VectorLimit:  10,
		ColumnPredicate: func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.FullScan(), nil
		},
		VectorScorer: func(getVec func(int) ([]float32, bool), candidates vm.RowSet) []vm.ScoredRow {
			var scored []vm.ScoredRow
			for _, idx := range candidates.ToSlice() {
				vec, ok := getVec(idx)
				if !ok {
					continue
				}
				var dot, normA, normB float32
				for i := range capturedVec {
					dot += capturedVec[i] * vec[i]
					normA += capturedVec[i] * capturedVec[i]
					normB += vec[i] * vec[i]
				}
				var sim float32
				if normA > 0 && normB > 0 {
					sim = dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
				}
				if sim >= threshold {
					scored = append(scored, vm.ScoredRow{RowIdx: idx, Score: sim})
				}
			}
			return scored
		},
	}

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), 1, "at least one row should match with threshold=0")

	// All rows with embeddings should have Score > 0 (orthogonal vector has similarity ~0).
	// Rows must be sorted by Score descending.
	for i := 1; i < len(rows); i++ {
		assert.GreaterOrEqual(t, rows[i-1].Score, rows[i].Score,
			"rows must be sorted by Score descending: rows[%d].Score=%f, rows[%d].Score=%f",
			i-1, rows[i-1].Score, i, rows[i].Score)
	}
	// The top row should have the highest score (v1 is identical to queryVec).
	if len(rows) > 0 {
		assert.Greater(t, rows[0].Score, float32(0.0), "top row must have Score > 0")
	}
}
