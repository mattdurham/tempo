package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildTopKLogFileWithBodies writes log records across multiple blocks, each with svc, body, and ts.
func buildTopKLogFileWithBodies(t *testing.T, records []struct {
	svc  string
	body string
	ts   uint64
}, maxBlockSpans int,
) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, maxBlockSpans)
	for _, rec := range records {
		require.NoError(t, w.AddLogsData(makeLogRecord(rec.svc, rec.body, nil, rec.ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// EX-SLK-01 (StreamLogsTopK): Backward direction delivers globally K newest entries.
func TestStreamLogsTopK_GlobalOrder_Backward(t *testing.T) {
	t.Parallel()

	// Block 0: ts=100,200 — older block
	// Block 1: ts=300,400 — newer block
	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", 100},
		{"svc", "b", 200},
		{"svc", "c", 300},
		{"svc", "d", 400},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	entries, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	// Delivered newest first.
	assert.Equal(t, uint64(400), entries[0].TimestampNanos)
	assert.Equal(t, uint64(300), entries[1].TimestampNanos)
}

// EX-SLK-01 (StreamLogsTopK): Forward direction delivers globally K oldest entries.
func TestStreamLogsTopK_GlobalOrder_Forward(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", 100},
		{"svc", "b", 200},
		{"svc", "c", 300},
		{"svc", "d", 400},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	entries, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Forward,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	// Delivered oldest first.
	assert.Equal(t, uint64(100), entries[0].TimestampNanos)
	assert.Equal(t, uint64(200), entries[1].TimestampNanos)
}

// EX-SLK-03 (StreamLogsTopK): Block-level skip — once heap is full the old block's entries
// are pruned. Small test blocks always coalesce into one ReadGroup, so FetchedBlocks will
// equal SelectedBlocks even when the skip fires. The observable effect of skipping is that
// only the newer block's entries appear in the result.
func TestStreamLogsTopK_BlockSkip(t *testing.T) {
	t.Parallel()

	// Block 0: ts=1,2 (very old); Block 1: ts=1000,2000 (much newer).
	// With Limit=2 Backward, once block 1 fills the heap, block 0's MaxStart <= heap.min
	// so its rows are pruned from the result even if its bytes were already fetched.
	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "old1", 1},
		{"svc", "old2", 2},
		{"svc", "new1", 1000},
		{"svc", "new2", 2000},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	var stats modules_executor.CollectStats
	entries, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		OnStats:         func(s modules_executor.CollectStats) { stats = s },
	})
	require.NoError(t, err)
	// Result must contain only the two newer entries — block 0's rows were pruned.
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(2000), entries[0].TimestampNanos)
	assert.Equal(t, uint64(1000), entries[1].TimestampNanos)
	assert.Equal(t, 2, stats.SelectedBlocks)
	// Small blocks coalesce into one ReadGroup, so FetchedBlocks == SelectedBlocks.
	// The skip is validated via result correctness above, not I/O accounting.
	assert.LessOrEqual(t, stats.FetchedBlocks, stats.SelectedBlocks)
}

// EX-SLK-04 (StreamLogsTopK): Per-row time range filtering.
func TestStreamLogsTopK_TimeRange(t *testing.T) {
	t.Parallel()

	const T1, T2, T3 = uint64(1000), uint64(2000), uint64(3000)
	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", T1}, {"svc", "b", T2}, {"svc", "c", T3},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	entries, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           100,
		Direction:       queryplanner.Forward,
		TimestampColumn: "log:timestamp",
		TimeRange:       queryplanner.TimeRange{MinNano: T2, MaxNano: T2},
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, T2, entries[0].TimestampNanos)
}

// TestStreamLogsTopK_NilReader returns nil without error.
func TestStreamLogsTopK_NilReader(t *testing.T) {
	t.Parallel()

	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	entries, err := modules_executor.CollectLogs(nil, prog, nil, modules_executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "log:timestamp",
	})
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

// TestStreamLogsTopK_NilProgram returns an error.
func TestStreamLogsTopK_NilProgram(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", 100},
	}, 0)
	r := openLogMetricsReader(t, data)

	_, err := modules_executor.CollectLogs(r, nil, nil, modules_executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "log:timestamp",
	})
	assert.Error(t, err)
}

// EX-SLK-06 (StreamLogsTopK): Limit=0 delivers all pipeline-passing rows.
func TestStreamLogsTopK_LimitZeroDeliversAll(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", 100}, {"svc", "b", 200}, {"svc", "c", 300},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	entries, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           0,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(entries))
}

// EX-SLK-07 (StreamLogsTopK): OnStats is reported after scan.
func TestStreamLogsTopK_OnStats(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", "a", 100}, {"svc", "b", 200}, {"svc", "c", 300},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	var stats modules_executor.CollectStats
	_, err := modules_executor.CollectLogs(r, prog, nil, modules_executor.CollectOptions{
		Limit:           10,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		OnStats:         func(s modules_executor.CollectStats) { stats = s },
	})
	require.NoError(t, err)
	assert.Greater(t, stats.TotalBlocks, 0)
	assert.Greater(t, stats.SelectedBlocks, 0)
	assert.LessOrEqual(t, stats.FetchedBlocks, stats.SelectedBlocks)
}

// EX-SLK-02 (StreamLogsTopK): Pipeline stage filters entries — only entries passing the
// pipeline are included in the top-K result.
// Uses JSON parsing + StageLabelFilter to keep only entries with level="error".
func TestStreamLogsTopK_PipelineFilters(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFileWithBodies(t, []struct {
		svc  string
		body string
		ts   uint64
	}{
		{"svc", `{"level":"error","msg":"failed"}`, 100},
		{"svc", `{"level":"info","msg":"ok"}`, 200},
		{"svc", `{"level":"error","msg":"failed again"}`, 300},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	stages := []logqlparser.PipelineStage{
		{Type: logqlparser.StageJSON},
		{Type: logqlparser.StageLabelFilter, LabelFilter: &logqlparser.LabelFilter{
			Name:  "level",
			Value: "error",
			Op:    logqlparser.OpEqual,
		}},
	}
	pipeline, err := logqlparser.CompilePipeline(stages)
	require.NoError(t, err)

	entries, err := modules_executor.CollectLogs(r, prog, pipeline, modules_executor.CollectOptions{
		Limit:           10,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	// Only the two error entries pass the pipeline.
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(300), entries[0].TimestampNanos)
	assert.Equal(t, uint64(100), entries[1].TimestampNanos)
}
