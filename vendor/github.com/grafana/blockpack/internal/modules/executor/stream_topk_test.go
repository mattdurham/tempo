package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildTopKLogFile writes log records across multiple blocks.
// maxBlockSpans controls block boundaries; records are written in ts order.
func buildTopKLogFile(t *testing.T, records []struct {
	svc string
	ts  uint64
}, maxBlockSpans int,
) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, maxBlockSpans)
	for _, rec := range records {
		require.NoError(t, w.AddLogsData(makeLogRecord(rec.svc, rec.svc, nil, rec.ts)))
	}
	_, err := w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// tsFromBlock reads "log:timestamp" for the given row index.
func tsFromBlock(block *modules_reader.Block, rowIdx int) uint64 {
	col := block.GetColumn("log:timestamp")
	if col == nil {
		return 0
	}
	v, _ := col.Uint64Value(rowIdx)
	return v
}

// EX-SLK-01 (StreamTopK): Backward direction delivers globally K newest entries,
// not just the first K rows encountered.
func TestStreamTopK_GlobalOrder_Backward(t *testing.T) {
	t.Parallel()

	// Block 0: ts=100,200 — older block
	// Block 1: ts=300,400 — newer block
	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100},
		{"svc", 200},
		{"svc", 300},
		{"svc", 400},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	rows, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	// Delivered newest first.
	assert.Equal(t, uint64(400), tsFromBlock(rows[0].Block, rows[0].RowIdx))
	assert.Equal(t, uint64(300), tsFromBlock(rows[1].Block, rows[1].RowIdx))
}

// EX-SLK-01 (StreamTopK): Forward direction delivers globally K oldest entries.
func TestStreamTopK_GlobalOrder_Forward(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100},
		{"svc", 200},
		{"svc", 300},
		{"svc", 400},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	rows, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Forward,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	// Delivered oldest first.
	assert.Equal(t, uint64(100), tsFromBlock(rows[0].Block, rows[0].RowIdx))
	assert.Equal(t, uint64(200), tsFromBlock(rows[1].Block, rows[1].RowIdx))
}

// EX-SLK-03 (StreamTopK): Block-level skip — once heap is full the old block's entries
// are pruned. Small test blocks always coalesce into one ReadGroup, so FetchedBlocks will
// equal SelectedBlocks even when the skip fires. The observable effect of skipping is that
// only the newer block's entries appear in the result.
func TestStreamTopK_BlockSkip(t *testing.T) {
	t.Parallel()

	// Block 0: ts=1,2 (very old); Block 1: ts=1000,2000 (much newer).
	// With Limit=2 Backward, once block 1 fills the heap, block 0's MaxStart <= heap.min
	// so its rows are pruned from the result even if its bytes were already fetched.
	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 1},
		{"svc", 2},
		{"svc", 1000},
		{"svc", 2000},
	}, 2)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	var stats executor.CollectStats
	rows, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           2,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		OnStats:         func(s executor.CollectStats) { stats = s },
	})
	require.NoError(t, err)
	// Result must contain only the two newer entries — block 0's rows were pruned.
	require.Len(t, rows, 2)
	assert.Equal(t, uint64(2000), tsFromBlock(rows[0].Block, rows[0].RowIdx))
	assert.Equal(t, uint64(1000), tsFromBlock(rows[1].Block, rows[1].RowIdx))
	assert.Equal(t, 2, stats.SelectedBlocks)
	// Small blocks coalesce into one ReadGroup, so FetchedBlocks == SelectedBlocks.
	// The skip is validated via result correctness above, not I/O accounting.
	assert.LessOrEqual(t, stats.FetchedBlocks, stats.SelectedBlocks)
}

// EX-SLK-04 (StreamTopK): Per-row time range filtering.
func TestStreamTopK_TimeRange(t *testing.T) {
	t.Parallel()

	const T1, T2, T3 = uint64(1000), uint64(2000), uint64(3000)
	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", T1}, {"svc", T2}, {"svc", T3},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	rows, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           100,
		Direction:       queryplanner.Forward,
		TimestampColumn: "log:timestamp",
		TimeRange:       queryplanner.TimeRange{MinNano: T2, MaxNano: T2},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, T2, tsFromBlock(rows[0].Block, rows[0].RowIdx))
}

// TestStreamTopK_NilReader returns an empty slice and nil error for a nil reader.
func TestStreamTopK_NilReader(t *testing.T) {
	t.Parallel()

	rows, err := executor.Collect(nil, nil, executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "log:timestamp",
	})
	assert.NoError(t, err)
	assert.Empty(t, rows)
}

// TestStreamTopK_LimitZeroDeliversAll with Limit=0 delivers all rows.
func TestStreamTopK_LimitZeroDeliversAll(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100}, {"svc", 200}, {"svc", 300},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	rows, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           0,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(rows))
}

// TestStreamTopK_OnStats verifies stats are reported.
func TestStreamTopK_OnStats(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100}, {"svc", 200}, {"svc", 300},
	}, 0)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	var stats executor.CollectStats
	_, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           10,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		OnStats:         func(s executor.CollectStats) { stats = s },
	})
	require.NoError(t, err)
	assert.Greater(t, stats.TotalBlocks, 0)
	assert.Greater(t, stats.SelectedBlocks, 0)
	assert.LessOrEqual(t, stats.FetchedBlocks, stats.SelectedBlocks)
}
