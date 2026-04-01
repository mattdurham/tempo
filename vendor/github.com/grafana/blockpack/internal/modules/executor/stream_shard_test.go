package executor_test

// Tests for sub-file sharding (StartBlock/BlockCount) in Collect and CollectTopK.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildMultiBlockTraceFile creates a trace file with exactly blockCount blocks,
// each containing one span with service.name="svc". MaxBlockSpans=1 forces one
// span per block.
func buildMultiBlockTraceFile(t *testing.T, blockCount int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 1) // 1 span per block
	tid := [16]byte{0xAA}
	for i := range blockCount {
		addSpan(t, w, tid, i, "op", 0, nil, map[string]any{"service.name": "svc"})
	}
	mustFlush(t, w)
	return buf.Bytes()
}

// EX-SHARD-01: Collect with StartBlock/BlockCount returns only rows from the
// specified block range.
func TestCollect_SubFileShard_RestrictsBlocks(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 6)
	r := openReader(t, data)
	require.Equal(t, 6, r.BlockCount(), "expected 6 blocks")

	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	// Shard: blocks [2, 4) — should return exactly 2 rows.
	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: 2,
		BlockCount: 2,
	})

	require.NoError(t, err)
	assert.Len(t, rows, 2, "shard [2,4) should return exactly 2 rows")
	for _, row := range rows {
		assert.GreaterOrEqual(t, row.BlockIdx, 2)
		assert.Less(t, row.BlockIdx, 4)
	}
	planStep := findStep(qs, "plan")
	require.NotNil(t, planStep, "plan step must be present")
	assert.Equal(t, 6, planStep.Metadata["total_blocks"], "TotalBlocks reflects entire file")
}

// EX-SHARD-02: Collect with BlockCount=0 (no sharding) returns all blocks.
func TestCollect_SubFileShard_ZeroMeansAll(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 4)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: 0,
		BlockCount: 0,
	})

	require.NoError(t, err)
	assert.Len(t, rows, 4, "BlockCount=0 should scan all blocks")
}

// EX-SHARD-03: Collect with a shard range beyond available blocks returns empty.
func TestCollect_SubFileShard_BeyondRange(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 3)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: 10,
		BlockCount: 5,
	})

	require.NoError(t, err)
	assert.Empty(t, rows, "shard beyond block range should return empty")
}

// EX-SHARD-04: Collect with Limit + sharding stops early within the shard.
func TestCollect_SubFileShard_WithLimit(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 6)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	rows, _, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: 0,
		BlockCount: 4,
		Limit:      2,
	})

	require.NoError(t, err)
	assert.Len(t, rows, 2, "Limit=2 should cap results within shard")
}

// EX-SHARD-05: Collect rejects negative StartBlock.
func TestCollect_SubFileShard_NegativeStartBlock(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 2)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	_, _, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: -1,
		BlockCount: 1,
	})
	require.Error(t, err, "negative StartBlock should return error")
}

// EX-SHARD-06: Collect rejects negative BlockCount.
func TestCollect_SubFileShard_NegativeBlockCount(t *testing.T) {
	t.Parallel()

	data := buildMultiBlockTraceFile(t, 2)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "svc" }`)

	_, _, err := executor.Collect(r, program, executor.CollectOptions{
		StartBlock: 0,
		BlockCount: -1,
	})
	require.Error(t, err, "negative BlockCount should return error")
}

// EX-SHARD-07: Collect with top-K (Limit + TimestampColumn) and sharding restricts
// to the assigned block range and still delivers globally top-K entries within the shard.
// (Previously documented as CollectTopK; now handled by Collect directly.)
func TestCollectTopK_SubFileShard(t *testing.T) {
	t.Parallel()

	// Build log file: 4 blocks, 1 record each with increasing timestamps.
	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100},
		{"svc", 200},
		{"svc", 300},
		{"svc", 400},
	}, 1)

	r := openLogMetricsReader(t, data)
	require.Equal(t, 4, r.BlockCount())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	// Shard: blocks [1, 3) — ts=200, ts=300. Top-1 backward = ts=300.
	rows, _, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           1,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		StartBlock:      1,
		BlockCount:      2,
	})

	require.NoError(t, err)
	require.Len(t, rows, 1)
	ts := tsFromBlock(rows[0].Block, rows[0].RowIdx)
	assert.Equal(t, uint64(300), ts, "top-1 backward within shard [1,3) should be ts=300")
}

// EX-SHARD-08: CollectTopK rejects negative shard params.
func TestCollectTopK_SubFileShard_NegativeParams(t *testing.T) {
	t.Parallel()

	data := buildTopKLogFile(t, []struct {
		svc string
		ts  uint64
	}{
		{"svc", 100},
		{"svc", 200},
	}, 1)

	r := openLogMetricsReader(t, data)
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	_, _, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           1,
		Direction:       queryplanner.Backward,
		TimestampColumn: "log:timestamp",
		StartBlock:      -1,
		BlockCount:      1,
	})
	require.Error(t, err, "negative StartBlock should return error")
}
