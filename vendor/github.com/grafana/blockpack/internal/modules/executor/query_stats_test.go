package executor_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// EX-QS-01: QueryStats zero value is valid and safe to read.
func TestQueryStats_ZeroValue(t *testing.T) {
	var qs executor.QueryStats
	assert.Equal(t, "", qs.ExecutionPath)
	assert.Equal(t, time.Duration(0), qs.TotalDuration)
	assert.Nil(t, qs.Steps)
}

// EX-QS-02: StepStats Metadata map is nil-safe for missing keys.
func TestStepStats_NilMetadata(t *testing.T) {
	var s executor.StepStats
	v, ok := s.Metadata["total_blocks"]
	assert.False(t, ok)
	assert.Nil(t, v)
}

// EX-QS-04: CollectLogs returns QueryStats with correct execution path for limited queries.
func TestCollectLogs_ReturnsQueryStats(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewLogMetricsWriter(t, &buf, 100)
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "line 1", nil, 100)))
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "line 2", nil, 200)))
	require.NoError(t, w.AddLogsData(makeLogRecord("svc", "line 3", nil, 300)))
	_, err := w.Flush()
	require.NoError(t, err)

	r := openLogMetricsReader(t, buf.Bytes())
	prog := compileSelectorProgram(t, `{service.name = "svc"}`)

	_, qs, err := executor.CollectLogs(r, prog, nil, executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "log:timestamp",
	})
	require.NoError(t, err)
	assert.Equal(t, "block-topk", qs.ExecutionPath)
	assert.Greater(t, qs.TotalDuration, time.Duration(0))
}
