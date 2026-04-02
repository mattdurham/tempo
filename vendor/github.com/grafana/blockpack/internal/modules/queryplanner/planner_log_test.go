package queryplanner_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildLogFileSingleBatch writes one log record per block (MaxBlockSpans=1).
// Each record has its TimeUnixNano set to the corresponding value in timeNanos.
// The writer sorts records by timestamp, so block order reflects timestamp order.
func buildLogFileSingleBatch(t *testing.T, timeNanos []uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 1,
	})
	require.NoError(t, err)

	records := make([]*logsv1.LogRecord, len(timeNanos))
	for i, ts := range timeNanos {
		records[i] = &logsv1.LogRecord{TimeUnixNano: ts}
	}

	ld := &logsv1.LogsData{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
		}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: records}},
	}}}
	require.NoError(t, w.AddLogsData(ld))
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// TP-01: TimeRange prunes blocks outside the time window.
func TestPlan_TimeRange_PrunesOutOfWindowBlocks(t *testing.T) {
	// 3 blocks: minStart=maxStart at 150, 550, 950 ns respectively.
	data := buildLogFileSingleBatch(t, []uint64{150, 550, 950})
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount(), "expected 3 blocks")

	p := queryplanner.NewPlanner(r)
	plan := p.Plan(nil, queryplanner.TimeRange{MinNano: 450, MaxNano: 650})

	// Only block 1 (timestamp 550) should survive.
	assert.Equal(t, []int{1}, plan.SelectedBlocks)
	assert.Equal(t, 2, plan.PrunedByTime, "blocks 0 and 2 must be pruned by time")
}

// TP-02: Zero TimeRange disables time-range pruning — all blocks selected.
func TestPlan_TimeRange_ZeroDisablesPruning(t *testing.T) {
	data := buildLogFileSingleBatch(t, []uint64{150, 550, 950})
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	plan := p.Plan(nil, queryplanner.TimeRange{})

	assert.Equal(t, 3, len(plan.SelectedBlocks))
	assert.Equal(t, 0, plan.PrunedByTime)
}

// TP-03: TimeRange beyond all blocks selects nothing.
func TestPlan_TimeRange_NoOverlap_Empty(t *testing.T) {
	data := buildLogFileSingleBatch(t, []uint64{150, 550, 950})
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	plan := p.Plan(nil, queryplanner.TimeRange{MinNano: 2000, MaxNano: 3000})

	assert.Empty(t, plan.SelectedBlocks)
	assert.Equal(t, 3, plan.PrunedByTime)
}

// TP-04: Zero MaxNano means open upper bound — blocks at or after MinNano survive.
func TestPlan_TimeRange_ZeroMaxNano_OpenUpperBound(t *testing.T) {
	data := buildLogFileSingleBatch(t, []uint64{150, 550, 950})
	r := openReader(t, data)
	require.Equal(t, 3, r.BlockCount())

	p := queryplanner.NewPlanner(r)
	// MinNano=450, MaxNano=0 (open upper bound): blocks 1 (550) and 2 (950) survive.
	plan := p.Plan(nil, queryplanner.TimeRange{MinNano: 450, MaxNano: 0})

	assert.Equal(t, []int{1, 2}, plan.SelectedBlocks)
	assert.Equal(t, 1, plan.PrunedByTime, "only block 0 (time=150) is before MinNano=450")
}
