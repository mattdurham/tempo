package executor

// NOTE-036: planBlocks unification tests.

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/modules/sketch"
	"github.com/grafana/blockpack/internal/vm"
)

// pbMemProvider is a minimal in-memory ReaderProvider for plan_blocks tests.
type pbMemProvider struct{ data []byte }

func (p *pbMemProvider) Size() (int64, error) { return int64(len(p.data)), nil }
func (p *pbMemProvider) ReadAt(b []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(p.data)) {
		return 0, bytes.ErrTooLarge
	}
	return copy(b, p.data[off:]), nil
}

// pbOpenReader creates a reader from a two-block file with spans for svc-a and svc-b.
func pbOpenReader(t *testing.T) *modules_reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	tid := [16]byte{0xAA}
	for i, svc := range []string{"svc-a", "svc-b", "svc-a", "svc-b"} {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
		}
		res := map[string]any{"service.name": svc}
		require.NoError(t, w.AddSpan(tid[:], span, res, svc, nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestPlanBlocks_NilProgram verifies that a nil program selects all blocks
// (no predicates = no pruning).
func TestPlanBlocks_NilProgram(t *testing.T) {
	r := pbOpenReader(t)
	plan := planBlocks(r, nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "nil program should select all blocks")
	assert.Equal(t, plan.TotalBlocks, len(plan.SelectedBlocks), "no pruning with nil program")
}

// TestPlanBlocks_MatchAllProgram verifies that a match-all program selects all blocks.
func TestPlanBlocks_MatchAllProgram(t *testing.T) {
	r := pbOpenReader(t)
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
	}
	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "match-all program should select blocks")
}

// TestPlanBlocks_TotalBlocksPopulated verifies that TotalBlocks is set on the plan.
func TestPlanBlocks_TotalBlocksPopulated(t *testing.T) {
	r := pbOpenReader(t)
	plan := planBlocks(r, nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Equal(t, r.BlockCount(), plan.TotalBlocks, "TotalBlocks must equal file block count")
}

// pbOpenReaderWithDuration writes a blockpack file with spans of varying durations
// so that span:duration is range-indexed and boundaries are meaningful.
func pbOpenReaderWithDuration(t *testing.T) *modules_reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)

	tid := [16]byte{0xDD}
	// Two blocks: first block spans with duration ~1000 ns, second ~100000 ns.
	durations := []struct{ start, end uint64 }{
		{1_000_000_000, 1_000_001_000}, // 1000 ns
		{1_000_000_000, 1_000_001_500}, // 1500 ns
		{1_000_000_000, 1_000_100_000}, // 100000 ns
		{1_000_000_000, 1_000_150_000}, // 150000 ns
	}
	for i, d := range durations {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: d.start,
			EndTimeUnixNano:   d.end,
		}
		require.NoError(t, w.AddSpan(tid[:], span, nil, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestPlanBlocks_FileLevelReject_OutsideRange verifies that planBlocks returns an
// empty SelectedBlocks when the query value is entirely above the file's max.
func TestPlanBlocks_FileLevelReject_OutsideRange(t *testing.T) {
	r := pbOpenReaderWithDuration(t)

	// Check that span:duration is range-indexed; if not, skip.
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed — skip file-level reject test")
	}

	// Query: span:duration > 999_999_999_000 (1000 seconds, way above any span duration).
	// This should trigger fileLevelReject and return empty SelectedBlocks.
	huge := int64(999_999_999_000)
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "span:duration",
					Min:    &vm.Value{Type: vm.TypeDuration, Data: huge},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "file-level reject should produce empty SelectedBlocks")
}

// TestPlanBlocks_FileLevelReject_InRange verifies that planBlocks does NOT reject
// when the query range overlaps the file's span:duration range.
func TestPlanBlocks_FileLevelReject_InRange(t *testing.T) {
	r := pbOpenReaderWithDuration(t)

	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed — skip file-level reject test")
	}

	// Query: span:duration > 500 ns (well within the file's range).
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "span:duration",
					Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "in-range query should select blocks")
}

// --- rejectByBoundary unit tests ---

// --- rejectInt64Range tests ---

func TestRejectInt64Range_MinOnly_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(100)},
	}
	// fileMax = 50, queryMin = 100 → queryMin > fileMax → reject
	assert.True(t, rejectInt64Range(0, 50, node))
}

func TestRejectInt64Range_MinOnly_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(30)},
	}
	// fileMax = 50, queryMin = 30 → 30 <= 50 → no reject
	assert.False(t, rejectInt64Range(0, 50, node))
}

func TestRejectInt64Range_MinOnly_BoundaryEqual(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(50)},
	}
	// queryMin == fileMax → no reject (boundary inclusive)
	assert.False(t, rejectInt64Range(0, 50, node))
}

func TestRejectInt64Range_MaxOnly_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(-10)},
	}
	// fileMin = 0, queryMax = -10 → queryMax < fileMin → reject
	assert.True(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_MaxOnly_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(5)},
	}
	// fileMin = 0, queryMax = 5 → 5 >= 0 → no reject
	assert.False(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_MaxOnly_BoundaryEqual(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(0)},
	}
	// queryMax == fileMin → no reject (boundary inclusive)
	assert.False(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_BothMinMax_Reject_Above(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(200)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(300)},
	}
	// file [0,100], query [200,300] → entirely above → reject
	assert.True(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_BothMinMax_Reject_Below(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(-100)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(-50)},
	}
	// file [0,100], query [-100,-50] → entirely below → reject
	assert.True(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_BothMinMax_NoReject_Overlap(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(50)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(150)},
	}
	// file [0,100], query [50,150] → overlaps → no reject
	assert.False(t, rejectInt64Range(0, 100, node))
}

func TestRejectInt64Range_NilMinMax_NoReject(t *testing.T) {
	// Both nil → no reject (equality predicate handled elsewhere)
	assert.False(t, rejectInt64Range(0, 100, &vm.RangeNode{}))
}

func TestRejectInt64Range_Duration_MinOnly_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeDuration, Data: int64(1_000_000_000)}, // 1s
	}
	// fileMax = 500_000 ns, queryMin = 1s → reject
	assert.True(t, rejectInt64Range(0, 500_000, node))
}

func TestRejectInt64Range_NegativeValues(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(-5)},
	}
	// file [-100, -10], queryMin = -5 → -5 > -10 → reject
	assert.True(t, rejectInt64Range(-100, -10, node))
}

func TestRejectInt64Range_NegativeValues_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(-50)},
	}
	// file [-100, -10], queryMin = -50 → -50 <= -10 → no reject
	assert.False(t, rejectInt64Range(-100, -10, node))
}

func TestRejectInt64Range_WrongType_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(100)},
	}
	// Wrong type → ptrValueToInt64 returns false → no reject
	assert.False(t, rejectInt64Range(0, 50, node))
}

// --- rejectUint64Range tests ---

func TestRejectUint64Range_MinOnly_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(200)},
	}
	// fileMax = 100, queryMin = 200 → reject
	assert.True(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_MinOnly_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(50)},
	}
	assert.False(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_MinOnly_BoundaryEqual(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(100)},
	}
	// queryMin == fileMax → no reject
	assert.False(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_MaxOnly_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(5)},
	}
	// fileMin = 10, queryMax = 5 → queryMax < fileMin → reject
	assert.True(t, rejectUint64Range(10, 100, node))
}

func TestRejectUint64Range_MaxOnly_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(50)},
	}
	assert.False(t, rejectUint64Range(10, 100, node))
}

func TestRejectUint64Range_MaxOnly_BoundaryEqual(t *testing.T) {
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(10)},
	}
	// queryMax == fileMin → no reject
	assert.False(t, rejectUint64Range(10, 100, node))
}

func TestRejectUint64Range_BothMinMax_Reject_Above(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(500)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(1000)},
	}
	assert.True(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_BothMinMax_Reject_Below(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(0)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(5)},
	}
	assert.True(t, rejectUint64Range(10, 100, node))
}

func TestRejectUint64Range_BothMinMax_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(50)},
		Max: &vm.Value{Type: vm.TypeInt, Data: int64(150)},
	}
	assert.False(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_NegativeMin_NoReject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(-1)},
	}
	// Negative value → ptrValueToUint64 returns false → no reject
	assert.False(t, rejectUint64Range(0, 100, node))
}

func TestRejectUint64Range_Duration_Reject(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeDuration, Data: int64(1_000_000_000)}, // 1s in ns
	}
	// fileMax = 100_000 ns (100µs), queryMin = 1s → reject
	assert.True(t, rejectUint64Range(0, 100_000, node))
}

func TestRejectUint64Range_NilMinMax_NoReject(t *testing.T) {
	assert.False(t, rejectUint64Range(0, 100, &vm.RangeNode{}))
}

func TestRejectUint64Range_MaxUint64(t *testing.T) {
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(1)},
	}
	// fileMax = math.MaxInt64 (cast to uint64) → queryMin = 1 → no reject
	assert.False(t, rejectUint64Range(0, math.MaxUint64, node))
}

// --- rejectFloat64Range tests ---

func makeFloat64Bounds(fMin, fMax float64, colType modules_shared.ColumnType) *modules_reader.RangeBoundaries {
	return &modules_reader.RangeBoundaries{
		ColType:   colType,
		BucketMin: int64(math.Float64bits(fMin)), //nolint:gosec
		BucketMax: int64(math.Float64bits(fMax)), //nolint:gosec
	}
}

func TestRejectFloat64Range_MinOnly_Reject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(2.0)},
	}
	// queryMin=2.0 > fileMax=1.0 → reject
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_MinOnly_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(0.5)},
	}
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_MinOnly_BoundaryEqual(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(1.0)},
	}
	// queryMin == fileMax → no reject
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_MaxOnly_Reject(t *testing.T) {
	bounds := makeFloat64Bounds(5.0, 10.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(3.0)},
	}
	// queryMax=3.0 < fileMin=5.0 → reject
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_MaxOnly_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(5.0, 10.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(7.0)},
	}
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_MaxOnly_BoundaryEqual(t *testing.T) {
	bounds := makeFloat64Bounds(5.0, 10.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(5.0)},
	}
	// queryMax == fileMin → no reject
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_BothMinMax_Reject_Above(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(2.0)},
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(3.0)},
	}
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_BothMinMax_Reject_Below(t *testing.T) {
	bounds := makeFloat64Bounds(5.0, 10.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(1.0)},
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(3.0)},
	}
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_BothMinMax_NoReject_Overlap(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 10.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(5.0)},
		Max: &vm.Value{Type: vm.TypeFloat, Data: float64(15.0)},
	}
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_NilMinMax_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	assert.False(t, rejectFloat64Range(bounds, &vm.RangeNode{}))
}

func TestRejectFloat64Range_NegativeValues(t *testing.T) {
	bounds := makeFloat64Bounds(-10.0, -1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(0.0)},
	}
	// queryMin=0.0 > fileMax=-1.0 → reject
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_NaN_FileMin_NoReject(t *testing.T) {
	// NaN in file bounds → conservative, no reject
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnTypeRangeFloat64,
		BucketMin: int64(math.Float64bits(math.NaN())), //nolint:gosec
		BucketMax: int64(math.Float64bits(10.0)),       //nolint:gosec
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(100.0)},
	}
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_NaN_FileMax_NoReject(t *testing.T) {
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnTypeRangeFloat64,
		BucketMin: int64(math.Float64bits(0.0)),        //nolint:gosec
		BucketMax: int64(math.Float64bits(math.NaN())), //nolint:gosec
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(100.0)},
	}
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_NaN_QueryMin_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: math.NaN()},
	}
	// NaN query value → conservative, no reject
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_Inf_Reject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: math.Inf(1)},
	}
	// +Inf > fileMax → reject
	assert.True(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_NegInf_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: math.Inf(-1)},
	}
	// -Inf < fileMax → no reject
	assert.False(t, rejectFloat64Range(bounds, node))
}

func TestRejectFloat64Range_WrongType_NoReject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(100)},
	}
	// ptrValueToFloat64 returns false for TypeInt → no reject
	assert.False(t, rejectFloat64Range(bounds, node))
}

// --- rangeRejectsFile: string/bytes skip ---

func TestRangeRejectsFile_StringColumn_NoReject(t *testing.T) {
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnTypeRangeString,
		BucketMin: 0,
		BucketMax: 0,
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeString, Data: "a"},
	}
	// String columns always return false (skip)
	assert.False(t, rangeRejectsFile(bounds, node))
}

func TestRangeRejectsFile_BytesColumn_NoReject(t *testing.T) {
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnTypeRangeBytes,
		BucketMin: 0,
		BucketMax: 0,
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeString, Data: "a"},
	}
	assert.False(t, rangeRejectsFile(bounds, node))
}

// --- rejectByBoundary composite (AND/OR) node tests ---
// We test these indirectly via fileLevelReject which takes a real reader.
// For composite logic we use planBlocks with multiple RangeNodes.

func TestFileLevelReject_EmptyNodes_NoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	// Empty nodes list → no reject
	assert.False(t, fileLevelReject(r, nil))
	assert.False(t, fileLevelReject(r, []vm.RangeNode{}))
}

func TestFileLevelReject_Leaf_NoColumn_NoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	node := vm.RangeNode{
		// No column set
		Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)},
	}
	assert.False(t, fileLevelReject(r, []vm.RangeNode{node}))
}

func TestFileLevelReject_Leaf_NoMinMax_NoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	node := vm.RangeNode{
		Column: "span:duration",
		// No Min or Max set → equality predicate, defer to block-level
	}
	assert.False(t, fileLevelReject(r, []vm.RangeNode{node}))
}

func TestFileLevelReject_Leaf_UnknownColumn_NoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	node := vm.RangeNode{
		Column: "nonexistent.column",
		Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)},
	}
	// Column has no range index → nil bounds → no reject
	assert.False(t, fileLevelReject(r, []vm.RangeNode{node}))
}

func TestFileLevelReject_AND_OneChildRejects(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// AND of two nodes: first doesn't reject, second does → AND rejects
	nodes := []vm.RangeNode{
		{
			Column: "span:duration",
			Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)}, // in range → no reject
		},
		{
			Column: "span:duration",
			Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}, // above max → reject
		},
	}
	assert.True(t, fileLevelReject(r, nodes))
}

func TestFileLevelReject_AND_NoChildRejects(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	nodes := []vm.RangeNode{
		{
			Column: "span:duration",
			Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)}, // in range
		},
		{
			Column: "span:duration",
			Max:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999)}, // in range
		},
	}
	assert.False(t, fileLevelReject(r, nodes))
}

func TestRejectByBoundary_ORNode_AllChildrenReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// OR: both children reject → OR rejects
	orNode := vm.RangeNode{
		IsOR: true,
		Children: []vm.RangeNode{
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)},
			},
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_001)},
			},
		},
	}
	assert.True(t, rejectByBoundary(r, &orNode))
}

func TestRejectByBoundary_ORNode_OneChildNoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// OR: one child in range → OR does NOT reject
	orNode := vm.RangeNode{
		IsOR: true,
		Children: []vm.RangeNode{
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}, // rejects
			},
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)}, // in range, no reject
			},
		},
	}
	assert.False(t, rejectByBoundary(r, &orNode))
}

func TestRejectByBoundary_ANDComposite_OneChildRejects(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// AND composite (IsOR=false): one child rejects → AND rejects
	andNode := vm.RangeNode{
		IsOR: false,
		Children: []vm.RangeNode{
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)}, // no reject
			},
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}, // rejects
			},
		},
	}
	assert.True(t, rejectByBoundary(r, &andNode))
}

func TestRejectByBoundary_ANDComposite_NoChildRejects(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	andNode := vm.RangeNode{
		IsOR: false,
		Children: []vm.RangeNode{
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)},
			},
			{
				Column: "span:duration",
				Max:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999)},
			},
		},
	}
	assert.False(t, rejectByBoundary(r, &andNode))
}

func TestRejectByBoundary_NestedANDInsideOR(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// OR( AND(reject, reject), AND(no-reject, no-reject) )
	// → OR(reject, no-reject) → no reject (one OR child doesn't reject)
	orNode := vm.RangeNode{
		IsOR: true,
		Children: []vm.RangeNode{
			{
				IsOR: false,
				Children: []vm.RangeNode{
					{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}},
					{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_001)}},
				},
			},
			{
				IsOR: false,
				Children: []vm.RangeNode{
					{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(500)}},
					{Column: "span:duration", Max: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999)}},
				},
			},
		},
	}
	assert.False(t, rejectByBoundary(r, &orNode))
}

func TestRejectByBoundary_NestedORAllReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}
	// AND with an OR child where all OR children reject
	andNode := vm.RangeNode{
		IsOR: false,
		Children: []vm.RangeNode{
			{
				Column: "span:duration",
				Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)}, // no reject
			},
			{
				IsOR: true,
				Children: []vm.RangeNode{
					{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}},
					{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_001)}},
				},
			},
		},
	}
	assert.True(t, rejectByBoundary(r, &andNode))
}

// --- ptrValueToInt64 tests ---

func TestPtrValueToInt64_NilPointer(t *testing.T) {
	_, ok := ptrValueToInt64(nil)
	assert.False(t, ok)
}

func TestPtrValueToInt64_TypeInt(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: int64(42)}
	n, ok := ptrValueToInt64(&v)
	require.True(t, ok)
	assert.Equal(t, int64(42), n)
}

func TestPtrValueToInt64_TypeDuration(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: int64(1_000_000)}
	n, ok := ptrValueToInt64(&v)
	require.True(t, ok)
	assert.Equal(t, int64(1_000_000), n)
}

func TestPtrValueToInt64_WrongType(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: float64(1.0)}
	_, ok := ptrValueToInt64(&v)
	assert.False(t, ok)
}

func TestPtrValueToInt64_WrongDataType(t *testing.T) {
	// Data is wrong Go type for TypeInt
	v := vm.Value{Type: vm.TypeInt, Data: "not-an-int64"}
	_, ok := ptrValueToInt64(&v)
	assert.False(t, ok)
}

// --- ptrValueToUint64 tests ---

func TestPtrValueToUint64_NilPointer(t *testing.T) {
	_, ok := ptrValueToUint64(nil)
	assert.False(t, ok)
}

func TestPtrValueToUint64_TypeInt_Positive(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: int64(100)}
	n, ok := ptrValueToUint64(&v)
	require.True(t, ok)
	assert.Equal(t, uint64(100), n)
}

func TestPtrValueToUint64_TypeInt_Zero(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: int64(0)}
	n, ok := ptrValueToUint64(&v)
	require.True(t, ok)
	assert.Equal(t, uint64(0), n)
}

func TestPtrValueToUint64_TypeInt_Negative(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: int64(-1)}
	_, ok := ptrValueToUint64(&v)
	assert.False(t, ok, "negative int should not convert to uint64")
}

func TestPtrValueToUint64_TypeDuration_Positive(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: int64(5_000_000)}
	n, ok := ptrValueToUint64(&v)
	require.True(t, ok)
	assert.Equal(t, uint64(5_000_000), n)
}

func TestPtrValueToUint64_TypeDuration_Negative(t *testing.T) {
	v := vm.Value{Type: vm.TypeDuration, Data: int64(-1)}
	_, ok := ptrValueToUint64(&v)
	assert.False(t, ok)
}

func TestPtrValueToUint64_WrongType(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: float64(1.0)}
	_, ok := ptrValueToUint64(&v)
	assert.False(t, ok)
}

func TestPtrValueToUint64_WrongDataType(t *testing.T) {
	v := vm.Value{Type: vm.TypeInt, Data: "not-int64"}
	_, ok := ptrValueToUint64(&v)
	assert.False(t, ok)
}

// --- ptrValueToFloat64 tests ---

func TestPtrValueToFloat64_NilPointer(t *testing.T) {
	_, ok := ptrValueToFloat64(nil)
	assert.False(t, ok)
}

func TestPtrValueToFloat64_Float64(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: float64(3.14)}
	f, ok := ptrValueToFloat64(&v)
	require.True(t, ok)
	assert.InDelta(t, 3.14, f, 1e-9)
}

func TestPtrValueToFloat64_WrongDataType(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: int64(42)}
	_, ok := ptrValueToFloat64(&v)
	assert.False(t, ok)
}

func TestPtrValueToFloat64_NaN(t *testing.T) {
	v := vm.Value{Type: vm.TypeFloat, Data: math.NaN()}
	f, ok := ptrValueToFloat64(&v)
	require.True(t, ok)
	assert.True(t, math.IsNaN(f))
}

// --- planBlocks integration: program with no Predicates field ---

func TestPlanBlocks_ProgramWithNilPredicates_NoFileLevelReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	// Program has ColumnPredicate but Predicates==nil → fileLevelReject not called
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: nil,
	}
	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0)
}

func TestPlanBlocks_ProgramWithEmptyPredicateNodes_NoFileLevelReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	// Predicates set but Nodes is empty → no file-level reject
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{Nodes: nil},
	}
	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0)
}

// --- planBlocks with time range pruning ---

func TestPlanBlocks_TimeRange_ExcludesAll(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	// Time range far in the future — excludes all blocks
	tr := queryplanner.TimeRange{
		MinNano: 9_000_000_000_000_000_000,
		MaxNano: 9_999_999_999_000_000_000,
	}
	plan := planBlocks(r, nil, tr, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "time range in the future should select no blocks")
}

func TestPlanBlocks_TimeRange_IncludesAll(t *testing.T) {
	r := pbOpenReaderWithDuration(t)
	// Zero time range → no time pruning
	plan := planBlocks(r, nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Equal(t, plan.TotalBlocks, len(plan.SelectedBlocks))
}

// --- planBlocks edge cases ---

func TestPlanBlocks_SingleBlock(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 100,
	})
	require.NoError(t, err)
	tid := [16]byte{0x01}
	span := &tracev1.Span{
		TraceId:           tid[:],
		SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Name:              "op",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   1_000_002_000,
	}
	require.NoError(t, w.AddSpan(tid[:], span, nil, "", nil, ""))
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)

	plan := planBlocks(r, nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Equal(t, 1, plan.TotalBlocks)
	assert.Equal(t, 1, len(plan.SelectedBlocks))
}

func TestPlanBlocks_ManyBlocks(t *testing.T) {
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 1, // 1 span per block → many blocks
	})
	require.NoError(t, err)
	tid := [16]byte{0x02}
	for i := range 20 {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
		}
		require.NoError(t, w.AddSpan(tid[:], span, nil, "", nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)

	plan := planBlocks(r, nil, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Equal(t, 20, plan.TotalBlocks)
	assert.Equal(t, 20, len(plan.SelectedBlocks))
}

// TestPlanBlocks_FileLevelReject_BelowMin verifies rejection when query max is below file min.
func TestPlanBlocks_FileLevelReject_BelowMin(t *testing.T) {
	r := pbOpenReaderWithDuration(t)

	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}

	// Query: span:duration < 1 ns (below any span duration in the file).
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "span:duration",
					Max:    &vm.Value{Type: vm.TypeDuration, Data: int64(1)},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "below-min query should be rejected")
}

// TestPlanBlocks_FileLevelReject_ORAllReject verifies that an OR predicate node
// where all children reject causes the file to be rejected.
func TestPlanBlocks_FileLevelReject_ORAllReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)

	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}

	// Both OR children have duration way above file max → all reject → OR rejects → file reject
	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)}},
						{Column: "span:duration", Min: &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_001)}},
					},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks)
}

// TestRangeRejectsFile_Float64_Reject exercises the ColumnTypeRangeFloat64 branch in
// rangeRejectsFile (ensures dispatch line is covered).
func TestRangeRejectsFile_Float64_Reject(t *testing.T) {
	bounds := makeFloat64Bounds(0.0, 1.0, modules_shared.ColumnTypeRangeFloat64)
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeFloat, Data: float64(5.0)},
	}
	assert.True(t, rangeRejectsFile(bounds, node))
}

// TestRangeRejectsFile_UnknownColumnType_NoReject verifies that an unrecognized column
// type in RangeBoundaries never triggers a false reject.
func TestRangeRejectsFile_UnknownColumnType_NoReject(t *testing.T) {
	// Use a ColType that doesn't match any known case.
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnType(255),
		BucketMin: 0,
		BucketMax: 100,
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeInt, Data: int64(999_999)},
	}
	assert.False(t, rangeRejectsFile(bounds, node))
}

// TestRangeRejectsFile_ColumnTypeRangeDuration_Reject exercises the
// ColumnTypeRangeDuration branch in rangeRejectsFile (same path as Int64).
func TestRangeRejectsFile_ColumnTypeRangeDuration_Reject(t *testing.T) {
	bounds := &modules_reader.RangeBoundaries{
		ColType:   modules_shared.ColumnTypeRangeDuration,
		BucketMin: 0,
		BucketMax: int64(500_000), // 500µs
	}
	node := &vm.RangeNode{
		Min: &vm.Value{Type: vm.TypeDuration, Data: int64(1_000_000_000)}, // 1s → above max
	}
	assert.True(t, rangeRejectsFile(bounds, node))
}

// TestPlanBlocks_FileLevelReject_OROneNoReject verifies that an OR with one in-range
// child is NOT rejected.
func TestPlanBlocks_FileLevelReject_OROneNoReject(t *testing.T) {
	r := pbOpenReaderWithDuration(t)

	if b := r.RangeColumnBoundaries("span:duration"); b == nil {
		t.Skip("span:duration not range-indexed")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					IsOR: true,
					Children: []vm.RangeNode{
						{
							Column: "span:duration",
							Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(999_999_999_000)},
						}, // rejects
						{
							Column: "span:duration",
							Min:    &vm.Value{Type: vm.TypeDuration, Data: int64(500)},
						}, // in range
					},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0)
}

// pbOpenReaderServiceNames writes a file with distinct service names in separate blocks.
// MaxBlockSpans=1 ensures each span goes into its own block.
func pbOpenReaderServiceNames(t *testing.T) *modules_reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 1,
	})
	require.NoError(t, err)

	tid := [16]byte{0xBB}
	services := []string{"svc-a", "svc-b", "svc-a", "svc-b"}
	for i, svc := range services {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "checkout",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
		}
		res := map[string]any{"service.name": svc}
		require.NoError(t, w.AddSpan(tid[:], span, res, svc, nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)

	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestPlanBlocks_IntrinsicTOC_Prunes verifies that planBlocks intersects with
// BlocksFromIntrinsicTOC when an intrinsic column predicate prunes some blocks.
func TestPlanBlocks_IntrinsicTOC_Prunes(t *testing.T) {
	r := pbOpenReaderServiceNames(t)

	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section — skip intrinsic TOC test")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.service.name",
					Values: []vm.Value{{Type: vm.TypeString, Data: "svc-a"}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Less(t, len(plan.SelectedBlocks), plan.TotalBlocks, "intrinsic TOC should prune svc-b blocks")
}

// TestPlanBlocks_IntrinsicTOC_NoMatch verifies that when the intrinsic predicate
// matches no blocks, SelectedBlocks is empty.
func TestPlanBlocks_IntrinsicTOC_NoMatch(t *testing.T) {
	r := pbOpenReaderServiceNames(t)

	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section — skip intrinsic TOC test")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.service.name",
					Values: []vm.Value{{Type: vm.TypeString, Data: "svc-z"}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "no blocks should match svc-z")
}

// TestPlanBlocks_IntrinsicTOC_NonIntrinsicPredicate verifies that a program with
// only non-intrinsic column predicates does not trigger intrinsic TOC pruning.
func TestPlanBlocks_IntrinsicTOC_NonIntrinsicPredicate(t *testing.T) {
	r := pbOpenReaderServiceNames(t)

	if !r.HasIntrinsicSection() {
		t.Skip("file has no intrinsic section — skip intrinsic TOC test")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{Column: "span.http.method", Values: []vm.Value{{Type: vm.TypeString, Data: "GET"}}},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Equal(t, plan.TotalBlocks, len(plan.SelectedBlocks))
}

// --- fileLevelBloomReject tests ---

// pbOpenReaderSingleService writes a file with spans for one service name only.
func pbOpenReaderSingleService(t *testing.T, svcName string) *modules_reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)
	tid := [16]byte{0xCC}
	for i := range 4 {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(1_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_002_000 + i*1000), //nolint:gosec
		}
		res := map[string]any{"service.name": svcName}
		require.NoError(t, w.AddSpan(tid[:], span, res, svcName, nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestFileLevelBloomReject_AbsentServiceName verifies that a query for a service name
// not in the file is rejected via the FileBloom section.
func TestFileLevelBloomReject_AbsentServiceName(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-present")
	if r.FileBloom() == nil {
		t.Skip("file has no FileBloom section")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.service.name",
					Values: []vm.Value{{Type: vm.TypeString, Data: "svc-absent"}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "absent service name should reject entire file")
}

// TestFileLevelBloomReject_PresentServiceName verifies that a query for a service name
// that IS in the file is NOT rejected.
func TestFileLevelBloomReject_PresentServiceName(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-present")
	if r.FileBloom() == nil {
		t.Skip("file has no FileBloom section")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.service.name",
					Values: []vm.Value{{Type: vm.TypeString, Data: "svc-present"}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "present service name should not reject file")
}

// TestFileLevelBloomReject_TraceID_Absent verifies that a trace:id not in the file is rejected.
func TestFileLevelBloomReject_TraceID_Absent(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-x")
	// Trace ID 0xCC (written above) is in the file; 0xFF is not.
	absentID := [16]byte{0xFF}
	if r.MayContainTraceID(absentID) {
		t.Skip("compact bloom not available or false-positive — cannot test rejection")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "trace:id",
					Values: []vm.Value{{Type: vm.TypeBytes, Data: absentID[:]}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "absent trace:id should reject entire file")
}

// TestFileLevelBloomReject_OR_AllAbsent verifies that an OR node where all children
// have absent service names causes file rejection.
func TestFileLevelBloomReject_OR_AllAbsent(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-present")
	if r.FileBloom() == nil {
		t.Skip("file has no FileBloom section")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: "resource.service.name", Values: []vm.Value{{Type: vm.TypeString, Data: "absent-1"}}},
						{Column: "resource.service.name", Values: []vm.Value{{Type: vm.TypeString, Data: "absent-2"}}},
					},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "OR of all-absent service names should reject file")
}

// TestFileLevelBloomReject_OR_OnePresent verifies that an OR node where one child
// has a present service name does NOT reject the file.
func TestFileLevelBloomReject_OR_OnePresent(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-present")
	if r.FileBloom() == nil {
		t.Skip("file has no FileBloom section")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: "resource.service.name", Values: []vm.Value{{Type: vm.TypeString, Data: "absent-1"}}},
						{
							Column: "resource.service.name",
							Values: []vm.Value{{Type: vm.TypeString, Data: "svc-present"}},
						},
					},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "OR with one present service name should not reject file")
}

// --- fileLevelCMSReject unit tests ---
// These tests call cmsRejectByNodes directly with a manually constructed FileSketchSummary
// so they are independent of CMS false-positive behavior in the sketch index.

// buildCMSSummary creates a FileSketchSummary with a fresh CMS containing only the
// given values for the named column. Used to construct controlled test inputs.
func buildCMSSummary(col string, values ...string) *modules_reader.FileSketchSummary {
	cms := sketch.NewCountMinSketch()
	for _, v := range values {
		cms.Add(v, 1)
	}
	return &modules_reader.FileSketchSummary{
		Columns: map[string]*modules_reader.FileColumnSketch{
			col: {CMS: cms},
		},
	}
}

// absentCMSKey returns a key that the given CMS estimates as 0 (definitely absent).
// It searches by appending numeric suffixes to prefix until one maps to 0.
// Fails the test if no zero-estimate key is found within 10 000 attempts.
func absentCMSKey(t *testing.T, cms *sketch.CountMinSketch, prefix string) string {
	t.Helper()
	for i := range 10_000 {
		key := fmt.Sprintf("%s_%d", prefix, i)
		if cms.Estimate(key) == 0 {
			return key
		}
	}
	t.Fatal("absentCMSKey: could not find a key with Estimate==0 after 10 000 attempts — CMS may be saturated")
	return ""
}

// pbOpenReaderWithExtraAttr writes a file with spans that carry an extra resource
// attribute (attrKey=attrVal). Used to exercise CMS for columns outside FileBloom coverage.
func pbOpenReaderWithExtraAttr(t *testing.T, svcName, attrKey, attrVal string) *modules_reader.Reader {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 2,
	})
	require.NoError(t, err)
	tid := [16]byte{0xEA}
	for i := range 4 {
		span := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: uint64(4_000_000_000 + i*1000), //nolint:gosec
			EndTimeUnixNano:   uint64(4_000_002_000 + i*1000), //nolint:gosec
		}
		res := map[string]any{"service.name": svcName, attrKey: attrVal}
		require.NoError(t, w.AddSpan(tid[:], span, res, svcName, nil, ""))
	}
	_, err = w.Flush()
	require.NoError(t, err)
	r, err := modules_reader.NewReaderFromProvider(&pbMemProvider{data: buf.Bytes()})
	require.NoError(t, err)
	return r
}

// TestCMSRejectByNodes_AbsentValue verifies that a leaf equality predicate for a value
// with CMS estimate==0 triggers rejection.
func TestCMSRejectByNodes_AbsentValue(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")
	absent := absentCMSKey(t, summary.Columns["span.env"].CMS, "absent")

	nodes := []vm.RangeNode{
		{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: absent}}},
	}
	assert.True(t, cmsRejectByNodes(summary, nodes), "CMS estimate==0 must trigger rejection")
}

// TestCMSRejectByNodes_PresentValue verifies that a leaf equality predicate for a value
// with CMS estimate>0 does NOT trigger rejection.
func TestCMSRejectByNodes_PresentValue(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")
	require.Greater(t, summary.Columns["span.env"].CMS.Estimate("present-val"), uint16(0))

	nodes := []vm.RangeNode{
		{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: "present-val"}}},
	}
	assert.False(t, cmsRejectByNodes(summary, nodes), "CMS estimate>0 must not trigger rejection")
}

// TestCMSRejectByNodes_NilSummary verifies no rejection when summary is nil.
func TestCMSRejectByNodes_NilSummary(t *testing.T) {
	nodes := []vm.RangeNode{
		{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: "any-val"}}},
	}
	assert.False(t, cmsRejectByNodes(nil, nodes), "nil summary must not trigger rejection")
}

// TestCMSRejectByNodes_RangePredicateNoReject verifies that a Min/Max range predicate
// is never rejected by CMS (CMS only handles equality).
func TestCMSRejectByNodes_RangePredicateNoReject(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")

	nodes := []vm.RangeNode{
		{
			Column: "span.env",
			Min:    &vm.Value{Type: vm.TypeString, Data: "a"},
			Max:    &vm.Value{Type: vm.TypeString, Data: "z"},
		},
	}
	assert.False(t, cmsRejectByNodes(summary, nodes), "range predicates must not be rejected by CMS")
}

// TestCMSRejectByNodes_PatternNoReject verifies that a Pattern predicate is not rejected.
func TestCMSRejectByNodes_PatternNoReject(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")

	nodes := []vm.RangeNode{
		{Column: "span.env", Pattern: ".*"},
	}
	assert.False(t, cmsRejectByNodes(summary, nodes), "pattern predicates must not be rejected by CMS")
}

// TestCMSRejectByNodes_OR_AllAbsent verifies that an OR node where ALL children have
// CMS estimate==0 triggers rejection.
func TestCMSRejectByNodes_OR_AllAbsent(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")
	cms := summary.Columns["span.env"].CMS
	absent1 := absentCMSKey(t, cms, "absent-or-1")
	absent2 := absentCMSKey(t, cms, "absent-or-2")

	nodes := []vm.RangeNode{
		{
			IsOR: true,
			Children: []vm.RangeNode{
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: absent1}}},
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: absent2}}},
			},
		},
	}
	assert.True(t, cmsRejectByNodes(summary, nodes), "OR of all absent values must trigger rejection")
}

// TestCMSRejectByNodes_OR_OnePresent verifies that an OR node where one child is present
// does NOT trigger rejection.
func TestCMSRejectByNodes_OR_OnePresent(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")
	absentX := absentCMSKey(t, summary.Columns["span.env"].CMS, "absent-or-x")

	nodes := []vm.RangeNode{
		{
			IsOR: true,
			Children: []vm.RangeNode{
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: "present-val"}}},
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: absentX}}},
			},
		},
	}
	assert.False(t, cmsRejectByNodes(summary, nodes), "OR with one present child must not trigger rejection")
}

// TestCMSRejectByNodes_AND_OneAbsent verifies that an AND node where one child is absent
// triggers rejection (AND semantics: reject if ANY child rejects).
func TestCMSRejectByNodes_AND_OneAbsent(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")
	absentY := absentCMSKey(t, summary.Columns["span.env"].CMS, "absent-and-y")

	nodes := []vm.RangeNode{
		{
			// IsOR=false → AND
			Children: []vm.RangeNode{
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: "present-val"}}},
				{Column: "span.env", Values: []vm.Value{{Type: vm.TypeString, Data: absentY}}},
			},
		},
	}
	assert.True(t, cmsRejectByNodes(summary, nodes), "AND with one absent child must trigger rejection")
}

// TestCMSRejectByNodes_UnknownColumn verifies that a query for a column not in the
// sketch (no CMS data) does NOT trigger rejection.
func TestCMSRejectByNodes_UnknownColumn(t *testing.T) {
	summary := buildCMSSummary("span.env", "present-val")

	nodes := []vm.RangeNode{
		{Column: "span.unknown", Values: []vm.Value{{Type: vm.TypeString, Data: "any-val"}}},
	}
	assert.False(t, cmsRejectByNodes(summary, nodes), "unknown column must not trigger rejection")
}

// TestFileLevelCMSReject_WiredIntoPlanBlocks verifies that planBlocks does not
// incorrectly reject a file when querying for a value that IS present.
// This is a smoke test of the wiring.
func TestFileLevelCMSReject_WiredIntoPlanBlocks(t *testing.T) {
	r := pbOpenReaderSingleService(t, "svc-wired")
	if r.FileSketchSummary() == nil {
		t.Skip("file has no sketch section")
	}

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.service.name",
					Values: []vm.Value{{Type: vm.TypeString, Data: "svc-wired"}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Greater(t, len(plan.SelectedBlocks), 0, "planBlocks must not reject file for present value")
}

// TestFileLevelCMSReject_ExplainOnRejection verifies that when planBlocks rejects a file
// via the CMS path, plan.Explain is set to the CMS rejection string.
// Uses resource.region — a column tracked by the sketch but NOT covered by FileBloom,
// so bloom reject cannot fire first and mask the CMS rejection.
func TestFileLevelCMSReject_ExplainOnRejection(t *testing.T) {
	r := pbOpenReaderWithExtraAttr(t, "svc-explain", "region", "us-east-1")

	s := r.FileSketchSummary()
	if s == nil {
		t.Skip("file has no sketch section")
	}
	col := s.Columns["resource.region"]
	if col == nil {
		t.Skip("resource.region not in sketch — writer may not track this attribute")
	}

	// Find a region value that CMS guarantees is absent (Estimate==0).
	absentRegion := absentCMSKey(t, col.CMS, "absent-region")

	prog := &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
		Predicates: &vm.QueryPredicates{
			Nodes: []vm.RangeNode{
				{
					Column: "resource.region",
					Values: []vm.Value{{Type: vm.TypeString, Data: absentRegion}},
				},
			},
		},
	}

	plan := planBlocks(r, prog, queryplanner.TimeRange{}, queryplanner.PlanOptions{})
	require.NotNil(t, plan)
	assert.Empty(t, plan.SelectedBlocks, "absent CMS value must reject all blocks")
	assert.Equal(t, "file-level reject: CMS absence for equality predicate", plan.Explain,
		"plan.Explain must identify CMS as the rejection source")
}
