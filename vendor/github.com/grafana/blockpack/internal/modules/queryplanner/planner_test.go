package queryplanner_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// memProvider is an in-memory ReaderProvider for tests.
type memProvider struct{ data []byte }

func (m *memProvider) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *memProvider) ReadAt(p []byte, off int64, _ rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// testSpan creates a minimal OTLP span.
func testSpan(traceID [16]byte, spanIdx int, name string) *tracev1.Span {
	return &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              name,
		StartTimeUnixNano: uint64(spanIdx) * 1_000_000_000,             //nolint:gosec
		EndTimeUnixNano:   uint64(spanIdx)*1_000_000_000 + 500_000_000, //nolint:gosec
	}
}

// addSpan writes one span via AddSpan; fatals on error.
func addSpan(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	name string,
	resAttrs map[string]any,
) {
	t.Helper()
	sp := testSpan(traceID, spanIdx, name)
	require.NoError(t, w.AddSpan(sp.TraceId, sp, resAttrs, "", nil, ""))
}

// newWriter creates a modules Writer with the given maxBlockSpans.
func newWriter(t *testing.T, buf *bytes.Buffer, maxBlockSpans int) *modules_blockio.Writer {
	t.Helper()
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: maxBlockSpans,
	})
	require.NoError(t, err)
	return w
}

// flush calls Flush and fatals on error.
func flush(t *testing.T, w *modules_blockio.Writer) {
	t.Helper()
	_, err := w.Flush()
	require.NoError(t, err)
}

// openReader opens a modules Reader from raw bytes.
func openReader(t *testing.T, data []byte) *reader.Reader {
	t.Helper()
	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(t, err)
	return r
}

// traceID builds a deterministic 16-byte trace ID from an integer.
func traceID(n int) [16]byte {
	var id [16]byte
	id[0] = byte(n)      //nolint:gosec
	id[1] = byte(n >> 8) //nolint:gosec
	return id
}

// --- Tests ---

// TestPlanNoPredicates verifies that with no predicates all blocks are selected.
func TestPlanNoPredicates(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5) // 5 spans/block → multiple blocks
	for i := range 20 {
		addSpan(t, w, traceID(i), i, "op", map[string]any{"service.name": "svc"})
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	p := queryplanner.NewPlanner(r)

	plan := p.Plan(nil, queryplanner.TimeRange{})

	assert.Equal(t, r.BlockCount(), plan.TotalBlocks)
	assert.Equal(t, r.BlockCount(), len(plan.SelectedBlocks))
	for i := 1; i < len(plan.SelectedBlocks); i++ {
		assert.Less(t, plan.SelectedBlocks[i-1], plan.SelectedBlocks[i], "blocks must be sorted")
	}
}

// TestPlanEmptyFile verifies that an empty file produces an empty plan.
func TestPlanEmptyFile(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 0)
	flush(t, w)

	r := openReader(t, buf.Bytes())
	p := queryplanner.NewPlanner(r)

	plan := p.Plan([]queryplanner.Predicate{
		{Columns: []string{"resource.service.name"}},
	}, queryplanner.TimeRange{})

	assert.Equal(t, 0, plan.TotalBlocks)
	assert.Empty(t, plan.SelectedBlocks)
}

// TestPlanRangePruning verifies that range-index lookup eliminates blocks that
// do not contain the queried service name value.
//
// Setup: 3 blocks with distinct service names (alpha, beta, gamma). A query for
// "svc-alpha" should select only the block(s) containing that service.
func TestPlanRangePruning(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)

	services := []string{"svc-alpha", "svc-beta", "svc-gamma"}
	for blk, svc := range services {
		for i := range 5 {
			spanIdx := blk*5 + i
			addSpan(t, w, traceID(spanIdx), spanIdx, "op", map[string]any{"service.name": svc})
		}
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 2, "need at least 2 blocks for range pruning test")

	// Check that the range index exists for service.name.
	_, hasIndex := r.RangeColumnType("resource.service.name")
	if !hasIndex {
		t.Skip("range index not present for resource.service.name — skip range pruning test")
	}

	p := queryplanner.NewPlanner(r)
	plan := p.Plan([]queryplanner.Predicate{
		{
			Columns: []string{"resource.service.name"},
			Values:  []string{"svc-alpha"},
			ColType: shared.ColumnTypeString,
		},
	}, queryplanner.TimeRange{})

	// At least one block must be selected (the alpha block).
	require.NotEmpty(t, plan.SelectedBlocks, "at least one block must match svc-alpha")
	// The range index should eliminate at least some blocks.
	assert.Greater(t, plan.PrunedByIndex, 0, "range index must prune at least one block")
	// Fewer blocks than total must be selected.
	assert.Less(t, len(plan.SelectedBlocks), plan.TotalBlocks,
		"range index must eliminate blocks without svc-alpha")
}

// TestPlanRangePruningNoIndex verifies that when no range index exists for the
// column, PrunedByIndex remains 0 and all surviving blocks are returned.
func TestPlanRangePruningNoIndex(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)
	for i := range 10 {
		addSpan(t, w, traceID(i), i, "op", map[string]any{"service.name": "svc"})
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	p := queryplanner.NewPlanner(r)

	// Use a column that definitely has no range index.
	plan := p.Plan([]queryplanner.Predicate{
		{
			Columns: []string{"span.nonexistent.col"},
			Values:  []string{"anything"},
			ColType: shared.ColumnTypeString,
		},
	}, queryplanner.TimeRange{})

	assert.Equal(t, 0, plan.PrunedByIndex, "no range index → PrunedByIndex must be 0")
}

// TestPlanRangePruningIndexedNoMatch verifies that when a range-indexed column is queried
// for a value that exists in no block, all candidates are pruned (PrunedByIndex == total).
// This distinguishes "indexed but no matches" (empty set → prune all) from "no index"
// (nil → skip conservatively).
func TestPlanRangePruningIndexedNoMatch(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)
	services := []string{"svc-alpha", "svc-beta"}
	for blk, svc := range services {
		for i := range 5 {
			spanIdx := blk*5 + i
			addSpan(t, w, traceID(spanIdx), spanIdx, "op", map[string]any{"service.name": svc})
		}
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	_, hasIndex := r.RangeColumnType("resource.service.name")
	if !hasIndex {
		t.Skip("range index not present for resource.service.name")
	}

	p := queryplanner.NewPlanner(r)
	// "AAA" sorts before "svc-alpha" in ASCII order (uppercase < lowercase),
	// so BlocksForRange returns nil (below all stored lower boundaries) — the
	// union is empty and leafBlockSet returns a non-nil empty map.
	plan := p.Plan([]queryplanner.Predicate{
		{
			Columns: []string{"resource.service.name"},
			Values:  []string{"AAA"},
			ColType: shared.ColumnTypeString,
		},
	}, queryplanner.TimeRange{})

	// The index was consulted and found nothing — all blocks must be pruned.
	assert.Equal(t, plan.TotalBlocks, plan.PrunedByIndex,
		"indexed lookup with no matches must prune all blocks")
	assert.Empty(t, plan.SelectedBlocks)
}

// TestPlanRangeIntervalPruning verifies that IntervalMatch predicates use
// BlocksForRangeInterval to prune blocks. With 3 blocks containing distinct service
// names, an interval query for ["svc-a", "svc-b"] should select blocks for both
// svc-alpha and svc-beta but prune svc-gamma.
// NOTE-011: interval matching for case-insensitive regex prefix lookups.
func TestPlanRangeIntervalPruning(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)

	services := []string{"svc-alpha", "svc-beta", "svc-gamma"}
	for blk, svc := range services {
		for i := range 5 {
			spanIdx := blk*5 + i
			addSpan(t, w, traceID(spanIdx), spanIdx, "op", map[string]any{"service.name": svc})
		}
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 2, "need at least 2 blocks for interval test")

	_, hasIndex := r.RangeColumnType("resource.service.name")
	if !hasIndex {
		t.Skip("range index not present for resource.service.name — skip interval pruning test")
	}

	p := queryplanner.NewPlanner(r)
	plan := p.Plan([]queryplanner.Predicate{
		{
			Columns:       []string{"resource.service.name"},
			Values:        []string{"svc-a", "svc-b"},
			ColType:       shared.ColumnTypeString,
			IntervalMatch: true,
		},
	}, queryplanner.TimeRange{})

	require.NotEmpty(t, plan.SelectedBlocks, "interval query must select at least one block")
	// The interval [svc-a, svc-b] should include blocks for svc-alpha and svc-beta
	// but may prune blocks only containing svc-gamma.
	assert.Greater(t, plan.PrunedByIndex, 0,
		"interval range index must prune at least one block (svc-gamma)")
}

// TestPlanRangePruningOR verifies that a composite OR predicate unions block sets
// correctly. With 3 blocks (svc-alpha, svc-beta, svc-gamma) and an OR range lookup
// for svc-alpha and svc-gamma, svc-beta must be pruned.
func TestPlanRangePruningOR(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)

	services := []string{"svc-alpha", "svc-beta", "svc-gamma"}
	for blk, svc := range services {
		for i := range 5 {
			spanIdx := blk*5 + i
			addSpan(t, w, traceID(spanIdx), spanIdx, "op", map[string]any{"service.name": svc})
		}
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	require.GreaterOrEqual(t, r.BlockCount(), 3, "need at least 3 blocks for OR pruning test")

	_, hasIndex := r.RangeColumnType("resource.service.name")
	if !hasIndex {
		t.Skip("range index not present for resource.service.name — skip OR pruning test")
	}

	p := queryplanner.NewPlanner(r)

	// OR composite: svc-alpha OR svc-gamma. svc-beta block must be pruned.
	plan := p.Plan([]queryplanner.Predicate{
		{
			Op: queryplanner.LogicalOR,
			Children: []queryplanner.Predicate{
				{Columns: []string{"resource.service.name"}, Values: []string{"svc-alpha"}},
				{Columns: []string{"resource.service.name"}, Values: []string{"svc-gamma"}},
			},
		},
	}, queryplanner.TimeRange{})

	require.NotEmpty(t, plan.SelectedBlocks, "at least one block must match")
	assert.Greater(t, plan.PrunedByIndex, 0, "range index must prune svc-beta block")
	assert.Less(t, len(plan.SelectedBlocks), plan.TotalBlocks,
		"range index must eliminate the svc-beta block")
}

// TestFetchBlocksReturnsValidBytes verifies FetchBlocks returns parseable block bytes.
func TestFetchBlocksReturnsValidBytes(t *testing.T) {
	var buf bytes.Buffer
	w := newWriter(t, &buf, 5)
	for i := range 20 {
		addSpan(t, w, traceID(i), i, "op", map[string]any{"service.name": "svc"})
	}
	flush(t, w)

	r := openReader(t, buf.Bytes())
	p := queryplanner.NewPlanner(r)

	plan := p.Plan(nil, queryplanner.TimeRange{})
	rawBlocks, err := p.FetchBlocks(plan)
	require.NoError(t, err)

	assert.Equal(t, len(plan.SelectedBlocks), len(rawBlocks))

	for blockIdx, raw := range rawBlocks {
		meta := r.BlockMeta(blockIdx)
		bwb, parseErr := r.ParseBlockFromBytes(raw, nil, meta)
		require.NoError(t, parseErr, "block %d must parse cleanly", blockIdx)
		assert.NotNil(t, bwb.Block)
		assert.Greater(t, bwb.Block.SpanCount(), 0)
	}
}
