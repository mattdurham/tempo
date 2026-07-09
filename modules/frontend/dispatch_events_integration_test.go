package frontend

// dispatch_events_integration_test.go — issue #493 Task 5: integration coverage driving the REAL
// asyncSearchSharder.backendRequests/timeSlicedJobsFunc dispatch (R7) against a fixture producing
// >20 advancement points, using tracetest, per R2's own explicit "pin it with a test using a
// fixture that exceeds the cap" instruction.

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/tempodb/backend"
)

// manyBlocksOneShardEach builds n blocks, each occupying its own shard (MostRecentShards == n
// forces blocksPerShard == 1), each fully inside [0, windowEnd) so every block overlaps the
// single TimeSlice callers pass to backendRequests.
func manyBlocksOneShardEach(n int) []*backend.BlockMeta {
	blocks := make([]*backend.BlockMeta, n)
	for i := range n {
		bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
		// Distinct, strictly increasing EndTime so blockMetasForSearch's own
		// "search backwards in time" sort produces a deterministic, distinct-per-block order.
		bm.StartTime = time.Unix(int64(100+i*10), 0)
		bm.EndTime = time.Unix(int64(105+i*10), 0)
		bm.Size_ = 1024
		bm.TotalRecords = 1
		blocks[i] = bm
	}
	return blocks
}

// TestBackendRequests_TimeSliced_AttachesDispatchSpanInfo drives the REAL
// asyncSearchSharder.backendRequests entry point (R7) with 25 blocks (> the 20-event cap) each
// overlapping a single TimeSlice, asserting: total span events <= 20, the scalar dispatch
// attributes match hand-computed expectations, and dispatch.position_* are present.
func TestBackendRequests_TimeSliced_AttachesDispatchSpanInfo(t *testing.T) {
	rec := recordedSpansFrontend(t)

	const n = 25
	blocks := manyBlocksOneShardEach(n)

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: n},
		reader: &mockReader{metas: blocks},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=1&end=100000", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices:   []blockpack.TimeSlice{{Start: 1, End: 100000}},
	}

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, false, reqCh, func(error) {})

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, n, jobCount, "one job per block for the single overlapping slice")
	require.Equal(t, n, searchJobResponse.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)

	require.LessOrEqual(t, len(got.Events()), maxAdvancementSpanEvents,
		"advancement events must never exceed the 20-event cap even with %d shards", n)

	attrs := frontendAttrs(got)
	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok)
	assert.EqualValues(t, n, jobsTotal.AsInt64())

	jobsSkipped, ok := attrs["dispatch.jobs_skipped_overlap"]
	require.True(t, ok)
	assert.EqualValues(t, 0, jobsSkipped.AsInt64(), "every block overlaps the single slice -- nothing skipped")

	cancelled, ok := attrs["dispatch.jobs_cancelled_undispatched"]
	require.True(t, ok)
	assert.EqualValues(t, 0, cancelled.AsInt64(), "ctx was never Done during counting")

	posMin, ok := attrs["dispatch.position_min"]
	require.True(t, ok)
	assert.EqualValues(t, 0, posMin.AsInt64(), "every one of the n shards dispatched >=1 job -- min position is shard 0")

	posMax, ok := attrs["dispatch.position_max"]
	require.True(t, ok)
	assert.EqualValues(t, n-1, posMax.AsInt64())
}

// TestBackendRequests_BlockSharded_AttachesDispatchSpanInfo (reviewer-2 finding, issue #493 Task
// 5) drives the REAL asyncSearchSharder.backendRequests entry point (R7) down the
// backendJobsFunc FALLBACK path (nil plan / DispatchBlockSharded / DispatchBoundedRecentFirst) —
// the most common dispatch model in production, and the one Task 5's first pass left
// uninstrumented entirely. Asserts dispatch.jobs_total and the position_min/max attributes are
// attached exactly like the DispatchTimeSliced path's own test above, using the SAME >20-block
// fixture so the event cap is exercised on this path too.
func TestBackendRequests_BlockSharded_AttachesDispatchSpanInfo(t *testing.T) {
	rec := recordedSpansFrontend(t)

	const n = 25
	blocks := manyBlocksOneShardEach(n)

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: n, TargetBytesPerRequest: defaultTargetBytesPerRequest},
		reader: &mockReader{metas: blocks},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=1&end=100000", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	// nil plan -- the DispatchBlockSharded/nil-plan fallback, per backendRequests' own switch.
	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, nil, false, reqCh, func(error) {})

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, n, jobCount, "one job per block (each block is smaller than TargetBytesPerRequest)")
	require.Equal(t, n, searchJobResponse.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)

	require.LessOrEqual(t, len(got.Events()), maxAdvancementSpanEvents,
		"the block-sharded fallback path must respect the same 20-event cap")

	attrs := frontendAttrs(got)
	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok, "dispatch.jobs_total must be attached on the backendJobsFunc fallback path, not just DispatchTimeSliced")
	assert.EqualValues(t, n, jobsTotal.AsInt64())

	posMin, ok := attrs["dispatch.position_min"]
	require.True(t, ok)
	assert.EqualValues(t, 0, posMin.AsInt64())

	posMax, ok := attrs["dispatch.position_max"]
	require.True(t, ok)
	assert.EqualValues(t, n-1, posMax.AsInt64())

	_, hasSkippedOverlap := attrs["dispatch.jobs_skipped_overlap"]
	assert.False(t, hasSkippedOverlap,
		"dispatch.jobs_skipped_overlap has no meaning for the backendJobsFunc dispatch model and must not be emitted")
}

// TestBackendRequests_BlockSharded_PageSplitting_OmitsSkippedOverlapAttribute (reviewer-2 finding,
// issue #493 Task 5) is the fixture reviewer-2 explicitly asked for: REAL page-splitting (a block
// producing MANY jobs, not just one) mixed with a genuinely fully-skipped block (pages == 0) --
// exactly the shape the first version of this instrumentation got wrong (reusing
// attachDispatchSpanInfo's block-count-subtraction formula, which silently clamped to a
// misleading 0 the moment jobsDispatched exceeded len(blocks), hiding the real skip). Drives the
// REAL asyncSearchSharder.backendRequests entry point (R7) and asserts: (a) dispatch.jobs_total
// correctly reflects the true, page-split job count (not the block count), and (b)
// dispatch.jobs_skipped_overlap is absent entirely -- proving the fix is "don't emit a
// misleading number" (reviewer-2's preferred option a), not "emit a differently-wrong number".
func TestBackendRequests_BlockSharded_PageSplitting_OmitsSkippedOverlapAttribute(t *testing.T) {
	rec := recordedSpansFrontend(t)

	// skipped: TotalRecords == 0 -- pagesPerRequest returns 0, backendJobsFunc's own `if pages ==
	// 0 { continue }` drops this block entirely, contributing zero jobs.
	skipped := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	skipped.StartTime = time.Unix(100, 0)
	skipped.EndTime = time.Unix(105, 0)
	skipped.Size_ = 1024
	skipped.TotalRecords = 0

	// pageSplitA/B: Size_ >> TargetBytesPerRequest with TotalRecords=10 forces pagesPerRequest to
	// clamp to 1 page/request (pagesPerQuery computes to 0, clamped up to 1), so jobsInBlock =
	// ceil(10/1) = 10 -- ONE block producing TEN jobs, the exact "many jobs per block" shape a
	// naive len(blocks) denominator cannot represent.
	const targetBytesPerRequest = 100
	pageSplitA := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	pageSplitA.StartTime = time.Unix(200, 0)
	pageSplitA.EndTime = time.Unix(205, 0)
	pageSplitA.Size_ = 10_000
	pageSplitA.TotalRecords = 10

	pageSplitB := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	pageSplitB.StartTime = time.Unix(300, 0)
	pageSplitB.EndTime = time.Unix(305, 0)
	pageSplitB.Size_ = 10_000
	pageSplitB.TotalRecords = 10

	blocks := []*backend.BlockMeta{skipped, pageSplitA, pageSplitB}

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards, TargetBytesPerRequest: targetBytesPerRequest},
		reader: &mockReader{metas: blocks},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=1&end=100000", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, nil, false, reqCh, func(error) {})

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	const wantJobs = 20 // 0 (skipped) + 10 (pageSplitA) + 10 (pageSplitB) -- 3 blocks, 20 jobs.
	require.Equal(t, wantJobs, jobCount,
		"3 blocks must produce 20 jobs total (0+10+10) -- proving jobsDispatched is a JOB count, not a block count")
	require.Equal(t, wantJobs, searchJobResponse.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)
	attrs := frontendAttrs(got)

	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok)
	assert.EqualValues(t, wantJobs, jobsTotal.AsInt64(),
		"dispatch.jobs_total must reflect the TRUE page-split job count (20), not len(blocks) (3) -- "+
			"the naive block-count denominator this test guards against would have clamped a "+
			"'skipped' figure to 0 here despite a real, fully-skipped block existing")

	_, hasSkippedOverlap2 := attrs["dispatch.jobs_skipped_overlap"]
	assert.False(t, hasSkippedOverlap2,
		"dispatch.jobs_skipped_overlap must be absent -- emitting len(blocks)-jobsDispatched here "+
			"would compute max(0, 3-20)=0, silently hiding that 1 of 3 blocks was genuinely fully skipped")
}

// TestBackendRequests_TimeSliced_SkippedOverlapCounted proves jobs_skipped_overlap reflects real
// (block, slice) pairs the overlap predicate rejected — a second slice with NO block overlap at
// all must count as skipped, not silently absent from the total.
func TestBackendRequests_TimeSliced_SkippedOverlapCounted(t *testing.T) {
	rec := recordedSpansFrontend(t)

	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = 1024
	bm.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=500", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	// Slice A overlaps the block; slice B [300,400) does not (the block ends at 200).
	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 100, End: 200},
			{Start: 300, End: 400},
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, false, reqCh, func(error) {})

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, 1, jobCount, "only the overlapping (block, slice) pair dispatches")

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)
	attrs := frontendAttrs(got)

	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok)
	assert.EqualValues(t, 1, jobsTotal.AsInt64())

	jobsSkipped, ok := attrs["dispatch.jobs_skipped_overlap"]
	require.True(t, ok)
	assert.EqualValues(t, 1, jobsSkipped.AsInt64(), "1 block x 2 slices = 2 candidates, 1 dispatched, 1 skipped")
}
