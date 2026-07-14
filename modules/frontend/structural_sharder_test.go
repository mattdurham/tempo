package frontend

// structural_sharder_test.go — pins the #489 (plan-d.md DT1 Correction log) dispatch contract:
// a structural query's DispatchTimeSliced plan emits ONE job per SLICE, never one job per
// (block, slice) pair — the opposite of TestSearchSharder_TimeSlicedDispatch_UsesQueryPlanSlicesNotBlockPaging's
// own filter-path assertion, which this file's tests deliberately mirror in shape to make the
// contrast explicit.

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSearchSharder_StructuralTimeSlicedDispatch_OneJobPerSliceNotPerBlock is this file's
// PRIMARY correctness pin: TWO blocks both overlap BOTH slices below. A structural plan must
// still dispatch exactly ONE job per slice (2 total), never one job per (block, slice) pair (4
// total) — the (block, slice) model the filter path uses would have both blocks' jobs each
// independently return the query's full answer for that slice window, a real duplication (see
// structural_sharder.go's package doc comment). Mutation-verification: this test was confirmed to
// FAIL (4 jobs, not 2) against structuralTimeSlicedJobsFunc's own predecessor draft, which reused
// timeSlicedJobsFunc directly instead of firstOverlappingBlock's single-carrier selection, before
// being left in its correct, passing state.
func TestSearchSharder_StructuralTimeSlicedDispatch_OneJobPerSliceNotPerBlock(t *testing.T) {
	bmA := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bmA.StartTime = time.Unix(100, 0)
	bmA.EndTime = time.Unix(200, 0)
	bmA.Size_ = defaultTargetBytesPerRequest
	bmA.TotalRecords = 1

	bmB := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bmB.StartTime = time.Unix(100, 0)
	bmB.EndTime = time.Unix(200, 0)
	bmB.Size_ = defaultTargetBytesPerRequest
	bmB.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bmA, bmB}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=200", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 150, End: 200},
			{Start: 100, End: 150},
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, true, reqCh, cancelCause)

	var gotReqs []*tempopb.SearchBlockRequest
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}
	require.NoError(t, ctx.Err())

	require.Equal(t, 2, searchJobResponse.TotalJobs, "one job per SLICE, not one per (block, slice) pair")
	require.Len(t, gotReqs, 2)

	sort.Slice(gotReqs, func(i, j int) bool { return gotReqs[i].SearchReq.Start < gotReqs[j].SearchReq.Start })

	require.Equal(t, uint32(100), gotReqs[0].SearchReq.Start)
	require.Equal(t, uint32(150), gotReqs[0].SearchReq.End)
	require.True(t, gotReqs[0].IndexOnly, "a structural time-sliced job must set IndexOnly=true")

	require.Equal(t, uint32(150), gotReqs[1].SearchReq.Start)
	require.Equal(t, uint32(200), gotReqs[1].SearchReq.End)
	require.True(t, gotReqs[1].IndexOnly, "a structural time-sliced job must set IndexOnly=true")
}

// TestSearchSharder_StructuralTimeSlicedDispatch_SkipsSliceWithNoOverlappingBlocks mirrors
// TestSearchSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs's own filter-path
// assertion for the structural one-job-per-slice model: a slice with ZERO overlapping blocks
// dispatches no job at all (nothing exists yet to search).
func TestSearchSharder_StructuralTimeSlicedDispatch_SkipsSliceWithNoOverlappingBlocks(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(150, 0) // only overlaps the first slice below
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=250", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 100, End: 150}, // overlaps bm
			{Start: 200, End: 250}, // does NOT overlap bm
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, true, reqCh, cancelCause)

	var gotReqs []*tempopb.SearchBlockRequest
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}
	require.NoError(t, ctx.Err())

	require.Equal(t, 1, searchJobResponse.TotalJobs, "the non-overlapping slice must dispatch no job")
	require.Len(t, gotReqs, 1)
	require.Equal(t, uint32(100), gotReqs[0].SearchReq.Start)
	require.Equal(t, uint32(150), gotReqs[0].SearchReq.End)
}

// TestStructuralSharder_TimeSlicedDispatch_SkipDispatchSlice_NoJobDispatchedAndTotalJobsExcludesIt
// (issue #499 Phase 3) mirrors this file's own SkipsSliceWithNoOverlappingBlocks test, but for the
// NEW SkipDispatch gate rather than a lack of overlapping blocks: a block overlaps BOTH slices
// below, yet the SkipDispatch=true slice must still contribute zero jobs, through
// structuralTimeSlicedJobsFunc's ONE-job-per-slice model (not per (block, slice) pair) — proving
// the gate is checked regardless of how many blocks would otherwise have carried the slice.
func TestStructuralSharder_TimeSlicedDispatch_SkipDispatchSlice_NoJobDispatchedAndTotalJobsExcludesIt(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0) // overlaps both slices below.
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=200", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 150, End: 200, SkipDispatch: true},
			{Start: 100, End: 150, SkipDispatch: false},
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, cancelCause := context.WithCancelCause(context.Background())
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, true, reqCh, cancelCause)

	var gotReqs []*tempopb.SearchBlockRequest
	for pr := range reqCh {
		parsed, err := api.ParseSearchBlockRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}
	require.NoError(t, ctx.Err())

	require.Equal(t, 1, searchJobResponse.TotalJobs, "the SkipDispatch slice must dispatch no job")
	require.Len(t, gotReqs, 1)
	require.Equal(t, uint32(100), gotReqs[0].SearchReq.Start)
	require.Equal(t, uint32(150), gotReqs[0].SearchReq.End)
}

// TestSearchSharder_StructuralTimeSlicedDispatch_AttachesDispatchSpanInfo (go-presubmit/holistic
// MEDIUM finding, issue #493) drives the REAL asyncSearchSharder.backendRequests entry point (R7)
// down the structural (planIsStructural=true) branch with a tracer installed -- search_sharder.go's
// structural call site (`attachDispatchSpanInfo(ctx, resp.TotalJobs, len(plan.Slices), ...)`) had
// zero tracer-based test coverage before this: its own dedicated test file asserted only on job
// counts/IndexOnly, never on a single dispatch.* attribute or span event, leaving this call site's
// own independently-written totalCandidatePairs=len(plan.Slices) formula unguarded against exactly
// the "silently clamps to a misleading 0" bug class reviewer-2 already caught once for the
// backendJobsFunc call sites. Mirrors this file's own TestSearchSharder_StructuralTimeSlicedDispatch_
// SkipsSliceWithNoOverlappingBlocks fixture shape (one non-overlapping slice) so
// dispatch.jobs_skipped_overlap has a real, non-zero value to pin.
func TestSearchSharder_StructuralTimeSlicedDispatch_AttachesDispatchSpanInfo(t *testing.T) {
	rec := recordedSpansFrontend(t)

	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(150, 0) // only overlaps the first slice below
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1

	s := &asyncSearchSharder{
		cfg:    SearchSharderConfig{MostRecentShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", "/?tags=foo%3Dbar&limit=50&start=100&end=250", nil)
	searchReq, err := api.ParseSearchRequest(r)
	require.NoError(t, err)

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 100, End: 150}, // overlaps bm -- 1 job dispatched
			{Start: 200, End: 250}, // does NOT overlap bm -- 1 slice skipped
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(r)
	searchJobResponse := &combiner.SearchJobResponse{}

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, searchJobResponse, plan, true, reqCh, func(error) {})

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, 1, jobCount)
	require.Equal(t, 1, searchJobResponse.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)
	attrs := frontendAttrs(got)

	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok, "dispatch.jobs_total must be attached on the structural dispatch path")
	assert.EqualValues(t, 1, jobsTotal.AsInt64())

	jobsSkipped, ok := attrs["dispatch.jobs_skipped_overlap"]
	require.True(t, ok, "the structural path DOES have an overlap-filter concept (one job per slice) -- unlike backendJobsFunc, dispatch.jobs_skipped_overlap must be present")
	assert.EqualValues(t, 1, jobsSkipped.AsInt64(), "2 slices, 1 dispatched, 1 skipped (no overlapping block)")

	require.LessOrEqual(t, len(got.Events()), maxAdvancementSpanEvents)
}
