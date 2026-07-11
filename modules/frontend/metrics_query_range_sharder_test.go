package frontend

import (
	"context"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

func TestBuildBackendRequestsExemplarsOneBlock(t *testing.T) {
	// Create the test sharder with exemplars enabled
	sharder := &queryRangeSharder{
		logger: log.NewNopLogger(),
		cfg: QueryRangeSharderConfig{
			MaxExemplars:    100,
			StreamingShards: defaultMostRecentShards,
		},
	}
	tenantID := "test-tenant"
	targetBytesPerRequest := 1000

	testCases := []struct {
		name              string
		totalRecords      uint32
		blockSize         uint64
		exemplars         uint32
		expectedBatches   int
		expectedExemplars int
	}{
		{
			name:              "basic",
			totalRecords:      100,
			blockSize:         uint64(targetBytesPerRequest),
			exemplars:         5,
			expectedExemplars: 6, // 5 * 1.2
			expectedBatches:   1,
		},
		{
			name:              "two batches",
			totalRecords:      100,
			blockSize:         uint64(2 * targetBytesPerRequest),
			exemplars:         5,
			expectedExemplars: 6, // 5 * 1.2
			expectedBatches:   2,
		},
		{
			name:              "high record count",
			totalRecords:      10000,
			blockSize:         50000,
			exemplars:         10,
			expectedExemplars: 50, // 1 per each batch
			expectedBatches:   50,
		},
		{
			name:              "totalRecords == blockSize == targetBytesPerRequest",
			totalRecords:      uint32(targetBytesPerRequest),
			blockSize:         uint64(targetBytesPerRequest),
			exemplars:         10,
			expectedExemplars: 12, // 10 * 1.2
			expectedBatches:   1,
		},
		{
			name:              "large block size",
			totalRecords:      500,
			blockSize:         50000,
			exemplars:         20,
			expectedExemplars: 50, // 1 per each batch
			expectedBatches:   50,
		},
		{
			name:              "small block",
			totalRecords:      10,
			blockSize:         100,
			exemplars:         1,
			expectedExemplars: 2, // 1 * 1.2 -> rounded up to 2
			expectedBatches:   1,
		},
		{
			name:              "block with single record",
			totalRecords:      1,
			blockSize:         uint64(2 * targetBytesPerRequest),
			exemplars:         1,
			expectedExemplars: 2, // 1 * 1.2 -> rounded up to 2
			expectedBatches:   1,
		},
		{
			name:              "block with single record",
			totalRecords:      1,
			blockSize:         uint64(1.5 * float64(targetBytesPerRequest)),
			exemplars:         1,
			expectedExemplars: 2, // 1 * 1.2 -> rounded up to 2
			expectedBatches:   1,
		},
		{
			name:              "block with 2 records",
			totalRecords:      2,
			blockSize:         uint64(2 * targetBytesPerRequest),
			exemplars:         1,
			expectedExemplars: 2, // 1 * 1.2 -> rounded up to 2
			expectedBatches:   2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test requests with exemplars enabled
			req := httptest.NewRequest("GET", "/test", nil)
			parentReq := pipeline.NewHTTPRequest(req)
			searchReq := tempopb.QueryRangeRequest{
				Query:     "test_query",
				Start:     uint64(time.Now().Add(-1 * time.Hour).UnixNano()),
				End:       uint64(time.Now().UnixNano()),
				Step:      uint64(60 * time.Second.Nanoseconds()),
				Exemplars: tc.exemplars,
			}

			// Create mock block metadata
			blockMeta := &backend.BlockMeta{
				BlockID:      backend.MustParse(uuid.NewString()),
				TotalRecords: tc.totalRecords,
				Size_:        tc.blockSize,
				StartTime:    time.Now().Add(-1 * time.Hour),
				EndTime:      time.Now(),
			}

			reqCh := make(chan pipeline.Request, 100)

			blocks := []*backend.BlockMeta{blockMeta}
			blockIter := backendJobsFunc(blocks, targetBytesPerRequest, defaultMostRecentShards, uint32(searchReq.End))

			go func() {
				// Calculate total duration for exemplar distribution
				var totalDurationNanos int64
				for _, b := range blocks {
					if !b.EndTime.Before(b.StartTime) {
						totalDurationNanos += b.EndTime.UnixNano() - b.StartTime.UnixNano()
					}
				}

				getExemplarsForBlock := func(m *backend.BlockMeta) uint32 {
					return sharder.exemplarsForBlock(m, searchReq.Exemplars, totalDurationNanos)
				}

				sharder.buildBackendRequests(t.Context(), tenantID, parentReq, searchReq, 0, blockIter, reqCh, getExemplarsForBlock)
			}()

			// Collect requests
			var generatedRequests []pipeline.Request
			for req := range reqCh {
				generatedRequests = append(generatedRequests, req)
			}
			assert.Equal(t, tc.expectedBatches, len(generatedRequests), "Number of generated requests should match expected value")

			var totalExemplars int
			for _, req := range generatedRequests {
				uri := req.HTTPRequest().URL.String()
				exemplarsValue := extractExemplarsValue(t, uri)
				assert.Greater(t, exemplarsValue, 0, "Exemplars per batch should be at least 1")
				totalExemplars += exemplarsValue
			}
			assert.Equal(t, tc.expectedExemplars, totalExemplars, "Total exemplars should match expected value")
		})
	}
}

// extractExemplarsValue extracts the exemplars value from the URL
func extractExemplarsValue(t *testing.T, uri string) int {
	require.True(t, strings.Contains(uri, "exemplars="), "Request should contain exemplars parameter")
	exemplarsParam := ""
	for param := range strings.SplitSeq(uri, "&") {
		if strings.HasPrefix(param, "exemplars=") {
			exemplarsParam = strings.TrimPrefix(param, "exemplars=")
			break
		}
	}
	require.NotEmpty(t, exemplarsParam, "Exemplars parameter should not be empty")

	exemplarsValue, err := strconv.Atoi(exemplarsParam)
	require.NoError(t, err, "Should be able to parse exemplars value")

	return exemplarsValue
}

// TestMetricsQueryRangeSharder_TimeSlicedDispatch_UsesQueryPlanSlicesNotBlockPaging is the
// metrics analog of the search sharder's #487 pin: given a QueryPlan with Strategy:
// DispatchTimeSliced and a non-empty Slices, backendRequests must emit one job per (block,
// slice) pair with IndexOnly=true and Start/End narrowed to the slice's window intersected
// with the block's own overlap (via the same TrimToBlockOverlap narrowing already used
// today) — NOT the pagesPerRequest-computed page range.
func TestMetricsQueryRangeSharder_TimeSlicedDispatch_UsesQueryPlanSlicesNotBlockPaging(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2
	bm.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	s := &queryRangeSharder{
		logger: log.NewNopLogger(),
		cfg:    QueryRangeSharderConfig{StreamingShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	searchReq := tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(100 * time.Second),
		End:   uint64(200 * time.Second),
		Step:  uint64(10 * time.Second),
	}

	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 150, End: 200},
			{Start: 100, End: 150},
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx := context.Background()
	pipelineRequest := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "/", nil))
	jobMetadata := &combiner.QueryRangeJobResponse{}
	cutoff := time.Unix(1000, 0) // well after the block/slices so nothing is trimmed away

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, cutoff, defaultTargetBytesPerRequest, plan, reqCh, jobMetadata)

	var gotReqs []*tempopb.QueryRangeRequest
	for pr := range reqCh {
		parsed, err := api.ParseQueryRangeRequest(pr.HTTPRequest())
		require.NoError(t, err)
		gotReqs = append(gotReqs, parsed)
	}

	require.Equal(t, 2, jobMetadata.TotalJobs, "one job per (block, slice) pair for the single block")
	require.Len(t, gotReqs, 2)

	sort.Slice(gotReqs, func(i, j int) bool { return gotReqs[i].Start < gotReqs[j].Start })

	require.Equal(t, uint64(100*time.Second), gotReqs[0].Start)
	require.Equal(t, uint64(150*time.Second), gotReqs[0].End)
	require.True(t, gotReqs[0].IndexOnly, "a time-sliced job must set IndexOnly=true")

	require.Equal(t, uint64(150*time.Second), gotReqs[1].Start)
	require.Equal(t, uint64(200*time.Second), gotReqs[1].End)
	require.True(t, gotReqs[1].IndexOnly, "a time-sliced job must set IndexOnly=true")
}

// TestMetricsQueryRangeSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs pins the
// fix for a HIGH correctness bug found during review: timeSlicedJobsFunc does a full cross
// product of every block against every slice with no per-pair time-overlap filtering, so a
// (block, slice) pair with ZERO time overlap is routine, not a contrived edge case, once a
// query spans multiple blocks with different time ranges. traceql.TrimToBlockOverlap returns
// an INVERTED range (start > end, not merely start == end) for a genuinely disjoint
// (block, slice) pair — the guard must catch both shapes and skip silently, never emit a
// malformed/inverted QueryRangeRequest downstream.
func TestMetricsQueryRangeSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs(t *testing.T) {
	blockA := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockA.StartTime = time.Unix(100, 0)
	blockA.EndTime = time.Unix(150, 0)
	blockA.Size_ = defaultTargetBytesPerRequest
	blockA.TotalRecords = 1
	blockA.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	blockB := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockB.StartTime = time.Unix(500, 0)
	blockB.EndTime = time.Unix(600, 0)
	blockB.Size_ = defaultTargetBytesPerRequest
	blockB.TotalRecords = 1
	blockB.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	s := &queryRangeSharder{
		logger: log.NewNopLogger(),
		cfg:    QueryRangeSharderConfig{StreamingShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{blockA, blockB}},
	}

	searchReq := tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(100 * time.Second),
		End:   uint64(600 * time.Second),
		Step:  uint64(10 * time.Second),
	}

	// slice1 overlaps ONLY blockA; slice2 overlaps ONLY blockB. The cross product therefore
	// includes two genuinely disjoint (block, slice) pairs: (blockA, slice2), (blockB, slice1).
	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 500, End: 600}, // slice2
			{Start: 100, End: 150}, // slice1
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx := context.Background()
	pipelineRequest := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "/", nil))
	jobMetadata := &combiner.QueryRangeJobResponse{}
	cutoff := time.Unix(1000, 0)

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, cutoff, defaultTargetBytesPerRequest, plan, reqCh, jobMetadata)

	var gotReqs []*tempopb.QueryRangeRequest
	for pr := range reqCh {
		parsed, err := api.ParseQueryRangeRequest(pr.HTTPRequest())
		require.NoError(t, err)
		// The bug under test: a disjoint pair must never reach this point as an inverted
		// range. Assert the invariant directly on every request that IS emitted.
		require.LessOrEqual(t, parsed.Start, parsed.End, "must never emit an inverted time range")
		gotReqs = append(gotReqs, parsed)
	}

	// Only the two genuinely overlapping (block, slice) pairs must produce a job; the two
	// disjoint pairs must be silently skipped, not sent as malformed requests.
	require.Len(t, gotReqs, 2)

	sort.Slice(gotReqs, func(i, j int) bool { return gotReqs[i].Start < gotReqs[j].Start })
	require.Equal(t, uint64(100*time.Second), gotReqs[0].Start)
	require.Equal(t, uint64(150*time.Second), gotReqs[0].End)
	require.Equal(t, uint64(500*time.Second), gotReqs[1].Start)
	require.Equal(t, uint64(600*time.Second), gotReqs[1].End)

	// Fix for #161: timeSlicedJobsFunc counted the full, unfiltered cross product (4) into
	// jobMetadata.TotalJobs / Shard.TotalJobs, even though the two disjoint pairs above are
	// silently skipped and only 2 jobs are ever dispatched. shardtracker.CompletionTracker
	// compares foundResponses[shard] against exactly this TotalJobs value to decide a shard is
	// complete (tracker.go) — an inflated count can never be reached, stalling completion
	// tracking. TotalJobs (and the sum of every Shard's own TotalJobs) must equal the number of
	// requests actually sent to reqCh, not the raw block x slice cross product.
	require.Equal(t, len(gotReqs), jobMetadata.TotalJobs, "TotalJobs must match jobs actually dispatched, not the raw block x slice cross product")
	var sumShardJobs int
	for _, sh := range jobMetadata.Shards {
		sumShardJobs += int(sh.TotalJobs)
	}
	require.Equal(t, jobMetadata.TotalJobs, sumShardJobs, "sum of per-shard TotalJobs must equal the overall TotalJobs")
}

// TestMetricsQueryRangeSharder_BlockShardedDispatch_UnchangedWhenStrategyIsBlockSharded pins
// the parity-and-fallback-first discipline for metrics: a nil plan, and a plan whose Strategy
// is the zero-value DispatchBlockSharded, must both produce identical job counts to today's
// block-sharded-only path, with IndexOnly=false (the zero value) on every job.
func TestMetricsQueryRangeSharder_BlockShardedDispatch_UnchangedWhenStrategyIsBlockSharded(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2
	bm.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	newSharder := func() *queryRangeSharder {
		return &queryRangeSharder{
			logger: log.NewNopLogger(),
			cfg:    QueryRangeSharderConfig{StreamingShards: defaultMostRecentShards},
			reader: &mockReader{metas: []*backend.BlockMeta{bm}},
		}
	}

	runWithPlan := func(t *testing.T, plan *blockpack.QueryPlan) int {
		t.Helper()
		searchReq := tempopb.QueryRangeRequest{
			Query: "{} | count_over_time()",
			Start: uint64(100 * time.Second),
			End:   uint64(200 * time.Second),
			Step:  uint64(10 * time.Second),
		}

		reqCh := make(chan pipeline.Request)
		ctx := context.Background()
		pipelineRequest := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "/", nil))
		jobMetadata := &combiner.QueryRangeJobResponse{}
		cutoff := time.Unix(1000, 0)

		go newSharder().backendRequests(ctx, "test", pipelineRequest, searchReq, cutoff, defaultTargetBytesPerRequest, plan, reqCh, jobMetadata)

		for pr := range reqCh {
			parsed, err := api.ParseQueryRangeRequest(pr.HTTPRequest())
			require.NoError(t, err)
			require.False(t, parsed.IndexOnly, "block-sharded jobs must have IndexOnly=false")
		}
		return jobMetadata.TotalJobs
	}

	nilJobs := runWithPlan(t, nil)
	blockShardedJobs := runWithPlan(t, &blockpack.QueryPlan{Strategy: blockpack.DispatchBlockSharded})

	require.Equal(t, nilJobs, blockShardedJobs)
}

// TestMetricsQueryRangeSharder_TimeSlicedDispatch_AttachesDispatchSpanInfo (go-presubmit/holistic
// MEDIUM finding, issue #493) drives the REAL queryRangeSharder.backendRequests entry point (R7)
// down its DispatchTimeSliced branch with a tracer installed -- this call site
// (metrics_query_range_sharder.go:317) had zero tracer-based test coverage before this: existing
// tests asserted only on dispatched job shape, never on a dispatch.* span attribute. Reuses
// TestMetricsQueryRangeSharder_TimeSlicedDispatch_SkipsNonOverlappingBlockSlicePairs' own
// disjoint-pairs fixture shape (2 blocks x 2 slices, only 2 of the 4 candidate pairs genuinely
// overlap) so dispatch.jobs_skipped_overlap has a real, non-zero, hand-verifiable value to pin --
// this sharder's own overlap predicate (a per-slice traceql.TrimToBlockOverlap closure) is
// independently written from search_sharder.go's blockOverlapsSlice, so nothing before this
// regression-tested that ITS formula/arithmetic holds at this specific call site either.
func TestMetricsQueryRangeSharder_TimeSlicedDispatch_AttachesDispatchSpanInfo(t *testing.T) {
	rec := recordedSpansFrontend(t)

	blockA := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockA.StartTime = time.Unix(100, 0)
	blockA.EndTime = time.Unix(150, 0)
	blockA.Size_ = defaultTargetBytesPerRequest
	blockA.TotalRecords = 1
	blockA.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	blockB := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	blockB.StartTime = time.Unix(500, 0)
	blockB.EndTime = time.Unix(600, 0)
	blockB.Size_ = defaultTargetBytesPerRequest
	blockB.TotalRecords = 1
	blockB.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	s := &queryRangeSharder{
		logger: log.NewNopLogger(),
		cfg:    QueryRangeSharderConfig{StreamingShards: defaultMostRecentShards},
		reader: &mockReader{metas: []*backend.BlockMeta{blockA, blockB}},
	}

	searchReq := tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(100 * time.Second),
		End:   uint64(600 * time.Second),
		Step:  uint64(10 * time.Second),
	}

	// slice1 overlaps ONLY blockA; slice2 overlaps ONLY blockB -- 2 of the 4 (block, slice)
	// candidate pairs are genuinely disjoint and must be skipped.
	plan := &blockpack.QueryPlan{
		Strategy: blockpack.DispatchTimeSliced,
		Slices: []blockpack.TimeSlice{
			{Start: 500, End: 600}, // slice2
			{Start: 100, End: 150}, // slice1
		},
	}

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "/", nil))
	jobMetadata := &combiner.QueryRangeJobResponse{}
	cutoff := time.Unix(1000, 0)

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, cutoff, defaultTargetBytesPerRequest, plan, reqCh, jobMetadata)

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, 2, jobCount)
	require.Equal(t, 2, jobMetadata.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)
	attrs := frontendAttrs(got)

	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok)
	assert.EqualValues(t, 2, jobsTotal.AsInt64())

	jobsSkipped, ok := attrs["dispatch.jobs_skipped_overlap"]
	require.True(t, ok, "the metrics time-sliced path DOES have an overlap-filter concept -- dispatch.jobs_skipped_overlap must be present")
	assert.EqualValues(t, 2, jobsSkipped.AsInt64(), "2 blocks x 2 slices = 4 candidates, 2 dispatched, 2 disjoint pairs skipped")

	require.LessOrEqual(t, len(got.Events()), maxAdvancementSpanEvents)
}

// TestMetricsQueryRangeSharder_BlockShardedDispatch_AttachesDispatchSpanInfoWithEventCap
// (go-presubmit/holistic MEDIUM finding, issue #493) drives the REAL
// queryRangeSharder.backendRequests entry point (R7) down its backendJobsFunc FALLBACK branch
// (metrics_query_range_sharder.go:347, nil plan) with a tracer installed AND a >20-shard fixture,
// per the review's own explicit request ("including a >20-shard fixture to pin the event cap on
// that sharder too" -- previously only pinned for the search sharder). Asserts dispatch.jobs_total
// reflects the true job count, dispatch.jobs_skipped_overlap is absent (backendJobsFunc has no
// overlap-filter concept -- see attachDispatchSpanInfoNoOverlapFilter's own doc comment), and the
// 20-event cap holds even at this scale.
func TestMetricsQueryRangeSharder_BlockShardedDispatch_AttachesDispatchSpanInfoWithEventCap(t *testing.T) {
	rec := recordedSpansFrontend(t)

	const n = 25
	blocks := make([]*backend.BlockMeta, n)
	for i := range n {
		bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
		bm.StartTime = time.Unix(int64(100+i*10), 0)
		bm.EndTime = time.Unix(int64(105+i*10), 0)
		bm.Size_ = 1024
		bm.TotalRecords = 1
		bm.ReplicationFactor = backend.MetricsGeneratorReplicationFactor
		blocks[i] = bm
	}

	s := &queryRangeSharder{
		logger: log.NewNopLogger(),
		cfg:    QueryRangeSharderConfig{StreamingShards: n},
		reader: &mockReader{metas: blocks},
	}

	searchReq := tempopb.QueryRangeRequest{
		Query: "{} | count_over_time()",
		Start: uint64(100 * time.Second),
		End:   uint64(100000 * time.Second),
		Step:  uint64(10 * time.Second),
	}

	reqCh := make(chan pipeline.Request)
	ctx, span := tracer.Start(context.Background(), "test.caller")
	pipelineRequest := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "/", nil))
	jobMetadata := &combiner.QueryRangeJobResponse{}
	cutoff := time.Unix(1000000, 0) // well after every block so nothing is trimmed away

	go s.backendRequests(ctx, "test", pipelineRequest, searchReq, cutoff, defaultTargetBytesPerRequest, nil, reqCh, jobMetadata)

	jobCount := 0
	for range reqCh {
		jobCount++
	}
	span.End()

	require.Equal(t, n, jobCount, "one job per block (each block is smaller than TargetBytesPerRequest)")
	require.Equal(t, n, jobMetadata.TotalJobs)

	got, ok := frontendSpanByName(rec.Ended(), "test.caller")
	require.True(t, ok)

	require.LessOrEqual(t, len(got.Events()), maxAdvancementSpanEvents,
		"the backendJobsFunc fallback must respect the same 20-event cap at this scale")

	attrs := frontendAttrs(got)
	jobsTotal, ok := attrs["dispatch.jobs_total"]
	require.True(t, ok)
	assert.EqualValues(t, n, jobsTotal.AsInt64())

	_, hasSkippedOverlap := attrs["dispatch.jobs_skipped_overlap"]
	assert.False(t, hasSkippedOverlap,
		"dispatch.jobs_skipped_overlap has no meaning for the backendJobsFunc dispatch model and must not be emitted")
}

func TestExemplarsForBlock(t *testing.T) {
	s := &queryRangeSharder{}

	createBlockMeta := func(durationSeconds int) *backend.BlockMeta {
		now := time.Now()
		return &backend.BlockMeta{
			BlockID:   backend.MustParse(uuid.NewString()),
			StartTime: now.Add(-time.Duration(durationSeconds) * time.Second),
			EndTime:   now,
		}
	}

	testCases := []struct {
		name               string
		block              *backend.BlockMeta
		totalExemplars     uint32
		totalDurationNanos int64
		expectedResult     uint32
	}{
		{
			name:               "limit is zero",
			block:              createBlockMeta(60),
			totalExemplars:     0,
			totalDurationNanos: 60 * 1e9,
			expectedResult:     0,
		},
		{
			name:               "total duration is zero",
			block:              createBlockMeta(60),
			totalExemplars:     100,
			totalDurationNanos: 0,
			expectedResult:     0,
		},
		{
			name:               "single block gets all exemplars with overhead",
			block:              createBlockMeta(60),
			totalExemplars:     100,
			totalDurationNanos: 60 * 1e9,
			expectedResult:     120, // 100 * 1.2
		},
		{
			name:               "block gets proportional share - 90% of time",
			block:              createBlockMeta(90),
			totalExemplars:     100,
			totalDurationNanos: 100 * 1e9,
			expectedResult:     108, // 90/100 * 100 * 1.2 = 108
		},
		{
			name:               "block gets proportional share - 10% of time",
			block:              createBlockMeta(10),
			totalExemplars:     100,
			totalDurationNanos: 100 * 1e9,
			expectedResult:     12, // 10/100 * 100 * 1.2 = 12
		},
		{
			name:               "at least one exemplar for very small block",
			block:              createBlockMeta(1),
			totalExemplars:     10,
			totalDurationNanos: 1000 * 1e9,
			expectedResult:     1, // Very small share, but still gets 1
		},
		{
			name:               "invalid block returns zero",
			block:              createBlockMeta(-60),
			totalExemplars:     100,
			totalDurationNanos: 100 * 1e9,
			expectedResult:     0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := s.exemplarsForBlock(tc.block, tc.totalExemplars, tc.totalDurationNanos)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

func FuzzExemplarsForBlock(f *testing.F) {
	f.Add(uint32(100), uint32(60)) // limit = 100, duration = 60s
	f.Add(uint32(0), uint32(30))   // limit = 0, duration = 30s
	f.Add(uint32(1000), uint32(0)) // limit = 1000, duration = 0s

	s := &queryRangeSharder{}

	f.Fuzz(func(t *testing.T, limit uint32, value uint32) {
		now := time.Now()
		block := &backend.BlockMeta{
			BlockID:   backend.MustParse(uuid.NewString()),
			StartTime: now.Add(-time.Duration(value) * time.Second),
			EndTime:   now,
		}

		totalDurationNanos := int64(value) * 1e9
		result := s.exemplarsForBlock(block, limit, totalDurationNanos)

		if limit == 0 || value == 0 {
			assert.Equal(t, uint32(0), result, "result should be 0")
		} else {
			assert.Greater(t, result, uint32(0), "result should be greater than 0")
		}
	})
}

// nolint: gosec // G115
func TestExemplarsCutoff(t *testing.T) {
	s := &queryRangeSharder{}
	now := time.Now()
	cutoff := now.Add(-1 * time.Hour)

	testCases := []struct {
		name              string
		req               tempopb.QueryRangeRequest
		expectedBeforeCut uint32
		expectedAfterCut  uint32
	}{
		{
			// Instant queries zero exemplars before calling exemplarsCutoff; both sides must be 0.
			name: "zero exemplars (instant mode) spanning cutoff",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-20 * time.Minute).UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 0,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  0,
		},
		{
			// When all data is after the cutoff, all exemplars should go to the 'after' portion
			name: "all data after cutoff",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(50 * time.Minute).UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  100,
		},
		{
			// When all data is before the cutoff, all exemplars should go to the 'before' portion
			name: "all data before cutoff",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-2 * time.Hour).UnixNano()),
				End:       uint64(cutoff.Add(-10 * time.Minute).UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 100,
			expectedAfterCut:  0,
		},
		{
			name: "data spans the cutoff - 75% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-20 * time.Minute).UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 25,
			expectedAfterCut:  75,
		},
		{
			name: "data spans the cutoff - 25% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-3 * time.Hour).UnixNano()),
				End:       uint64(cutoff.Add(1 * time.Hour).UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 75,
			expectedAfterCut:  25,
		},
		// in case of small limits, it gives favor to after (request to generator)
		{
			name: "small limit: 25% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-3 * time.Hour).UnixNano()),
				End:       uint64(cutoff.Add(1 * time.Hour).UnixNano()),
				Exemplars: 2,
			},
			expectedBeforeCut: 1,
			expectedAfterCut:  1,
		},
		{
			name: "small limit: 25% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-3 * time.Hour).UnixNano()),
				End:       uint64(cutoff.Add(1 * time.Hour).UnixNano()),
				Exemplars: 1,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  1,
		},
		{
			name: "small limit: 75% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-20 * time.Minute).UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 2,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  2,
		},
		{
			name: "small limit: 75% after",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-20 * time.Minute).UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 1,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  1,
		},
		{
			name: "exactly at cutoff",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 0,
			expectedAfterCut:  100,
		},
		{
			name: "exactly at cutoff",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(cutoff.Add(-1 * time.Hour).UnixNano()),
				End:       uint64(cutoff.UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 100,
			expectedAfterCut:  0,
		},
		{
			name: "start equals end",
			req: tempopb.QueryRangeRequest{
				Start:     uint64(now.UnixNano()),
				End:       uint64(now.UnixNano()),
				Exemplars: 100,
			},
			expectedBeforeCut: 100,
			expectedAfterCut:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			beforeCut, afterCut := s.exemplarsCutoff(tc.req, cutoff)
			assert.Equal(t, tc.expectedBeforeCut, beforeCut, "Exemplars before cutoff should match expected value")
			assert.Equal(t, tc.expectedAfterCut, afterCut, "Exemplars after cutoff should match expected value")
		})
	}
}

// emptyVIStore is a minimal blockpack.LookupStore + blockpack.ValueIndexFileStore with no files
// at all, used only to install a genuinely-configured (non-nil) value-index query reader for
// TestMetricsQueryRangeSharder_TimeSlicedDispatch_ReachesDispatchTimeSlicedThroughRealCompile.
// An empty store is sufficient: vibuilder.BuildSource's own "at least one leaf resolved" ok
// (which CheckIndexCoverage's data-availability half relies on) is satisfied by a leaf's
// SHAPE being buildable, independent of whether any files are actually found for it (see
// tempodb/encoding/vblockpack's own coverage_decline_test.go for the same "empty store still
// reports ok=true for a shape-valid leaf" finding) — this test needs the reader configured, not
// populated with real data, since a genuine VI-write integration is already covered elsewhere
// (TestQualification_IndexCoverageMatchAllowsTimeSliced).
type emptyVIStore struct{}

func (emptyVIStore) List(context.Context, string) ([]string, error) { return nil, nil }
func (emptyVIStore) Get(context.Context, string) ([]byte, error)    { return nil, errNotFoundForTest }
func (emptyVIStore) Size(string) (int64, error)                     { return 0, errNotFoundForTest }
func (emptyVIStore) ReadAt(string, []byte, int64) (int, error)      { return 0, errNotFoundForTest }

var errNotFoundForTest = &testNotFoundError{}

type testNotFoundError struct{}

func (*testNotFoundError) Error() string { return "not found (test fake)" }

// mockReaderWithRawReader adds the tempodb.RawReaderProvider capability to mockReader (a plain
// embed, not a change to the shared mockReader type itself) so a test can exercise the REAL
// newAsyncQueryRangeSharder constructor's RawReaderProvider type-assertion path — a bare
// &queryRangeSharder{reader: &mockReader{...}} struct literal (as most of this file's existing
// tests use) bypasses that constructor entirely and leaves rawR nil forever.
type mockReaderWithRawReader struct {
	*mockReader
	rawR backend.RawReader
}

func (m *mockReaderWithRawReader) RawReader() backend.RawReader { return m.rawR }

// TestMetricsQueryRangeSharder_TimeSlicedDispatch_ReachesDispatchTimeSlicedThroughRealCompile is
// the REQUIRED end-to-end regression test for holistic-review Issue 2/B: before this fix,
// buildQueryPlan compiled every query — including every real, piped QueryRangeRequest.Query —
// with blockpack.CompileTraceQL (filter expressions only), which always errors for a real
// metrics query, so DispatchTimeSliced was unreachable in production for QueryRange. Every
// existing time-sliced metrics test hand-constructs a *blockpack.QueryPlan directly and calls
// backendRequests, bypassing RoundTrip/buildMetricsQueryPlan entirely — exactly the coverage gap
// that let Issue 2 land unnoticed. This test goes through the REAL queryRangeSharder.RoundTrip
// with a real, piped query string (`{ span.foo = "bar" } | rate()`), a real RawReaderProvider
// (local backend), and a real (test-configured) value-index reader, and asserts at least one
// dispatched job has IndexOnly=true with a window narrower than the whole query range — proof
// DispatchTimeSliced, not DispatchBlockSharded, was actually used.
func TestMetricsQueryRangeSharder_TimeSlicedDispatch_ReachesDispatchTimeSlicedThroughRealCompile(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	bm := backend.NewBlockMeta("test-tenant", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(300, 0)
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1
	bm.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	rawR, _ := newLocalRawReadWriter(t)
	reader := &mockReaderWithRawReader{
		mockReader: &mockReader{metas: []*backend.BlockMeta{bm}},
		rawR:       rawR,
	}

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder := newAsyncQueryRangeSharder(reader, o, QueryRangeSharderConfig{
		StreamingShards:    defaultMostRecentShards,
		ConcurrentRequests: 10,
	}, nil, false, newJobsPerQueryHistogram(), log.NewNopLogger())

	var (
		dispatchedMu sync.Mutex
		dispatched   []*tempopb.QueryRangeRequest
	)
	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		parsed, perr := api.ParseQueryRangeRequest(r.HTTPRequest())
		require.NoError(t, perr)
		dispatchedMu.Lock()
		dispatched = append(dispatched, parsed)
		dispatchedMu.Unlock()
		return pipeline.NewAsyncResponse(&combiner.QueryRangeJobResponse{}), nil
	})
	testRT := sharder.Wrap(next)

	httpReq := api.BuildQueryRangeRequest(httptest.NewRequest("GET", "/", nil), &tempopb.QueryRangeRequest{
		Query: `{ span.foo = "bar" } | rate()`,
		Start: uint64(100 * time.Second),
		End:   uint64(300 * time.Second),
		Step:  uint64(10 * time.Second),
	}, "")
	httpReq = httpReq.WithContext(user.InjectOrgID(httpReq.Context(), "test-tenant"))

	resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(httpReq))
	require.NoError(t, err)
	for {
		res, done, rerr := resps.Next(context.Background())
		require.NoError(t, rerr)
		if done || res == nil {
			break
		}
	}

	dispatchedMu.Lock()
	defer dispatchedMu.Unlock()

	require.NotEmpty(t, dispatched, "expected at least one dispatched backend job")

	var sawIndexOnlyNarrowedJob bool
	fullWindowNanos := uint64(300*time.Second) - uint64(100*time.Second)
	for _, d := range dispatched {
		if d.IndexOnly && (d.End-d.Start) < fullWindowNanos {
			sawIndexOnlyNarrowedJob = true
		}
	}
	require.True(t, sawIndexOnlyNarrowedJob,
		"expected at least one dispatched job with IndexOnly=true and a window narrower than the "+
			"whole query range — proof DispatchTimeSliced (not DispatchBlockSharded) was reached "+
			"through the real compile path (blockpack.CompileTraceQLMetricsFilter)")
}

// TestMetricsQueryRangeSharder_GroupByQueryStaysBlockSharded is the REQUIRED companion to the
// above (team-lead's ruling on the combined B+A fix): a group-by metrics query's filter
// predicate can be fully indexable, but the VALUE-INDEX METRICS ENGINE cannot execute a
// group-by shape at all (ExecuteTraceMetricsFromVI's own static gate, mirrored by
// blockpack.CompileTraceQLMetricsFilter's viAnswerableShape). buildMetricsQueryPlan must
// therefore return nil for a group-by query — never qualifying it for DispatchTimeSliced —
// so it dispatches with today's ordinary block-sharded jobs (IndexOnly=false, full block
// windows), never depending on the querier's inner-decline typed-error safety net (fix A) to
// keep a group-by query working. This is the frontend-side half of holistic-review Issue 1: the
// ONLY place that can keep an unsupported metrics shape on the safe, already-working path
// instead of a user-visible ErrSliceIndexCoverageGap failure.
func TestMetricsQueryRangeSharder_GroupByQueryStaysBlockSharded(t *testing.T) {
	restore := vblockpack.ConfigureValueIndexQueryForTest(emptyVIStore{}, testIndexPrefix)
	defer restore()

	bm := backend.NewBlockMeta("test-tenant", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(300, 0)
	bm.Size_ = defaultTargetBytesPerRequest
	bm.TotalRecords = 1
	bm.ReplicationFactor = backend.MetricsGeneratorReplicationFactor

	rawR, _ := newLocalRawReadWriter(t)
	reader := &mockReaderWithRawReader{
		mockReader: &mockReader{metas: []*backend.BlockMeta{bm}},
		rawR:       rawR,
	}

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder := newAsyncQueryRangeSharder(reader, o, QueryRangeSharderConfig{
		StreamingShards:    defaultMostRecentShards,
		ConcurrentRequests: 10,
	}, nil, false, newJobsPerQueryHistogram(), log.NewNopLogger())

	var (
		dispatchedMu sync.Mutex
		dispatched   []*tempopb.QueryRangeRequest
	)
	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(r pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		parsed, perr := api.ParseQueryRangeRequest(r.HTTPRequest())
		require.NoError(t, perr)
		dispatchedMu.Lock()
		dispatched = append(dispatched, parsed)
		dispatchedMu.Unlock()
		return pipeline.NewAsyncResponse(&combiner.QueryRangeJobResponse{}), nil
	})
	testRT := sharder.Wrap(next)

	httpReq := api.BuildQueryRangeRequest(httptest.NewRequest("GET", "/", nil), &tempopb.QueryRangeRequest{
		Query: `{ span.foo = "bar" } | rate() by (resource.service.name)`,
		Start: uint64(100 * time.Second),
		End:   uint64(300 * time.Second),
		Step:  uint64(10 * time.Second),
	}, "")
	httpReq = httpReq.WithContext(user.InjectOrgID(httpReq.Context(), "test-tenant"))

	resps, err := testRT.RoundTrip(pipeline.NewHTTPRequest(httpReq))
	require.NoError(t, err)
	for {
		res, done, rerr := resps.Next(context.Background())
		require.NoError(t, rerr)
		if done || res == nil {
			break
		}
	}

	dispatchedMu.Lock()
	defer dispatchedMu.Unlock()

	require.NotEmpty(t, dispatched, "expected at least one dispatched backend job")
	for _, d := range dispatched {
		require.False(t, d.IndexOnly, "a group-by metrics query must dispatch ordinary block-sharded jobs, never IndexOnly=true ones")
	}
}
