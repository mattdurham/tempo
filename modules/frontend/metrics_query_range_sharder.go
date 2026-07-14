package frontend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/go-kit/log" //nolint:all deprecated
	"github.com/go-kit/log/level"
	"github.com/segmentio/fasthash/fnv1a"
	"go.opentelemetry.io/otel/attribute"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/frontend/shardtracker"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/querier"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/validation"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultStreamingShards = 200
)

type queryRangeSharder struct {
	next                   pipeline.AsyncRoundTripper[combiner.PipelineResponse]
	reader                 tempodb.Reader
	overrides              overrides.Interface
	cfg                    QueryRangeSharderConfig
	skipASTTransformations []string
	logger                 log.Logger
	instantMode            bool
	jobsPerQuery           *prometheus.HistogramVec

	// rawR (issue #487) backs the frontend-local VCNT fetch buildQueryPlan uses to build a
	// real #487 QueryPlan at RoundTrip's backendRequests call site. See asyncSearchSharder's
	// identical field for the full rationale.
	rawR backend.RawReader
}

type QueryRangeSharderConfig struct {
	ConcurrentRequests    int           `yaml:"concurrent_jobs,omitempty"`
	TargetBytesPerRequest int           `yaml:"target_bytes_per_job,omitempty"`
	MaxDuration           time.Duration `yaml:"max_duration"`
	// QueryBackendAfter determines when to query backend storage vs ingesters only.
	QueryBackendAfter time.Duration `yaml:"query_backend_after,omitempty"`
	Interval          time.Duration `yaml:"interval,omitempty"`
	MaxExemplars      uint32        `yaml:"max_exemplars,omitempty"`
	MaxResponseSeries int           `yaml:"max_response_series,omitempty"`
	StreamingShards   int           `yaml:"streaming_shards,omitempty"`
}

// newAsyncQueryRangeSharder creates a sharding middleware for search
func newAsyncQueryRangeSharder(reader tempodb.Reader, o overrides.Interface, cfg QueryRangeSharderConfig, skipASTTransformations []string, instantMode bool, jobsPerQuery *prometheus.HistogramVec, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	var rawR backend.RawReader
	if rrp, ok := reader.(tempodb.RawReaderProvider); ok {
		rawR = rrp.RawReader()
	}
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return queryRangeSharder{
			next:                   next,
			reader:                 reader,
			overrides:              o,
			instantMode:            instantMode,
			cfg:                    cfg,
			skipASTTransformations: skipASTTransformations,
			logger:                 logger,
			jobsPerQuery:           jobsPerQuery,

			rawR: rawR,
		}
	})
}

func (s queryRangeSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	r := pipelineRequest.HTTPRequest()
	spanName := "frontend.QueryRangeSharder.range"

	if s.instantMode {
		spanName = "frontend.QueryRangeSharder.instant"
	}

	ctx, span := tracer.Start(r.Context(), spanName)
	defer span.End()

	req, err := api.ParseQueryRangeRequest(r)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	req.SkipASTTransformations = mergeSkipASTTransformations(s.skipASTTransformations, req.SkipASTTransformations)

	expr, err := traceql.ParseNoOptimizations(req.Query)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	if expr.IsNoop() {
		// Empty response
		ch := make(chan pipeline.Request, 2)
		close(ch)
		return pipeline.NewAsyncSharderChan(ctx, s.cfg.ConcurrentRequests, ch, nil, s.next), nil
	}

	tenantID, err := validation.ExtractValidTenantID(ctx)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	if req.Step == 0 {
		return pipeline.NewBadRequest(errors.New("step must be greater than 0")), nil
	}

	// calculate and enforce max search duration
	// This is checked before alignment because we may need to read a larger
	// range internally to satisfy the query.
	maxDuration := s.maxDuration(tenantID)
	if maxDuration != 0 && time.Duration(req.End-req.Start)*time.Nanosecond > maxDuration {
		err = fmt.Errorf("metrics query time range exceeds the maximum allowed duration of %s", maxDuration)
		return pipeline.NewBadRequest(err), nil
	}

	traceql.AlignRequest(req)

	// Instant queries must not compute exemplars
	if s.instantMode {
		req.Exemplars = 0
	}

	// if a limit is being enforced, honor the request if it is less than the limit
	// else set it to max limit
	if s.cfg.MaxResponseSeries > 0 && (req.MaxSeries > uint32(s.cfg.MaxResponseSeries) || req.MaxSeries == 0) {
		req.MaxSeries = uint32(s.cfg.MaxResponseSeries)
	}

	var (
		allowUnsafe           = s.overrides.UnsafeQueryHints(tenantID)
		targetBytesPerRequest = s.jobSize(expr, allowUnsafe)
		cutoff                = time.Now().Add(-s.cfg.QueryBackendAfter)
	)

	backendExemplars, generatorExemplars := s.exemplarsCutoff(*req, cutoff)
	req.Exemplars = generatorExemplars
	generatorReq, jobMetadata := s.generatorRequest(tenantID, pipelineRequest, *req, cutoff)
	req.Exemplars = backendExemplars

	reqCh := make(chan pipeline.Request, 2) // buffer of 2 allows us to insert generatorReq and metrics
	if generatorReq != nil {
		reqCh <- generatorReq
	}

	// plan (issue #487): req.Start/End are nanoseconds; buildMetricsQueryPlan/TimeSliceOracle
	// work in unix seconds (matching TimeSlice's own minute-aligned-unix-seconds contract),
	// hence the division. buildMetricsQueryPlan (holistic-review Issue 2/B: compiles via
	// blockpack.CompileTraceQLMetricsFilter, the metrics-aware compiler a real, piped
	// QueryRangeRequest.Query needs — buildQueryPlan's filter-only CompileTraceQL always
	// rejects one) returns nil whenever a real plan can't be built (no RawReaderProvider
	// capability, compile failure, an aggregate shape the value-index metrics engine cannot
	// execute — group-by, or anything but count_over_time()/rate() — or nothing plannable) —
	// backendRequests already treats a nil plan exactly like DispatchBlockSharded.
	//
	// Skipped entirely (holistic-review Issue 4, mirroring the search sharder's own companion
	// fix) when req.Start/End are zero: backendRequests' own first check (below) is exactly
	// this same condition and would discard any plan built here unused.
	var plan *blockpack.QueryPlan
	if req.Start != 0 && req.End != 0 {
		dedicated := s.overrides.DedicatedColumns(tenantID)
		var planErr error
		var vcntBytesRead int64
		plan, vcntBytesRead, planErr = buildMetricsQueryPlan(ctx, s.rawR, tenantID, dedicated, req.Query, req.Start/uint64(time.Second), req.End/uint64(time.Second), s.cfg.ConcurrentRequests)
		if planErr != nil {
			// F-6 (issue #481 parts 2/3, R6): a resolvable-but-low-selectivity metrics query has
			// no safe answer (R2: metrics is never bounded-served) — fail HERE, at plan time,
			// rather than dispatch N per-block jobs that would each independently decline.
			return pipeline.NewBadRequest(planErr), nil
		}
		// issue #218 Phase 3: surface the frontend's plan-time VCNT fetch total on the
		// job-metadata response the combiner reads via its "metadata" callback — see
		// shardtracker.JobMetadata.VcntBytesRead's own doc comment for why 0 is expected.
		jobMetadata.VcntBytesRead = vcntBytesRead
	}
	s.backendRequests(ctx, tenantID, pipelineRequest, *req, cutoff, targetBytesPerRequest, plan, reqCh, jobMetadata)

	span.SetAttributes(attribute.Int64("totalJobs", int64(jobMetadata.TotalJobs)))
	span.SetAttributes(attribute.Int64("totalBlocks", int64(jobMetadata.TotalBlocks)))
	span.SetAttributes(attribute.Int64("totalBlockBytes", int64(jobMetadata.TotalBytes)))
	if s.jobsPerQuery != nil {
		s.jobsPerQuery.WithLabelValues(metricsOp).Observe(float64(jobMetadata.TotalJobs))
	}

	return pipeline.NewAsyncSharderChan(ctx, s.cfg.ConcurrentRequests, reqCh, pipeline.NewAsyncResponse(jobMetadata), s.next), nil
}

// exemplarsCutoff calculates how to distribute exemplars between the generator (for recent data) and
// backend blocks. It returns two values: the number of exemplars for blocks before the cutoff time,
// and the number of exemplars for data after the cutoff time. The distribution is proportional to
// the time range of each segment relative to the total query time range.
func (s *queryRangeSharder) exemplarsCutoff(req tempopb.QueryRangeRequest, cutoff time.Time) (uint32, uint32) {
	timeRange := req.End - req.Start
	limit := req.Exemplars
	traceql.TrimToAfter(&req, cutoff)

	if req.Start >= req.End { // no need to query generator
		return limit, 0 // after - no exemplars needed
	}
	if req.End-req.Start >= timeRange { // no need to query backend
		return 0, limit
	}

	shareAfterCutoff := float64(limit) * float64(req.End-req.Start) / float64(timeRange)
	shareAfterCutoffCeil := uint32(math.Ceil(shareAfterCutoff))
	if limit <= shareAfterCutoffCeil {
		return 0, limit // after - receives all exemplars
	}
	return limit - shareAfterCutoffCeil, shareAfterCutoffCeil
}

// backendRequests builds backend metrics requests. plan (issue #487) is the time-slice job
// plan for this query, or nil if slice-mode wasn't evaluated (today, always nil — wiring a
// real plan into this call site is T4/#153's job). When plan != nil && plan.Strategy ==
// blockpack.DispatchTimeSliced, dispatch uses timeSlicedJobsFunc/
// buildTimeSlicedMetricsBackendRequests (one job per (block, slice) pair, IndexOnly=true)
// instead of today's backendJobsFunc/buildBackendRequests (one job per (block, page-range)
// pair) — both existing functions are untouched, so a nil or DispatchBlockSharded plan is
// byte-identical to today (parity-first discipline).
func (s *queryRangeSharder) backendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq tempopb.QueryRangeRequest, cutoff time.Time, targetBytesPerRequest int, plan *blockpack.QueryPlan, reqCh chan pipeline.Request, jobMetadata *combiner.QueryRangeJobResponse) {
	// request without start or end, search only in generator
	if searchReq.Start == 0 || searchReq.End == 0 {
		close(reqCh)
		return
	}

	// Make a copy and limit to backend time range.
	// Preserve instant nature of request if needed
	// Don't realign the request, preserve the range for the blocks overlapping the cutoff.
	backendReq := searchReq
	traceql.TrimToBefore(&backendReq, cutoff)

	// If empty window then no need to search backend
	if backendReq.Start == backendReq.End {
		close(reqCh)
		return
	}

	// Blocks within overall time range. This is just for instrumentation, more precise time
	// range is checked for each window.
	start := time.Unix(0, int64(backendReq.Start))
	end := time.Unix(0, int64(backendReq.End))
	blocks := blockMetasForSearch(s.reader.BlockMetas(tenantID), start, end, func(m *backend.BlockMeta) bool {
		return m.ReplicationFactor == backend.MetricsGeneratorReplicationFactor
	})
	if len(blocks) == 0 {
		// no need to search backend
		close(reqCh)
		return
	}

	// calculate metrics to return to the caller
	jobMetadata.TotalBlocks = len(blocks)

	// Calculate total duration across all blocks for exemplar distribution
	var totalDurationNanos int64
	for _, b := range blocks {
		if !b.EndTime.Before(b.StartTime) {
			totalDurationNanos += b.EndTime.UnixNano() - b.StartTime.UnixNano()
		}
	}

	// Create function to calculate exemplars per block
	getExemplarsForBlock := func(m *backend.BlockMeta) uint32 {
		return s.exemplarsForBlock(m, searchReq.Exemplars, totalDurationNanos)
	}

	// Group blocks into shards
	maxShards := s.cfg.StreamingShards
	if maxShards <= 0 {
		maxShards = defaultStreamingShards
	}

	firstShardIdx := len(jobMetadata.Shards)

	if plan != nil && plan.Strategy == blockpack.DispatchTimeSliced {
		// overlaps must agree exactly with buildTimeSlicedMetricsBackendRequests' own skip
		// check (fix for #161: TotalJobs was overcounting relative to what actually got
		// dispatched) — reusing the SAME traceql.TrimToBlockOverlap call on the SAME
		// slice-narrowed request copy, rather than a second, potentially-drifting heuristic.
		overlaps := func(m *backend.BlockMeta, slice blockpack.TimeSlice) bool {
			sliceReq := backendReq
			sliceReq.Start = slice.Start * uint64(time.Second)
			sliceReq.End = slice.End * uint64(time.Second)
			start, end, step := traceql.TrimToBlockOverlap(&sliceReq, m.StartTime, m.EndTime)
			return start < end && step != 0
		}
		blockIter := timeSlicedJobsFunc(blocks, plan.Slices, maxShards, overlaps)
		var advancementPoints []advancementPoint
		blockIter(func(jobs int, sz uint64, completedThroughTime uint32) {
			jobMetadata.TotalJobs += jobs
			jobMetadata.TotalBytes += sz

			jobMetadata.Shards = append(jobMetadata.Shards, shardtracker.Shard{
				TotalJobs:               uint32(jobs),
				CompletedThroughSeconds: completedThroughTime,
			})
			// issue #493 Task 5: see search_sharder.go's identical wrapping for the full
			// rationale (thin closure around the counting-pass callback, no
			// timeSlicedJobsFunc signature change).
			advancementPoints = append(advancementPoints, advancementPoint{jobs: jobs, bytes: sz, completedThroughSeconds: completedThroughTime})
		}, nil)
		attachDispatchSpanInfo(ctx, jobMetadata.TotalJobs, len(blocks)*len(plan.Slices), len(blocks)*countSkipDispatchSlices(plan.Slices), advancementPoints)

		go func() {
			s.buildTimeSlicedMetricsBackendRequests(ctx, tenantID, parent, backendReq, firstShardIdx, blockIter, reqCh, getExemplarsForBlock)
		}()
		return
	}

	// This fallthrough only ever sees DispatchBlockSharded or a nil plan here.
	blockIter := backendJobsFunc(blocks, targetBytesPerRequest, maxShards, uint32(time.Unix(0, int64(searchReq.End)).Unix()))
	var advancementPoints []advancementPoint
	blockIter(func(jobs int, sz uint64, completedThroughTime uint32) {
		jobMetadata.TotalJobs += jobs
		jobMetadata.TotalBytes += sz

		jobMetadata.Shards = append(jobMetadata.Shards, shardtracker.Shard{
			TotalJobs:               uint32(jobs),
			CompletedThroughSeconds: completedThroughTime,
		})
		// issue #493 Task 5 (reviewer-2 finding): see search_sharder.go's identical fallback
		// wrapping for the full rationale -- this is the most common dispatch model in
		// production and needs the same observability as the DispatchTimeSliced branch above.
		advancementPoints = append(advancementPoints, advancementPoint{jobs: jobs, bytes: sz, completedThroughSeconds: completedThroughTime})
	}, nil)
	// backendJobsFunc has no overlap-filtering concept -- see search_sharder.go's identical
	// fallback wrapping (and attachDispatchSpanInfoNoOverlapFilter's own doc comment) for why
	// dispatch.jobs_skipped_overlap is not emitted at all on this path.
	attachDispatchSpanInfoNoOverlapFilter(ctx, jobMetadata.TotalJobs, advancementPoints)

	go func() {
		s.buildBackendRequests(ctx, tenantID, parent, backendReq, firstShardIdx, blockIter, reqCh, getExemplarsForBlock)
	}()
}

func (s *queryRangeSharder) buildBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq tempopb.QueryRangeRequest, firstShardIdx int, blockIter func(shardIterFn, jobIterFn), reqCh chan<- pipeline.Request, getExemplarsForBlock func(*backend.BlockMeta) uint32) {
	defer close(reqCh)

	queryHash := hashForQueryRangeRequest(&searchReq)
	colsToJSON := api.NewDedicatedColumnsToJSON()

	blockIter(nil, func(m *backend.BlockMeta, shard, startPage, pages int) {
		dedColsJSON, err := colsToJSON.JSONForDedicatedColumns(m.DedicatedColumns)
		if err != nil {
			_ = level.Error(s.logger).Log("msg", "failed to convert dedicated columns in query range sharder. skipping", "block", m.BlockID, "err", err)
			return
		}

		// Trim and align the request for this block. I.e. if the request is "Last Hour" we don't want to
		// cache the response for that, we want only the few minutes time range for this block. This has
		// size savings but the main thing is that the response is reuseable for any overlapping query.
		start, end, step := traceql.TrimToBlockOverlap(&searchReq, m.StartTime, m.EndTime)
		if start == end || step == 0 {
			level.Warn(s.logger).Log("msg", "invalid start/step end. skipping", "start", start, "end", end, "step", step, "blockStart", m.StartTime.UnixNano(), "blockEnd", m.EndTime.UnixNano())
			return
		}

		// Calculate exemplars for this specific request
		exemplars := getExemplarsForBlock(m)
		if exemplars > 0 {
			// Scale the number of exemplars per block to match the size
			// of each sub request on this block. For very small blocks or other edge cases, return at least 1.
			exemplars = max(uint32(float64(exemplars)*float64(pages)/float64(m.TotalRecords)), 1)
		}

		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			queryRangeReq := &tempopb.QueryRangeRequest{
				Query:     searchReq.Query,
				Start:     start,
				End:       end,
				Step:      step,
				QueryMode: searchReq.QueryMode,
				// New RF1 fields
				BlockID:       m.BlockID.String(),
				StartPage:     uint32(startPage),
				PagesToSearch: uint32(pages),
				Version:       m.Version,
				Size_:         m.Size_,
				FooterSize:    m.FooterSize,
				// DedicatedColumns: dc, for perf reason we pass dedicated columns json in directly to not have to realloc object -> proto -> json
				Exemplars:              exemplars,
				MaxSeries:              searchReq.MaxSeries,
				XInstant:               searchReq.XInstant,
				SkipASTTransformations: searchReq.SkipASTTransformations,
			}

			return api.BuildQueryRangeRequest(r, queryRangeReq, dedColsJSON), nil
		})
		if err != nil {
			_ = level.Error(s.logger).Log("msg", "failed to cloneRequestForQuerirs in the query range sharder. skipping", "block", m.BlockID, "err", err)
			return
		}

		startTime := time.Unix(0, int64(searchReq.Start)) // start/end are in nanoseconds
		endTime := time.Unix(0, int64(searchReq.End))
		// TODO: Handle sampling rate
		key := queryRangeCacheKey(tenantID, queryHash, startTime, endTime, m, startPage, pages)
		if len(key) > 0 {
			pipelineR.SetCacheKey(key)
		}

		// Set which shard this request belongs to
		pipelineR.SetResponseData(shard + firstShardIdx)

		select {
		case reqCh <- pipelineR:
		case <-ctx.Done():
			return
		}
	})
}

// buildTimeSlicedMetricsBackendRequests is s.buildBackendRequests' #487 time-sliced sibling:
// for each (block, slice) pair it builds a QueryRangeRequest with IndexOnly=true whose
// Start/End/Step are the slice's window intersected with the block's own overlap — the SAME
// traceql.TrimToBlockOverlap narrowing s.buildBackendRequests already applies, just fed a
// per-job request copy narrowed to the slice first, rather than the whole query's window (no
// new proto field for the narrowed window, per issue #487 T1; only the new IndexOnly bool).
// s.buildBackendRequests itself is untouched by this addition.
func (s *queryRangeSharder) buildTimeSlicedMetricsBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq tempopb.QueryRangeRequest, firstShardIdx int, blockIter func(shardIterFn, sliceJobIterFn), reqCh chan<- pipeline.Request, getExemplarsForBlock func(*backend.BlockMeta) uint32) {
	defer close(reqCh)

	queryHash := hashForQueryRangeRequest(&searchReq)
	colsToJSON := api.NewDedicatedColumnsToJSON()

	blockIter(nil, func(m *backend.BlockMeta, shard int, slice blockpack.TimeSlice) {
		dedColsJSON, err := colsToJSON.JSONForDedicatedColumns(m.DedicatedColumns)
		if err != nil {
			_ = level.Error(s.logger).Log("msg", "failed to convert dedicated columns in query range sharder. skipping", "block", m.BlockID, "err", err)
			return
		}

		// Narrow to the slice's window (seconds -> nanoseconds) before intersecting with
		// the block's own overlap — the same TrimToBlockOverlap call s.buildBackendRequests
		// makes, just fed the slice's window instead of the whole query's window.
		sliceReq := searchReq
		sliceReq.Start = slice.Start * uint64(time.Second)
		sliceReq.End = slice.End * uint64(time.Second)
		start, end, step := traceql.TrimToBlockOverlap(&sliceReq, m.StartTime, m.EndTime)
		// start > end (not merely start == end) is the genuinely common case here: blocks are
		// selected because they overlap the OVERALL query window, slices subdivide that same
		// window into narrower sub-windows, so a (block, slice) pair with zero time overlap is
		// routine for a multi-block, multi-slice query, not a rare edge case. TrimToBlockOverlap
		// returns an inverted range (start > end) for a disjoint pair — `>=` catches both the
		// exact-equality and inverted shapes; `==` alone would let an inverted range through.
		if start >= end || step == 0 {
			level.Warn(s.logger).Log("msg", "invalid start/step end for time-sliced job. skipping", "start", start, "end", end, "step", step, "blockStart", m.StartTime.UnixNano(), "blockEnd", m.EndTime.UnixNano())
			return
		}

		exemplars := getExemplarsForBlock(m)

		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			queryRangeReq := &tempopb.QueryRangeRequest{
				Query:                  searchReq.Query,
				Start:                  start,
				End:                    end,
				Step:                   step,
				QueryMode:              searchReq.QueryMode,
				BlockID:                m.BlockID.String(),
				IndexOnly:              true,
				Version:                m.Version,
				Size_:                  m.Size_,
				FooterSize:             m.FooterSize,
				Exemplars:              exemplars,
				MaxSeries:              searchReq.MaxSeries,
				XInstant:               searchReq.XInstant,
				SkipASTTransformations: searchReq.SkipASTTransformations,
			}

			return api.BuildQueryRangeRequest(r, queryRangeReq, dedColsJSON), nil
		})
		if err != nil {
			_ = level.Error(s.logger).Log("msg", "failed to cloneRequestForQuerirs in the query range sharder. skipping", "block", m.BlockID, "err", err)
			return
		}

		startTime := time.Unix(0, int64(searchReq.Start)) // start/end are in nanoseconds
		endTime := time.Unix(0, int64(searchReq.End))
		// startPage/pagesToSearch have no meaning for an IndexOnly job; slice.Start/width
		// are threaded through those cache-key slots instead so two different slices over
		// the same small block never collide on the same cache key.
		key := queryRangeCacheKey(tenantID, queryHash, startTime, endTime, m, int(slice.Start), int(slice.End-slice.Start)) //nolint:gosec // slice bounds are unix seconds, well within int range
		if len(key) > 0 {
			pipelineR.SetCacheKey(key)
		}

		// Set which shard this request belongs to
		pipelineR.SetResponseData(shard + firstShardIdx)

		select {
		case reqCh <- pipelineR:
		case <-ctx.Done():
			return
		}
	})
}

func (s *queryRangeSharder) generatorRequest(tenantID string, parent pipeline.Request, searchReq tempopb.QueryRangeRequest, cutoff time.Time) (pipeline.Request, *combiner.QueryRangeJobResponse) {
	jobMetadata := &combiner.QueryRangeJobResponse{}

	// Trim the time range to only the recent which is covered by the generators.
	// Important - don't align the request after trimming. We always need to ensure
	// the start/end time range sent to the generators is accurate.
	traceql.TrimToAfter(&searchReq, cutoff)

	// if start == end then we don't need to query it
	if searchReq.Start == searchReq.End {
		return nil, jobMetadata
	}

	searchReq.QueryMode = querier.QueryModeRecent

	subR, _ := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
		return api.BuildQueryRangeRequest(r, &searchReq, ""), nil
	})

	// Add shard metadata for the generator request, similar to ingesterRequests
	// The generator covers the most recent data, so it completes through MaxUint32
	jobMetadata.TotalJobs = 1
	jobMetadata.Shards = append(jobMetadata.Shards, shardtracker.Shard{
		TotalJobs:               1,
		CompletedThroughSeconds: shardtracker.TimestampNever,
	})

	subR.SetResponseData(0) // generator requests are always shard 0

	return subR, jobMetadata
}

// maxDuration returns the max search duration allowed for this tenant.
func (s *queryRangeSharder) maxDuration(tenantID string) time.Duration {
	// check overrides first, if no overrides then grab from our config
	maxDuration := s.overrides.MaxMetricsDuration(tenantID)
	if maxDuration != 0 {
		return maxDuration
	}

	return s.cfg.MaxDuration
}

func (s *queryRangeSharder) jobSize(expr *traceql.RootExpr, allowUnsafe bool) int {
	// If we have a query hint then use it
	if v, ok := expr.Hints.GetInt(traceql.HintJobSize, allowUnsafe); ok && v > 0 {
		return v
	}

	// Else use configured value.
	size := s.cfg.TargetBytesPerRequest

	return size
}

// exemplarsForBlock calculates exemplars for a single block based on its proportional duration.
// Example: if a block is 90s out of 100s total, with limit=100, it gets 90*1.2=108 exemplars.
func (s *queryRangeSharder) exemplarsForBlock(m *backend.BlockMeta, totalExemplars uint32, totalDurationNanos int64) uint32 {
	const overhead = 1.2 // 20% overhead for shard size

	if totalExemplars == 0 || totalDurationNanos <= 0 {
		return 0
	}

	if m.EndTime.Before(m.StartTime) { // Skip blocks with invalid time ranges
		return 0
	}

	blockDuration := m.EndTime.UnixNano() - m.StartTime.UnixNano()
	share := (float64(blockDuration) / float64(totalDurationNanos)) * float64(totalExemplars) * overhead
	return max(uint32(math.Ceil(share)), 1)
}

func hashForQueryRangeRequest(req *tempopb.QueryRangeRequest) uint64 {
	if req.Query == "" {
		return 0
	}

	ast, err := traceql.ParseNoOptimizations(req.Query)
	if err != nil { // this should never occur. if we've made this far we've already validated the query can parse. however, for sanity, just fail to cache if we can't parse
		return 0
	}

	// forces the query into a canonical form
	query := ast.String()

	// add the query and other fields that change the response to the hash
	hash := fnv1a.HashString64(query)
	hash = fnv1a.AddUint64(hash, req.Step)
	hash = fnv1a.AddUint64(hash, uint64(req.MaxSeries))
	hash = fnv1a.AddUint64(hash, uint64(req.Exemplars))

	// TODO: once we have IN/NOT IN syntax in TraceQL, we should pass down the optimized query with the
	//       request and remove req.SkipASTTransformations entirely and skip this step
	for _, name := range req.SkipASTTransformations {
		hash = fnv1a.AddString64(hash, name)
	}

	return hash
}
