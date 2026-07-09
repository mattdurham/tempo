package frontend

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/go-kit/log" //nolint:all deprecated
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/frontend/shardtracker"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/validation"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultTargetBytesPerRequest = 100 * 1024 * 1024
	defaultConcurrentRequests    = 1000
	defaultMostRecentShards      = 200
)

type SearchSharderConfig struct {
	ConcurrentRequests    int           `yaml:"concurrent_jobs,omitempty"`
	TargetBytesPerRequest int           `yaml:"target_bytes_per_job,omitempty"`
	DefaultLimit          uint32        `yaml:"default_result_limit"`
	MaxLimit              uint32        `yaml:"max_result_limit"`
	MaxDuration           time.Duration `yaml:"max_duration"`
	// QueryBackendAfter determines when to query backend storage vs ingesters only.
	QueryBackendAfter      time.Duration `yaml:"query_backend_after,omitempty"`
	IngesterShards         int           `yaml:"ingester_shards,omitempty"`
	MostRecentShards       int           `yaml:"most_recent_shards,omitempty"`
	DefaultSpansPerSpanSet uint32        `yaml:"default_spans_per_span_set,omitempty"`
	MaxSpansPerSpanSet     uint32        `yaml:"max_spans_per_span_set,omitempty"`
}

type asyncSearchSharder struct {
	next      pipeline.AsyncRoundTripper[combiner.PipelineResponse]
	reader    tempodb.Reader
	overrides overrides.Interface

	cfg                    SearchSharderConfig
	skipASTTransformations []string
	logger                 log.Logger
	jobsPerQuery           *prometheus.HistogramVec

	// rawR/indexPrefix (issue #487) back the frontend-local VCNT fetch buildQueryPlan uses to
	// build a real #487 QueryPlan at RoundTrip's backendRequests call site. Derived once, at
	// construction, from reader via the optional tempodb.RawReaderProvider capability — rawR
	// is nil whenever reader doesn't implement it (e.g. a test fake), which buildQueryPlan
	// treats as "no plan available", falling back to today's block-sharded dispatch.
	//
	// Captured once, at construction, deliberately — no live-config-reload support exists for
	// these fields today (no race: both are set once and never mutated, so concurrent
	// RoundTrip calls are safe). If tempo ever grows a live-config-reload story for
	// storage/block settings, indexPrefix could then drift from IndexPrefix()'s current value
	// independently of this sharder's captured snapshot — not actionable today, flagged so a
	// future reload path treats this as an explicit gap to close, not an implicit oversight.
	rawR        backend.RawReader
	indexPrefix string
}

// newAsyncSearchSharder creates a sharding middleware for search
func newAsyncSearchSharder(reader tempodb.Reader, o overrides.Interface, cfg SearchSharderConfig, skipASTTransformations []string, jobsPerQuery *prometheus.HistogramVec, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	var rawR backend.RawReader
	var indexPrefix string
	if rrp, ok := reader.(tempodb.RawReaderProvider); ok {
		rawR = rrp.RawReader()
		indexPrefix = rrp.IndexPrefix()
	}
	return pipeline.AsyncMiddlewareFunc[combiner.PipelineResponse](func(next pipeline.AsyncRoundTripper[combiner.PipelineResponse]) pipeline.AsyncRoundTripper[combiner.PipelineResponse] {
		return asyncSearchSharder{
			next:      next,
			reader:    reader,
			overrides: o,

			cfg:                    cfg,
			skipASTTransformations: skipASTTransformations,
			logger:                 logger,
			jobsPerQuery:           jobsPerQuery,

			rawR:        rawR,
			indexPrefix: indexPrefix,
		}
	})
}

// RoundTrip implements http.RoundTripper
// execute up to concurrentRequests simultaneously where each request scans ~targetMBsPerRequest
// until limit results are found
func (s asyncSearchSharder) RoundTrip(pipelineRequest pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
	r := pipelineRequest.HTTPRequest()

	// Use configured default (defaults to 3 if not set in config)
	// If default_spans_per_span_set=0 is explicitly configured, it means unlimited (return all matching spans)
	searchReq, err := api.ParseSearchRequestWithDefault(r, s.cfg.DefaultSpansPerSpanSet)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	searchReq.SkipASTTransformations = mergeSkipASTTransformations(s.skipASTTransformations, searchReq.SkipASTTransformations)

	// adjust limit based on config
	searchReq.Limit, err = adjustLimit(searchReq.Limit, s.cfg.DefaultLimit, s.cfg.MaxLimit)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}

	requestCtx := r.Context()
	tenantID, err := validation.ExtractValidTenantID(requestCtx)
	if err != nil {
		return pipeline.NewBadRequest(err), nil
	}
	ctx, span := tracer.Start(requestCtx, "frontend.ShardSearch")
	defer span.End()

	// calculate and enforce max search duration
	maxDuration := s.maxDuration(tenantID)
	if maxDuration != 0 && time.Duration(searchReq.End-searchReq.Start)*time.Second > maxDuration {
		return pipeline.NewBadRequest(fmt.Errorf("range specified by start and end exceeds %s. received start=%d end=%d", maxDuration, searchReq.Start, searchReq.End)), nil
	}

	// Validate SpansPerSpanSet against MaxSpansPerSpanSet
	// If MaxSpansPerSpanSet is 0, it means unlimited spans are allowed
	// If MaxSpansPerSpanSet is non-zero, enforce the limit
	if s.cfg.MaxSpansPerSpanSet != 0 && searchReq.SpansPerSpanSet > s.cfg.MaxSpansPerSpanSet {
		return pipeline.NewBadRequest(fmt.Errorf("spans per span set exceeds %d. received %d", s.cfg.MaxSpansPerSpanSet, searchReq.SpansPerSpanSet)), nil
	}

	// buffer of shards+1 allows us to insert ingestReq and metrics
	reqCh := make(chan pipeline.Request, s.cfg.IngesterShards+1)

	jobMetrics, err := s.ingesterRequests(tenantID, pipelineRequest, *searchReq, reqCh)
	if err != nil {
		return nil, err
	}

	// plan (issue #487): buildQueryPlan returns nil whenever a real plan can't be built (no
	// RawReaderProvider capability, compile failure, or nothing plannable in the query) —
	// backendRequests already treats a nil plan exactly like DispatchBlockSharded, so this is
	// byte-identical to today's dispatch whenever plan-building doesn't apply.
	//
	// Skipped entirely (holistic-review Issue 4) when searchReq.Start/End are zero: backendRequests'
	// own first check (below) is exactly this same condition ("ingester-only, no backend blocks at
	// all") and would discard any plan built here unused — building one anyway would pay for
	// CheckIndexCoverage/fetchVCNTSection for a query that can never reach backendRequests' block
	// dispatch at all.
	// planIsStructural (issue #489, plan-d.md DT1 Correction log): buildQueryPlan always returns
	// nil for a structural query (blockpack.CompileTraceQL only accepts filter expressions), so
	// buildStructuralQueryPlan is tried as a fallback when the filter-shaped attempt declines.
	// backendRequests dispatches these two plan kinds with DIFFERENT fanout models (see
	// structural_sharder.go's own package doc comment for why) — planIsStructural is the single
	// signal that tells it which one it's holding.
	var plan *blockpack.QueryPlan
	var planIsStructural bool
	// F-6 (issue #481 parts 2/3, R6): a query with NO safe plan-time answer (low VCNT
	// selectivity, no limit) must fail HERE — before any backend job is constructed — rather
	// than dispatch N per-block jobs that would each independently decline identically.
	hasLimit := searchReq.Limit > 0
	if searchReq.Start != 0 && searchReq.End != 0 {
		var planErr error
		plan, planErr = buildQueryPlan(ctx, s.rawR, tenantID, s.indexPrefix, searchReq.Query, uint64(searchReq.Start), uint64(searchReq.End), s.cfg.ConcurrentRequests, hasLimit)
		if planErr != nil {
			return pipeline.NewBadRequest(planErr), nil
		}
		if plan == nil {
			plan, planErr = buildStructuralQueryPlan(ctx, s.rawR, tenantID, s.indexPrefix, searchReq.Query, uint64(searchReq.Start), uint64(searchReq.End), s.cfg.ConcurrentRequests, hasLimit)
			if planErr != nil {
				return pipeline.NewBadRequest(planErr), nil
			}
			planIsStructural = plan != nil
		}
	}

	// pass subCtx in requests so we can cancel and exit early
	s.backendRequests(ctx, tenantID, pipelineRequest, searchReq, jobMetrics, plan, planIsStructural, reqCh, func(err error) {
		// todo: actually find a way to return this error to the user
		s.logger.Log("msg", "search: failed to build backend requests", "err", err)
	})

	s.jobsPerQuery.WithLabelValues(searchOp).Observe(float64(jobMetrics.TotalJobs))

	// execute requests
	return pipeline.NewAsyncSharderChan(ctx, s.cfg.ConcurrentRequests, reqCh, pipeline.NewAsyncResponse(jobMetrics), s.next), nil
}

// backendRequest builds backend requests to search backend blocks. backendRequest takes ownership of reqCh and closes it.
// it returns 3 int values: totalBlocks, totalBlockBytes, and estimated jobs
//
// plan (issue #487) is the time-slice job plan for this query, or nil if slice-mode wasn't
// evaluated (today, always nil — wiring a real plan into this call site is T4/#153's job).
// When plan != nil && plan.Strategy == blockpack.DispatchTimeSliced, dispatch uses
// timeSlicedJobsFunc/buildTimeSlicedBackendRequests (one job per (block, slice) pair,
// IndexOnly=true) instead of today's backendJobsFunc/buildBackendRequests (one job per
// (block, page-range) pair) — backendJobsFunc/buildBackendRequests themselves are untouched,
// so a nil or DispatchBlockSharded plan is byte-identical to today (parity-first discipline).
//
// planIsStructural (issue #489, plan-d.md DT1 Correction log, 2026-07-08 ruling) further narrows
// the DispatchTimeSliced branch: a structural query's plan uses
// structuralTimeSlicedJobsFunc/buildStructuralTimeSlicedBackendRequests (ONE job per SLICE, no
// per-block fanout) instead of timeSlicedJobsFunc/buildTimeSlicedBackendRequests — see
// structural_sharder.go's own package doc comment for why the (block, slice) model is unsound for
// this job type specifically. planIsStructural is only ever true when plan is also non-nil (RoundTrip
// only sets it alongside a successfully-built structural plan).
func (s *asyncSearchSharder) backendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, resp *combiner.SearchJobResponse, plan *blockpack.QueryPlan, planIsStructural bool, reqCh chan<- pipeline.Request, errFn func(error)) {
	// request without start or end, search only in ingester
	if searchReq.Start == 0 || searchReq.End == 0 {
		close(reqCh)
		return
	}

	// calculate duration (start and end) to search the backend blocks
	start, end := backendRange(searchReq.Start, searchReq.End, s.cfg.QueryBackendAfter)

	// no need to search backend
	if start == end {
		close(reqCh)
		return
	}

	startT := time.Unix(int64(start), 0)
	endT := time.Unix(int64(end), 0)

	blocks := blockMetasForSearch(s.reader.BlockMetas(tenantID), startT, endT, acceptAllBlocks)

	// calculate metrics to return to the caller
	resp.TotalBlocks = len(blocks)

	firstShardIdx := len(resp.Shards)

	if plan != nil && plan.Strategy == blockpack.DispatchTimeSliced {
		if planIsStructural {
			// One job per SLICE, never one job per (block, slice) pair — see
			// structuralTimeSlicedJobsFunc's own doc comment for why the (block, slice) model is
			// unsound for a structural query's index-driven path.
			blockIter := structuralTimeSlicedJobsFunc(blocks, plan.Slices, s.cfg.MostRecentShards, blockOverlapsSlice)
			var advancementPoints []advancementPoint
			blockIter(func(jobs int, sz uint64, completedThroughTime uint32) {
				resp.TotalJobs += jobs
				resp.TotalBytes += sz

				resp.Shards = append(resp.Shards, shardtracker.Shard{
					TotalJobs:               uint32(jobs),
					CompletedThroughSeconds: completedThroughTime,
				})
				// issue #493 Task 5: buffered here (the counting pass, per-shard-boundary
				// callback), never inside structuralTimeSlicedJobsFunc itself — a thin wrapping
				// closure, per R2's own plan, so the function's shared shardIterFn signature
				// (also used by timeSlicedJobsFunc/backendJobsFunc) needs no change.
				advancementPoints = append(advancementPoints, advancementPoint{jobs: jobs, bytes: sz, completedThroughSeconds: completedThroughTime})
			}, nil)
			// One candidate per SLICE for the structural dispatch model (never per (block,
			// slice) pair — see structuralTimeSlicedJobsFunc's own doc comment).
			attachDispatchSpanInfo(ctx, resp.TotalJobs, len(plan.Slices), advancementPoints)

			go func() {
				buildStructuralTimeSlicedBackendRequests(ctx, tenantID, parent, searchReq, firstShardIdx, blockIter, reqCh, errFn)
			}()
			return
		}

		// blockOverlapsSlice (team-lead's layer-(a) completion): a (block, slice) pair with no
		// time overlap dispatches no job at all, in both this count and buildTimeSlicedBackendRequests'
		// own dispatch loop — see timeSlicedJobsFunc's doc comment for why this needs no
		// TrimToBlockOverlap-style narrowing the way the metrics sharder's predicate does.
		blockIter := timeSlicedJobsFunc(blocks, plan.Slices, s.cfg.MostRecentShards, blockOverlapsSlice)
		var advancementPoints []advancementPoint
		blockIter(func(jobs int, sz uint64, completedThroughTime uint32) {
			resp.TotalJobs += jobs
			resp.TotalBytes += sz

			resp.Shards = append(resp.Shards, shardtracker.Shard{
				TotalJobs:               uint32(jobs),
				CompletedThroughSeconds: completedThroughTime,
			})
			advancementPoints = append(advancementPoints, advancementPoint{jobs: jobs, bytes: sz, completedThroughSeconds: completedThroughTime})
		}, nil)
		// One candidate per (block, slice) pair for the plain time-sliced dispatch model.
		attachDispatchSpanInfo(ctx, resp.TotalJobs, len(blocks)*len(plan.Slices), advancementPoints)

		go func() {
			buildTimeSlicedBackendRequests(ctx, tenantID, parent, searchReq, firstShardIdx, blockIter, reqCh, errFn)
		}()
		return
	}

	// DispatchBoundedRecentFirst (issue #481 part 3, F-5/F-6) falls through to here identically
	// to DispatchBlockSharded and a nil plan — this switch deliberately has no
	// `case blockpack.DispatchBoundedRecentFirst` branch. Per R17, the bounded-recent-first
	// decision NEVER crosses the frontend/querier wire: there is no QueryPlan field a per-block
	// backend request could carry it in (adding one needs protobuf generator surgery, unavailable
	// in this environment and rejected by R17), so the querier's blockpackBlock.Fetch derives
	// boundedAuthorized itself, locally, from the same hasLimit predicate this file already
	// computed to select DispatchBoundedRecentFirst in the first place (see hasLimit above and
	// tempodb/encoding/vblockpack/value_index_query.go's tryIndexFetch doc comment). The dispatch
	// shape for a DispatchBoundedRecentFirst plan is therefore ordinary block-sharded fanout —
	// only each block's own per-block decline handling changes, querier-side. A future phase
	// (tracked informally as Phase G) may want a tracing attribute or job count hint surfaced here
	// for observability, but no wire field is required for correctness.
	blockIter := backendJobsFunc(blocks, s.cfg.TargetBytesPerRequest, s.cfg.MostRecentShards, searchReq.End)
	var advancementPoints []advancementPoint
	blockIter(func(jobs int, sz uint64, completedThroughTime uint32) {
		resp.TotalJobs += jobs
		resp.TotalBytes += sz

		resp.Shards = append(resp.Shards, shardtracker.Shard{
			TotalJobs:               uint32(jobs),
			CompletedThroughSeconds: completedThroughTime,
		})
		// issue #493 Task 5 (reviewer-2 finding): this fallback path -- DispatchBlockSharded,
		// DispatchBoundedRecentFirst, and nil plan all land here -- is the MOST COMMON dispatch
		// model in production (every ordinary, non-time-sliced query), so it needs the same
		// observability as the two DispatchTimeSliced branches above, not just the new #487 path.
		advancementPoints = append(advancementPoints, advancementPoint{jobs: jobs, bytes: sz, completedThroughSeconds: completedThroughTime})
	}, nil)
	// backendJobsFunc has no overlap-filtering concept and no fixed 1-job-per-block relationship
	// (a block can page-split into many jobs) -- see attachDispatchSpanInfoNoOverlapFilter's own
	// doc comment for why dispatch.jobs_skipped_overlap is not emitted at all on this path
	// (reviewer-2 finding: reusing attachDispatchSpanInfo's subtraction formula here silently
	// clamped to a misleading 0 whenever any block page-split, which is the common case, not rare).
	attachDispatchSpanInfoNoOverlapFilter(ctx, resp.TotalJobs, advancementPoints)

	go func() {
		buildBackendRequests(ctx, tenantID, parent, searchReq, firstShardIdx, blockIter, reqCh, errFn)
	}()
}

// ingesterRequest returns a new start and end time range for the backend as well as an http request
// that covers the ingesters. If nil is returned for the http.Request then there is no ingesters query.
// since this function modifies searchReq.Start and End we are taking a value instead of a pointer to prevent it from
// unexpectedly changing the passed searchReq.
func (s *asyncSearchSharder) ingesterRequests(tenantID string, parent pipeline.Request, searchReq tempopb.SearchRequest, reqCh chan pipeline.Request) (*combiner.SearchJobResponse, error) {
	resp := &combiner.SearchJobResponse{}
	resp.Shards = make([]shardtracker.Shard, 0, s.cfg.MostRecentShards+1) // +1 for the ingester shard

	// request without start or end, search only in ingester
	if searchReq.Start == 0 || searchReq.End == 0 {
		// one shard that covers all time
		resp.TotalJobs = 1
		resp.Shards = append(resp.Shards, shardtracker.Shard{
			TotalJobs:               1,
			CompletedThroughSeconds: 1,
		})

		return resp, buildIngesterRequest(tenantID, parent, &searchReq, reqCh)
	}

	ingesterUntil := uint32(time.Now().Add(-s.cfg.QueryBackendAfter).Unix())

	// if there's no overlap between the query and ingester range just return nil
	if searchReq.End < ingesterUntil {
		return resp, nil
	}

	ingesterStart := searchReq.Start
	ingesterEnd := searchReq.End

	// adjust ingesterStart if necessary
	if ingesterStart < ingesterUntil {
		ingesterStart = ingesterUntil
	}

	// if ingester start == ingester end then we don't need to query it
	if ingesterStart == ingesterEnd {
		return resp, nil
	}

	searchReq.Start = ingesterStart
	searchReq.End = ingesterEnd

	// Split the start and end range into sub requests for each range.
	duration := searchReq.End - searchReq.Start
	interval := duration / uint32(s.cfg.IngesterShards)
	intervalMinimum := uint32(60)

	if interval < intervalMinimum {
		interval = intervalMinimum
	}

	for i := 0; i < s.cfg.IngesterShards; i++ {
		var (
			subReq     = searchReq
			shardStart = ingesterStart + uint32(i)*interval
			shardEnd   = shardStart + interval
		)

		// stop if we've gone past the end of the range
		if shardStart >= ingesterEnd {
			break
		}

		// snap shardEnd to the end of the query range
		if shardEnd >= ingesterEnd || i == s.cfg.IngesterShards-1 {
			shardEnd = ingesterEnd
		}

		subReq.Start = shardStart
		subReq.End = shardEnd

		err := buildIngesterRequest(tenantID, parent, &subReq, reqCh)
		if err != nil {
			return nil, err
		}
	}

	// add one shard that covers no time at all. this will force the combiner to wait
	//  for ingester requests to complete before moving on to the backend requests
	ingesterJobs := len(reqCh)
	resp.TotalJobs = ingesterJobs
	resp.Shards = append(resp.Shards, shardtracker.Shard{
		TotalJobs:               uint32(ingesterJobs),
		CompletedThroughSeconds: shardtracker.TimestampNever,
	})

	return resp, nil
}

// maxDuration returns the max search duration allowed for this tenant.
func (s *asyncSearchSharder) maxDuration(tenantID string) time.Duration {
	// check overrides first, if no overrides then grab from our config
	maxDuration := s.overrides.MaxSearchDuration(tenantID)
	if maxDuration != 0 {
		return maxDuration
	}

	return s.cfg.MaxDuration
}

// backendRange returns a new start/end range for the backend based on the config parameter
// query_backend_after. If the returned start == the returned end then backend querying is not necessary.
func backendRange(start, end uint32, queryBackendAfter time.Duration) (uint32, uint32) {
	now := time.Now()
	backendAfter := uint32(now.Add(-queryBackendAfter).Unix())

	// adjust start/end if necessary. if the entire query range was inside backendAfter then
	// start will == end. This signals we don't need to query the backend.
	if end > backendAfter {
		end = backendAfter
	}
	if start > backendAfter {
		start = backendAfter
	}

	return start, end
}

// buildBackendRequests returns a slice of requests that cover all blocks in the store
// that are covered by start/end.
func buildBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, firstShardIdx int, blockIter func(shardIterFn, jobIterFn), reqCh chan<- pipeline.Request, errFn func(error)) {
	defer close(reqCh)

	queryHash := hashForSearchRequest(searchReq)
	colsToJSON := api.NewDedicatedColumnsToJSON()

	blockIter(nil, func(m *backend.BlockMeta, shard, startPage, pages int) {
		blockID := m.BlockID.String()

		dedColsJSON, err := colsToJSON.JSONForDedicatedColumns(m.DedicatedColumns)
		if err != nil {
			errFn(fmt.Errorf("failed to convert dedicated columns. block: %s tempopb: %w", blockID, err))
			return
		}

		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			r, err = api.BuildSearchBlockRequest(r, &tempopb.SearchBlockRequest{
				SearchReq:     searchReq,
				BlockID:       blockID,
				StartPage:     uint32(startPage),
				PagesToSearch: uint32(pages),
				IndexPageSize: m.IndexPageSize,
				TotalRecords:  m.TotalRecords,
				Version:       m.Version,
				Size_:         m.Size_,
				FooterSize:    m.FooterSize,
				// DedicatedColumns: dc, for perf reason we pass dedicated columns json in directly to not have to realloc object -> proto -> json
			}, dedColsJSON)

			return r, err
		})
		if err != nil {
			errFn(fmt.Errorf("failed to build search block request. block: %s tempopb: %w", blockID, err))
			return
		}

		startTime := time.Unix(int64(searchReq.Start), 0)
		endTime := time.Unix(int64(searchReq.End), 0)
		key := searchJobCacheKey(tenantID, queryHash, startTime, endTime, m, startPage, pages)
		pipelineR.SetCacheKey(key)
		pipelineR.SetResponseData(firstShardIdx + shard)

		select {
		case reqCh <- pipelineR:
		case <-ctx.Done():
			// ignore the error if there is one. it will be handled elsewhere
			return
		}
	})
}

// hashForSearchRequest returns a uint64 hash of the query. if the query is invalid it returns a 0 hash.
// before hashing the query is forced into a canonical form so equivalent queries will hash to the same value.
func hashForSearchRequest(searchRequest *tempopb.SearchRequest) uint64 {
	if searchRequest.Query == "" {
		return 0
	}

	ast, err := traceql.ParseNoOptimizations(searchRequest.Query)
	if err != nil { // this should never occur. if we've made this far we've already validated the query can parse. however, for sanity, just fail to cache if we can't parse
		return 0
	}

	// forces the query into a canonical form
	query := ast.String()

	// add the query, limit and spss to the hash
	hash := fnv1a.HashString64(query)
	hash = fnv1a.AddUint64(hash, uint64(searchRequest.Limit))
	hash = fnv1a.AddUint64(hash, uint64(searchRequest.SpansPerSpanSet))
	for _, name := range searchRequest.SkipASTTransformations {
		hash = fnv1a.AddString64(hash, name)
	}

	return hash
}

// pagesPerRequest returns an integer value that indicates the number of pages
// that should be searched per query. This value is based on the target number of bytes
// 0 is returned if there is no valid answer
func pagesPerRequest(m *backend.BlockMeta, bytesPerRequest int) int {
	if m.Size_ == 0 || m.TotalRecords == 0 {
		return 0
	}
	// if the block is smaller than the bytesPerRequest, we can search the entire block
	if m.Size_ < uint64(bytesPerRequest) {
		return int(m.TotalRecords)
	}

	bytesPerPage := m.Size_ / uint64(m.TotalRecords)
	if bytesPerPage == 0 {
		return 0
	}

	pagesPerQuery := bytesPerRequest / int(bytesPerPage)
	if pagesPerQuery == 0 {
		pagesPerQuery = 1 // have to have at least 1 page per query
	}

	return pagesPerQuery
}

func buildIngesterRequest(tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, reqCh chan pipeline.Request) error {
	subR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
		return api.BuildSearchRequest(r, searchReq)
	})
	if err != nil {
		return err
	}

	subR.SetResponseData(0) // ingester requests are always shard 0
	reqCh <- subR

	return nil
}

type (
	shardIterFn func(jobs int, sz uint64, completedThroughTime uint32)
	jobIterFn   func(m *backend.BlockMeta, shard, startPage, pages int)
)

// backendJobsFunc provides an iter func with 2 callbacks designed to be used once to calculate job and shard metrics and a second time
// to generate actual jobs.
func backendJobsFunc(blocks []*backend.BlockMeta, targetBytesPerRequest int, maxShards int, end uint32) func(shardIterFn, jobIterFn) {
	blocksPerShard := len(blocks) / maxShards

	// if we have fewer blocks than shards then every shard is one block
	if blocksPerShard == 0 {
		blocksPerShard = 1
	}

	return func(shardIterCallback shardIterFn, jobIterCallback jobIterFn) {
		currentShard := 0
		jobsInShard := 0
		bytesInShard := uint64(0)
		blocksInShard := 0

		for _, b := range blocks {
			pages := pagesPerRequest(b, targetBytesPerRequest)
			jobsInBlock := 0

			if pages == 0 {
				continue
			}

			// if jobIterCallBack is nil we can skip the loop and directly calc the jobsInBlock
			if jobIterCallback == nil {
				jobsInBlock = int(b.TotalRecords) / pages
				if int(b.TotalRecords)%pages != 0 {
					jobsInBlock++
				}
			} else {
				for startPage := 0; startPage < int(b.TotalRecords); startPage += pages {
					jobIterCallback(b, currentShard, startPage, pages)
					jobsInBlock++
				}
			}

			// do we need to roll to a new shard?
			jobsInShard += jobsInBlock
			bytesInShard += b.Size_
			blocksInShard++

			// -1 b/c we will likely add a final shard below
			//  end comparison b/c there's no point in ending a shard that can't release any results
			if blocksInShard >= blocksPerShard && currentShard < maxShards-1 && b.EndTime.Unix() < int64(end) {
				if shardIterCallback != nil {
					shardIterCallback(jobsInShard, bytesInShard, uint32(b.EndTime.Unix()))
				}
				currentShard++

				jobsInShard = 0
				bytesInShard = 0
				blocksInShard = 0
			}
		}

		// final shard - note that we are overpacking the final shard due to the integer math as well as the limit of 200 shards total. if the search
		//  this is the least impactful shard to place extra jobs in as it is searched last. if we make it here the chances of this being an exhaustive search
		//  are higher
		if shardIterCallback != nil && jobsInShard > 0 {
			shardIterCallback(jobsInShard, bytesInShard, 1) // final shard can cover all time. we don't need to be precise
		}
	}
}

// sliceJobIterFn is jobIterFn's #487 time-sliced sibling: instead of a (block, page-range)
// pair it carries a (block, slice) pair, since a slice job's window is a TimeSlice, not a
// page count. It intentionally does NOT reuse jobIterFn's signature — a slice's Start/End
// (uint64 seconds) has no natural encoding as jobIterFn's startPage/pages (int) without
// overloading their meaning, and keeping the two types distinct makes the two dispatch paths
// (backendJobsFunc/buildBackendRequests vs. timeSlicedJobsFunc/buildTimeSlicedBackendRequests)
// impossible to accidentally cross-wire.
type sliceJobIterFn func(m *backend.BlockMeta, shard int, slice blockpack.TimeSlice)

// blockOverlapsSlice reports whether m's time range and slice's time range share any second.
// m.StartTime/EndTime are both inclusive (tempo's own block-metadata convention); slice.Start/End
// are half-open ([Start, End), per TimeSlice's own doc comment). Two integer-second intervals
// [a1, b1] (closed) and [a2, b2) (half-open) overlap iff a1 <= b2-1 (i.e. a1 < b2, since these are
// whole seconds) and a2 <= b1 — hence the asymmetric `<` vs `<=` below. Deliberately a plain
// bounds check with no traceql.TrimToBlockOverlap-style narrowing: search's own dispatch uses the
// slice's own bounds unmodified (see buildTimeSlicedBackendRequests), so it only needs a yes/no
// answer, not a computed intersection — a TrimToBlockOverlap-shaped predicate would introduce
// intricacy (step alignment, instant-query handling) this call site has no use for and no way to
// keep in sync with, so a second, drift-free implementation is deliberately simpler here rather
// than reusing the metrics sharder's own more precise predicate (see timeSlicedJobsFunc's doc
// comment on why each sharder gets its own).
func blockOverlapsSlice(m *backend.BlockMeta, slice blockpack.TimeSlice) bool {
	blockStart := uint64(m.StartTime.Unix()) //nolint:gosec // unix seconds, well within int64/uint64 range
	blockEnd := uint64(m.EndTime.Unix())     //nolint:gosec // unix seconds, well within int64/uint64 range
	return blockStart < slice.End && slice.Start <= blockEnd
}

// timeSlicedJobsFunc is backendJobsFunc's #487 time-sliced sibling: one job per (block,
// slice) pair — every block in the query window gets queried once per slice, since block
// boundaries and time slices are independent partitions of the same time range. It mirrors
// backendJobsFunc's shard-rolling shape (roll to a new shard every blocksPerShard blocks) but
// jobsInBlock is always len(slices) (fixed per block) rather than pagesPerRequest's
// byte-budget-derived page count. backendJobsFunc itself is untouched by this addition.
//
// overlaps (issue #487, team-lead's layer-(a) ruling on #160/#161) is the SINGLE source of
// truth for whether a (block, slice) pair is dispatched at all: a pair for which overlaps
// reports false is skipped in BOTH the job COUNT below AND the jobIterCallback dispatch itself,
// so a non-overlapping pair never reaches the caller's per-job builder — the count can no
// longer silently drift out of sync with what actually lands in reqCh (the #161 bug this design
// replaces). nil means "every (block, slice) pair counts and dispatches" (used by no caller
// today; kept as the safe default for a hypothetical future caller with nothing to filter).
// Both sharders pass a real predicate: search's blockOverlapsSlice (a plain, drift-free bounds
// intersection — search's own dispatch needs no narrower bounds than the slice's own, so a
// simple boolean is enough) and the metrics sharder's own traceql.TrimToBlockOverlap-based
// closure (metrics additionally needs the PRECISE narrowed start/end/step for the request body,
// not just a boolean, so it keeps computing that itself; its post-TrimToBlockOverlap `start >=
// end` check is now defense-in-depth only, since a non-overlapping pair never reaches it).
func timeSlicedJobsFunc(blocks []*backend.BlockMeta, slices []blockpack.TimeSlice, maxShards int, overlaps func(m *backend.BlockMeta, slice blockpack.TimeSlice) bool) func(shardIterFn, sliceJobIterFn) {
	blocksPerShard := len(blocks) / maxShards
	if blocksPerShard == 0 {
		blocksPerShard = 1
	}

	return func(shardIterCallback shardIterFn, jobIterCallback sliceJobIterFn) {
		currentShard := 0
		jobsInShard := 0
		bytesInShard := uint64(0)
		blocksInShard := 0

		for _, b := range blocks {
			if len(slices) == 0 {
				continue
			}

			overlappingJobs := 0
			for _, slice := range slices {
				if overlaps != nil && !overlaps(b, slice) {
					continue
				}
				if jobIterCallback != nil {
					jobIterCallback(b, currentShard, slice)
				}
				overlappingJobs++
			}

			jobsInShard += overlappingJobs
			bytesInShard += b.Size_
			blocksInShard++

			// -1 b/c we will likely add a final shard below
			if blocksInShard >= blocksPerShard && currentShard < maxShards-1 {
				if shardIterCallback != nil {
					shardIterCallback(jobsInShard, bytesInShard, uint32(b.EndTime.Unix()))
				}
				currentShard++

				jobsInShard = 0
				bytesInShard = 0
				blocksInShard = 0
			}
		}

		// final shard - same overpacking rationale as backendJobsFunc's own final shard.
		if shardIterCallback != nil && jobsInShard > 0 {
			shardIterCallback(jobsInShard, bytesInShard, 1) // final shard can cover all time. we don't need to be precise
		}
	}
}

// buildTimeSlicedBackendRequests is buildBackendRequests' #487 time-sliced sibling: for each
// (block, slice) pair it builds a SearchBlockRequest with IndexOnly=true and a per-job
// SearchRequest copy whose Start/End are overridden to the slice's bounds — reusing the SAME
// per-job Start/End override pattern ingesterRequests' subReq.Start/End already uses (issue
// #487 T1: no new proto field for the narrowed window, only the new IndexOnly bool).
// buildBackendRequests itself is untouched by this addition.
func buildTimeSlicedBackendRequests(ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest, firstShardIdx int, blockIter func(shardIterFn, sliceJobIterFn), reqCh chan<- pipeline.Request, errFn func(error)) {
	defer close(reqCh)

	queryHash := hashForSearchRequest(searchReq)
	colsToJSON := api.NewDedicatedColumnsToJSON()

	blockIter(nil, func(m *backend.BlockMeta, shard int, slice blockpack.TimeSlice) {
		blockID := m.BlockID.String()

		dedColsJSON, err := colsToJSON.JSONForDedicatedColumns(m.DedicatedColumns)
		if err != nil {
			errFn(fmt.Errorf("failed to convert dedicated columns. block: %s tempopb: %w", blockID, err))
			return
		}

		// Per-job SearchRequest narrowed to the slice's window — a copy, so concurrent jobs
		// for other slices/blocks never see each other's Start/End (mirrors
		// ingesterRequests' subReq pattern).
		//
		// A non-overlapping (block, slice) pair never reaches this callback at all: blockIter's
		// own overlaps predicate (blockOverlapsSlice, passed at this sharder's RoundTrip call
		// site) already filtered it out of both the job count and this dispatch loop — see
		// timeSlicedJobsFunc's doc comment. This body therefore does not need its own
		// TrimToBlockOverlap-style narrowing the way the metrics sharder's builder does: the
		// slice's own bounds are used unmodified below, since every (block, slice) pair reaching
		// here is already known to genuinely overlap the block.
		subReq := *searchReq
		subReq.Start = uint32(slice.Start)
		subReq.End = uint32(slice.End)

		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			r, err = api.BuildSearchBlockRequest(r, &tempopb.SearchBlockRequest{
				SearchReq: &subReq,
				BlockID:   blockID,
				IndexOnly: true,
				// StartPage/PagesToSearch are meaningless for an IndexOnly job (the
				// querier answers from the value index, never by page range) but
				// ParseSearchBlockRequest requires PagesToSearch > 0 — set a harmless
				// placeholder covering the whole block so a slice job's request still
				// parses cleanly through the same validation every other job uses.
				StartPage:     0,
				PagesToSearch: 1,
				IndexPageSize: m.IndexPageSize,
				TotalRecords:  m.TotalRecords,
				Version:       m.Version,
				Size_:         m.Size_,
				FooterSize:    m.FooterSize,
			}, dedColsJSON)

			return r, err
		})
		if err != nil {
			errFn(fmt.Errorf("failed to build search block request. block: %s tempopb: %w", blockID, err))
			return
		}

		// cacheKey's validity check requires [start, end) to fully encapsulate the block
		// ([StartTime, EndTime]) — using the slice-narrowed subReq.Start/End here (rather than
		// the whole, unnarrowed query window) would almost always fail that check, since a
		// slice's window is by construction narrower than or equal to the block it overlaps
		// (this is what actually happened before this fix: caching was silently disabled for
		// virtually every time-sliced job). Use searchReq's own unnarrowed bounds instead,
		// exactly like buildBackendRequests (the block-sharded path, above) and the metrics
		// sharder's own buildTimeSlicedMetricsBackendRequests both already do.
		startTime := time.Unix(int64(searchReq.Start), 0)
		endTime := time.Unix(int64(searchReq.End), 0)
		// startPage/pagesToSearch have no meaning for an IndexOnly job (there is no page
		// range); slice.Start/width are threaded through those cache-key slots instead so
		// two different slices over the same small block (one that happens to fully
		// encapsulate it) never collide on the same cache key.
		key := searchJobCacheKey(tenantID, queryHash, startTime, endTime, m, int(slice.Start), int(slice.End-slice.Start)) //nolint:gosec // slice bounds are unix seconds, well within int range
		pipelineR.SetCacheKey(key)
		pipelineR.SetResponseData(firstShardIdx + shard)

		select {
		case reqCh <- pipelineR:
		case <-ctx.Done():
			// ignore the error if there is one. it will be handled elsewhere
			return
		}
	})
}

// mergeSkipASTTransformations merges and deduplicates AST transformations skip-lists
func mergeSkipASTTransformations(a, b []string) []string {
	merged := slices.Concat(a, b)
	slices.Sort(merged)
	return slices.Compact(merged)
}
