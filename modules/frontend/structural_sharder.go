package frontend

// structural_sharder.go — issue #489 (plan-d.md DT1 Correction log, 2026-07-08 ruling): dispatch
// for index-driven structural queries. RULED (team-lead, Option B narrow form): a structural
// query's #487 time-sliced plan dispatches EXACTLY ONE JOB PER SLICE, never one job per
// (block, slice) pair the way every other index-driven job type (filter, metrics) does today —
// see structuralTimeSlicedJobsFunc's own doc comment for why the (block, slice) model is unsound
// for this specific job type. buildQueryPlanFromProgram (vcnt_fetch.go)'s shared VCNT-fetch/
// cost-estimation tail is reused unchanged; only the DISPATCH FANOUT differs.

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
)

// buildStructuralQueryPlan is buildQueryPlan's structural sibling (mirrors buildMetricsQueryPlan's
// own precedent, vcnt_fetch.go): blockpack.CompileTraceQL always rejects a structural query
// (`{A} >> {B}`, only filter expressions are supported), so buildQueryPlan itself always returns
// nil for one — DispatchTimeSliced was unreachable for structural queries before this. Uses
// blockpack.CompileStructuralLegs (D5b) for the query's LEFT leg only — the walk anchor,
// unconditionally resolved via VI per plan-d.md D4's own design, and the SAME leg tempo's
// querier-side tryStructuralIndexFetch (vblockpack/value_index_structural_query.go) builds a
// ValueIndexSource for — and hands it to the SAME buildQueryPlanFromProgram tail buildQueryPlan/
// buildMetricsQueryPlan already share: the VCNT-fetch/qualification/cost-estimation logic itself
// needs no structural-specific behavior, only the caller-side leg-compile step differs. A nil
// left leg (match-all anchor, `{} >> {...}`) declines: there is no reasonable way to VI-discover
// "every trace" from the search index without defeating the point of the index-driven path.
func buildStructuralQueryPlan(
	ctx context.Context, rawR backend.RawReader, tenant string, dedicated backend.DedicatedColumns, query string,
	minTS, maxTS uint64, concurrentRequests int, hasLimit bool, compactedChecker compactedKeyChecker,
) (*blockpack.QueryPlan, int64, error) {
	if rawR == nil || query == "" {
		return nil, 0, nil
	}
	leftProg, _, _, ok, err := blockpack.CompileStructuralLegs(query)
	if err != nil || !ok || leftProg == nil {
		return nil, 0, nil
	}
	// boundedEligible=true: structural chains that flatten to other than exactly 2 nodes go
	// through the same plan-time classification as plain search (LowSelectivity+no-limit still
	// plan-time-declines; every other outcome falls through to the same resolvability-only
	// DispatchTimeSliced/DispatchBlockSharded gate). A structural query itself never gets ANY
	// bounded path querier-side, with or without a limit (Phase 6's asymmetry finding, made
	// permanent by Phase 7's removal of the #481-part-2 bounded-scan mechanism) — this
	// classification's only remaining effect on a structural query is the plan-time decline gate.
	return buildQueryPlanFromProgram(
		ctx, rawR, tenant, dedicated, leftProg, minTS, maxTS, concurrentRequests, true, hasLimit, compactedChecker,
	)
}

// structuralTimeSlicedJobsFunc is timeSlicedJobsFunc's structural-query sibling (issue #489,
// plan-d.md DT1 Correction log, 2026-07-08 ruling — Option B, narrow form): ONE JOB PER SLICE,
// never one job per (block, slice) pair.
//
// Why the (block, slice) model (every other index-driven job type's dispatch, including plain
// structural queries were they ever to use it) is UNSOUND here: blockpack.QueryStructuralFromIndex
// has no per-block ownership/sourceRef restriction — Option A's multi-file trace materialization
// (plan-d.md D3) means a structural match's spans can legitimately live in ANY block reachable
// through the tenant's own backend.Reader, not just the block a job happens to be dispatched
// against. It answers a query's ENTIRE [Start, End) window from the value index + TraceGroup
// index, independent of which block "carries" the HTTP request. Dispatching one job per
// (block, slice) pair — as blockOverlapsSlice/timeSlicedJobsFunc do for the filter/metrics paths,
// where a matching SPAN always lives in exactly one file and the querier's own sourceRef
// restriction (tryIndexFetch) partitions the answer cleanly across jobs — would instead have N
// blocks overlapping one slice each independently compute and return the query's FULL answer for
// that slice, not a disjoint partition of it: real duplication, not mere inefficiency (verified:
// tempo's own search combiner, traceql.anyCombiner.AddMetadata → combineSearchResults, merges by
// TraceID across job responses rather than discarding a "duplicate" one).
//
// Any ONE block whose window overlaps the slice is used purely as the job's nominal HTTP
// carrier/route target (SearchBlockRequest.BlockID) — Fetch's structural index-driven path
// (tempo's tryStructuralIndexFetch) never reads that block's own data for a structural query; it
// answers the whole slice window via VI + TraceGroup + per-span SourceRef resolution regardless
// of which block the request nominally names. A slice with ZERO overlapping blocks dispatches no
// job (nothing exists yet to search).
//
// Cross-slice-straddle verification (team-lead's requirement before this was wired, 2026-07-08):
// a single trace whose distinct match pairs straddle two different slices could in principle be
// discovered independently by both slices' jobs. Checked traceql/combine.go's actual behavior:
// combineSearchResults' spansetID keys on "by"-prefixed group attributes only, which a plain
// (non-metrics) search spanset never has — every spanset for one trace collides on the SAME
// (empty-string) key, and combineSpansets REPLACES existing.Spans with an incoming spanset only
// when it has a STRICTLY higher Matched count (`if existing.Matched >= incoming.Matched { return
// }` — a tie keeps the existing side, no replace) rather than unioning the two. This means the
// combiner can never DOUBLE-COUNT a span across two job responses for the same trace (the
// documented risk this dispatch model exists to avoid) — the only residual, PRE-EXISTING
// (not introduced or worsened by this change; identical behavior already applies to today's
// per-(block,slice) filter/metrics dispatch, and to plain multi-block search generally) risk is
// that if a trace's matches split across two slices non-trivially, only the higher-Matched-count
// side's spans survive rather than a union of both — a data-completeness question orthogonal to
// this dispatch model, not a correctness regression it introduces.
func structuralTimeSlicedJobsFunc(
	blocks []*backend.BlockMeta, slices []blockpack.TimeSlice, maxShards int,
	overlaps func(m *backend.BlockMeta, slice blockpack.TimeSlice) bool,
) func(shardIterFn, sliceJobIterFn) {
	slicesPerShard := len(slices) / maxShards
	if slicesPerShard == 0 {
		slicesPerShard = 1
	}

	return func(shardIterCallback shardIterFn, jobIterCallback sliceJobIterFn) {
		currentShard := 0
		jobsInShard := 0
		bytesInShard := uint64(0)
		slicesInShard := 0

		for _, slice := range slices {
			// issue #499: SkipDispatch is a prior, separate gate from overlaps — see
			// search_sharder.go's timeSlicedJobsFunc for the identical discipline and rationale.
			if slice.SkipDispatch {
				continue
			}
			carrier, carrierSize := firstOverlappingBlock(blocks, slice, overlaps)
			if carrier == nil {
				continue // nothing exists yet to search for this slice
			}

			if jobIterCallback != nil {
				jobIterCallback(carrier, currentShard, slice)
			}
			jobsInShard++
			bytesInShard += carrierSize
			slicesInShard++

			// -1 b/c we will likely add a final shard below
			if slicesInShard >= slicesPerShard && currentShard < maxShards-1 {
				if shardIterCallback != nil {
					shardIterCallback(jobsInShard, bytesInShard, uint32(slice.End)) //nolint:gosec // slice bounds are unix seconds
				}
				currentShard++

				jobsInShard = 0
				bytesInShard = 0
				slicesInShard = 0
			}
		}

		// final shard - same overpacking rationale as backendJobsFunc's own final shard.
		if shardIterCallback != nil && jobsInShard > 0 {
			shardIterCallback(jobsInShard, bytesInShard, 1) // final shard can cover all time. we don't need to be precise
		}
	}
}

// firstOverlappingBlock returns the first block (by input order) whose window overlaps slice,
// and its size, or (nil, 0) if none do. Single-source-of-truth for structuralTimeSlicedJobsFunc's
// carrier selection.
func firstOverlappingBlock(
	blocks []*backend.BlockMeta, slice blockpack.TimeSlice, overlaps func(m *backend.BlockMeta, slice blockpack.TimeSlice) bool,
) (*backend.BlockMeta, uint64) {
	for _, b := range blocks {
		if overlaps == nil || overlaps(b, slice) {
			return b, b.Size_
		}
	}
	return nil, 0
}

// buildStructuralTimeSlicedBackendRequests is buildTimeSlicedBackendRequests' structural sibling:
// identical request-construction shape (IndexOnly=true, per-slice Start/End override, same cache
// key convention), but blockIter (structuralTimeSlicedJobsFunc) yields at most one job per slice
// instead of one job per (block, slice) pair. buildTimeSlicedBackendRequests itself is untouched
// by this addition.
func buildStructuralTimeSlicedBackendRequests(
	ctx context.Context, tenantID string, parent pipeline.Request, searchReq *tempopb.SearchRequest,
	firstShardIdx int, blockIter func(shardIterFn, sliceJobIterFn), reqCh chan<- pipeline.Request, errFn func(error),
) {
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

		// Per-job SearchRequest narrowed to the slice's window — see
		// buildTimeSlicedBackendRequests' own doc comment for why this is a copy.
		subReq := *searchReq
		subReq.Start = uint32(slice.Start) //nolint:gosec // slice bounds are unix seconds
		subReq.End = uint32(slice.End)     //nolint:gosec // slice bounds are unix seconds

		pipelineR, err := cloneRequestforQueriers(parent, tenantID, func(r *http.Request) (*http.Request, error) {
			r, err = api.BuildSearchBlockRequest(r, &tempopb.SearchBlockRequest{
				SearchReq: &subReq,
				BlockID:   blockID,
				IndexOnly: true,
				// StartPage/PagesToSearch are meaningless for an IndexOnly job (the querier
				// answers from the value/trace-by-id index, never by page range, and never
				// even reads THIS carrier block's own data for a structural query — see this
				// file's package doc comment) but ParseSearchBlockRequest requires
				// PagesToSearch > 0 — set a harmless placeholder, mirroring
				// buildTimeSlicedBackendRequests' own convention exactly.
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
			errFn(fmt.Errorf("failed to build structural search block request. block: %s tempopb: %w", blockID, err))
			return
		}

		// Mirrors buildTimeSlicedBackendRequests' own cache-key rationale: use searchReq's
		// unnarrowed bounds (not the slice-narrowed subReq) for the encapsulation check, and
		// thread slice.Start/width through the startPage/pagesToSearch slots so two different
		// slices sharing the same carrier block never collide on the same cache key.
		startTime := time.Unix(int64(searchReq.Start), 0)
		endTime := time.Unix(int64(searchReq.End), 0)
		key := searchJobCacheKey(
			tenantID, queryHash, startTime, endTime, m, int(slice.Start), int(slice.End-slice.Start),
		) //nolint:gosec // slice bounds are unix seconds, well within int range
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
