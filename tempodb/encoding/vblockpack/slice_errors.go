package vblockpack

import "errors"

// ErrSliceIndexCoverageGap is returned by a #487 time-slice job (opts.IndexOnly / req.IndexOnly)
// when the value index would otherwise routinely decline to answer the query (no coverage for
// a leaf, an unindexable/negation predicate, a non-filter query, or the index reader being
// unconfigured). A slice job's narrowed [Start, End) window means a full block scan is not a
// safe fallback here the way it is for a normal (non-slice) query: a scan would ignore the
// slice's narrow window at the per-span level and could double-count or over-fetch across
// overlapping slice jobs dispatched for the same block. This is distinct from an index/data
// inconsistency (the index HAD coverage but named a block/page the data file cannot resolve,
// NOTE-VI-078) — that is index corruption; this is "the index architecturally cannot answer,
// and this job is not allowed to fall back."
var ErrSliceIndexCoverageGap = errors.New("vblockpack: slice-mode index coverage gap")

// ErrSearchNoCoverage (issue #481 part 3, F-7, team-lead rulings R7/R17) is a NEW tempo-local
// sentinel — deliberately NOT one of blockpack's F-4 sentinels, which are metrics-path-only
// (ExecuteMetricsTraceQL's decline contract); search's plan-time story is #62's frontend decline
// (ErrPlanTimeLowSelectivityNoLimit, vcnt_fetch.go), which lives in the FRONTEND, not the
// querier. ErrSearchNoCoverage is returned by declineOutcomeBounded when a routine
// (non-slice-job, non-vr==nil) index decline occurs WITHOUT an explicit boundedAuthorized signal
// from Fetch. Per R17, boundedAuthorized is Fetch's LOCAL derivation — true exactly when this
// query carries a limit and is not a #487 slice job, the same hasLimit predicate
// blockpack.SelectSearchStrategy applies frontend-side (F-6), re-evaluated locally rather than
// threaded over the wire (no wire path from the frontend's QueryPlan.Strategy to this per-block
// call exists, and building one would have required protobuf generator surgery unavailable in
// this environment). This is the SAFE-DEFAULT backstop for declines boundedAuthorized doesn't
// cover (slice jobs already have their own ErrSliceIndexCoverageGap; declines on a query with no
// limit; a coarse/tenant-level CheckIndexCoverage signal that doesn't hold for this specific
// block) — converting the decline directly to a hard error rather than an implicit scan, matching
// #481's central anti-pattern elimination. Joins F-10's declineErrorToHTTPResponse mapper switch
// (unlike the frontend's plan-time sentinel, this one fires querier-side and travels through the
// combiner) as a 4xx-class, actionable-message case.
var ErrSearchNoCoverage = errors.New("vblockpack: search index has no coverage")

// ErrMaterializedIndexBuilding is returned when the value-index query path is not
// configured on this querier at all (vr == nil) — a deployment-level absence, distinct
// from ErrSearchNoCoverage (configured, but this specific column/window lacks coverage).
// No supported deployment scans as a fallback for either case.
var ErrMaterializedIndexBuilding = errors.New("vblockpack: materialized index is not configured on this querier")
