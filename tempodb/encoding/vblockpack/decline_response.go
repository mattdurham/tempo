package vblockpack

// decline_response.go — F-10 (issue #481 part 3, team-lead ruling R5): a minimal typed-error-to-
// HTTP-status mapping at the tempo boundary, distinguishing (i) query-shape-not-answerable
// (actionable client-class error naming the reason: unsupported metrics shape / no index
// coverage / cube warming) from (ii) corruption-or-internal (5xx). Given the frontend combiner's
// all-or-nothing error semantics (modules/frontend/combiner/common.go: any non-200 response
// fails the WHOLE tenant-wide query), an opaque 500 on every unsupported shape is not shippable.
//
// Insertion point VERIFIED (not guessed) by tracing the actual error flow: the combiner
// (combiner/common.go's AddResponse/HTTPFinal) only ever reads an ALREADY-CONVERTED
// *http.Response's StatusCode — it never inspects the underlying Go error at all, since by the
// time a per-block response reaches the combiner, the querier's own HTTP handler has already
// converted the error into a status code. The actual conversion point is
// modules/querier/http.go's handleError(w, err) — the SINGLE function both SearchHandler
// (Fetch's errors, via q.SearchBlock) and QueryRangeHandler (QueryRange's errors, via
// q.QueryRange) funnel every non-nil error through. DeclineErrorToHTTPResponse below is called
// from there.

import (
	"errors"
	"net/http"

	"github.com/grafana/blockpack"

	"github.com/grafana/tempo/tempodb/backend"
)

// DeclineErrorToHTTPResponse maps a typed decline sentinel (from either repo — blockpack's F-4
// metrics-decline family or tempo's own search/cube sentinels) to an HTTP status code and an
// actionable message. matched=false means err is not one of the recognized decline sentinels —
// the caller (modules/querier/http.go's handleError) should fall through to its own existing
// classification (ErrTraceTooLarge, default 500) unchanged; this function never overrides an
// error it doesn't specifically recognize.
//
// (i) shape/coverage/warming — the query's SHAPE is not answerable by the index (or the answer
// is transiently unavailable, cube warming) — actionable 4xx: the client can retry with a
// different query shape, wait for backfill, or (for the config-level ErrMetricsValueIndexDisabled
// case) the operator can act. (ii) index/data inconsistency or any unrecognized error — 5xx: an
// internal condition the client cannot act on.
//
// Formerly deliberately absent from this mapper: modules/frontend/vcnt_fetch.go's
// ErrPlanTimeLowSelectivityNoLimit — it never reached the querier at all (it converted to a
// BadRequest directly at the frontend, PRE-DISPATCH), so it never became a per-block error this
// function would see. REMOVED entirely by issue #535 (team-lead ruling, reversing issue #481's
// R6): a query may only decline for a genuine index/cube coverage gap, never as a
// cost/selectivity heuristic for a query the system CAN answer correctly, and that sentinel's
// whole premise no longer holds (see queryplan.SelectSearchStrategy's doc comment, blockpack
// SPEC-QP-6). Noted here, rather than silently dropped, so a future reader auditing "is every
// decline sentinel mapped?" doesn't wonder why a prior version of this comment mentioned a
// symbol that no longer exists.
func DeclineErrorToHTTPResponse(err error) (status int, message string, matched bool) {
	switch {
	case err == nil:
		return 0, "", false

	case errors.Is(err, blockpack.ErrMetricsShapeNotAnswerable):
		return http.StatusUnprocessableEntity,
			"metrics query uses an aggregate shape the value index cannot answer (only count_over_time()/rate() without group-by are index-answerable)",
			true

	case errors.Is(err, blockpack.ErrMetricsNoCoverage):
		return http.StatusUnprocessableEntity,
			"metrics query has no value-index coverage for a leaf predicate (an unindexable or negated condition)",
			true

	case errors.Is(err, blockpack.ErrMetricsLegacyTimeSecZero):
		return http.StatusUnprocessableEntity,
			"metrics query matched a legacy block with no per-span timestamps",
			true

	case errors.Is(err, blockpack.ErrMetricsValueIndexDisabled):
		// Distinct from the per-shape limitations above: this is an OPERATOR/DEPLOYMENT
		// issue (the value index isn't reachable on this querier at all — no S3 backend
		// configured, or the S3 client failed to build), not a property of the query
		// itself — per coder-f1's #70 fix, this must not be lumped with the per-shape
		// sentinels. There is no longer an explicit operator opt-out setting for a
		// CONFIGURED backend (2026-07-11, the index-driven query path is unconditional on
		// any S3-backed target) -- this case fires only when no backend at all is
		// configured for this querier, or the S3 client failed to initialize.
		return http.StatusUnprocessableEntity,
			"metrics query requires the value index, which is not reachable on this querier (no S3 backend configured, or the S3 client failed to initialize)",
			true

	case errors.Is(err, ErrCubeWarming):
		// R1's self-healing story: distinguishable from the permanent "shape not answerable"
		// reason — a repeat query after backfill completes should succeed.
		return http.StatusUnprocessableEntity,
			"cube not yet backfilled for this query shape/window; cube creation was triggered, retry shortly",
			true

	case errors.Is(err, ErrMaterializedIndexBuilding):
		return http.StatusUnprocessableEntity,
			"search requires the materialized value index, which is not configured on this querier",
			true

	case errors.Is(err, ErrSearchNoCoverage):
		return http.StatusUnprocessableEntity,
			"search index has no coverage yet (materialized index still building for this window)",
			true

	case errors.Is(err, ErrSliceIndexCoverageGap):
		return http.StatusUnprocessableEntity,
			"time-slice job's index coverage gap has no safe scan fallback for its narrowed window",
			true

	case errors.Is(err, blockpack.ErrStructuralIndexCoverageGap):
		return http.StatusUnprocessableEntity,
			"structural query's index coverage gap has no safe fallback for this job",
			true

	case errors.Is(err, blockpack.ErrTraceByIDIndexNotConfigured):
		// Mirrors ErrMaterializedIndexBuilding's text/reason (a deployment-level absence, not a
		// per-trace coverage gap, NOTE-VI-073: there is no scan fallback) but names "trace
		// lookup" specifically, so the message stays distinct and actionable for an operator
		// diagnosing which of the two paths (search vs. trace-by-id) actually failed.
		return http.StatusUnprocessableEntity,
			"trace lookup requires the materialized value index, which is not configured on this querier",
			true

	case errors.Is(err, blockpack.ErrTraceByIDCoverageGap):
		// Mirrors ErrSearchNoCoverage's text/reason (the index is configured, but this specific
		// window lacks coverage, NOTE-VI-072) but names "trace lookup" specifically, for the
		// same distinct-message reason as ErrTraceByIDIndexNotConfigured above.
		return http.StatusUnprocessableEntity,
			"trace lookup index has no coverage yet (materialized index still building for this window)",
			true

	case errors.Is(err, backend.ErrDoesNotExist):
		// 2026-07-21: the querier's own local blocklist cache (poller-refreshed on a multi-minute
		// cycle, independently per replica) can still reference a block that blockpack's own
		// compaction-worker has already deleted via catalog_reap -- the SAME data now lives in a
		// newer, already-compacted output block that IS a current candidate, so dropping this one
		// stale reference loses nothing once the blocklist catches up (grafana/blockpack#525 is
		// the real, structural fix: source block existence from Postgres directly instead of the
		// classic poller). Tolerable exactly like ErrCubeWarming above -- a repeat query after the
		// next poll cycle should no longer even select this block.
		return http.StatusUnprocessableEntity,
			"a candidate block was already compacted away since this querier's last blocklist refresh; retry shortly",
			true

	default:
		// Index/data inconsistency (NOTE-VI-078) or any unrecognized error: an internal
		// condition, not something the client can act on — matched=false, caller's existing
		// default-500 classification applies unchanged.
		return 0, "", false
	}
}
