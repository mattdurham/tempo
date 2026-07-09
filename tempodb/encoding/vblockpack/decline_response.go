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
// Deliberately absent from this mapper: modules/frontend/vcnt_fetch.go's
// ErrPlanTimeLowSelectivityNoLimit. It never reaches the querier at all — it converts to a
// BadRequest directly at the frontend, PRE-DISPATCH (search_sharder.go/
// metrics_query_range_sharder.go via pipeline.NewBadRequest), so it never becomes a per-block
// error DeclineErrorToHTTPResponse would ever see. This is intentional, not a missing mapping —
// noted here so a future reader auditing "is every decline sentinel mapped?" by grepping this
// file alone doesn't flag it as a gap.
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
		// Distinct from the per-shape limitations above: this is an OPERATOR configuration
		// issue (value_index_query.enabled=false), not a property of the query itself — per
		// coder-f1's #70 fix, this must not be lumped with the per-shape sentinels.
		return http.StatusUnprocessableEntity,
			"metrics query requires the value index, which is not configured for this querier (value_index_query.enabled)",
			true

	case errors.Is(err, ErrCubeWarming):
		// R1's self-healing story: distinguishable from the permanent "shape not answerable"
		// reason — a repeat query after backfill completes should succeed.
		return http.StatusUnprocessableEntity,
			"cube not yet backfilled for this query shape/window; cube creation was triggered, retry shortly",
			true

	case errors.Is(err, ErrSearchNoCoverage):
		return http.StatusUnprocessableEntity,
			"search index has no coverage for this query and no bounded-recent-first path was authorized",
			true

	case errors.Is(err, ErrSliceIndexCoverageGap):
		return http.StatusUnprocessableEntity,
			"time-slice job's index coverage gap has no safe scan fallback for its narrowed window",
			true

	case errors.Is(err, blockpack.ErrStructuralIndexCoverageGap):
		return http.StatusUnprocessableEntity,
			"structural query's index coverage gap has no safe fallback for this job",
			true

	default:
		// Index/data inconsistency (NOTE-VI-078) or any unrecognized error: an internal
		// condition, not something the client can act on — matched=false, caller's existing
		// default-500 classification applies unchanged.
		return 0, "", false
	}
}
