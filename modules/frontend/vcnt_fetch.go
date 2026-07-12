package frontend

// vcnt_fetch.go — frontend-local VCNT section fetch (issue #487, T3/#153). Thin, cache-free
// (team-lead ruling: plan-time, once-per-query frequency does not justify caching; a cache is
// a measurement-driven follow-up if warranted) fetch of the .vcnt objects covering a set of
// columns, built entirely from public blockpack primitives (VCNTColHash,
// VCNTBuildSectionFromObjects) and tempodb's own backend.RawReader — reusing the exact
// already-configured backend the rest of tempodb uses, no new object-store client, no new
// config, and deliberately NOT extracted into a shared package with the querier's own
// vblockpack.buildVCNTSection (different lifecycle: querier-side is a long-lived
// per-process cache serving many concurrent block-jobs; this runs once per query at plan
// time).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

// ErrPlanTimeLowSelectivityNoLimit (issue #481 parts 2/3, F-6, team-lead ruling R6) is returned
// by buildQueryPlanFromProgram when a query is resolvable but has NO safe answer at plan time:
// its VCNT-classified selectivity is low (would match most of the column's live values) and
// either (a) it's a search query with no limit, or (b) it's a metrics query (never bounded-
// served, R2) — in both cases every per-block job dispatched would decline identically, so per
// R6 the frontend fails HERE, at plan time, rather than fanning out N certain-to-decline
// queries. This is deliberately NOT one of blockpack's F-4 querier-side decline sentinels
// (ErrMetricsShapeNotAnswerable et al.) — those fire from Fetch/QueryRange per-block, after
// dispatch; this fires from RoundTrip BEFORE any block job is ever constructed, so it never
// reaches the querier, the combiner, or F-10's declineErrorToHTTPResponse mapper. Callers
// (search_sharder.go, metrics_query_range_sharder.go) convert this directly to
// pipeline.NewBadRequest — a plan-time failure IS "failing in the frontend," no combiner
// round-trip required.
var ErrPlanTimeLowSelectivityNoLimit = errors.New(
	"query has low value-index selectivity with no bounded answer available: " +
		"a full per-block scan would be required to answer it correctly, which is not attempted",
)

// fetchVCNTSection lists and downloads the .vcnt files covering each of dims (candidate
// leaf column names) for tenant, using rawR's generic Find+Read. It merges them into one
// consolidated VCNT section via blockpack.VCNTBuildSectionFromObjects — the same shape
// vblockpack's cube_backfill.go:buildVCNTSection produces for the querier's own cardinality
// gate (issue #483), just fetched through tempodb's RawReader instead of a bespoke minio
// client.
//
// On any error, or when rawR is nil (no RawReaderProvider capability available — e.g. a
// backend that doesn't yet implement it, or VCNT simply hasn't been written for this
// tenant/column), it returns a nil/empty section. The caller MUST treat that as "no VCNT
// signal", never as an error that blocks the query — mirrors buildVCNTSection's own
// no-block-on-VCNT-failure contract and blockpack.TimeSliceOracle's own tolerance for an
// empty section (its CostFunc returns UnknownCost() for every leaf, and BuildTimeSlices
// falls back to its uniform-width no-signal path).
//
// Uses Find, not List: List's contract on every tempodb backend is a directory/common-prefix
// listing (local's implementation filters to directories only; S3's uses a "/" delimiter,
// returning CommonPrefixes only) — neither ever returns a leaf-level file at the queried
// keypath. Find recursively walks and reports every actual file.
//
// filesCount/bytesRead (issue #493 Task 4c) report the number of .vcnt objects actually
// downloaded and their total byte size — already-computed local state (len(objects) and each
// object's len(data)), now returned so the caller can attach them to the "frontend.vcntFetch"
// child span it wraps this call in, without this function needing any tracing awareness of its
// own.
func fetchVCNTSection(
	ctx context.Context, rawR backend.RawReader, tenant string, dims []string,
	minTS, maxTS uint64,
) (data []byte, dir []blockpack.VCNTChunkDirEntry, filesCount int, bytesRead int64) {
	if rawR == nil || len(dims) == 0 {
		return nil, nil, 0, 0
	}

	var objects [][]byte
	for _, dim := range dims {
		colHash := blockpack.VCNTColHash(dim)
		prefix := backend.KeyPath{tenant, "value_counts", colHash}

		var keys []string
		// Find errors (e.g. the prefix doesn't exist yet for this column) are not fatal —
		// they simply mean no VCNT coverage for this dim, same as an empty listing.
		_ = rawR.Find(ctx, prefix, func(m backend.FindMatch) {
			if path.Ext(m.Key) == ".vcnt" {
				keys = append(keys, m.Key)
			}
		})

		for _, k := range keys {
			keypath, name := splitObjectKey(k)
			if !vblockpack.VCNTFileOverlapsRange(name, minTS, maxTS) {
				continue
			}
			rc, _, err := rawR.Read(ctx, name, keypath, nil)
			if err != nil {
				continue
			}
			objData, readErr := io.ReadAll(rc)
			_ = rc.Close()
			if readErr != nil || len(objData) == 0 {
				continue
			}
			objects = append(objects, objData)
			filesCount++
			bytesRead += int64(len(objData))
		}
	}
	if len(objects) == 0 {
		return nil, nil, filesCount, bytesRead
	}
	data, dir, _ = blockpack.VCNTBuildSectionFromObjects(objects)
	return data, dir, filesCount, bytesRead
}

// splitObjectKey splits a Find-reported full key (tenant/.../file) into the KeyPath (every
// segment but the last) and name (the last segment) backend.RawReader.Read expects — Find
// reports one combined path string, but Read takes the directory and filename separately.
func splitObjectKey(fullKey string) (backend.KeyPath, string) {
	parts := strings.Split(fullKey, "/")
	if len(parts) == 0 {
		return nil, fullKey
	}
	return backend.KeyPath(parts[:len(parts)-1]), parts[len(parts)-1]
}

// buildQueryPlan compiles query, fetches VCNT signal for its candidate columns via rawR, and
// composes the #487 blockpack.QueryPlan the caller passes into backendRequests — the shared
// call-site logic both sharders' RoundTrip use (search's minTS/maxTS are already unix
// seconds; the metrics sharder must convert its own nanosecond Start/End before calling this).
//
// Returns nil whenever a real plan cannot be built (rawR is nil — no RawReaderProvider
// capability on this deployment's backend; query fails to compile; or query has nothing
// plannable) — the caller MUST treat a nil plan exactly like DispatchBlockSharded (pass it
// straight through to backendRequests, which already falls back to today's per-block dispatch
// for a nil plan). Compile failure here is never a hard error: RoundTrip already re-compiles
// (or otherwise validates) the query for its own purposes downstream and will surface a real
// parse error to the caller through that path — this call site only feeds an optional planning
// optimization, so it must never itself reject a query the rest of the pipeline accepts.
//
// allLeavesResolvable (issue #481, T5/#155, tightened T5b/#162) reuses vblockpack.
// CheckIndexCoverage — the SAME singleton value-index reader and blockpack.BuildValueIndexSource
// call tryIndexFetch's own first decline gate uses, ANDed with blockpack.AllLeavesIndexable so a
// mixed indexable/non-indexable-shape query cannot slip through — rather than a second,
// independently-invented coverage check. See CheckIndexCoverage's own doc comment for the exact
// composition and its one remaining, structural limitation (tryIndexFetch's SECOND decline gate,
// which enforces per-query completeness against one block's real data, needs a *blockpack.Reader
// and is unreachable from this block-independent plan-time call site).
func buildQueryPlan(
	ctx context.Context, rawR backend.RawReader, tenant string, dedicated backend.DedicatedColumns, query string,
	minTS, maxTS uint64, concurrentRequests int, hasLimit bool,
) (*blockpack.QueryPlan, error) {
	if rawR == nil || query == "" {
		return nil, nil
	}
	prog, err := blockpack.CompileTraceQL(query, blockpack.QueryOptions{})
	if err != nil || prog == nil {
		return nil, nil
	}
	return buildQueryPlanFromProgram(ctx, rawR, tenant, dedicated, prog, minTS, maxTS, concurrentRequests, true, hasLimit)
}

// buildMetricsQueryPlan is buildQueryPlan's metrics sibling (holistic-review Issue 2/B): a real
// QueryRangeRequest.Query always carries an aggregation pipeline (`{...} | rate()`), which
// blockpack.CompileTraceQL — filter expressions only — always rejects, so buildQueryPlan itself
// returned nil for every production metrics query and DispatchTimeSliced was unreachable for
// QueryRange. blockpack.CompileTraceQLMetricsFilter compiles the metrics query and hands back its
// filter-predicate *Program (the same shape BuildQueryPlan/AllLeavesIndexable already consume),
// plus viAnswerableShape: whether the query's aggregate SHAPE (function + group-by) is one the
// value-index metrics engine can execute at all (holistic-review Issue 1's frontend-side half —
// see this function's own use of it below).
//
// viAnswerableShape is ANDed into the qualification the same way allLeavesResolvable is: a
// group-by or non-count/rate metrics query must NOT qualify for DispatchTimeSliced even when its
// filter predicate is otherwise fully indexable, because the querier's value-index metrics
// engine (ExecuteTraceMetricsFromVI) cannot execute that shape at all — dispatching IndexOnly=true
// jobs for it would depend entirely on the querier's own inner-decline typed-error safety net
// (holistic-review Issue 1/fix A) rather than never generating that dispatch in the first place.
// Both gates failing closed independently (frontend qualification AND querier inner decline) is
// deliberate defense in depth, not redundant: this frontend-side check is the ONLY one who can
// keep an unsupported metrics shape on the safe, working block-sharded path instead of it
// depending on a typed error the user would otherwise see.
func buildMetricsQueryPlan(
	ctx context.Context, rawR backend.RawReader, tenant string, dedicated backend.DedicatedColumns, query string,
	minTS, maxTS uint64, concurrentRequests int,
) (*blockpack.QueryPlan, error) {
	if rawR == nil || query == "" {
		return nil, nil
	}
	prog, viAnswerableShape, err := blockpack.CompileTraceQLMetricsFilter(query)
	if err != nil || prog == nil || !viAnswerableShape {
		return nil, nil
	}
	// boundedEligible=false (R2: metrics is never bounded-served); hasLimit is irrelevant on
	// this path and unused by buildQueryPlanFromProgram's boundedEligible=false branch.
	return buildQueryPlanFromProgram(ctx, rawR, tenant, dedicated, prog, minTS, maxTS, concurrentRequests, false, false)
}

// buildQueryPlanFromProgram is the shared tail buildQueryPlan/buildStructuralQueryPlan (search,
// boundedEligible=true) and buildMetricsQueryPlan (metrics, boundedEligible=false) all call once
// they have a compiled *blockpack.Program that has already passed any caller-specific
// qualification (buildMetricsQueryPlan's own viAnswerableShape check, buildStructuralQueryPlan's
// own left-leg compile, in particular) — the VCNT-fetch/qualification/selectivity logic itself
// must stay identical across all three call sites, so it lives in exactly one place.
//
// R10 (issue #481 parts 2/3, F-6): the signature changed from a bare *blockpack.QueryPlan return
// to (*blockpack.QueryPlan, error) — a non-nil error means the query has NO safe answer at plan
// time (ErrPlanTimeLowSelectivityNoLimit; see its own doc comment) and the caller MUST fail the
// request here (pipeline.NewBadRequest), never dispatch. A nil plan AND nil error (unchanged from
// before this phase) means "no real plan could be built for other reasons" (no RawReaderProvider,
// compile failure, unresolvable index coverage, nothing plannable) — treat exactly like
// DispatchBlockSharded, byte-identical to today.
//
// hasLimit is meaningful ONLY when boundedEligible is true (search/structural); metrics callers
// pass it as false and it is never read on the boundedEligible=false branch, since R2 already
// forecloses metrics from ever being bounded-served regardless of a limit.
func buildQueryPlanFromProgram(
	ctx context.Context, rawR backend.RawReader, tenant string, dedicated backend.DedicatedColumns,
	prog *blockpack.Program, minTS, maxTS uint64, concurrentRequests int, boundedEligible, hasLimit bool,
) (*blockpack.QueryPlan, error) {
	// issue #493 Task 4a/4b: attach qualification/plan attributes to whatever span is already
	// active on ctx — this function has no span of its own; ctx is the SAME ctx
	// search_sharder.go/metrics_query_range_sharder.go already attached frontend.ShardSearch/
	// frontend.QueryRangeSharder.* to before calling in, so trace.SpanFromContext(ctx) gets that
	// span directly, with zero signature changes needed to thread a span parameter through 3 call
	// sites (buildQueryPlan/buildStructuralQueryPlan/buildMetricsQueryPlan all tail-call here).
	span := trace.SpanFromContext(ctx)

	// Root-cause fix (issue #496 follow-up): record on the frontend's own long-lived ctx,
	// independent of whatever CheckIndexCoverage decides below — a query can be overall
	// shape-indexable while one specific leaf column genuinely has zero VI files, and
	// CheckIndexCoverage's early return below must not suppress recording for the (more
	// common) fully-uncovered case. The querier's own recordUsageForDeclinedQuery call sites
	// (value_index_query.go, value_index_structural_query.go, backend_block.go) remain
	// untouched as a backstop; this call fixes the root cause that those run inside the
	// querier's per-block Fetch/QueryRange, whose ctx is starved/canceled before a slow
	// registry conditional-PUT round-trip can complete.
	vblockpack.RecordUsageIfNoIndexCoverage(ctx, tenant, prog, dedicated, minTS, maxTS, time.Now())

	// (Issue 4/holistic-review fix E) Check resolvability BEFORE any VCNT fetch I/O.
	// allLeavesResolvable is BuildQueryPlan's ONLY gate on Strategy (see its own doc comment):
	// a query that fails this check always resolves to DispatchBlockSharded regardless of what
	// cost/perMinuteForLead say, so computing those first — which requires fetchVCNTSection's
	// S3 Find+Read fan-out — would pay that I/O cost for a query statically guaranteed never to
	// benefit from it. CheckIndexCoverage itself does no VCNT/object-store I/O (it only reads
	// the query's already-compiled shape plus, when shape-eligible, the value-index's own
	// separate discovery cache), so this ordering costs nothing extra for queries that DO
	// qualify.
	if !vblockpack.CheckIndexCoverage(ctx, tenant, prog, minTS, maxTS) {
		if span.IsRecording() {
			span.SetAttributes(attribute.String("plan.qualification_outcome", "not_indexable"))
		}
		return nil, nil
	}

	dims := make([]string, 0, len(prog.WantColumns))
	for c := range prog.WantColumns {
		dims = append(dims, c)
	}

	// issue #493 Task 4c: the only genuinely new span in this whole phase (besides Task 6's
	// reuse of blockpack.query) — a distinct I/O phase (S3 Find+Read fan-out) worth timing on
	// its own, not routine attribute promotion onto an existing span.
	vcntCtx, vcntSpan := tracer.Start(ctx, "frontend.vcntFetch")
	data, dir, filesCount, bytesRead := fetchVCNTSection(vcntCtx, rawR, tenant, dims, minTS, maxTS)
	if vcntSpan.IsRecording() {
		vcntSpan.SetAttributes(
			attribute.Int("files.count", filesCount),
			attribute.Int64("bytes.read", bytesRead),
		)
	}
	vcntSpan.End()

	// F-6/R3/R6: classify selectivity over the SAME decoded section TimeSliceOracle below
	// consumes — no extra I/O, a pure-function call over already-in-hand bytes.
	//
	// issue #493 Task 4b: ClassifyProgramVCNTWithDetail additionally returns the lead leaf's
	// both-sides cost detail (index-side count, full-scan-side column total) that the plain
	// ClassifyProgramVCNT call used to discard — attached below (plan.lead_column/
	// plan.lead_index_cost/plan.lead_column_total, only when known) so a trace viewer can see
	// WHY a query classified the way it did, not just the 3-state verdict.
	sel, leadDetail := blockpack.ClassifyProgramVCNTWithDetail(prog, data, dir, minTS, maxTS)
	if span.IsRecording() && leadDetail.HasLead {
		span.SetAttributes(attribute.String("plan.lead_column", leadDetail.LeadColumn))
		if leadDetail.IndexCostKnown {
			span.SetAttributes(attribute.Int64("plan.lead_index_cost", leadDetail.IndexCost))
		}
		if leadDetail.ColumnTotalKnown {
			span.SetAttributes(attribute.Int64("plan.lead_column_total", leadDetail.ColumnTotal))
		}
	}

	if boundedEligible {
		_, planTimeDecline := blockpack.SelectSearchStrategy(sel, hasLimit)
		if planTimeDecline {
			if span.IsRecording() {
				span.SetAttributes(attribute.String("plan.qualification_outcome", "low_selectivity_no_limit_search"))
			}
			return nil, fmt.Errorf("plan-time decline for tenant %s: %w", tenant, ErrPlanTimeLowSelectivityNoLimit)
		}
		// SelectSearchStrategy's only other outcome is DispatchBlockSharded (Phase 7,
		// plan-scan-fallback.md, removed the DispatchBoundedRecentFirst strategy entirely — the
		// querier's own per-block bounded-index path now handles a limit-bearing low/unknown-
		// selectivity query directly, without any plan-time signal) — fall through to the
		// existing cost/perMinuteForLead/BuildQueryPlan flow below exactly as before this phase
		// (it decides DispatchTimeSliced vs. DispatchBlockSharded on its own, unrelated,
		// resolvability-only gate).
	} else if sel == blockpack.LowSelectivity {
		// Metrics (boundedEligible=false, R2): a resolvable-but-low-selectivity metrics query
		// has no safe answer — SUM/AVG/HISTOGRAM/etc. need the aggregate over the FULL matching
		// corpus, and a truncated aggregate is a wrong answer, not a partial one (R2). Per R6,
		// fail at plan time rather than dispatch N block jobs that would each independently
		// decline identically once the block-level executor also observes low coverage/shape.
		if span.IsRecording() {
			span.SetAttributes(attribute.String("plan.qualification_outcome", "low_selectivity_metrics_no_partial_aggregate"))
		}
		return nil, fmt.Errorf("plan-time decline for tenant %s: %w", tenant, ErrPlanTimeLowSelectivityNoLimit)
	}

	cost, perMinuteForLead := blockpack.TimeSliceOracle(data, dir, minTS, maxTS)

	plan := blockpack.BuildQueryPlan(
		prog, cost, true, perMinuteForLead, minTS, maxTS, concurrentRequests, blockpack.DefaultK,
	)
	if span.IsRecording() {
		knownFraction := 0.0
		if len(plan.Slices) > 0 {
			known := 0
			for _, s := range plan.Slices {
				if s.EstKnown {
					known++
				}
			}
			knownFraction = float64(known) / float64(len(plan.Slices))
		}
		span.SetAttributes(
			attribute.String("plan.qualification_outcome", "qualified"),
			attribute.String("plan.strategy", dispatchStrategyString(plan.Strategy)),
			attribute.Int("plan.slice_count", len(plan.Slices)),
			attribute.Float64("plan.slices_est_known_fraction", knownFraction),
		)
	}
	return &plan, nil
}

// dispatchStrategyString renders a blockpack.DispatchStrategy for the plan.strategy span
// attribute (issue #493 Task 4a) — blockpack.DispatchStrategy has no String method of its own
// (a root-level export of one was not requested and would grow blockpack's public API beyond
// what R4 pre-authorized for this phase), so this is a small local mapping instead.
func dispatchStrategyString(s blockpack.DispatchStrategy) string {
	switch s {
	case blockpack.DispatchTimeSliced:
		return "time_sliced"
	default:
		return "block_sharded"
	}
}
