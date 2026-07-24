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

// ErrPlanTimeLowSelectivityNoLimit (issue #481 parts 2/3, F-6, team-lead ruling R6) used to be
// returned by buildQueryPlanFromProgram when a query was resolvable but had, in R6's own
// (since-reversed) judgment, no safe answer at plan time. REMOVED by issue #535 (team-lead
// ruling, an explicit reversal of R6, not a silent behavior change): "we cannot decline a valid
// query merely because it is expensive... never as a cost/selectivity heuristic for a query the
// system CAN answer correctly." Direct reads of executor.ExecuteTraceMetricsFromVI and the
// unbounded value-index read path (vibuilder.BuildSource) confirmed neither has any
// selectivity-based bailout of its own — this sentinel's whole premise (every per-block job
// would decline identically) was false, and it had zero remaining production callers once both
// of buildQueryPlanFromProgram's decline branches were removed. See queryplan.SelectSearchStrategy's
// doc comment (blockpack, SPEC-QP-6) for the reversed decision table this sentinel used to
// enforce, and this file's buildQueryPlanFromProgram (the "ISSUE #535" comment) for the removed
// call sites.

// compactedKeyChecker is the minimal seam fetchVCNTSection needs to exclude
// already-compacted-but-not-yet-reaped VCNT object keys before downloading them (issue #522
// #157) — lets unit tests exercise the exclusion logic against a fake, without a real Postgres
// connection. *blockpack.FileCatalogStore (a type alias for pgcatalog.Store) satisfies this
// structurally; ListCompactedKeys is exported specifically for this purpose (SPEC-PGCATALOG-7).
type compactedKeyChecker interface {
	ListCompactedKeys(ctx context.Context, keys []string) (map[string]struct{}, error)
}

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
//
// compactedChecker (issue #522 #157, mandatory, mirrors vblockpack.buildVCNTSection's #149
// fix): every candidate key surviving the time-range filter below is checked against it BEFORE
// being downloaded and merged — any key already marked compacted is excluded, closing the same
// NOTE-VC-009-class double-counting window this call path was newly exposed to once VCNT
// compaction moved from immediate-delete to mark-compacted+30-minute-grace-window (#149). A
// compactedChecker error fails the WHOLE call closed (nil/empty section, "no VCNT signal"),
// never falls through to an unfiltered fetch — mirrors buildVCNTSection's own "unverifiable
// exclusion is treated exactly like absent coverage" contract. nil disables the filter (matches
// this function's own rawR-nil-tolerance convention) — the caller is expected to pass nil only
// when tempodb.PgPoolProvider's pool itself is nil (Postgres not configured on this
// deployment).
func fetchVCNTSection(
	ctx context.Context, rawR backend.RawReader, tenant string, dims []string,
	minTS, maxTS uint64, compactedChecker compactedKeyChecker,
) (data []byte, dir []blockpack.VCNTChunkDirEntry, filesCount int, bytesRead int64) {
	if rawR == nil || len(dims) == 0 {
		return nil, nil, 0, 0
	}

	type candidate struct {
		key     string
		keypath backend.KeyPath
		name    string
	}
	var candidates []candidate
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
			candidates = append(candidates, candidate{key: k, keypath: keypath, name: name})
		}
	}
	if len(candidates) == 0 {
		return nil, nil, 0, 0
	}

	var compacted map[string]struct{}
	if compactedChecker != nil {
		candidateKeys := make([]string, len(candidates))
		for i, c := range candidates {
			candidateKeys[i] = c.key
		}
		var checkErr error
		compacted, checkErr = compactedChecker.ListCompactedKeys(ctx, candidateKeys)
		if checkErr != nil {
			return nil, nil, 0, 0
		}
	}

	var objects [][]byte
	for _, c := range candidates {
		if _, isCompacted := compacted[c.key]; isCompacted {
			continue
		}
		rc, _, err := rawR.Read(ctx, c.name, c.keypath, nil)
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
	minTS, maxTS uint64, concurrentRequests int, hasLimit bool, compactedChecker compactedKeyChecker,
) (*blockpack.QueryPlan, int64, error) {
	if rawR == nil || query == "" {
		return nil, 0, nil
	}
	prog, err := blockpack.CompileTraceQL(query, blockpack.QueryOptions{})
	if err != nil || prog == nil {
		return nil, 0, nil
	}
	return buildQueryPlanFromProgram(
		ctx, rawR, tenant, dedicated, prog, minTS, maxTS, concurrentRequests, true, hasLimit, compactedChecker,
	)
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
	minTS, maxTS uint64, concurrentRequests int, compactedChecker compactedKeyChecker,
) (*blockpack.QueryPlan, int64, error) {
	if rawR == nil || query == "" {
		return nil, 0, nil
	}
	prog, viAnswerableShape, err := blockpack.CompileTraceQLMetricsFilter(query)
	if err != nil || prog == nil || !viAnswerableShape {
		return nil, 0, nil
	}
	// boundedEligible=false (R2: metrics is never bounded-served); hasLimit is irrelevant on
	// this path and unused by buildQueryPlanFromProgram's boundedEligible=false branch.
	return buildQueryPlanFromProgram(
		ctx, rawR, tenant, dedicated, prog, minTS, maxTS, concurrentRequests, false, false, compactedChecker,
	)
}

// buildQueryPlanFromProgram is the shared tail buildQueryPlan/buildStructuralQueryPlan (search,
// boundedEligible=true) and buildMetricsQueryPlan (metrics, boundedEligible=false) all call once
// they have a compiled *blockpack.Program that has already passed any caller-specific
// qualification (buildMetricsQueryPlan's own viAnswerableShape check, buildStructuralQueryPlan's
// own left-leg compile, in particular) — the VCNT-fetch/qualification/selectivity logic itself
// must stay identical across all three call sites, so it lives in exactly one place.
//
// R10 (issue #481 parts 2/3, F-6) added a (*blockpack.QueryPlan, error) signature so a non-nil
// error could mean the query has NO safe answer at plan time (the now-removed
// ErrPlanTimeLowSelectivityNoLimit). Issue #535 (team-lead ruling, reversing R6) removed both
// plan-time decline branches this function used to have (see the "ISSUE #535" comment below,
// where selectivity is classified) — as of that change, this function can no longer return a
// non-nil error at all; every remaining early return is (nil, 0, nil) or (nil, bytesRead, nil).
// The error return is kept in the signature (rather than dropped) purely for call-site
// stability — search_sharder.go/metrics_query_range_sharder.go's existing
// `if planErr != nil { return pipeline.NewBadRequest(planErr), nil }` guards remain correct,
// harmless, defensive code; they simply never fire from this call path anymore. A nil plan AND
// nil error (unchanged from before this phase) means "no real plan could be built for other
// reasons" (no RawReaderProvider, compile failure, unresolvable index coverage, nothing
// plannable) — treat exactly like DispatchBlockSharded, byte-identical to today.
//
// hasLimit is meaningful ONLY when boundedEligible is true (search/structural); metrics callers
// pass it as false. As of issue #535, neither hasLimit nor boundedEligible gates dispatch or
// decline inside this function's body anymore (see the "ISSUE #535" comment below for why) —
// both are still read once, purely to attach plan.bounded_eligible/plan.has_limit as span
// observability, so a trace viewer retains the caller-side context even though it no longer
// changes the outcome.
// buildQueryPlanFromProgram's second return value (issue #218 Phase 3) is the VCNT bytesRead
// fetchVCNTSection already computes below — surfaced here so callers can thread it into
// SearchMetrics.vcntBytesRead without this function needing any response-shape awareness of
// its own. It is 0 whenever the function returns before reaching the VCNT fetch (nil rawR/
// query, CheckIndexCoverage decline).
func buildQueryPlanFromProgram(
	ctx context.Context, rawR backend.RawReader, tenant string, dedicated backend.DedicatedColumns,
	prog *blockpack.Program, minTS, maxTS uint64, concurrentRequests int, boundedEligible, hasLimit bool,
	compactedChecker compactedKeyChecker,
) (*blockpack.QueryPlan, int64, error) {
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
	// benefit from it. CheckIndexCoverage itself does no I/O at all as of #217/Phase 2.1 — it is
	// a pure shape (AllLeavesIndexable) + deployment (reader configured) check with no
	// whole-window data-availability opinion; per-slice coverage is decided later, locally and
	// authoritatively, by each DispatchTimeSliced slice's own tryIndexFetch call.
	if !vblockpack.CheckIndexCoverage(prog) {
		if span.IsRecording() {
			span.SetAttributes(attribute.String("plan.qualification_outcome", "not_indexable"))
		}
		return nil, 0, nil
	}

	// #205 correction, narrowed by #205 review: also request the duration-histogram variant
	// (blockpack.VCNTDurationHistogramColumnName) of "span:duration" specifically — histogram
	// records are written under their OWN colHash directory (colHash("span:duration#hist")),
	// entirely separate from the bare column's own directory (colHash("span:duration"), which
	// never has any records written under it at all). Without this widening, fetchVCNTSection
	// never lists/downloads the histogram object, ClassifyProgramVCNTWithDetail always sees
	// Covered=false for it, and #205's whole selectivity signal is an inert no-op through this
	// call path regardless of what vcntwriter.go wrote.
	//
	// "span:duration" is the ONLY histogram-eligible column in Phase 1 (mirrors queryplan's own
	// hardcoded durationHistogramColumn constant, vcnt_duration_cost.go) — widening every
	// WantColumns entry (as an earlier version of this fix did) issued an extra, always-empty
	// Find+List against every non-duration column's "#hist" directory for zero benefit, doubling
	// unnecessary object-storage calls and undermining this whole feature's I/O-reduction goal.
	const durationColumn = "span:duration"
	dims := make([]string, 0, len(prog.WantColumns)+1)
	for c := range prog.WantColumns {
		dims = append(dims, c)
		if c == durationColumn {
			dims = append(dims, blockpack.VCNTDurationHistogramColumnName(c))
		}
	}

	// issue #493 Task 4c: the only genuinely new span in this whole phase (besides Task 6's
	// reuse of blockpack.query) — a distinct I/O phase (S3 Find+Read fan-out) worth timing on
	// its own, not routine attribute promotion onto an existing span.
	vcntCtx, vcntSpan := tracer.Start(ctx, "frontend.vcntFetch")
	data, dir, filesCount, bytesRead := fetchVCNTSection(vcntCtx, rawR, tenant, dims, minTS, maxTS, compactedChecker)
	if vcntSpan.IsRecording() {
		vcntSpan.SetAttributes(
			attribute.Int("files.count", filesCount),
			attribute.Int64("bytes.read", bytesRead),
		)
	}
	vcntSpan.End()

	// F-6/R3: classify selectivity over the SAME decoded section TimeSliceOracle below
	// consumes — no extra I/O, a pure-function call over already-in-hand bytes.
	//
	// issue #493 Task 4b: ClassifyProgramVCNTWithDetail additionally returns the lead leaf's
	// both-sides cost detail (index-side count, full-scan-side column total) that the plain
	// ClassifyProgramVCNT call used to discard — attached below (plan.lead_column/
	// plan.lead_index_cost/plan.lead_column_total, only when known) so a trace viewer can see
	// WHY a query classified the way it did, not just the 3-state verdict.
	sel, leadDetail := blockpack.ClassifyProgramVCNTWithDetail(prog, data, dir, minTS, maxTS)
	if span.IsRecording() {
		// plan.selectivity/plan.bounded_eligible/plan.has_limit (issue #535): none of the three
		// gates dispatch/decline below anymore, but they remain real, useful observability — a
		// trace viewer can see how a query classified and which caller-side path it came
		// through, even though, post-#535, every combination now routes identically.
		span.SetAttributes(
			attribute.String("plan.selectivity", selectivityString(sel)),
			attribute.Bool("plan.bounded_eligible", boundedEligible),
			attribute.Bool("plan.has_limit", hasLimit),
		)
		if leadDetail.HasLead {
			span.SetAttributes(attribute.String("plan.lead_column", leadDetail.LeadColumn))
			if leadDetail.IndexCostKnown {
				span.SetAttributes(attribute.Int64("plan.lead_index_cost", leadDetail.IndexCost))
			}
			if leadDetail.ColumnTotalKnown {
				span.SetAttributes(attribute.Int64("plan.lead_column_total", leadDetail.ColumnTotal))
			}
		}
	}

	// ISSUE #535 (team-lead ruling, reversing R6): a query may only decline when the index/cube
	// genuinely isn't built yet for the queried window (a real coverage gap, or cube warming) —
	// never as a cost/selectivity heuristic for a query the system CAN answer correctly. This
	// function used to fail here at plan time for TWO cases that both turned out to be
	// unnecessary once the actual execution code was read directly:
	//
	//   - search/structural (boundedEligible=true): blockpack.SelectSearchStrategy's
	//     LowSelectivity+no-limit row used to set planTimeDecline=true. It no longer can — the
	//     unbounded value-index read path used for a no-limit query (vibuilder.BuildSource)
	//     enumerates and returns everything the index finds, with no selectivity-based
	//     limitation of its own, so there was never an execution-side reason to decline it here.
	//   - metrics (boundedEligible=false, R2): a resolvable-but-low-selectivity metrics query
	//     used to decline directly below on sel == blockpack.LowSelectivity. But
	//     executor.ExecuteTraceMetricsFromVI computes the exact bucketed count over EVERY
	//     matched value-index entry unconditionally — there is no selectivity-based bailout
	//     anywhere in that function — so a "resolvable-but-low-selectivity" metrics query was
	//     never actually unanswerable; it was simply being declined for being expensive.
	//
	// Both branches are gone. boundedEligible/hasLimit no longer gate anything below (they are
	// read once, above, purely for the plan.bounded_eligible/plan.has_limit span attributes) —
	// sel/leadDetail similarly remain solely observability (issue #493 Task 4b's
	// WHY-it-classified-this-way span attributes); every Selectivity value, bounded or not,
	// limited or not, now flows straight into the same cost/TimeSliceOracle/BuildQueryPlan
	// pipeline, exactly as Selective/UnknownSelectivity already did before this phase.
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
	return &plan, bytesRead, nil
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

// selectivityString renders a blockpack.Selectivity for the plan.selectivity span attribute
// (issue #535) — mirrors dispatchStrategyString's own small local-mapping convention;
// blockpack.Selectivity has no String method of its own for the same reason DispatchStrategy
// doesn't (an export was not requested and would grow blockpack's public API beyond what was
// pre-authorized for this phase).
func selectivityString(sel blockpack.Selectivity) string {
	switch sel {
	case blockpack.Selective:
		return "selective"
	case blockpack.LowSelectivity:
		return "low_selectivity"
	default:
		return "unknown"
	}
}
