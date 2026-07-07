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

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
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
func fetchVCNTSection(
	ctx context.Context, rawR backend.RawReader, tenant, indexPrefix string, dims []string,
) ([]byte, []blockpack.VCNTChunkDirEntry) {
	if rawR == nil || len(dims) == 0 {
		return nil, nil
	}

	var objects [][]byte
	for _, dim := range dims {
		colHash := blockpack.VCNTColHash(dim)
		prefix := backend.KeyPath{tenant, indexPrefix, "unique_values", colHash}

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
			rc, _, err := rawR.Read(ctx, name, keypath, nil)
			if err != nil {
				continue
			}
			data, readErr := io.ReadAll(rc)
			_ = rc.Close()
			if readErr != nil || len(data) == 0 {
				continue
			}
			objects = append(objects, data)
		}
	}
	if len(objects) == 0 {
		return nil, nil
	}
	data, dir, _ := blockpack.VCNTBuildSectionFromObjects(objects)
	return data, dir
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
	ctx context.Context, rawR backend.RawReader, tenant, indexPrefix, query string,
	minTS, maxTS uint64, concurrentRequests int,
) *blockpack.QueryPlan {
	if rawR == nil || query == "" {
		return nil
	}
	prog, err := blockpack.CompileTraceQL(query, blockpack.QueryOptions{})
	if err != nil || prog == nil {
		return nil
	}
	return buildQueryPlanFromProgram(ctx, rawR, tenant, indexPrefix, prog, minTS, maxTS, concurrentRequests)
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
	ctx context.Context, rawR backend.RawReader, tenant, indexPrefix, query string,
	minTS, maxTS uint64, concurrentRequests int,
) *blockpack.QueryPlan {
	if rawR == nil || query == "" {
		return nil
	}
	prog, viAnswerableShape, err := blockpack.CompileTraceQLMetricsFilter(query)
	if err != nil || prog == nil || !viAnswerableShape {
		return nil
	}
	return buildQueryPlanFromProgram(ctx, rawR, tenant, indexPrefix, prog, minTS, maxTS, concurrentRequests)
}

// buildQueryPlanFromProgram is the shared tail both buildQueryPlan and buildMetricsQueryPlan call
// once they have a compiled *blockpack.Program that has already passed any caller-specific
// qualification (buildMetricsQueryPlan's own viAnswerableShape check, in particular) — the
// VCNT-fetch/qualification logic itself must stay identical for both call sites, so it lives in
// exactly one place.
func buildQueryPlanFromProgram(
	ctx context.Context, rawR backend.RawReader, tenant, indexPrefix string, prog *blockpack.Program,
	minTS, maxTS uint64, concurrentRequests int,
) *blockpack.QueryPlan {
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
		return nil
	}

	dims := make([]string, 0, len(prog.WantColumns))
	for c := range prog.WantColumns {
		dims = append(dims, c)
	}

	data, dir := fetchVCNTSection(ctx, rawR, tenant, indexPrefix, dims)
	cost, perMinuteForLead := blockpack.TimeSliceOracle(data, dir, minTS, maxTS)

	plan := blockpack.BuildQueryPlan(
		prog, cost, true, perMinuteForLead, minTS, maxTS, concurrentRequests, blockpack.DefaultK,
	)
	return &plan
}
