package vblockpack

// unfiltered_metrics_vi_misattribution_test.go — task #198 regression.
//
// Live symptom (tempo-dev-test-03): unfiltered metrics queries ({} | rate(),
// {} | count_over_time()) return HTTP 422 with "metrics query requires the value index,
// which is not reachable on this querier (no S3 backend configured, or the S3 client
// failed to initialize)" even when the querier's value index IS correctly configured and
// initialized (confirmed via config + logs — no init-failure warning anywhere).
//
// Root cause, confirmed by reading the real call chain (not assumed):
//
//  1. `{} | count_over_time()` / `{} | rate()` have an empty filter clause. TraceQL's
//     parseFilterExpression("{}") returns (nil, nil), so MetricsQuery.Filter is nil.
//     blockpack/internal/vm/metrics_compiler.go:CompileTraceQLMetrics therefore takes the
//     `else` branch and compiles via compileMatchAllProgram() (traceql_compiler.go), which
//     builds a *Program with NO Predicates field set at all (zero value: nil).
//
//  2. blockpack/internal/modules/vibuilder/builder.go:BuildSource's very first real branch,
//     `if preds == nil || (len(preds.Nodes) == 0 && len(preds.Columns) == 0)`, fires for
//     this program and returns (nil, false, nil) UNCONDITIONALLY — see its own comment:
//     "Truly nothing referenced (e.g. `{}`): nothing to discover. ... we cannot enumerate
//     'all spans across all columns' without a column list." This ok=false carries NO
//     signal about whether the reader (disc/store) was ever configured — it fires the
//     exact same way whether disc is nil or a fully live, populated index.
//
//  3. tempo's own backend_block.go QueryRange (~line 308-317) calls
//     blockpack.BuildValueIndexSourceForMetrics only when `vr := getValueIndexQueryReader()`
//     is non-nil (the reader IS configured here), but on ok=false it just leaves
//     opts.ValueIndex nil — it does not record why the build declined.
//
//  4. blockpack's api.go:ExecuteMetricsTraceQL (~line 535-549) branches solely on
//     `opts.ValueIndex != nil`. Because step 3 never populated it, the function takes the
//     nil branch and returns ErrMetricsValueIndexDisabled — R8's "no ValueIndexSource was
//     supplied at all… an operator-config action" category — even though the reader WAS
//     supplied to QueryRange and IS reachable; it is only this particular query SHAPE
//     (match-all, no leaf, no explicit column list) that BuildSource cannot resolve. The
//     more accurate sentinel for a configured-but-declining index, ErrMetricsNoCoverage,
//     is only ever reached via ExecuteTraceMetricsFromVI, which is never invoked here
//     because opts.ValueIndex is nil.
//
// This test proves the misattribution against the REAL production call chain: a real
// vblockpack block written through the real CreateBlock write path, a real, in-memory-backed
// value-index reader installed via the same withVIQueryReader/withVISink helpers every other
// test in this package uses (not a nil-reader synthetic stand-in), with reachability of that
// exact reader/block pair proven in-test by a companion FILTERED query that genuinely answers
// through the index. Before the fix it FAILED: QueryRange returned
// blockpack.ErrMetricsValueIndexDisabled for the unfiltered queries, exactly reproducing the live
// 422 misattribution.
//
// Fix (task #198, backend_block.go QueryRange, ~line 309-333): when vr != nil (the reader IS
// configured) but BuildValueIndexSourceForMetrics declines with ok=false, berr=nil (no coverage
// for this query's shape, not "no reader at all"), QueryRange now sets opts.ValueIndex to a
// fresh, empty blockpack.NewSliceValueIndexSource() instead of leaving it nil. This routes
// ExecuteMetricsTraceQL into ExecuteTraceMetricsFromVI, whose existing viMatchSpans/AllResults
// logic on an empty source correctly declines with the per-query-shape sentinel
// (blockpack.ErrMetricsNoCoverage for this match-all-no-columns shape) instead of the
// zeroth/config-level ErrMetricsValueIndexDisabled. No blockpack code changed — only tempo's
// backend_block.go, using blockpack's already-public NewSliceValueIndexSource API.

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// TestQueryRange_UnfilteredMetrics_MisattributesConfiguredIndexAsDisabled is task #198's
// regression test. See the file doc comment above for the full root-cause chain.
func TestQueryRange_UnfilteredMetrics_MisattributesConfiguredIndexAsDisabled(t *testing.T) {
	dir := t.TempDir()
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	tenant := "test-tenant"
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 300)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	// Sanity check: prove the value-index reader installed above is genuinely configured
	// AND reachable against this exact block — a filtered query over the same fixture must
	// answer through the real index path (mirrors
	// TestQueryRange_EmitsDeclineDetail_SuccessPath in decline_reason_test.go). If this
	// fails, the test below would be meaningless (a broken/unreachable index would trivially
	// "explain" ErrMetricsValueIndexDisabled).
	filteredReq := countOverTimeReq(`{ resource.service.name = "svc-alpha" } | count_over_time()`)
	filteredResp, err := block.QueryRange(context.Background(), filteredReq, common.SearchOptions{})
	require.NoError(t, err, "sanity check: the configured, reachable value index must answer a real filtered query")
	require.NotNil(t, filteredResp)

	// Now run the genuinely unfiltered/match-all-shaped queries the live incident reported.
	// BuildValueIndexSourceForMetrics legitimately declines this shape (no leaf, no column
	// list to enumerate) with ok=false, berr=nil — NOT because the reader is unconfigured or
	// unreachable, as the sanity check above just proved.
	for _, query := range []string{"{} | count_over_time()", "{} | rate()"} {
		t.Run(query, func(t *testing.T) {
			req := countOverTimeReq(query)
			_, err := block.QueryRange(context.Background(), req, common.SearchOptions{})
			require.Error(t, err, "an unfiltered metrics query with no resolvable VI column list must still decline")

			// This was the bug (task #198): QueryRange used to report
			// ErrMetricsValueIndexDisabled — the "no index configured for this querier at
			// all" sentinel — even though the sanity check immediately above just proved the
			// SAME reader against the SAME block answers a real query through the index. The
			// correct category is a per-shape coverage gap: blockpack.ErrMetricsNoCoverage,
			// which fires because `{} | count_over_time()`/`{} | rate()` are match-all shapes
			// with no column list to enumerate (vibuilder.BuildSource's "truly nothing
			// referenced" branch, then ExecuteTraceMetricsFromVI's own AllResults-on-empty-
			// source decline) — the index IS otherwise configured, only this specific query
			// shape lacks coverage. Assert the correct positive case now, not just the
			// negative "not this one".
			require.True(t, errors.Is(err, blockpack.ErrMetricsNoCoverage),
				"task #198: unfiltered metrics query must decline with the per-shape coverage-gap "+
					"sentinel (blockpack.ErrMetricsNoCoverage) now that the configured, reachable "+
					"value index's own decline is correctly attributed, not misattributed as "+
					"ErrMetricsValueIndexDisabled (config-level absence); got: %v", err)
			require.False(t, errors.Is(err, blockpack.ErrMetricsValueIndexDisabled),
				"task #198: unfiltered metrics query wrongly misattributed a configured, "+
					"reachable value index as ErrMetricsValueIndexDisabled (config-level "+
					"absence) instead of a per-shape coverage-gap sentinel; got: %v", err)
		})
	}
}
