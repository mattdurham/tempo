package blockpack

// metricsfilter.go — CompileTraceQLMetricsFilter, issue #487 holistic-review Issue 2/B. Additive,
// no existing file (api.go, vcnt.go, timeslice.go) is touched.
//
// buildQueryPlan (tempo's frontend, modules/frontend/vcnt_fetch.go) needs a *Program to feed
// blockpack.BuildQueryPlan's leaf-cost-ordering and AllLeavesIndexable checks, for BOTH the
// search call site (a filter-only query, already served by CompileTraceQL) and the metrics call
// site (a REAL QueryRangeRequest.Query always has an aggregation pipeline — `{...} | rate()` — so
// traceqlparser.ParseTraceQL never returns a *FilterExpression for it; CompileTraceQL always
// errors, and #487's metrics-side time-slice dispatch was consequently unreachable in production
// prior to this fix). This file closes that gap the same way vm.CompileTraceQLMetrics already
// does internally for BuildValueIndexSourceForMetrics: compile the metrics query and hand back
// its filter-predicate *Program, without requiring the caller to hand-extract a filter substring
// from the query text itself (which would need to replicate TraceQL's own pipeline-boundary
// parsing rules to do safely).

import (
	"fmt"

	"github.com/grafana/blockpack/internal/vm"
)

// CompileTraceQLMetricsFilter compiles a TraceQL metrics query (e.g. `{ status = "error" } |
// rate() by (service.name)`) and returns its filter-predicate *Program — the same Program shape
// CompileTraceQL returns for a plain filter expression, safe to pass to BuildQueryPlan/
// AllLeavesIndexable/QueryTraceQLWithProgram exactly like any other *Program.
//
// viAnswerableShape reports whether the query's aggregate SHAPE (function + group-by) is one the
// value-index metrics engine (ExecuteTraceMetricsFromVI) can execute at all — the exact same
// static gate that function itself applies (vm.MetricsShapeIsVIAnswerable), reused verbatim
// rather than re-derived, so a caller's plan-time qualification decision (e.g. tempo's frontend
// deciding whether to dispatch IndexOnly=true time-sliced jobs at all) can never drift from what
// the real execution path will actually do. Today: only count_over_time()/rate() with no
// group-by report true; every other aggregate function or any group-by reports false. This is
// ONLY the static, compile-time-knowable half of the real decline surface — it does not, and
// cannot, predict execution-time-only declines (no value-index coverage, a legacy block with
// per-span TimeSec == 0, an unresolvable filter leaf), which still require a caller (or the
// querier's own inner gate) to fail closed rather than silently scan when running in an
// IndexOnly/no-safe-fallback mode.
//
// A query that is not a metrics query at all (no aggregation pipeline — use CompileTraceQL for
// that), or that fails to compile for any other reason, returns a non-nil error and a nil
// Program; viAnswerableShape is meaningless in that case and always false.
func CompileTraceQLMetricsFilter(traceqlQuery string) (prog *Program, viAnswerableShape bool, err error) {
	p, spec, cerr := vm.CompileTraceQLMetrics(traceqlQuery, 0, 0)
	if cerr != nil {
		return nil, false, fmt.Errorf("compile metrics query: %w", cerr)
	}
	if spec == nil {
		return p, false, nil
	}
	return p, vm.MetricsShapeIsVIAnswerable(*spec), nil
}
