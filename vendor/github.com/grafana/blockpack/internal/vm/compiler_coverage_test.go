package vm

// Additional tests to boost coverage in traceql_compiler.go and metrics_compiler.go.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/traceqlparser"
)

// --- helpers ---

// compileFilter parses and compiles a filter query, returning the Program.
func compileFilter(t *testing.T, query string) *Program {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoErrorf(t, err, "parse %q", query)
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	require.Truef(t, ok, "expected FilterExpression for %q", query)
	prog, err := CompileTraceQLFilter(fe)
	require.NoErrorf(t, err, "compile %q", query)
	return prog
}

// --- CompileTraceQLFilter with various literal types ---

// Covers compileColumnPredicateComparison for status/kind/duration/float/bool literals.
func TestCompileFilter_StatusLiteral(t *testing.T) {
	prog := compileFilter(t, `{ status = error }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

func TestCompileFilter_KindLiteral(t *testing.T) {
	prog := compileFilter(t, `{ kind = server }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

func TestCompileFilter_DurationLiteral(t *testing.T) {
	prog := compileFilter(t, `{ duration > 5ms }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

func TestCompileFilter_FloatLiteral(t *testing.T) {
	prog := compileFilter(t, `{ .score > 3.14 }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

func TestCompileFilter_BoolLiteral(t *testing.T) {
	prog := compileFilter(t, `{ .active = true }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// Covers compileColumnPredicateBinary with AND operator.
func TestCompileFilter_AND(t *testing.T) {
	prog := compileFilter(t, `{ span.http.method = "GET" && resource.service.name = "svc" }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// Covers compileColumnPredicateBinary with OR operator.
func TestCompileFilter_OR(t *testing.T) {
	prog := compileFilter(t, `{ span.http.method = "GET" || span.http.method = "POST" }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// Covers compileComparison for all comparison operators.
func TestCompileFilter_AllComparisons(t *testing.T) {
	queries := []string{
		`{ span.count = 5 }`,
		`{ span.count != 5 }`,
		`{ span.count > 5 }`,
		`{ span.count >= 5 }`,
		`{ span.count < 5 }`,
		`{ span.count <= 5 }`,
		`{ span.name =~ "foo.*" }`,
		`{ span.name !~ "bar.*" }`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			prog := compileFilter(t, q)
			assert.NotNil(t, prog)
		})
	}
}

// Covers CompileTraceQLFilter with empty filter expression.
func TestCompileFilter_EmptyMatchAll(t *testing.T) {
	parsed, err := traceqlparser.ParseTraceQL(`{}`)
	require.NoError(t, err)
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	require.True(t, ok)
	prog, err := CompileTraceQLFilter(fe)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// Covers CompileTraceQLFilter with nil filter.
func TestCompileFilter_NilFilter(t *testing.T) {
	prog, err := CompileTraceQLFilter(nil)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// Covers hex-bytes path for trace:id comparison.
func TestCompileFilter_TraceID(t *testing.T) {
	prog := compileFilter(t, `{ trace:id = "0102030405060708090a0b0c0d0e0f10" }`)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.ColumnPredicate)
}

// --- Metrics compiler coverage ---

func TestCompileMetrics_AllAggregateFunctions(t *testing.T) {
	cases := []struct {
		query    string
		wantFunc string
	}{
		{"{} | rate()", FuncNameRATE},
		{"{} | count_over_time()", FuncNameCOUNT},
		{"{} | avg(span.duration) by (resource.service.name)", FuncNameAVG},
		{"{} | min(span.duration)", FuncNameMIN},
		{"{} | max(span.duration)", FuncNameMAX},
		{"{} | sum(span.duration) by (resource.service.name)", FuncNameSUM},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			prog, spec, err := CompileTraceQLMetrics(tc.query, 0, 1e9)
			require.NoError(t, err)
			assert.NotNil(t, prog)
			assert.Equal(t, tc.wantFunc, spec.Aggregate.Function)
		})
	}
}

func TestCompileMetrics_WithRangePredicate(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ span.duration > 100 } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
	assert.NotNil(t, prog.Predicates)
}

func TestCompileMetrics_WithEqualityPredicate(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ resource.service.name = "svc-a" } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	require.NotNil(t, spec)
	require.NotNil(t, prog.Predicates)
}

func TestCompileMetrics_WithORFilter(t *testing.T) {
	// OR filters are currently skipped (no error, just no predicates extracted).
	prog, spec, err := CompileTraceQLMetrics(
		`{ .env = "prod" || .env = "staging" } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
}

func TestCompileMetrics_WithANDFilter(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ resource.service.name = "svc" && span.http.status = "200" } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
}

func TestCompileMetrics_WithGteRangePredicate(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ span.count >= 10 } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
	require.NotNil(t, prog.Predicates)
	require.NotEmpty(t, prog.Predicates.Nodes)
}

func TestCompileMetrics_WithLteRangePredicate(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ span.count <= 100 } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
	require.NotNil(t, prog.Predicates)
	require.NotEmpty(t, prog.Predicates.Nodes)
}

func TestCompileMetrics_WithLtRangePredicate(t *testing.T) {
	prog, _, err := CompileTraceQLMetrics(
		`{ span.count < 10 } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	require.NotNil(t, prog.Predicates)
	require.NotEmpty(t, prog.Predicates.Nodes)
}

func TestCompileMetrics_WithNeqFilter(t *testing.T) {
	// Neq predicates are currently skipped (not extracted).
	prog, spec, err := CompileTraceQLMetrics(
		`{ resource.service.name != "svc" } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
}

func TestCompileMetrics_WithRegexFilter(t *testing.T) {
	prog, spec, err := CompileTraceQLMetrics(
		`{ resource.service.name =~ "svc.*" } | count_over_time()`,
		0, 1e9,
	)
	require.NoError(t, err)
	assert.NotNil(t, prog)
	assert.NotNil(t, spec)
}

func TestCompileMetrics_InvalidQuery(t *testing.T) {
	_, _, err := CompileTraceQLMetrics("invalid query %%", 0, 1e9)
	assert.Error(t, err)
}

func TestCompileMetrics_FilterQueryError(t *testing.T) {
	// Passing a filter query to CompileTraceQLMetrics should fail.
	_, _, err := CompileTraceQLMetrics(`{ name = "test" }`, 0, 1e9)
	assert.Error(t, err)
}

// --- compileColumnPredicate paths ---

// These queries exercise compileColumnPredicateExpr through CompileTraceQLFilter.
func TestCompileFilter_ColumnPredicateExpr(t *testing.T) {
	queries := []string{
		`{ span.http.method = "GET" }`,
		`{ span.http.method != "GET" }`,
		`{ span.count > 5 }`,
		`{ span.count >= 5 }`,
		`{ span.count < 5 }`,
		`{ span.count <= 5 }`,
		`{ span.name =~ "foo.*" }`,
		`{ span.name !~ "bar.*" }`,
		`{ span.name = "a" && span.env = "prod" }`,
		`{ span.name = "a" || span.env = "prod" }`,
		`{ .http.method = "GET" }`, // unscoped → OR expansion
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			prog := compileFilter(t, q)
			require.NotNil(t, prog)
			require.NotNil(t, prog.ColumnPredicate, "ColumnPredicate must not be nil")
		})
	}
}
