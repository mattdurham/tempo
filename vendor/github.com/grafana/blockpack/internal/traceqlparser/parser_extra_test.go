package traceqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- BinaryOp.String ---

func TestBinaryOp_String(t *testing.T) {
	cases := []struct {
		want string
		op   BinaryOp
	}{
		{"&&", OpAnd},
		{"||", OpOr},
		{"=", OpEq},
		{"!=", OpNeq},
		{">", OpGt},
		{">=", OpGte},
		{"<", OpLt},
		{"<=", OpLte},
		{"=~", OpRegex},
		{"!~", OpNotRegex},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.op.String(), "op %d", tc.op)
	}
}

func TestBinaryOp_StringUnknown(t *testing.T) {
	var unknown BinaryOp = 999
	assert.Equal(t, "UNKNOWN", unknown.String())
}

// --- Error paths ---

func TestParseTraceQL_InvalidQuery(t *testing.T) {
	_, err := ParseTraceQL(`{ name = }`)
	assert.Error(t, err)
}

func TestParseTraceQL_UnterminatedString(t *testing.T) {
	_, err := ParseTraceQL(`{ name = "unterminated }`)
	assert.Error(t, err)
}

func TestParseTraceQL_InvalidScope(t *testing.T) {
	_, err := ParseTraceQL(`{ badscope.foo = "bar" }`)
	assert.Error(t, err)
}

func TestParseTraceQL_EmptyQuery(t *testing.T) {
	result, err := ParseTraceQL("")
	require.NoError(t, err)
	assert.Nil(t, result)
}

// --- String escape sequences ---

func TestParseString_EscapeSequences(t *testing.T) {
	// Test that escape sequences are handled correctly
	result, err := ParseTraceQL(`{ name = "hello\nworld" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	require.NotNil(t, fe)

	// The expression should be a BinaryExpr with a LiteralExpr containing the escaped string
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, "hello\nworld", lit.Value)
}

func TestParseString_TabEscape(t *testing.T) {
	result, err := ParseTraceQL(`{ name = "col1\tcol2" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, "col1\tcol2", lit.Value)
}

func TestParseString_BackslashEscape(t *testing.T) {
	result, err := ParseTraceQL(`{ name = "a\\b" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, `a\b`, lit.Value)
}

func TestParseString_QuoteEscape(t *testing.T) {
	result, err := ParseTraceQL(`{ name = "say \"hi\"" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, `say "hi"`, lit.Value)
}

// --- Duration literals ---

func TestParseTraceQL_DurationNs(t *testing.T) {
	result, err := ParseTraceQL(`{ duration > 500ns }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitDuration, lit.Type)
	assert.Equal(t, int64(500), lit.Value)
}

func TestParseTraceQL_DurationMs(t *testing.T) {
	result, err := ParseTraceQL(`{ duration > 5ms }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitDuration, lit.Type)
	assert.Equal(t, int64(5_000_000), lit.Value) // 5ms in nanoseconds
}

func TestParseTraceQL_DurationS(t *testing.T) {
	result, err := ParseTraceQL(`{ duration > 2s }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitDuration, lit.Type)
	assert.Equal(t, int64(2_000_000_000), lit.Value) // 2s in nanoseconds
}

func TestParseTraceQL_DurationM(t *testing.T) {
	result, err := ParseTraceQL(`{ duration > 1m }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitDuration, lit.Type)
	assert.Equal(t, int64(60_000_000_000), lit.Value)
}

func TestParseTraceQL_DurationH(t *testing.T) {
	result, err := ParseTraceQL(`{ duration > 1h }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitDuration, lit.Type)
	assert.Equal(t, int64(3_600_000_000_000), lit.Value)
}

// --- All attribute scopes ---

func TestParseTraceQL_AllScopes(t *testing.T) {
	scopes := []string{"span", "resource", "trace", "log"}
	for _, scope := range scopes {
		query := `{ ` + scope + `.foo = "bar" }`
		result, err := ParseTraceQL(query)
		require.NoErrorf(t, err, "scope %q", scope)
		fe, ok := result.(*FilterExpression)
		require.Truef(t, ok, "scope %q", scope)
		be, ok := fe.Expr.(*BinaryExpr)
		require.Truef(t, ok, "scope %q", scope)
		field, ok := be.Left.(*FieldExpr)
		require.Truef(t, ok, "scope %q", scope)
		assert.Equal(t, scope, field.Scope, "scope %q", scope)
	}
}

// --- Comparison operators ---

func TestParseTraceQL_ComparisonOperators(t *testing.T) {
	cases := []struct {
		query  string
		wantOp BinaryOp
	}{
		{`{ .x = "v" }`, OpEq},
		{`{ .x != "v" }`, OpNeq},
		{`{ .x > 1 }`, OpGt},
		{`{ .x >= 1 }`, OpGte},
		{`{ .x < 1 }`, OpLt},
		{`{ .x <= 1 }`, OpLte},
		{`{ .x =~ "v.*" }`, OpRegex},
		{`{ .x !~ "v.*" }`, OpNotRegex},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			result, err := ParseTraceQL(tc.query)
			require.NoError(t, err)
			fe, ok := result.(*FilterExpression)
			require.True(t, ok)
			be, ok := fe.Expr.(*BinaryExpr)
			require.True(t, ok)
			assert.Equal(t, tc.wantOp, be.Op)
		})
	}
}

// --- Logical AND/OR ---

func TestParseTraceQL_LogicalAND(t *testing.T) {
	result, err := ParseTraceQL(`{ .a = "x" && .b = "y" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, OpAnd, be.Op)
}

func TestParseTraceQL_LogicalOR(t *testing.T) {
	result, err := ParseTraceQL(`{ .a = "x" || .b = "y" }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, OpOr, be.Op)
}

// --- Status and kind literals ---

func TestParseTraceQL_StatusLiteral(t *testing.T) {
	queries := []string{
		`{ status = ok }`,
		`{ status = error }`,
		`{ status = unset }`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, err := ParseTraceQL(q)
			require.NoError(t, err)
			_, ok := result.(*FilterExpression)
			assert.True(t, ok)
		})
	}
}

func TestParseTraceQL_KindLiteral(t *testing.T) {
	queries := []string{
		`{ kind = server }`,
		`{ kind = client }`,
		`{ kind = internal }`,
		`{ kind = producer }`,
		`{ kind = consumer }`,
		`{ kind = unspecified }`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, err := ParseTraceQL(q)
			require.NoError(t, err)
			_, ok := result.(*FilterExpression)
			assert.True(t, ok)
		})
	}
}

// --- Colon syntax for intrinsics ---

func TestParseTraceQL_ColonSyntax(t *testing.T) {
	queries := []string{
		`{ span:name = "op" }`,
		`{ trace:id = "abc" }`,
		`{ log:timestamp > 0 }`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, err := ParseTraceQL(q)
			require.NoError(t, err)
			_, ok := result.(*FilterExpression)
			assert.True(t, ok)
		})
	}
}

// --- Float literal ---

func TestParseTraceQL_FloatLiteral(t *testing.T) {
	result, err := ParseTraceQL(`{ .score > 3.14 }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitFloat, lit.Type)
	assert.InDelta(t, 3.14, lit.Value.(float64), 1e-9)
}

// --- Int literal ---

func TestParseTraceQL_IntLiteral(t *testing.T) {
	result, err := ParseTraceQL(`{ .count > 42 }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	lit, ok := be.Right.(*LiteralExpr)
	require.True(t, ok)
	assert.Equal(t, LitInt, lit.Type)
	assert.Equal(t, int64(42), lit.Value)
}

// --- Bool literal ---

func TestParseTraceQL_BoolLiteral(t *testing.T) {
	for _, boolStr := range []string{"true", "false"} {
		query := `{ .active = ` + boolStr + ` }`
		result, err := ParseTraceQL(query)
		require.NoErrorf(t, err, "query %q", query)
		fe, ok := result.(*FilterExpression)
		require.True(t, ok)
		be, ok := fe.Expr.(*BinaryExpr)
		require.True(t, ok)
		lit, ok := be.Right.(*LiteralExpr)
		require.True(t, ok)
		assert.Equal(t, LitBool, lit.Type, "query %q", query)
	}
}

// --- isDurationUnit ---

func TestIsDurationUnit(t *testing.T) {
	validUnits := []string{"ns", "us", "µs", "ms", "s", "m", "h"}
	for _, u := range validUnits {
		assert.Truef(t, isDurationUnit(u), "expected %q to be a duration unit", u)
	}
	invalidUnits := []string{"", "d", "w", "y", "sec", "min"}
	for _, u := range invalidUnits {
		assert.Falsef(t, isDurationUnit(u), "expected %q not to be a duration unit", u)
	}
}

// --- Metrics query parsing ---

func TestParseMetricsQuery_CountOverTime(t *testing.T) {
	result, err := ParseTraceQL(`{} | count_over_time()`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok, "expected *MetricsQuery, got %T", result)
	assert.Equal(t, "count_over_time", mq.Pipeline.Aggregate.Name)
	assert.Empty(t, mq.Pipeline.By)
}

func TestParseMetricsQuery_Rate(t *testing.T) {
	result, err := ParseTraceQL(`{} | rate()`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "rate", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_WithFilter(t *testing.T) {
	result, err := ParseTraceQL(`{ span.http.status = "200" } | count_over_time()`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	require.NotNil(t, mq.Filter)
}

func TestParseMetricsQuery_WithByClause(t *testing.T) {
	result, err := ParseTraceQL(`{} | count_over_time() by (resource.service.name)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, []string{"resource.service.name"}, mq.Pipeline.By)
}

func TestParseMetricsQuery_WithMultipleByFields(t *testing.T) {
	result, err := ParseTraceQL(`{} | count_over_time() by (resource.service.name, span.http.method)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, []string{"resource.service.name", "span.http.method"}, mq.Pipeline.By)
}

func TestParseMetricsQuery_Avg(t *testing.T) {
	result, err := ParseTraceQL(`{} | avg(span.duration) by (resource.service.name)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "avg", mq.Pipeline.Aggregate.Name)
	assert.Equal(t, "span.duration", mq.Pipeline.Aggregate.Field)
}

func TestParseMetricsQuery_Min(t *testing.T) {
	result, err := ParseTraceQL(`{} | min(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "min", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_Max(t *testing.T) {
	result, err := ParseTraceQL(`{} | max(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "max", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_Sum(t *testing.T) {
	result, err := ParseTraceQL(`{} | sum(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "sum", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_QuantileOverTime(t *testing.T) {
	result, err := ParseTraceQL(`{} | quantile_over_time(span.duration, 0.99)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "quantile_over_time", mq.Pipeline.Aggregate.Name)
	assert.Equal(t, "span.duration", mq.Pipeline.Aggregate.Field)
	assert.InDelta(t, 0.99, mq.Pipeline.Aggregate.Quantile, 1e-9)
}

func TestParseMetricsQuery_AvgOverTime(t *testing.T) {
	result, err := ParseTraceQL(`{} | avg_over_time(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "avg", mq.Pipeline.Aggregate.Name) // normalized
}

func TestParseMetricsQuery_MinOverTime(t *testing.T) {
	result, err := ParseTraceQL(`{} | min_over_time(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "min", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_MaxOverTime(t *testing.T) {
	result, err := ParseTraceQL(`{} | max_over_time(span.duration)`)
	require.NoError(t, err)
	mq, ok := result.(*MetricsQuery)
	require.True(t, ok)
	assert.Equal(t, "max", mq.Pipeline.Aggregate.Name)
}

func TestParseMetricsQuery_ErrorCases(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{"unknown aggregate function", `{} | unknown_func()`},
		{"count with args", `{} | count_over_time(span.duration)`},
		{"avg without arg", `{} | avg()`},
		{"quantile without enough args", `{} | quantile_over_time(span.duration)`},
		{"invalid quantile value", `{} | quantile_over_time(span.duration, abc)`},
		{"quantile out of range", `{} | quantile_over_time(span.duration, 1.5)`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTraceQL(tc.query)
			assert.Error(t, err, "query %q should return an error", tc.query)
		})
	}
}

func TestParseMetricsQuery_PipeInsideBraces_IsFilter(t *testing.T) {
	// A pipe inside braces should not be treated as a metrics separator
	result, err := ParseTraceQL(`{ .a = "b" || .c = "d" }`)
	require.NoError(t, err)
	_, ok := result.(*FilterExpression)
	assert.True(t, ok, "expected FilterExpression for query with || inside braces")
}

// --- StructuralOp.String ---

func TestStructuralOp_String(t *testing.T) {
	cases := []struct {
		want string
		op   StructuralOp
	}{
		{">>", OpDescendant},
		{">", OpChild},
		{"~", OpSibling},
		{"<<", OpAncestor},
		{"<", OpParent},
		{"!~", OpNotSibling},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.op.String())
	}
}

func TestStructuralOp_StringUnknown(t *testing.T) {
	var unknown StructuralOp = 999
	assert.Equal(t, "UNKNOWN", unknown.String())
}

// --- VectorExpr parsing ---

func TestVectorAI_Simple(t *testing.T) {
	result, err := ParseTraceQL(`{ VECTOR_AI("database connection errors") }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAI, ve.Mode)
	assert.Equal(t, "database connection errors", ve.QueryText)
	assert.Empty(t, ve.KVPairs)
}

func TestVectorAI_WithMap(t *testing.T) {
	result, err := ParseTraceQL(`{ VECTOR_AI("database errors", {"env": "prod", "svc": "auth"}) }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAI, ve.Mode)
	assert.Equal(t, "database errors", ve.QueryText)
	assert.Equal(t, map[string]string{"env": "prod", "svc": "auth"}, ve.KVPairs)
}

func TestVectorAI_Combined(t *testing.T) {
	result, err := ParseTraceQL(`{ resource.service.name = "auth" && VECTOR_AI("connection timeout") }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, OpAnd, be.Op)
	_, ok = be.Left.(*BinaryExpr)
	require.True(t, ok)
	ve, ok := be.Right.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAI, ve.Mode)
	assert.Equal(t, "connection timeout", ve.QueryText)
}

func TestVectorAI_EmptyMapLiteral(t *testing.T) {
	result, err := ParseTraceQL(`{ VECTOR_AI("text", {}) }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAI, ve.Mode)
	assert.Equal(t, "text", ve.QueryText)
	assert.Empty(t, ve.KVPairs)
}

func TestVectorAI_MissingQueryText(t *testing.T) {
	_, err := ParseTraceQL(`{ VECTOR_AI() }`)
	assert.Error(t, err)
}

func TestVectorAI_MissingCloseParen(t *testing.T) {
	_, err := ParseTraceQL(`{ VECTOR_AI("text" }`)
	assert.Error(t, err)
}

func TestVectorAI_InvalidMapLiteral(t *testing.T) {
	_, err := ParseTraceQL(`{ VECTOR_AI("text", {notastring: "val"}) }`)
	assert.Error(t, err)
}

func TestVectorAI_BackwardCompat_VECTOR(t *testing.T) {
	// Plain VECTOR() should still work as alias for VECTOR_AI()
	result, err := ParseTraceQL(`{ VECTOR("legacy query") }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAI, ve.Mode)
	assert.Equal(t, "legacy query", ve.QueryText)
}

func TestVectorAll_Simple(t *testing.T) {
	result, err := ParseTraceQL(`{ VECTOR_ALL("find database errors") }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAll, ve.Mode)
	assert.Equal(t, "find database errors", ve.QueryText)
}

func TestVectorAll_WithMap(t *testing.T) {
	result, err := ParseTraceQL(`{ VECTOR_ALL("timeout errors", {"env": "prod"}) }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	ve, ok := fe.Expr.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAll, ve.Mode)
	assert.Equal(t, "timeout errors", ve.QueryText)
	assert.Equal(t, map[string]string{"env": "prod"}, ve.KVPairs)
}

func TestVectorAll_Combined(t *testing.T) {
	result, err := ParseTraceQL(`{ resource.service.name = "auth" && VECTOR_ALL("login errors") }`)
	require.NoError(t, err)
	fe, ok := result.(*FilterExpression)
	require.True(t, ok)
	be, ok := fe.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, OpAnd, be.Op)
	ve, ok := be.Right.(*VectorExpr)
	require.True(t, ok)
	assert.Equal(t, VectorModeAll, ve.Mode)
	assert.Equal(t, "login errors", ve.QueryText)
}

func TestVectorAll_EmptyQuery_Error(t *testing.T) {
	_, err := ParseTraceQL(`{ VECTOR_ALL("") }`)
	assert.Error(t, err)
}

func TestVectorAll_NoArgs_Error(t *testing.T) {
	_, err := ParseTraceQL(`{ VECTOR_ALL() }`)
	assert.Error(t, err)
}
