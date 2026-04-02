package vm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/traceqlparser"
)

// --- helpers ---

func compilePredicates(t *testing.T, query string) *QueryPredicates {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoErrorf(t, err, "ParseTraceQL(%q)", query)
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	require.Truef(t, ok, "expected FilterExpression for %q", query)
	prog, err := CompileTraceQLFilter(fe)
	require.NoErrorf(t, err, "CompileTraceQLFilter(%q)", query)
	require.NotNil(t, prog.Predicates)
	return prog.Predicates
}

// findLeafNode finds a leaf RangeNode with the given column in a flat slice.
func findLeafNode(nodes []RangeNode, col string) (RangeNode, bool) {
	for _, n := range nodes {
		if len(n.Children) == 0 && n.Column == col {
			return n, true
		}
		// Search OR/AND composites recursively.
		if len(n.Children) > 0 {
			if found, ok := findLeafNode(n.Children, col); ok {
				return found, ok
			}
		}
	}
	return RangeNode{}, false
}

// findORNode finds an OR composite node in a flat slice.
func findORNode(nodes []RangeNode) (RangeNode, bool) {
	for _, n := range nodes {
		if n.IsOR && len(n.Children) > 0 {
			return n, true
		}
	}
	return RangeNode{}, false
}

// orNodeColumns returns the Column values of the direct children of an OR node.
func orNodeColumns(n RangeNode) []string {
	cols := make([]string, 0, len(n.Children))
	for _, c := range n.Children {
		cols = append(cols, c.Column)
	}
	return cols
}

// --- normalizeAttributePath ---

func TestNormalizeAttributePath(t *testing.T) {
	tests := []struct {
		scope, name string
		want        string
	}{
		// Span intrinsics: unscoped and scoped dot-forms both map to colon-form
		{"", "name", "span:name"},
		{"span", "name", "span:name"},
		{"", "kind", "span:kind"},
		{"span", "kind", "span:kind"},
		{"", "status", "span:status"},
		{"span", "status", "span:status"},
		{"", "duration", "span:duration"},
		{"span", "duration", "span:duration"},
		{"", "start", "span:start"},
		{"span", "start", "span:start"},
		{"", "end", "span:end"},
		{"span", "end", "span:end"},
		{"", "status_message", "span:status_message"},
		{"span", "status_message", "span:status_message"},
		// Trace intrinsics
		{"trace", "id", "trace:id"},
		{"span", "id", "span:id"},
		{"span", "parent_id", "span:parent_id"},
		// Colon-syntax intrinsics parsed as FieldExpr{Scope, Name}
		{"trace", "state", "trace:state"},
		{"resource", "schema_url", "resource:schema_url"},
		{"scope", "schema_url", "scope:schema_url"},
		// Regular user attributes
		{"span", "http.method", "span.http.method"},
		{"resource", "service.name", "resource.service.name"},
		// Unscoped user attribute: returned as bare name, caller handles OR expansion
		{"", "http.method", "http.method"},
		{"", "service.name", "service.name"},
	}
	for _, tc := range tests {
		got := normalizeAttributePath(tc.scope, tc.name)
		assert.Equalf(t, tc.want, got, "normalizeAttributePath(%q, %q)", tc.scope, tc.name)
	}
}

// --- unscoped attribute predicate extraction ---

// TC-US-01: equality on unscoped attr produces an OR composite covering all three scopes.
func TestPredicates_UnscopedEquality_ProducesORNode(t *testing.T) {
	preds := compilePredicates(t, `{ .http.method = "GET" }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok, "expected an OR composite node for unscoped equality")
	cols := orNodeColumns(orNode)
	assert.ElementsMatch(t, []string{"resource.http.method", "span.http.method", "log.http.method"}, cols)
}

// TC-US-02: unscoped equality does NOT produce a leaf directly on the unscoped name.
func TestPredicates_UnscopedEquality_NoUnscopedLeaf(t *testing.T) {
	preds := compilePredicates(t, `{ .http.method = "GET" }`)

	_, found := findLeafNode(preds.Nodes, "http.method")
	assert.False(t, found, "unscoped name must not appear as a leaf column")
}

// TC-US-03: unscoped equality puts all three scoped columns into Columns.
func TestPredicates_UnscopedEquality_ColumnsContainsAllScopes(t *testing.T) {
	preds := compilePredicates(t, `{ .http.method = "GET" }`)
	assert.Contains(t, preds.Columns, "resource.http.method")
	assert.Contains(t, preds.Columns, "span.http.method")
	assert.Contains(t, preds.Columns, "log.http.method")
}

// TC-US-04: service.name is treated as unscoped (expands to all three scopes).
func TestPredicates_UnscopedServiceName_ExpandsAllScopes(t *testing.T) {
	preds := compilePredicates(t, `{ .service.name = "svc-a" }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok, "expected OR composite for unscoped .service.name")
	cols := orNodeColumns(orNode)
	assert.ElementsMatch(t, []string{"resource.service.name", "span.service.name", "log.service.name"}, cols)
}

// TC-US-05: negation on unscoped attr adds all three scoped columns to Columns but no nodes.
func TestPredicates_UnscopedNegation_AddsColumnsOnly(t *testing.T) {
	preds := compilePredicates(t, `{ .env != "prod" }`)

	// Negations produce no pruning nodes.
	for _, n := range preds.Nodes {
		if n.IsOR {
			for _, c := range n.Children {
				assert.NotEqual(t, "resource.env", c.Column, "negation must not produce pruning node")
			}
		}
	}
	// But all three scoped columns appear in Columns for row-level decode.
	assert.Contains(t, preds.Columns, "resource.env")
	assert.Contains(t, preds.Columns, "span.env")
	assert.Contains(t, preds.Columns, "log.env")
}

// TC-US-06: range predicate on unscoped attr produces an OR composite with Min/Max set.
func TestPredicates_UnscopedRange_ProducesORNode(t *testing.T) {
	preds := compilePredicates(t, `{ .retry_count > 3 }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok, "expected OR composite for unscoped range predicate")
	cols := orNodeColumns(orNode)
	assert.ElementsMatch(t, []string{"resource.retry_count", "span.retry_count", "log.retry_count"}, cols)
	// Each child should have Min set (> 3).
	for _, child := range orNode.Children {
		assert.NotNil(t, child.Min, "OR child must have Min set for > predicate")
	}
}

// TC-US-07: regex predicate on unscoped attr produces an OR composite with Pattern set.
func TestPredicates_UnscopedRegex_ProducesORNode(t *testing.T) {
	preds := compilePredicates(t, `{ .http.url =~ "https://.*" }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok, "expected OR composite for unscoped regex predicate")
	cols := orNodeColumns(orNode)
	assert.ElementsMatch(t, []string{"resource.http.url", "span.http.url", "log.http.url"}, cols)
	for _, child := range orNode.Children {
		assert.Equal(t, "https://.*", child.Pattern, "OR child must have Pattern set for =~ predicate")
	}
}

// TC-US-08: multiple unscoped attrs each get their own OR composite node.
func TestPredicates_MultipleUnscopedAttrs_EachHaveORNode(t *testing.T) {
	preds := compilePredicates(t, `{ .env = "prod" && .region = "us-east" }`)

	// Both OR composites should exist in Nodes.
	require.GreaterOrEqual(t, len(preds.Nodes), 2, "expected at least 2 nodes for two AND clauses")
	orCount := 0
	var envCols, regionCols []string
	for _, n := range preds.Nodes {
		if n.IsOR {
			orCount++
			cols := orNodeColumns(n)
			for _, c := range cols {
				switch c {
				case "resource.env", "span.env", "log.env":
					envCols = cols
				case "resource.region", "span.region", "log.region":
					regionCols = cols
				}
			}
		}
	}
	assert.Equal(t, 2, orCount, "expected 2 OR composite nodes")
	assert.ElementsMatch(t, []string{"resource.env", "span.env", "log.env"}, envCols)
	assert.ElementsMatch(t, []string{"resource.region", "span.region", "log.region"}, regionCols)
}

// TC-US-09: scoped attr produces a leaf node, not an OR composite.
func TestPredicates_ScopedAttr_ProducesLeafNode(t *testing.T) {
	preds := compilePredicates(t, `{ span.http.method = "GET" }`)

	leaf, ok := findLeafNode(preds.Nodes, "span.http.method")
	require.True(t, ok, "expected leaf node for scoped attr")
	assert.Len(t, leaf.Values, 1)
	assert.Equal(t, "GET", leaf.Values[0].Data)
	// Must not produce OR composite.
	_, isOR := findORNode(preds.Nodes)
	assert.False(t, isOR, "scoped attr must not produce OR composite")
}

// TC-US-10: OR query produces an OR composite at the top level wrapping both sides.
func TestPredicates_ORQuery_ProducesORNode(t *testing.T) {
	preds := compilePredicates(t, `{ .env = "prod" || .env = "staging" }`)

	_, ok := findORNode(preds.Nodes)
	assert.True(t, ok, "OR query must produce an OR composite node")
}

// TC-LOG-01: unscoped equality on a log-style attribute includes log. scope in OR node.
func TestPredicates_UnscopedEquality_IncludesLogColumn(t *testing.T) {
	preds := compilePredicates(t, `{ .level = "error" }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok)
	cols := orNodeColumns(orNode)
	assert.Contains(t, cols, "log.level")
	assert.Contains(t, cols, "resource.level")
	assert.Contains(t, cols, "span.level")
}

// TC-LOG-02: log intrinsics in dot-form normalize to colon-form.
func TestNormalizeAttributePath_LogIntrinsics(t *testing.T) {
	tests := []struct {
		scope, name, want string
	}{
		{"log", "timestamp", "log:timestamp"},
		{"log", "body", "log:body"},
		{"log", "severity_number", "log:severity_number"},
		{"log", "severity_text", "log:severity_text"},
		{"log", "trace_id", "log:trace_id"},
		{"log", "span_id", "log:span_id"},
		{"log", "flags", "log:flags"},
		{"log", "observed_timestamp", "log:observed_timestamp"},
	}
	for _, tc := range tests {
		got := normalizeAttributePath(tc.scope, tc.name)
		assert.Equalf(t, tc.want, got, "normalizeAttributePath(%q, %q)", tc.scope, tc.name)
	}
}

// TestValuesEqual_TypeBytes verifies that valuesEqual correctly handles TypeBytes
// so that trace:id / span:id VM comparisons are not silently wrong.
func TestValuesEqual_TypeBytes(t *testing.T) {
	a := Value{Type: TypeBytes, Data: []byte{0x01, 0x02, 0x03}}
	b := Value{Type: TypeBytes, Data: []byte{0x01, 0x02, 0x03}}
	c := Value{Type: TypeBytes, Data: []byte{0x01, 0x02, 0x04}}

	assert.True(t, valuesEqual(a, b), "equal byte slices should be equal")
	assert.False(t, valuesEqual(a, c), "different byte slices should not be equal")
	assert.False(t, valuesEqual(a, Value{Type: TypeString, Data: "abc"}), "different types should not be equal")
}

// TC-US-11: unscoped equality carries actual values in OR children (not just column names).
// This is the key improvement over the old UnscopedColumnNames (bloom-only, no values).
func TestPredicates_UnscopedEquality_ChildrenCarryValues(t *testing.T) {
	preds := compilePredicates(t, `{ .http.method = "GET" }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok)
	for _, child := range orNode.Children {
		require.Len(t, child.Values, 1, "each OR child must carry the equality value")
		assert.Equal(t, TypeString, child.Values[0].Type)
		assert.Equal(t, "GET", child.Values[0].Data)
	}
}

// TC-US-12: unscoped range carries Min/Max values with correct type in each child.
func TestPredicates_UnscopedRange_ChildrenCarryMinMax(t *testing.T) {
	preds := compilePredicates(t, `{ .latency > 100 }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok)
	for _, child := range orNode.Children {
		require.NotNil(t, child.Min, "each child must have Min for > predicate")
		assert.Equal(t, TypeInt, child.Min.Type)
		assert.Equal(t, int64(100), child.Min.Data)
		assert.Nil(t, child.Max, "> predicate should not set Max")
	}
}

// TC-US-13: .a = "x" && .b = "y" produces TWO separate OR composites (AND-combined).
// Each OR has 3 children (resource, span, log) carrying their respective values.
func TestPredicates_TwoUnscopedAND_TwoORComposites(t *testing.T) {
	preds := compilePredicates(t, `{ .a = "x" && .b = "y" }`)

	orCount := 0
	for _, n := range preds.Nodes {
		if n.IsOR && len(n.Children) > 0 {
			orCount++
			// Verify each child carries its value.
			for _, c := range n.Children {
				require.Len(t, c.Values, 1)
			}
		}
	}
	assert.Equal(t, 2, orCount, "two unscoped ANDs must produce two OR composites")
}

// TC-US-14: scoped + unscoped mixed: span.http.method="GET" && .env="prod".
// Must produce one leaf (span.http.method) and one OR composite (env).
func TestPredicates_MixedScopedAndUnscoped(t *testing.T) {
	preds := compilePredicates(t, `{ span.http.method = "GET" && .env = "prod" }`)

	// Find the scoped leaf.
	leaf, ok := findLeafNode(preds.Nodes, "span.http.method")
	require.True(t, ok, "scoped attr must produce a leaf")
	require.Len(t, leaf.Values, 1)
	assert.Equal(t, "GET", leaf.Values[0].Data)

	// Find the unscoped OR composite.
	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok, "unscoped attr must produce an OR composite")
	cols := orNodeColumns(orNode)
	assert.ElementsMatch(t, []string{"resource.env", "span.env", "log.env"}, cols)
	for _, c := range orNode.Children {
		require.Len(t, c.Values, 1)
		assert.Equal(t, "prod", c.Values[0].Data)
	}
}

// TC-US-15: trace:id with valid hex produces a leaf with TypeBytes values.
func TestPredicates_TraceID_ProducesLeafWithBytes(t *testing.T) {
	preds := compilePredicates(t, `{ trace:id = "0102030405060708090a0b0c0d0e0f10" }`)

	leaf, ok := findLeafNode(preds.Nodes, "trace:id")
	require.True(t, ok, "trace:id should produce a leaf node")
	require.Len(t, leaf.Values, 1)
	assert.Equal(t, TypeBytes, leaf.Values[0].Type, "trace:id value must be decoded to bytes")
}

// TC-US-16: unscoped LT predicate .x < 50 produces OR with Max set.
func TestPredicates_UnscopedLT_ChildrenCarryMax(t *testing.T) {
	preds := compilePredicates(t, `{ .x < 50 }`)

	orNode, ok := findORNode(preds.Nodes)
	require.True(t, ok)
	for _, child := range orNode.Children {
		require.NotNil(t, child.Max, "each child must have Max for < predicate")
		assert.Equal(t, TypeInt, child.Max.Type)
		assert.Equal(t, int64(50), child.Max.Data)
		assert.Nil(t, child.Min, "< predicate should not set Min")
	}
}

// TC-LOG-03: log:timestamp must produce a leaf node with Min set, not an OR composite.
func TestPredicates_LogIntrinsic_ProducesLeafRangeNode(t *testing.T) {
	preds := compilePredicates(t, `{ log:timestamp > 1000 }`)

	// Must not produce OR composite.
	_, isOR := findORNode(preds.Nodes)
	assert.False(t, isOR, "log intrinsic must not be unscoped-expanded")

	// Must produce a leaf with Min set.
	leaf, ok := findLeafNode(preds.Nodes, "log:timestamp")
	require.True(t, ok, "log:timestamp should produce a leaf range node")
	assert.NotNil(t, leaf.Min, "range predicate > 1000 should set Min")
}

// --- ScanEqualAny / gatherOrEqualAny tests ---

// TC-OR-01: same-column OR equality compiles without error and produces a ColumnPredicate.
// The predicate is non-nil, indicating gatherOrEqualAny collapsed the OR tree.
func TestCompiler_SameColumnOREquality_CompilesOK(t *testing.T) {
	prog, err := CompileTraceQLFilter(
		mustParseFilter(t, `{ resource.service.name = "grafana" || resource.service.name = "loki" }`),
	)
	require.NoError(t, err)
	require.NotNil(t, prog.ColumnPredicate, "same-column OR must produce a ColumnPredicate")
}

// TC-OR-02: three-arm same-column OR compiles successfully.
// Note: flattenORLeaves (nested OR composite handling) lives in predicates.go, not the compiler.
func TestCompiler_ThreeArmSameColumnOR_CompilesOK(t *testing.T) {
	prog, err := CompileTraceQLFilter(
		mustParseFilter(
			t,
			`{ resource.service.name = "a" || resource.service.name = "b" || resource.service.name = "c" }`,
		),
	)
	require.NoError(t, err)
	require.NotNil(t, prog.ColumnPredicate, "three-arm same-column OR must compile successfully")
}

// TC-OR-03: mixed-column OR also compiles (falls back to union path, no ScanEqualAny).
func TestCompiler_MixedColumnOR_CompilesOK(t *testing.T) {
	prog, err := CompileTraceQLFilter(
		mustParseFilter(t, `{ resource.service.name = "grafana" || span.http.method = "GET" }`),
	)
	require.NoError(t, err)
	require.NotNil(t, prog.ColumnPredicate, "mixed-column OR must compile successfully")
}

func mustParseFilter(t *testing.T, q string) *traceqlparser.FilterExpression {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(q)
	require.NoError(t, err)
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	require.True(t, ok)
	return fe
}
