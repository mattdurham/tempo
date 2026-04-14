package traceqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseStructuralQuery verifies that ParseTraceQL correctly identifies and parses
// all 6 structural operators, returning a *StructuralQuery with the correct Op and
// non-nil Left/Right when filter expressions are present.
func TestParseStructuralQuery(t *testing.T) {
	tests := []struct {
		query    string
		wantOp   StructuralOp
		hasLeft  bool
		hasRight bool
	}{
		{
			query:    `{ name = "A" } >> { name = "B" }`,
			wantOp:   OpDescendant,
			hasLeft:  true,
			hasRight: true,
		},
		{
			query:    `{ name = "A" } > { name = "B" }`,
			wantOp:   OpChild,
			hasLeft:  true,
			hasRight: true,
		},
		{
			query:    `{ name = "A" } ~ { name = "B" }`,
			wantOp:   OpSibling,
			hasLeft:  true,
			hasRight: true,
		},
		{
			query:    `{ name = "A" } << { name = "B" }`,
			wantOp:   OpAncestor,
			hasLeft:  true,
			hasRight: true,
		},
		{
			query:    `{ name = "A" } < { name = "B" }`,
			wantOp:   OpParent,
			hasLeft:  true,
			hasRight: true,
		},
		{
			query:    `{ name = "A" } !~ { name = "B" }`,
			wantOp:   OpNotSibling,
			hasLeft:  true,
			hasRight: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			result, err := ParseTraceQL(tc.query)
			require.NoError(t, err)

			sq, ok := result.(*StructuralQuery)
			require.True(t, ok, "expected *StructuralQuery, got %T", result)
			assert.Equal(t, tc.wantOp, sq.Op, "operator mismatch")

			if tc.hasLeft {
				require.NotNil(t, sq.Left, "expected non-nil Left filter")
				assert.NotNil(t, sq.Left.Expr, "expected non-nil Left.Expr (filter must not be empty wrapper)")
			} else {
				assert.Nil(t, sq.Left, "expected nil Left filter")
			}
			if tc.hasRight {
				require.NotNil(t, sq.Right, "expected non-nil Right filter")
				assert.NotNil(t, sq.Right.Expr, "expected non-nil Right.Expr (filter must not be empty wrapper)")
			} else {
				assert.Nil(t, sq.Right, "expected nil Right filter")
			}
		})
	}
}

// TestParseStructuralQuery_EmptyFilters verifies that empty {} on either side
// is accepted and produces a non-nil StructuralQuery.
func TestParseStructuralQuery_EmptyFilters(t *testing.T) {
	queries := []struct {
		query  string
		wantOp StructuralOp
	}{
		{`{} >> {}`, OpDescendant},
		{`{} > {}`, OpChild},
		{`{} ~ {}`, OpSibling},
		{`{} << {}`, OpAncestor},
		{`{} < {}`, OpParent},
		{`{} !~ {}`, OpNotSibling},
	}

	for _, tc := range queries {
		t.Run(tc.query, func(t *testing.T) {
			result, err := ParseTraceQL(tc.query)
			require.NoError(t, err)

			sq, ok := result.(*StructuralQuery)
			require.True(t, ok, "expected *StructuralQuery, got %T", result)
			assert.Equal(t, tc.wantOp, sq.Op)
		})
	}
}

// TestParseStructuralQuery_OperatorString verifies StructuralOp.String() roundtrips correctly.
func TestParseStructuralQuery_OperatorString(t *testing.T) {
	ops := []struct {
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
	for _, tc := range ops {
		assert.Equal(t, tc.want, tc.op.String())
	}
}

// TestParseTraceQL_FilterRegression verifies that filter queries still parse
// as *FilterExpression (not *StructuralQuery) after structural parsing was added.
func TestParseTraceQL_FilterRegression(t *testing.T) {
	filterQueries := []string{
		`{ name = "hello" }`,
		`{ duration > 1s }`,
		`{ .http.status_code = 200 && resource.service.name = "svc" }`,
		`{ status = error }`,
		`{}`,
	}

	for _, q := range filterQueries {
		t.Run(q, func(t *testing.T) {
			result, err := ParseTraceQL(q)
			require.NoError(t, err)
			_, ok := result.(*FilterExpression)
			assert.True(t, ok, "expected *FilterExpression for filter query %q, got %T", q, result)
		})
	}
}

// TestParseStructuralQuery_NestedError verifies that chained structural queries
// without parentheses return an error rather than silently mis-parsing.
func TestParseStructuralQuery_NestedError(t *testing.T) {
	_, err := ParseTraceQL(`{ name = "A" } >> { name = "B" } >> { name = "C" }`)
	assert.Error(t, err, "chained structural query without parens must return an error")
}

// TestParseFieldPath_LogScope verifies that dot-scope syntax with "log" is accepted.
func TestParseFieldPath_LogScope(t *testing.T) {
	result, err := ParseTraceQL(`{ log.timestamp > 0 }`)
	require.NoError(t, err, "log.timestamp should parse without error")
	fe, ok := result.(*FilterExpression)
	require.True(t, ok, "expected *FilterExpression")
	require.NotNil(t, fe)
}
