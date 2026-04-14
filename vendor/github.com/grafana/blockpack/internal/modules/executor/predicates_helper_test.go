package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// mustCompileProgram compiles a TraceQL query string into a *vm.Program for internal tests.
func mustCompileProgram(t *testing.T, query string) *vm.Program {
	t.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	require.NoErrorf(t, err, "ParseTraceQL %q", query)
	filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
	require.Truef(t, ok, "expected FilterExpression for %q", query)
	prog, err := vm.CompileTraceQLFilter(filterExpr)
	require.NoErrorf(t, err, "CompileTraceQLFilter %q", query)
	return prog
}

// EX-INT-11: Unit test for hasSomeIntrinsicPredicates helper function.
func TestHasSomeIntrinsicPredicates(t *testing.T) {
	cases := []struct {
		desc  string
		query string // empty means nil program
		want  bool
	}{
		{"intrinsic service.name", `{ resource.service.name = "svc" }`, true},
		{"intrinsic span:duration", `{ span:duration > 1000 }`, true},
		{"non-intrinsic span.http.method", `{ span.http.method = "GET" }`, false},
		{"AND intrinsic + non-intrinsic", `{ resource.service.name = "svc" && span.http.method = "GET" }`, true},
		{"OR intrinsic + non-intrinsic", `{ span.http.method = "GET" || resource.service.name = "svc" }`, true},
		{"match-all", `{}`, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			var prog *vm.Program
			if tc.query != "" {
				prog = mustCompileProgram(t, tc.query)
			}
			got := hasSomeIntrinsicPredicates(prog)
			assert.Equal(t, tc.want, got, "query: %s", tc.query)
		})
	}

	// nil program case
	t.Run("nil program", func(t *testing.T) {
		assert.False(t, hasSomeIntrinsicPredicates(nil))
	})
}

// TestExtractLiteralAlternatives verifies that extractLiteralAlternatives correctly
// classifies patterns. EX-18.
func TestExtractLiteralAlternatives(t *testing.T) {
	cases := []struct {
		pattern string
		want    []string // nil means "not pure literals"
	}{
		{"cluster-0|cluster-1", []string{"cluster-0", "cluster-1"}},
		{"prod|staging", []string{"prod", "staging"}},
		{"error|warn|info", []string{"error", "warn", "info"}},
		{"debug", []string{"debug"}},
		{"debug.*", nil},
		{"cluster-0.*|cluster-1", nil},
		{"(?i)error|warn", nil},
		{"[a-z]+", nil},
		{"", nil},
		{"foo|bar|baz.qux", nil},
		{"foo|", nil},
		{`cluster\-0|cluster\-1`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.pattern, func(t *testing.T) {
			got := extractLiteralAlternatives(tc.pattern)
			assert.Equal(t, tc.want, got)
		})
	}
}
