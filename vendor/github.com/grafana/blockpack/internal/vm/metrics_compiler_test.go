package vm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompileTraceQLMetricsSimple(t *testing.T) {
	query := "{} | count_over_time()"
	startTime := int64(1000000000)
	endTime := int64(2000000000)

	program, spec, err := CompileTraceQLMetrics(query, startTime, endTime)
	if err != nil {
		t.Fatalf("compilation failed: %v", err)
	}

	if program == nil {
		t.Error("expected program, got nil")
	}

	if spec == nil {
		t.Fatal("expected QuerySpec, got nil")
	}

	if spec.Aggregate.Function != FuncNameCOUNT {
		t.Errorf("expected COUNT function, got %s", spec.Aggregate.Function)
	}

	if !spec.Filter.IsMatchAll {
		t.Error("expected match-all filter for empty filter")
	}

	if !spec.TimeBucketing.Enabled {
		t.Error("expected time bucketing to be enabled")
	}
}

func TestCompileTraceQLMetricsWithFilter(t *testing.T) {
	query := `{ span.status = "error" } | count_over_time() by (resource.service.name)`
	startTime := int64(1000000000)
	endTime := int64(2000000000)

	program, spec, err := CompileTraceQLMetrics(query, startTime, endTime)
	if err != nil {
		t.Fatalf("compilation failed: %v", err)
	}

	if program == nil {
		t.Fatal("expected program, got nil")
	}

	if spec.Aggregate.Function != FuncNameCOUNT {
		t.Errorf("expected COUNT function, got %s", spec.Aggregate.Function)
	}

	if len(spec.Aggregate.GroupBy) != 1 {
		t.Errorf("expected 1 group by field, got %d", len(spec.Aggregate.GroupBy))
	}

	if program.Predicates == nil {
		t.Error("expected filter predicates, got none")
	}
}

// TestMetricsPredicates_IntrinsicNormalization verifies that span:duration (colon form)
// is produced — the old getFieldPath path produced "span.duration" (dot form) which
// didn't match the stored column name.
func TestMetricsPredicates_IntrinsicNormalization(t *testing.T) {
	prog, _, err := CompileTraceQLMetrics(`{ span.duration > 5ms } | count_over_time()`, 0, int64(1e9))
	require.NoError(t, err)
	require.NotNil(t, prog)
	require.NotNil(t, prog.Predicates, "predicates must not be nil")
	found := false
	for _, n := range prog.Predicates.Nodes {
		if n.Column == "span:duration" {
			found = true
		}
	}
	assert.True(t, found, "expected predicate column 'span:duration' (colon form), got: %v", prog.Predicates.Nodes)
}

// TestMetricsPredicates_ORComposite verifies that OR queries produce non-nil predicates.
// The old extractPredicates silently dropped OR subtrees; extractTraceQLPredicates handles them.
func TestMetricsPredicates_ORComposite(t *testing.T) {
	prog, _, err := CompileTraceQLMetrics(
		`{ .env = "prod" || .env = "staging" } | count_over_time()`,
		0, int64(1e9),
	)
	require.NoError(t, err)
	require.NotNil(t, prog)
	assert.NotNil(t, prog.Predicates, "OR query should produce non-nil predicates")
}

func TestCompileTraceQLMetricsAvg(t *testing.T) {
	query := `{ resource.service.name = "api" } | avg(span.duration)`
	startTime := int64(1000000000)
	endTime := int64(2000000000)

	_, spec, err := CompileTraceQLMetrics(query, startTime, endTime)
	if err != nil {
		t.Fatalf("compilation failed: %v", err)
	}

	if spec.Aggregate.Function != FuncNameAVG {
		t.Errorf("expected AVG function, got %s", spec.Aggregate.Function)
	}

	if spec.Aggregate.Field == "" {
		t.Error("expected aggregate field for AVG, got empty")
	}
}
