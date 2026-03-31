package logqlparser

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	regexp "github.com/coregx/coregex"

	"github.com/grafana/blockpack/internal/vm"
	"github.com/stretchr/testify/require"
)

// mockRowSet is a simple RowSet backed by a map.
type mockRowSet struct {
	rows map[int]struct{}
}

func newMockRowSet(indices ...int) *mockRowSet {
	rs := &mockRowSet{rows: make(map[int]struct{})}
	for _, i := range indices {
		rs.rows[i] = struct{}{}
	}
	return rs
}

func (rs *mockRowSet) Add(rowIdx int)           { rs.rows[rowIdx] = struct{}{} }
func (rs *mockRowSet) Contains(rowIdx int) bool { _, ok := rs.rows[rowIdx]; return ok }
func (rs *mockRowSet) Size() int                { return len(rs.rows) }
func (rs *mockRowSet) IsEmpty() bool            { return len(rs.rows) == 0 }
func (rs *mockRowSet) ToSlice() []int {
	s := make([]int, 0, len(rs.rows))
	for k := range rs.rows {
		s = append(s, k)
	}
	slices.Sort(s)
	return s
}

// mockProvider records which scan methods were called and returns preset row sets.
type mockProvider struct {
	// allRows is returned by FullScan and used as the universe for Complement.
	allRows    *mockRowSet
	bodyValues map[int]string // log:body values by row index, for two-phase tests
	calls      []string
	rowCount   int
}

func newMockProvider(rowCount int) *mockProvider {
	all := newMockRowSet()
	for i := range rowCount {
		all.Add(i)
	}
	return &mockProvider{rowCount: rowCount, allRows: all}
}

func (p *mockProvider) GetRowCount() int { return p.rowCount }

func (p *mockProvider) ScanEqual(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanEqual(%s,%v)", column, value))
	return newMockRowSet(0, 1), nil
}

func (p *mockProvider) ScanEqualAny(column string, values []any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanEqualAny(%s,%v)", column, values))
	return newMockRowSet(0, 1), nil
}

func (p *mockProvider) ScanNotEqual(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanNotEqual(%s,%v)", column, value))
	return newMockRowSet(2, 3), nil
}

func (p *mockProvider) ScanRegex(column string, pattern string) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanRegex(%s,%s)", column, pattern))
	return newMockRowSet(0, 2), nil
}

func (p *mockProvider) ScanRegexNotMatch(column string, pattern string) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanRegexNotMatch(%s,%s)", column, pattern))
	return newMockRowSet(1, 3), nil
}

func (p *mockProvider) ScanRegexFast(column string, re *regexp.Regexp, prefixes []string) (vm.RowSet, error) {
	var reStr string
	if re != nil {
		reStr = re.String()
	} else if len(prefixes) > 0 {
		reStr = "fold:" + strings.Join(prefixes, "|")
	}
	p.calls = append(p.calls, fmt.Sprintf("ScanRegexFast(%s,%s)", column, reStr))
	return newMockRowSet(0, 2), nil
}

func (p *mockProvider) ScanRegexNotMatchFast(column string, re *regexp.Regexp, prefixes []string) (vm.RowSet, error) {
	var reStr string
	if re != nil {
		reStr = re.String()
	} else if len(prefixes) > 0 {
		reStr = "fold:" + strings.Join(prefixes, "|")
	}
	p.calls = append(p.calls, fmt.Sprintf("ScanRegexNotMatchFast(%s,%s)", column, reStr))
	return newMockRowSet(1, 3), nil
}

func (p *mockProvider) ScanContains(column string, substring string) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanContains(%s,%s)", column, substring))
	return newMockRowSet(0), nil
}

func (p *mockProvider) ScanLessThan(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanLessThan(%s,%v)", column, value))
	return newMockRowSet(), nil
}

func (p *mockProvider) ScanLessThanOrEqual(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanLessThanOrEqual(%s,%v)", column, value))
	return newMockRowSet(), nil
}

func (p *mockProvider) ScanGreaterThan(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanGreaterThan(%s,%v)", column, value))
	return newMockRowSet(), nil
}

func (p *mockProvider) ScanGreaterThanOrEqual(column string, value any) (vm.RowSet, error) {
	p.calls = append(p.calls, fmt.Sprintf("ScanGreaterThanOrEqual(%s,%v)", column, value))
	return newMockRowSet(), nil
}

func (p *mockProvider) ScanIsNull(string) (vm.RowSet, error)    { return newMockRowSet(), nil }
func (p *mockProvider) ScanIsNotNull(string) (vm.RowSet, error) { return newMockRowSet(), nil }

// Streaming scans — unused by logqlparser but required by the interface.
func (p *mockProvider) StreamScanEqual(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanNotEqual(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanLessThan(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanLessThanOrEqual(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanGreaterThan(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanGreaterThanOrEqual(string, any, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanIsNull(string, vm.RowCallback) (int, error)    { return 0, nil }
func (p *mockProvider) StreamScanIsNotNull(string, vm.RowCallback) (int, error) { return 0, nil }

func (p *mockProvider) StreamScanRegex(string, string, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanRegexNotMatch(string, string, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamScanContains(string, string, vm.RowCallback) (int, error) {
	return 0, nil
}

func (p *mockProvider) StreamFullScan(vm.RowCallback) (int, error) { return 0, nil }

func (p *mockProvider) Union(a, b vm.RowSet) vm.RowSet {
	result := newMockRowSet()
	for k := range a.(*mockRowSet).rows {
		result.Add(k)
	}
	for k := range b.(*mockRowSet).rows {
		result.Add(k)
	}
	return result
}

func (p *mockProvider) Intersect(a, b vm.RowSet) vm.RowSet {
	result := newMockRowSet()
	for k := range a.(*mockRowSet).rows {
		if b.Contains(k) {
			result.Add(k)
		}
	}
	return result
}

func (p *mockProvider) Complement(rs vm.RowSet) vm.RowSet {
	p.calls = append(p.calls, "Complement")
	result := newMockRowSet()
	for k := range p.allRows.rows {
		if !rs.Contains(k) {
			result.Add(k)
		}
	}
	return result
}

func (p *mockProvider) FullScan() vm.RowSet {
	p.calls = append(p.calls, "FullScan")
	return p.allRows
}

func (p *mockProvider) GetValue(column string, rowIdx int) (any, bool, error) {
	if column == "log:body" && p.bodyValues != nil {
		if v, ok := p.bodyValues[rowIdx]; ok {
			return v, true, nil
		}
	}
	return nil, false, nil
}

// --- Compile tests ---

func TestCompile_MatchAll_NilSelector(t *testing.T) {
	prog, err := Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 4 {
		t.Errorf("expected FullScan to return all 4 rows, got %d", rs.Size())
	}
}

func TestCompile_MatchAll_EmptySelector(t *testing.T) {
	prog, err := Compile(&LogSelector{})
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 4 {
		t.Errorf("expected FullScan to return all 4 rows, got %d", rs.Size())
	}
}

func TestCompile_MatcherEqual(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "cluster", Value: "dev", Type: MatchEqual},
	}}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
	assertCall(t, provider.calls, "ScanEqual(resource.cluster,dev)")
}

func TestCompile_MatcherNotEqual(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "env", Value: "prod", Type: MatchNotEqual},
	}}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
	assertCall(t, provider.calls, "ScanNotEqual(resource.env,prod)")
}

func TestCompile_MatcherRegex(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "ns", Value: "mimir.*", Type: MatchRegex},
	}}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
	assertCall(t, provider.calls, "ScanRegexFast(resource.ns,mimir.*)")
}

func TestCompile_MatcherNotRegex(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "tag", Value: "debug.*", Type: MatchNotRegex},
	}}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
	assertCall(t, provider.calls, "ScanRegexNotMatchFast(resource.tag,debug.*)")
}

func TestCompile_MatcherUnsupportedType(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "x", Value: "y", Type: MatchType(99)},
	}}
	_, err := Compile(sel)
	if err == nil {
		t.Fatal("expected error for unsupported match type")
	}
}

func TestCompile_FilterContains(t *testing.T) {
	sel := &LogSelector{
		Matchers:    []LabelMatcher{{Name: "cluster", Value: "dev", Type: MatchEqual}},
		LineFilters: []LineFilter{{Pattern: "error", Type: FilterContains}},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	// ScanEqual returns {0,1}, ScanContains returns {0} → intersect = {0}
	if rs.Size() != 1 {
		t.Errorf("expected 1 row, got %d", rs.Size())
	}
	assertCall(t, provider.calls, "ScanEqual(resource.cluster,dev)")
	assertCall(t, provider.calls, "ScanContains(log:body,error)")
}

func TestCompile_FilterNotContains(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "debug", Type: FilterNotContains}},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	// ScanContains returns {0}, Complement with 4 rows → {1,2,3}
	assertCall(t, provider.calls, "ScanContains(log:body,debug)")
	assertCall(t, provider.calls, "Complement")
	if rs.Size() != 3 {
		t.Errorf("expected 3 rows (complement of 1 from 4), got %d", rs.Size())
	}
}

func TestCompile_FilterRegex(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "(?i)err", Type: FilterRegex}},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanRegexFast(log:body,fold:err)")
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
}

func TestCompile_FilterNotRegex(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "(?i)debug", Type: FilterNotRegex}},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanRegexNotMatchFast(log:body,fold:debug)")
	if rs.Size() != 2 {
		t.Errorf("expected 2 rows, got %d", rs.Size())
	}
}

func TestCompile_FilterUnsupportedType(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "x", Type: FilterType(99)}},
	}
	_, err := Compile(sel)
	if err == nil {
		t.Fatal("expected error for unsupported filter type")
	}
}

func TestCompile_MultiplePredicatesIntersect(t *testing.T) {
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "cluster", Value: "dev", Type: MatchEqual}, // → {0,1}
			{Name: "ns", Value: "mimir.*", Type: MatchRegex},  // → {0,2}
		},
		LineFilters: []LineFilter{
			{Pattern: "error", Type: FilterContains}, // → {0}
		},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	// {0,1} ∩ {0,2} = {0}, then {0} ∩ {0} = {0}
	if rs.Size() != 1 {
		t.Errorf("expected 1 row from triple intersect, got %d", rs.Size())
	}
}

func TestExtractPredicates_EqualMatcher(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "cluster", Value: "dev", Type: MatchEqual},
	}}
	preds := extractPredicates(sel)
	if preds == nil {
		t.Fatal("expected non-nil predicates")
	}
	// MatchEqual → leaf RangeNode{Column:"resource.cluster", Values:[{TypeString,"dev"}]}
	node := findLeafNode(preds.Nodes, "resource.cluster")
	if node == nil {
		t.Fatalf("expected leaf node for resource.cluster, got nodes: %v", preds.Nodes)
	}
	if len(node.Values) != 1 || node.Values[0].Data != "dev" || node.Values[0].Type != vm.TypeString {
		t.Errorf("unexpected values: %+v", node.Values)
	}
}

func TestExtractPredicates_RegexMatcher(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "ns", Value: "mimir.*", Type: MatchRegex},
	}}
	preds := extractPredicates(sel)
	if preds == nil {
		t.Fatal("expected non-nil predicates")
	}
	// MatchRegex → leaf RangeNode{Column:"resource.ns", Pattern:"mimir.*"}
	node := findLeafNode(preds.Nodes, "resource.ns")
	if node == nil {
		t.Fatalf("expected leaf node for resource.ns, got nodes: %v", preds.Nodes)
	}
	if node.Pattern != "mimir.*" {
		t.Errorf("expected pattern mimir.*, got %q", node.Pattern)
	}
}

func TestExtractPredicates_NegationMatcher(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "env", Value: "prod", Type: MatchNotEqual},
	}}
	preds := extractPredicates(sel)
	// Negation produces no pruning nodes; only Columns entry for row-level decode.
	// preds may be non-nil (Columns is populated) but Nodes must be empty.
	if preds != nil && len(preds.Nodes) > 0 {
		t.Errorf("negation must produce no pruning nodes, got %+v", preds.Nodes)
	}
}

func TestExtractPredicates_NoMatchers(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "error", Type: FilterContains}},
	}
	preds := extractPredicates(sel)
	if preds != nil {
		t.Errorf("expected nil predicates when no label matchers, got %+v", preds)
	}
}

// TestCompileAll_LineFilter_AddsLogBodyToColumns guards NOTE-018:
// ColumnPredicate evaluates line filters against log:body inside buildTwoPhasePredicate.
// ProgramWantColumns must include log:body in the first-pass column set so the
// predicate can find matches. If log:body is missing, every block returns 0 matches.
func TestCompileAll_LineFilter_AddsLogBodyToColumns(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "error", Type: FilterContains}},
	}
	prog, _, err := CompileAll(sel)
	require.NoError(t, err)
	require.NotNil(t, prog.Predicates)
	require.Contains(t, prog.Predicates.Columns, "log:body",
		"line filter must add log:body to Columns so ProgramWantColumns includes it")
}

// TestCompileAll_LineFilter_WithLabelMatcher verifies both the label column and
// log:body are present when a label matcher and line filter are combined.
func TestCompileAll_LineFilter_WithLabelMatcher(t *testing.T) {
	sel := &LogSelector{
		Matchers:    []LabelMatcher{{Name: "service.name", Value: ".+", Type: MatchRegex}},
		LineFilters: []LineFilter{{Pattern: "message 3", Type: FilterContains}},
	}
	prog, _, err := CompileAll(sel)
	require.NoError(t, err)
	require.NotNil(t, prog.Predicates)
	assertCol := func(col string) {
		t.Helper()
		require.Contains(t, prog.Predicates.Columns, col)
	}
	assertCol("resource.service.name")
	assertCol("log:body")
}

// TestCompileAll_NoLineFilter_LogBodyAbsent verifies log:body is not injected
// when there are no line filters (avoids unnecessary column decoding).
func TestCompileAll_NoLineFilter_LogBodyAbsent(t *testing.T) {
	sel := &LogSelector{
		Matchers: []LabelMatcher{{Name: "env", Value: "prod", Type: MatchEqual}},
	}
	prog, _, err := CompileAll(sel)
	require.NoError(t, err)
	require.NotNil(t, prog.Predicates)
	for _, col := range prog.Predicates.Columns {
		if col == "log:body" {
			t.Errorf("log:body must not be in Columns when there are no line filters")
		}
	}
}

func TestExtractPredicates_DedupColumns(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "cluster", Value: "dev", Type: MatchEqual},
		{Name: "cluster", Value: "staging", Type: MatchEqual},
	}}
	preds := extractPredicates(sel)
	if preds == nil {
		t.Fatal("expected non-nil predicates")
	}
	count := 0
	for _, col := range preds.Columns {
		if col == "resource.cluster" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected resource.cluster once in Columns, got %d", count)
	}
}

func TestCompile_PredicatesPopulated(t *testing.T) {
	sel := &LogSelector{Matchers: []LabelMatcher{
		{Name: "cluster", Value: "dev", Type: MatchEqual},
	}}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	if prog.Predicates == nil {
		t.Fatal("expected non-nil Predicates for block pruning")
	}
	// MatchEqual → 1 leaf RangeNode
	if len(prog.Predicates.Nodes) != 1 {
		t.Errorf("expected 1 Nodes entry, got %d", len(prog.Predicates.Nodes))
	}
}

func TestCompile_PredicatesNilForLineFiltersOnly(t *testing.T) {
	sel := &LogSelector{
		LineFilters: []LineFilter{{Pattern: "err", Type: FilterContains}},
	}
	prog, err := Compile(sel)
	if err != nil {
		t.Fatal(err)
	}
	if prog.Predicates != nil {
		t.Errorf("expected nil Predicates when only line filters, got %+v", prog.Predicates)
	}
}

// LQP-SPEC-010: StageLabelFilter pushdown tests

// LQP-TEST-120: pipeline | level="error" pushdown into OR composite RangeNode
// TestExtractPredicates_PipelineEqualPushdown verifies that | level="error"
// produces an OR composite node with log.level and resource.level children.
func TestExtractPredicates_PipelineEqualPushdown(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "level",
					Value: "error",
					Op:    OpEqual,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	require.NotNil(t, preds)
	// OpEqual pipeline filter → OR composite with log.level and resource.level children.
	node := findORCompositeWithChildren(preds.Nodes, "log.level", "resource.level")
	require.NotNil(t, node, "expected OR composite with log.level and resource.level children")
}

// LQP-TEST-121: pipeline | level=~"err.*" pushdown into OR composite RangeNode with Pattern
// TestExtractPredicates_PipelineRegexPushdown verifies | level=~"err.*"
// produces an OR composite node with log.level and resource.level pattern children.
func TestExtractPredicates_PipelineRegexPushdown(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "level",
					Value: `err.*`,
					Op:    OpRegex,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	require.NotNil(t, preds)
	// OpRegex pipeline filter → OR composite with log.level and resource.level pattern children.
	node := findORCompositeWithChildren(preds.Nodes, "log.level", "resource.level")
	require.NotNil(t, node, "expected OR composite with log.level and resource.level children")
}

// LQP-TEST-122: pipeline | level!="error" negation skipped (unsafe for pruning)
// TestExtractPredicates_PipelineNotEqualSkipped verifies | level!="error"
// does NOT generate block-pruning nodes (negation is unsafe for pruning).
func TestExtractPredicates_PipelineNotEqualSkipped(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "level",
					Value: "error",
					Op:    OpNotEqual,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	// Negation produces no pruning nodes — either nil preds or empty Nodes.
	if preds != nil && len(preds.Nodes) > 0 {
		t.Errorf("negation-only pipeline should produce no pruning nodes, got %+v", preds.Nodes)
	}
}

// LQP-TEST-123: pipeline | level!~"err.*" negation skipped (unsafe for pruning)
// TestExtractPredicates_PipelineNotRegexSkipped verifies | level!~"err.*"
// does NOT generate block-pruning nodes.
func TestExtractPredicates_PipelineNotRegexSkipped(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "level",
					Value: `err.*`,
					Op:    OpNotRegex,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	// Negation produces no pruning nodes — either nil preds or empty Nodes.
	if preds != nil && len(preds.Nodes) > 0 {
		t.Errorf("negation-only pipeline should produce no pruning nodes, got %+v", preds.Nodes)
	}
}

// LQP-TEST-124: pipeline | duration>100 pushes OR composite with Min set
// TestExtractPredicates_PipelineNumericRangePushdown verifies | duration>100
// produces an OR composite node with Min set on log.duration and resource.duration.
func TestExtractPredicates_PipelineNumericRangePushdown(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "duration",
					Value: "100",
					Op:    OpGT,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	require.NotNil(t, preds)
	// OpGT pipeline filter → OR composite with Min set on log.duration and resource.duration.
	node := findORCompositeWithChildren(preds.Nodes, "log.duration", "resource.duration")
	require.NotNil(t, node, "expected OR composite with log.duration and resource.duration children")
	// Verify at least one child has Min set with value "100".
	found := false
	for _, child := range node.Children {
		if child.Min != nil && child.Min.Data == "100" {
			found = true
		}
	}
	require.True(t, found, "expected child with Min.Data=100")
}

// LQP-TEST-125: stream selector and pipeline both contribute predicates
// TestExtractPredicates_MixedSelectorAndPipeline verifies that stream selector
// matchers and pipeline filters both contribute to predicates.
func TestExtractPredicates_MixedSelectorAndPipeline(t *testing.T) {
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "env", Value: "prod", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "level",
					Value: "error",
					Op:    OpEqual,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	require.NotNil(t, preds)
	// Stream selector MatchEqual → leaf node for resource.env
	envNode := findLeafNode(preds.Nodes, "resource.env")
	require.NotNil(t, envNode, "stream selector should contribute resource.env leaf node")
	// Pipeline OpEqual → OR composite with log.level and resource.level
	levelNode := findORCompositeWithChildren(preds.Nodes, "log.level", "resource.level")
	require.NotNil(t, levelNode, "pipeline filter should contribute log.level/resource.level OR node")
}

// LQP-TEST-126: pipeline | count<=50 pushes OR composite with Max set
// TestExtractPredicates_PipelineLTEPushdown verifies | count<=50 produces an
// OR composite node with Max set on log.count and resource.count children.
func TestExtractPredicates_PipelineLTEPushdown(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type: StageLabelFilter,
				LabelFilter: &LabelFilter{
					Name:  "count",
					Value: "50",
					Op:    OpLTE,
				},
			},
		},
	}
	preds := extractPredicates(sel)
	require.NotNil(t, preds)
	// OpLTE pipeline filter → OR composite with Max set.
	node := findORCompositeWithChildren(preds.Nodes, "log.count", "resource.count")
	require.NotNil(t, node, "expected OR composite with log.count and resource.count children")
	found := false
	for _, child := range node.Children {
		if child.Max != nil && child.Max.Data == "50" {
			found = true
		}
	}
	require.True(t, found, "expected child with Max.Data=50")
}

// findLeafNode finds the first leaf RangeNode with the given column name.
func findLeafNode(nodes []vm.RangeNode, col string) *vm.RangeNode {
	for i := range nodes {
		if len(nodes[i].Children) == 0 && nodes[i].Column == col {
			return &nodes[i]
		}
	}
	return nil
}

// findORCompositeWithChildren finds the first IsOR composite node whose children
// include all the given column names (at least).
func findORCompositeWithChildren(nodes []vm.RangeNode, cols ...string) *vm.RangeNode {
	for i := range nodes {
		node := &nodes[i]
		if !node.IsOR || len(node.Children) == 0 {
			continue
		}
		childCols := make(map[string]bool, len(node.Children))
		for _, child := range node.Children {
			childCols[child.Column] = true
		}
		allFound := true
		for _, col := range cols {
			if !childCols[col] {
				allFound = false
				break
			}
		}
		if allFound {
			return node
		}
	}
	return nil
}

// isNodeForColumn checks if a RangeNode (leaf or composite) references the given column.
func isNodeForColumn(node vm.RangeNode, col string) bool {
	if len(node.Children) == 0 {
		return node.Column == col
	}
	for _, child := range node.Children {
		if child.Column == col {
			return true
		}
	}
	return false
}

// assertCall checks that a specific call string is present in the calls list.
func assertCall(t *testing.T, calls []string, expected string) {
	t.Helper()
	for _, c := range calls {
		if c == expected {
			return
		}
	}
	t.Errorf("expected call %q not found in %v", expected, calls)
}

func assertNoCall(t *testing.T, calls []string, unexpected string) {
	t.Helper()
	for _, c := range calls {
		if c == unexpected {
			t.Errorf("unexpected call %q found in %v", unexpected, calls)
			return
		}
	}
}

// CompilePipeline tests

func TestCompilePipeline_Empty(t *testing.T) {
	p, err := CompilePipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Error("expected nil pipeline for empty stages")
	}
}

func TestCompilePipeline_JSON(t *testing.T) {
	stages := []PipelineStage{{Type: StageJSON}}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil || len(p.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %v", p)
	}
	// Verify the stage works: JSON body extracts labels.
	ls := NewEmptyLabelSet()
	_, ls, keep := p.Stages[0](0, `{"level":"info","msg":"hello"}`, ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected level=info, got %q", ls.Get("level"))
	}
}

func TestCompilePipeline_Logfmt(t *testing.T) {
	stages := []PipelineStage{{Type: StageLogfmt}}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil || len(p.Stages) != 1 {
		t.Fatalf("expected 1 stage")
	}
	ls := NewEmptyLabelSet()
	_, ls, keep := p.Stages[0](0, "level=warn msg=timeout", ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if ls.Get("level") != "warn" {
		t.Errorf("expected level=warn, got %q", ls.Get("level"))
	}
}

func TestCompilePipeline_LabelFormat(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageLabelFormat, Params: []string{"svc=service"}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewMapLabelSet(map[string]string{"service": "api"})
	_, ls, keep := p.Stages[0](0, "", ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if ls.Get("svc") != "api" {
		t.Errorf("expected svc=api, got %q", ls.Get("svc"))
	}
	for _, k := range ls.Keys() {
		if k == "service" {
			t.Error("expected service label deleted after rename")
		}
	}
}

func TestCompilePipeline_LineFormat(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageLineFormat, Params: []string{"level={{ .level }}"}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	line, _, keep := p.Stages[0](0, "original", ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if line != "level=error" {
		t.Errorf("expected line=level=error, got %q", line)
	}
}

func TestCompilePipeline_Drop(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageDrop, Params: []string{"ts", "trace_id"}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewMapLabelSet(map[string]string{"level": "info", "ts": "123", "trace_id": "abc"})
	_, ls, keep := p.Stages[0](0, "", ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected level=info")
	}
	for _, k := range ls.Keys() {
		if k == "ts" {
			t.Error("expected ts dropped")
		}
		if k == "trace_id" {
			t.Error("expected trace_id dropped")
		}
	}
}

func TestCompilePipeline_Keep(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageKeep, Params: []string{"level"}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewMapLabelSet(map[string]string{"level": "info", "ts": "123", "msg": "hello"})
	_, ls, keep := p.Stages[0](0, "", ls)
	if !keep {
		t.Fatal("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected level=info")
	}
	for _, k := range ls.Keys() {
		if k == "ts" {
			t.Error("expected ts removed by keep")
		}
	}
}

func TestCompilePipeline_LabelFilter(t *testing.T) {
	stages := []PipelineStage{
		{
			Type: StageLabelFilter,
			LabelFilter: &LabelFilter{
				Name: "level", Value: "error", Op: OpEqual,
			},
		},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}

	// Matching row — keep.
	_, _, keep := p.Stages[0](0, "", NewMapLabelSet(map[string]string{"level": "error"}))
	if !keep {
		t.Error("expected keep=true for level=error")
	}

	// Non-matching row — drop.
	_, _, keep = p.Stages[0](0, "", NewMapLabelSet(map[string]string{"level": "info"}))
	if keep {
		t.Error("expected keep=false for level=info")
	}
}

func TestCompilePipeline_LabelFilter_MissingLF(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageLabelFilter, LabelFilter: nil},
	}
	_, err := CompilePipeline(stages)
	if err == nil {
		t.Error("expected error for nil LabelFilter")
	}
}

func TestCompilePipeline_Unwrap(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageUnwrap, Params: []string{"duration_ms"}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	ls := NewMapLabelSet(map[string]string{"duration_ms": "42.5"})
	_, ls, keep := p.Stages[0](0, "", ls)
	if !keep {
		t.Fatal("expected keep=true for valid numeric")
	}
	if ls.Get("__unwrap_value__") != "42.5" {
		t.Errorf("expected __unwrap_value__=42.5, got %q", ls.Get("__unwrap_value__"))
	}
}

func TestCompilePipeline_MultipleStages(t *testing.T) {
	// | json | level = "error"
	stages := []PipelineStage{
		{Type: StageJSON},
		{
			Type: StageLabelFilter,
			LabelFilter: &LabelFilter{
				Name: "level", Value: "error", Op: OpEqual,
			},
		},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(p.Stages))
	}

	// Error row passes.
	line, ls, keep := p.Process(0, `{"level":"error","msg":"failed"}`, NewEmptyLabelSet())
	_ = line
	if !keep {
		t.Error("expected level=error to pass pipeline")
	}
	if ls.Get("level") != "error" {
		t.Errorf("expected level=error in labels, got %q", ls.Get("level"))
	}

	// Info row is dropped.
	_, _, keep = p.Process(0, `{"level":"info","msg":"ok"}`, NewEmptyLabelSet())
	if keep {
		t.Error("expected level=info to be dropped by pipeline")
	}
}

func TestCompilePipeline_InvalidLineFormatTemplate(t *testing.T) {
	stages := []PipelineStage{
		{Type: StageLineFormat, Params: []string{"{{ invalid }}"}},
	}
	_, err := CompilePipeline(stages)
	if err == nil {
		t.Error("expected error for invalid template")
	}
}

// --- CompileAll / predicate pushdown tests ---

func TestCompileAll_NilSelector(t *testing.T) {
	prog, pipeline, err := CompileAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	if prog == nil {
		t.Fatal("expected non-nil program")
	}
	if pipeline != nil {
		t.Error("expected nil pipeline for nil selector")
	}
}

func TestCompileAll_LabelFilterBeforeParser_PushedDown(t *testing.T) {
	// {app="foo"} | detected_level="error"
	// The label filter should be pushed down into the program as a ScanEqual on log.detected_level.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "detected_level", Value: "error", Op: OpEqual},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}

	// The label filter was pushed down, so the pipeline should be nil (no remaining stages).
	if pipeline != nil {
		t.Errorf("expected nil pipeline after pushdown, got %d stages", len(pipeline.Stages))
	}

	// Verify the program includes a ScanEqual on log.detected_level.
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanEqual(resource.app,foo)")
	assertCall(t, provider.calls, "ScanEqual(log.detected_level,error)")

	// Verify QueryPredicates has OR composite for bloom-OR across both scopes.
	if prog.Predicates == nil {
		t.Fatal("expected non-nil Predicates")
	}
	orNode := findORCompositeWithChildren(prog.Predicates.Nodes, "log.detected_level", "resource.detected_level")
	if orNode == nil {
		t.Fatalf(
			"expected OR composite with log.detected_level and resource.detected_level, got nodes: %v",
			prog.Predicates.Nodes,
		)
	}
}

func TestCompileAll_LabelFilterAfterParser_NotPushedDown(t *testing.T) {
	// {app="foo"} | json | level="error"
	// Post-parser label filters must NOT be pushed to ColumnPredicate: ingest-time
	// log.* columns use last-wins semantics and may differ from body re-parsing
	// (e.g. duplicate keys, format mismatch). The pipeline is the authoritative filter.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{Type: StageJSON},
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}

	// Pipeline should have 2 stages (json + level filter — post-parser filter kept in pipeline).
	if pipeline == nil || len(pipeline.Stages) != 2 {
		stages := 0
		if pipeline != nil {
			stages = len(pipeline.Stages)
		}
		t.Fatalf("expected 2 pipeline stages, got %d", stages)
	}

	// Program should NOT have a ScanEqual for log.level — post-parser filters
	// are not pushed to ColumnPredicate to avoid false negatives from ingest-time
	// last-wins column values diverging from re-parsed body values.
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertNoCall(t, provider.calls, "ScanEqual(log.level,error)")
	_ = prog
}

func TestCompileAll_RegexFilterPushedDown(t *testing.T) {
	// {app="foo"} | detected_level=~"err.*"
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "detected_level", Value: "err.*", Op: OpRegex},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline != nil {
		t.Error("expected nil pipeline after pushdown")
	}

	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanRegex(log.detected_level,err.*)")

	// Verify QueryPredicates has OR composite for bloom-OR.
	if prog.Predicates == nil {
		t.Fatal("expected non-nil Predicates")
	}
	orNode := findORCompositeWithChildren(prog.Predicates.Nodes, "log.detected_level", "resource.detected_level")
	if orNode == nil {
		t.Fatalf(
			"expected OR composite with log.detected_level and resource.detected_level, got nodes: %v",
			prog.Predicates.Nodes,
		)
	}
}

func TestCompileAll_NegationFilterPushedDown(t *testing.T) {
	// {app="foo"} | detected_level!="debug"
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "detected_level", Value: "debug", Op: OpNotEqual},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline != nil {
		t.Error("expected nil pipeline after pushdown")
	}

	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanNotEqual(log.detected_level,debug)")

	// Negation must NOT produce pruning nodes (bloom-pruning safety).
	if prog.Predicates != nil {
		for _, node := range prog.Predicates.Nodes {
			if isNodeForColumn(node, "log.detected_level") {
				t.Error("negation should NOT produce a pruning node for log.detected_level")
			}
		}
	}
}

func TestCompileAll_NumericFilterPushedToColumnPred(t *testing.T) {
	// {app="foo"} | status > 500
	// Pre-parser numeric ops are pushed to ColumnPredicate and removed from the
	// pipeline (NOTE-012). ScanGreaterThan handles both typed and string columns
	// correctly, so LabelFilterStage would only repeat the comparison redundantly.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "status", Value: "500", Op: OpGT},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-parser numeric filter: removed from pipeline, handled entirely by ColumnPredicate.
	if pipeline != nil {
		t.Fatalf("expected nil pipeline for pre-parser numeric filter, got %d stages", len(pipeline.Stages))
	}
	// Numeric filter pushed to ColumnPredicate.
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanGreaterThan(log.status,500)")
}

func TestCompileAll_OrFilterPushedDown(t *testing.T) {
	// {app="foo"} | level="error" or level="warn"
	// Pre-parser OR filters are pushed to ColumnPredicate (union) and removed from pipeline.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual},
				OrFilters:   []*LabelFilter{{Name: "level", Value: "warn", Op: OpEqual}},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-parser OR filter: removed from pipeline (native column, same as single filters).
	if pipeline != nil {
		t.Fatalf("expected nil pipeline, OR filter pushed down, got %d stages", len(pipeline.Stages))
	}
	// ColumnPredicate should union both alternatives.
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanEqual(log.level,error)")
	assertCall(t, provider.calls, "ScanEqual(log.level,warn)")
}

func TestCompileAll_MixedPushdownAndPipeline(t *testing.T) {
	// {app="foo"} | detected_level="error" | json | msg="failed"
	// detected_level is before parser → pushed down AND removed from pipeline (pre-parser, native column).
	// json → stays in pipeline.
	// msg="failed" is after parser → added to ColumnPredicate as hint, but kept in pipeline.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "detected_level", Value: "error", Op: OpEqual},
			},
			{Type: StageJSON},
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "msg", Value: "failed", Op: OpEqual},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}

	// Pipeline should have 2 stages (json + msg filter). detected_level was pushed down and removed.
	// msg is kept in pipeline because it appears after json (may only exist after body parsing).
	if pipeline == nil || len(pipeline.Stages) != 2 {
		stages := 0
		if pipeline != nil {
			stages = len(pipeline.Stages)
		}
		t.Fatalf("expected 2 pipeline stages, got %d", stages)
	}

	// ColumnPredicate: detected_level (pre-parser) is pushed; msg (post-parser) is NOT.
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanEqual(log.detected_level,error)")
	assertNoCall(t, provider.calls, "ScanEqual(log.msg,failed)")
}

// TestCompileAll_NumericFilterInvalidValue verifies that an unparseable numeric
// threshold (e.g. | status > "not-a-number") is NOT pushed to ColumnPredicate
// and remains in pipeline only.
func TestCompileAll_NumericFilterInvalidValue(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "status", Value: "not-a-number", Op: OpGT},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline == nil || len(pipeline.Stages) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %v", pipeline)
	}
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range provider.calls {
		if strings.HasPrefix(call, "ScanGreaterThan") {
			t.Error("unparseable numeric threshold should NOT be pushed to ColumnPredicate")
		}
	}
}

// TestCompileAll_NumericGTEAndLTE verifies >= and <= numeric ops are pushed to ColumnPredicate
// and removed from the pipeline (NOTE-012).
func TestCompileAll_NumericGTEAndLTE(t *testing.T) {
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{Type: StageLabelFilter, LabelFilter: &LabelFilter{Name: "latency_ms", Value: "100", Op: OpGTE}},
			{Type: StageLabelFilter, LabelFilter: &LabelFilter{Name: "latency_ms", Value: "500", Op: OpLTE}},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-parser numeric filters: removed from pipeline, handled by ColumnPredicate.
	if pipeline != nil {
		t.Fatalf("expected nil pipeline for pre-parser numeric filters, got %d stages", len(pipeline.Stages))
	}
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanGreaterThanOrEqual(log.latency_ms,100)")
	assertCall(t, provider.calls, "ScanLessThanOrEqual(log.latency_ms,500)")
}

// TestCompileAll_OrFilterDifferentNames verifies OR filters with different label names.
func TestCompileAll_OrFilterDifferentNames(t *testing.T) {
	// | level="error" or component="api" — two different columns, both scanned
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual},
				OrFilters:   []*LabelFilter{{Name: "component", Value: "api", Op: OpEqual}},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-parser OR filter: pushed down and removed from pipeline.
	if pipeline != nil {
		t.Fatalf("expected nil pipeline, got %d stages", len(pipeline.Stages))
	}
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, provider.calls, "ScanEqual(log.level,error)")
	assertCall(t, provider.calls, "ScanEqual(log.component,api)")
}

// TestCompileAll_OrFilterAfterParser verifies OR filters after logfmt stay in pipeline.
func TestCompileAll_OrFilterAfterParser(t *testing.T) {
	// | logfmt | level="error" or level="warn" — post-parser: kept in pipeline
	sel := &LogSelector{
		Pipeline: []PipelineStage{
			{Type: StageLogfmt},
			{
				Type:        StageLabelFilter,
				LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual},
				OrFilters:   []*LabelFilter{{Name: "level", Value: "warn", Op: OpEqual}},
			},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	// Post-parser OR filter: kept in pipeline, NOT pushed to ColumnPredicate.
	if pipeline == nil || len(pipeline.Stages) != 2 {
		t.Fatalf("expected 2 pipeline stages (logfmt + OR filter), got %v", pipeline)
	}
	provider := newMockProvider(4)
	_, err = prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	assertNoCall(t, provider.calls, "ScanEqual(log.level,error)")
	assertNoCall(t, provider.calls, "ScanEqual(log.level,warn)")
}

func TestCompileAll_NoPipeline(t *testing.T) {
	// {app="foo"} — no pipeline stages at all.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
	}
	prog, pipeline, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline != nil {
		t.Error("expected nil pipeline")
	}
	if prog == nil {
		t.Fatal("expected non-nil program")
	}
}

// TestCompileAll_TwoPhase_CheapPredsAndLineFilter verifies that when both a
// cheap predicate (label matcher) and a line filter are present, the two-phase
// evaluation path runs: phase 1 narrows candidates via bulk scan, phase 2
// evaluates the line filter per-row only on survivors.
func TestCompileAll_TwoPhase_CheapPredsAndLineFilter(t *testing.T) {
	// {app="foo"} |= "error"
	// ScanEqual returns rows {0,1}; only rows 0 and 2 have "error" in body.
	// Phase 2 checks rows 0 and 1 against body: row 0 has "error occurred" (pass),
	// row 1 has "info message" (fail) → result should be {0}.
	sel := &LogSelector{
		Matchers: []LabelMatcher{
			{Name: "app", Value: "foo", Type: MatchEqual},
		},
		LineFilters: []LineFilter{
			{Pattern: "error", Type: FilterContains},
		},
	}
	prog, _, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}

	provider := newMockProvider(4)
	provider.bodyValues = map[int]string{
		0: "error occurred",
		1: "info message",
		2: "error again",
		3: "debug log",
	}

	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	// ScanEqual returns {0,1}; phase 2 checks body per row:
	//   row 0 → "error occurred" → contains "error" → pass
	//   row 1 → "info message" → does not contain "error" → drop
	// Result: {0}
	if rs.Size() != 1 {
		t.Errorf("expected 1 row (two-phase: cheap+line), got %d", rs.Size())
	}
	got := rs.ToSlice()
	if len(got) != 1 || got[0] != 0 {
		t.Errorf("expected row index [0], got %v", got)
	}
	// Phase 1 must have used ScanEqual (cheap bulk scan).
	assertCall(t, provider.calls, "ScanEqual(resource.app,foo)")
	// Phase 2 must NOT have called ScanContains (it uses per-row GetValue instead).
	for _, c := range provider.calls {
		if c == "ScanContains(log:body,error)" {
			t.Error("two-phase path must not call bulk ScanContains when cheap preds exist")
		}
	}
}

// TestCompileAll_LineFiltersOnly_BulkFallback verifies that when there are no
// cheap predicates (no label matchers, no pushed-down metadata), line filters
// fall back to the bulk ScanContains/ScanRegex path rather than the per-row path.
func TestCompileAll_LineFiltersOnly_BulkFallback(t *testing.T) {
	// No matchers, only a line filter — should use bulk ScanContains.
	sel := &LogSelector{
		LineFilters: []LineFilter{
			{Pattern: "error", Type: FilterContains},
		},
	}
	prog, _, err := CompileAll(sel)
	if err != nil {
		t.Fatal(err)
	}

	provider := newMockProvider(4)
	rs, err := prog.ColumnPredicate(provider)
	if err != nil {
		t.Fatal(err)
	}
	// ScanContains returns {0} per the mock.
	if rs.Size() != 1 {
		t.Errorf("expected 1 row from bulk fallback, got %d", rs.Size())
	}
	// Must use bulk ScanContains, not per-row GetValue.
	assertCall(t, provider.calls, "ScanContains(log:body,error)")
}

// TestCompilePipeline_ParserFreeStages_LeadingOnly verifies that compilePipelineSkipping
// only removes parser stages from the leading contiguous prefix. A JSON stage that
// appears after a non-parser stage must be retained in parserFreeStages.
func TestCompilePipeline_ParserFreeStages_LeadingOnly(t *testing.T) {
	// Pipeline: | json | level="error"
	// json is leading — should be absent from parserFreeStages.
	stages := []PipelineStage{
		{Type: StageJSON},
		{Type: StageLabelFilter, LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual}},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Stages) != 2 {
		t.Fatalf("Stages: expected 2, got %d", len(p.Stages))
	}
	if len(p.parserFreeStages) != 1 {
		t.Fatalf("parserFreeStages: expected 1 (label filter only), got %d", len(p.parserFreeStages))
	}
	// ProcessSkipParsers should still apply the label filter.
	ls := NewMapLabelSet(map[string]string{"level": "info"})
	_, _, keep := p.ProcessSkipParsers(0, "anything", ls)
	if keep {
		t.Fatal("ProcessSkipParsers: expected level=info to be dropped by level=error filter")
	}
}

// TestCompilePipeline_ParserFreeStages_AfterMutation verifies that a JSON stage after
// a non-parser stage (e.g. line_format) is NOT removed from parserFreeStages — it must
// run on the mutated line.
func TestCompilePipeline_ParserFreeStages_AfterMutation(t *testing.T) {
	// Pipeline: | line_format "fixed-line" | json
	// json appears after a mutation — must be kept in parserFreeStages.
	stages := []PipelineStage{
		{Type: StageLineFormat, Params: []string{"fixed-line"}},
		{Type: StageJSON},
	}
	p, err := CompilePipeline(stages)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Stages) != 2 {
		t.Fatalf("Stages: expected 2, got %d", len(p.Stages))
	}
	// parserFreeStages should also have 2 — the json stage after line_format must be retained.
	if len(p.parserFreeStages) != 2 {
		t.Fatalf("parserFreeStages: expected 2 (line_format + json retained), got %d", len(p.parserFreeStages))
	}
}
