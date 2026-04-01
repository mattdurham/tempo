package logqlparser

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/template"

	regexp "github.com/coregx/coregex"

	"github.com/grafana/blockpack/internal/vm"
)

// simpleRowSet is a lightweight RowSet for collecting per-row filter results.
// It avoids the O(n) allocation of FullScan()+Complement() when building
// result sets incrementally.
type simpleRowSet struct {
	rows map[int]struct{}
}

func newSimpleRowSet() *simpleRowSet {
	return &simpleRowSet{rows: make(map[int]struct{})}
}

func (rs *simpleRowSet) Add(rowIdx int)           { rs.rows[rowIdx] = struct{}{} }
func (rs *simpleRowSet) Contains(rowIdx int) bool { _, ok := rs.rows[rowIdx]; return ok }
func (rs *simpleRowSet) Size() int                { return len(rs.rows) }
func (rs *simpleRowSet) IsEmpty() bool            { return len(rs.rows) == 0 }
func (rs *simpleRowSet) ToSlice() []int {
	s := make([]int, 0, len(rs.rows))
	for k := range rs.rows {
		s = append(s, k)
	}
	slices.Sort(s)
	return s
}

// Compile converts a LogSelector AST into a vm.Program suitable for execution
// by the blockpack executor pipeline.
//
// Label matchers compile to predicates on "resource.{name}" columns:
//
//	{cluster="val"}   → ScanEqual("resource.cluster", "val")
//	{ns=~"regex"}     → ScanRegex("resource.ns", "regex")
//	{env!="prod"}     → ScanNotEqual("resource.env", "prod")
//	{tag!~"regex"}    → ScanRegexNotMatch("resource.tag", "regex")
//
// Line filters compile to predicates on the "log:body" column:
//
//	|= "text"         → ScanContains("log:body", "text")
//	!= "text"         → Complement(ScanContains("log:body", "text"))
//	|~ "regex"        → ScanRegex("log:body", "regex")
//	!~ "regex"        → ScanRegexNotMatch("log:body", "regex")
//
// Multiple matchers and line filters are ANDed together via Intersect.
func Compile(sel *LogSelector) (*vm.Program, error) {
	if sel == nil || (len(sel.Matchers) == 0 && len(sel.LineFilters) == 0) {
		return compileMatchAll(), nil
	}

	// Build ColumnPredicate closures for each matcher and line filter.
	var predicates []vm.ColumnPredicate

	for _, m := range sel.Matchers {
		pred, err := compileMatcherPredicate(m)
		if err != nil {
			return nil, fmt.Errorf("logql compile matcher: %w", err)
		}
		predicates = append(predicates, pred)
	}

	for _, lf := range sel.LineFilters {
		pred, err := compileLineFilterPredicate(lf)
		if err != nil {
			return nil, fmt.Errorf("logql compile line filter: %w", err)
		}
		predicates = append(predicates, pred)
	}

	// Combine all predicates via AND (Intersect).
	combined := predicates[0]
	for i := 1; i < len(predicates); i++ {
		left := combined
		right := predicates[i]
		combined = func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			leftRows, err := left(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := right(provider)
			if err != nil {
				return nil, err
			}
			return provider.Intersect(leftRows, rightRows), nil
		}
	}

	// Also extract QueryPredicates for block-level pruning.
	queryPreds := extractPredicates(sel)

	return &vm.Program{
		ColumnPredicate: combined,
		Predicates:      queryPreds,
	}, nil
}

// compileMatcherPredicate compiles a label matcher to a ColumnPredicate.
func compileMatcherPredicate(m LabelMatcher) (vm.ColumnPredicate, error) {
	col := "resource." + m.Name
	val := m.Value

	switch m.Type {
	case MatchEqual:
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanEqual(col, val)
		}, nil
	case MatchNotEqual:
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanNotEqual(col, val)
		}, nil
	case MatchRegex:
		if a := vm.AnalyzeRegex(val); a != nil && a.IsLiteralContains && a.CaseInsensitive {
			lp := a.Prefixes // pre-lowercased by AnalyzeRegex
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanRegexFast(col, nil, lp)
			}, nil
		}
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, fmt.Errorf("logql compile regex: %w", err)
		}
		prefixes := vm.RegexPrefixes(val)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexFast(col, re, prefixes)
		}, nil
	case MatchNotRegex:
		if a := vm.AnalyzeRegex(val); a != nil && a.IsLiteralContains && a.CaseInsensitive {
			lp := a.Prefixes
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanRegexNotMatchFast(col, nil, lp)
			}, nil
		}
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, fmt.Errorf("logql compile not-regex: %w", err)
		}
		prefixes := vm.RegexPrefixes(val)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexNotMatchFast(col, re, prefixes)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported match type: %d", m.Type)
	}
}

// compileLineFilterPredicate compiles a line filter to a ColumnPredicate
// operating on the "log:body" column.
func compileLineFilterPredicate(lf LineFilter) (vm.ColumnPredicate, error) {
	const bodyCol = "log:body"
	pattern := lf.Pattern

	switch lf.Type {
	case FilterContains:
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanContains(bodyCol, pattern)
		}, nil
	case FilterNotContains:
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			rows, err := provider.ScanContains(bodyCol, pattern)
			if err != nil {
				return nil, err
			}
			return provider.Complement(rows), nil
		}, nil
	case FilterRegex:
		if a := vm.AnalyzeRegex(pattern); a != nil && a.IsLiteralContains && a.CaseInsensitive {
			lp := a.Prefixes // pre-lowercased by AnalyzeRegex
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanRegexFast(bodyCol, nil, lp)
			}, nil
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("logql compile regex: %w", err)
		}
		prefixes := vm.RegexPrefixes(pattern)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexFast(bodyCol, re, prefixes)
		}, nil
	case FilterNotRegex:
		if a := vm.AnalyzeRegex(pattern); a != nil && a.IsLiteralContains && a.CaseInsensitive {
			lp := a.Prefixes
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanRegexNotMatchFast(bodyCol, nil, lp)
			}, nil
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("logql compile not-regex: %w", err)
		}
		prefixes := vm.RegexPrefixes(pattern)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexNotMatchFast(bodyCol, re, prefixes)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported filter type: %d", lf.Type)
	}
}

// extractPredicates builds QueryPredicates from the LogSelector for block-level
// pruning (bloom filters and range indices).
//
// Stream matchers:
//   - MatchEqual  → leaf RangeNode{Column:"resource."+name, Values:[value]}
//   - MatchRegex  → leaf RangeNode{Column:"resource."+name, Pattern:pattern}
//   - Negations   → no node; column added to Columns for row-level decode
func extractPredicates(sel *LogSelector) *vm.QueryPredicates {
	var nodes []vm.RangeNode
	var cols []string

	for _, m := range sel.Matchers {
		col := "resource." + m.Name
		switch m.Type {
		case MatchEqual:
			nodes = append(nodes, vm.RangeNode{
				Column: col,
				Values: []vm.Value{{Type: vm.TypeString, Data: m.Value}},
			})
			cols = append(cols, col)
		case MatchRegex:
			nodes = append(nodes, vm.RangeNode{Column: col, Pattern: m.Value})
			cols = append(cols, col)
		case MatchNotEqual, MatchNotRegex:
			// Negations must NOT produce pruning nodes: a missing column matches
			// negation semantics (!=, !~), so bloom-pruning on column absence
			// would produce false negatives by dropping blocks that legitimately match.
			cols = append(cols, col)
		}
	}

	// LQP-SPEC-010 (extended): Walk pipeline for StageLabelFilter positive ops.
	// StageLogfmt and StageJSON are transparent (no-ops). Positive ops produce OR
	// composites covering log.{name} and resource.{name}. Numeric range ops produce
	// OR composites with Min/Max. Negations produce no nodes but add to Columns.
	// Barrier stages (StageLabelFormat, StageDrop, StageKeep, StageLineFormat) stop
	// the walk because they mutate the label map.
	for _, stage := range sel.Pipeline {
		if stage.Type == StageLogfmt || stage.Type == StageJSON {
			continue
		}
		if stage.Type != StageLabelFilter {
			break
		}
		if stage.LabelFilter == nil {
			break
		}
		allFilters := make([]*LabelFilter, 0, 1+len(stage.OrFilters))
		allFilters = append(allFilters, stage.LabelFilter)
		allFilters = append(allFilters, stage.OrFilters...)

		for _, lf := range allFilters {
			logCol := "log." + lf.Name
			resCol := "resource." + lf.Name

			switch lf.Op {
			case OpEqual:
				val := vm.Value{Type: vm.TypeString, Data: lf.Value}
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Values: []vm.Value{val}},
						{Column: resCol, Values: []vm.Value{val}},
					},
				})
				cols = append(cols, logCol, resCol)
			case OpRegex:
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Pattern: lf.Value},
						{Column: resCol, Pattern: lf.Value},
					},
				})
				cols = append(cols, logCol, resCol)
			case OpNotEqual, OpNotRegex:
				// Negations skipped — unsafe for block pruning (see NOTE 6).
				cols = append(cols, logCol, resCol)
			case OpGT:
				minVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Min: &minVal},
						{Column: resCol, Min: &minVal},
					},
				})
				cols = append(cols, logCol, resCol)
			case OpGTE:
				minVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Min: &minVal},
						{Column: resCol, Min: &minVal},
					},
				})
				cols = append(cols, logCol, resCol)
			case OpLT:
				maxVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Max: &maxVal},
						{Column: resCol, Max: &maxVal},
					},
				})
				cols = append(cols, logCol, resCol)
			case OpLTE:
				maxVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				nodes = append(nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Max: &maxVal},
						{Column: resCol, Max: &maxVal},
					},
				})
				cols = append(cols, logCol, resCol)
			}
		}
	}

	if len(nodes) == 0 && len(cols) == 0 {
		return nil
	}

	return &vm.QueryPredicates{Nodes: nodes, Columns: dedupCols(cols)}
}

// dedupCols returns a slice with duplicate strings removed, preserving order.
func dedupCols(ss []string) []string {
	if len(ss) == 0 {
		return ss
	}
	seen := make(map[string]struct{}, len(ss))
	out := ss[:0]
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// compileMatchAll returns a program that matches all rows.
func compileMatchAll() *vm.Program {
	return &vm.Program{
		ColumnPredicate: func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		},
	}
}

// containsStr checks if a string slice contains a value.
func containsStr(slice []string, value string) bool {
	return slices.Contains(slice, value)
}

// CompilePipeline compiles a slice of AST PipelineStage nodes into an executable
// *Pipeline. Each stage type is mapped to the corresponding PipelineStageFunc.
//
// Returns an error if any stage cannot be compiled (e.g., invalid template or regex).
// Returns nil pipeline (nil, nil) for an empty stages slice.
func CompilePipeline(stages []PipelineStage) (*Pipeline, error) {
	if len(stages) == 0 {
		return nil, nil
	}
	return compilePipelineSkipping(stages, nil)
}

// compilePipelineStage compiles a single PipelineStage to a PipelineStageFunc.
func compilePipelineStage(stage PipelineStage) (PipelineStageFunc, error) {
	switch stage.Type {
	case StageJSON:
		return JSONStage(), nil

	case StageLogfmt:
		return LogfmtStage(), nil

	case StageLabelFormat:
		mappings := parseLabelFormatParams(stage.Params)
		return LabelFormatStage(mappings), nil

	case StageLineFormat:
		if len(stage.Params) == 0 {
			return nil, fmt.Errorf("line_format: missing template string")
		}
		tmpl, err := template.New("line_format").Option("missingkey=zero").Parse(stage.Params[0])
		if err != nil {
			return nil, fmt.Errorf("line_format: template parse error: %w", err)
		}
		return LineFormatStage(tmpl), nil

	case StageDrop:
		return DropStage(stage.Params), nil

	case StageKeep:
		return KeepStage(stage.Params), nil

	case StageLabelFilter:
		if stage.LabelFilter == nil {
			return nil, fmt.Errorf("label filter stage missing LabelFilter")
		}
		lf := stage.LabelFilter
		primary := LabelFilterStage(lf.Name, lf.Value, lf.Op)
		if len(stage.OrFilters) == 0 {
			return primary, nil
		}
		// Build OR chain: primary OR alt1 OR alt2 ...
		alts := make([]PipelineStageFunc, 0, len(stage.OrFilters))
		for _, olf := range stage.OrFilters {
			alts = append(alts, LabelFilterStage(olf.Name, olf.Value, olf.Op))
		}
		return OrLabelFilterStage(primary, alts), nil

	case StageUnwrap:
		if len(stage.Params) == 0 {
			return nil, fmt.Errorf("unwrap: missing field name")
		}
		return UnwrapStage(stage.Params[0]), nil

	default:
		return nil, fmt.Errorf("unknown pipeline stage type: %d", stage.Type)
	}
}

// CompileAll compiles a LogSelector into both a vm.Program and a Pipeline with
// two-phase predicate evaluation and structured metadata pushdown.
//
// Phase 1 (cheap): Label matchers (resource.* columns) and pushed-down structured
// metadata filters (log.* columns) are evaluated as bulk column scans. These columns
// are small, often RLE/dictionary-encoded, and scan in microseconds.
//
// Phase 2 (expensive): Line filters (|=, |~, etc.) are evaluated per-row only on
// rows surviving phase 1. Instead of a full-column ScanContains/ScanRegex over
// log:body (which touches every row's variable-length string data), we use
// GetValue("log:body", rowIdx) per candidate row. When phase 1 eliminates 70-90%
// of rows, phase 2 does proportionally less work.
//
// Structured metadata fields are stored as "log.*" columns in blockpack. StageLabelFilter
// stages appearing before any parser stage are pushed into phase 1 AND removed from the
// pipeline (they reference native columns, no re-evaluation needed). | logfmt and | json
// are treated as transparent so filters after them are also added to phase 1 as an
// optimization hint — but are kept in the pipeline, since the field may only exist after
// body parsing. Pipe-OR filters (| a=x or a=y) and numeric comparisons (| f > N) are
// also added to ColumnPredicate; numeric filters always stay in the pipeline since the
// stored column type is unknown at compile time.
func CompileAll(sel *LogSelector) (*vm.Program, *Pipeline, error) {
	if sel == nil {
		return compileMatchAll(), nil, nil
	}

	// Identify pushdown-eligible label filter stages.
	// skip: remove from pipeline (pre-parser, native columns).
	// columnPred: add to ColumnPredicate (pre AND post parser, optimization hint).
	skip, columnPredIdxs := identifyPushdownStages(sel.Pipeline)

	// --- Phase 1: cheap predicates (label matchers + structured metadata) ---
	var cheapPreds []vm.ColumnPredicate

	for _, m := range sel.Matchers {
		pred, err := compileMatcherPredicate(m)
		if err != nil {
			return nil, nil, fmt.Errorf("logql compile matcher: %w", err)
		}
		cheapPreds = append(cheapPreds, pred)
	}

	for idx := range columnPredIdxs {
		stage := sel.Pipeline[idx]
		pred := compilePushdownPredicate(stage.LabelFilter, stage.OrFilters)
		if pred != nil {
			cheapPreds = append(cheapPreds, pred)
		}
	}

	// --- Phase 2: expensive per-row line filter matchers ---
	lineMatchers, err := compileLineFilterMatchers(sel.LineFilters)
	if err != nil {
		return nil, nil, err
	}

	// Build the combined two-phase ColumnPredicate.
	combined := buildTwoPhasePredicate(cheapPreds, sel.LineFilters, lineMatchers)

	// Build QueryPredicates for block-level pruning.
	// Use columnPredIdxs (all eligible, pre AND post parser) so that post-parser
	// filters also contribute to block-level bloom pruning.
	queryPreds := extractPredicates(sel)
	queryPreds = addPushdownQueryPredicates(queryPreds, sel.Pipeline, columnPredIdxs)

	// Line filters evaluate log:body inside ColumnPredicate (phase 2 of buildTwoPhasePredicate).
	// Add log:body to Columns so ProgramWantColumns includes it in the first-pass column set.
	if len(sel.LineFilters) > 0 {
		if queryPreds == nil {
			queryPreds = &vm.QueryPredicates{}
		}
		if !containsStr(queryPreds.Columns, "log:body") {
			queryPreds.Columns = append(queryPreds.Columns, "log:body")
		}
	}

	program := &vm.Program{
		ColumnPredicate: combined,
		Predicates:      queryPreds,
	}

	// Compile the pipeline, skipping only pre-parser pushed-down stages.
	pipeline, err := compilePipelineSkipping(sel.Pipeline, skip)
	if err != nil {
		return nil, nil, err
	}

	return program, pipeline, nil
}

// lineFilterMatcher evaluates a single line filter against a log body string.
type lineFilterMatcher struct {
	filterFn func(body string) bool
	compiled *regexp.Regexp // non-nil for regex/not-regex filters
	pattern  string
}

// compileLineFilterMatchers compiles line filters into per-row matchers.
func compileLineFilterMatchers(filters []LineFilter) ([]lineFilterMatcher, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	matchers := make([]lineFilterMatcher, 0, len(filters))
	for _, lf := range filters {
		m, err := compileOneLineFilterMatcher(lf)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}
	return matchers, nil
}

func compileOneLineFilterMatcher(lf LineFilter) (lineFilterMatcher, error) {
	pattern := lf.Pattern
	switch lf.Type {
	case FilterContains:
		return lineFilterMatcher{
			pattern:  pattern,
			filterFn: func(body string) bool { return strings.Contains(body, pattern) },
		}, nil
	case FilterNotContains:
		return lineFilterMatcher{
			pattern:  pattern,
			filterFn: func(body string) bool { return !strings.Contains(body, pattern) },
		}, nil
	case FilterRegex:
		re, err := regexp.Compile(pattern)
		if err != nil {
			return lineFilterMatcher{}, fmt.Errorf("logql compile regex line filter %q: %w", pattern, err)
		}
		return lineFilterMatcher{
			pattern:  pattern,
			compiled: re,
			filterFn: func(body string) bool { return re.MatchString(body) },
		}, nil
	case FilterNotRegex:
		re, err := regexp.Compile(pattern)
		if err != nil {
			return lineFilterMatcher{}, fmt.Errorf("logql compile not-regex line filter %q: %w", pattern, err)
		}
		return lineFilterMatcher{
			pattern:  pattern,
			compiled: re,
			filterFn: func(body string) bool { return !re.MatchString(body) },
		}, nil
	default:
		return lineFilterMatcher{}, fmt.Errorf("unsupported filter type: %d", lf.Type)
	}
}

// compilePushdownPredicate compiles a pushed-down label filter to a ColumnPredicate
// that scans both "log.*" (structured metadata) and "resource.*" (stream labels) columns.
//
// Pipeline label filters use unprefixed names (e.g., "detected_level") which can
// reference either structured metadata (stored as log.*) or stream labels (stored as
// resource.*). Positive ops (=, =~) union the results; negative ops (!=, !~) intersect.
//
// Collision note: if both log.X and resource.X exist with different values for the same
// row, logReadLabels (which builds the pipeline label map) applies last-write-wins where
// log.* overwrites resource.* (both strip their prefix to the same key). The union
// semantics here are safe because they're a superset — they may keep rows that the
// pipeline would also keep (true positive) or keep rows where only one scope matches
// (also correct since the pipeline only sees one value). The pushed-down stage is
// removed from the pipeline, so no double-filtering occurs.
//
// OR filters are unioned: each filter's column scan is unioned with the others.
// Numeric ops (>, <, >=, <=) pass the threshold as float64; rowCompare handles
// type-aware comparison (typed columns compare natively; string columns parse as float64).
func compilePushdownPredicate(lf *LabelFilter, orFilters []*LabelFilter) vm.ColumnPredicate {
	// Build one predicate per filter (primary + OR alternatives), then union all.
	filters := make([]*LabelFilter, 0, 1+len(orFilters))
	filters = append(filters, lf)
	filters = append(filters, orFilters...)

	preds := make([]vm.ColumnPredicate, 0, len(filters))
	for _, f := range filters {
		if p := compileSinglePushdownPredicate(f); p != nil {
			preds = append(preds, p)
		}
	}
	if len(preds) == 0 {
		return nil
	}
	if len(preds) == 1 {
		return preds[0]
	}
	return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
		result, err := preds[0](p)
		if err != nil {
			return nil, err
		}
		for _, pred := range preds[1:] {
			rs, err := pred(p)
			if err != nil {
				return nil, err
			}
			result = p.Union(result, rs)
		}
		return result, nil
	}
}

// compileSinglePushdownPredicate compiles a ColumnPredicate for one LabelFilter.
// Scans both log.{name} and resource.{name} columns:
//   - Equal/Regex: union (match if either scope has the value)
//   - NotEqual/NotRegex: intersect (both scopes must not have the value)
//   - GT/GTE/LT/LTE: union on log.{name} only (range predicates are log-scoped)
func compileSinglePushdownPredicate(lf *LabelFilter) vm.ColumnPredicate {
	logCol := "log." + lf.Name
	resourceCol := "resource." + lf.Name
	val := lf.Value
	switch lf.Op {
	case OpEqual:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			logSet, err := p.ScanEqual(logCol, val)
			if err != nil {
				return nil, err
			}
			resSet, err := p.ScanEqual(resourceCol, val)
			if err != nil {
				return nil, err
			}
			return p.Union(logSet, resSet), nil
		}
	case OpNotEqual:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			logSet, err := p.ScanNotEqual(logCol, val)
			if err != nil {
				return nil, err
			}
			resSet, err := p.ScanNotEqual(resourceCol, val)
			if err != nil {
				return nil, err
			}
			return p.Intersect(logSet, resSet), nil
		}
	case OpRegex:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			logSet, err := p.ScanRegex(logCol, val)
			if err != nil {
				return nil, err
			}
			resSet, err := p.ScanRegex(resourceCol, val)
			if err != nil {
				return nil, err
			}
			return p.Union(logSet, resSet), nil
		}
	case OpNotRegex:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			logSet, err := p.ScanRegexNotMatch(logCol, val)
			if err != nil {
				return nil, err
			}
			resSet, err := p.ScanRegexNotMatch(resourceCol, val)
			if err != nil {
				return nil, err
			}
			return p.Intersect(logSet, resSet), nil
		}
	case OpGT:
		threshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanGreaterThan(logCol, threshold)
		}
	case OpGTE:
		threshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanGreaterThanOrEqual(logCol, threshold)
		}
	case OpLT:
		threshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanLessThan(logCol, threshold)
		}
	case OpLTE:
		threshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil
		}
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanLessThanOrEqual(logCol, threshold)
		}
	default:
		return nil
	}
}

// buildTwoPhasePredicate builds a ColumnPredicate that evaluates cheap predicates
// first (bulk column scans on small columns), then evaluates expensive line filters
// per-row only on the surviving candidates.
func buildTwoPhasePredicate(
	cheapPreds []vm.ColumnPredicate,
	lineFilters []LineFilter,
	lineMatchers []lineFilterMatcher,
) vm.ColumnPredicate {
	// No cheap predicates and no line matchers — match all.
	if len(cheapPreds) == 0 && len(lineMatchers) == 0 {
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) { return p.FullScan(), nil }
	}

	// No line matchers — just AND the cheap predicates (original behavior).
	if len(lineMatchers) == 0 {
		return intersectPredicates(cheapPreds)
	}

	// No cheap predicates — fall back to bulk scan for line filters (no candidates
	// to constrain, so per-row would be slower than bulk).
	if len(cheapPreds) == 0 {
		bulkPreds := make([]vm.ColumnPredicate, 0, len(lineFilters))
		for _, lf := range lineFilters {
			bulkPreds = append(bulkPreds, lineFilterToBulkPredicate(lf))
		}
		return intersectPredicates(bulkPreds)
	}

	// Two-phase: cheap first, then per-row line filter on candidates.
	cheapCombined := intersectPredicates(cheapPreds)

	return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
		// Phase 1: evaluate cheap predicates via bulk column scans.
		candidates, err := cheapCombined(provider)
		if err != nil {
			return nil, err
		}
		if candidates.IsEmpty() {
			return candidates, nil
		}

		// Phase 2: evaluate line filters per-row on candidates only.
		const bodyCol = "log:body"
		filtered := candidates
		for _, lm := range lineMatchers {
			if filtered.IsEmpty() {
				break
			}
			next, evalErr := evalLineMatcherPerRow(provider, bodyCol, filtered, lm)
			if evalErr != nil {
				return nil, evalErr
			}
			filtered = next
		}
		return filtered, nil
	}
}

// evalLineMatcherPerRow evaluates a line filter matcher on each row in the candidate
// set, returning a new RowSet containing only the rows that pass.
func evalLineMatcherPerRow(
	provider vm.ColumnDataProvider,
	bodyCol string,
	candidates vm.RowSet,
	lm lineFilterMatcher,
) (vm.RowSet, error) {
	result := newSimpleRowSet()
	for _, rowIdx := range candidates.ToSlice() {
		val, ok, err := provider.GetValue(bodyCol, rowIdx)
		if err != nil {
			return nil, fmt.Errorf("GetValue(%s, %d): %w", bodyCol, rowIdx, err)
		}
		if !ok {
			// Missing body — treat as empty string for filter evaluation.
			if lm.filterFn("") {
				result.Add(rowIdx)
			}
			continue
		}
		body, isStr := val.(string)
		if !isStr {
			body = fmt.Sprint(val)
		}
		if lm.filterFn(body) {
			result.Add(rowIdx)
		}
	}
	return result, nil
}

// lineFilterToBulkPredicate converts a LineFilter to a bulk ColumnPredicate
// (used when no cheap predicates exist to constrain candidates).
func lineFilterToBulkPredicate(lf LineFilter) vm.ColumnPredicate {
	const bodyCol = "log:body"
	pattern := lf.Pattern
	switch lf.Type {
	case FilterContains:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanContains(bodyCol, pattern)
		}
	case FilterNotContains:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			rows, err := p.ScanContains(bodyCol, pattern)
			if err != nil {
				return nil, err
			}
			return p.Complement(rows), nil
		}
	case FilterRegex:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanRegex(bodyCol, pattern)
		}
	case FilterNotRegex:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) {
			return p.ScanRegexNotMatch(bodyCol, pattern)
		}
	default:
		return func(p vm.ColumnDataProvider) (vm.RowSet, error) { return p.FullScan(), nil }
	}
}

// intersectPredicates ANDs multiple ColumnPredicate closures via Intersect.
func intersectPredicates(preds []vm.ColumnPredicate) vm.ColumnPredicate {
	if len(preds) == 1 {
		return preds[0]
	}
	combined := preds[0]
	for i := 1; i < len(preds); i++ {
		left := combined
		right := preds[i]
		combined = func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			leftRows, err := left(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := right(provider)
			if err != nil {
				return nil, err
			}
			return provider.Intersect(leftRows, rightRows), nil
		}
	}
	return combined
}

// addPushdownQueryPredicates adds block-level pruning nodes for pushed-down
// structured metadata filters to QueryPredicates.
//
// Pushed-down label filters must scan both log.* and resource.* columns.
// Positive ops produce OR composites so a block is kept if EITHER scope has
// the column. Negations produce no nodes but add columns for row-level decode.
func addPushdownQueryPredicates(
	queryPreds *vm.QueryPredicates,
	stages []PipelineStage,
	pushedDown map[int]struct{},
) *vm.QueryPredicates {
	if len(pushedDown) == 0 {
		return queryPreds
	}
	if queryPreds == nil {
		queryPreds = &vm.QueryPredicates{}
	}

	for idx := range pushedDown {
		stage := stages[idx]
		allFilters := make([]*LabelFilter, 0, 1+len(stage.OrFilters))
		allFilters = append(allFilters, stage.LabelFilter)
		allFilters = append(allFilters, stage.OrFilters...)

		for _, lf := range allFilters {
			logCol := "log." + lf.Name
			resCol := "resource." + lf.Name

			switch lf.Op {
			case OpEqual:
				val := vm.Value{Type: vm.TypeString, Data: lf.Value}
				queryPreds.Nodes = append(queryPreds.Nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Values: []vm.Value{val}},
						{Column: resCol, Values: []vm.Value{val}},
					},
				})
				queryPreds.Columns = appendIfMissing(queryPreds.Columns, logCol, resCol)
			case OpRegex:
				queryPreds.Nodes = append(queryPreds.Nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Pattern: lf.Value},
						{Column: resCol, Pattern: lf.Value},
					},
				})
				queryPreds.Columns = appendIfMissing(queryPreds.Columns, logCol, resCol)
			case OpGT, OpGTE:
				minVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				queryPreds.Nodes = append(queryPreds.Nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Min: &minVal},
						{Column: resCol, Min: &minVal},
					},
				})
				queryPreds.Columns = appendIfMissing(queryPreds.Columns, logCol, resCol)
			case OpLT, OpLTE:
				maxVal := vm.Value{Type: vm.TypeString, Data: lf.Value}
				queryPreds.Nodes = append(queryPreds.Nodes, vm.RangeNode{
					IsOR: true,
					Children: []vm.RangeNode{
						{Column: logCol, Max: &maxVal},
						{Column: resCol, Max: &maxVal},
					},
				})
				queryPreds.Columns = appendIfMissing(queryPreds.Columns, logCol, resCol)
			case OpNotEqual, OpNotRegex:
				// Negations NOT added as nodes: unsafe for bloom pruning (see NOTE 6).
				queryPreds.Columns = appendIfMissing(queryPreds.Columns, logCol, resCol)
			}
		}
	}
	return queryPreds
}

// appendIfMissing appends each col to dst if not already present.
func appendIfMissing(dst []string, cols ...string) []string {
	for _, col := range cols {
		if !containsStr(dst, col) {
			dst = append(dst, col)
		}
	}
	return dst
}

// identifyPushdownStages returns the indices of pipeline stages eligible for
// predicate pushdown. StageLogfmt and StageJSON are treated as transparent (no-ops)
// because structured metadata is stored natively as log.* columns at ingest time.
// Filters after logfmt/json are added to the ColumnPredicate as a performance hint
// but are NOT removed from the pipeline — since the field may only exist after body
// parsing, the pipeline must remain the authoritative filter.
//
// Returns two sets:
//   - skip: indices to remove from pipeline (pre-parser label filters only — these
//     reference native log.*/resource.* columns that exist without parsing)
//   - columnPred: indices to add to ColumnPredicate (pre AND post parser — optimization
//     hint for fast rejection when native columns exist)
//
// A StageLabelFilter is eligible for columnPred when:
//   - It uses a string op (=, !=, =~, !~), with or without OR alternatives, OR
//   - It uses a numeric op (>, >=, <, <=) with a parseable float threshold
//
// A StageLabelFilter is eligible for skip (removed from pipeline) only when:
//   - It appears before any parser stage (!seenParser) AND uses a string op or parseable numeric op
//   - Post-parser string and numeric ops are kept in the pipeline (see NOTE-012)
//
// Barrier stage types: StageLabelFormat, StageDrop, StageKeep, StageLineFormat —
// these mutate the label map, so filters after them may depend on the mutation.
func identifyPushdownStages(stages []PipelineStage) (skip, columnPred map[int]struct{}) {
	skip = make(map[int]struct{})
	columnPred = make(map[int]struct{})
	seenParser := false
	for i, stage := range stages {
		// logfmt/json are transparent: structured metadata is stored natively as log.*
		// columns at ingest time, so these parser stages are no-ops for native fields.
		if stage.Type == StageLogfmt || stage.Type == StageJSON {
			seenParser = true
			continue
		}
		// Stop at mutating stages (label_format, drop, keep, line_format).
		if stage.Type != StageLabelFilter {
			break
		}
		if stage.LabelFilter == nil {
			break
		}
		// Labels starting with "__" (e.g. __error__, __preserve_error__) are
		// synthetic pipeline labels set by parser stages — they are never stored
		// as block columns. Pushing them down to ColumnPredicate would cause the
		// bloom filter to return an empty row set (no log.__error__ column exists),
		// silently dropping all rows.
		isSynthetic := strings.HasPrefix(stage.LabelFilter.Name, "__")
		switch stage.LabelFilter.Op {
		case OpEqual, OpNotEqual, OpRegex, OpNotRegex:
			if !isSynthetic && !seenParser {
				// Post-parser label filters must NOT be pushed to ColumnPredicate:
				// ingest-time log.* columns use last-wins semantics and may differ
				// from body re-parsing (duplicate keys, format mismatch like JSON
				// bodies with | logfmt). A false-negative from ColumnPredicate
				// silently drops rows before the pipeline can correct them.
				columnPred[i] = struct{}{}
			}
			if !seenParser && !isSynthetic {
				skip[i] = struct{}{}
			}
		case OpGT, OpGTE, OpLT, OpLTE:
			// Post-parser numeric filters are not pushed down for the same reasons
			// as string filters above: ingest-time columns may differ from re-parsed values.
			// Synthetic labels (__error__ etc.) have no backing column, so pushing them
			// would cause ColumnPredicate to return an empty row set.
			if seenParser || isSynthetic {
				continue
			}
			// Numeric: parse value to ensure it's a valid threshold. If unparseable,
			// skip this stage entirely — it stays in the pipeline to correctly drop
			// all rows (LabelFilterStage drops on ParseFloat error), but contributes
			// no useful column predicate for block-level pruning.
			if _, err := strconv.ParseFloat(stage.LabelFilter.Value, 64); err != nil {
				continue
			}
			// Check all OR alternatives are also parseable.
			allParseable := true
			for _, olf := range stage.OrFilters {
				if olf.Op == OpGT || olf.Op == OpGTE || olf.Op == OpLT || olf.Op == OpLTE {
					if _, err := strconv.ParseFloat(olf.Value, 64); err != nil {
						allParseable = false
						break
					}
				}
			}
			if !allParseable {
				continue
			}
			columnPred[i] = struct{}{}
			// NOTE-012: numeric ops are skipped from pipeline when no parser has been seen,
			// same as string ops. ScanGreaterThan/ScanLessThan handle both typed and
			// ColumnTypeRangeString columns (dict-level float pre-parse), so the
			// ColumnPredicate is the authoritative evaluator — LabelFilterStage would
			// only repeat the same comparison redundantly.
			// When a parser (json/logfmt) has been seen, keep in pipeline: the pipeline
			// may run on blocks without body-parsed columns, where the parser stage
			// extracts values the column predicate cannot see.
			if !seenParser {
				skip[i] = struct{}{}
			}
		}
	}
	return skip, columnPred
}

// compilePipelineSkipping compiles pipeline stages, skipping those at the given indices.
// Also builds Pipeline.parserFreeStages for ProcessSkipParsers: the same stage list with
// the contiguous leading run of JSON/logfmt stages removed.
//
// Only the leading prefix of parser stages is removed. A JSON or logfmt stage that appears
// after any non-parser stage (e.g. | line_format "..." | json) must NOT be skipped: it
// parses the mutated line produced by the preceding stage, not the original log body.
// Skipping it would silently drop fields that only exist after the mutation.
func compilePipelineSkipping(stages []PipelineStage, skip map[int]struct{}) (*Pipeline, error) {
	if len(stages) == 0 {
		return nil, nil
	}
	var funcs []PipelineStageFunc
	var parserFree []PipelineStageFunc
	seenNonParser := false // tracks whether a non-parser stage has been added to funcs
	hasLineFormat := false
	hasParserStage := false
	for i, stage := range stages {
		if _, skipped := skip[i]; skipped {
			continue
		}
		fn, err := compilePipelineStage(stage)
		if err != nil {
			return nil, fmt.Errorf("compile pipeline stage %d (%v): %w", i, stage.Type, err)
		}
		if fn != nil {
			funcs = append(funcs, fn)
			isParser := stage.Type == StageJSON || stage.Type == StageLogfmt
			// Retain in parserFreeStages unless this is a leading parser stage.
			// A parser that appears after a non-parser must be kept (it processes
			// the mutated line, not the original body).
			if !isParser || seenNonParser {
				parserFree = append(parserFree, fn)
			}
			if !isParser {
				seenNonParser = true
			}
			if stage.Type == StageLineFormat {
				hasLineFormat = true
			}
			if isParser {
				hasParserStage = true
			}
		}
	}
	if len(funcs) == 0 {
		return nil, nil
	}
	return &Pipeline{
		Stages:           funcs,
		parserFreeStages: parserFree,
		HasLineFormat:    hasLineFormat,
		HasParserStage:   hasParserStage,
	}, nil
}

// parseLabelFormatParams converts "dst=src" param strings into a mappings map.
func parseLabelFormatParams(params []string) map[string]string {
	mappings := make(map[string]string, len(params))
	for _, p := range params {
		idx := -1
		for i := 0; i < len(p); i++ {
			if p[i] == '=' {
				idx = i
				break
			}
		}
		if idx > 0 {
			dst := p[:idx]
			src := p[idx+1:]
			mappings[dst] = src
		}
	}
	return mappings
}
