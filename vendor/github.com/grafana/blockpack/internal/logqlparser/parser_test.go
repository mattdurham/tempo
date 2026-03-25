package logqlparser

import (
	"testing"
)

func TestParse_EmptyQuery(t *testing.T) {
	sel, err := Parse("")
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 0 {
		t.Errorf("expected 0 matchers, got %d", len(sel.Matchers))
	}
}

func TestParse_EmptySelector(t *testing.T) {
	sel, err := Parse("{}")
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 0 {
		t.Errorf("expected 0 matchers, got %d", len(sel.Matchers))
	}
}

func TestParse_SingleEquals(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 1 {
		t.Fatalf("expected 1 matcher, got %d", len(sel.Matchers))
	}
	m := sel.Matchers[0]
	if m.Name != "cluster" || m.Value != "dev-us-central-0" || m.Type != MatchEqual {
		t.Errorf("unexpected matcher: %+v", m)
	}
}

func TestParse_MultipleMatchers(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0", namespace="mimir-dev-14"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 2 {
		t.Fatalf("expected 2 matchers, got %d", len(sel.Matchers))
	}
	if sel.Matchers[0].Name != "cluster" || sel.Matchers[0].Type != MatchEqual {
		t.Errorf("unexpected first matcher: %+v", sel.Matchers[0])
	}
	if sel.Matchers[1].Name != "namespace" || sel.Matchers[1].Type != MatchEqual {
		t.Errorf("unexpected second matcher: %+v", sel.Matchers[1])
	}
}

func TestParse_ThreeLabels(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0", namespace=~"mimir.*", container="ingester"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 3 {
		t.Fatalf("expected 3 matchers, got %d", len(sel.Matchers))
	}
	if sel.Matchers[1].Type != MatchRegex || sel.Matchers[1].Value != "mimir.*" {
		t.Errorf("expected regex matcher, got %+v", sel.Matchers[1])
	}
}

func TestParse_AllMatchTypes(t *testing.T) {
	sel, err := Parse(`{a="1", b!="2", c=~"3", d!~"4"}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 4 {
		t.Fatalf("expected 4 matchers, got %d", len(sel.Matchers))
	}
	expected := []MatchType{MatchEqual, MatchNotEqual, MatchRegex, MatchNotRegex}
	for i, m := range sel.Matchers {
		if m.Type != expected[i] {
			t.Errorf("matcher %d: expected type %d, got %d", i, expected[i], m.Type)
		}
	}
}

func TestParse_LineFilterContains(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0"} |= "error"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	lf := sel.LineFilters[0]
	if lf.Type != FilterContains || lf.Pattern != "error" {
		t.Errorf("unexpected line filter: %+v", lf)
	}
}

func TestParse_LineFilterNotContains(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0"} != "debug"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterNotContains || sel.LineFilters[0].Pattern != "debug" {
		t.Errorf("unexpected: %+v", sel.LineFilters[0])
	}
}

func TestParse_LineFilterRegex(t *testing.T) {
	sel, err := Parse(`{cluster="dev"} |~ "(?i)timeout|deadline|canceled"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterRegex {
		t.Errorf("expected regex filter, got %d", sel.LineFilters[0].Type)
	}
	if sel.LineFilters[0].Pattern != "(?i)timeout|deadline|canceled" {
		t.Errorf("unexpected pattern: %s", sel.LineFilters[0].Pattern)
	}
}

func TestParse_LineFilterNotRegex(t *testing.T) {
	sel, err := Parse(`{cluster="dev"} !~ "(?i)debug|trace"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterNotRegex {
		t.Errorf("expected not-regex filter, got %d", sel.LineFilters[0].Type)
	}
}

func TestParse_MultiLineFilters(t *testing.T) {
	sel, err := Parse(`{cluster="dev-us-central-0"} |= "level" != "debug" |~ "(?i)err"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 3 {
		t.Fatalf("expected 3 line filters, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterContains || sel.LineFilters[0].Pattern != "level" {
		t.Errorf("filter 0: %+v", sel.LineFilters[0])
	}
	if sel.LineFilters[1].Type != FilterNotContains || sel.LineFilters[1].Pattern != "debug" {
		t.Errorf("filter 1: %+v", sel.LineFilters[1])
	}
	if sel.LineFilters[2].Type != FilterRegex || sel.LineFilters[2].Pattern != "(?i)err" {
		t.Errorf("filter 2: %+v", sel.LineFilters[2])
	}
}

// TestParse_BenchmarkQueries tests all selector+filter queries from the lokibench suite.
func TestParse_BenchmarkQueries(t *testing.T) {
	queries := []struct {
		name     string
		query    string
		matchers int
		filters  int
	}{
		{"sel/all", `{cluster=~".+"}`, 1, 0},
		{"sel/cluster-eq", `{cluster="dev-us-central-0"}`, 1, 0},
		{"sel/cluster-regex", `{cluster=~"dev-.*"}`, 1, 0},
		{"sel/ns-eq", `{cluster="dev-us-central-0", namespace="mimir-dev-14"}`, 2, 0},
		{"sel/ns-regex", `{cluster="dev-us-central-0", namespace=~"mimir.*"}`, 2, 0},
		{"sel/container", `{cluster="dev-us-central-0", container="ingester"}`, 2, 0},
		{"sel/job", `{job="mimir-dev-14/cortex-gw-internal-zone-a"}`, 1, 0},
		{"sel/3-label", `{cluster="dev-us-central-0", namespace=~"mimir.*", container="ingester"}`, 3, 0},
		{"sel/stream-eq", `{cluster="dev-us-central-0", container="ingester", stream="stderr"}`, 3, 0},
		{"filter/contains", `{cluster="dev-us-central-0"} |= "error"`, 1, 1},
		{"filter/not-contains", `{cluster="dev-us-central-0"} != "debug"`, 1, 1},
		{"filter/regex", `{cluster="dev-us-central-0"} |~ "(?i)timeout|deadline|canceled"`, 1, 1},
		{"filter/not-regex", `{cluster="dev-us-central-0"} !~ "(?i)debug|trace"`, 1, 1},
		{"filter/multi", `{cluster="dev-us-central-0"} |= "level" != "debug" |~ "(?i)err"`, 1, 3},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			sel, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.query, err)
			}
			if len(sel.Matchers) != tc.matchers {
				t.Errorf("expected %d matchers, got %d", tc.matchers, len(sel.Matchers))
			}
			if len(sel.LineFilters) != tc.filters {
				t.Errorf("expected %d filters, got %d", tc.filters, len(sel.LineFilters))
			}
		})
	}
}

func TestParse_StopsAtPipelineStage(t *testing.T) {
	// The parser now fully parses pipeline stages like "| json".
	sel, err := Parse(`{cluster="dev"} | json`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 1 {
		t.Errorf("expected 1 matcher, got %d", len(sel.Matchers))
	}
	if len(sel.LineFilters) != 0 {
		t.Errorf("expected 0 line filters, got %d", len(sel.LineFilters))
	}
	if len(sel.Pipeline) != 1 {
		t.Errorf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageJSON {
		t.Errorf("expected StageJSON, got %d", sel.Pipeline[0].Type)
	}
}

func TestParse_BacktickString(t *testing.T) {
	sel, err := Parse("{cluster=`dev-us-central-0`}")
	if err != nil {
		t.Fatal(err)
	}
	if sel.Matchers[0].Value != "dev-us-central-0" {
		t.Errorf("unexpected value: %s", sel.Matchers[0].Value)
	}
}

func TestParse_EscapedString(t *testing.T) {
	sel, err := Parse(`{cluster="a\"b"}`)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Matchers[0].Value != `a"b` {
		t.Errorf("unexpected value: %q", sel.Matchers[0].Value)
	}
}

func TestParse_DottedLabelName(t *testing.T) {
	sel, err := Parse(`{service.name="myapp"}`)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Matchers[0].Name != "service.name" {
		t.Errorf("unexpected name: %s", sel.Matchers[0].Name)
	}
}

func TestParse_TrailingTokenError(t *testing.T) {
	// Unexpected trailing tokens after selector/filters should error.
	cases := []struct {
		name  string
		query string
	}{
		{"bare_text", `{a="1"} garbage`},
		{"number", `{a="1"} 123`},
		{"after_filter", `{a="1"} |= "error" garbage`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.query)
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.query)
			}
		})
	}
}

func TestParse_ErrorCases(t *testing.T) {
	cases := []string{
		`cluster="val"`,     // missing {
		`{cluster`,          // missing operator
		`{cluster=}`,        // missing value
		`{cluster="val"`,    // missing }
		`{cluster="val`,     // unterminated string
		`{"cluster"="val"}`, // label name can't start with quote
		`{cluster=="val"}`,  // unknown operator
		`{=~"val"}`,         // missing label name
	}
	for _, q := range cases {
		_, err := Parse(q)
		if err == nil {
			t.Errorf("expected error for %q", q)
		}
	}
}

// Compile tests are in compile_test.go.

func TestParsePipeline_JSON(t *testing.T) {
	sel, err := Parse(`{app="svc"} | json`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageJSON {
		t.Errorf("expected StageJSON, got %d", sel.Pipeline[0].Type)
	}
}

func TestParsePipeline_Logfmt(t *testing.T) {
	sel, err := Parse(`{app="svc"} | logfmt`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageLogfmt {
		t.Errorf("expected StageLogfmt, got %d", sel.Pipeline[0].Type)
	}
}

func TestParsePipeline_LabelFormat(t *testing.T) {
	sel, err := Parse(`{app="svc"} | label_format svc=service, lvl=level`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageLabelFormat {
		t.Fatalf("expected StageLabelFormat, got %d", stage.Type)
	}
	if len(stage.Params) != 2 {
		t.Fatalf("expected 2 params, got %d: %v", len(stage.Params), stage.Params)
	}
	if stage.Params[0] != "svc=service" {
		t.Errorf("expected 'svc=service', got %q", stage.Params[0])
	}
	if stage.Params[1] != "lvl=level" {
		t.Errorf("expected 'lvl=level', got %q", stage.Params[1])
	}
}

func TestParsePipeline_LineFormat(t *testing.T) {
	sel, err := Parse(`{app="svc"} | line_format "level={{ .level }}"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageLineFormat {
		t.Fatalf("expected StageLineFormat, got %d", stage.Type)
	}
	if len(stage.Params) != 1 || stage.Params[0] != "level={{ .level }}" {
		t.Errorf("unexpected template: %v", stage.Params)
	}
}

func TestParsePipeline_Drop(t *testing.T) {
	sel, err := Parse(`{app="svc"} | drop level, ts`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageDrop {
		t.Fatalf("expected StageDrop, got %d", stage.Type)
	}
	if len(stage.Params) != 2 || stage.Params[0] != "level" || stage.Params[1] != "ts" {
		t.Errorf("unexpected params: %v", stage.Params)
	}
}

func TestParsePipeline_Keep(t *testing.T) {
	sel, err := Parse(`{app="svc"} | keep level`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageKeep {
		t.Fatalf("expected StageKeep, got %d", stage.Type)
	}
	if len(stage.Params) != 1 || stage.Params[0] != "level" {
		t.Errorf("unexpected params: %v", stage.Params)
	}
}

func TestParsePipeline_LabelFilterEqual(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level = "error"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageLabelFilter {
		t.Fatalf("expected StageLabelFilter, got %d", stage.Type)
	}
	if stage.LabelFilter == nil {
		t.Fatal("expected non-nil LabelFilter")
	}
	if stage.LabelFilter.Name != "level" || stage.LabelFilter.Value != "error" || stage.LabelFilter.Op != OpEqual {
		t.Errorf("unexpected LabelFilter: %+v", stage.LabelFilter)
	}
}

func TestParsePipeline_LabelFilterNotEqual(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level != "debug"`)
	if err != nil {
		t.Fatal(err)
	}
	if stage := sel.Pipeline[0]; stage.LabelFilter.Op != OpNotEqual {
		t.Errorf("expected OpNotEqual, got %d", stage.LabelFilter.Op)
	}
}

func TestParsePipeline_LabelFilterRegex(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level =~ "err.*"`)
	if err != nil {
		t.Fatal(err)
	}
	if stage := sel.Pipeline[0]; stage.LabelFilter.Op != OpRegex {
		t.Errorf("expected OpRegex, got %d", stage.LabelFilter.Op)
	}
}

func TestParsePipeline_LabelFilterNotRegex(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level !~ "debug.*"`)
	if err != nil {
		t.Fatal(err)
	}
	if stage := sel.Pipeline[0]; stage.LabelFilter.Op != OpNotRegex {
		t.Errorf("expected OpNotRegex, got %d", stage.LabelFilter.Op)
	}
}

func TestParsePipeline_LabelFilterGT(t *testing.T) {
	sel, err := Parse(`{app="svc"} | latency_ms > 100`)
	if err != nil {
		t.Fatal(err)
	}
	stage := sel.Pipeline[0]
	if stage.LabelFilter.Op != OpGT {
		t.Errorf("expected OpGT, got %d", stage.LabelFilter.Op)
	}
	if stage.LabelFilter.Value != "100" {
		t.Errorf("expected value '100', got %q", stage.LabelFilter.Value)
	}
}

func TestParsePipeline_LabelFilterLT(t *testing.T) {
	sel, err := Parse(`{app="svc"} | latency_ms < 50`)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Pipeline[0].LabelFilter.Op != OpLT {
		t.Errorf("expected OpLT")
	}
}

func TestParsePipeline_LabelFilterGTE(t *testing.T) {
	sel, err := Parse(`{app="svc"} | latency_ms >= 10`)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Pipeline[0].LabelFilter.Op != OpGTE {
		t.Errorf("expected OpGTE")
	}
}

func TestParsePipeline_LabelFilterLTE(t *testing.T) {
	sel, err := Parse(`{app="svc"} | latency_ms <= 50`)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Pipeline[0].LabelFilter.Op != OpLTE {
		t.Errorf("expected OpLTE")
	}
}

func TestParsePipeline_LabelFilterOr(t *testing.T) {
	sel, err := Parse(`{app="svc"} | json | logfmt | drop __error__, __error_details__ | level="error" or level="warn"`)
	if err != nil {
		t.Fatal(err)
	}
	// json, logfmt, drop, label_filter(or)
	if len(sel.Pipeline) != 4 {
		t.Fatalf("expected 4 pipeline stages, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[3]
	if stage.Type != StageLabelFilter {
		t.Fatalf("expected StageLabelFilter, got %d", stage.Type)
	}
	if stage.LabelFilter.Name != "level" || stage.LabelFilter.Value != "error" || stage.LabelFilter.Op != OpEqual {
		t.Errorf("unexpected primary LabelFilter: %+v", stage.LabelFilter)
	}
	if len(stage.OrFilters) != 1 {
		t.Fatalf("expected 1 OrFilter, got %d", len(stage.OrFilters))
	}
	orf := stage.OrFilters[0]
	if orf.Name != "level" || orf.Value != "warn" || orf.Op != OpEqual {
		t.Errorf("unexpected OrFilter: %+v", orf)
	}
}

func TestParsePipeline_LabelFilterOrTriple(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level="error" or level="warn" or level="info"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.LabelFilter.Value != "error" {
		t.Errorf("expected primary value 'error', got %q", stage.LabelFilter.Value)
	}
	if len(stage.OrFilters) != 2 {
		t.Fatalf("expected 2 OrFilters, got %d", len(stage.OrFilters))
	}
	if stage.OrFilters[0].Value != "warn" {
		t.Errorf("expected first or value 'warn', got %q", stage.OrFilters[0].Value)
	}
	if stage.OrFilters[1].Value != "info" {
		t.Errorf("expected second or value 'info', got %q", stage.OrFilters[1].Value)
	}
}

func TestParsePipeline_LabelFilterOrParen(t *testing.T) {
	// Loki's String() wraps or-expressions in parentheses.
	sel, err := Parse(
		`{app="svc"} | json | logfmt | drop __error__,__error_details__ | ( level="error" or level="warn" )`,
	)
	if err != nil {
		t.Fatal(err)
	}
	// json, logfmt, drop, label_filter(or)
	if len(sel.Pipeline) != 4 {
		t.Fatalf("expected 4 pipeline stages, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[3]
	if stage.Type != StageLabelFilter {
		t.Fatalf("expected StageLabelFilter, got %d", stage.Type)
	}
	if stage.LabelFilter.Name != "level" || stage.LabelFilter.Value != "error" {
		t.Errorf("unexpected primary LabelFilter: %+v", stage.LabelFilter)
	}
	if len(stage.OrFilters) != 1 {
		t.Fatalf("expected 1 OrFilter, got %d", len(stage.OrFilters))
	}
	if stage.OrFilters[0].Name != "level" || stage.OrFilters[0].Value != "warn" {
		t.Errorf("unexpected OrFilter: %+v", stage.OrFilters[0])
	}
}

func TestParsePipeline_Unwrap(t *testing.T) {
	sel, err := Parse(`{app="svc"} | unwrap duration_ms`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageUnwrap {
		t.Fatalf("expected StageUnwrap, got %d", stage.Type)
	}
	if len(stage.Params) != 1 || stage.Params[0] != "duration_ms" {
		t.Errorf("unexpected unwrap param: %v", stage.Params)
	}
}

func TestParsePipeline_MultipleStages(t *testing.T) {
	sel, err := Parse(`{app="svc"} | json | level = "error" | drop ts`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 3 {
		t.Fatalf("expected 3 pipeline stages, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageJSON {
		t.Errorf("expected StageJSON first")
	}
	if sel.Pipeline[1].Type != StageLabelFilter {
		t.Errorf("expected StageLabelFilter second")
	}
	if sel.Pipeline[2].Type != StageDrop {
		t.Errorf("expected StageDrop third")
	}
}

func TestParsePipeline_WithLineFilter(t *testing.T) {
	sel, err := Parse(`{app="svc"} |= "error" | json | level = "critical"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.LineFilters) != 1 {
		t.Errorf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	if len(sel.Pipeline) != 2 {
		t.Fatalf("expected 2 pipeline stages, got %d", len(sel.Pipeline))
	}
}

func TestParsePipeline_LabelFormatSingle(t *testing.T) {
	sel, err := Parse(`{app="svc"} | label_format dst=src`)
	if err != nil {
		t.Fatal(err)
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageLabelFormat {
		t.Fatalf("expected StageLabelFormat")
	}
	if len(stage.Params) != 1 || stage.Params[0] != "dst=src" {
		t.Errorf("unexpected params: %v", stage.Params)
	}
}

func TestParseMetric_CountOverTime(t *testing.T) {
	q, err := ParseQuery(`count_over_time({app="svc"}[5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric, got nil")
	}
	if q.Metric.Function != "count_over_time" {
		t.Errorf("expected count_over_time, got %q", q.Metric.Function)
	}
	if q.Metric.RangeDuration != 5*60*1000000000 {
		t.Errorf("expected 5m, got %v", q.Metric.RangeDuration)
	}
}

func TestParseMetric_Rate(t *testing.T) {
	q, err := ParseQuery(`rate({app="svc"}[30s])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric, got nil")
	}
	if q.Metric.Function != "rate" {
		t.Errorf("expected rate, got %q", q.Metric.Function)
	}
	if q.Metric.RangeDuration != 30*1000000000 {
		t.Errorf("expected 30s, got %v", q.Metric.RangeDuration)
	}
}

func TestParseMetric_WithPipeline(t *testing.T) {
	q, err := ParseQuery(`count_over_time({app="svc"} | json | level = "error" [1m])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric, got nil")
	}
	if len(q.Metric.Pipeline) != 2 {
		t.Fatalf("expected 2 pipeline stages, got %d", len(q.Metric.Pipeline))
	}
}

func TestParseMetric_WithUnwrap(t *testing.T) {
	q, err := ParseQuery(`sum_over_time({app="svc"} | logfmt | unwrap duration [5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric, got nil")
	}
	if q.Metric.Unwrap != "duration" {
		t.Errorf("expected unwrap=duration, got %q", q.Metric.Unwrap)
	}
	if len(q.Metric.Pipeline) != 1 {
		t.Errorf("expected 1 pipeline stage (logfmt), got %d", len(q.Metric.Pipeline))
	}
}

func TestParseVectorAgg_SumBy(t *testing.T) {
	q, err := ParseQuery(`sum by (level, app) (count_over_time({app="svc"}[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if q.VectorAgg == nil {
		t.Fatal("expected VectorAgg, got nil")
	}
	if q.VectorAgg.Op != "sum" {
		t.Errorf("expected sum, got %q", q.VectorAgg.Op)
	}
	if len(q.VectorAgg.GroupBy) != 2 {
		t.Errorf("expected 2 group-by labels, got %d", len(q.VectorAgg.GroupBy))
	}
	if q.VectorAgg.Inner == nil || q.VectorAgg.Inner.Function != "count_over_time" {
		t.Errorf("unexpected inner metric: %v", q.VectorAgg.Inner)
	}
}

func TestParseVectorAgg_Topk(t *testing.T) {
	q, err := ParseQuery(`topk(5, count_over_time({app="svc"}[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if q.VectorAgg == nil {
		t.Fatal("expected VectorAgg, got nil")
	}
	if q.VectorAgg.Op != "topk" {
		t.Errorf("expected topk, got %q", q.VectorAgg.Op)
	}
	if q.VectorAgg.Param != 5 {
		t.Errorf("expected Param=5, got %d", q.VectorAgg.Param)
	}
}

func TestParseQuery_LogSelector(t *testing.T) {
	q, err := ParseQuery(`{app="svc"} | json | level = "error"`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Selector == nil {
		t.Fatal("expected Selector, got nil")
	}
	if len(q.Selector.Pipeline) != 2 {
		t.Errorf("expected 2 pipeline stages, got %d", len(q.Selector.Pipeline))
	}
}

// Tests from Task #4 specification.

func TestParseMetric_CountOverTimeWithLineFilter(t *testing.T) {
	q, err := ParseQuery(`count_over_time({app="foo"} |= "error" [5m])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric")
	}
	if q.Metric.Function != "count_over_time" {
		t.Errorf("expected count_over_time, got %q", q.Metric.Function)
	}
	if len(q.Metric.Selector.LineFilters) != 1 {
		t.Errorf("expected 1 line filter, got %d", len(q.Metric.Selector.LineFilters))
	}
	if q.Metric.RangeDuration.Minutes() != 5 {
		t.Errorf("expected 5m, got %v", q.Metric.RangeDuration)
	}
}

func TestParseMetric_RateWithJSONPipelineAndStatus(t *testing.T) {
	q, err := ParseQuery(`rate({app="foo"} | json | status = "500" [1m])`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Metric == nil {
		t.Fatal("expected Metric")
	}
	if q.Metric.Function != "rate" {
		t.Errorf("expected rate, got %q", q.Metric.Function)
	}
	if len(q.Metric.Pipeline) != 2 {
		t.Errorf("expected 2 pipeline stages (json, label_filter), got %d", len(q.Metric.Pipeline))
	}
	if q.Metric.RangeDuration.Minutes() != 1 {
		t.Errorf("expected 1m, got %v", q.Metric.RangeDuration)
	}
}

func TestParseVectorAgg_SumByApp(t *testing.T) {
	q, err := ParseQuery(`sum by (app) (count_over_time({}[5m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if q.VectorAgg == nil {
		t.Fatal("expected VectorAgg")
	}
	if q.VectorAgg.Op != "sum" {
		t.Errorf("expected sum, got %q", q.VectorAgg.Op)
	}
	if len(q.VectorAgg.GroupBy) != 1 || q.VectorAgg.GroupBy[0] != "app" {
		t.Errorf("expected GroupBy=[app], got %v", q.VectorAgg.GroupBy)
	}
	if q.VectorAgg.Inner.Function != "count_over_time" {
		t.Errorf("expected count_over_time inner, got %q", q.VectorAgg.Inner.Function)
	}
}

func TestParseVectorAgg_TopkRate(t *testing.T) {
	q, err := ParseQuery(`topk(10, rate({}[1m]))`)
	if err != nil {
		t.Fatal(err)
	}
	if q.VectorAgg == nil {
		t.Fatal("expected VectorAgg")
	}
	if q.VectorAgg.Op != "topk" {
		t.Errorf("expected topk, got %q", q.VectorAgg.Op)
	}
	if q.VectorAgg.Param != 10 {
		t.Errorf("expected Param=10, got %d", q.VectorAgg.Param)
	}
	if q.VectorAgg.Inner.Function != "rate" {
		t.Errorf("expected rate inner, got %q", q.VectorAgg.Inner.Function)
	}
}

// TestAST verifies that all new AST types are correctly defined and usable.
func TestAST_PipelineStageTypes(t *testing.T) {
	// Verify PipelineStageType constants are distinct.
	types := []PipelineStageType{
		StageJSON, StageLogfmt, StageLabelFormat, StageLineFormat,
		StageDrop, StageKeep, StageLabelFilter,
	}
	seen := make(map[PipelineStageType]bool)
	for _, st := range types {
		if seen[st] {
			t.Errorf("duplicate PipelineStageType value: %d", st)
		}
		seen[st] = true
	}
}

func TestAST_FilterOpConstants(t *testing.T) {
	// Verify FilterOp constants are distinct.
	ops := []FilterOp{
		OpEqual, OpNotEqual, OpRegex, OpNotRegex,
		OpGT, OpLT, OpGTE, OpLTE,
	}
	seen := make(map[FilterOp]bool)
	for _, op := range ops {
		if seen[op] {
			t.Errorf("duplicate FilterOp value: %d", op)
		}
		seen[op] = true
	}
}

func TestAST_PipelineStage(t *testing.T) {
	// Verify PipelineStage struct fields.
	stage := PipelineStage{
		Type: StageLabelFilter,
		LabelFilter: &LabelFilter{
			Name:  "level",
			Value: "error",
			Op:    OpEqual,
		},
	}
	if stage.Type != StageLabelFilter {
		t.Errorf("expected StageLabelFilter, got %d", stage.Type)
	}
	if stage.LabelFilter == nil {
		t.Fatal("expected non-nil LabelFilter")
	}
	if stage.LabelFilter.Op != OpEqual {
		t.Errorf("expected OpEqual, got %d", stage.LabelFilter.Op)
	}
}

func TestAST_LogSelectorPipeline(t *testing.T) {
	// Verify LogSelector includes Pipeline field.
	sel := LogSelector{
		Matchers: []LabelMatcher{{Name: "app", Value: "myapp", Type: MatchEqual}},
		Pipeline: []PipelineStage{
			{Type: StageJSON},
			{Type: StageLabelFilter, LabelFilter: &LabelFilter{Name: "level", Value: "error", Op: OpEqual}},
		},
	}
	if len(sel.Matchers) != 1 {
		t.Errorf("expected 1 matcher, got %d", len(sel.Matchers))
	}
	if len(sel.Pipeline) != 2 {
		t.Errorf("expected 2 pipeline stages, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageJSON {
		t.Errorf("expected StageJSON first, got %d", sel.Pipeline[0].Type)
	}
}

func TestAST_MetricExpr(t *testing.T) {
	// Verify MetricExpr struct fields.
	sel := &LogSelector{Matchers: []LabelMatcher{{Name: "app", Value: "svc", Type: MatchEqual}}}
	expr := MetricExpr{
		Function: "count_over_time",
		Selector: sel,
	}
	if expr.Function != "count_over_time" {
		t.Errorf("expected count_over_time, got %q", expr.Function)
	}
	if expr.Selector != sel {
		t.Error("selector mismatch")
	}
}

func TestAST_VectorAggExpr(t *testing.T) {
	// Verify VectorAggExpr struct fields.
	inner := &MetricExpr{Function: "count_over_time"}
	agg := VectorAggExpr{
		Op:      "sum",
		GroupBy: []string{"level", "app"},
		Inner:   inner,
	}
	if agg.Op != "sum" {
		t.Errorf("expected sum, got %q", agg.Op)
	}
	if len(agg.GroupBy) != 2 {
		t.Errorf("expected 2 group-by labels, got %d", len(agg.GroupBy))
	}
	if agg.Inner != inner {
		t.Error("inner metric expr mismatch")
	}
}

func TestAST_VectorAggExpr_TopK(t *testing.T) {
	// Verify Param field for topk/bottomk.
	agg := VectorAggExpr{
		Param: 10,
	}
	if agg.Param != 10 {
		t.Errorf("expected Param=10, got %d", agg.Param)
	}
}

func TestParsePipeline_LineFilterAfterLabelFilter(t *testing.T) {
	sel, err := Parse(`{app="svc"} | level="error" |~ "timeout"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	if sel.Pipeline[0].Type != StageLabelFilter {
		t.Fatalf("expected StageLabelFilter, got %d", sel.Pipeline[0].Type)
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterRegex || sel.LineFilters[0].Pattern != "timeout" {
		t.Errorf("unexpected line filter: %+v", sel.LineFilters[0])
	}
}

func TestParsePipeline_MultiLineFiltersAfterPipeline(t *testing.T) {
	sel, err := Parse(`{app="svc"} | json | level="error" |= "text" |~ "regex"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 2 { // json + label_filter
		t.Fatalf("expected 2 pipeline stages, got %d", len(sel.Pipeline))
	}
	if len(sel.LineFilters) != 2 {
		t.Fatalf("expected 2 line filters, got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterContains {
		t.Errorf("expected FilterContains, got %d", sel.LineFilters[0].Type)
	}
	if sel.LineFilters[1].Type != FilterRegex {
		t.Errorf("expected FilterRegex, got %d", sel.LineFilters[1].Type)
	}
}

func TestParsePipeline_NestedParenOr(t *testing.T) {
	sel, err := Parse(`{app="svc"} | ( ( level="error" or level="warn" ) or level="info" )`)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Pipeline) != 1 {
		t.Fatalf("expected 1 pipeline stage, got %d", len(sel.Pipeline))
	}
	stage := sel.Pipeline[0]
	if stage.Type != StageLabelFilter {
		t.Fatalf("expected StageLabelFilter, got %d", stage.Type)
	}
	if stage.LabelFilter.Value != "error" {
		t.Errorf("expected primary value 'error', got %q", stage.LabelFilter.Value)
	}
	// The inner ( level="error" or level="warn" ) has 1 OrFilter (warn)
	// Then the outer `or level="info"` adds another
	if len(stage.OrFilters) != 2 {
		t.Fatalf("expected 2 OrFilters, got %d", len(stage.OrFilters))
	}
}

func TestParsePipeline_LokiComplexSelector(t *testing.T) {
	// Exact string from Loki's expr.Selector().String()
	sel, err := Parse(
		`{service_name=~"(?i)loki"} | ( ( detected_level="debug" or detected_level="info" ) or detected_level="warn" ) |~ "(?i)(?i)duration" | json | logfmt | drop __error__,__error_details__ | level=~"(?i)INFO"`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel.Matchers) != 1 {
		t.Errorf("expected 1 matcher, got %d", len(sel.Matchers))
	}
	if len(sel.LineFilters) != 1 {
		t.Fatalf("expected 1 line filter (|~ regex), got %d", len(sel.LineFilters))
	}
	if sel.LineFilters[0].Type != FilterRegex {
		t.Errorf("expected FilterRegex, got %d", sel.LineFilters[0].Type)
	}
	// Pipeline: paren_or, json, logfmt, drop, label_filter
	if len(sel.Pipeline) != 5 {
		t.Fatalf("expected 5 pipeline stages, got %d", len(sel.Pipeline))
	}
	// First pipeline stage is the or filter
	if sel.Pipeline[0].Type != StageLabelFilter {
		t.Errorf("expected StageLabelFilter first, got %d", sel.Pipeline[0].Type)
	}
	if len(sel.Pipeline[0].OrFilters) != 2 {
		t.Errorf("expected 2 OrFilters, got %d", len(sel.Pipeline[0].OrFilters))
	}
}

func TestAST_LogQuery(t *testing.T) {
	// Verify LogQuery union type with each variant.
	sel := &LogSelector{}
	qSel := LogQuery{Selector: sel}
	if qSel.Selector != sel || qSel.Metric != nil || qSel.VectorAgg != nil {
		t.Error("LogQuery with Selector: unexpected non-nil fields")
	}

	metric := &MetricExpr{Function: "rate"}
	qMetric := LogQuery{Metric: metric}
	if qMetric.Metric != metric || qMetric.Selector != nil || qMetric.VectorAgg != nil {
		t.Error("LogQuery with Metric: unexpected non-nil fields")
	}

	agg := &VectorAggExpr{Op: "sum", Inner: metric}
	qAgg := LogQuery{VectorAgg: agg}
	if qAgg.VectorAgg != agg || qAgg.Selector != nil || qAgg.Metric != nil {
		t.Error("LogQuery with VectorAgg: unexpected non-nil fields")
	}
}

// GAP-30: TestParseQuery_CRLFLineEndings verifies that queries with Windows-style
// CRLF line endings parse successfully (same result as LF-only equivalent).
func TestParseQuery_CRLFLineEndings(t *testing.T) {
	// A query with CRLF between selector and pipeline stage.
	query := "{service_name=\"svc\"}\r\n| json"
	q, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("ParseQuery with CRLF line ending must not error, got: %v", err)
	}
	if q.Selector == nil {
		t.Fatal("expected Selector to be non-nil")
	}
	if len(q.Selector.Matchers) != 1 {
		t.Errorf("expected 1 matcher, got %d", len(q.Selector.Matchers))
	}
	// The test verifies the query with a CRLF between selector and pipeline parses
	// correctly without error or panic.
}

// GAP-29: TestParseQuery_NestedBraces verifies that a regex containing brace quantifiers
// (e.g., "a{1,3}") is parsed correctly and does not misinterpret braces as a nested block.
func TestParseQuery_NestedBraces(t *testing.T) {
	// Regex with brace quantifier — must parse without error.
	q, err := Parse(`{service_name=~"a{1,3}"}`)
	if err != nil {
		t.Fatalf("Parse with brace quantifier in regex must not error, got: %v", err)
	}
	if len(q.Matchers) != 1 {
		t.Fatalf("expected 1 matcher, got %d", len(q.Matchers))
	}
	m := q.Matchers[0]
	if m.Type != MatchRegex {
		t.Errorf("expected MatchRegex, got %v", m.Type)
	}
	if m.Value != "a{1,3}" {
		t.Errorf("expected regex value %q, got %q", "a{1,3}", m.Value)
	}
	// Also verify that a regex with literal braces in a string value parses correctly.
	q2, err := Parse(`{msg=~"use {braces}"}`)
	if err != nil {
		t.Fatalf("Parse with literal braces in regex must not error, got: %v", err)
	}
	if len(q2.Matchers) != 1 || q2.Matchers[0].Value != "use {braces}" {
		t.Errorf("unexpected matcher: %+v", q2.Matchers)
	}
}
