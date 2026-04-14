package logqlparser

import (
	"testing"
	"text/template"
)

// LQL-TEST-001: TestPipeline_Empty
func TestPipeline_Empty(t *testing.T) {
	p := &Pipeline{}
	line, labels, keep := p.Process(1000, "hello", NewMapLabelSet(map[string]string{"app": "foo"}))
	if !keep {
		t.Fatalf("expected keep=true, got false")
	}
	if line != "hello" {
		t.Errorf("expected line=%q, got %q", "hello", line)
	}
	if labels.Get("app") != "foo" {
		t.Errorf("expected labels[app]=foo, got %q", labels.Get("app"))
	}
}

// LQL-TEST-001 (nil variant)
func TestPipeline_Nil(t *testing.T) {
	var p *Pipeline
	line, labels, keep := p.Process(1000, "hello", NewMapLabelSet(map[string]string{"app": "foo"}))
	if !keep {
		t.Fatalf("expected keep=true for nil pipeline, got false")
	}
	if line != "hello" {
		t.Errorf("expected line=%q, got %q", "hello", line)
	}
	if labels.Get("app") != "foo" {
		t.Errorf("expected labels[app]=foo, got %q", labels.Get("app"))
	}
}

// LQL-TEST-002: TestPipeline_ShortCircuit
func TestPipeline_ShortCircuit(t *testing.T) {
	stage1 := func(_ uint64, line string, labels LabelSet) (string, LabelSet, bool) {
		return line, labels, false // drop all rows
	}
	stage2 := func(_ uint64, _ string, labels LabelSet) (string, LabelSet, bool) {
		t.Fatal("stage2 should not be called after stage1 drops the row")
		return "", labels, false
	}
	p := &Pipeline{Stages: []PipelineStageFunc{stage1, stage2}}
	_, _, keep := p.Process(0, "hello", NewEmptyLabelSet())
	if keep {
		t.Fatalf("expected keep=false, got true")
	}
}

// LQL-TEST-003: TestJSONStage_ValidJSON
func TestJSONStage_ValidJSON(t *testing.T) {
	stage := JSONStage()
	ls := NewEmptyLabelSet()
	_, ls, keep := stage(0, `{"level":"info","msg":"started","count":42}`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected labels[level]=info, got %q", ls.Get("level"))
	}
	if ls.Get("msg") != "started" {
		t.Errorf("expected labels[msg]=started, got %q", ls.Get("msg"))
	}
	if ls.Get("count") != "42" {
		t.Errorf("expected labels[count]=42, got %q", ls.Get("count"))
	}
}

// LQL-TEST-004: TestJSONStage_InvalidJSON
func TestJSONStage_InvalidJSON(t *testing.T) {
	stage := JSONStage()
	ls := NewMapLabelSet(map[string]string{"app": "foo"})
	line, ls, keep := stage(0, "not json", ls)
	if !keep {
		t.Fatalf("expected keep=true for invalid JSON")
	}
	if line != "not json" {
		t.Errorf("expected line unchanged, got %q", line)
	}
	if ls.Get("app") != "foo" {
		t.Errorf("expected labels unchanged, got app=%q", ls.Get("app"))
	}
}

// LQL-TEST-005: TestJSONStage_NestedValues
func TestJSONStage_NestedValues(t *testing.T) {
	stage := JSONStage()
	ls := NewEmptyLabelSet()
	_, ls, keep := stage(0, `{"outer": {"inner": "val"}}`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("outer") == "" {
		t.Errorf("expected labels[outer] to be a non-empty string, got %q", ls.Get("outer"))
	}
}

// LQL-TEST-006: TestLogfmtStage_Valid
func TestLogfmtStage_Valid(t *testing.T) {
	stage := LogfmtStage()
	ls := NewEmptyLabelSet()
	_, ls, keep := stage(0, "level=info msg=started latency=12.5ms", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected labels[level]=info, got %q", ls.Get("level"))
	}
	if ls.Get("msg") != "started" {
		t.Errorf("expected labels[msg]=started, got %q", ls.Get("msg"))
	}
	if ls.Get("latency") != "12.5ms" {
		t.Errorf("expected labels[latency]=12.5ms, got %q", ls.Get("latency"))
	}
}

// LQL-TEST-007: TestLogfmtStage_Invalid
func TestLogfmtStage_Invalid(t *testing.T) {
	stage := LogfmtStage()
	ls := NewMapLabelSet(map[string]string{"app": "foo"})
	_, ls, keep := stage(0, "this is not valid logfmt ====", ls)
	if !keep {
		t.Fatalf("expected keep=true even for invalid logfmt")
	}
	// app label should still be present
	if ls.Get("app") != "foo" {
		t.Errorf("expected labels[app]=foo, got %q", ls.Get("app"))
	}
}

// LQL-TEST-008: TestLabelFormatStage_Rename
func TestLabelFormatStage_Rename(t *testing.T) {
	stage := LabelFormatStage(map[string]string{"dst": "src"})
	ls := NewMapLabelSet(map[string]string{"src": "value", "other": "x"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("dst") != "value" {
		t.Errorf("expected labels[dst]=value, got %q", ls.Get("dst"))
	}
	for _, k := range ls.Keys() {
		if k == "src" {
			t.Errorf("expected labels[src] to be absent after rename")
		}
	}
	if ls.Get("other") != "x" {
		t.Errorf("expected labels[other]=x, got %q", ls.Get("other"))
	}
}

// LQL-TEST-009: TestLabelFormatStage_MissingSource
func TestLabelFormatStage_MissingSource(t *testing.T) {
	stage := LabelFormatStage(map[string]string{"dst": "missing"})
	ls := NewMapLabelSet(map[string]string{"other": "x"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("dst") != "" {
		t.Errorf("expected labels[dst]=\"\", got %q", ls.Get("dst"))
	}
}

// LQL-TEST-010: TestLineFormatStage_Basic
func TestLineFormatStage_Basic(t *testing.T) {
	tmpl, err := template.New("t").Option("missingkey=zero").Parse("level={{ .level }} app={{ .app }}")
	if err != nil {
		t.Fatalf("template parse error: %v", err)
	}
	stage := LineFormatStage(tmpl)
	ls := NewMapLabelSet(map[string]string{"level": "warn", "app": "svc"})
	line, _, keep := stage(0, "original line", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if line != "level=warn app=svc" {
		t.Errorf("expected %q, got %q", "level=warn app=svc", line)
	}
}

// LQL-TEST-011: TestLineFormatStage_MissingKey
func TestLineFormatStage_MissingKey(t *testing.T) {
	tmpl, err := template.New("t").Option("missingkey=zero").Parse("{{ .missing }}")
	if err != nil {
		t.Fatalf("template parse error: %v", err)
	}
	stage := LineFormatStage(tmpl)
	ls := NewEmptyLabelSet()
	_, _, keep := stage(0, "original", ls)
	if !keep {
		t.Fatalf("expected keep=true even with missing key")
	}
	// With missingkey=zero, template produces empty string for missing keys.
	// The row is kept regardless of template output.
}

// LQL-TEST-012: TestDropStage
func TestDropStage(t *testing.T) {
	stage := DropStage([]string{"level", "ts"})
	ls := NewMapLabelSet(map[string]string{"level": "info", "ts": "123", "msg": "hello"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	for _, k := range ls.Keys() {
		if k == "level" {
			t.Errorf("expected labels[level] to be absent")
		}
		if k == "ts" {
			t.Errorf("expected labels[ts] to be absent")
		}
	}
	if ls.Get("msg") != "hello" {
		t.Errorf("expected labels[msg]=hello, got %q", ls.Get("msg"))
	}
}

// LQL-TEST-013: TestKeepStage
func TestKeepStage(t *testing.T) {
	stage := KeepStage([]string{"level"})
	ls := NewMapLabelSet(map[string]string{"level": "info", "ts": "123", "msg": "hello"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected labels[level]=info, got %q", ls.Get("level"))
	}
	for _, k := range ls.Keys() {
		if k == "ts" {
			t.Errorf("expected labels[ts] to be absent")
		}
		if k == "msg" {
			t.Errorf("expected labels[msg] to be absent")
		}
	}
}

// LQL-TEST-014: TestLabelFilterStage_Equal
func TestLabelFilterStage_Equal(t *testing.T) {
	stage := LabelFilterStage("level", "error", OpEqual)
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for matching equal filter")
	}
	ls2 := NewMapLabelSet(map[string]string{"level": "info"})
	_, _, keep2 := stage(0, "", ls2)
	if keep2 {
		t.Fatalf("expected keep=false for non-matching equal filter")
	}
}

// LQL-TEST-015: TestLabelFilterStage_NotEqual
func TestLabelFilterStage_NotEqual(t *testing.T) {
	stage := LabelFilterStage("level", "error", OpNotEqual)
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for matching not-equal filter")
	}
	ls2 := NewMapLabelSet(map[string]string{"level": "info"})
	_, _, keep2 := stage(0, "", ls2)
	if !keep2 {
		t.Fatalf("expected keep=true for non-matching not-equal filter")
	}
}

// LQL-TEST-016: TestLabelFilterStage_Regex
func TestLabelFilterStage_Regex(t *testing.T) {
	stage := LabelFilterStage("level", "err.*", OpRegex)
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for matching regex filter")
	}
	ls2 := NewMapLabelSet(map[string]string{"level": "info"})
	_, _, keep2 := stage(0, "", ls2)
	if keep2 {
		t.Fatalf("expected keep=false for non-matching regex filter")
	}
}

func TestLabelFilterStage_NotRegex(t *testing.T) {
	stage := LabelFilterStage("level", "err.*", OpNotRegex)
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false when label matches not-regex filter")
	}
	ls2 := NewMapLabelSet(map[string]string{"level": "info"})
	_, _, keep2 := stage(0, "", ls2)
	if !keep2 {
		t.Fatalf("expected keep=true when label does not match not-regex filter")
	}
}

// LQL-TEST-017: TestLabelFilterStage_NumericGT
func TestLabelFilterStage_NumericGT(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpGT)
	ls := NewMapLabelSet(map[string]string{"latency_ms": "150"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for 150 > 100")
	}
	ls2 := NewMapLabelSet(map[string]string{"latency_ms": "50"})
	_, _, keep2 := stage(0, "", ls2)
	if keep2 {
		t.Fatalf("expected keep=false for 50 > 100")
	}
}

func TestLabelFilterStage_NumericLT(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpLT)
	ls := NewMapLabelSet(map[string]string{"latency_ms": "50"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for 50 < 100")
	}
	ls2 := NewMapLabelSet(map[string]string{"latency_ms": "150"})
	_, _, keep2 := stage(0, "", ls2)
	if keep2 {
		t.Fatalf("expected keep=false for 150 < 100")
	}
}

func TestLabelFilterStage_NumericGTE(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpGTE)
	ls := NewMapLabelSet(map[string]string{"latency_ms": "100"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for 100 >= 100")
	}
}

func TestLabelFilterStage_NumericLTE(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpLTE)
	ls := NewMapLabelSet(map[string]string{"latency_ms": "100"})
	_, _, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for 100 <= 100")
	}
}

// LQL-TEST-018: TestLabelFilterStage_NumericNonParseable
func TestLabelFilterStage_NumericNonParseable(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpGT)
	ls := NewMapLabelSet(map[string]string{"latency_ms": "not-a-number"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for non-parseable numeric label")
	}
}

func TestLabelFilterStage_MissingLabel(t *testing.T) {
	stage := LabelFilterStage("latency_ms", "100", OpGT)
	ls := NewEmptyLabelSet()
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for missing label with numeric op")
	}
}

// LQL-TEST-019: TestUnwrapStage_Valid
func TestUnwrapStage_Valid(t *testing.T) {
	stage := UnwrapStage("duration_ms")
	ls := NewMapLabelSet(map[string]string{"duration_ms": "42.5"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true for valid numeric label")
	}
	if ls.Get(UnwrapValueKey) == "" {
		t.Errorf("expected labels[%s] to be set", UnwrapValueKey)
	}
	if ls.Get(UnwrapValueKey) != "42.5" {
		t.Errorf("expected labels[%s]=42.5, got %q", UnwrapValueKey, ls.Get(UnwrapValueKey))
	}
}

// LQL-TEST-020: TestUnwrapStage_MissingLabel
func TestUnwrapStage_MissingLabel(t *testing.T) {
	stage := UnwrapStage("duration_ms")
	ls := NewMapLabelSet(map[string]string{"other": "x"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for absent label in unwrap")
	}
}

// LQL-TEST-021: TestUnwrapStage_NonNumeric
func TestUnwrapStage_NonNumeric(t *testing.T) {
	stage := UnwrapStage("duration_ms")
	ls := NewMapLabelSet(map[string]string{"duration_ms": "not-a-number"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for non-numeric label in unwrap")
	}
}

// LQL-TEST-022: TestPipeline_JSONThenLabelFilter
func TestPipeline_JSONThenLabelFilter(t *testing.T) {
	p := &Pipeline{Stages: []PipelineStageFunc{
		JSONStage(),
		LabelFilterStage("level", "error", OpEqual),
	}}

	// Row 1: should pass (level=error)
	_, ls1, keep1 := p.Process(0, `{"level":"error","svc":"api"}`, NewEmptyLabelSet())
	if !keep1 {
		t.Errorf("row 1 should pass")
	}
	if ls1.Get("level") != "error" {
		t.Errorf("expected level=error, got %q", ls1.Get("level"))
	}

	// Row 2: should drop (level=info)
	_, _, keep2 := p.Process(0, `{"level":"info","svc":"api"}`, NewEmptyLabelSet())
	if keep2 {
		t.Errorf("row 2 should be dropped (level=info)")
	}

	// Row 3: should drop (plain text, no json level key)
	_, _, keep3 := p.Process(0, "plain text", NewEmptyLabelSet())
	if keep3 {
		t.Errorf("row 3 should be dropped (no level label)")
	}
}

// LQL-TEST-023: TestPipeline_LogfmtThenLabelFormat
func TestPipeline_LogfmtThenLabelFormat(t *testing.T) {
	p := &Pipeline{Stages: []PipelineStageFunc{
		LogfmtStage(),
		LabelFormatStage(map[string]string{"svc": "service"}),
	}}
	_, ls, keep := p.Process(0, "service=web level=info", NewEmptyLabelSet())
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("svc") != "web" {
		t.Errorf("expected labels[svc]=web, got %q", ls.Get("svc"))
	}
	for _, k := range ls.Keys() {
		if k == "service" {
			t.Errorf("expected labels[service] to be absent after rename")
		}
	}
}

// LQL-TEST-024: TestPipeline_JSONThenDropKeep
func TestPipeline_JSONThenDropKeep(t *testing.T) {
	p := &Pipeline{Stages: []PipelineStageFunc{
		JSONStage(),
		DropStage([]string{"ts"}),
		KeepStage([]string{"level"}),
	}}
	_, ls, keep := p.Process(0, `{"level":"warn","ts":"123","msg":"hello"}`, NewEmptyLabelSet())
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "warn" {
		t.Errorf("expected labels[level]=warn, got %q", ls.Get("level"))
	}
	for _, k := range ls.Keys() {
		if k == "ts" {
			t.Errorf("expected labels[ts] to be absent")
		}
		if k == "msg" {
			t.Errorf("expected labels[msg] to be absent")
		}
	}
}

// TestLabelFilterStage_InvalidRegex verifies that an invalid regex always drops rows.
func TestLabelFilterStage_InvalidRegex(t *testing.T) {
	stage := LabelFilterStage("level", "[invalid", OpRegex)
	ls := NewMapLabelSet(map[string]string{"level": "error"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for invalid regex pattern")
	}
}

// TestLabelFilterStage_InvalidNumericThreshold verifies that an invalid threshold always drops rows.
func TestLabelFilterStage_InvalidNumericThreshold(t *testing.T) {
	stage := LabelFilterStage("count", "not-a-number", OpGT)
	ls := NewMapLabelSet(map[string]string{"count": "42"})
	_, _, keep := stage(0, "", ls)
	if keep {
		t.Fatalf("expected keep=false for invalid numeric threshold")
	}
}

// TestLabelFormatStage_SameSourceDest verifies dst==src is a no-op rename (doesn't delete).
func TestLabelFormatStage_SameSourceDest(t *testing.T) {
	stage := LabelFormatStage(map[string]string{"level": "level"})
	ls := NewMapLabelSet(map[string]string{"level": "info"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected labels[level]=info after no-op rename, got %q", ls.Get("level"))
	}
}

// TestDropStage_MissingFields verifies drop of absent fields is a no-op.
func TestDropStage_MissingFields(t *testing.T) {
	stage := DropStage([]string{"nonexistent"})
	ls := NewMapLabelSet(map[string]string{"level": "info"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "info" {
		t.Errorf("expected labels[level]=info, got %q", ls.Get("level"))
	}
}

// TestKeepStage_Empty verifies that keeping an empty set removes all labels.
func TestKeepStage_Empty(t *testing.T) {
	stage := KeepStage([]string{})
	ls := NewMapLabelSet(map[string]string{"level": "info", "app": "svc"})
	_, ls, keep := stage(0, "", ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if len(ls.Keys()) != 0 {
		t.Errorf("expected all labels removed, got %v", ls.Keys())
	}
}

func TestOrLabelFilterStage_PrimaryMatch(t *testing.T) {
	primary := LabelFilterStage("level", "error", OpEqual)
	alt := LabelFilterStage("level", "warn", OpEqual)
	stage := OrLabelFilterStage(primary, []PipelineStageFunc{alt})

	_, _, keep := stage(0, "line", NewMapLabelSet(map[string]string{"level": "error"}))
	if !keep {
		t.Fatal("expected keep=true when primary matches")
	}
}

func TestOrLabelFilterStage_AltMatch(t *testing.T) {
	primary := LabelFilterStage("level", "error", OpEqual)
	alt := LabelFilterStage("level", "warn", OpEqual)
	stage := OrLabelFilterStage(primary, []PipelineStageFunc{alt})

	_, _, keep := stage(0, "line", NewMapLabelSet(map[string]string{"level": "warn"}))
	if !keep {
		t.Fatal("expected keep=true when alt matches")
	}
}

func TestOrLabelFilterStage_NoneMatch(t *testing.T) {
	primary := LabelFilterStage("level", "error", OpEqual)
	alt := LabelFilterStage("level", "warn", OpEqual)
	stage := OrLabelFilterStage(primary, []PipelineStageFunc{alt})

	_, _, keep := stage(0, "line", NewMapLabelSet(map[string]string{"level": "info"}))
	if keep {
		t.Fatal("expected keep=false when none match")
	}
}

// LQP-TEST-127: JSONStage skips keys already present in labels map (SPEC-11.5 no-op)
func TestJSONStage_SkipsPrePopulatedKeys(t *testing.T) {
	stage := JSONStage()
	// Pre-populate "level" — ingest stored it from the body column
	ls := NewMapLabelSet(map[string]string{"level": "stored-at-ingest"})
	_, ls, keep := stage(0, `{"level":"from-pipeline","msg":"hello"}`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	// The stored value must win; pipeline does NOT overwrite it
	if ls.Get("level") != "stored-at-ingest" {
		t.Errorf("expected level=stored-at-ingest (ingest wins), got %q", ls.Get("level"))
	}
	// But a new key (msg) should still be extracted
	if ls.Get("msg") != "hello" {
		t.Errorf("expected msg=hello, got %q", ls.Get("msg"))
	}
}

// LQP-TEST-128: LogfmtStage skips keys already present in labels map
func TestLogfmtStage_SkipsPrePopulatedKeys(t *testing.T) {
	stage := LogfmtStage()
	ls := NewMapLabelSet(map[string]string{"level": "stored-at-ingest"})
	_, ls, keep := stage(0, `level=from-pipeline msg=timeout`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "stored-at-ingest" {
		t.Errorf("expected level=stored-at-ingest (ingest wins), got %q", ls.Get("level"))
	}
	if ls.Get("msg") != "timeout" {
		t.Errorf("expected msg=timeout, got %q", ls.Get("msg"))
	}
}

// LQP-TEST-129: JSONStage with empty labels still extracts all fields normally
func TestJSONStage_EmptyLabels_ExtractsAll(t *testing.T) {
	stage := JSONStage()
	ls := NewEmptyLabelSet()
	_, ls, keep := stage(0, `{"level":"warn","count":99}`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "warn" {
		t.Errorf("expected level=warn, got %q", ls.Get("level"))
	}
	if ls.Get("count") != "99" {
		t.Errorf("expected count=99, got %q", ls.Get("count"))
	}
}

// LQP-TEST-130: LogfmtStage with empty labels still extracts all fields normally
func TestLogfmtStage_EmptyLabels_ExtractsAll(t *testing.T) {
	stage := LogfmtStage()
	ls := NewEmptyLabelSet()
	_, ls, keep := stage(0, `level=error service=payments`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if ls.Get("level") != "error" {
		t.Errorf("expected level=error, got %q", ls.Get("level"))
	}
	if ls.Get("service") != "payments" {
		t.Errorf("expected service=payments, got %q", ls.Get("service"))
	}
}

// GAP-28: TestPipeline_UnicodeLabels verifies that label values containing Unicode
// (emoji, non-ASCII scripts) survive a pipeline stage round-trip unchanged.
func TestPipeline_UnicodeLabels(t *testing.T) {
	cases := []struct {
		key   string
		value string
	}{
		{"emoji", "🚀"},
		{"arabic", "مرحبا"},
		{"chinese", "你好"},
		{"mixed", "level=error 🔴"},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			// Use a label-filter stage that passes rows having the key present.
			stage := LabelFilterStage(tc.key, ".*", OpRegex) // regex matches anything
			ls := NewMapLabelSet(map[string]string{tc.key: tc.value})
			_, ls2, keep := stage(0, "", ls)
			if !keep {
				t.Fatalf("expected keep=true for unicode label %q=%q", tc.key, tc.value)
			}
			got := ls2.Get(tc.key)
			if got != tc.value {
				t.Errorf("unicode label %q: expected %q, got %q", tc.key, tc.value, got)
			}
		})
	}
}

// GAP-29: TestPipeline_EmptyStages verifies that a Pipeline with no stages is a no-op:
// returns the same line and labels with keep=true.
// This is a complement to TestPipeline_Empty which tests the struct with empty Stages slice.
func TestPipeline_EmptyStages(t *testing.T) {
	p := &Pipeline{Stages: nil}
	line, labels, keep := p.Process(0, "test-line", NewMapLabelSet(map[string]string{"k": "v"}))
	if !keep {
		t.Fatalf("expected keep=true for empty-stages pipeline, got false")
	}
	if line != "test-line" {
		t.Errorf("expected line unchanged, got %q", line)
	}
	if labels.Get("k") != "v" {
		t.Errorf("expected labels[k]=v, got %q", labels.Get("k"))
	}
}

// hasLiveMockLabelSet is a test-only LabelSet where Has() returns true for keys in
// storedKeys (simulating undecoded block columns) but HasLive() returns false for them.
// Keys in liveKeys have both Has() and HasLive() return true (simulating decoded/overlay).
type hasLiveMockLabelSet struct {
	storedKeys map[string]bool
	liveKeys   map[string]bool
	overlay    map[string]string
}

func newHasLiveMock(stored, live []string) *hasLiveMockLabelSet {
	m := &hasLiveMockLabelSet{
		storedKeys: make(map[string]bool, len(stored)),
		liveKeys:   make(map[string]bool, len(live)),
		overlay:    make(map[string]string),
	}
	for _, k := range stored {
		m.storedKeys[k] = true
	}
	for _, k := range live {
		m.liveKeys[k] = true
	}
	return m
}

func (m *hasLiveMockLabelSet) Get(key string) string { return m.overlay[key] }
func (m *hasLiveMockLabelSet) Has(key string) bool {
	if m.storedKeys[key] || m.liveKeys[key] {
		return true
	}
	_, inOverlay := m.overlay[key]
	return inOverlay
}

func (m *hasLiveMockLabelSet) HasLive(key string) bool {
	_, inOverlay := m.overlay[key]
	return m.liveKeys[key] || inOverlay
}
func (m *hasLiveMockLabelSet) Set(key, val string) { m.overlay[key] = val }
func (m *hasLiveMockLabelSet) Delete(key string) {
	delete(m.overlay, key)
	delete(m.liveKeys, key)
}

func (m *hasLiveMockLabelSet) Keys() []string {
	seen := make(map[string]bool)
	var out []string
	for k := range m.storedKeys {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for k := range m.liveKeys {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for k := range m.overlay {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func (m *hasLiveMockLabelSet) Materialize() map[string]string {
	out := make(map[string]string, len(m.overlay))
	for k, v := range m.overlay {
		out[k] = v
	}
	return out
}
func (m *hasLiveMockLabelSet) HideBodyParsedColumns() {} // no-op in tests

// LQP-TEST-131: LogfmtStage extracts field when HasLive=false (undecoded column simulation)
func TestLogfmtStage_ExtractsFieldWhenHasLiveFalse(t *testing.T) {
	stage := LogfmtStage()
	ls := newHasLiveMock([]string{"caller"}, []string{"level"})
	ls.overlay["level"] = "stored-level"

	_, out, keep := stage(0, `caller=main.go level=debug msg=hello`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if out.Get("caller") != "main.go" {
		t.Errorf("expected caller=main.go (extracted from logfmt), got %q", out.Get("caller"))
	}
	if out.Get("level") != "stored-level" {
		t.Errorf("expected level=stored-level (live wins), got %q", out.Get("level"))
	}
	if out.Get("msg") != "hello" {
		t.Errorf("expected msg=hello, got %q", out.Get("msg"))
	}
}

// LQP-TEST-132: LogfmtStage sets __error__="" on successful parse
func TestLogfmtStage_SetsErrorLabelOnSuccess(t *testing.T) {
	stage := LogfmtStage()
	ls := NewEmptyLabelSet()
	_, out, keep := stage(0, `level=info msg=ok`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if !out.Has("__error__") {
		t.Errorf("expected __error__ label to be set on successful logfmt parse")
	}
	if out.Get("__error__") != "" {
		t.Errorf("expected __error__=\"\" on success, got %q", out.Get("__error__"))
	}
}

// LQP-TEST-133: JSONStage extracts field when HasLive=false (undecoded column simulation)
func TestJSONStage_ExtractsFieldWhenHasLiveFalse(t *testing.T) {
	stage := JSONStage()
	ls := newHasLiveMock([]string{"caller"}, []string{"level"})
	ls.overlay["level"] = "stored-level"

	_, out, keep := stage(0, `{"caller":"main.go","level":"debug","msg":"hello"}`, ls)
	if !keep {
		t.Fatalf("expected keep=true")
	}
	if out.Get("caller") != "main.go" {
		t.Errorf("expected caller=main.go (extracted from JSON), got %q", out.Get("caller"))
	}
	if out.Get("level") != "stored-level" {
		t.Errorf("expected level=stored-level (live wins), got %q", out.Get("level"))
	}
	if out.Get("msg") != "hello" {
		t.Errorf("expected msg=hello, got %q", out.Get("msg"))
	}
}

// LQP-TEST-134: LogfmtStage sets __error__="" even on parse failure (non-strict mode)
func TestLogfmtStage_SetsErrorLabelOnFailure(t *testing.T) {
	stage := LogfmtStage()
	ls := NewEmptyLabelSet()
	// A bare "=" with no key causes a logfmt decoder error.
	// Non-strict mode: __error__ is always "" (Loki non-strict behavior).
	_, out, keep := stage(0, "=bad", ls)
	if !keep {
		t.Fatalf("expected keep=true even on logfmt error")
	}
	if !out.Has("__error__") {
		t.Fatalf("expected __error__ label to be set")
	}
	if out.Get("__error__") != "" {
		t.Errorf("expected __error__=\"\" in non-strict mode, got %q", out.Get("__error__"))
	}
}
