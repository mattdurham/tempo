package logqlparser

import (
	"sort"
	"testing"
)

// LQL-TEST-LS-001: NewEmptyLabelSet returns empty set
func TestNewEmptyLabelSet_Empty(t *testing.T) {
	ls := NewEmptyLabelSet()
	if ls.Get("k") != "" {
		t.Fatalf("expected empty string for missing key")
	}
	if len(ls.Keys()) != 0 {
		t.Fatalf("expected no keys in empty set, got %v", ls.Keys())
	}
	m := ls.Materialize()
	if len(m) != 0 {
		t.Fatalf("expected empty map from Materialize, got %v", m)
	}
}

// LQL-TEST-LS-002: NewMapLabelSet wraps an existing map
func TestNewMapLabelSet_GetSet(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"level": "info", "app": "svc"})
	if ls.Get("level") != "info" {
		t.Fatalf("expected level=info, got %q", ls.Get("level"))
	}
	ls.Set("level", "warn")
	if ls.Get("level") != "warn" {
		t.Fatalf("expected level=warn after Set, got %q", ls.Get("level"))
	}
}

// LQL-TEST-LS-003: Delete removes a key from Keys() and Get() returns ""
func TestMapLabelSet_Delete(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"level": "info", "app": "svc"})
	ls.Delete("level")
	if ls.Get("level") != "" {
		t.Fatalf("expected empty string after Delete, got %q", ls.Get("level"))
	}
	for _, k := range ls.Keys() {
		if k == "level" {
			t.Fatalf("deleted key 'level' still present in Keys()")
		}
	}
}

// LQL-TEST-LS-004: Keys returns all present (non-deleted) keys
func TestMapLabelSet_Keys(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"a": "1", "b": "2", "c": "3"})
	ls.Delete("b")
	keys := ls.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Fatalf("expected [a, c], got %v", keys)
	}
}

// LQL-TEST-LS-005: Materialize returns map with all present keys
func TestMapLabelSet_Materialize(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"level": "error", "app": "svc"})
	ls.Set("region", "us-east")
	ls.Delete("app")
	m := ls.Materialize()
	if m["level"] != "error" {
		t.Fatalf("expected level=error, got %q", m["level"])
	}
	if m["region"] != "us-east" {
		t.Fatalf("expected region=us-east, got %q", m["region"])
	}
	if _, ok := m["app"]; ok {
		t.Fatalf("deleted key 'app' should not appear in Materialize")
	}
}

// LQL-TEST-LS-006: Set on empty label set works
func TestEmptyLabelSet_SetAndGet(t *testing.T) {
	ls := NewEmptyLabelSet()
	ls.Set("level", "debug")
	if ls.Get("level") != "debug" {
		t.Fatalf("expected level=debug, got %q", ls.Get("level"))
	}
}

// LQL-TEST-LS-007: PipelineStageFunc accepts and returns LabelSet (compile-time check)
// This test compiles only after PipelineStageFunc uses LabelSet.
func TestPipelineStageFunc_AcceptsLabelSet(t *testing.T) {
	var _ PipelineStageFunc = func(_ uint64, line string, ls LabelSet) (string, LabelSet, bool) {
		return line, ls, true
	}
}

// LQL-TEST-LS-008: Has distinguishes present-but-empty from absent
func TestMapLabelSet_Has(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"present": "value", "empty": ""})

	if !ls.Has("present") {
		t.Fatal("Has should return true for key with non-empty value")
	}
	if !ls.Has("empty") {
		t.Fatal("Has should return true for key with empty value")
	}
	if ls.Has("missing") {
		t.Fatal("Has should return false for absent key")
	}

	ls.Delete("present")
	if ls.Has("present") {
		t.Fatal("Has should return false after Delete")
	}

	ls.Set("new", "")
	if !ls.Has("new") {
		t.Fatal("Has should return true for key set to empty string via Set")
	}
}

// LQL-TEST-LS-009: LogfmtStage skips keys present with empty value (stored-column wins)
func TestLogfmtStage_SkipsEmptyStoredKey(t *testing.T) {
	// A stored column may have an explicitly-empty value. The parser must not overwrite it.
	ls := NewMapLabelSet(map[string]string{"level": ""})
	stage := LogfmtStage()
	_, outLS, keep := stage(0, "level=info msg=timeout", ls)
	if !keep {
		t.Fatal("LogfmtStage should not drop the row")
	}
	if outLS.Get("level") != "" {
		t.Fatalf("LogfmtStage should not overwrite pre-populated key, got %q", outLS.Get("level"))
	}
	if outLS.Get("msg") != "timeout" {
		t.Fatalf("LogfmtStage should extract new key msg, got %q", outLS.Get("msg"))
	}
}

// LQL-TEST-LS-010: JSONStage skips keys present with empty value (stored-column wins)
func TestJSONStage_SkipsEmptyStoredKey(t *testing.T) {
	ls := NewMapLabelSet(map[string]string{"level": ""})
	stage := JSONStage()
	_, outLS, keep := stage(0, `{"level":"info","msg":"hello"}`, ls)
	if !keep {
		t.Fatal("JSONStage should not drop the row")
	}
	if outLS.Get("level") != "" {
		t.Fatalf("JSONStage should not overwrite pre-populated key, got %q", outLS.Get("level"))
	}
	if outLS.Get("msg") != "hello" {
		t.Fatalf("JSONStage should extract new key msg, got %q", outLS.Get("msg"))
	}
}
