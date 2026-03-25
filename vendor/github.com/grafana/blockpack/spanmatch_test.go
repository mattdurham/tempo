package blockpack_test

import (
	"testing"

	blockpack "github.com/grafana/blockpack"
)

// staticFields is a simple SpanFieldsProvider backed by a fixed map.
type staticFields map[string]any

func (f staticFields) GetField(name string) (any, bool) {
	v, ok := f[name]
	return v, ok
}

func (f staticFields) IterateFields(fn func(string, any) bool) {
	for k, v := range f {
		if !fn(k, v) {
			return
		}
	}
}

func makeSpan(fields map[string]any) blockpack.SpanMatch {
	return blockpack.SpanMatch{Fields: staticFields(fields)}
}

// =============================================================================
// SpanMatch typed accessors
// =============================================================================

func TestSpanMatch_NilFields(t *testing.T) {
	m := blockpack.SpanMatch{} // Fields is nil
	if _, ok := m.StartNano(); ok {
		t.Error("StartNano: expected false for nil Fields")
	}
	if _, ok := m.EndNano(); ok {
		t.Error("EndNano: expected false for nil Fields")
	}
	if _, ok := m.DurationNano(); ok {
		t.Error("DurationNano: expected false for nil Fields")
	}
	if _, ok := m.Name(); ok {
		t.Error("Name: expected false for nil Fields")
	}
	if _, ok := m.StatusCode(); ok {
		t.Error("StatusCode: expected false for nil Fields")
	}
	if _, ok := m.KindCode(); ok {
		t.Error("KindCode: expected false for nil Fields")
	}
	if _, ok := m.ServiceName(); ok {
		t.Error("ServiceName: expected false for nil Fields")
	}
	if m.IsRoot() {
		t.Error("IsRoot: expected false for nil Fields")
	}
	if _, ok := m.ResourceAttr("k8s.pod.name"); ok {
		t.Error("ResourceAttr: expected false for nil Fields")
	}
	if _, ok := m.SpanAttr("http.method"); ok {
		t.Error("SpanAttr: expected false for nil Fields")
	}
}

func TestSpanMatch_TypedAccessors(t *testing.T) {
	m := makeSpan(map[string]any{
		"span:start":            uint64(1000),
		"span:end":              uint64(2000),
		"span:duration":         uint64(1000),
		"span:name":             "GET /api",
		"span:status":           int64(2),
		"span:kind":             int64(3),
		"resource.service.name": "my-service",
		"span.http.method":      "GET",
		"resource.k8s.pod.name": "pod-1",
	})

	if v, ok := m.StartNano(); !ok || v != 1000 {
		t.Errorf("StartNano: got (%d, %v)", v, ok)
	}
	if v, ok := m.EndNano(); !ok || v != 2000 {
		t.Errorf("EndNano: got (%d, %v)", v, ok)
	}
	if v, ok := m.DurationNano(); !ok || v != 1000 {
		t.Errorf("DurationNano: got (%d, %v)", v, ok)
	}
	if v, ok := m.Name(); !ok || v != "GET /api" {
		t.Errorf("Name: got (%q, %v)", v, ok)
	}
	if v, ok := m.StatusCode(); !ok || v != 2 {
		t.Errorf("StatusCode: got (%d, %v)", v, ok)
	}
	if v, ok := m.KindCode(); !ok || v != 3 {
		t.Errorf("KindCode: got (%d, %v)", v, ok)
	}
	if v, ok := m.ServiceName(); !ok || v != "my-service" {
		t.Errorf("ServiceName: got (%q, %v)", v, ok)
	}
	if v, ok := m.SpanAttr("http.method"); !ok || v != "GET" {
		t.Errorf("SpanAttr: got (%v, %v)", v, ok)
	}
	if v, ok := m.ResourceAttr("k8s.pod.name"); !ok || v != "pod-1" {
		t.Errorf("ResourceAttr: got (%v, %v)", v, ok)
	}
}

func TestSpanMatch_IsRoot(t *testing.T) {
	root := makeSpan(map[string]any{
		"span:name": "root",
		// no span:parent_id
	})
	child := makeSpan(map[string]any{
		"span:name":      "child",
		"span:parent_id": "deadbeef",
	})

	if !root.IsRoot() {
		t.Error("IsRoot: expected true for span without parent_id")
	}
	if child.IsRoot() {
		t.Error("IsRoot: expected false for span with parent_id")
	}
}

// =============================================================================
// SpanMatchesMetadata
// =============================================================================

func TestSpanMatchesMetadata_Empty(t *testing.T) {
	name, svc, start, dur := blockpack.SpanMatchesMetadata(nil)
	if name != "" || svc != "" || start != 0 || dur != 0 {
		t.Errorf("expected zero values for empty slice, got (%q,%q,%d,%d)", name, svc, start, dur)
	}
}

func TestSpanMatchesMetadata_SingleRootSpan(t *testing.T) {
	spans := []blockpack.SpanMatch{
		makeSpan(map[string]any{
			"span:start":            uint64(1_000_000),
			"span:end":              uint64(5_000_000),
			"span:duration":         uint64(4_000_000),
			"span:name":             "root-op",
			"resource.service.name": "svc-a",
			// no span:parent_id → root
		}),
	}

	name, svc, start, dur := blockpack.SpanMatchesMetadata(spans)
	if name != "root-op" {
		t.Errorf("rootName: got %q, want root-op", name)
	}
	if svc != "svc-a" {
		t.Errorf("rootService: got %q, want svc-a", svc)
	}
	if start != 1_000_000 {
		t.Errorf("startNano: got %d, want 1000000", start)
	}
	if dur != 4_000_000 {
		t.Errorf("durationNano: got %d, want 4000000", dur)
	}
}

func TestSpanMatchesMetadata_NoRootInSet(t *testing.T) {
	// Both spans are children (have parent_id). Metadata should fall back to
	// the span with minimum start for name/service, and use span-range for duration.
	spans := []blockpack.SpanMatch{
		makeSpan(map[string]any{
			"span:start":            uint64(200),
			"span:end":              uint64(400),
			"span:name":             "child-b",
			"resource.service.name": "svc-b",
			"span:parent_id":        "parent1",
		}),
		makeSpan(map[string]any{
			"span:start":            uint64(100),
			"span:end":              uint64(300),
			"span:name":             "child-a",
			"resource.service.name": "svc-a",
			"span:parent_id":        "parent1",
		}),
	}

	name, svc, start, dur := blockpack.SpanMatchesMetadata(spans)
	// Proxy is the span with minimum start (child-a at t=100)
	if name != "child-a" {
		t.Errorf("rootName fallback: got %q, want child-a", name)
	}
	if svc != "svc-a" {
		t.Errorf("rootService fallback: got %q, want svc-a", svc)
	}
	if start != 100 {
		t.Errorf("startNano: got %d, want 100", start)
	}
	// Duration should be maxEnd(400) - minStart(100) = 300
	if dur != 300 {
		t.Errorf("durationNano span-range fallback: got %d, want 300", dur)
	}
}

func TestSpanMatchesMetadata_IntrinsicFastPath_NoSpanEnd(t *testing.T) {
	// Simulate intrinsic-fast-path results: span:end is absent; only span:start
	// and span:duration are present. No root span in set (all have parent_id).
	// SpanMatchesMetadata must synthesize maxEnd from start+duration.
	spans := []blockpack.SpanMatch{
		makeSpan(map[string]any{
			"span:start":            uint64(1000),
			"span:duration":         uint64(500),
			"span:name":             "child-a",
			"resource.service.name": "svc-x",
			"span:parent_id":        "abc",
			// no span:end
		}),
		makeSpan(map[string]any{
			"span:start":            uint64(800),
			"span:duration":         uint64(300),
			"span:name":             "child-b",
			"resource.service.name": "svc-x",
			"span:parent_id":        "abc",
			// no span:end
		}),
	}

	_, _, start, dur := blockpack.SpanMatchesMetadata(spans)
	if start != 800 {
		t.Errorf("startNano: got %d, want 800", start)
	}
	// maxEnd = max(1000+500, 800+300) = max(1500, 1100) = 1500
	// dur = 1500 - 800 = 700
	if dur != 700 {
		t.Errorf("durationNano (start+duration fallback): got %d, want 700", dur)
	}
}

func TestSpanMatchesMetadata_NilFields(t *testing.T) {
	// Structural query results have nil Fields — should not panic.
	spans := []blockpack.SpanMatch{
		{TraceID: "abc", SpanID: "def"}, // Fields is nil
	}
	name, svc, start, dur := blockpack.SpanMatchesMetadata(spans)
	if name != "" || svc != "" || start != 0 || dur != 0 {
		t.Errorf("nil Fields: expected zero values, got (%q,%q,%d,%d)", name, svc, start, dur)
	}
}

// =============================================================================
// SpanMatchesServiceStats
// =============================================================================

func TestSpanMatchesServiceStats_Empty(t *testing.T) {
	if blockpack.SpanMatchesServiceStats(nil) != nil {
		t.Error("expected nil for empty input")
	}
}

func TestSpanMatchesServiceStats_Counts(t *testing.T) {
	spans := []blockpack.SpanMatch{
		makeSpan(map[string]any{"resource.service.name": "svc-a", "span:status": int64(0)}),
		makeSpan(map[string]any{"resource.service.name": "svc-a", "span:status": int64(2)}), // error
		makeSpan(map[string]any{"resource.service.name": "svc-b", "span:status": int64(1)}),
	}

	stats := blockpack.SpanMatchesServiceStats(spans)
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	a := stats["svc-a"]
	if a.SpanCount != 2 || a.ErrorCount != 1 {
		t.Errorf("svc-a: got SpanCount=%d ErrorCount=%d, want 2/1", a.SpanCount, a.ErrorCount)
	}
	b := stats["svc-b"]
	if b.SpanCount != 1 || b.ErrorCount != 0 {
		t.Errorf("svc-b: got SpanCount=%d ErrorCount=%d, want 1/0", b.SpanCount, b.ErrorCount)
	}
}

// =============================================================================
// Column naming helpers
// =============================================================================

func TestAttributeColumnName(t *testing.T) {
	tests := []struct{ scope, name, want string }{
		{"span", "http.method", "span.http.method"},
		{"resource", "service.name", "resource.service.name"},
	}
	for _, tt := range tests {
		if got := blockpack.AttributeColumnName(tt.scope, tt.name); got != tt.want {
			t.Errorf("AttributeColumnName(%q,%q) = %q, want %q", tt.scope, tt.name, got, tt.want)
		}
	}
}

func TestIntrinsicColumnName(t *testing.T) {
	if got := blockpack.IntrinsicColumnName("duration"); got != "span:duration" {
		t.Errorf("got %q, want span:duration", got)
	}
	if blockpack.TraceIDColumnName != "trace:id" {
		t.Errorf("TraceIDColumnName = %q, want trace:id", blockpack.TraceIDColumnName)
	}
}

func TestColumnScope(t *testing.T) {
	tests := []struct {
		col                 string
		wantScope, wantAttr string
		wantIntrinsic       bool
	}{
		{"span.http.method", "span", "http.method", false},
		{"resource.service.name", "resource", "service.name", false},
		{"span:duration", "span", "duration", true},
		{"span:name", "span", "name", true},
		{"trace:id", "", "trace:id", false}, // not matched by any prefix
		{"unknown", "", "unknown", false},
	}
	for _, tt := range tests {
		scope, attr, isIntrinsic := blockpack.ColumnScope(tt.col)
		if scope != tt.wantScope || attr != tt.wantAttr || isIntrinsic != tt.wantIntrinsic {
			t.Errorf("ColumnScope(%q) = (%q,%q,%v), want (%q,%q,%v)",
				tt.col, scope, attr, isIntrinsic, tt.wantScope, tt.wantAttr, tt.wantIntrinsic)
		}
	}
}
