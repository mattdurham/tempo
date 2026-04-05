package blockpack

// spanmatch.go — SpanMatch type, Clone, materialized/filtered field providers,
// typed accessors, trace-level metadata helpers, and column naming utilities.

import "sync"

// SpanMatch represents a single span that matched the query.
// Fields is safe to retain after QueryTraceQL or QueryLogQL returns.
type SpanMatch struct {
	Fields  SpanFieldsProvider
	TraceID string
	SpanID  string
	Score   float32 // Vector similarity score (0 = no vector query, >0 = cosine similarity)
}

// kvField is a name-value pair used in materializedSpanFields.
// NOTE-ALLOC-2: slice-backed materialization avoids map allocation in Clone.
type kvField struct {
	Value any
	Name  string
}

// kvFieldSlicePool recycles []kvField backing arrays to reduce Clone allocations.
// NOTE-ALLOC-2: pool holds *[]kvField so Reset clears slice length without losing capacity.
var kvFieldSlicePool = sync.Pool{
	New: func() any {
		s := make([]kvField, 0, 32)
		return &s
	},
}

// Clone materializes all fields from the lazy provider and returns a deep copy
// with stable ownership, safe to hold beyond any internal callback or function return.
// If Fields is nil (e.g. structural queries), the clone has nil Fields.
// NOTE-ALLOC-2: uses pooled []kvField backing slice instead of map[string]any.
func (m *SpanMatch) Clone() SpanMatch {
	out := SpanMatch{TraceID: m.TraceID, SpanID: m.SpanID}
	if m.Fields == nil {
		return out
	}
	// Get pooled backing slice, fill it, then transfer ownership to materializedSpanFields.
	sp := kvFieldSlicePool.Get().(*[]kvField)
	fields := (*sp)[:0]
	m.Fields.IterateFields(func(name string, value any) bool {
		fields = append(fields, kvField{Name: name, Value: value})
		return true
	})
	// Copy into an exactly-sized owned slice for materializedSpanFields,
	// then return the original pool slice (reset to len=0) for reuse.
	// This ensures the pool backing array is actually reused across calls.
	owned := make([]kvField, len(fields))
	copy(owned, fields)
	out.Fields = &materializedSpanFields{fields: owned}
	*sp = fields[:0]
	kvFieldSlicePool.Put(sp)
	return out
}

// materializedSpanFields is a heap-allocated slice-backed SpanFieldsProvider
// returned by SpanMatch.Clone(). Safe to hold beyond the callback lifetime.
// NOTE-ALLOC-2: uses []kvField instead of map[string]any to avoid map overhead.
type materializedSpanFields struct {
	fields []kvField
}

// GetField is an O(n) linear scan. For typical span field counts (10–30),
// this is faster than a map lookup due to cache locality.
// See executor/NOTES.md NOTE-047 for the trade-off rationale.
func (m *materializedSpanFields) GetField(name string) (any, bool) {
	for i := range m.fields {
		if m.fields[i].Name == name {
			return m.fields[i].Value, true
		}
	}
	return nil, false
}

func (m *materializedSpanFields) IterateFields(fn func(name string, value any) bool) {
	for i := range m.fields {
		if !fn(m.fields[i].Name, m.fields[i].Value) {
			return
		}
	}
}

// filteredSpanFields wraps a SpanFieldsProvider and limits GetField / IterateFields
// to a caller-specified column allowlist. Used to implement QueryOptions.SelectColumns.
type filteredSpanFields struct {
	inner   SpanFieldsProvider
	allowed map[string]struct{}
}

func newFilteredSpanFields(inner SpanFieldsProvider, cols []string) *filteredSpanFields {
	m := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		m[c] = struct{}{}
	}
	return &filteredSpanFields{inner: inner, allowed: m}
}

func (f *filteredSpanFields) GetField(name string) (any, bool) {
	if _, ok := f.allowed[name]; !ok {
		return nil, false
	}
	return f.inner.GetField(name)
}

func (f *filteredSpanFields) IterateFields(fn func(name string, value any) bool) {
	f.inner.IterateFields(func(name string, value any) bool {
		if _, ok := f.allowed[name]; !ok {
			return true // skip, but keep iterating
		}
		return fn(name, value)
	})
}

// spanMatchFn is the internal callback type used by streaming query helpers.
// match is valid only for the duration of the call; more=false signals end of results.
type spanMatchFn func(match *SpanMatch, more bool) bool

// =============================================================================
// SpanMatch typed field accessors
// =============================================================================

// StartNano returns the span's start timestamp in nanoseconds since Unix epoch,
// and whether the field was present. Returns (0, false) when Fields is nil.
func (m *SpanMatch) StartNano() (uint64, bool) {
	if m.Fields == nil {
		return 0, false
	}
	v, ok := m.Fields.GetField("span:start")
	if !ok {
		return 0, false
	}
	u, ok := v.(uint64)
	return u, ok
}

// EndNano returns the span's end timestamp in nanoseconds since Unix epoch.
func (m *SpanMatch) EndNano() (uint64, bool) {
	if m.Fields == nil {
		return 0, false
	}
	v, ok := m.Fields.GetField("span:end")
	if !ok {
		return 0, false
	}
	u, ok := v.(uint64)
	return u, ok
}

// DurationNano returns the span's duration in nanoseconds.
func (m *SpanMatch) DurationNano() (uint64, bool) {
	if m.Fields == nil {
		return 0, false
	}
	v, ok := m.Fields.GetField("span:duration")
	if !ok {
		return 0, false
	}
	u, ok := v.(uint64)
	return u, ok
}

// Name returns the span's operation name.
func (m *SpanMatch) Name() (string, bool) {
	if m.Fields == nil {
		return "", false
	}
	v, ok := m.Fields.GetField("span:name")
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// StatusCode returns the OTLP status code (0=Unset, 1=Ok, 2=Error).
func (m *SpanMatch) StatusCode() (int64, bool) {
	if m.Fields == nil {
		return 0, false
	}
	v, ok := m.Fields.GetField("span:status")
	if !ok {
		return 0, false
	}
	i, ok := v.(int64)
	return i, ok
}

// KindCode returns the OTLP span kind (0=Unspecified,1=Internal,2=Server,3=Client,4=Producer,5=Consumer).
func (m *SpanMatch) KindCode() (int64, bool) {
	if m.Fields == nil {
		return 0, false
	}
	v, ok := m.Fields.GetField("span:kind")
	if !ok {
		return 0, false
	}
	i, ok := v.(int64)
	return i, ok
}

// ServiceName returns the value of resource.service.name.
func (m *SpanMatch) ServiceName() (string, bool) {
	if m.Fields == nil {
		return "", false
	}
	v, ok := m.Fields.GetField("resource.service.name")
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// IsRoot returns true when the span has no parent (span:parent_id absent).
// The blockpack writer only sets the parent_id column when ParentSpanId is non-empty,
// so column absence is the reliable root signal.
func (m *SpanMatch) IsRoot() bool {
	if m.Fields == nil {
		return false
	}
	_, hasParent := m.Fields.GetField("span:parent_id")
	return !hasParent
}

// ResourceAttr returns the value of a resource-scoped attribute column.
// name should be the bare attribute name, e.g. "k8s.pod.name" (without "resource." prefix).
func (m *SpanMatch) ResourceAttr(name string) (any, bool) {
	if m.Fields == nil {
		return nil, false
	}
	return m.Fields.GetField("resource." + name)
}

// SpanAttr returns the value of a span-scoped attribute column.
// name should be the bare attribute name, e.g. "http.method" (without "span." prefix).
func (m *SpanMatch) SpanAttr(name string) (any, bool) {
	if m.Fields == nil {
		return nil, false
	}
	return m.Fields.GetField("span." + name)
}

// =============================================================================
// Trace-level metadata helpers
// =============================================================================

// SpanMatchesMetadata derives trace-level display fields from a slice of SpanMatch
// values belonging to a single trace. It returns the root span's operation name
// and service name, the earliest span:start across all spans, and the root span's
// duration (falling back to max(span:end)-min(span:start) when no root is present).
//
// Root span detection: a span is the root when span:parent_id is absent.
// If no root is found in the filtered set (e.g. the query only matched child spans),
// the span with the minimum span:start is used as a proxy for name and service.
func SpanMatchesMetadata(spans []SpanMatch) (rootName, rootService string, startNano, durationNano uint64) {
	if len(spans) == 0 {
		return
	}

	const maxUint64 = ^uint64(0)
	var (
		minStart    = maxUint64
		maxEnd      uint64
		rootIdx     = -1
		minStartIdx = 0
	)

	for i := range spans {
		m := &spans[i]
		if m.Fields == nil {
			continue
		}
		if st, ok := m.StartNano(); ok && st < minStart {
			minStart = st
			minStartIdx = i
		}
		// Prefer span:end; fall back to start+duration for intrinsic-fast-path
		// results where the executor omits span:end from the materialized fields.
		if et, ok := m.EndNano(); ok {
			if et > maxEnd {
				maxEnd = et
			}
		} else if st, ok2 := m.StartNano(); ok2 {
			if d, ok3 := m.DurationNano(); ok3 && st+d > maxEnd {
				maxEnd = st + d
			}
		}
		if rootIdx == -1 && m.IsRoot() {
			rootIdx = i
		}
	}

	if minStart == maxUint64 {
		minStart = 0
	}
	startNano = minStart

	proxyIdx := rootIdx
	if proxyIdx == -1 {
		proxyIdx = minStartIdx
	}
	proxy := &spans[proxyIdx]
	rootName, _ = proxy.Name()
	rootService, _ = proxy.ServiceName()

	if rootIdx != -1 {
		root := &spans[rootIdx]
		if d, ok := root.DurationNano(); ok {
			durationNano = d
		}
		if durationNano == 0 {
			var rs, re uint64
			if st, ok := root.StartNano(); ok {
				rs = st
			}
			if et, ok := root.EndNano(); ok {
				re = et
			}
			if re > rs {
				durationNano = re - rs
			}
		}
	} else if maxEnd > minStart {
		durationNano = maxEnd - minStart
	}
	return
}

// SpanMatchesServiceStats builds a per-service span and error count map from a slice
// of SpanMatch values belonging to a single trace. Error detection uses OTLP status
// code 2 (STATUS_CODE_ERROR). The returned map is nil when spans is empty.
func SpanMatchesServiceStats(spans []SpanMatch) map[string]ServiceStats {
	if len(spans) == 0 {
		return nil
	}
	stats := make(map[string]ServiceStats, 4)
	for i := range spans {
		m := &spans[i]
		svc, _ := m.ServiceName()
		s := stats[svc]
		s.SpanCount++
		if code, ok := m.StatusCode(); ok && code == 2 {
			s.ErrorCount++
		}
		stats[svc] = s
	}
	return stats
}

// ServiceStats holds per-service span and error counts for a trace.
type ServiceStats struct {
	SpanCount  uint32
	ErrorCount uint32
}

// =============================================================================
// Column naming helpers
// =============================================================================

// AttributeColumnName returns the blockpack column name for a span or resource
// attribute. scope must be "span" or "resource"; name is the bare attribute key.
// For intrinsic span fields use IntrinsicColumnName instead.
func AttributeColumnName(scope, name string) string {
	return scope + "." + name
}

// IntrinsicColumnName returns the blockpack column name for a span intrinsic field.
// intrinsic should be one of: "duration", "name", "status", "status_message",
// "kind", "id", "parent_id", "start", "end".
func IntrinsicColumnName(intrinsic string) string {
	return "span:" + intrinsic
}

// TraceIDColumnName is the blockpack column name for the trace identifier.
const TraceIDColumnName = "trace:id"

// ColumnScope returns "span", "resource", or "" for blockpack column names.
// It also returns the bare attribute name (without prefix) and whether the
// column is a span intrinsic (span:duration, span:name, etc.).
func ColumnScope(colName string) (scope, attrName string, isIntrinsic bool) {
	if len(colName) > 5 && colName[:5] == "span." {
		return "span", colName[5:], false
	}
	if len(colName) > 9 && colName[:9] == "resource." {
		return "resource", colName[9:], false
	}
	if len(colName) > 5 && colName[:5] == "span:" {
		return "span", colName[5:], true
	}
	return "", colName, false
}
