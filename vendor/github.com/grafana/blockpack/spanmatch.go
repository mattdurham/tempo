package blockpack

// spanmatch.go — typed accessors on SpanMatch, trace-level metadata helpers,
// and column naming utilities. These are the primary integration surface for
// callers that want to extract span fields without knowing column name strings
// or performing type assertions on GetField results.

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
