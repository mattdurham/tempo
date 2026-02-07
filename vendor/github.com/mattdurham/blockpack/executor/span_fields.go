package executor

// SpanFields stores materialized span data with typed intrinsic fields to avoid boxing allocations.
// Intrinsic fields are stored as typed struct fields (no boxing during materialization).
// Dynamic attributes are stored in a map (only for projected attributes from WHERE clause).
type SpanFields struct {
	// Intrinsic fields (typed - no boxing!)
	TraceID         string
	SpanID          string
	ParentSpanID    string
	Name            string
	StartTime       uint64
	EndTime         uint64
	Duration        uint64
	Status          int64
	Kind            int64
	StatusMessage   string
	ResourceService string

	// Bitmask indicating which intrinsic fields are present
	// This avoids needing pointer types which would allocate
	HasFields uint16

	// Dynamic attributes (projected from WHERE clause)
	// This map is much smaller than the old map[string]any approach
	// which stored ALL fields (intrinsics + attributes)
	Attributes map[string]any
}

// Bitmask constants for intrinsic field presence
const (
	HasTraceID uint16 = 1 << iota
	HasSpanID
	HasParentSpanID
	HasName
	HasStartTime
	HasEndTime
	HasDuration
	HasStatus
	HasKind
	HasStatusMessage
	HasResourceService
)

// GetField implements SpanFieldsProvider interface.
// Boxing happens here (on access), not during field materialization.
func (sf *SpanFields) GetField(name string) (any, bool) {
	// Check intrinsic fields first (boxes on access, but only if accessed)
	switch name {
	case "trace:id":
		if sf.HasFields&HasTraceID != 0 {
			return sf.TraceID, true
		}
	case "span:id":
		if sf.HasFields&HasSpanID != 0 {
			return sf.SpanID, true
		}
	case "span:parent_id":
		if sf.HasFields&HasParentSpanID != 0 {
			return sf.ParentSpanID, true
		}
	case "span:name":
		if sf.HasFields&HasName != 0 {
			return sf.Name, true
		}
	case "span:start":
		if sf.HasFields&HasStartTime != 0 {
			return sf.StartTime, true
		}
	case "span:end":
		if sf.HasFields&HasEndTime != 0 {
			return sf.EndTime, true
		}
	case "span:duration":
		if sf.HasFields&HasDuration != 0 {
			return sf.Duration, true
		}
	case "span:status":
		if sf.HasFields&HasStatus != 0 {
			return sf.Status, true
		}
	case "span:kind":
		if sf.HasFields&HasKind != 0 {
			return sf.Kind, true
		}
	case "span:status_message":
		if sf.HasFields&HasStatusMessage != 0 {
			return sf.StatusMessage, true
		}
	case "resource.service.name":
		if sf.HasFields&HasResourceService != 0 {
			return sf.ResourceService, true
		}
	}

	// Check dynamic attributes
	if sf.Attributes != nil {
		v, ok := sf.Attributes[name]
		return v, ok
	}

	return nil, false
}

// IterateFields implements SpanFieldsProvider interface.
// Calls fn for each present field (intrinsics + dynamic attributes).
// Stops iteration if fn returns false.
func (sf *SpanFields) IterateFields(fn func(name string, value any) bool) {
	// Iterate intrinsic fields (boxes on iteration)
	if sf.HasFields&HasTraceID != 0 {
		if !fn("trace:id", sf.TraceID) {
			return
		}
	}
	if sf.HasFields&HasSpanID != 0 {
		if !fn("span:id", sf.SpanID) {
			return
		}
	}
	if sf.HasFields&HasParentSpanID != 0 {
		if !fn("span:parent_id", sf.ParentSpanID) {
			return
		}
	}
	if sf.HasFields&HasName != 0 {
		if !fn("span:name", sf.Name) {
			return
		}
	}
	if sf.HasFields&HasStartTime != 0 {
		if !fn("span:start", sf.StartTime) {
			return
		}
	}
	if sf.HasFields&HasEndTime != 0 {
		if !fn("span:end", sf.EndTime) {
			return
		}
	}
	if sf.HasFields&HasDuration != 0 {
		if !fn("span:duration", sf.Duration) {
			return
		}
	}
	if sf.HasFields&HasStatus != 0 {
		if !fn("span:status", sf.Status) {
			return
		}
	}
	if sf.HasFields&HasKind != 0 {
		// Only output Kind from typed field if it's NOT in the Attributes map
		// (when span:kind is part of the query, it's stored in Attributes and
		// should be returned from there to match Tempo's behavior)
		if _, inAttrs := sf.Attributes["span:kind"]; !inAttrs {
			if !fn("span:kind", sf.Kind) {
				return
			}
		}
	}
	if sf.HasFields&HasStatusMessage != 0 {
		if !fn("span:status_message", sf.StatusMessage) {
			return
		}
	}
	// NOTE: resource.service.name is NOT output in IterateFields because it's a
	// resource attribute, not a span attribute. It's only stored in the struct
	// for determining RootServiceName in TraceSearchMetadata, but shouldn't appear
	// in the Span's Attributes list (Tempo separates resource and span attributes).

	// Iterate dynamic attributes
	for k, v := range sf.Attributes {
		if !fn(k, v) {
			return
		}
	}
}
