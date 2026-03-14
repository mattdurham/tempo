package logqlparser

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// LabelSet is a mutable label bag. Implementations may be backed by block columns (lazy)
// or a plain map. SPEC-001: PipelineStageFunc receives and returns a LabelSet.
// Implementations MUST NOT retain references to any input beyond the call scope.
type LabelSet interface {
	// Get returns the value for key, or "" if absent or deleted.
	Get(key string) string
	// Has reports whether key is present (even if the value is "").
	Has(key string) bool
	// Set adds or overwrites key with val.
	Set(key, val string)
	// Delete removes key. No-op if absent.
	Delete(key string)
	// Keys returns all present (non-deleted) keys in unspecified order.
	Keys() []string
	// Materialize copies all present labels into a new map[string]string.
	Materialize() map[string]string
}

// mapLabelSet is a LabelSet backed by a plain map[string]string.
// Used for the non-pre-parsed block fallback path and in tests.
type mapLabelSet struct {
	m map[string]string
}

// NewMapLabelSet wraps an existing map as a LabelSet. The LabelSet takes ownership;
// callers must not modify m after this call. A nil m is treated as empty.
func NewMapLabelSet(m map[string]string) LabelSet {
	if m == nil {
		m = make(map[string]string)
	}
	return &mapLabelSet{m: m}
}

// NewEmptyLabelSet returns an empty map-backed LabelSet.
func NewEmptyLabelSet() LabelSet {
	return &mapLabelSet{m: make(map[string]string)}
}

func (s *mapLabelSet) Get(key string) string { return s.m[key] }
func (s *mapLabelSet) Has(key string) bool   { _, ok := s.m[key]; return ok }
func (s *mapLabelSet) Set(key, val string)   { s.m[key] = val }
func (s *mapLabelSet) Delete(key string)     { delete(s.m, key) }

func (s *mapLabelSet) Keys() []string {
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	return keys
}

func (s *mapLabelSet) Materialize() map[string]string {
	out := make(map[string]string, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}
