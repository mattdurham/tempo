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
	// HasLive reports whether key has a live (immediately accessible) value.
	// A value is live if it is in the overlay (set by a previous stage) or in a
	// decoded block column. Undecoded block columns return false — the value is in
	// storage but not yet accessible to the pipeline.
	// For mapLabelSet, HasLive == Has (all values are immediately accessible).
	// SPEC-001a: Used by JSONStage and LogfmtStage to distinguish "column present in
	// block storage" (Has) from "column has a usable value right now" (HasLive).
	HasLive(key string) bool
	// HideBodyParsedColumns marks all ingest-time body-parsed (log.*) columns as
	// deleted so that Get() returns "" for them unless a pipeline stage explicitly
	// sets the value via Set(). Called by LogfmtStage and JSONStage before parsing
	// the body so that ingest-time last-wins column values cannot shadow re-parsed
	// body values. Set() un-deletes a key when the stage writes to it, so the
	// parsed result takes precedence. For mapLabelSet this is a no-op.
	HideBodyParsedColumns()
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

func (s *mapLabelSet) Get(key string) string   { return s.m[key] }
func (s *mapLabelSet) Has(key string) bool     { _, ok := s.m[key]; return ok }
func (s *mapLabelSet) HasLive(key string) bool { _, ok := s.m[key]; return ok }
func (s *mapLabelSet) Set(key, val string)     { s.m[key] = val }
func (s *mapLabelSet) Delete(key string)       { delete(s.m, key) }
func (s *mapLabelSet) HideBodyParsedColumns()  {} // no-op: map has no ingest-time columns

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
