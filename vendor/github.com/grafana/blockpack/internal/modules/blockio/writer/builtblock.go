package writer

type builtBlock struct {
	colMinMax map[string]*blockColMinMax
	// NOTE: colStats field removed (2026-06-29, in-file block pruning removal).
	// columnNames is the set of column names actually written to this block (issue #531)
	// — a fresh copy of bb.columns' keys, NOT an alias, extracted before bb is returned to
	// the pool (bb.reset deletes entries from b.columns in place, which would corrupt an
	// aliased slice/map the same way it would corrupt an aliased colMinMax map, per that
	// field's own doc comment above). mergeBuiltBlock feeds this into
	// shared.BuildColumnBloom.
	columnNames []string
	payload     []byte
	spanCount   int
	minStart    uint64
	maxStart    uint64
	minTraceID  [16]byte
	maxTraceID  [16]byte
}
