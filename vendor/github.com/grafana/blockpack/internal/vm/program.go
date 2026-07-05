package vm

// Program is a blockpack data type.
type Program struct {
	ColumnPredicate          ColumnPredicate
	StreamingColumnPredicate StreamingColumnPredicate
	Predicates               *QueryPredicates
	// WantColumns is the pre-computed set of column names needed to evaluate this program.
	// Populated at compile time. nil means WantColumns was not pre-computed; ProgramWantColumns
	// will walk the predicate tree on each call.
	// Read-only after compilation — callers of ProgramWantColumns MUST NOT mutate the returned map.
	WantColumns   map[string]struct{}
	OriginalQuery string
}

// ComputeWantColumns builds the want-column set from p.Predicates.
// Called once at compile time; result stored in p.WantColumns.
// Does not include caller-supplied extras (trace:id, span:id etc.) — those are merged
// at call time by ProgramWantColumns when extra args are present.
func (p *Program) ComputeWantColumns() {
	if p.Predicates == nil {
		return
	}
	preds := p.Predicates
	if len(preds.Nodes) == 0 && len(preds.Columns) == 0 {
		return
	}
	cols := make(map[string]struct{})
	collectRangeNodeColumns(preds.Nodes, cols)
	for _, c := range preds.Columns {
		cols[c] = struct{}{}
	}
	if len(cols) > 0 {
		p.WantColumns = cols
	}
}

// collectRangeNodeColumns recursively walks a RangeNode slice and collects leaf Column values.
// Intentionally duplicated from executor.collectNodeColumns — an import cycle (vm ← executor)
// prevents sharing. Keep both in sync.
func collectRangeNodeColumns(nodes []RangeNode, cols map[string]struct{}) {
	for _, n := range nodes {
		if len(n.Children) > 0 {
			collectRangeNodeColumns(n.Children, cols)
		} else if n.Column != "" {
			cols[n.Column] = struct{}{}
		}
	}
}
