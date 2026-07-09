package queryplan

import (
	shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/vm"
)

// VCNTCostFunc is the FIRST (and initially only) concrete CostFunc, plugging #484's
// VCNT selectivity oracle into this package's generic AND/OR combination logic
// (issue #485, NOTE-QP-001). It closes over one decoded VCNT section (data + dir)
// and the query window [minTS, maxTS], and answers per-leaf costs by summing the net
// live span count for exactly that leaf's `column = value` pair.
//
// It returns a Known cost ONLY for single-value equality leaves the oracle can
// canonically encode — the same scope as #484's SelectivityInRange. Every other leaf
// shape (range / regex / multi-value OR / present-only / a column the VCNT section
// has no record for) returns UnknownCost(), which sorts last. Critically, a leaf the
// oracle DOES cover but finds zero live spans for returns KnownCost(0) — maximally
// selective, sorting first — distinct from the Unknown no-coverage case.
//
// The returned CostFunc performs no object-storage I/O: data/dir are already-decoded
// in-memory bytes handed in by the caller.
func VCNTCostFunc(data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS uint64) CostFunc {
	return func(leaf *vm.RangeNode) LeafCost {
		colType, value, ok := leafEqualityValue(leaf)
		if !ok {
			return UnknownCost()
		}
		canon, err := valueindex.CanonicalValue(colType, value)
		if err != nil {
			return UnknownCost()
		}
		est, err := valuecounts.SelectivityInRange(data, dir, leaf.Column, canon, minTS, maxTS)
		if err != nil || !est.Covered {
			return UnknownCost()
		}
		return KnownCost(est.Count)
	}
}

// VCNTColumnTotalFunc is the concrete ColumnTotalFunc backing selectivity
// classification (issue #486, NOTE-QP-002), the population-side counterpart to
// VCNTCostFunc. It closes over the same decoded VCNT section (data + dir) and query
// window and answers a leaf's column DENOMINATOR — the total live-span count across
// all values of the leaf's column — via valuecounts.ColumnTotalInRange.
//
// It returns ok=false for a leaf with no column (an unresolvable node) or a column
// the VCNT section has no live record for; the classifier then reads that leaf as
// UnknownSelectivity. Like VCNTCostFunc it performs no object-storage I/O.
func VCNTColumnTotalFunc(data []byte, dir []valuecounts.ChunkDirEntry, minTS, maxTS uint64) ColumnTotalFunc {
	return func(leaf *vm.RangeNode) (int64, bool) {
		if leaf == nil || leaf.Column == "" {
			return 0, false
		}
		ct, err := valuecounts.ColumnTotalInRange(data, dir, leaf.Column, minTS, maxTS)
		if err != nil || !ct.Covered {
			return 0, false
		}
		return ct.Total, true
	}
}

// ClassifyProgramVCNT is the single consumer entry point that composes the VCNT cost
// and column-total oracles over ONE decoded VCNT section into a selectivity verdict
// for prog's predicate over [minTS, maxTS] (issue #481 part 2 / #486 consumer wiring,
// NOTE-QP-003).
//
// It is the glue #486 deferred: the recognition primitive shipped as four disjoint
// pieces — Plan, VCNTCostFunc, Classify, VCNTColumnTotalFunc — and every caller that
// wants a verdict must otherwise build both oracles over the SAME (data, dir, minTS,
// maxTS), Plan, then Classify, in that exact order. Doing it by hand invites a subtle
// but severe correctness bug: if the cost func and the column-total func are ever
// built over mismatched windows, the leaf's own count (numerator) and its column's
// population (denominator) no longer describe the same slice of time and the fraction
// is meaningless. Threading a single window through one call makes that mismatch
// unrepresentable.
//
// Contract:
//
//   - Returns UnknownSelectivity when prog has no plannable predicate (Plan ok=false),
//     when no leaf carried a Known VCNT cost, or when the lead leaf's column has no
//     VCNT population coverage — i.e. "no signal". The caller keeps its default
//     (index-source-over-window) strategy on Unknown.
//   - Returns Selective / LowSelectivity per ClassifyWithThreshold's contract, using
//     the same window for BOTH oracles by construction.
//
// It performs no object-storage I/O: data/dir are the caller's already-decoded VCNT
// section bytes, identical to the inputs VCNTCostFunc/VCNTColumnTotalFunc take.
func ClassifyProgramVCNT(
	prog *vm.Program,
	data []byte,
	dir []valuecounts.ChunkDirEntry,
	minTS, maxTS uint64,
) Selectivity {
	return ClassifyProgramVCNTWithThreshold(prog, data, dir, minTS, maxTS, DefaultLowSelectivityFraction)
}

// ClassifyProgramVCNTWithThreshold is ClassifyProgramVCNT with an explicit
// low-selectivity fraction, for callers with a non-default policy. The fraction is
// clamped by ClassifyWithThreshold (a value <= 0 or > 1 falls back to the default),
// so a caller cannot accidentally classify everything or nothing as low-selectivity.
func ClassifyProgramVCNTWithThreshold(
	prog *vm.Program,
	data []byte,
	dir []valuecounts.ChunkDirEntry,
	minTS, maxTS uint64,
	fraction float64,
) Selectivity {
	cost := VCNTCostFunc(data, dir, minTS, maxTS)
	g, ok := Plan(prog, cost)
	if !ok {
		return UnknownSelectivity
	}
	total := VCNTColumnTotalFunc(data, dir, minTS, maxTS)
	return ClassifyWithThreshold(g, total, fraction)
}

// LeadDetail (SPEC-QP-7, NOTE-QP-011) carries the "both sides' costs" a selectivity
// classification computes internally and then discards: the lead leaf's own estimated matching
// count (the index side, the numerator) and its column's total live population over the window
// (the full-scan side, the denominator) — issue #493 Task 4b, R4 (PRE-AUTHORIZED new blockpack
// public API). Exposing this lets an external caller (tempo's frontend) report WHY a query
// qualified or declined, not just the 3-state Selectivity verdict.
//
// HasLead is false when the plan had no lead leaf at all (UnknownSelectivity, no signal) —
// every other field is then zero-value and must not be read. IndexCostKnown/ColumnTotalKnown are
// independently false whenever their respective oracle had no coverage for the lead leaf,
// mirroring LeafCost's own Known/Unknown split (NOTE-QP-001) — check the paired *Known flag
// before trusting IndexCost/ColumnTotal, exactly like LeafCost.Known. IndexCostKnown is always
// equal to HasLead in practice (leadLeaf, selectivity.go, only ever returns a leaf whose own cost
// is Known) — it is still a distinct field, for the same defensive symmetry LeafCost itself uses,
// rather than callers inferring it from HasLead.
type LeadDetail struct {
	// LeadColumn is the lead leaf's column name. Empty when HasLead is false.
	LeadColumn string
	// IndexCost is the lead leaf's estimated matching-span count (the VI/VCNT index side).
	// Meaningful only when IndexCostKnown.
	IndexCost int64
	// ColumnTotal is the lead leaf's column's total live-span population over the window (the
	// full-scan side). Meaningful only when ColumnTotalKnown.
	ColumnTotal      int64
	HasLead          bool
	IndexCostKnown   bool
	ColumnTotalKnown bool
}

// ClassifyProgramVCNTWithDetail is ClassifyProgramVCNT plus the LeadDetail its classification
// already computes internally — same composition (cost oracle -> Plan -> column-total oracle ->
// classify), same default threshold, zero additional object-storage I/O. See LeadDetail's own
// doc comment for the field contract.
func ClassifyProgramVCNTWithDetail(
	prog *vm.Program,
	data []byte,
	dir []valuecounts.ChunkDirEntry,
	minTS, maxTS uint64,
) (Selectivity, LeadDetail) {
	cost := VCNTCostFunc(data, dir, minTS, maxTS)
	g, ok := Plan(prog, cost)
	if !ok {
		return UnknownSelectivity, LeadDetail{}
	}
	total := VCNTColumnTotalFunc(data, dir, minTS, maxTS)
	sel, d := classifyDetailed(g, total, DefaultLowSelectivityFraction)
	return sel, LeadDetail{
		LeadColumn:       d.leadColumn,
		IndexCost:        d.leafCount,
		ColumnTotal:      d.colTotal,
		HasLead:          d.hasLead,
		IndexCostKnown:   d.hasLead,
		ColumnTotalKnown: d.colTotalKnown,
	}
}

// leafEqualityValue extracts a single-value equality (`column = value`) from a leaf
// as a (columnType, concrete value) pair the VCNT oracle can canonically encode.
// Returns ok=false for any non-equality or multi-value leaf — those carry no
// single-value VCNT lookup and must read as UnknownCost. It mirrors the equality arm
// of vibuilder.buildPredicate/valueAsColType so the two agree on which leaves are
// point lookups.
func leafEqualityValue(n *vm.RangeNode) (shared.ColumnType, any, bool) {
	if n == nil || n.Column == "" {
		return 0, nil, false
	}
	// Only a single-value equality is a VCNT point lookup. Multi-value (OR'd values),
	// range (Min/Max), regex (Pattern), and present-only leaves are not.
	if len(n.Values) != 1 || n.Min != nil || n.Max != nil || n.Pattern != "" || n.RequirePresent {
		return 0, nil, false
	}
	return valueAsColType(n.Values[0])
}

// valueAsColType maps a vm.Value to the value-index column type and the concrete Go
// value CanonicalValue expects. It is the same mapping vibuilder uses; duplicated
// here (rather than exported and shared) to keep this cost-estimation package's
// dependency surface minimal — it needs only the equality types the oracle covers.
func valueAsColType(v vm.Value) (shared.ColumnType, any, bool) {
	switch v.Type {
	case vm.TypeString:
		s, ok := v.Data.(string)
		if !ok {
			return 0, nil, false
		}
		return shared.ColumnTypeString, s, true
	case vm.TypeInt, vm.TypeDuration:
		switch d := v.Data.(type) {
		case int64:
			return shared.ColumnTypeInt64, d, true
		case int:
			return shared.ColumnTypeInt64, int64(d), true
		default:
			return 0, nil, false
		}
	case vm.TypeFloat:
		f, ok := v.Data.(float64)
		if !ok {
			return 0, nil, false
		}
		return shared.ColumnTypeFloat64, f, true
	default:
		return 0, nil, false
	}
}
