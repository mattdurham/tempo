package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// traceIntrinsicColumns is the set of column names served by the intrinsic section
// for trace files. Includes "practically intrinsic" columns (resource.service.name).
var traceIntrinsicColumns = map[string]struct{}{
	colNameTraceID:       {},
	colNameSpanID:        {},
	colNameParentID:      {},
	colNameSpanName:      {},
	colNameSpanKind:      {},
	colNameSpanStart:     {},
	colNameSpanEnd:       {},
	colNameSpanDuration:  {},
	colNameSpanStatus:    {},
	colNameStatusMessage: {},
	colNameServiceName:   {},
}

// traceIntrinsicStringColumns is the subset of traceIntrinsicColumns that contain
// string values. Only these columns support regex predicates via nilIntrinsicScan.
var traceIntrinsicStringColumns = map[string]struct{}{
	colNameSpanName:      {},
	colNameStatusMessage: {},
	colNameServiceName:   {},
}

// BuildPredicates converts a compiled vm.Program into queryplanner.Predicate values
// for bloom-filter and range-index block pruning.
//
// Each top-level RangeNode in program.Predicates.Nodes is translated to a
// queryplanner.Predicate via translateNode; the planner AND-combines the result.
//
// Tree structure is preserved:
//   - OR composites (IsOR:true) → queryplanner.Predicate{Op:LogicalOR, Children:...}
//   - AND composites (IsOR:false) → queryplanner.Predicate{Op:LogicalAND, Children:...}
//   - Leaf with Values → bloom + range-index point-lookup predicate
//   - Leaf with Min/Max → bloom + range-index interval predicate
//   - Leaf with Pattern → bloom + regex-prefix range predicate (see translateRegexNode)
//   - Leaf without range constraint → bloom-only predicate
//
// NOTE-030: replaces the old flat-map approach (DedicatedColumns/DedicatedRanges/
// UnscopedColumnNames/HasOROperations). See executor/NOTES.md §NOTE-030.
func BuildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
	if program == nil || program.Predicates == nil {
		return nil
	}
	preds := program.Predicates
	if len(preds.Nodes) == 0 && len(preds.Columns) == 0 {
		return nil
	}

	result := make([]queryplanner.Predicate, 0, len(preds.Nodes))
	for _, node := range preds.Nodes {
		p := translateNode(node)
		result = append(result, p)
	}

	return result
}

// translateNode converts a single RangeNode into a queryplanner.Predicate.
//
// NOTE(#439): Range-index value pruning was removed — the planner no longer prunes
// blocks by predicate value, so a leaf Predicate only records the column name(s) it
// references (used for explain output). Value/Min/Max/Pattern are not carried.
func translateNode(node vm.RangeNode) queryplanner.Predicate {
	// Composite node: recursively translate children, combine with AND or OR.
	if len(node.Children) > 0 {
		children := make([]queryplanner.Predicate, 0, len(node.Children))
		for _, child := range node.Children {
			children = append(children, translateNode(child))
		}
		op := queryplanner.LogicalAND
		if node.IsOR {
			op = queryplanner.LogicalOR
		}
		return queryplanner.Predicate{Op: op, Children: children}
	}

	// Leaf node: record the column it references.
	return queryplanner.Predicate{Columns: []string{node.Column}}
}

// searchMetaColumns returns the minimal set of blockpack column names needed to
// construct a Tempo search result from a matched span.
//
// searchMetaCols is the fixed set of columns needed to construct a Tempo search result
// from a matched span. Read-only — never mutate this map.
//
// NOTE-028: Mirrors Tempo's SearchMetaConditions() (pkg/traceql/storage.go), translated
// to blockpack column names. Tempo pre-computes RootSpanName, RootServiceName,
// TraceDuration, and TraceStartTime as trace-level parquet columns; blockpack stores
// everything per-span so root span detection requires span:parent_id, and root name/
// service are derived from span:name and resource.service.name of the root span.
// span:end is included for duration fallback when no root span is present in the result set.
//
// NOTE-050: Trace signal identity columns (trace:id, span:id, span:start, span:end,
// span:duration, span:name, span:parent_id, resource.service.name) are stored exclusively
// in the intrinsic TOC section (not in block payloads). They are excluded from
// searchMetaCols because they are injected directly into secondPassCols via the
// traceIntrinsicColumns loop in stream.go; identity values are fetched via
// lookupIntrinsicFields. See NOTE-050 in executor/NOTES.md for rationale.
var searchMetaCols = map[string]struct{}{
	// Log signal identity columns — included so wantColumns covers them for log
	// signal blocks. Log blocks use different identity column names than trace blocks
	// (log:trace_id / log:span_id vs trace:id / span:id). NOTE-008.
	// Trace signal equivalents (trace:id, span:id, etc.) are injected via
	// traceIntrinsicColumns in stream.go and are intentionally excluded from this map.
	"log:trace_id":  {},
	"log:span_id":   {},
	"log:timestamp": {},
}

// ProgramWantColumns returns the minimal set of column names needed to evaluate program.
//
// Sources (unioned):
//  1. Leaf Column values from the RangeNode tree (preds.Nodes) — collected recursively.
//  2. preds.Columns — explicit column list for attributes that need decode but not pruning:
//     negations (!=, !~), log:body for line filters, pushdown label-filter columns.
//  3. extra — caller-supplied columns (e.g. identity columns like trace:id, span:id).
//
// Returns nil if program has no predicates, which ParseBlockFromBytes treats as "all columns".
// NOTE-018: used by all executor code paths for two-pass column decode.
// NOTE-030: preds.Columns replaces the old AttributesAccessed / UnscopedColumnNames fields.
func ProgramWantColumns(program *vm.Program, extra ...string) map[string]struct{} {
	if program == nil || program.Predicates == nil {
		return nil
	}
	// NOTE-103: fast path — return pre-computed set when available and no extra columns needed.
	// WantColumns is populated at compile time by ComputeWantColumns (program.go).
	// When extra is non-empty (e.g. identity columns like trace:id, span:id added by callers),
	// copy the cached set and merge extra into the copy — O(cached-set-size + extra) rather
	// than O(predicate-tree). Never modify program.WantColumns directly (it is read-only after
	// compilation and shared across callers).
	if program.WantColumns != nil && len(extra) == 0 {
		return program.WantColumns
	}
	if program.WantColumns != nil && len(extra) > 0 {
		merged := make(map[string]struct{}, len(program.WantColumns)+len(extra))
		for k := range program.WantColumns {
			merged[k] = struct{}{}
		}
		for _, e := range extra {
			merged[e] = struct{}{}
		}
		return merged
	}
	p := program.Predicates
	if len(p.Nodes) == 0 && len(p.Columns) == 0 && len(extra) == 0 {
		return nil
	}

	cols := make(map[string]struct{})
	// Collect all leaf column names from the Nodes tree.
	collectNodeColumns(p.Nodes, cols)
	// Add explicit Columns (negations, log:body, etc. that have no pruning node).
	for _, c := range p.Columns {
		cols[c] = struct{}{}
	}
	for _, c := range extra {
		cols[c] = struct{}{}
	}
	// Ensure the correct embedding column is always included when the program has a VECTOR() predicate.
	// Use program.VectorColumn (set at compile time) to handle both VECTOR_AI (__embedding__)
	// and VECTOR_ALL (__embedding_all__) correctly. Fall back to EmbeddingColumnName for
	// programs constructed without VectorColumn (e.g. legacy tests).
	if program.HasVector {
		embCol := program.VectorColumn
		if embCol == "" {
			embCol = modules_shared.EmbeddingColumnName
		}
		cols[embCol] = struct{}{}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// ComputeSecondPassCols returns the column set needed for result materialization.
// It is the correct wantCols value for NewSpanFieldsAdapterWithReader at stream call sites.
// SPEC-ROOT-017: secondPassCols is the correct filter for second-pass block decodes.
// NOTE-098: always includes traceIntrinsicColumns so span:end synthesis works.
func ComputeSecondPassCols(program *vm.Program, selectColumns []string) map[string]struct{} {
	_, secondPassCols := computeColumnFilters(program, CollectOptions{SelectColumns: selectColumns})
	return secondPassCols
}

// collectNodeColumns recursively walks a RangeNode slice and adds all leaf Column
// values to the cols set.
func collectNodeColumns(nodes []vm.RangeNode, cols map[string]struct{}) {
	for _, n := range nodes {
		if len(n.Children) > 0 {
			collectNodeColumns(n.Children, cols)
		} else if n.Column != "" {
			cols[n.Column] = struct{}{}
		}
	}
}
