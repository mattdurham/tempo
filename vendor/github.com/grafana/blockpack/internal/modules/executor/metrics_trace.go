package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"slices"
	"strings"
	"sync"

	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// TraceMetricsResult is the output of ExecuteTraceMetricsFromVI — the sole surviving
// metrics execution path (issue #481 part 3 deleted ExecuteTraceMetrics's full-block-scan
// engine outright; see ExecuteTraceMetricsFromVI's own doc comment for the decline contract).

// timeBucketIndex returns the right-closed bucket index for ts within (startTime, endTime].
// Precondition: ts > startTime (caller must range-check before calling; all callers
// use binary-search lo/hi to guarantee this invariant — see NOTE-118).
// Intervals: (startTime, startTime+step], (startTime+step, startTime+2*step], …
// A span at exactly startTime+N*step belongs to bucket N-1 (previous bucket).
//
// NOTE-118: (offset-1)/stepNanos is algebraically equivalent to the original two-division
// formula (offset/stepNanos with bkt-- when offset%stepNanos==0) for all offset>0.
// Uses one DIVQ instead of two: ~2x fewer division cycles for hot loops (M1/M3 150M calls).
func timeBucketIndex(ts, startTime, stepNanos int64) int64 {
	return (ts - startTime - 1) / stepNanos
}

// ValueIndexSource provides value-index data for a specific column query.
// Implementations typically wrap a list of pre-downloaded VI file bytes.
type ValueIndexSource interface {
	// LookupResults returns all matching LookupResults for the given column,
	// predicate, and optional time range. The bool is false when no VI data is
	// available for this column (caller must fall back to a block scan).
	LookupResults(colName string, colType modules_shared.ColumnType) ([]VILookupResult, bool)

	// AllResults returns every indexed span across all columns, deduplicated by
	// (TraceID, SpanID). It backs match-all queries ({} | rate()) where there is
	// no leaf condition to look up. The bool is false when the source cannot
	// enumerate all spans (caller must fall back to a block scan).
	AllResults() ([]VILookupResult, bool)
}

// VILookupResult is a matching entry from a value-index lookup.
// Mirrors valueindex.LookupResult but avoids a cross-package import in the executor.
//
// BlockID and RowIdx locate the span within a blockpack data file for the
// zero-scan search path (NOTE-VI-035, issue #459): BlockID is the block index
// within SourceRef and RowIdx is the row within that block, giving O(1) direct
// access without scanning. They are zero for VI files that predate per-row
// addressing; the search path treats a zero RowIdx as "row 0" and relies on
// the caller to gate on coverage. The metrics path (#460) ignores both.
type VILookupResult struct {
	SourceRef string
	TimeSec   uint64
	BlockID   uint32
	// BlockPage is the v2 page-addressed block start (NOTE-VI-045, #429): with the
	// BucketGroup write path a span is identified by (SourceRef, BlockPage, RowIdx)
	// rather than SpanID, so BlockPage participates in the span identity key.
	BlockPage uint32
	// BlockLen is the v2 block length in 4 KB pages (for direct ranged fetch).
	BlockLen uint16
	RowIdx   uint16
	TraceID  [16]byte
	SpanID   [8]byte
}

// ExecuteTraceMetricsFromVI runs a count_over_time() or rate() query using only
// value-index data — zero blockpack file reads required (NOTE-VI-032, issue #440).
//
// Returns (result, true, nil) when the query can be fully answered from VI data.
// Returns (nil, false, err) as a typed sentinel error when VI data is insufficient
// (issue #481 part 3): there is no full-block-scan fallback — a decline is a hard,
// typed error for the caller (ExecuteMetricsTraceQL) to propagate, not a silent
// signal to retry via a scan.
//
// Currently supports: count_over_time() and rate() without group-by clauses.
//
// NOTE-VI-033 (issue #460): the value index carries (TraceID, SpanID, TimeSec) per
// indexed span per column. count_over_time()/rate() without group-by need nothing
// from the block payloads — counting distinct TraceIDs per time bucket is fully
// answerable from index TimeSec alone. This is the zero-block-read metrics path.
func ExecuteTraceMetricsFromVI(
	ctx context.Context,
	source ValueIndexSource,
	prog *vm.Program,
	spec vm.QuerySpec,
) (*TraceMetricsResult, bool, error) {
	// Issue #481 part 3 / go-presubmit #2: both nil-input branches below are unreachable via
	// the sole production caller (ExecuteMetricsTraceQL only invokes this function when
	// opts.ValueIndex != nil, and prog is always a successfully-compiled program by the time
	// it reaches here — see api.go:ExecuteMetricsTraceQL) but are converted to typed
	// sentinels rather than a silent (nil, false, nil) decline so a future caller can't
	// misattribute an unreachable-today guard as ErrMetricsValueIndexDisabled by accident.
	if source == nil {
		// A nil source is, from this function's own signature, indistinguishable from "no
		// ValueIndexSource supplied" — the same condition ExecuteMetricsTraceQL's caller-
		// facing contract maps to ErrMetricsValueIndexDisabled (R8).
		return nil, false, ErrMetricsValueIndexDisabled
	}
	if prog == nil {
		// A nil compiled program means the query shape cannot be evaluated at all — there is
		// nothing to check against vm.MetricsShapeIsVIAnswerable, so this is treated as the
		// same decline category as an unanswerable shape.
		return nil, false, ErrMetricsShapeNotAnswerable
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	// Gate: only count_over_time() and rate() without group-by are supported. Delegates to
	// vm.MetricsShapeIsVIAnswerable so this stays the single source of truth for the shape
	// rule — a plan-time caller (blockpack's public CompileTraceQLMetricsFilter) reuses the
	// exact same function rather than a second, independently-maintained copy.
	// Issue #481 part 3: this decline is now a typed sentinel error (production default),
	// not a silent (nil, false, nil) signal to fall back to a scan — there is no scan anymore.
	if !vm.MetricsShapeIsVIAnswerable(spec) {
		return nil, false, ErrMetricsShapeNotAnswerable
	}

	tb := spec.TimeBucketing
	if !tb.Enabled || tb.StepSizeNanos <= 0 {
		// Precondition-violation guard: production callers (vm.CompileTraceQLMetrics)
		// always produce Enabled=true with a positive StepSizeNanos, so this branch is
		// currently unreachable in practice. Treated as a shape-not-answerable decline
		// rather than a silent (nil, false, nil) signal, consistent with this function's
		// own no-silent-decline contract (issue #481 part 3 / go-presubmit #2).
		return nil, false, ErrMetricsShapeNotAnswerable
	}

	// Collect the matching spans for the filter from the value index.
	// Issue #481 part 3: an uncovered leaf is now a typed sentinel error (production default).
	matches, ok := viMatchSpans(source, prog)
	if !ok {
		return nil, false, ErrMetricsNoCoverage
	}

	numBuckets := (tb.EndTime - tb.StartTime + tb.StepSizeNanos - 1) / tb.StepSizeNanos
	if numBuckets <= 0 {
		return &TraceMetricsResult{IndexBytesRead: viIndexBytesRead(source)}, true, nil
	}

	// Count distinct TraceIDs per bucket. A TraceID may appear in multiple spans;
	// count_over_time()/rate() over the trace-level series counts each distinct
	// TraceID once per bucket (matches the block-scan accumulator, which keys the
	// dense series by bucket only when there is no group-by).
	seenPerBucket := make(map[int64]map[[16]byte]struct{}, numBuckets)
	for _, m := range matches {
		if m.TimeSec == 0 {
			// TimeSec == 0 means the file predates per-span timestamps; its time
			// bucket is unknown, so this span cannot be safely placed. Issue #481 part
			// 3: this is a typed sentinel error — there is no scan to fall back to, and
			// silently dropping the span would be a wrong answer.
			return nil, false, ErrMetricsLegacyTimeSecZero
		}
		tsNanos := int64(m.TimeSec) * 1_000_000_000 //nolint:gosec
		// Right-closed intervals (StartTime, EndTime] — matches the scan path.
		if tsNanos <= tb.StartTime || tsNanos > tb.EndTime {
			continue
		}
		bucketIdx := timeBucketIndex(tsNanos, tb.StartTime, tb.StepSizeNanos)
		if bucketIdx < 0 || bucketIdx >= numBuckets {
			continue
		}
		traces, exists := seenPerBucket[bucketIdx]
		if !exists {
			traces = make(map[[16]byte]struct{})
			seenPerBucket[bucketIdx] = traces
		}
		traces[m.TraceID] = struct{}{}
	}

	values := make([]float64, numBuckets)
	stepSec := float64(tb.StepSizeNanos) / 1e9
	for bucketIdx, traces := range seenPerBucket {
		count := float64(len(traces))
		if spec.Aggregate.Function == vm.FuncNameRATE && stepSec > 0 {
			count /= stepSec
		}
		values[bucketIdx] = count
	}

	result := &TraceMetricsResult{
		Series:         []TraceTimeSeries{{Values: values}},
		IndexBytesRead: viIndexBytesRead(source),
	}
	return result, true, nil
}

// viIndexBytesRead returns the total value-index byte count the querier's ValueIndexSource
// downloaded resolving this query (issue #218), or 0 when source isn't the production
// *SliceValueIndexSource (e.g. a test fake ValueIndexSource that doesn't track file I/O).
func viIndexBytesRead(source ValueIndexSource) int64 {
	if sl, ok := source.(*SliceValueIndexSource); ok {
		return sl.Stats().BytesRead
	}
	return 0
}

// viMatchSpans walks the program's predicate tree and resolves the matching spans
// from the value index. It returns (spans, true) when every leaf has index coverage,
// or (nil, false) when any leaf is unindexed (caller falls back to a block scan).
//
// Match-all queries ({}) have no leaf conditions; they enumerate every indexed span
// via source.AllResults. A nil prog is ALSO match-all — compileStructuralPair's own
// nil-filter-leg convention (plan-d.md D2, issue #489) compiles a match-all structural leg
// directly to a nil *vm.Program (skipping vm.CompileTraceQLFilter entirely, rather than
// producing a Program with empty Predicates), so this must be checked before dereferencing prog,
// not folded into the len(preds.Nodes)==0 case below.
func viMatchSpans(source ValueIndexSource, prog *vm.Program) ([]VILookupResult, bool) {
	if prog == nil {
		return source.AllResults()
	}
	preds := prog.Predicates
	if preds == nil || (len(preds.Nodes) == 0 && len(preds.Columns) == 0) {
		// Match-all: count every indexed span.
		return source.AllResults()
	}
	if len(preds.Nodes) == 0 {
		// Columns referenced but no evaluable nodes — cannot resolve from the index.
		return nil, false
	}
	// Explicit opt-in (reviewer-2-6 MEDIUM fix): only a source vibuilder.BuildSourceBounded
	// itself marked (via MarkNewestFirst) may route through viEvalAND/viEvalOR below. Every
	// other caller — ExecuteTraceMetricsFromVI, the structural path, and QueryTraceQLFromIndex
	// when the source came from the ordinary unbounded BuildSource — gets newestFirst=false
	// here and stays on the pre-Phase-3/4 key-sorted merge-join chain, byte-identical to
	// before those phases existed. sl==nil (a test fake ValueIndexSource) also defaults false.
	newestFirst := false
	if sl, ok := source.(*SliceValueIndexSource); ok {
		newestFirst = sl.isNewestFirst()
	}
	// issue #206: leafIdx counts leaf SLOTS (nodes with no Children) in the exact same DFS
	// pre-order this whole viEvalNode*/viEvalNodes recursion already walks, one increment per
	// slot visited regardless of whether that slot's Column is set — see viEvalNode's own
	// doc comment for why this must be a purely structural, position-based counter (not a
	// pointer) and vibuilder's collectLeaves for the build-time counterpart that numbers
	// leaves identically over the same tree shape.
	leafIdx := 0
	return viEvalNodes(source, preds.Nodes, false, newestFirst, &leafIdx)
}

// viEvalNodes evaluates a slice of sibling RangeNodes combined by AND, returning the
// span set that satisfies all of them. When isOR is true the siblings are combined by
// OR (union) instead. Returns (nil, false) if any leaf lacks index coverage.
//
// NOTE-VI-040 (#430): the accumulator is kept sorted by the 24-byte span key
// (TraceID++SpanID) so AND intersection and OR union are streaming merge-joins
// over two sorted runs rather than hash-map materialisations. The first set is
// sorted+deduplicated once; every subsequent merge-join output is itself sorted,
// so the accumulator stays sorted across the whole tree walk with no per-node map
// allocation. This bounds intersect/union to O(n+m) time and O(1) extra space
// beyond the output, matching the spec's "merge-join on sorted span ID sets to
// avoid materializing large sets in memory".
//
// Phase 2 (plan-scan-fallback.md): a GENUINE single leaf (exactly one top-level node,
// itself a leaf with no children — nothing to intersect/union with) skips the sort-by-
// span-key step entirely and returns viEvalNode's own result unchanged. Sorting was never
// load-bearing for a lone leaf's correctness (there is no second set to merge-join
// against); it was only ever a precondition for the multi-node merge-join below. Skipping
// it here lets the caller's own result order survive unchanged — specifically,
// BuildSourceBounded's newest-first early-stopping order — which the multi-node path
// still destroys today (Phase 3/4's own job to fix, not this one: viUnionSorted/
// viIntersectSorted are both key-sorted, not time-sorted, exactly per this file's own
// existing doc comments on those functions). A genuine duplicate within a single leaf's
// own results is still safely collapsed downstream by QueryTraceQLFromIndex's own
// identity map (search_trace_vi.go), so this is not a correctness regression for that
// case either.
// Phase 3 (plan-scan-fallback.md): a GENUINE flat OR of leaves (isOR, every sibling itself
// a leaf with no children -- "absence of nested AND") resolves via ViUnionNewestFirst
// instead of the sort-by-span-key merge-join above. This applies at ANY nesting depth
// (viEvalNode recurses into a composite's own Children with the SAME isOR flag, so a
// nested `(B OR C)` sub-clause of leaves gets this treatment too, not just a top-level
// OR). limit=0 (unbounded) is used here: correctness (WHICH spans end up in the union,
// deduplicated by the same viSpanCmp identity rule viUnionSorted uses) does not depend on
// input order -- a k-way merge visits every element of every set exactly once regardless
// of whether each set happens to be newest-first-sorted. Only the OUTPUT order depends on
// that: when the source was built via BuildSourceBounded's early-stopping path (Phase 2),
// each leaf's own results already arrive newest-first, so the merged output is genuinely
// newest-first end to end; when built via the ordinary unbounded BuildSource, this is
// simply a harmless reordering with zero effect on an unbounded query's correctness.
func viEvalOR(source ValueIndexSource, nodes []vm.RangeNode, leafIdx *int) ([]VILookupResult, bool) {
	sets := make([][]VILookupResult, len(nodes))
	for i := range nodes {
		// nodes are all leaves here (viNodesAreAllLeaves already checked by the caller), so
		// viEvalNode never recurses into viEvalNodes for these -- newestFirst=true is passed
		// for semantic consistency (we are already inside a confirmed newest-first context)
		// but has no observable effect since len(node.Children)==0 for every node here.
		set, ok := viEvalNode(source, &nodes[i], true, leafIdx)
		if !ok {
			return nil, false
		}
		sets[i] = set
	}
	return ViUnionNewestFirst(sets, 0), true
}

// viEvalAND is viEvalOR's AND-side twin (Phase 4, plan-scan-fallback.md): a GENUINE flat
// AND of leaves (every sibling itself a leaf with no children) resolves via
// viIntersectOrdered instead of the key-sorted viSortDedup/viIntersectSorted merge-join
// below, preserving whatever order the per-leaf sets already carry (newest-first, when the
// source was built via BuildSourceBounded's Phase 4 anchor+confirm path; unchanged content
// either way when built via the ordinary unbounded BuildSource). Correctness never depends
// on WHICH leaf's order is preserved or on how the per-leaf sets were produced -- each set
// is already independently correct for its own leaf's predicate regardless of source, so
// narrowing to their intersection is safe by construction (viIntersectOrdered only ever
// removes elements, never adds any).
func viEvalAND(source ValueIndexSource, nodes []vm.RangeNode, leafIdx *int) ([]VILookupResult, bool) {
	sets := make([][]VILookupResult, len(nodes))
	for i := range nodes {
		// nodes are all leaves here (viNodesAreAllLeaves already checked by the caller), so
		// viEvalNode never recurses into viEvalNodes for these -- newestFirst=true is passed
		// for semantic consistency (we are already inside a confirmed newest-first context)
		// but has no observable effect since len(node.Children)==0 for every node here.
		set, ok := viEvalNode(source, &nodes[i], true, leafIdx)
		if !ok {
			return nil, false
		}
		sets[i] = set
	}
	return viIntersectOrdered(sets), true
}

// viNodesAreAllLeaves reports whether every node in nodes is itself a leaf (no children) --
// the precondition for viEvalOR's flat-OR optimization above.
func viNodesAreAllLeaves(nodes []vm.RangeNode) bool {
	for i := range nodes {
		if len(nodes[i].Children) > 0 {
			return false
		}
	}
	return true
}

// newestFirst is the explicit opt-in reviewer-2-6's MEDIUM finding required (see
// viMatchSpans's own doc comment): only true when the caller's source was built via
// vibuilder.BuildSourceBounded's genuine early-stopping path. False routes every flat-
// leaves AND/OR shape through the ORIGINAL key-sorted viSortDedup/viUnionSorted/
// viIntersectSorted chain below, byte-identical to pre-Phase-3/4 behavior -- this is the
// default for every existing (non-bounded) caller.
func viEvalNodes(
	source ValueIndexSource,
	nodes []vm.RangeNode,
	isOR, newestFirst bool,
	leafIdx *int,
) ([]VILookupResult, bool) {
	if len(nodes) == 1 {
		// A single top-level/sibling node has nothing else to merge-join against at THIS
		// level, whether it is a leaf (nothing to combine, full stop) or itself a composite
		// (viEvalNode recurses into it and that recursive call already performs its own
		// correct dedup/combination internally -- wrapping the result in another
		// viSortDedup here would be redundant at best and, for a composite child resolved
		// via viEvalOR's newest-first merge below, actively destructive: it would re-sort
		// away the exact order Phase 2/3's early-stopping worked to establish.
		return viEvalNode(source, &nodes[0], newestFirst, leafIdx)
	}
	if newestFirst && isOR && viNodesAreAllLeaves(nodes) {
		return viEvalOR(source, nodes, leafIdx)
	}
	if newestFirst && !isOR && viNodesAreAllLeaves(nodes) {
		return viEvalAND(source, nodes, leafIdx)
	}
	var acc []VILookupResult
	first := true
	for i := range nodes {
		set, ok := viEvalNode(source, &nodes[i], newestFirst, leafIdx)
		if !ok {
			return nil, false
		}
		switch {
		case first:
			acc = viSortDedup(set)
			first = false
		case isOR:
			acc = viUnionSorted(acc, viSortDedup(set))
		default:
			acc = viIntersectSorted(acc, viSortDedup(set))
		}
	}
	if first {
		return nil, false
	}
	return acc, true
}

// viEvalNode evaluates a single node. A node with children is an internal AND/OR
// combiner; a leaf node carries a Column and is resolved via the value index.
// newestFirst is forwarded unchanged into any recursive viEvalNodes call — see that
// function's own doc comment for the explicit-opt-in contract.
// leafIdx is a running DFS leaf-slot counter shared across this whole recursive walk
// (viEvalNode/viEvalNodes/viEvalOR/viEvalAND): every node with no Children consumes the
// NEXT slot, in the exact same pre-order vibuilder's collectLeaves numbers leaves in at
// build time — see this function's own "issue #206" comment below and AddLeaf's doc
// comment for why this structural position, not the node's own pointer, is the safe
// cross-call leaf identity.
func viEvalNode(source ValueIndexSource, node *vm.RangeNode, newestFirst bool, leafIdx *int) ([]VILookupResult, bool) {
	if len(node.Children) > 0 {
		return viEvalNodes(source, node.Children, node.IsOR, newestFirst, leafIdx)
	}
	myIdx := *leafIdx
	*leafIdx++
	if node.Column == "" {
		return nil, false
	}
	// issue #206: a same-column multi-leaf AND (e.g. `col >= 100 && col <= 150`) compiles to
	// TWO separate leaf RangeNodes on the same column (see AddLeaf's doc comment) — resolving
	// both leaves via LookupResults' column-name-only keying would return the SAME merged
	// (both leaves') set for each, so viEvalAND's "intersection" is really a union. When
	// source is the production *SliceValueIndexSource, resolve via LookupLeaf instead, which
	// disambiguates by this exact leaf's structural DFS position (myIdx). Every other
	// ValueIndexSource implementation (test fakes elsewhere in this package) is unaffected
	// and keeps the pre-existing column-only resolution — this type assertion changes
	// behavior for the production source only, and is a no-op (identical result) whenever
	// there is no same-column ambiguity to resolve.
	if sl, ok := source.(*SliceValueIndexSource); ok {
		return sl.LookupLeaf(myIdx, node.Column, modules_shared.ColumnTypeString)
	}
	// The SliceValueIndexSource already holds predicate-matched results per column
	// (QueryFiles applied the leaf predicate at download time), so the executor only
	// needs the column identity here. colType is resolved by the source from the
	// stored index entries.
	return source.LookupResults(node.Column, modules_shared.ColumnTypeString)
}

// viSortDedup returns set sorted ascending by the 24-byte span key
// (TraceID++SpanID) with consecutive duplicate keys collapsed to their first
// occurrence. It sorts in place (set is owned by the caller's per-column slice,
// which the boolean walk does not reuse afterwards) and returns the deduplicated
// prefix. This is the precondition for the merge-join intersect/union below.
func viSortDedup(set []VILookupResult) []VILookupResult {
	if len(set) < 2 {
		return set
	}
	slices.SortFunc(set, viSpanCmp)
	out := set[:1]
	last := set[0]
	for _, s := range set[1:] {
		if viSpanCmp(s, last) == 0 {
			continue
		}
		last = s
		out = append(out, s)
	}
	return out
}

// ViUnionNewestFirst merges N per-leaf newest-first-ordered VILookupResult slices
// (Phase 2's BuildSourceBounded output per leaf, plan-scan-fallback.md Phase 3) into a
// single newest-first stream, deduplicating by the SAME (SourceRef, span key) identity
// rule viSpanCmp/viSortDedup/viUnionSorted use, stopping once the deduplicated output
// reaches limit. A k-way heap merge (container/heap) over N per-leaf streams, not a
// repeated pairwise viUnionSorted (which is key-sorted, not time-sorted, and would defeat
// early-stopping).
//
// limit<=0 means unbounded: every element of every set is eventually visited and
// deduplicated. Exported (capitalized) so vibuilder — a different package, mirroring how
// it already consumes VILookupResult/SliceValueIndexSource — can call it directly for
// Phase 3's multi-leaf-OR routing, per this file's own "boolean combination logic lives in
// executor, callers never re-implement it" convention (viEvalNodes itself).
//
// Correctness does not depend on the input sets actually being sorted newest-first: the
// heap visits every element of every set exactly once regardless of order, so the returned
// UNION (which elements are included) is correct even over arbitrarily-ordered inputs.
// Only the OUTPUT's own order depends on the inputs' order — when every input set is
// genuinely newest-first (as BuildSourceBounded's early-stopping resolution produces), the
// merged output is too, and early-stopping at limit then represents the true newest-limit
// union rather than an arbitrary same-size subset.
func ViUnionNewestFirst(sets [][]VILookupResult, limit int) []VILookupResult {
	h := &viNewestFirstHeap{sets: sets}
	for si, set := range sets {
		if len(set) > 0 {
			h.items = append(h.items, viNewestFirstHeapItem{setIdx: si, elemIdx: 0})
		}
	}
	heap.Init(h)

	type seenKey struct {
		sourceRef string
		key       [22]byte
	}
	seen := make(map[seenKey]struct{})
	var out []VILookupResult
	for h.Len() > 0 {
		if limit > 0 && len(out) >= limit {
			break
		}
		it := heap.Pop(h).(viNewestFirstHeapItem) //nolint:forcetypeassert // heap.Interface contract, this package's own type
		e := sets[it.setIdx][it.elemIdx]
		sk := seenKey{sourceRef: e.SourceRef, key: viSpanKey(e)}
		if _, dup := seen[sk]; !dup {
			seen[sk] = struct{}{}
			out = append(out, e)
		}
		if it.elemIdx+1 < len(sets[it.setIdx]) {
			heap.Push(h, viNewestFirstHeapItem{setIdx: it.setIdx, elemIdx: it.elemIdx + 1})
		}
	}
	return out
}

// viNewestFirstHeapItem identifies one candidate element (the current head of one input
// set) in viNewestFirstHeap's k-way merge.
type viNewestFirstHeapItem struct {
	setIdx, elemIdx int
}

// viNewestFirstHeap is a container/heap.Interface max-heap over the CURRENT head element of
// each input set, ordered by TimeSec descending (newest first) — ViUnionNewestFirst's k-way
// merge primitive.
type viNewestFirstHeap struct {
	sets  [][]VILookupResult
	items []viNewestFirstHeapItem
}

func (h *viNewestFirstHeap) Len() int { return len(h.items) }

func (h *viNewestFirstHeap) Less(i, j int) bool {
	a := h.sets[h.items[i].setIdx][h.items[i].elemIdx]
	b := h.sets[h.items[j].setIdx][h.items[j].elemIdx]
	return a.TimeSec > b.TimeSec // max-heap by TimeSec: newest first
}

func (h *viNewestFirstHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *viNewestFirstHeap) Push(x any) {
	item, ok := x.(viNewestFirstHeapItem)
	if !ok {
		return
	}
	h.items = append(h.items, item)
}

func (h *viNewestFirstHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// viUnionSorted returns the union of two key-sorted, key-deduplicated span sets
// as a streaming merge-join. Both inputs MUST already be sorted ascending by the
// 24-byte span key (viSortDedup); the output is sorted the same way.
func viUnionSorted(a, b []VILookupResult) []VILookupResult {
	out := make([]VILookupResult, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch viSpanCmp(a[i], b[j]) {
		case 0:
			out = append(out, a[i])
			i++
			j++
		case -1:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// viIntersectSorted returns the intersection of two key-sorted, key-deduplicated
// span sets as a streaming merge-join. Both inputs MUST already be sorted
// ascending by the 24-byte span key (viSortDedup); the output is sorted the same
// way. Uses O(1) extra space beyond the output, no hash map.
func viIntersectSorted(a, b []VILookupResult) []VILookupResult {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := make([]VILookupResult, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch viSpanCmp(a[i], b[j]) {
		case 0:
			out = append(out, a[i])
			i++
			j++
		case -1:
			i++
		default:
			j++
		}
	}
	return out
}

// viSpanKey is the dedup/intersect key: TraceID (16) ++ SpanID (8).
// viSpanKey builds the span-identity key used for AND/OR intersection across columns.
//
// NOTE-VI-045 (#429): the v2 BucketGroup write path identifies a span by its physical
// location — (SourceRef, BlockPage, RowIdx) — because that format stores span row indexes,
// not the 8-byte SpanID. The key therefore packs TraceID[16] + BlockPage[4] + RowIdx[2] into
// a fixed 22-byte array; SourceRef equality is enforced separately by viSpanCmp so two files
// that reuse the same page number cannot collide.
func viSpanKey(s VILookupResult) [22]byte {
	var k [22]byte
	copy(k[:16], s.TraceID[:])
	binary.LittleEndian.PutUint32(k[16:20], s.BlockPage)
	binary.LittleEndian.PutUint16(k[20:22], s.RowIdx)
	return k
}

// viSpanCmp orders two results ascending by (SourceRef, span key). SourceRef is compared
// first so results from distinct files never collide on a shared page number.
func viSpanCmp(x, y VILookupResult) int {
	if c := strings.Compare(x.SourceRef, y.SourceRef); c != 0 {
		return c
	}
	kx, ky := viSpanKey(x), viSpanKey(y)
	return bytes.Compare(kx[:], ky[:])
}

// ValueIndexBuildStats records the I/O the builder performed assembling a
// SliceValueIndexSource: how many value-index files were downloaded, their total
// byte size, and how many span entries were matched. The builder populates these
// so the querier can attach them to its OTel span / log line (tempo issue #465).
// All counters are advisory observability — they never affect query results.
type ValueIndexBuildStats struct {
	// FilesRead is the number of value-index files downloaded from object storage.
	FilesRead int
	// BytesRead is the total byte size of those downloaded files.
	BytesRead int64
	// Hits is the number of span entries the per-column predicates matched (the
	// raw lookup-result count before AND/OR span-ID intersection).
	Hits int
}

// SliceValueIndexSource implements ValueIndexSource from pre-downloaded value-index
// results, grouped by column name and type. Callers populate it from
// valueindex.QueryFiles output (one leaf predicate already applied per column).
//
// mu guards data and stats: vibuilder's leaf-loop parallelization (tempo timeout
// incident, issue #465) calls Add/RecordFileIO concurrently from multiple leaf
// goroutines, so every method must be safe for concurrent callers.
type SliceValueIndexSource struct {
	// data maps colName → colType → matched results for that column.
	data map[string]map[modules_shared.ColumnType][]VILookupResult
	// leafData maps one specific predicate LEAF's structural position (its DFS leaf-slot
	// index, per viEvalNode's DFS leaf-slot counter) to that leaf's own matched results,
	// keeping same-column leaves separate — see AddLeaf's doc comment for the full issue
	// #206 rationale and why a STRUCTURAL index, not a *vm.RangeNode pointer, is the only
	// safe cross-call leaf identity here.
	leafData map[int][]VILookupResult
	// requirePresentLeaves marks leaf slots that vibuilder's leaf loop identified as a
	// RequirePresent-only leaf (vm.RangeNode.RequirePresent -- buildPredicate's own default
	// branch, "RequirePresent or empty leaf: existence-only, not a value predicate") and
	// therefore deliberately never AddLeaf'd. Keyed by leafIdx, valued by the SPECIFIC
	// sibling leaf-slot indices that provide the safe substitute (task #213, CRITICAL
	// regression fix — corrects task #212's original bare-column-name keying, see
	// MarkRequirePresentLeaf's own doc comment for the full root-cause writeup).
	//
	// Task #212 (NOTE-VI-107 addendum): #207's leaf-aware decline gate cannot tell "this leaf
	// was never meant to be leaf-resolved at all, by design" (this map) apart from "this leaf
	// was genuinely declined for some other reason" (task #204/#206's own decidability
	// declines) using leafData absence alone -- see LookupLeaf's doc comment for how this map
	// lets it draw that distinction safely.
	requirePresentLeaves map[int][]int
	// stats records the build-time I/O so the querier can report it (issue #465).
	stats ValueIndexBuildStats
	mu    sync.Mutex
	// leafAware is true once AddLeaf has ever been called on this source with a valid
	// (leafIdx >= 0) structural position, for ANY leaf — see LookupLeaf's own doc comment
	// (issue #207) for why this must gate the column-aggregate fallback. Once leaf-aware
	// keying has been used for even one leaf of a query, every OTHER leaf sharing that
	// source must be resolved leaf-specifically too: a leaf with no leafData entry in a
	// leaf-aware source means "this exact leaf was declined at build time" (e.g. task
	// #204's decidability gate), NOT "this source predates leaf-aware keying" — falling
	// back to LookupResults' column aggregate in that case would silently borrow an
	// unrelated SAME-COLUMN sibling leaf's own results, reintroducing issue #206's
	// union-instead-of-intersection bug through a different path. The fallback remains
	// correct ONLY for a source that never used AddLeaf with a valid index at all (a
	// genuinely legacy, non-leaf-aware caller populated purely via Add, or every AddLeaf
	// call it made used leafIdx < 0).
	leafAware bool
	// newestFirst is true only when EVERY leaf Added to this source was resolved via a
	// genuine newest-first early-stopping path (vibuilder.BuildSourceBounded's Phase
	// 2/3/4 codepaths — single leaf, flat OR, or pure AND anchor+confirm), never when any
	// leaf fell through to the ordinary unbounded lookupColumn. Set once via
	// MarkNewestFirst by the builder; read by viMatchSpans to decide whether viEvalNodes
	// may route through viEvalAND/viEvalOR's order-preserving merge instead of the
	// default key-sorted viSortDedup/viUnionSorted/viIntersectSorted chain. Defaults
	// false, so every existing (non-bounded) caller is byte-identical to pre-Phase-3/4
	// behavior — the fix for reviewer-2-6's MEDIUM finding (isolate the new routing
	// behind an explicit opt-in instead of firing for any flat-leaves shape regardless of
	// caller).
	newestFirst bool
}

// NewSliceValueIndexSource builds an empty source. Use Add to populate per-column
// results.
func NewSliceValueIndexSource() *SliceValueIndexSource {
	return &SliceValueIndexSource{
		data: make(map[string]map[modules_shared.ColumnType][]VILookupResult),
	}
}

// MarkNewestFirst records that every leaf this source will have (or already has) Added
// was resolved via a genuine newest-first early-stopping path — see the newestFirst
// field's own doc comment for the exact contract. The caller (vibuilder.BuildSourceBounded)
// must call this ONLY when true for the WHOLE source, never per-leaf: viEvalAND/viEvalOR
// combine leaves from the same source together, so a partially-bounded source (some
// leaves early-stopped, others not) must NOT be marked -- BuildSourceBounded's own
// single-leaf/flat-OR/pure-AND branches are each all-or-nothing across every leaf in the
// query today, so this is always safe to call unconditionally when one of those branches
// was taken.
func (s *SliceValueIndexSource) MarkNewestFirst() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.newestFirst = true
}

// isNewestFirst reports whether MarkNewestFirst was called on this source. Unexported:
// only viMatchSpans (same package) needs to read this, via a type assertion against the
// ValueIndexSource interface parameter it receives -- deliberately NOT part of the
// ValueIndexSource interface itself, so test fakes implementing that interface elsewhere
// in this package need no changes and implicitly behave as newestFirst=false.
func (s *SliceValueIndexSource) isNewestFirst() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.newestFirst
}

// RecordFileIO accumulates the builder's per-column download I/O into the source's
// stats (issue #465). filesRead is the file count for the column, bytesRead their
// total size. Safe to call repeatedly; counters accumulate.
func (s *SliceValueIndexSource) RecordFileIO(filesRead int, bytesRead int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.FilesRead += filesRead
	s.stats.BytesRead += bytesRead
}

// Stats returns the build-time I/O counters. Hits is derived from the stored
// results so it stays correct regardless of Add ordering.
func (s *SliceValueIndexSource) Stats() ValueIndexBuildStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	for _, byType := range s.data {
		for _, results := range byType {
			out.Hits += len(results)
		}
	}
	return out
}

// Add records the matched results for one (colName, colType). Repeated calls for the
// same key append.
func (s *SliceValueIndexSource) Add(colName string, colType modules_shared.ColumnType, results []VILookupResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byType, ok := s.data[colName]
	if !ok {
		byType = make(map[modules_shared.ColumnType][]VILookupResult)
		s.data[colName] = byType
	}
	byType[colType] = append(byType[colType], results...)
}

// AddLeaf records results for one specific predicate LEAF, identified by leafIdx — its
// structural position per viEvalNode's DFS leaf-slot counter — in addition to colName's
// per-column aggregate bucket (Add's existing behavior, still needed for AllResults/Stats,
// and as LookupLeaf's fallback for any leaf recorded without a valid index).
//
// issue #206: a same-column multi-leaf AND (e.g. `score >= 100 && score <= 150`) compiles
// to TWO separate leaf RangeNodes on the same column — traceql_compiler's
// extractTraceQLNodes always flattens an AND into sibling leaves and NEVER combines two
// range bounds into one Min+Max node (confirmed by direct inspection of its compiled
// output, not assumed). Add's column-name-only keying merges both leaves' results into one
// bucket, so a later LookupResults(colName) call cannot tell leaf A's own matches from leaf
// B's — viEvalAND then "intersects" two identical (already-merged) sets, which returns
// their union, not the true intersection. AddLeaf/LookupLeaf fix this by keying the
// leaf-specific bucket on the leaf's own structural position instead of its column name.
//
// A structural (DFS leaf-slot) index, not a *vm.RangeNode pointer, is the only identity
// that is actually safe across the build/eval split in production: the public
// blockpack.QueryTraceQLFromIndex takes the query as a STRING and recompiles it internally
// (search_trace_vi.go calls vm.CompileTraceQLFilter itself) rather than reusing the
// *vm.Program the caller already compiled to build the source — so the *vm.RangeNode
// pointers vibuilder saw while building the source and the ones the executor sees while
// evaluating it are, in general, two DIFFERENT tree instances, even for the identical
// query text (verified directly: pointer identity does NOT hold across independent
// compiles). What IS guaranteed identical across two compiles of the same query text is
// the tree's STRUCTURAL SHAPE — same leaf count, same nesting, same sibling order — because
// compilation is a deterministic, purely syntax-driven walk with no per-call randomness.
// viEvalNode's DFS leaf-slot counter's pre-order numbering over that shape is therefore stable regardless
// of which *vm.Program instance is walked, as long as both walks compiled the identical
// query text (vibuilder's collectLeaves uses the exact same numbering rule, so build-time
// and eval-time indices always agree for a given leaf's position in the tree).
//
// leafIdx < 0 means "no identity to offer" (e.g. a match-all column-only Add, or a caller
// with nothing better); AddLeaf then behaves exactly like Add, recording only the
// column-level aggregate.
func (s *SliceValueIndexSource) AddLeaf(
	leafIdx int, colName string, colType modules_shared.ColumnType, results []VILookupResult,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byType, ok := s.data[colName]
	if !ok {
		byType = make(map[modules_shared.ColumnType][]VILookupResult)
		s.data[colName] = byType
	}
	byType[colType] = append(byType[colType], results...)
	if leafIdx >= 0 {
		if s.leafData == nil {
			s.leafData = make(map[int][]VILookupResult)
		}
		s.leafData[leafIdx] = append(s.leafData[leafIdx], results...)
		// issue #207: recorded even for an empty results slice — a covered-but-empty leaf
		// still means "leaf-aware keying is in use for this source" (see leafAware's own
		// doc comment); only the ABSENCE of a leafData entry, not an empty one, signals a
		// build-time decline.
		s.leafAware = true
	}
}

// MarkRequirePresentLeaf records that leafIdx is a RequirePresent-only leaf (existence
// check, no value predicate — vm.RangeNode.RequirePresent) that vibuilder's leaf loop
// deliberately never AddLeaf'd: buildPredicate's own default branch never builds a value
// predicate for it ("RequirePresent or empty leaf: existence-only, not a value
// predicate"), so this leafIdx's leafData entry is never populated.
//
// Signature changed by task #213 (CRITICAL regression fix, corrects task #212's original
// MarkRequirePresentLeaf(leafIdx int, colName string)): pairedLeafIdxs replaces colName.
//
// Task #212 (NOTE-VI-107 addendum, issue #207 follow-up) originally keyed this by the
// leaf's bare column NAME, reasoning that the `attr != V` rewrite always emits this
// RequirePresent leaf as a sibling of a real `attr < V OR attr > V` comparison on the SAME
// column, so substituting that column's own aggregate (LookupResults) was "safe."
//
// Task #213 found this true ONLY for the SCOPED rewrite (`span.attr != V` /
// `resource.attr != V`), where the RequirePresent leaf's column has no other leaf on it
// besides this exact rewrite's own sibling — the aggregate and the sibling's own data are
// identical. It is FALSE for the UNSCOPED rewrite (`.attr != V`, no scope prefix), which
// compiles to `OR{RequirePresent(resource.attr), RequirePresent(span.attr)}` with NO
// value-bearing sibling of its own at all (deliberate — composing a per-scope range-OR
// across two scopes was judged not worth the complexity, see extractNeqNode's own
// doc comment). unscopedCols produces the exact same expanded column names
// ("resource.<name>", "span.<name>") a completely unrelated, independently-authored leaf
// elsewhere in the SAME query might reference — e.g. `{ .score != 100 && resource.score >
// 5 }` — and the bare-column-name keying could not tell the two apart: it substituted
// whatever arbitrary match set already existed for that column name, which is neither a
// superset nor an exact match of true presence, silently producing wrong answers (false
// positives via AND, false negatives via OR — see internal/modules/executor/NOTES.md's
// task #213 addendum for both reproductions).
//
// The fix: pairedLeafIdxs identifies the EXACT sibling leaf(s) — never a bare column name —
// that provide the safe substitute. vibuilder's collectLeaves (builder.go) computes these
// ONLY for the scoped rewrite's RequirePresent leaf (vm.RangeNode.NeqPairedRange), by
// construction pointing at the adjacent range-OR composite's own two child leaves from the
// SAME rewrite call. The unscoped rewrite's RequirePresent leaves never get a non-empty
// pairedLeafIdxs, so vibuilder's leaf loop never calls this function for them at all — see
// LookupLeaf's own doc comment for how a leaf with no entry in this map still correctly
// declines.
//
// leafIdx < 0 or an empty pairedLeafIdxs is a no-op, mirroring AddLeaf's own "no identity to
// offer" contract.
func (s *SliceValueIndexSource) MarkRequirePresentLeaf(leafIdx int, pairedLeafIdxs []int) {
	if leafIdx < 0 || len(pairedLeafIdxs) == 0 {
		return
	}
	cp := make([]int, len(pairedLeafIdxs))
	copy(cp, pairedLeafIdxs)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requirePresentLeaves == nil {
		s.requirePresentLeaves = make(map[int][]int)
	}
	s.requirePresentLeaves[leafIdx] = cp
}

// LookupLeaf returns the matched results recorded for the exact structural leaf position
// via AddLeaf (issue #206's fix — see AddLeaf's own doc comment for the structural-index
// contract and why it must be positional, not pointer-based).
//
// issue #207: a missing leafData[leafIdx] entry means two very different things depending
// on whether THIS source ever used leaf-aware keying at all (s.leafAware — see that
// field's own doc comment):
//
//   - leafAware == false: this source was populated purely through the legacy Add (or
//     every AddLeaf call it saw used leafIdx < 0) — no leaf of this query was ever keyed
//     positionally, so falling back to LookupResults' column-level aggregate is the
//     original, correct legacy behavior and the only data available.
//
//   - leafAware == true: at least one OTHER leaf of this same query WAS keyed
//     positionally via AddLeaf. A missing entry for THIS leafIdx then means this exact
//     leaf was declined at build time (e.g. task #204's decidability gate on
//     span:duration, or a same-column multi-value-equality decline) — NOT "this source
//     predates leaf-aware keying". Falling back to LookupResults here would silently
//     borrow an unrelated SAME-COLUMN sibling leaf's own results (LookupResults'
//     aggregate bucket is shared by every leaf on that column — see AddLeaf's doc
//     comment), reintroducing issue #206's union-instead-of-intersection bug through a
//     different path. The correct answer is "not covered": (nil, false), forcing the
//     caller (viEvalNode/viEvalAND/viEvalOR) to decline the whole query rather than
//     silently answer a different one.
//
//     Task #212 carve-out (NOTE-VI-107 addendum), corrected by task #213 (CRITICAL
//     regression fix): the ONE exception is a leaf MarkRequirePresentLeaf identified as
//     RequirePresent-only AND paired with specific sibling leaf(s) (pairedLeafIdxs) that
//     ALL have their own leafData entry — see MarkRequirePresentLeaf's own doc comment for
//     the full root-cause writeup of why substituting a bare column-name aggregate was
//     unsafe, and why substituting ONLY the exact paired sibling(s)' own data is safe. A
//     RequirePresent leaf with no pairing at all (never marked — the unscoped `!=`
//     rewrite's shape) or whose paired sibling(s) lack their own leafData entry still falls
//     through to the ordinary decline.
//
// colName/colType are LookupResults' own arguments, used only on the leafAware==false
// fallback path.
func (s *SliceValueIndexSource) LookupLeaf(
	leafIdx int, colName string, colType modules_shared.ColumnType,
) ([]VILookupResult, bool) {
	if leafIdx >= 0 {
		s.mu.Lock()
		results, ok := s.leafData[leafIdx]
		leafAware := s.leafAware
		pairedIdxs, isRequirePresent := s.requirePresentLeaves[leafIdx]
		var pairedResults []VILookupResult
		pairedOK := isRequirePresent
		if isRequirePresent {
			for _, pIdx := range pairedIdxs {
				pr, pok := s.leafData[pIdx]
				if !pok {
					pairedOK = false
					break
				}
				pairedResults = append(pairedResults, pr...)
			}
		}
		s.mu.Unlock()
		if ok {
			out := make([]VILookupResult, len(results))
			copy(out, results)
			return out, true
		}
		if leafAware {
			if isRequirePresent && pairedOK {
				out := make([]VILookupResult, len(pairedResults))
				copy(out, pairedResults)
				return out, true
			}
			return nil, false
		}
	}
	return s.LookupResults(colName, colType)
}

// LookupResults returns the matched results for colName. The colType argument is
// advisory: the source returns results across all stored types for the column (the
// leaf predicate that produced them already constrained the type), so an unindexed
// column yields (nil, false) and an indexed one yields (results, true).
func (s *SliceValueIndexSource) LookupResults(colName string, _ modules_shared.ColumnType) ([]VILookupResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byType, ok := s.data[colName]
	if !ok {
		return nil, false
	}
	var out []VILookupResult
	for _, results := range byType {
		out = append(out, results...)
	}
	return out, true
}

// AllResults returns every stored span across all columns, deduplicated by
// (TraceID, SpanID). It returns (nil, false) when the source holds no data, so a
// match-all query falls back to a block scan rather than reporting an empty result.
func (s *SliceValueIndexSource) AllResults() ([]VILookupResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return nil, false
	}
	var all []VILookupResult
	// NOTE-VI-045 (#429): span identity is (SourceRef, BlockPage, RowIdx, TraceID); the
	// dedup key packs SourceRef in front of the 22-byte span key so results from distinct
	// files sharing a page number do not collide.
	seen := make(map[string]struct{})
	for _, byType := range s.data {
		for _, results := range byType {
			for _, r := range results {
				k := viSpanKey(r)
				dk := r.SourceRef + string(k[:])
				if _, dup := seen[dk]; dup {
					continue
				}
				seen[dk] = struct{}{}
				all = append(all, r)
			}
		}
	}
	return all, true
}
