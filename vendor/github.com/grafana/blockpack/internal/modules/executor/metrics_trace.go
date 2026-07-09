package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
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
		return &TraceMetricsResult{}, true, nil
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
		Series: []TraceTimeSeries{{Values: values}},
	}
	return result, true, nil
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
	return viEvalNodes(source, preds.Nodes, false)
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
func viEvalNodes(source ValueIndexSource, nodes []vm.RangeNode, isOR bool) ([]VILookupResult, bool) {
	var acc []VILookupResult
	first := true
	for i := range nodes {
		set, ok := viEvalNode(source, &nodes[i])
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
func viEvalNode(source ValueIndexSource, node *vm.RangeNode) ([]VILookupResult, bool) {
	if len(node.Children) > 0 {
		return viEvalNodes(source, node.Children, node.IsOR)
	}
	if node.Column == "" {
		return nil, false
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
	// stats records the build-time I/O so the querier can report it (issue #465).
	stats ValueIndexBuildStats
	mu    sync.Mutex
}

// NewSliceValueIndexSource builds an empty source. Use Add to populate per-column
// results.
func NewSliceValueIndexSource() *SliceValueIndexSource {
	return &SliceValueIndexSource{
		data: make(map[string]map[modules_shared.ColumnType][]VILookupResult),
	}
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
