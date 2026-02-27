// Package blockpack provides a minimal public API for blockpack.
//
// AGENT: This public API must remain minimal and focused. Before adding any new
// public functions, types, or interfaces, you MUST ask the user for explicit
// permission. The design goal is to keep the API surface as small as possible
// and hide all implementation details in internal/ packages.
//
// The public API intentionally exposes ONLY:
//   - Query execution functions (TraceQL filter queries)
//   - Reader interface and basic types
//   - Provider interfaces for storage abstraction
//
// Everything else is internal implementation detail.
package blockpack

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_compaction "github.com/grafana/blockpack/internal/modules/blockio/compaction"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

// Reader reads modules-format blockpack files and provides query execution.
// This is a thin type alias for the internal modules reader.
type Reader = modules_reader.Reader

// Writer encodes OTLP spans into the modules blockpack format.
// This is a thin type alias for the internal modules writer.
type Writer = modules_blockio.Writer

// Block represents a decoded block of spans.
type Block = modules_reader.Block

// Column represents a decoded column.
type Column = modules_reader.Column

// DataType represents the type of data being read for caching optimization.
type DataType = modules_shared.DataType

// DataType constants for read optimization hints.
const (
	DataTypeFooter   = modules_shared.DataTypeFooter
	DataTypeHeader   = modules_shared.DataTypeHeader
	DataTypeMetadata = modules_shared.DataTypeMetadata
	DataTypeBlock    = modules_shared.DataTypeBlock
	DataTypeIndex    = modules_shared.DataTypeIndex
	DataTypeCompact  = modules_shared.DataTypeCompact
)

// ReaderProvider supplies random access to blockpack data.
// Implementations can use files, memory, cloud storage, etc.
type ReaderProvider interface {
	Size() (int64, error)
	ReadAt(p []byte, off int64, dataType DataType) (int, error)
}

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
type CloseableReaderProvider interface {
	ReaderProvider
	Close() error
}

// AGENT: Reader constructors - minimal set needed for creating readers.

// NewReaderFromProvider creates a modules-format reader from a ReaderProvider.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	wrappedProvider := &readerProviderAdapter{provider: provider}
	return modules_reader.NewReaderFromProvider(wrappedProvider)
}

// NewLeanReaderFromProvider creates a lean Reader using only 2 I/Os (footer + compact
// trace index). Ideal for GetTraceByID workloads. Falls back to NewReaderFromProvider
// for files without a compact trace index (v3 footer).
func NewLeanReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	wrapped := &readerProviderAdapter{provider: provider}
	return modules_reader.NewLeanReaderFromProvider(wrapped)
}

// GetTraceByID looks up all spans for the given trace ID and calls fn for each.
// traceIDHex must be a 32-character lowercase hex string (16 bytes).
// Returns nil if the trace is not found. fn may return false to stop early.
// Use NewLeanReaderFromProvider for the lowest-I/O path.
func GetTraceByID(r *Reader, traceIDHex string, fn SpanMatchCallback) error {
	if r == nil {
		return fmt.Errorf("GetTraceByID: reader cannot be nil")
	}

	if len(traceIDHex) != 32 {
		return fmt.Errorf("GetTraceByID: traceIDHex must be 32 hex chars, got %d", len(traceIDHex))
	}

	traceIDBytes, err := hex.DecodeString(traceIDHex)
	if err != nil {
		return fmt.Errorf("GetTraceByID: invalid trace ID hex: %w", err)
	}

	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	entries := r.TraceEntries(traceID)
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		bwb, blockErr := r.GetBlockWithBytes(entry.BlockID, nil, nil)
		if blockErr != nil {
			return fmt.Errorf("GetTraceByID: block %d: %w", entry.BlockID, blockErr)
		}

		for _, rowIdx := range entry.SpanIndices {
			fields := modules_blockio.NewSpanFieldsAdapter(bwb.Block, int(rowIdx))
			traceIDStr, spanIDStr := extractIDs(bwb.Block, int(rowIdx))
			match := &SpanMatch{
				Fields:  fields,
				TraceID: traceIDStr,
				SpanID:  spanIDStr,
			}
			if !fn(match) {
				return nil
			}
		}
	}

	return nil
}

// readerProviderAdapter adapts the public ReaderProvider to modules_shared.ReaderProvider.
type readerProviderAdapter struct {
	provider ReaderProvider
}

func (a *readerProviderAdapter) Size() (int64, error) {
	return a.provider.Size()
}

func (a *readerProviderAdapter) ReadAt(p []byte, off int64, dataType modules_shared.DataType) (int, error) {
	return a.provider.ReadAt(p, off, dataType)
}

// AGENT: Writer constructors - minimal set needed for creating writers.

// NewWriter creates a streaming modules-format blockpack writer that writes to output.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func NewWriter(output io.Writer, maxSpansPerBlock int) (*Writer, error) {
	return modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  output,
		MaxBlockSpans: maxSpansPerBlock,
	})
}

// AGENT: Query execution - this is the main public API for querying.
// Keep this minimal - just TraceQL filter query function.

// SpanFieldsProvider gives access to all attributes for a single span row.
// Use GetField for known attribute names, IterateFields to enumerate all.
type SpanFieldsProvider = modules_shared.SpanFieldsProvider

// QueryOptions configures query execution.
type QueryOptions struct {
	Limit int // Maximum number of spans to return (0 = unlimited).
}

// SpanMatch represents a single span that matched the query.
// Fields is valid only for the duration of the SpanMatchCallback call;
// call Clone() to retain ownership beyond the callback.
type SpanMatch struct {
	Fields  SpanFieldsProvider
	TraceID string
	SpanID  string
}

// Clone materializes all fields from the lazy provider and returns a deep copy
// of the match that is safe to hold after the SpanMatchCallback returns.
func (m *SpanMatch) Clone() SpanMatch {
	out := SpanMatch{TraceID: m.TraceID, SpanID: m.SpanID}
	materialized := make(map[string]any)
	m.Fields.IterateFields(func(name string, value any) bool {
		materialized[name] = value
		return true
	})
	out.Fields = &materializedSpanFields{fields: materialized}
	return out
}

// materializedSpanFields is a heap-allocated map-backed SpanFieldsProvider
// returned by SpanMatch.Clone(). Safe to hold beyond the callback lifetime.
type materializedSpanFields struct {
	fields map[string]any
}

func (m *materializedSpanFields) GetField(name string) (any, bool) {
	v, ok := m.fields[name]
	return v, ok
}

func (m *materializedSpanFields) IterateFields(fn func(name string, value any) bool) {
	for k, v := range m.fields {
		if !fn(k, v) {
			return
		}
	}
}

// SpanMatchCallback is called for each matching span in a streaming query.
// match is only valid for the duration of this call; call match.Clone() to retain ownership.
// Return false to stop iteration early.
type SpanMatchCallback func(match *SpanMatch) bool

// StreamTraceQL executes a TraceQL filter query against a modules-format blockpack file
// and calls fn for each matching span.
//
// Only TraceQL filter expressions are supported (e.g., `{ span.http.method = "GET" }`).
// Structural TraceQL (>>, >, ~, <<, <, !~) and metrics queries return an error.
//
// match.Fields implements GetField(name) and IterateFields backed by the modules block.
// fn may return false to stop iteration early.
func StreamTraceQL(r *Reader, traceqlQuery string, opts QueryOptions, fn SpanMatchCallback) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in StreamTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return fmt.Errorf("StreamTraceQL: reader cannot be nil")
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return fmt.Errorf("parse TraceQL: %w", parseErr)
	}

	switch q := parsed.(type) {
	case *traceqlparser.FilterExpression:
		return streamFilterQuery(r, q, opts, fn)
	case *traceqlparser.StructuralQuery:
		return streamStructuralQuery(r, q, opts, fn)
	default:
		return fmt.Errorf(
			"StreamTraceQL: query type %T is not supported; only filter and structural expressions are supported",
			parsed,
		)
	}
}

// streamFilterQuery executes a TraceQL filter query against a modules-format reader.
func streamFilterQuery(r *Reader, filterExpr *traceqlparser.FilterExpression, opts QueryOptions, fn SpanMatchCallback) error {
	program, compileErr := vm.CompileTraceQLFilter(filterExpr)
	if compileErr != nil {
		return fmt.Errorf("compile TraceQL filter: %w", compileErr)
	}

	return streamFilterProgram(r, program, opts, fn)
}

// streamFilterProgram executes a compiled filter program against a modules-format reader.
func streamFilterProgram(r *Reader, program *vm.Program, opts QueryOptions, fn SpanMatchCallback) error {
	planner := modules_queryplanner.NewPlanner(r)
	predicates := modules_executor.BuildPredicates(r, program)
	plan := planner.Plan(predicates)

	if len(plan.SelectedBlocks) == 0 {
		return nil
	}

	rawBlocks, fetchErr := planner.FetchBlocks(plan)
	if fetchErr != nil {
		return fmt.Errorf("FetchBlocks: %w", fetchErr)
	}

	matched := 0
	for _, blockIdx := range plan.SelectedBlocks {
		raw, rawOK := rawBlocks[blockIdx]
		if !rawOK {
			continue
		}

		meta := r.BlockMeta(blockIdx)
		bwb, blockParseErr := r.ParseBlockFromBytes(raw, nil, meta)
		if blockParseErr != nil {
			return fmt.Errorf("ParseBlockFromBytes block %d: %w", blockIdx, blockParseErr)
		}

		colProvider := modules_executor.NewColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(colProvider)
		if evalErr != nil {
			return fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		for _, rowIdx := range rowSet.ToSlice() {
			fields := modules_blockio.NewSpanFieldsAdapter(bwb.Block, rowIdx)
			traceIDHex, spanIDHex := extractIDs(bwb.Block, rowIdx)
			match := &SpanMatch{
				Fields:  fields,
				TraceID: traceIDHex,
				SpanID:  spanIDHex,
			}
			if !fn(match) {
				return nil
			}
			matched++
			if opts.Limit > 0 && matched >= opts.Limit {
				return nil
			}
		}
	}

	return nil
}

// extractIDs extracts hex-encoded trace ID and span ID strings from a block row.
func extractIDs(block *modules_reader.Block, rowIdx int) (traceID, spanID string) {
	if col := block.GetColumn("trace:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			traceID = fmt.Sprintf("%x", v)
		}
	}
	if col := block.GetColumn("span:id"); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			spanID = fmt.Sprintf("%x", v)
		}
	}
	return traceID, spanID
}

// structSpanRec records per-span data collected during a structural query scan.
type structSpanRec struct {
	spanID     []byte
	parentID   []byte // nil after phase 2
	parentIdx  int    // -1 = root; set during phase 2
	leftMatch  bool
	rightMatch bool
}

// streamStructuralQuery executes a structural TraceQL query against all blocks.
// It collects span records in phase 1, resolves parent indices in phase 2,
// and evaluates the structural operator per trace in phase 3.
func streamStructuralQuery(r *Reader, q *traceqlparser.StructuralQuery, opts QueryOptions, fn SpanMatchCallback) error {
	if r == nil {
		return fmt.Errorf("streamStructuralQuery: reader cannot be nil")
	}

	leftProg, rightProg, err := compileStructuralPrograms(q)
	if err != nil {
		return err
	}

	traceSpans, err := collectStructuralSpans(r, leftProg, rightProg)
	if err != nil {
		return err
	}

	resolveParentIndices(traceSpans)
	return emitStructuralMatches(traceSpans, q.Op, opts, fn)
}

// compileStructuralPrograms compiles left and right filter expressions.
// A nil FilterExpression compiles to a nil program (matches all rows).
func compileStructuralPrograms(q *traceqlparser.StructuralQuery) (left, right *vm.Program, err error) {
	if q.Left != nil {
		left, err = vm.CompileTraceQLFilter(q.Left)
		if err != nil {
			return nil, nil, fmt.Errorf("compile structural left filter: %w", err)
		}
	}
	if q.Right != nil {
		right, err = vm.CompileTraceQLFilter(q.Right)
		if err != nil {
			return nil, nil, fmt.Errorf("compile structural right filter: %w", err)
		}
	}
	return left, right, nil
}

// collectStructuralSpans scans all blocks and accumulates per-trace span records.
// Returns a map: traceIDHex → []structSpanRec (in block scan order).
func collectStructuralSpans(r *Reader, leftProg, rightProg *vm.Program) (map[string][]structSpanRec, error) {
	planner := modules_queryplanner.NewPlanner(r)
	plan := planner.Plan(nil) // structural queries need all blocks

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	rawBlocks, fetchErr := planner.FetchBlocks(plan)
	if fetchErr != nil {
		return nil, fmt.Errorf("structural FetchBlocks: %w", fetchErr)
	}

	result := make(map[string][]structSpanRec)
	for _, blockIdx := range plan.SelectedBlocks {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			continue
		}
		if err := collectBlockStructuralSpans(r, blockIdx, raw, leftProg, rightProg, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// collectBlockStructuralSpans parses one block and appends span records to result.
func collectBlockStructuralSpans(
	r *Reader,
	blockIdx int,
	raw []byte,
	leftProg, rightProg *vm.Program,
	result map[string][]structSpanRec,
) error {
	meta := r.BlockMeta(blockIdx)
	bwb, err := r.ParseBlockFromBytes(raw, nil, meta)
	if err != nil {
		return fmt.Errorf("structural ParseBlockFromBytes block %d: %w", blockIdx, err)
	}

	colProvider := modules_executor.NewColumnProvider(bwb.Block)
	leftSet, err := evalOptionalProgram(leftProg, colProvider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural left ColumnPredicate block %d: %w", blockIdx, err)
	}
	rightSet, err := evalOptionalProgram(rightProg, colProvider, bwb.Block.SpanCount())
	if err != nil {
		return fmt.Errorf("structural right ColumnPredicate block %d: %w", blockIdx, err)
	}

	appendBlockSpanRecs(bwb.Block, leftSet, rightSet, result)
	return nil
}

// evalOptionalProgram evaluates a compiled program or returns an all-match set if prog is nil.
func evalOptionalProgram(prog *vm.Program, colProvider vm.ColumnDataProvider, spanCount int) (vm.RowSet, error) {
	if prog != nil {
		return prog.ColumnPredicate(colProvider)
	}
	// nil program means "match all rows"
	rs := allRowsSet(spanCount)
	return rs, nil
}

// allRowsSet returns a RowSet containing all row indices [0, n).
type allMatchRowSet struct{ n int }

func allRowsSet(n int) vm.RowSet              { return &allMatchRowSet{n: n} }
func (a *allMatchRowSet) Add(_ int)           {}
func (a *allMatchRowSet) Contains(_ int) bool { return true }
func (a *allMatchRowSet) Size() int           { return a.n }
func (a *allMatchRowSet) IsEmpty() bool       { return a.n == 0 }
func (a *allMatchRowSet) ToSlice() []int {
	s := make([]int, a.n)
	for i := range a.n {
		s[i] = i
	}
	return s
}

// appendBlockSpanRecs appends structSpanRec entries to the result map for every row in the block.
func appendBlockSpanRecs(block *modules_reader.Block, leftSet, rightSet vm.RowSet, result map[string][]structSpanRec) {
	traceCol := block.GetColumn("trace:id")
	spanCol := block.GetColumn("span:id")
	parentCol := block.GetColumn("span:parent_id")

	n := block.SpanCount()
	for rowIdx := range n {
		traceIDHex := bytesColHex(traceCol, rowIdx)
		if traceIDHex == "" {
			continue
		}
		spanIDBytes := bytesColRaw(spanCol, rowIdx)
		parentIDBytes := bytesColRaw(parentCol, rowIdx)

		rec := structSpanRec{
			spanID:     spanIDBytes,
			parentID:   parentIDBytes,
			parentIdx:  -1,
			leftMatch:  leftSet.Contains(rowIdx),
			rightMatch: rightSet.Contains(rowIdx),
		}
		result[traceIDHex] = append(result[traceIDHex], rec)
	}
}

// bytesColHex returns a hex string for the bytes column value at rowIdx, or "" if absent.
func bytesColHex(col *modules_reader.Column, rowIdx int) string {
	if col == nil {
		return ""
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%x", v)
}

// bytesColRaw returns the raw bytes for a bytes column value at rowIdx, or nil if absent.
func bytesColRaw(col *modules_reader.Column, rowIdx int) []byte {
	if col == nil {
		return nil
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok {
		return nil
	}
	// Copy to avoid aliasing into the block's memory.
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// resolveParentIndices walks each trace's span list, builds a spanID→index map,
// and sets parentIdx for each span. After resolution, parentID is cleared.
func resolveParentIndices(traceSpans map[string][]structSpanRec) {
	for traceID := range traceSpans {
		spans := traceSpans[traceID]
		// Build spanID → index map using hex strings as keys.
		byID := make(map[string]int, len(spans))
		for i, sp := range spans {
			if len(sp.spanID) > 0 {
				byID[fmt.Sprintf("%x", sp.spanID)] = i
			}
		}
		// Set parentIdx for each span.
		for i := range spans {
			if len(spans[i].parentID) == 0 {
				spans[i].parentIdx = -1
			} else {
				parentKey := fmt.Sprintf("%x", spans[i].parentID)
				if idx, ok := byID[parentKey]; ok {
					spans[i].parentIdx = idx
				} else {
					spans[i].parentIdx = -1
				}
			}
			spans[i].parentID = nil // clear to release memory
		}
		traceSpans[traceID] = spans
	}
}

// emitStructuralMatches evaluates the structural operator for each trace and
// calls fn for each matching right-side span. Returns early if limit is reached.
func emitStructuralMatches(
	traceSpans map[string][]structSpanRec,
	op traceqlparser.StructuralOp,
	opts QueryOptions,
	fn SpanMatchCallback,
) error {
	matched := 0
	for traceIDHex, spans := range traceSpans {
		rightIndices := evalStructuralOp(spans, op)
		// Deduplicate right indices (keep first occurrence per index).
		seen := make(map[int]bool, len(rightIndices))
		for _, ri := range rightIndices {
			if seen[ri] {
				continue
			}
			seen[ri] = true

			spanIDHex := fmt.Sprintf("%x", spans[ri].spanID)
			match := &SpanMatch{
				TraceID: traceIDHex,
				SpanID:  spanIDHex,
			}
			if !fn(match) {
				return nil
			}
			matched++
			if opts.Limit > 0 && matched >= opts.Limit {
				return nil
			}
		}
	}
	return nil
}

// evalStructuralOp returns the right-side span indices matched by the operator.
func evalStructuralOp(spans []structSpanRec, op traceqlparser.StructuralOp) []int {
	switch op {
	case traceqlparser.OpDescendant:
		return evalOpDescendant(spans)
	case traceqlparser.OpChild:
		return evalOpChild(spans)
	case traceqlparser.OpSibling:
		return evalOpSibling(spans)
	case traceqlparser.OpAncestor:
		return evalOpAncestor(spans)
	case traceqlparser.OpParent:
		return evalOpParent(spans)
	case traceqlparser.OpNotSibling:
		return evalOpNotSibling(spans)
	default:
		return nil
	}
}

// evalOpDescendant: R is a descendant of L (>>) — walk R's ancestor chain.
func evalOpDescendant(spans []structSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		// Walk ancestor chain of R; if any ancestor is leftMatch, emit R.
		cur := r.parentIdx
		for cur >= 0 {
			if spans[cur].leftMatch {
				result = append(result, ri)
				break
			}
			cur = spans[cur].parentIdx
		}
	}
	return result
}

// evalOpChild: R's direct parent is L (>) .
func evalOpChild(spans []structSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch || r.parentIdx < 0 {
			continue
		}
		if spans[r.parentIdx].leftMatch {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpSibling: R shares a parent with a leftMatch span (~), R != L.
func evalOpSibling(spans []structSpanRec) []int {
	// Build set of parentIdx values that have at least one leftMatch child.
	leftParents := make(map[int]bool)
	for _, sp := range spans {
		if sp.leftMatch {
			leftParents[sp.parentIdx] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		if leftParents[r.parentIdx] && !r.leftMatch {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpAncestor: R is an ancestor of L (<<) — walk L's parent chain.
func evalOpAncestor(spans []structSpanRec) []int {
	var result []int
	for _, l := range spans {
		if !l.leftMatch {
			continue
		}
		cur := l.parentIdx
		for cur >= 0 {
			if spans[cur].rightMatch {
				result = append(result, cur)
			}
			cur = spans[cur].parentIdx
		}
	}
	return result
}

// evalOpParent: R is the direct parent of L (<).
func evalOpParent(spans []structSpanRec) []int {
	var result []int
	for _, l := range spans {
		if !l.leftMatch || l.parentIdx < 0 {
			continue
		}
		if spans[l.parentIdx].rightMatch {
			result = append(result, l.parentIdx)
		}
	}
	return result
}

// evalOpNotSibling: R is rightMatch with no leftMatch sibling (!~).
func evalOpNotSibling(spans []structSpanRec) []int {
	// For each parentIdx group, check if any leftMatch exists.
	leftParents := make(map[int]bool)
	for _, sp := range spans {
		if sp.leftMatch {
			leftParents[sp.parentIdx] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if r.rightMatch && !leftParents[r.parentIdx] {
			result = append(result, ri)
		}
	}
	return result
}

// FileLayoutReport is the top-level result of AnalyzeFileLayout.
type FileLayoutReport = modules_reader.FileLayoutReport

// FileLayoutSection describes one contiguous byte range in a blockpack file.
type FileLayoutSection = modules_reader.FileLayoutSection

// AnalyzeFileLayout inspects every byte in a blockpack file and returns a
// JSON-serializable report organized by section. Each entry carries its
// absolute offset, on-disk (compressed) size, column name, column type, and
// encoding kind for column data blobs.
//
// The returned Sections slice is sorted by Offset ascending and satisfies the
// invariant: sum(section.CompressedSize) == FileLayoutReport.FileSize.
func AnalyzeFileLayout(r *Reader) (report *FileLayoutReport, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			report = nil
			err = fmt.Errorf("internal error in AnalyzeFileLayout: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("AnalyzeFileLayout: reader cannot be nil")
	}

	return r.FileLayout()
}

// MetricsResult is the output of ExecuteMetricsTraceQL.
type MetricsResult = modules_executor.MetricsResult

// MetricsRow is one row in a MetricsResult time-series grid.
type MetricsRow = modules_executor.MetricsRow

// ExecuteMetricsTraceQL executes a TraceQL metrics query (count_over_time, avg_over_time, etc.)
// against a modules-format blockpack reader and returns dense time-bucketed results.
//
// startTime and endTime are Unix nanoseconds defining the query window.
// The step size is fixed at 1 minute (60 billion nanoseconds).
//
// Returns a MetricsResult with one MetricsRow per (time-bucket × group-by-key) pair.
// GroupKey[0] is the 0-indexed bucket number; GroupKey[1..] are group-by attribute values.
func ExecuteMetricsTraceQL(r *Reader, traceqlQuery string, startTime, endTime int64) (result *MetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: reader cannot be nil")
	}

	program, querySpec, compileErr := vm.CompileTraceQLMetrics(traceqlQuery, startTime, endTime)
	if compileErr != nil {
		return nil, fmt.Errorf("compile TraceQL metrics: %w", compileErr)
	}

	exec := modules_executor.New()
	return exec.ExecuteMetrics(r, program, querySpec)
}

// AGENT: Block metadata API - provides file-level metadata for query planning.

// BlockMeta contains metadata about a blockpack file for block selection.
// Tempo uses this information to determine which files to query based on time range overlap.
type BlockMeta struct {
	MinStartNanos uint64 // Earliest span start time (unix nanos)
	MaxStartNanos uint64 // Latest span start time (unix nanos)
	TotalSpans    int    // Total number of spans across all blocks
	TotalTraces   int    // Total number of unique trace IDs in the file
	BlockCount    int    // Number of blocks in the file
	Size          int64  // File size in bytes
}

// GetBlockMeta returns metadata about a blockpack file including time range,
// span count, and file size. This is used by Tempo for block selection
// to determine which files to query based on time range overlap.
//
// The returned metadata includes:
//   - MinStartNanos: Earliest span start time in the file (unix nanoseconds)
//   - MaxStartNanos: Latest span start time in the file (unix nanoseconds)
//   - TotalSpans: Total number of spans across all blocks
//   - BlockCount: Number of blocks in the file
//   - Size: File size in bytes
//
// This function reads only the file header and block index metadata (typically <1KB),
// making it suitable for frequent calls during query planning.
func GetBlockMeta(path string, storage Storage) (meta *BlockMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			meta = nil
			err = fmt.Errorf("internal error in GetBlockMeta: %v", r)
		}
	}()

	provider := &storageReaderProvider{storage: storage, path: path}
	r, readerErr := modules_reader.NewReaderFromProvider(provider)
	if readerErr != nil {
		return nil, fmt.Errorf("open blockpack: %w", readerErr)
	}

	size, sizeErr := storage.Size(path)
	if sizeErr != nil {
		return nil, fmt.Errorf("get file size: %w", sizeErr)
	}

	minStart := ^uint64(0)
	var maxStart uint64
	var totalSpans int
	for i := range r.BlockCount() {
		bm := r.BlockMeta(i)
		if bm.MinStart < minStart {
			minStart = bm.MinStart
		}
		if bm.MaxStart > maxStart {
			maxStart = bm.MaxStart
		}
		totalSpans += int(bm.SpanCount)
	}
	if r.BlockCount() == 0 {
		minStart = 0
	}

	return &BlockMeta{
		MinStartNanos: minStart,
		MaxStartNanos: maxStart,
		TotalSpans:    totalSpans,
		TotalTraces:   r.TraceCount(),
		BlockCount:    r.BlockCount(),
		Size:          size,
	}, nil
}

// AGENT: Conversion functions - convert from other formats into blockpack.

// ConvertProtoToBlockpack reads an OTLP protobuf-encoded TracesData file and writes
// blockpack-formatted trace data to output.
// The input file must contain a single wire-encoded tracev1.TracesData protobuf message.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func ConvertProtoToBlockpack(inputPath string, output io.Writer, maxSpansPerBlock int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error in ConvertProtoToBlockpack: %v", r)
		}
	}()
	return otlpconvert.ConvertFromProtoFile(inputPath, output, maxSpansPerBlock)
}

// AGENT: Storage interfaces - minimal abstraction for storage backends.

// Storage provides access to blockpack files for query execution.
// It exposes object storage primitives: Size and ReadAt.
type Storage interface {
	// Size returns the total size of the object at the given path.
	Size(path string) (int64, error)

	// ReadAt reads len(p) bytes from the object at path starting at offset off.
	//
	// The dataType parameter provides a hint about the type of data being read
	// (footer, header, metadata, block, index, or compact). Storage implementations
	// can use this hint to optimize caching strategies, TTLs, or read-ahead behavior.
	// Simple implementations that don't perform caching can ignore this parameter.
	//
	// Implementations MUST follow io.ReaderAt semantics for correct interoperability:
	//   - off must be non-negative. Return an error (e.g., os.ErrInvalid) if off < 0.
	//   - If off >= Size(path), ReadAt must return (0, io.EOF).
	//   - Any short read (where n < len(p)) MUST return a non-nil error. At end
	//     of file that error MUST be io.EOF.
	//   - ReadAt may return (len(p), nil) only when the buffer is completely filled.
	//
	// It returns the number of bytes read and any error encountered.
	ReadAt(path string, p []byte, off int64, dataType DataType) (int, error)
}

// WritableStorage extends Storage with write and delete capability.
type WritableStorage interface {
	Storage
	// Put writes data to the given path, creating or overwriting the file.
	Put(path string, data []byte) error
	// Delete removes the file at the given path.
	Delete(path string) error
}

// storageReaderProvider adapts a Storage + path to modules_shared.ReaderProvider.
type storageReaderProvider struct {
	storage Storage
	path    string
}

func (p *storageReaderProvider) Size() (int64, error) {
	return p.storage.Size(p.path)
}

func (p *storageReaderProvider) ReadAt(buf []byte, off int64, dataType modules_shared.DataType) (int, error) {
	return p.storage.ReadAt(p.path, buf, off, dataType)
}

// NewFileStorage creates a filesystem-based WritableStorage rooted at baseDir.
func NewFileStorage(baseDir string) WritableStorage {
	return &folderStorageWrapper{baseDir: baseDir}
}

// folderStorageWrapper implements WritableStorage using the local filesystem.
type folderStorageWrapper struct {
	baseDir string
}

func (w *folderStorageWrapper) Size(path string) (int64, error) {
	fi, err := os.Stat(filepath.Join(w.baseDir, path))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (w *folderStorageWrapper) ReadAt(path string, p []byte, off int64, _ DataType) (int, error) {
	f, err := os.Open(filepath.Join(w.baseDir, path)) //nolint:gosec // path is joined with baseDir
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	return f.ReadAt(p, off)
}

func (w *folderStorageWrapper) Put(path string, data []byte) error {
	full := filepath.Join(w.baseDir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o600)
}

func (w *folderStorageWrapper) Delete(path string) error {
	return os.RemoveAll(filepath.Join(w.baseDir, path))
}

// AGENT: Compaction - merge and deduplicate multiple blockpack files.

// CompactionConfig configures a CompactBlocks operation.
type CompactionConfig = modules_compaction.Config

// CompactBlocks reads spans from multiple blockpack providers, deduplicates them,
// and writes compacted output files to output.
//
// providers are the blockpack sources to merge; they are read sequentially.
// cfg controls staging directory, output file size limits, and spans per block.
// output receives the compacted files via its Put method.
//
// Returns the relative paths of all output files written to output.
func CompactBlocks(
	ctx context.Context,
	providers []ReaderProvider,
	cfg CompactionConfig,
	output WritableStorage,
) ([]string, error) {
	converted := make([]modules_shared.ReaderProvider, len(providers))
	for i, p := range providers {
		converted[i] = p
	}
	return modules_compaction.CompactBlocks(ctx, converted, cfg, output)
}
