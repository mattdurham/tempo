package executor

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/mattdurham/blockpack/internal/arena"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/sql"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

const maxNestedBlockpackDepth = 20

// defaultScanFallbackThreshold is the default byte threshold above which blockpack falls back
// to a full sequential scan instead of many scattered indexed reads.
// At 20ms per I/O op, reading 150MB in scattered 80KB reads = ~1,875 ops × 20ms = ~37.5s.
// A sequential 150MB read requires far fewer I/O ops and is orders of magnitude faster on S3.
const defaultScanFallbackThreshold int64 = 150 * 1024 * 1024 // 150 MB

// QueryOptions defines execution hints for blockpack queries.
type QueryOptions struct {
	MetricsCallback       func(bytesInspected int64, blocksScanned int) // Called during query execution with running totals
	Limit                 int
	StartTime             int64         // Unix nanoseconds
	EndTime               int64         // Unix nanoseconds
	IOLatency             time.Duration // Artificial I/O latency for benchmarking (simulates S3 first-byte latency)
	CoalescedReadSize     uint64        // Max bytes per coalesced read (0 = default 4MB). Reduces IO ops by reading adjacent blocks together.
	ScanFallbackThreshold int64         // Max bytes to read via indexed path before falling back to full sequential scan (0 = default 150MiB, negative = disabled).
	MaterializeAllFields  bool          // If true, materialize all columns regardless of WHERE clause
	DetailedIOLogging     bool          // If true, enable detailed IO tracking for analysis (use with IOLatency for realistic benchmarks)
}

// SpanFieldsProvider is an interface for accessing span fields.
// Implementations can be backed by blockpack storage or maps.
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}

// BlockpackSpanMatch represents a matching span with field access.
type BlockpackSpanMatch struct {
	Fields  SpanFieldsProvider // Field accessor (either lazy or materialized)
	TraceID string
	SpanID  string
}

// BlockPruningReason describes why a block was pruned
type BlockPruningReason string

const (
	// PruneTimeRangeBeforeQuery indicates the block was pruned because its MaxStart is before the query start time.
	PruneTimeRangeBeforeQuery BlockPruningReason = "time_range_before_query" // Block's MaxStart < QueryStartTime
	// PruneTimeRangeAfterQuery indicates the block was pruned because its MinStart is after the query end time.
	PruneTimeRangeAfterQuery BlockPruningReason = "time_range_after_query"
	// PruneColumnBloomMiss indicates the block was pruned because the column name is not in the bloom filter.
	PruneColumnBloomMiss BlockPruningReason = "column_bloom_miss"
	// PruneValueMinMaxMiss indicates the block was pruned because the value is outside the min/max range.
	PruneValueMinMaxMiss BlockPruningReason = "value_min_max_miss"
	// PruneDedicatedIndexMiss indicates the block was pruned because the value is not in the dedicated index.
	PruneDedicatedIndexMiss BlockPruningReason = "dedicated_index_miss"
	// PruneBlockTimeMetadata indicates the block was pruned because time metadata doesn't overlap the query.
	PruneBlockTimeMetadata BlockPruningReason = "block_time_metadata"
	// PruneBlockValuePredicate indicates the block was pruned because it doesn't match value predicates.
	PruneBlockValuePredicate BlockPruningReason = "block_value_predicate"
)

// BlockPruningDecision records why a block was included or pruned
type BlockPruningDecision struct {
	PrunedReason BlockPruningReason // Why the block was pruned (empty if included)
	BlockID      int                // Block index
	SpanCount    int                // Number of spans in this block
	Included     bool               // True if block was scanned, false if pruned
}

// PruningStageStats tracks blocks after each pruning stage
type PruningStageStats struct {
	StageName       string // e.g., "time_range", "column_bloom", "dedicated_index"
	BlocksRemaining int    // Blocks remaining after this stage
	BlocksPruned    int    // Blocks pruned in this stage
}

// ExecutionSkip records blocks that were selected but skipped during execution
type ExecutionSkip struct {
	SkipReason BlockPruningReason // Why block was skipped at execution time
	BlockID    int                // Block index
	SpanCount  int                // Number of spans in block
	BytesRead  int64              // Bytes read from disk for this block
}

// ScannedBlock records blocks that were actually scanned with their results
type ScannedBlock struct {
	BlockID    int   // Block index
	SpanCount  int   // Number of spans in block
	BytesRead  int64 // Bytes read from disk for this block
	MatchCount int   // Number of spans that matched predicates
}

// QueryPlan provides insight into query execution and block selection
type QueryPlan struct {
	// Enhanced query planning fields
	DecisionTree   *DecisionTreeNode      // Hierarchical decision tree
	PruningStages  []PruningStageStats    // Stats for each pruning stage
	BlockDecisions []BlockPruningDecision // Per-block pruning decisions
	ExecutionSkips []ExecutionSkip        // Blocks skipped during execution (after selection)
	ScannedBlocks  []ScannedBlock         // Blocks that were actually scanned with results

	Explanations      []Explanation      // Human-readable explanations
	OptimizationHints []OptimizationHint // Optimization suggestions
	PredicatesSummary PredicateSummary   // Summary of predicates used
	TotalBlocks       int                // Total blocks in file
	BlocksSelected    int                // Blocks selected for scanning
	BlocksPruned      int                // Blocks pruned (not scanned)
}

// copyQueryPlanFromArena copies QueryPlan slices from arena memory to heap.
// This is necessary because arena memory becomes invalid after arena.Free().
func copyQueryPlanFromArena(plan *QueryPlan) *QueryPlan {
	if plan == nil {
		return nil
	}

	// Copy PruningStages slice
	stages := make([]PruningStageStats, len(plan.PruningStages))
	copy(stages, plan.PruningStages)

	// Copy BlockDecisions slice
	decisions := make([]BlockPruningDecision, len(plan.BlockDecisions))
	copy(decisions, plan.BlockDecisions)

	// Copy top-level struct and replace slices with heap copies
	copied := *plan
	copied.PruningStages = stages
	copied.BlockDecisions = decisions
	// ExecutionSkips and ScannedBlocks will be set later, no need to copy yet
	copied.ExecutionSkips = nil
	copied.ScannedBlocks = nil

	return &copied
}

// PredicateSummary describes the predicates used for block selection
type PredicateSummary struct {
	ColumnFilters     []string `json:"columnFilters"`     // Column names with equality filters
	DedicatedColumns  []string `json:"dedicatedColumns"`  // Dedicated columns used for pruning
	HasTimeFilter     bool     `json:"hasTimeFilter"`     // Query has time range filter
	HasColumnFilters  bool     `json:"hasColumnFilters"`  // Query has column equality filters
	HasDedicatedIndex bool     `json:"hasDedicatedIndex"` // Query uses dedicated column indexes
	HasOROperations   bool     `json:"hasOROperations"`   // Query has OR operations
}

// BlockpackResult is the output of a blockpack query.
//
// Memory Management: Results currently use materialized SpanFields for performance
// (typed fields avoid boxing). LazySpanFields is available for future optimization
// when memory/latency tradeoffs favor on-demand field access. Call Release() after
// serialization to allow GC of any lazy field references.
type BlockpackResult struct {
	QueryPlan       *QueryPlan           // Query execution plan with block selection details
	DetailedIOStats *blockpackio.IOStats // Detailed IO analysis (only populated if DetailedIOLogging=true)
	IOAnalysisText  string               // Human-readable IO analysis (only populated if DetailedIOLogging=true)
	Matches         []BlockpackSpanMatch
	AggregateRows   []AggregateRow // Results for aggregate queries
	BlocksScanned   int
	BytesRead       int64 // Total bytes read from disk for scanned blocks
	IOOperations    int64 // Number of I/O operations (ReadAt calls) - critical for object storage
	IsAggregated    bool  // True if this is an aggregate query result
}

// Release clears references in all LazySpanFields to allow garbage collection.
// Call this after query results are serialized and no longer needed.
// Safe to call multiple times and on results with materialized fields.
func (r *BlockpackResult) Release() {
	if r == nil {
		return
	}
	for i := range r.Matches {
		if lsf, ok := r.Matches[i].Fields.(*LazySpanFields); ok {
			lsf.Release()
		}
	}
}

// AggregateRow represents a single row in an aggregated query result.
type AggregateRow struct {
	Values   map[string]float64 // Aggregated values keyed by aggregate function name
	GroupKey []string           // Values for GROUP BY columns (empty for ungrouped aggregates)
}

// Pool for reusing LazySpanFields to reduce allocations
// materializeFields extracts needed fields from a block row into a map.
// This allows the block to be garbage collected immediately instead of being
// held in memory by LazySpanFields references.
// getColumnValue extracts a typed value from a column at a specific row index
func getColumnValue(col *blockpack.Column, rowIdx int) (value any, hasValue bool) {
	if col == nil {
		return nil, false
	}
	switch col.Type {
	case blockpack.ColumnTypeString:
		value, hasValue = col.StringValue(rowIdx)
	case blockpack.ColumnTypeInt64:
		value, hasValue = col.Int64Value(rowIdx)
	case blockpack.ColumnTypeUint64:
		value, hasValue = col.Uint64Value(rowIdx)
	case blockpack.ColumnTypeBool:
		value, hasValue = col.BoolValue(rowIdx)
	case blockpack.ColumnTypeFloat64:
		value, hasValue = col.Float64Value(rowIdx)
	case blockpack.ColumnTypeBytes:
		var v []byte
		v, hasValue = col.BytesValueView(rowIdx)
		if hasValue {
			// Copy bytes out of arena before arena.Free()
			vCopy := make([]byte, len(v))
			copy(vCopy, v)
			value = vCopy
		}
	}
	return value, hasValue
}

// materializeBytesField extracts bytes value with copying and optional ID encoding
func materializeBytesField(col *blockpack.Column, rowIdx int, encodeAsID bool) (string, bool) {
	// Validate column type to prevent undefined behavior
	if col == nil || col.Type != blockpack.ColumnTypeBytes {
		return "", false
	}
	if v, ok := col.BytesValueView(rowIdx); ok {
		vCopy := make([]byte, len(v))
		copy(vCopy, v)
		if encodeAsID {
			return encodeIDCached(vCopy), true
		}
		return string(vCopy), true
	}
	return "", false
}

// materializeIntrinsicFields extracts all intrinsic fields from a block into SpanFields
func materializeIntrinsicFields(block *blockpackio.Block, rowIdx int, fields *SpanFields) {
	// trace:id (bytes -> string)
	if col := block.GetColumn(IntrinsicTraceID); col != nil {
		if id, ok := materializeBytesField(col, rowIdx, true); ok {
			fields.TraceID = id
			fields.HasFields |= HasTraceID
		}
	}

	// span:id (bytes -> string)
	if col := block.GetColumn(IntrinsicSpanID); col != nil {
		if id, ok := materializeBytesField(col, rowIdx, true); ok {
			fields.SpanID = id
			fields.HasFields |= HasSpanID
		}
	}

	// span:parent_id (bytes -> string)
	if col := block.GetColumn("span:parent_id"); col != nil {
		if id, ok := materializeBytesField(col, rowIdx, true); ok {
			fields.ParentSpanID = id
			fields.HasFields |= HasParentSpanID
		}
	}

	// span:name (string)
	if col := block.GetColumn("span:name"); col != nil {
		if v, ok := col.StringValue(rowIdx); ok {
			fields.Name = v
			fields.HasFields |= HasName
		}
	}

	// span:start (uint64)
	if col := block.GetColumn("span:start"); col != nil {
		if v, ok := col.Uint64Value(rowIdx); ok {
			fields.StartTime = v
			fields.HasFields |= HasStartTime
		}
	}

	// span:end (uint64)
	if col := block.GetColumn("span:end"); col != nil {
		if v, ok := col.Uint64Value(rowIdx); ok {
			fields.EndTime = v
			fields.HasFields |= HasEndTime
		}
	}

	// span:duration (uint64)
	if col := block.GetColumn(IntrinsicSpanDuration); col != nil {
		if v, ok := col.Uint64Value(rowIdx); ok {
			fields.Duration = v
			fields.HasFields |= HasDuration
		}
	} else {
		// If span:duration column doesn't exist, compute from span:end - span:start
		if (fields.HasFields&HasStartTime) != 0 && (fields.HasFields&HasEndTime) != 0 {
			fields.Duration = fields.EndTime - fields.StartTime
			fields.HasFields |= HasDuration
		}
	}

	// span:status (int64)
	if col := block.GetColumn("span:status"); col != nil {
		if v, ok := col.Int64Value(rowIdx); ok {
			fields.Status = v
			fields.HasFields |= HasStatus
		}
	}

	// span:kind (int64)
	if col := block.GetColumn("span:kind"); col != nil {
		if v, ok := col.Int64Value(rowIdx); ok {
			fields.Kind = v
			fields.HasFields |= HasKind
		}
	}

	// span:status_message (string)
	if col := block.GetColumn("span:status_message"); col != nil {
		if v, ok := col.StringValue(rowIdx); ok {
			fields.StatusMessage = v
			fields.HasFields |= HasStatusMessage
		}
	}

	// resource.service.name (string) - resource attributes use dots, not colons
	if col := block.GetColumn("resource.service.name"); col != nil {
		if v, ok := col.StringValue(rowIdx); ok {
			fields.ResourceService = v
			fields.HasFields |= HasResourceService
		}
	}
}

// materializeProjectedAttributes extracts specific attributes from a block
func materializeProjectedAttributes(
	block *blockpackio.Block,
	rowIdx int,
	attributesToProject []string,
	fields *SpanFields,
) {
	for _, attrName := range attributesToProject {
		// Get the canonical column name for this attribute (used as key in fields map)
		canonicalColumn := attributePathToColumnName(attrName)

		// For unscoped attributes, check all possible column names to find the value
		possibleColumns := GetPossibleColumnNames(attrName)

		for _, columnName := range possibleColumns {
			col := block.GetColumn(columnName)
			if col == nil {
				continue
			}

			value, hasValue := getColumnValue(col, rowIdx)
			if hasValue {
				// Store with canonical column name so it matches attrColumns in test helpers
				fields.Attributes[canonicalColumn] = value
				break // Found value in this scope, no need to check others
			}
		}
	}
}

// materializeAllAttributes extracts all non-intrinsic attributes from a block
func materializeAllAttributes(block *blockpackio.Block, rowIdx int, fields *SpanFields) {
	for name, col := range block.Columns() {
		// Skip if already materialized as intrinsic (intrinsics are in struct fields)
		if isIntrinsicColumn(name) {
			continue
		}

		// Handle special case for ID encoding in bytes columns
		if col.Type == blockpack.ColumnTypeBytes {
			var v []byte
			var hasValue bool
			v, hasValue = col.BytesValueView(rowIdx)
			if hasValue {
				vCopy := make([]byte, len(v))
				copy(vCopy, v)
				if name == IntrinsicTraceID || name == IntrinsicSpanID {
					fields.Attributes[name] = encodeID(vCopy)
				} else {
					fields.Attributes[name] = vCopy
				}
			}
		} else {
			value, hasValue := getColumnValue(col, rowIdx)
			if hasValue {
				fields.Attributes[name] = value
			}
		}
	}
}

func materializeFields(
	block *blockpackio.Block,
	rowIdx int,
	attributesToProject []string,
	materializeAll bool,
) *SpanFields {
	// Create typed fields struct - intrinsics stored as typed fields (no boxing!)
	fields := &SpanFields{
		Attributes: make(map[string]any, len(attributesToProject)),
	}

	// Materialize intrinsic fields directly into typed struct fields (NO BOXING!)
	// This eliminates 13M+ boxing allocations compared to map[string]any approach
	materializeIntrinsicFields(block, rowIdx, fields)

	// Materialize projected or all attributes
	if materializeAll {
		materializeAllAttributes(block, rowIdx, fields)
	} else {
		materializeProjectedAttributes(block, rowIdx, attributesToProject, fields)
	}

	return fields
}

// isIntrinsicColumn checks if a column name is an intrinsic field.
// Uses the canonical IntrinsicFieldNames list defined in intrinsics.go.
func isIntrinsicColumn(name string) bool {
	return IsIntrinsicField(name)
}

// Pool for reusing encoded ID buffers
var idBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32) // 16 bytes * 2 for hex encoding
		return &buf
	},
}

// BlockpackExecutor executes VM programs against blockpack-encoded data.
type BlockpackExecutor struct {
	storage FileStorage
	matcher *QueryMatcher // Optional query matcher for pre-computed metric streams
}

// NewBlockpackExecutor constructs an executor backed by FileStorage.
func NewBlockpackExecutor(storage FileStorage) *BlockpackExecutor {
	return &BlockpackExecutor{storage: storage}
}

// SetQueryMatcher configures the executor to use a QueryMatcher for routing
// queries to pre-computed metric streams.
func (e *BlockpackExecutor) SetQueryMatcher(matcher *QueryMatcher) {
	e.matcher = matcher
}

// extractFilterColumns identifies which columns are needed for filtering (WHERE clause).
// This allows loading only filter columns on first pass, avoiding decompression of irrelevant columns.
func extractFilterColumns(predicates *vm.QueryPredicates, startTime, endTime int64) map[string]struct{} {
	if predicates == nil {
		return nil // Load all columns if no predicates
	}

	columns := make(map[string]struct{})

	// Add columns from equality predicates
	for col := range predicates.AttributeEquals {
		columns[col] = struct{}{}
		// Also add possible variants (span.X could be resource.X, etc.)
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// Add columns from dedicated column predicates
	for col := range predicates.DedicatedColumns {
		columns[col] = struct{}{}
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// Add columns from regex predicates
	for col := range predicates.DedicatedColumnsRegex {
		columns[col] = struct{}{}
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// Add columns from range predicates (e.g., duration > 100ms)
	for col := range predicates.DedicatedRanges {
		columns[col] = struct{}{}
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// Add columns from attribute ranges (e.g., .foo > 100 on non-dedicated columns)
	for col := range predicates.AttributeRanges {
		columns[col] = struct{}{}
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// Add columns accessed for bloom filter checks (includes IS NULL/IS NOT NULL)
	for _, col := range predicates.AttributesAccessed {
		columns[col] = struct{}{}
		for _, variant := range GetPossibleColumnNames(col) {
			columns[variant] = struct{}{}
		}
	}

	// If time filtering is enabled, we need span:start column
	if startTime > 0 || endTime > 0 {
		columns["span:start"] = struct{}{}
	}

	// If duration is needed (in filter or result), ensure we have span:start and span:end
	// so we can compute it if the span:duration column doesn't exist
	_, hasDuration := columns[IntrinsicSpanDuration]
	if hasDuration {
		columns["span:start"] = struct{}{}
		columns["span:end"] = struct{}{}
	}

	// If no columns needed, return nil to load all columns (more efficient than selective reading)
	if len(columns) == 0 {
		return nil
	}

	// ALWAYS include essential intrinsic columns needed for BlockpackSpanMatch construction.
	// These must be loaded even if not referenced in the WHERE clause, because:
	// 1. trace:id and span:id are used to populate BlockpackSpanMatch.TraceID and SpanID fields
	// 2. span:parent_id is needed for structural queries (parent/child relationships)
	// Without these, blockpackAttributeProvider.traceID()/spanID() return nil, causing empty IDs.
	//
	// We add them at the END (after the len==0 check) to preserve the optimization where
	// predicates==nil or no predicates means "load all columns" (return nil).
	columns[IntrinsicTraceID] = struct{}{}
	columns[IntrinsicSpanID] = struct{}{}
	columns["span:parent_id"] = struct{}{}

	return columns
}

// extractResultColumns identifies which columns are needed for materialization (SELECT clause).
// Only these columns (plus filter columns) will be decompressed, never all columns unless explicitly requested.
func extractResultColumns(program *vm.Program, opts QueryOptions) map[string]struct{} {
	// If materialize all is requested, return nil to load everything
	if opts.MaterializeAllFields {
		return nil
	}

	columns := make(map[string]struct{})

	// Always include intrinsics needed for results
	// Uses canonical list from intrinsics.go
	for _, name := range IntrinsicFieldNames {
		columns[name] = struct{}{}
		// Add variants
		for _, variant := range GetPossibleColumnNames(name) {
			columns[variant] = struct{}{}
		}
	}

	// Add projected attributes from program
	for _, attr := range program.Attributes {
		columns[attr] = struct{}{}
		for _, variant := range GetPossibleColumnNames(attr) {
			columns[variant] = struct{}{}
		}
	}

	// If duration is needed, ensure we have span:start and span:end to compute it
	_, hasDuration := columns[IntrinsicSpanDuration]
	if hasDuration {
		columns["span:start"] = struct{}{}
		columns["span:end"] = struct{}{}
	}

	return columns
}

// extractAggregationColumns extracts only the columns needed for aggregation queries
// This is much more selective than extractResultColumns which loads all intrinsics
func extractAggregationColumns(program *vm.Program, opts QueryOptions) map[string]struct{} {
	if program.AggregationPlan == nil {
		return extractResultColumns(program, opts)
	}

	columns := make(map[string]struct{})
	plan := program.AggregationPlan

	// Add GROUP BY columns
	for _, field := range plan.GroupByFields {
		columns[field] = struct{}{}
		for _, variant := range GetPossibleColumnNames(field) {
			columns[variant] = struct{}{}
		}
	}

	// Add aggregate function argument columns
	for _, agg := range plan.Aggregates {
		// COUNT(*) doesn't need any column
		if agg.Field == "" {
			continue
		}
		columns[agg.Field] = struct{}{}
		for _, variant := range GetPossibleColumnNames(agg.Field) {
			columns[variant] = struct{}{}
		}

		// Special case: if field is IntrinsicSpanDuration and it doesn't exist as a stored column,
		// we need span:start and span:end to compute it
		if agg.Field == IntrinsicSpanDuration {
			columns["span:start"] = struct{}{}
			columns["span:end"] = struct{}{}
		}
	}

	// Add span:start for time bucketing if needed
	if opts.StartTime > 0 && opts.EndTime > 0 {
		columns["span:start"] = struct{}{}
	}

	return columns
}

// ExecuteQuery loads the blockpack file at path and executes the VM program.
// If querySpec is provided and a QueryMatcher is configured, the executor will
// attempt to route the query to pre-computed metric streams (fast path).
// If no match is found or querySpec is nil, the query executes using streaming (fallback path).
func (e *BlockpackExecutor) ExecuteQuery(
	path string,
	program *vm.Program,
	querySpec *sql.QuerySpec,
	opts QueryOptions,
) (*BlockpackResult, error) {
	// Input validation
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if program == nil {
		return nil, fmt.Errorf("program cannot be nil")
	}
	if e.storage == nil {
		return nil, fmt.Errorf("executor not properly initialized: storage is nil")
	}
	if opts.Limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative, got %d", opts.Limit)
	}
	// Treat zero limit (default/unbounded) as MaxQueryLimit to ensure all queries have an upper bound
	if opts.Limit == 0 {
		opts.Limit = MaxQueryLimit
	}
	if opts.Limit > MaxQueryLimit {
		return nil, fmt.Errorf("limit %d exceeds maximum allowed %d", opts.Limit, MaxQueryLimit)
	}
	if opts.StartTime > 0 && opts.EndTime > 0 && opts.StartTime > opts.EndTime {
		return nil, fmt.Errorf(
			"invalid time range: startTime (%d) cannot be after endTime (%d)",
			opts.StartTime,
			opts.EndTime,
		)
	}

	if querySpec != nil && e.matcher != nil {
		if result, ok, err := e.tryPrecomputedMetricStreams(querySpec, opts); ok {
			return result, err
		}
	}
	if querySpec != nil && e.matcher == nil {
		if result, ok, err := e.tryInlinePrecomputedMetricStreams(path, querySpec, opts); ok {
			return result, err
		}
	}

	if program.AggregationPlan != nil {
		return e.executeAggregationQuery(path, program, opts)
	}

	reader, tracking, closeFn, err := e.buildTrackedReader(path, opts)
	if err != nil {
		return nil, err
	}
	if closeFn != nil {
		defer closeFn()
	}

	return e.executeStreamingQuery(reader, program, opts, tracking)
}

type trackingContext struct {
	detailed *blockpackio.DetailedTrackingReader
	standard *blockpackio.DefaultProvider
}

// queryContext holds shared state for a single query execution.
//
//nolint:govet,betteralign // Struct field order optimized for readability, not memory
type queryContext struct {
	tracking   trackingContext
	reader     *blockpackio.Reader
	program    *vm.Program
	predicates *vm.QueryPredicates
	arena      *arena.Arena
	opts       QueryOptions
	depth      int
}

// scanOutput collects mutable results from block scanning.
//
//nolint:govet,betteralign // Struct field order optimized for readability, not memory
type scanOutput struct {
	executionSkips []ExecutionSkip
	scannedBlocks  []ScannedBlock
	blocksScanned  int
}

// aggContext holds aggregation-specific state.
//
//nolint:govet,betteralign // Struct field order optimized for readability, not memory
type aggContext struct {
	allColumnsNeeded  map[string]struct{}
	stringInterner    *StringInterner
	buckets           map[string]*vm.AggBucket
	timeBucketStrings map[int64]string // pre-computed bucket index -> string to avoid per-row allocation
	groupKeyBuffer    []string
	startTime         int64
	endTime           int64
	stepSizeNs        int64
	useTimeBuckets    bool
}

func (e *BlockpackExecutor) tryPrecomputedMetricStreams(
	querySpec *sql.QuerySpec,
	opts QueryOptions,
) (*BlockpackResult, bool, error) {
	stream := e.matcher.Match(querySpec)
	if stream == nil {
		return nil, false, nil
	}
	result, err := e.readPrecomputedMetricStreams(stream, opts)
	if err != nil {
		if errors.Is(err, errPrecomputedUnsupported) {
			return nil, false, nil
		}
		return nil, true, err
	}
	return result, true, nil
}

func (e *BlockpackExecutor) buildTrackedReader(
	path string,
	opts QueryOptions,
) (*blockpackio.Reader, trackingContext, func(), error) {
	providerStorage, ok := e.storage.(ProviderStorage)
	if !ok {
		return nil, trackingContext{}, nil, fmt.Errorf("storage must implement ProviderStorage interface")
	}

	provider, err := providerStorage.GetProvider(path)
	if err != nil {
		return nil, trackingContext{}, nil, fmt.Errorf("read blockpack file: %w", err)
	}

	var closeFn func()
	if closeable, ok := provider.(blockpackio.CloseableReaderProvider); ok {
		closeFn = func() {
			if err := closeable.Close(); err != nil { //nolint:govet
				log.Printf("failed to close provider for %s: %v", path, err)
			}
		}
	}

	var tracker blockpackio.ReaderProvider
	var ctx trackingContext
	if opts.DetailedIOLogging {
		detailed, err := blockpackio.NewDetailedTrackingReaderWithLatency(provider, opts.IOLatency, true) //nolint:govet
		if err != nil {
			return nil, trackingContext{}, closeFn, fmt.Errorf("create detailed tracking reader: %w", err)
		}
		ctx.detailed = detailed
		tracker = ctx.detailed
	} else {
		var standard *blockpackio.DefaultProvider
		if opts.IOLatency > 0 {
			standard, err = blockpackio.NewDefaultProviderWithLatency(provider, opts.IOLatency)
		} else {
			standard, err = blockpackio.NewDefaultProvider(provider)
		}
		if err != nil {
			return nil, trackingContext{}, closeFn, fmt.Errorf("create default provider: %w", err)
		}
		ctx.standard = standard
		tracker = standard
	}

	reader, err := blockpackio.NewReaderFromProvider(tracker)
	if err != nil {
		return nil, trackingContext{}, closeFn, fmt.Errorf("decode blockpack: %w", err)
	}

	return reader, ctx, closeFn, nil
}

// applyScanFallback replaces blockOrder with all blocks when the estimated read size
// of the indexed selection exceeds the scan fallback threshold.
// This prevents many small scattered reads from being worse than one sequential full scan.
// Returns the (possibly expanded) block order and a bool indicating whether fallback triggered.
//
// When fallback triggers, callers should consider setting filterColumns=nil for single-pass
// column loading, since all blocks will be read regardless.
//
// Note: when falling back, time-range-pruned block list is replaced with the full sequential
// order. Blocks outside the time window are still filtered during scan.
func applyScanFallback(reader *blockpackio.Reader, blockOrder []int, opts QueryOptions) ([]int, bool) {
	threshold := opts.ScanFallbackThreshold
	if threshold < 0 {
		return blockOrder, false // disabled
	}
	if threshold == 0 {
		threshold = defaultScanFallbackThreshold
	}
	var estimated int64
	for _, idx := range blockOrder {
		sz, err := reader.BlockSize(idx)
		if err != nil {
			// BlockSize returns an error only for out-of-range indices, which cannot occur here
			// since blockOrder contains validated indices. Silently skip to avoid over-counting.
			continue
		}
		estimated += sz
		if estimated > threshold {
			// Full sequential scan: build 0..N-1. Nested blockpack entries are included;
			// scanStreamingBlocks handles them correctly via SubReader.
			all := make([]int, reader.BlockCount())
			for i := range all {
				all[i] = i
			}
			return all, true
		}
	}
	return blockOrder, false
}

func (e *BlockpackExecutor) executeStreamingQuery(
	reader *blockpackio.Reader,
	program *vm.Program,
	opts QueryOptions,
	tracking trackingContext,
) (*BlockpackResult, error) {
	var queryArena arena.Arena
	defer queryArena.Free()

	predicates := ensurePredicates(program.Predicates)
	blockOrder, queryPlan := selectBlocksWithPlan(reader, predicates, opts, &queryArena)
	var scanFallbackTriggered bool
	blockOrder, scanFallbackTriggered = applyScanFallback(reader, blockOrder, opts)
	queryPlanCopy := copyQueryPlanFromArena(queryPlan)

	// Collect span-level hints from trace block index for direct row access
	spanHints := collectSpanHints(reader, predicates)

	matches := make([]BlockpackSpanMatch, 0, estimatedMatchCapacity(blockOrder, opts.Limit))

	filterColumns := extractFilterColumns(predicates, opts.StartTime, opts.EndTime)
	resultColumns := extractResultColumns(program, opts)

	// When MaterializeAllFields is set, or scan fallback triggered (all blocks read anyway),
	// skip two-pass column loading and load all columns upfront in a single pass.
	// This eliminates the AddColumnsToBlock re-reads for matching blocks.
	if opts.MaterializeAllFields || scanFallbackTriggered {
		filterColumns = nil
	}

	ctx := &queryContext{
		reader:     reader,
		program:    program,
		predicates: predicates,
		opts:       opts,
		arena:      &queryArena,
		tracking:   tracking,
		depth:      maxNestedBlockpackDepth,
	}
	output := &scanOutput{
		executionSkips: make([]ExecutionSkip, 0, len(blockOrder)),
		scannedBlocks:  make([]ScannedBlock, 0, len(blockOrder)),
	}
	_, err := e.scanStreamingBlocks(ctx, blockOrder, filterColumns, resultColumns, &matches, output, spanHints)
	if err != nil {
		return nil, err
	}

	// Truncate to exact limit if necessary
	if opts.Limit > 0 && len(matches) > opts.Limit {
		matches = matches[:opts.Limit]
	}

	if queryPlanCopy != nil {
		queryPlanCopy.ExecutionSkips = output.executionSkips
		queryPlanCopy.ScannedBlocks = output.scannedBlocks
	}

	stats := collectTrackingStats(tracking)
	return &BlockpackResult{
		Matches:         matches,
		BlocksScanned:   output.blocksScanned,
		BytesRead:       stats.bytesRead,
		IOOperations:    stats.ioOps,
		QueryPlan:       queryPlanCopy,
		DetailedIOStats: stats.detailedStats,
		IOAnalysisText:  stats.analysisText,
	}, nil
}

func (e *BlockpackExecutor) scanStreamingBlocks(
	ctx *queryContext,
	blockOrder []int,
	filterColumns map[string]struct{},
	resultColumns map[string]struct{},
	matches *[]BlockpackSpanMatch,
	output *scanOutput,
	spanHints map[int][]uint16,
) (bool, error) {
	if ctx.depth <= 0 {
		return false, fmt.Errorf("nested blockpack depth exceeds limit %d", maxNestedBlockpackDepth)
	}
	if len(blockOrder) == 0 {
		return false, nil
	}

	var reusableBlock *blockpackio.Block
	coalescedReads := ctx.reader.CoalesceBlocks(blockOrder, ctx.opts.CoalescedReadSize)

	for _, coalescedRead := range coalescedReads {
		blockBytesMap, err := ctx.reader.ReadCoalescedBlocks(coalescedRead)
		if err != nil {
			return false, fmt.Errorf("read coalesced blocks: %w", err)
		}

		for blockIdx := coalescedRead.StartBlockIdx; blockIdx < coalescedRead.EndBlockIdx; blockIdx++ {
			cachedBytes, ok := blockBytesMap[blockIdx]
			if !ok {
				return false, fmt.Errorf("block %d not in coalesced read result", blockIdx)
			}

			if ctx.reader.IsBlockpackEntry(blockIdx) {
				subReader, err := blockpackio.NewReaderFromBytes(cachedBytes)
				if err != nil {
					return false, fmt.Errorf("decode nested blockpack %d: %w", blockIdx, err)
				}
				subOrder, _ := selectBlocksWithPlan(subReader, ctx.predicates, ctx.opts, ctx.arena)
				subSpanHints := collectSpanHints(subReader, ctx.predicates)
				subCtx := *ctx
				subCtx.reader = subReader
				subCtx.depth = ctx.depth - 1
				reached, err := e.scanStreamingBlocks(
					&subCtx,
					subOrder,
					filterColumns,
					resultColumns,
					matches,
					output,
					subSpanHints,
				)
				if err != nil {
					return false, err
				}
				if reached {
					return true, nil
				}
				continue
			}

			blockWithBytes, err := ctx.reader.GetBlockWithBytes(
				blockIdx,
				filterColumns,
				reusableBlock,
				cachedBytes,
				ctx.arena,
			)
			if err != nil {
				return false, fmt.Errorf("get block %d: %w", blockIdx, err)
			}
			block := blockWithBytes.Block
			reusableBlock = block

			if skip, ok := timePruneSkip(block, ctx.opts.StartTime, ctx.opts.EndTime, blockIdx, len(blockWithBytes.RawBytes)); ok {
				output.executionSkips = append(output.executionSkips, skip)
				continue
			}
			if skip, ok := valuePredicateSkip(block, ctx.predicates, blockIdx, len(blockWithBytes.RawBytes)); ok {
				output.executionSkips = append(output.executionSkips, skip)
				continue
			}

			output.blocksScanned++

			// Invoke metrics callback if set
			if ctx.opts.MetricsCallback != nil {
				var currentBytes int64
				if ctx.tracking.detailed != nil {
					currentBytes = ctx.tracking.detailed.BytesRead()
				} else if ctx.tracking.standard != nil {
					currentBytes = ctx.tracking.standard.BytesRead()
				}
				ctx.opts.MetricsCallback(currentBytes, output.blocksScanned)
			}
			matchCountFromBlock, reached, err := e.applyBlockMatches(
				ctx.reader,
				blockWithBytes,
				blockIdx,
				ctx.program,
				ctx.opts,
				filterColumns,
				resultColumns,
				ctx.arena,
				matches,
				spanHints[blockIdx],
			)
			if err != nil {
				return false, err
			}

			output.scannedBlocks = append(output.scannedBlocks, ScannedBlock{
				BlockID:    blockIdx,
				SpanCount:  block.SpanCount(),
				BytesRead:  int64(len(blockWithBytes.RawBytes)),
				MatchCount: matchCountFromBlock,
			})

			if reached {
				return true, nil
			}

			// Early-exit: skip remaining coalesced read groups if limit reached
			if ctx.opts.Limit > 0 && len(*matches) >= ctx.opts.Limit {
				return true, nil
			}
		}
	}

	return false, nil
}

func ensurePredicates(predicates *vm.QueryPredicates) *vm.QueryPredicates {
	if predicates != nil {
		return predicates
	}
	return &vm.QueryPredicates{
		AttributeEquals:       make(map[string][]vm.Value),
		DedicatedColumns:      make(map[string][]vm.Value),
		UnscopedColumnNames:   make(map[string][]string),
		DedicatedColumnsRegex: make(map[string]string),
	}
}

func estimatedMatchCapacity(blockOrder []int, limit int) int {
	estimated := len(blockOrder) * 100
	if limit > 0 && estimated > limit {
		return limit
	}
	return estimated
}

func timePruneSkip(
	block *blockpackio.Block,
	startTime int64,
	endTime int64,
	blockIdx int,
	bytesRead int,
) (ExecutionSkip, bool) {
	if startTime > 0 && block.MaxStart < uint64(startTime) {
		return ExecutionSkip{
			BlockID:    blockIdx,
			SpanCount:  block.SpanCount(),
			BytesRead:  int64(bytesRead),
			SkipReason: PruneBlockTimeMetadata,
		}, true
	}
	if endTime > 0 && block.MinStart > uint64(endTime) {
		return ExecutionSkip{
			BlockID:    blockIdx,
			SpanCount:  block.SpanCount(),
			BytesRead:  int64(bytesRead),
			SkipReason: PruneBlockTimeMetadata,
		}, true
	}
	return ExecutionSkip{}, false
}

func valuePredicateSkip(
	block *blockpackio.Block,
	predicates *vm.QueryPredicates,
	blockIdx int,
	bytesRead int,
) (ExecutionSkip, bool) {
	if predicates == nil || len(predicates.AttributeEquals) == 0 || predicates.HasOROperations {
		return ExecutionSkip{}, false
	}
	if blockMightMatchValuePredicates(block, predicates) {
		return ExecutionSkip{}, false
	}
	return ExecutionSkip{
		BlockID:    blockIdx,
		SpanCount:  block.SpanCount(),
		BytesRead:  int64(bytesRead),
		SkipReason: PruneBlockValuePredicate,
	}, true
}

func (e *BlockpackExecutor) applyBlockMatches(
	reader *blockpackio.Reader,
	blockWithBytes *blockpackio.BlockWithBytes,
	blockIdx int,
	program *vm.Program,
	opts QueryOptions,
	filterColumns, resultColumns map[string]struct{},
	queryArena *arena.Arena,
	matches *[]BlockpackSpanMatch,
	blockSpanHints []uint16,
) (int, bool, error) {
	block := blockWithBytes.Block

	var matchedRowIndices []int
	if blockSpanHints != nil {
		// Fast path: use span-level hints from trace block index.
		// Skip expensive column predicate scan entirely.
		spanCount := block.SpanCount()
		matchedRowIndices = make([]int, 0, len(blockSpanHints))
		for _, idx := range blockSpanHints {
			// CRITICAL: Validate span hints against actual block span count
			// Corrupt or stale index data could cause out-of-bounds panic
			if int(idx) >= spanCount {
				return 0, false, fmt.Errorf("span hint index %d exceeds block span count %d", idx, spanCount)
			}
			matchedRowIndices = append(matchedRowIndices, int(idx))
		}
		sort.Ints(matchedRowIndices)
		// Deduplicate indices to avoid duplicate matches hitting Limit early
		if len(matchedRowIndices) > 1 {
			w := 1
			for r := 1; r < len(matchedRowIndices); r++ {
				if matchedRowIndices[r] != matchedRowIndices[w-1] {
					matchedRowIndices[w] = matchedRowIndices[r]
					w++
				}
			}
			matchedRowIndices = matchedRowIndices[:w]
		}
	} else {
		// Normal path: evaluate column predicate against all rows
		columnProvider := NewBlockColumnProvider(block)
		rowSet, err := program.ColumnPredicate(columnProvider)
		if err != nil {
			return 0, false, fmt.Errorf("closure execution failed: %w", err)
		}
		matchedRowIndices = rowSet.ToSlice()
	}
	if len(matchedRowIndices) == 0 {
		return 0, false, nil
	}

	if filterColumns != nil {
		if err := reader.AddColumnsToBlock(blockWithBytes, resultColumns, queryArena); err != nil {
			return 0, false, fmt.Errorf("add result columns to block %d: %w", blockIdx, err)
		}
	}

	provider := newBlockpackAttributeProvider(blockWithBytes.Block, 0)
	matchCountBefore := len(*matches)

	for _, rowIdx := range matchedRowIndices {
		provider.idx = rowIdx

		if opts.StartTime > 0 || opts.EndTime > 0 {
			spanStartTime, ok := provider.GetStartTime()
			if !ok {
				continue
			}
			st := int64(spanStartTime) //nolint:gosec
			if (opts.StartTime > 0 && st < opts.StartTime) || (opts.EndTime > 0 && st > opts.EndTime) {
				continue
			}
		}

		traceID := encodeIDCached(provider.traceID())
		materializedFields := materializeFields(block, rowIdx, program.Attributes, opts.MaterializeAllFields)

		*matches = append(*matches, BlockpackSpanMatch{
			TraceID: traceID,
			SpanID:  encodeIDCached(provider.spanID()),
			Fields:  materializedFields,
		})

		if opts.Limit > 0 && len(*matches) >= opts.Limit {
			return len(*matches) - matchCountBefore, true, nil
		}
	}

	return len(*matches) - matchCountBefore, false, nil
}

// collectSpanHints extracts span-level row indices from the trace block index.
// When a query filters by trace:id and that's the only column predicate, these
// hints allow applyBlockMatches to skip the expensive column predicate scan and
// go directly to the matching rows.
// Returns nil if no span hints are available or if other predicates exist
// (which would require full column scanning for correctness).
func collectSpanHints(reader *blockpackio.Reader, predicates *vm.QueryPredicates) map[int][]uint16 {
	if predicates == nil || !reader.HasTraceBlockIndex() {
		return nil
	}
	traceIDValues, ok := predicates.DedicatedColumns[IntrinsicTraceID]
	if !ok || len(traceIDValues) == 0 {
		return nil
	}

	// Only use span hints as a full replacement for ColumnPredicate when
	// trace:id is the sole column predicate. If other predicates exist,
	// we'd need to evaluate them too, which requires the full scan path.
	hasOtherPredicates := false
	for col := range predicates.DedicatedColumns {
		if col != IntrinsicTraceID {
			hasOtherPredicates = true
			break
		}
	}
	if !hasOtherPredicates {
		hasOtherPredicates = len(predicates.AttributeEquals) > 0 ||
			len(predicates.DedicatedColumnsRegex) > 0 ||
			len(predicates.DedicatedRanges) > 0 ||
			len(predicates.AttributeRanges) > 0
	}
	if hasOtherPredicates {
		return nil
	}

	// AND with multiple trace:id values (e.g. trace:id="A" AND trace:id="B")
	// is contradictory — a span can only have one trace ID. Fall back to
	// normal ColumnPredicate which will correctly return no matches.
	if !predicates.HasOROperations && len(traceIDValues) > 1 {
		return nil
	}

	hints := make(map[int][]uint16)
	for _, val := range traceIDValues {
		var traceID [16]byte
		decoded := false
		if hexStr, ok := val.Data.(string); ok {
			decodedBytes, err := hex.DecodeString(hexStr)
			if err == nil && len(decodedBytes) == 16 {
				copy(traceID[:], decodedBytes)
				decoded = true
			}
		} else if rawBytes, ok := val.Data.([]byte); ok && len(rawBytes) == 16 {
			copy(traceID[:], rawBytes)
			decoded = true
		}
		if decoded {
			entries := reader.TraceBlockEntries(traceID)
			for _, entry := range entries {
				hints[entry.BlockID] = append(hints[entry.BlockID], entry.SpanIndices...)
			}
		}
	}

	if len(hints) == 0 {
		return nil
	}
	return hints
}

type trackingStats struct {
	detailedStats *blockpackio.IOStats
	analysisText  string
	bytesRead     int64
	ioOps         int64
}

func collectTrackingStats(tracking trackingContext) trackingStats {
	if tracking.detailed != nil {
		stats := tracking.detailed.AnalyzeIOLog()
		return trackingStats{
			bytesRead:     tracking.detailed.BytesRead(),
			ioOps:         tracking.detailed.IOOperations(),
			detailedStats: &stats,
			analysisText:  tracking.detailed.PrintIOAnalysis(),
		}
	}
	if tracking.standard != nil {
		return trackingStats{
			bytesRead: tracking.standard.BytesRead(),
			ioOps:     tracking.standard.IOOperations(),
		}
	}
	return trackingStats{}
}

// encodeIDCached uses a pooled buffer to encode IDs with fewer allocations
func encodeIDCached(id []byte) string {
	if len(id) == 0 {
		return ""
	}

	// For small IDs (<=16 bytes), use pooled buffer
	if len(id) <= 16 {
		bufPtr := idBufPool.Get().(*[]byte)
		buf := *bufPtr
		hex.Encode(buf[:len(id)*2], id)
		result := string(buf[:len(id)*2])
		idBufPool.Put(bufPtr)
		return result
	}

	// Fallback for larger IDs
	return hex.EncodeToString(id)
}
