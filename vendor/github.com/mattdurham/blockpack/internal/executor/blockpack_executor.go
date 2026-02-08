package executor

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattdurham/blockpack/internal/arena"
	aslice "github.com/mattdurham/blockpack/internal/arena/slice"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/quantile"
	"github.com/mattdurham/blockpack/internal/sql"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

const maxNestedBlockpackDepth = 20

// QueryOptions defines execution hints for blockpack queries.
type QueryOptions struct {
	Limit                int
	StartTime            int64         // Unix nanoseconds
	EndTime              int64         // Unix nanoseconds
	MaterializeAllFields bool          // If true, materialize all columns regardless of WHERE clause
	IOLatency            time.Duration // Artificial I/O latency for benchmarking (simulates S3 first-byte latency)
	CoalescedReadSize    uint64        // Max bytes per coalesced read (0 = default 4MB). Reduces IO ops by reading adjacent blocks together.
	DetailedIOLogging    bool          // If true, enable detailed IO tracking for analysis (use with IOLatency for realistic benchmarks)
}

// SpanFieldsProvider is an interface for accessing span fields.
// Implementations can be backed by blockpack storage or maps.
type SpanFieldsProvider interface {
	GetField(name string) (any, bool)
	IterateFields(fn func(name string, value any) bool)
}

// BlockpackSpanMatch represents a matching span with field access.
type BlockpackSpanMatch struct {
	TraceID string
	SpanID  string
	Fields  SpanFieldsProvider // Field accessor (either lazy or materialized)
}

// BlockPruningReason describes why a block was pruned
type BlockPruningReason string

const (
	PruneTimeRangeBeforeQuery BlockPruningReason = "time_range_before_query" // Block's MaxStart < QueryStartTime
	PruneTimeRangeAfterQuery  BlockPruningReason = "time_range_after_query"  // Block's MinStart > QueryEndTime
	PruneColumnBloomMiss      BlockPruningReason = "column_bloom_miss"       // Column name not in bloom filter
	PruneValueMinMaxMiss      BlockPruningReason = "value_min_max_miss"      // Value outside min/max range
	PruneDedicatedIndexMiss   BlockPruningReason = "dedicated_index_miss"    // Value not in dedicated index
	PruneBlockTimeMetadata    BlockPruningReason = "block_time_metadata"     // Block time metadata doesn't overlap query
	PruneBlockValuePredicate  BlockPruningReason = "block_value_predicate"   // Block doesn't match value predicates
)

// BlockPruningDecision records why a block was included or pruned
type BlockPruningDecision struct {
	BlockID      int                // Block index
	Included     bool               // True if block was scanned, false if pruned
	PrunedReason BlockPruningReason // Why the block was pruned (empty if included)
	SpanCount    int                // Number of spans in this block
}

// PruningStageStats tracks blocks after each pruning stage
type PruningStageStats struct {
	StageName       string // e.g., "time_range", "column_bloom", "dedicated_index"
	BlocksRemaining int    // Blocks remaining after this stage
	BlocksPruned    int    // Blocks pruned in this stage
}

// ExecutionSkip records blocks that were selected but skipped during execution
type ExecutionSkip struct {
	BlockID    int                // Block index
	SpanCount  int                // Number of spans in block
	BytesRead  int64              // Bytes read from disk for this block
	SkipReason BlockPruningReason // Why block was skipped at execution time
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
	TotalBlocks       int                    // Total blocks in file
	BlocksSelected    int                    // Blocks selected for scanning
	BlocksPruned      int                    // Blocks pruned (not scanned)
	PruningStages     []PruningStageStats    // Stats for each pruning stage
	BlockDecisions    []BlockPruningDecision // Per-block pruning decisions
	PredicatesSummary PredicateSummary       // Summary of predicates used
	ExecutionSkips    []ExecutionSkip        // Blocks skipped during execution (after selection)
	ScannedBlocks     []ScannedBlock         // Blocks that were actually scanned with results
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
	HasTimeFilter     bool     // Query has time range filter
	HasColumnFilters  bool     // Query has column equality filters
	HasDedicatedIndex bool     // Query uses dedicated column indexes
	HasOROperations   bool     // Query has OR operations
	ColumnFilters     []string // Column names with equality filters
	DedicatedColumns  []string // Dedicated columns used for pruning
}

// BlockpackResult is the output of a blockpack query.
type BlockpackResult struct {
	Matches         []BlockpackSpanMatch
	BlocksScanned   int
	BytesRead       int64                // Total bytes read from disk for scanned blocks
	IOOperations    int64                // Number of I/O operations (ReadAt calls) - critical for object storage
	QueryPlan       *QueryPlan           // Query execution plan with block selection details
	AggregateRows   []AggregateRow       // Results for aggregate queries
	IsAggregated    bool                 // True if this is an aggregate query result
	DetailedIOStats *blockpackio.IOStats // Detailed IO analysis (only populated if DetailedIOLogging=true)
	IOAnalysisText  string               // Human-readable IO analysis (only populated if DetailedIOLogging=true)
}

// AggregateRow represents a single row in an aggregated query result.
type AggregateRow struct {
	GroupKey []string           // Values for GROUP BY columns (empty for ungrouped aggregates)
	Values   map[string]float64 // Aggregated values keyed by aggregate function name
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
	if col := block.GetColumn("trace:id"); col != nil {
		if id, ok := materializeBytesField(col, rowIdx, true); ok {
			fields.TraceID = id
			fields.HasFields |= HasTraceID
		}
	}

	// span:id (bytes -> string)
	if col := block.GetColumn("span:id"); col != nil {
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
	if col := block.GetColumn("span:duration"); col != nil {
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
func materializeProjectedAttributes(block *blockpackio.Block, rowIdx int, attributesToProject []string, fields *SpanFields) {
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
				if name == "trace:id" || name == "span:id" {
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

func materializeFields(block *blockpackio.Block, rowIdx int, attributesToProject []string, materializeAll bool) *SpanFields {
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
	_, hasDuration := columns["span:duration"]
	if hasDuration {
		columns["span:start"] = struct{}{}
		columns["span:end"] = struct{}{}
	}

	// If no columns needed, return nil to load all columns (more efficient than selective reading)
	if len(columns) == 0 {
		return nil
	}

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
	_, hasDuration := columns["span:duration"]
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

		// Special case: if field is "span:duration" and it doesn't exist as a stored column,
		// we need span:start and span:end to compute it
		if agg.Field == "span:duration" {
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

// blockRowPair represents a matched row in a specific block
// ExecuteQuery loads the blockpack file at path and executes the VM program.
// If querySpec is provided and a QueryMatcher is configured, the executor will
// attempt to route the query to pre-computed metric streams (fast path).
// If no match is found or querySpec is nil, the query executes using streaming (fallback path).
func (e *BlockpackExecutor) ExecuteQuery(path string, program *vm.Program, querySpec *sql.QuerySpec, opts QueryOptions) (*BlockpackResult, error) {
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
	standard *blockpackio.TrackingReaderProvider
}

func (e *BlockpackExecutor) tryPrecomputedMetricStreams(querySpec *sql.QuerySpec, opts QueryOptions) (*BlockpackResult, bool, error) {
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

func (e *BlockpackExecutor) buildTrackedReader(path string, opts QueryOptions) (*blockpackio.Reader, trackingContext, func(), error) {
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
			_ = closeable.Close() // Ignore close errors
		}
	}

	var tracker blockpackio.ReaderProvider
	var ctx trackingContext
	if opts.DetailedIOLogging {
		ctx.detailed = blockpackio.NewDetailedTrackingReaderWithLatency(provider, opts.IOLatency, true)
		tracker = ctx.detailed
	} else {
		if opts.IOLatency > 0 {
			ctx.standard = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
		} else {
			ctx.standard = blockpackio.NewTrackingReaderProvider(provider)
		}
		tracker = ctx.standard
	}

	reader, err := blockpackio.NewReaderFromProvider(tracker)
	if err != nil {
		return nil, trackingContext{}, closeFn, fmt.Errorf("decode blockpack: %w", err)
	}

	return reader, ctx, closeFn, nil
}

func (e *BlockpackExecutor) executeStreamingQuery(reader *blockpackio.Reader, program *vm.Program, opts QueryOptions, tracking trackingContext) (*BlockpackResult, error) {
	var queryArena arena.Arena
	defer queryArena.Free()

	predicates := ensurePredicates(program.Predicates)
	blockOrder, queryPlan := selectBlocksWithPlan(reader, predicates, opts, &queryArena)
	queryPlanCopy := copyQueryPlanFromArena(queryPlan)

	matches := make([]BlockpackSpanMatch, 0, estimatedMatchCapacity(blockOrder, opts.Limit))
	executionSkips := make([]ExecutionSkip, 0)
	scannedBlocks := make([]ScannedBlock, 0)

	filterColumns := extractFilterColumns(predicates, opts.StartTime, opts.EndTime)
	resultColumns := extractResultColumns(program, opts)

	blocksScanned := 0
	_, err := e.scanStreamingBlocks(reader, blockOrder, predicates, program, opts, filterColumns, resultColumns, &queryArena, &matches, &executionSkips, &scannedBlocks, &blocksScanned, maxNestedBlockpackDepth)
	if err != nil {
		return nil, err
	}

	if queryPlanCopy != nil {
		queryPlanCopy.ExecutionSkips = executionSkips
		queryPlanCopy.ScannedBlocks = scannedBlocks
	}

	stats := collectTrackingStats(tracking)
	return &BlockpackResult{
		Matches:         matches,
		BlocksScanned:   blocksScanned,
		BytesRead:       stats.bytesRead,
		IOOperations:    stats.ioOps,
		QueryPlan:       queryPlanCopy,
		DetailedIOStats: stats.detailedStats,
		IOAnalysisText:  stats.analysisText,
	}, nil
}

func (e *BlockpackExecutor) scanStreamingBlocks(
	reader *blockpackio.Reader,
	blockOrder []int,
	predicates *vm.QueryPredicates,
	program *vm.Program,
	opts QueryOptions,
	filterColumns map[string]struct{},
	resultColumns map[string]struct{},
	queryArena *arena.Arena,
	matches *[]BlockpackSpanMatch,
	executionSkips *[]ExecutionSkip,
	scannedBlocks *[]ScannedBlock,
	blocksScanned *int,
	depth int,
) (bool, error) {
	if depth <= 0 {
		return false, fmt.Errorf("nested blockpack depth exceeds limit %d", maxNestedBlockpackDepth)
	}
	if len(blockOrder) == 0 {
		return false, nil
	}

	var reusableBlock *blockpackio.Block
	coalescedReads := reader.CoalesceBlocks(blockOrder, opts.CoalescedReadSize)

	for _, coalescedRead := range coalescedReads {
		blockBytesMap, err := reader.ReadCoalescedBlocks(coalescedRead)
		if err != nil {
			return false, fmt.Errorf("read coalesced blocks: %w", err)
		}

		for blockIdx := coalescedRead.StartBlockIdx; blockIdx < coalescedRead.EndBlockIdx; blockIdx++ {
			cachedBytes, ok := blockBytesMap[blockIdx]
			if !ok {
				return false, fmt.Errorf("block %d not in coalesced read result", blockIdx)
			}

			if reader.IsBlockpackEntry(blockIdx) {
				subReader, err := blockpackio.NewReader(cachedBytes)
				if err != nil {
					return false, fmt.Errorf("decode nested blockpack %d: %w", blockIdx, err)
				}
				subOrder, _ := selectBlocksWithPlan(subReader, predicates, opts, queryArena)
				reached, err := e.scanStreamingBlocks(subReader, subOrder, predicates, program, opts, filterColumns, resultColumns, queryArena, matches, executionSkips, scannedBlocks, blocksScanned, depth-1)
				if err != nil {
					return false, err
				}
				if reached {
					return true, nil
				}
				continue
			}

			blockWithBytes, err := reader.GetBlockWithBytes(blockIdx, filterColumns, reusableBlock, cachedBytes, queryArena)
			if err != nil {
				return false, fmt.Errorf("get block %d: %w", blockIdx, err)
			}
			block := blockWithBytes.Block
			reusableBlock = block

			if skip, ok := timePruneSkip(block, opts.StartTime, opts.EndTime, blockIdx, len(blockWithBytes.RawBytes)); ok {
				*executionSkips = append(*executionSkips, skip)
				continue
			}
			if skip, ok := valuePredicateSkip(block, predicates, blockIdx, len(blockWithBytes.RawBytes)); ok {
				*executionSkips = append(*executionSkips, skip)
				continue
			}

			*blocksScanned++
			matchCountFromBlock, reached, err := e.applyBlockMatches(reader, blockWithBytes, blockIdx, program, opts, filterColumns, resultColumns, queryArena, matches)
			if err != nil {
				return false, err
			}

			*scannedBlocks = append(*scannedBlocks, ScannedBlock{
				BlockID:    blockIdx,
				SpanCount:  block.SpanCount(),
				BytesRead:  int64(len(blockWithBytes.RawBytes)),
				MatchCount: matchCountFromBlock,
			})

			if reached {
				return true, nil
			}
		}
	}

	return false, nil
}

func (e *BlockpackExecutor) scanAggregationBlocks(
	reader *blockpackio.Reader,
	blockOrder []int,
	predicates *vm.QueryPredicates,
	program *vm.Program,
	opts QueryOptions,
	allColumnsNeeded map[string]struct{},
	startTime int64,
	endTime int64,
	useTimeBuckets bool,
	stepSizeNs int64,
	groupKeyBuffer []string,
	stringInterner *StringInterner,
	buckets map[string]*vm.AggBucket,
	queryArena *arena.Arena,
	blocksScanned *int,
	executionSkips *[]ExecutionSkip,
	depth int,
) error {
	if depth <= 0 {
		return fmt.Errorf("nested blockpack depth exceeds limit %d", maxNestedBlockpackDepth)
	}
	if len(blockOrder) == 0 {
		return nil
	}

	var reusableBlock *blockpackio.Block
	coalescedReads := reader.CoalesceBlocks(blockOrder, opts.CoalescedReadSize)

	for _, coalescedRead := range coalescedReads {
		blockBytesMap, err := reader.ReadCoalescedBlocks(coalescedRead)
		if err != nil {
			return fmt.Errorf("read coalesced blocks: %w", err)
		}

		for blockIdx := coalescedRead.StartBlockIdx; blockIdx < coalescedRead.EndBlockIdx; blockIdx++ {
			cachedBytes, ok := blockBytesMap[blockIdx]
			if !ok {
				return fmt.Errorf("block %d not in coalesced read result", blockIdx)
			}

			if reader.IsBlockpackEntry(blockIdx) {
				subReader, err := blockpackio.NewReader(cachedBytes)
				if err != nil {
					return fmt.Errorf("decode nested blockpack %d: %w", blockIdx, err)
				}
				subOrder, _ := selectBlocksWithPlan(subReader, predicates, opts, queryArena)
				if err := e.scanAggregationBlocks(
					subReader,
					subOrder,
					predicates,
					program,
					opts,
					allColumnsNeeded,
					startTime,
					endTime,
					useTimeBuckets,
					stepSizeNs,
					groupKeyBuffer,
					stringInterner,
					buckets,
					queryArena,
					blocksScanned,
					executionSkips,
					depth-1,
				); err != nil {
					return err
				}
				continue
			}

			blockWithBytes, err := reader.GetBlockWithBytes(blockIdx, allColumnsNeeded, reusableBlock, cachedBytes, queryArena)
			if err != nil {
				return fmt.Errorf("get block %d: %w", blockIdx, err)
			}
			block := blockWithBytes.Block
			reusableBlock = block

			// Block-level pruning by time range when metadata is available
			if startTime > 0 && block.MaxStart < uint64(startTime) {
				*executionSkips = append(*executionSkips, ExecutionSkip{
					BlockID:    blockIdx,
					SpanCount:  block.SpanCount(),
					SkipReason: PruneBlockTimeMetadata,
				})
				continue
			}
			if endTime > 0 && block.MinStart > uint64(endTime) {
				*executionSkips = append(*executionSkips, ExecutionSkip{
					BlockID:    blockIdx,
					SpanCount:  block.SpanCount(),
					SkipReason: PruneBlockTimeMetadata,
				})
				continue
			}

			// Skip predicate-based block pruning for OR queries
			if predicates != nil && len(predicates.AttributeEquals) > 0 && !predicates.HasOROperations {
				blockMatches := blockMightMatchValuePredicates(block, predicates)
				if !blockMatches {
					*executionSkips = append(*executionSkips, ExecutionSkip{
						BlockID:    blockIdx,
						SpanCount:  block.SpanCount(),
						SkipReason: PruneBlockValuePredicate,
					})
					continue
				}
			}

			*blocksScanned++

			// Execute query using streaming callback to avoid RowSet allocation
			columnProvider := NewBlockColumnProvider(block)

			matchCount := 0

			// Use StreamingColumnPredicate if available, otherwise fall back to RowSet
			if program.StreamingColumnPredicate != nil {
				var err error
				matchCount, err = program.StreamingColumnPredicate(columnProvider, func(rowIdx int) bool {
					processAggregationRow(
						block, rowIdx, startTime, endTime, useTimeBuckets, stepSizeNs,
						program.AggregationPlan.GroupByFields, groupKeyBuffer, stringInterner,
						program.AggregationPlan.Aggregates, buckets,
					)
					return true // Continue iteration
				})

				if err != nil {
					return fmt.Errorf("streaming aggregation failed: %w", err)
				}
			} else {
				// Fallback: Use old RowSet-based approach
				rowSet, err := program.ColumnPredicate(columnProvider)
				if err != nil {
					return fmt.Errorf("closure execution failed: %w", err)
				}
				matchedRowIndices := rowSet.ToSlice()
				matchCount = len(matchedRowIndices)

				// Process matches and update aggregation buckets
				for _, rowIdx := range matchedRowIndices {
					processAggregationRow(
						block, rowIdx, startTime, endTime, useTimeBuckets, stepSizeNs,
						program.AggregationPlan.GroupByFields, groupKeyBuffer, stringInterner,
						program.AggregationPlan.Aggregates, buckets,
					)
				}
			}

			// Skip if no matches
			if matchCount == 0 {
				continue
			}
		}
	}

	return nil
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

func expandPredicateColumns(predicates *QueryPredicates, column string) []string {
	if strings.HasPrefix(column, vm.UnscopedColumnPrefix) {
		if predicates == nil {
			return nil
		}
		return predicates.UnscopedColumnNames[column]
	}
	return []string{column}
}

func hasDedicatedIndexInFile(reader *blockpackio.Reader, predicates *QueryPredicates) bool {
	for col := range predicates.DedicatedColumns {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	for col := range predicates.DedicatedColumnsRegex {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	for col := range predicates.DedicatedRanges {
		for _, scoped := range expandPredicateColumns(predicates, col) {
			if len(reader.DedicatedValues(scoped)) > 0 {
				return true
			}
		}
	}
	return false
}

func estimatedMatchCapacity(blockOrder []int, limit int) int {
	estimated := len(blockOrder) * 100
	if limit > 0 && estimated > limit {
		return limit
	}
	return estimated
}

func timePruneSkip(block *blockpackio.Block, startTime int64, endTime int64, blockIdx int, bytesRead int) (ExecutionSkip, bool) {
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

func valuePredicateSkip(block *blockpackio.Block, predicates *vm.QueryPredicates, blockIdx int, bytesRead int) (ExecutionSkip, bool) {
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

func (e *BlockpackExecutor) applyBlockMatches(reader *blockpackio.Reader, blockWithBytes *blockpackio.BlockWithBytes, blockIdx int, program *vm.Program, opts QueryOptions, filterColumns, resultColumns map[string]struct{}, queryArena *arena.Arena, matches *[]BlockpackSpanMatch) (int, bool, error) {
	block := blockWithBytes.Block
	columnProvider := NewBlockColumnProvider(block)
	rowSet, err := program.ColumnPredicate(columnProvider)
	if err != nil {
		return 0, false, fmt.Errorf("closure execution failed: %w", err)
	}
	matchedRowIndices := rowSet.ToSlice()
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
			st := int64(spanStartTime)
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

type trackingStats struct {
	bytesRead     int64
	ioOps         int64
	detailedStats *blockpackio.IOStats
	analysisText  string
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

// checkColumnBloom checks if a column name might exist in a block using the column name bloom filter.
// Returns false if the column definitely does not exist, true if it might exist (bloom false positive).
func checkColumnBloom(bloom *[blockpack.ColumnNameBloomBits / 8]byte, columnName string) bool {
	// Helper to check if a specific column name exists in bloom filter
	checkName := func(name string) bool {
		h1 := fnv.New32a()
		_, _ = h1.Write([]byte(name))
		hash1 := h1.Sum32()

		h2 := fnv.New32()
		_, _ = h2.Write([]byte(name))
		hash2 := h2.Sum32()

		pos1 := hash1 % blockpack.ColumnNameBloomBits
		pos2 := hash2 % blockpack.ColumnNameBloomBits

		byteIdx1 := pos1 / 8
		bit1 := pos1 % 8
		byteIdx2 := pos2 / 8
		bit2 := pos2 % 8

		// Both bits must be set for the column to potentially exist
		return (bloom[byteIdx1]&(1<<bit1)) != 0 && (bloom[byteIdx2]&(1<<bit2)) != 0
	}

	// First try exact match (for backwards compatibility)
	if checkName(columnName) {
		return true
	}

	// Check all type variants (columnName:0, columnName:1, etc.)
	// ColumnType is currently 0-8 (9 types), but check up to 15 for future expansion
	for typ := 0; typ <= 15; typ++ {
		suffixedName := fmt.Sprintf("%s:%d", columnName, typ)
		if checkName(suffixedName) {
			return true
		}
	}

	return false
}

// valueMinMaxAnyMatch checks if any of the given values could exist in the column
// based on its min/max statistics. This provides precise pruning for equality predicates.
func valueMinMaxAnyMatch(col *blockpack.Column, values []vm.Value) bool {
	// If we can't determine a match from min/max, conservatively return true
	supportedFound := false

	for _, v := range values {
		switch col.Type {
		case blockpack.ColumnTypeString:
			if v.Type == vm.TypeString {
				if s, ok := v.Data.(string); ok {
					supportedFound = true
					// Check if value is in range [StringMin, StringMax]
					if s >= col.Stats.StringMin && s <= col.Stats.StringMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeInt64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok {
					supportedFound = true
					// Check if value is in range [IntMin, IntMax]
					if val >= col.Stats.IntMin && val <= col.Stats.IntMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeUint64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok && val >= 0 {
					uval := uint64(val)
					supportedFound = true
					// Check if value is in range [UintMin, UintMax]
					if uval >= col.Stats.UintMin && uval <= col.Stats.UintMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeFloat64:
			if v.Type == vm.TypeFloat {
				if f, ok := v.Data.(float64); ok {
					supportedFound = true
					// Check if value is in range [FloatMin, FloatMax]
					if f >= col.Stats.FloatMin && f <= col.Stats.FloatMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBool:
			if v.Type == vm.TypeBool {
				if b, ok := v.Data.(bool); ok {
					supportedFound = true
					// Check if value matches either BoolMin or BoolMax
					if b == col.Stats.BoolMin || b == col.Stats.BoolMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBytes:
			// Bytes comparison is more expensive, skip min/max check
			// Fall through to scanning
			continue
		}
	}

	// If none of the values had a supported type, conservatively return true
	// to fall back to actual scanning
	return !supportedFound
}

func blockMightMatchValuePredicates(block *blockpackio.Block, predicates *QueryPredicates) bool {
	// INTERSECTION logic: block must match ALL predicates (AND query).
	// Note: This function is only called when !predicates.HasOROperations (lines 894, 1033),
	// so we only implement AND logic here. OR queries are handled elsewhere.
	for columnName, values := range predicates.AttributeEquals {
		// For unscoped attributes, check all possible column names
		possibleColumns := GetPossibleColumnNames(columnName)
		found := false

		for _, possibleColumn := range possibleColumns {
			col := block.GetColumn(possibleColumn)
			if col == nil {
				continue
			}
			if !col.Stats.HasValues {
				continue
			}
			minMaxMatch := valueMinMaxAnyMatch(col, values)
			if minMaxMatch {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}
	return true
}

// ValueBloomResult describes why a block passed or failed value min/max checks
// Note: Name retained for compatibility, but bloom filters have been removed.
// This now only checks min/max statistics, not bloom filters.
type ValueBloomResult struct {
	Matches     bool               // True if block might contain matching values
	PruneReason BlockPruningReason // Specific reason if !Matches
}

func blockMatchesValueBloom(
	reader *blockpackio.Reader,
	blockIdx int,
	predicates map[string][]vm.Value,
	columnNames []string,
) ValueBloomResult {
	// Build a list of all possible column names to check for stats
	// (handling unscoped attributes that can map to multiple column names)
	allColumnNames := make([]string, 0, len(columnNames)*2)
	for _, colName := range columnNames {
		possibleNames := GetPossibleColumnNames(colName)
		allColumnNames = append(allColumnNames, possibleNames...)
	}

	stats, err := reader.BlockColumnStats(blockIdx, allColumnNames)
	if err != nil {
		return ValueBloomResult{Matches: true} // fall back to scanning the block
	}

	for columnName, values := range predicates {
		// For unscoped attributes, check all possible column names
		possibleColumns := GetPossibleColumnNames(columnName)
		foundColumn := false
		minMaxFailed := false

		for _, possibleColumn := range possibleColumns {
			entry, ok := stats[possibleColumn]
			if !ok {
				continue
			}
			if !entry.Stats.HasValues {
				continue
			}

			foundColumn = true

			// Check min/max for value range pruning
			minMaxMatch := valueMinMaxAnyMatchStats(entry, values)
			if !minMaxMatch {
				minMaxFailed = true
				continue
			}
			// Found a matching column with matching values
			foundColumn = true
			minMaxFailed = false
			break
		}

		if !foundColumn || minMaxFailed {
			// Determine most specific pruning reason
			if minMaxFailed {
				return ValueBloomResult{Matches: false, PruneReason: PruneValueMinMaxMiss}
			}
			// Column not found at all
			return ValueBloomResult{Matches: false, PruneReason: PruneColumnBloomMiss}
		}
	}
	return ValueBloomResult{Matches: true}
}

// valueMinMaxAnyMatchStats checks if any of the given values could exist in the column
// based on its min/max statistics. This provides precise pruning for equality predicates.
func valueMinMaxAnyMatchStats(entry blockpack.ColumnStatsWithType, values []vm.Value) bool {
	// If we can't determine a match from min/max, conservatively return true
	supportedFound := false

	for _, v := range values {
		switch entry.Type {
		case blockpack.ColumnTypeString:
			if v.Type == vm.TypeString {
				if s, ok := v.Data.(string); ok {
					supportedFound = true
					// Check if value is in range [StringMin, StringMax]
					if s >= entry.Stats.StringMin && s <= entry.Stats.StringMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeInt64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok {
					supportedFound = true
					// Check if value is in range [IntMin, IntMax]
					if val >= entry.Stats.IntMin && val <= entry.Stats.IntMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeUint64:
			if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
				if val, ok := valueToInt64(v.Data); ok && val >= 0 {
					uval := uint64(val)
					supportedFound = true
					// Check if value is in range [UintMin, UintMax]
					if uval >= entry.Stats.UintMin && uval <= entry.Stats.UintMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeFloat64:
			if v.Type == vm.TypeFloat {
				if f, ok := v.Data.(float64); ok {
					supportedFound = true
					// Check if value is in range [FloatMin, FloatMax]
					if f >= entry.Stats.FloatMin && f <= entry.Stats.FloatMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBool:
			if v.Type == vm.TypeBool {
				if b, ok := v.Data.(bool); ok {
					supportedFound = true
					// Check if value matches either BoolMin or BoolMax
					if b == entry.Stats.BoolMin || b == entry.Stats.BoolMax {
						return true
					}
				}
			}
		case blockpack.ColumnTypeBytes:
			// Bytes comparison is more expensive, skip min/max check for now
			// Fall through to bloom filter check
			continue
		}
	}

	// If none of the values had a supported type, conservatively return true
	// to fall back to bloom filter and actual scanning
	return !supportedFound
}

func valueToInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	}
	return 0, false
}

// pruneBlocksByTimeRange filters blocks based on time range, updating decisions and returning candidates.
func pruneBlocksByTimeRange(
	blocks []*blockpackio.Block,
	opts QueryOptions,
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	candidate := aslice.Make[int](arena, 0)
	for idx, block := range blocks {
		startsBeforeRange := opts.StartTime > 0 && block.MaxStart < uint64(opts.StartTime)
		endsAfterRange := opts.EndTime > 0 && block.MinStart > uint64(opts.EndTime)
		if startsBeforeRange {
			decisions[idx].Included = false
			decisions[idx].PrunedReason = PruneTimeRangeBeforeQuery
			continue
		}
		if endsAfterRange {
			decisions[idx].Included = false
			decisions[idx].PrunedReason = PruneTimeRangeAfterQuery
			continue
		}
		candidate = candidate.AppendOne(arena, idx)
	}
	return candidate
}

// pruneBlocksByValueStats prunes blocks using per-block value statistics (v10 feature).
// Checks if attribute values in predicates could exist in blocks based on bloom filters and min/max ranges.
// Returns the filtered candidate list.
func pruneBlocksByValueStats(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	// Only apply for AttributeEquals predicates (non-OR queries)
	if predicates == nil || len(predicates.AttributeEquals) == 0 || predicates.HasOROperations {
		return candidate
	}

	filtered := aslice.Make[int](arena, 0)

	for _, blockIdx := range candidate.Raw() {
		// Get value statistics for this block
		valueStats := reader.BlockValueStats(blockIdx)
		if len(valueStats) == 0 {
			// No value stats available (v9 file or stats not tracked) - include block
			filtered = filtered.AppendOne(arena, blockIdx)
			continue
		}

		// Check if all required attributes match
		matchesAll := true
		for attrName, values := range predicates.AttributeEquals {
			stats, hasStats := valueStats[attrName]
			if !hasStats {
				// Attribute not tracked in stats - include block (conservative)
				continue
			}

			// Check if ANY of the query values matches the block stats
			matchesAny := false
			for _, queryValue := range values {
				// Use the Data field directly (already contains the Go value)
				if stats.MayContainValue(queryValue.Data) {
					matchesAny = true
					break
				}
			}

			if !matchesAny {
				// None of the query values match this attribute's stats - prune block
				matchesAll = false
				break
			}
		}

		if matchesAll {
			filtered = filtered.AppendOne(arena, blockIdx)
		} else {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = PruneBlockValuePredicate
		}
	}

	return filtered
}

// pruneBlocksByBloomFilter applies column bloom filter and value range pruning for AttributeEquals predicates.
// Returns the filtered candidate list.
func pruneBlocksByBloomFilter(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) aslice.Slice[int] {
	if len(predicates.AttributeEquals) == 0 || predicates.HasOROperations {
		return candidate
	}

	blocks := reader.Blocks()
	filtered := aslice.Make[int](arena, 0)
	columnNames := aslice.Make[string](arena, 0)
	for columnName := range predicates.AttributeEquals {
		columnNames = columnNames.AppendOne(arena, columnName)
	}

	for _, blockIdx := range candidate.Raw() {
		block := blocks[blockIdx]

		// Check column bloom first
		hasAllColumns := true
		for columnName := range predicates.AttributeEquals {
			possibleColumns := GetPossibleColumnNames(columnName)
			found := false
			for _, possibleColumn := range possibleColumns {
				bloomResult := checkColumnBloom(&block.ColumnNameBloom, possibleColumn)
				if bloomResult {
					found = true
					break
				}
			}
			if !found {
				hasAllColumns = false
				break
			}
		}

		if !hasAllColumns {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = PruneColumnBloomMiss
			continue
		}

		// Check value min/max
		valueBloomResult := blockMatchesValueBloom(reader, blockIdx, predicates.AttributeEquals, columnNames.Raw())
		if !valueBloomResult.Matches {
			decisions[blockIdx].Included = false
			decisions[blockIdx].PrunedReason = valueBloomResult.PruneReason
			continue
		}

		filtered = filtered.AppendOne(arena, blockIdx)
	}
	return filtered
}

// pruneBlocksByRegexPredicates applies regex predicates on dedicated columns (AND logic only).
// Returns the filtered current set (as map for efficient intersection).
func pruneBlocksByRegexPredicates(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) (map[int]struct{}, bool) {
	if predicates.HasOROperations {
		return current, false
	}

	appliedDedicatedRegex := false
	for column, pattern := range predicates.DedicatedColumnsRegex {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		filtered := make(map[int]struct{})
		appliedPredicate := false
		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}

			appliedDedicatedRegex = true
			appliedPredicate = true
			matchingBlocks := reader.BlocksForDedicatedRegex(scoped, re, arena)
			for _, blockID := range matchingBlocks {
				if _, allowed := current[blockID]; allowed {
					filtered[blockID] = struct{}{}
				}
			}
		}
		if !appliedPredicate {
			continue
		}

		// Mark pruned blocks - only if not already pruned
		for id := range current {
			if _, kept := filtered[id]; !kept {
				if decisions[id].Included {
					decisions[id].Included = false
					decisions[id].PrunedReason = PruneDedicatedIndexMiss
				}
			}
		}
		current = filtered
	}

	return current, appliedDedicatedRegex
}

// checkFloatInRange checks if a float value is within the specified range.
func checkFloatInRange(value float64, rangePred *vm.RangePredicate) bool {
	var minVal, maxVal *float64
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(float64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(int64); ok {
			fv := float64(v)
			minVal = &fv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(float64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(int64); ok {
			fv := float64(v)
			maxVal = &fv
		}
	}

	if minVal != nil {
		if rangePred.MinInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if rangePred.MaxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}

	return true
}

// checkIntInRange checks if an int64 value is within the specified range.
func checkIntInRange(value int64, rangePred *vm.RangePredicate) bool {
	var minVal, maxVal *int64
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(int64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be satisfied by any int64
			if v > math.MaxInt64 {
				// This range is unsatisfiable for int64 values
				return false
			}
			iv := int64(v)
			minVal = &iv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(int64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be safely converted
			if v > math.MaxInt64 {
				// Skip this bound - cannot safely convert to int64
				// This means we cannot prune based on this max bound
			} else {
				iv := int64(v)
				maxVal = &iv
			}
		}
	}

	if minVal != nil {
		if rangePred.MinInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if rangePred.MaxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}

	return true
}

// addBlocksForDedicatedRangesUnion adds blocks matching range predicates to the matched set (union logic).
func addBlocksForDedicatedRangesUnion(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	matched map[int]struct{},
	arena *arena.Arena,
) {
	for column, rangePred := range predicates.DedicatedRanges {
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}

			// Only handle range-bucketed column types or low-cardinality numeric columns
			if colType != blockpack.ColumnTypeRangeDuration && colType != blockpack.ColumnTypeRangeInt64 &&
				colType != blockpack.ColumnTypeRangeUint64 && colType != blockpack.ColumnTypeInt64 &&
				colType != blockpack.ColumnTypeUint64 && colType != blockpack.ColumnTypeFloat64 {
				continue
			}

			// Get blocks for matching values
			colBlocks := make(map[int]struct{})

			// Get all indexed values for this column
			dedicatedValues := reader.DedicatedValues(scoped)

			// For float columns, handle float comparisons
			if colType == blockpack.ColumnTypeFloat64 {
				// For each indexed value, check if it's in range
				for _, key := range dedicatedValues {
					// Extract float64 value from key data
					if len(key.Data()) < 8 {
						continue
					}
					value := math.Float64frombits(binary.LittleEndian.Uint64(key.Data()))

					if checkFloatInRange(value, rangePred) {
						// Get blocks for this value
						for _, blockID := range reader.BlocksForDedicated(scoped, key, arena) {
							if _, allowed := current[blockID]; allowed {
								colBlocks[blockID] = struct{}{}
							}
						}
					}
				}
			} else {
				// For int columns, handle int comparisons
				// For each indexed value, check if it's in range
				for _, key := range dedicatedValues {
					// Extract int64 value from key data
					if len(key.Data()) < 8 {
						continue
					}
					value := int64(binary.LittleEndian.Uint64(key.Data()))

					if checkIntInRange(value, rangePred) {
						// Get blocks for this value
						for _, blockID := range reader.BlocksForDedicated(scoped, key, arena) {
							if _, allowed := current[blockID]; allowed {
								colBlocks[blockID] = struct{}{}
							}
						}
					}
				}
			}

			// Union with matched set
			for id := range colBlocks {
				matched[id] = struct{}{}
			}
		}
	}
}

// selectBlocksWithUnionLogic applies union logic for OR operations or multiple dedicated columns.
// Returns non-nil plan if we should return early, nil if we should continue to intersection logic.
func selectBlocksWithUnionLogic(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	stages aslice.Slice[PruningStageStats],
	beforeDedicated int,
	totalBlocks int,
	summary PredicateSummary,
	arena *arena.Arena,
) ([]int, *QueryPlan) {
	useUnion := predicates.HasOROperations || len(predicates.DedicatedColumns) > 1
	if !useUnion {
		return nil, nil
	}

	matched := make(map[int]struct{})

	// Special handling for trace:id using trace block index (v8+)
	traceIDUsedIndex := false
	if traceIDValues, ok := predicates.DedicatedColumns["trace:id"]; ok && len(traceIDValues) > 0 {
		// Try to use trace block index for much faster lookups
		for _, val := range traceIDValues {
			// Convert value to [16]byte trace ID
			var traceID [16]byte
			decoded := false
			if hexStr, ok := val.Data.(string); ok {
				// Hex string format: "0102030405060708090a0b0c0d0e0f10"
				decodedBytes, err := hex.DecodeString(hexStr)
				if err == nil && len(decodedBytes) == 16 {
					copy(traceID[:], decodedBytes)
					decoded = true
				}
			} else if bytes, ok := val.Data.([]byte); ok && len(bytes) == 16 {
				// Raw bytes format
				copy(traceID[:], bytes)
				decoded = true
			}

			if decoded {
				blockIDs := reader.BlocksForTraceID(traceID)
				if blockIDs != nil {
					// Trace block index available - use it
					traceIDUsedIndex = true
					for _, id := range blockIDs {
						if _, allowed := current[id]; allowed {
							matched[id] = struct{}{}
						}
					}
				}
			}
		}
	}

	// Add blocks from DedicatedColumns
	for column, values := range predicates.DedicatedColumns {
		// Skip trace:id if we successfully used the trace block index
		if column == "trace:id" && traceIDUsedIndex {
			continue
		}
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}
		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			// Use type from file's dedicated index (supports both explicit and auto-detected columns)
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}
			for _, val := range values {
				key, ok := valueToDedicatedKey(colType, val, scoped)
				if !ok {
					continue
				}
				for _, id := range reader.BlocksForDedicated(scoped, key, arena) {
					if _, allowed := current[id]; allowed {
						matched[id] = struct{}{}
					}
				}
			}
		}
	}

	// Add blocks from DedicatedRanges
	addBlocksForDedicatedRangesUnion(reader, predicates, current, matched, arena)

	// Add blocks from DedicatedColumnsRegex
	for column, pattern := range predicates.DedicatedColumnsRegex {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}
		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			matchingBlocks := reader.BlocksForDedicatedRegex(scoped, re, arena)
			for _, blockID := range matchingBlocks {
				if _, allowed := current[blockID]; allowed {
					matched[blockID] = struct{}{}
				}
			}
		}
	}

	// Note: We don't apply bloom filter checks to dedicated columns here
	// See explanation in intersection logic above

	// Mark pruned blocks - only if not already pruned
	for id := range current {
		if _, kept := matched[id]; !kept {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
		}
	}

	out := aslice.Make[int](arena, 0)
	for id := range matched {
		out = out.AppendOne(arena, id)
	}
	outRaw := out.Raw()
	sort.Ints(outRaw)

	// Protect against integer underflow in pruning stats
	blocksPruned := 0
	if beforeDedicated >= len(outRaw) {
		blocksPruned = beforeDedicated - len(outRaw)
	}

	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "dedicated_index",
		BlocksRemaining: len(outRaw),
		BlocksPruned:    blocksPruned,
	})

	plan := &QueryPlan{
		TotalBlocks:       totalBlocks,
		BlocksSelected:    len(outRaw),
		BlocksPruned:      totalBlocks - len(outRaw),
		PruningStages:     stages.Raw(),
		BlockDecisions:    decisions,
		PredicatesSummary: summary,
	}
	return outRaw, plan
}

// pruneBlocksByDedicatedIntersection applies intersection logic for dedicated columns (AND logic).
// Modifies current in place and returns flags for what was applied.
// extractInt64FromValue extracts an int64 value from a vm.Value for range comparisons
func extractInt64FromValue(val *vm.Value) (int64, bool) {
	switch val.Type {
	case vm.TypeInt, vm.TypeDuration:
		switch v := val.Data.(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		}
	case vm.TypeBytes:
		// For bytes (trace:id, span:id), extract first 8 bytes as int64
		if bytes, ok := val.Data.([]byte); ok && len(bytes) >= 8 {
			return int64(binary.LittleEndian.Uint64(bytes[:8])), true
		}
	}
	return 0, false
}

// checkInt64InRange checks if a value is within the specified range
func checkInt64InRange(value int64, minVal, maxVal *int64, minInclusive, maxInclusive bool) bool {
	if minVal != nil {
		if minInclusive {
			if value < *minVal {
				return false
			}
		} else {
			if value <= *minVal {
				return false
			}
		}
	}
	if maxVal != nil {
		if maxInclusive {
			if value > *maxVal {
				return false
			}
		} else {
			if value >= *maxVal {
				return false
			}
		}
	}
	return true
}

// extractRangeBounds extracts min/max values from a vm.RangePredicate.
// Returns nil pointers if the range is unsatisfiable for int64 values.
func extractRangeBounds(rangePred *vm.RangePredicate) (minVal, maxVal *int64) {
	if rangePred.MinValue != nil {
		if v, ok := rangePred.MinValue.Data.(int64); ok {
			minVal = &v
		} else if v, ok := rangePred.MinValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be satisfied by any int64
			if v > math.MaxInt64 {
				// Return nil to signal unsatisfiable range
				// Callers should treat this as "no matches possible"
				return nil, nil
			}
			iv := int64(v)
			minVal = &iv
		}
	}
	if rangePred.MaxValue != nil {
		if v, ok := rangePred.MaxValue.Data.(int64); ok {
			maxVal = &v
		} else if v, ok := rangePred.MaxValue.Data.(uint64); ok {
			// Check for overflow - uint64 values > MaxInt64 cannot be safely converted
			if v > math.MaxInt64 {
				// Skip this bound - cannot safely convert to int64
				// This means we cannot prune based on this max bound
			} else {
				iv := int64(v)
				maxVal = &iv
			}
		}
	}
	return minVal, maxVal
}

// processTraceIDDedicated handles the special fast path for trace:id using trace block index
func processTraceIDDedicated(
	reader *blockpackio.Reader,
	values []vm.Value,
	current map[int]struct{},
	decisions []BlockPruningDecision,
) (filtered map[int]struct{}, usedIndex bool) {
	// Check if trace block index is available at all
	if !reader.HasTraceBlockIndex() {
		return nil, false
	}

	colBlocks := make(map[int]struct{})
	foundValidTraceID := false

	for _, val := range values {
		// Convert value to [16]byte trace ID
		var traceID [16]byte
		decoded := false
		if hexStr, ok := val.Data.(string); ok {
			// Hex string format: "0102030405060708090a0b0c0d0e0f10"
			decodedBytes, err := hex.DecodeString(hexStr)
			if err == nil && len(decodedBytes) == 16 {
				copy(traceID[:], decodedBytes)
				decoded = true
			}
		} else if bytes, ok := val.Data.([]byte); ok && len(bytes) == 16 {
			// Raw bytes format
			copy(traceID[:], bytes)
			decoded = true
		}

		if decoded {
			foundValidTraceID = true
			blockIDs := reader.BlocksForTraceID(traceID)
			// blockIDs == nil means trace not found (which is valid - prune all blocks)
			// Empty slice means trace exists but in zero blocks (rare, but valid)
			for _, id := range blockIDs {
				if _, allowed := current[id]; allowed {
					colBlocks[id] = struct{}{}
				}
			}
		}
	}

	// If no valid trace IDs were provided, can't use the index
	if !foundValidTraceID {
		return nil, false
	}

	// Successfully used trace block index - apply intersection
	filtered = make(map[int]struct{})
	for id := range colBlocks {
		if _, allowed := current[id]; allowed {
			filtered[id] = struct{}{}
		}
	}

	// Mark pruned blocks
	for id := range current {
		if _, kept := filtered[id]; !kept {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
		}
	}

	return filtered, true
}

// collectBlocksForRangeBuckets collects blocks for range-bucketed columns with exact values
func collectBlocksForRangeBuckets(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	values []vm.Value,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	bucketMeta, err := reader.GetRangeBucketMetadata(column)
	if err != nil || bucketMeta == nil || len(bucketMeta.Boundaries) == 0 {
		// No bucket metadata - try direct value lookup
		for _, val := range values {
			key, ok := valueToDedicatedKey(colType, val, column)
			if !ok {
				continue
			}
			for _, id := range reader.BlocksForDedicated(column, key, arena) {
				if _, allowed := current[id]; allowed {
					colBlocks[id] = struct{}{}
				}
			}
		}
		return colBlocks
	}

	// Has bucket metadata - convert values to bucket IDs
	for _, val := range values {
		int64Val, found := extractInt64FromValue(&val)
		if !found {
			continue
		}

		// Find the bucket ID for this value
		bucketID := blockpack.GetBucketID(int64Val, bucketMeta.Boundaries)

		// Create bucket key
		key := blockpack.RangeBucketValueKey(bucketID, colType)

		// Get blocks for this bucket
		for _, id := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[id]; allowed {
				colBlocks[id] = struct{}{}
			}
		}
	}

	return colBlocks
}

// collectBlocksForDirectValues collects blocks for non-range-bucketed columns
func collectBlocksForDirectValues(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	values []vm.Value,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	for _, val := range values {
		key, ok := valueToDedicatedKey(colType, val, column)
		if !ok {
			continue
		}
		for _, id := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[id]; allowed {
				colBlocks[id] = struct{}{}
			}
		}
	}

	return colBlocks
}

// intersectAndPruneBlocks intersects colBlocks with current and updates decisions
func intersectAndPruneBlocks(
	current map[int]struct{},
	colBlocks map[int]struct{},
	decisions []BlockPruningDecision,
) {
	for id := range current {
		if _, ok := colBlocks[id]; !ok {
			if decisions[id].Included {
				decisions[id].Included = false
				decisions[id].PrunedReason = PruneDedicatedIndexMiss
			}
			delete(current, id)
		}
	}
}

// collectBlocksForRangeWithBuckets collects blocks for range predicates on range-bucketed columns with metadata
func collectBlocksForRangeWithBuckets(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	rangePred *vm.RangePredicate,
	bucketMeta *blockpackio.RangeBucketMetadata,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})

	minVal, maxVal := extractRangeBounds(rangePred)

	// Get bucket IDs that intersect with the range
	bucketIDs := blockpack.GetBucketsForRange(minVal, maxVal, rangePred.MinInclusive, rangePred.MaxInclusive, bucketMeta.Boundaries)

	// Get blocks for each matching bucket
	for _, bucketID := range bucketIDs {
		key := blockpack.RangeBucketValueKey(bucketID, colType)
		for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
			if _, allowed := current[blockID]; allowed {
				colBlocks[blockID] = struct{}{}
			}
		}
	}

	return colBlocks
}

// collectBlocksForRangeWithoutBuckets collects blocks for range predicates on range-bucketed columns without metadata
func collectBlocksForRangeWithoutBuckets(
	reader *blockpackio.Reader,
	column string,
	rangePred *vm.RangePredicate,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})
	minVal, maxVal := extractRangeBounds(rangePred)

	dedicatedValues := reader.DedicatedValues(column)
	for _, key := range dedicatedValues {
		if len(key.Data()) < 8 {
			continue
		}
		value := int64(binary.LittleEndian.Uint64(key.Data()))

		if checkInt64InRange(value, minVal, maxVal, rangePred.MinInclusive, rangePred.MaxInclusive) {
			for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
				if _, allowed := current[blockID]; allowed {
					colBlocks[blockID] = struct{}{}
				}
			}
		}
	}

	return colBlocks
}

// collectBlocksForNonRangeColumnWithRange collects blocks for range predicates on non-range column types
func collectBlocksForNonRangeColumnWithRange(
	reader *blockpackio.Reader,
	column string,
	colType blockpack.ColumnType,
	rangePred *vm.RangePredicate,
	current map[int]struct{},
	arena *arena.Arena,
) map[int]struct{} {
	colBlocks := make(map[int]struct{})
	minVal, maxVal := extractRangeBounds(rangePred)

	dedicatedValues := reader.DedicatedValues(column)
	for _, key := range dedicatedValues {
		// Handle string column types
		if colType == blockpack.ColumnTypeString {
			stringValue := string(key.Data())
			inRange := true

			// Check minimum bound
			if rangePred.MinValue != nil {
				if minStr, ok := rangePred.MinValue.Data.(string); ok {
					if rangePred.MinInclusive {
						inRange = inRange && stringValue >= minStr
					} else {
						inRange = inRange && stringValue > minStr
					}
				}
			}

			// Check maximum bound
			if rangePred.MaxValue != nil {
				if maxStr, ok := rangePred.MaxValue.Data.(string); ok {
					if rangePred.MaxInclusive {
						inRange = inRange && stringValue <= maxStr
					} else {
						inRange = inRange && stringValue < maxStr
					}
				}
			}

			if inRange {
				for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
					if _, allowed := current[blockID]; allowed {
						colBlocks[blockID] = struct{}{}
					}
				}
			}
			continue
		}

		// Handle numeric column types
		if len(key.Data()) < 8 {
			continue
		}

		var value int64
		if colType == blockpack.ColumnTypeInt64 {
			value = int64(binary.LittleEndian.Uint64(key.Data()))
		} else if colType == blockpack.ColumnTypeUint64 {
			value = int64(binary.LittleEndian.Uint64(key.Data()))
		} else if colType == blockpack.ColumnTypeFloat64 {
			// For float columns, we need different handling
			// Skip for now in range predicates on non-range columns
			continue
		} else {
			continue
		}

		if checkInt64InRange(value, minVal, maxVal, rangePred.MinInclusive, rangePred.MaxInclusive) {
			for _, blockID := range reader.BlocksForDedicated(column, key, arena) {
				if _, allowed := current[blockID]; allowed {
					colBlocks[blockID] = struct{}{}
				}
			}
		}
	}

	return colBlocks
}

func pruneBlocksByDedicatedIntersection(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	current map[int]struct{},
	decisions []BlockPruningDecision,
	arena *arena.Arena,
) (appliedDedicated, appliedDedicatedRanges bool) {
	appliedDedicated = false
	for column, values := range predicates.DedicatedColumns {
		// Skip array columns - they need element-wise matching during scan phase
		if isArrayColumn(column) {
			continue
		}

		// Special fast path for trace:id using trace block index (v8+)
		if column == "trace:id" && len(values) > 0 {
			filtered, usedIndex := processTraceIDDedicated(reader, values, current, decisions)
			if usedIndex {
				appliedDedicated = true
				current = filtered
				continue // Skip regular dedicated column handling
			}
		}

		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		colBlocksUnion := make(map[int]struct{})
		appliedPredicate := false
		for _, scoped := range scopedColumns {
			// Skip array columns - they need element-wise matching during scan phase
			if isArrayColumn(scoped) {
				continue
			}
			// Regular dedicated column handling
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			// Use type from file's dedicated index (supports both explicit and auto-detected columns)
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}
			appliedDedicated = true
			appliedPredicate = true

			var colBlocks map[int]struct{}
			if blockpack.IsRangeColumnType(colType) {
				colBlocks = collectBlocksForRangeBuckets(reader, scoped, colType, values, current, arena)
			} else {
				colBlocks = collectBlocksForDirectValues(reader, scoped, colType, values, current, arena)
			}

			for id := range colBlocks {
				colBlocksUnion[id] = struct{}{}
			}
		}
		if !appliedPredicate {
			continue
		}

		// Intersect with current candidate set
		intersectAndPruneBlocks(current, colBlocksUnion, decisions)

		if len(current) == 0 {
			return appliedDedicated, false
		}
	}

	// Handle DedicatedRanges for intersection logic
	appliedDedicatedRanges = false
	for column, rangePred := range predicates.DedicatedRanges {
		scopedColumns := expandPredicateColumns(predicates, column)
		if len(scopedColumns) == 0 {
			continue
		}

		colBlocksUnion := make(map[int]struct{})
		appliedPredicate := false
		for _, scoped := range scopedColumns {
			if len(reader.DedicatedValues(scoped)) == 0 {
				continue
			}
			colType, ok := reader.DedicatedColumnTypeFromFile(scoped)
			if !ok {
				continue
			}

			appliedDedicatedRanges = true
			appliedPredicate = true

			var colBlocks map[int]struct{}
			if blockpack.IsRangeColumnType(colType) {
				bucketMeta, err := reader.GetRangeBucketMetadata(scoped)
				if err != nil || bucketMeta == nil || len(bucketMeta.Boundaries) == 0 {
					colBlocks = collectBlocksForRangeWithoutBuckets(reader, scoped, rangePred, current, arena)
				} else {
					colBlocks = collectBlocksForRangeWithBuckets(reader, scoped, colType, rangePred, bucketMeta, current, arena)
				}
			} else {
				colBlocks = collectBlocksForNonRangeColumnWithRange(reader, scoped, colType, rangePred, current, arena)
			}

			for id := range colBlocks {
				colBlocksUnion[id] = struct{}{}
			}
		}

		if !appliedPredicate {
			continue
		}

		// Intersect with current candidate set
		intersectAndPruneBlocks(current, colBlocksUnion, decisions)

		if len(current) == 0 {
			return appliedDedicated, appliedDedicatedRanges
		}
	}

	return appliedDedicated, appliedDedicatedRanges
}

// selectBlocksWithPlan is like selectBlocks but also tracks pruning decisions for query plan
func selectBlocksWithPlan(reader *blockpackio.Reader, predicates *QueryPredicates, opts QueryOptions, arena *arena.Arena) ([]int, *QueryPlan) {
	blocks := reader.Blocks()
	totalBlocks := len(blocks)

	// Track pruning decisions for each block (arena allocation)
	decisions := aslice.Make[BlockPruningDecision](arena, totalBlocks).Raw()
	for i, block := range blocks {
		decisions[i] = BlockPruningDecision{
			BlockID:   i,
			Included:  true, // Start optimistic
			SpanCount: block.SpanCount(),
		}
	}

	// Build predicates summary
	hasColumnFilters := false
	hasDedicatedIndex := false
	if predicates != nil {
		hasColumnFilters = len(predicates.AttributeEquals) > 0 || len(predicates.DedicatedColumns) > 0 || len(predicates.DedicatedColumnsRegex) > 0 || len(predicates.DedicatedRanges) > 0
		hasDedicatedIndex = len(predicates.DedicatedColumns) > 0 || len(predicates.DedicatedColumnsRegex) > 0 || len(predicates.DedicatedRanges) > 0
	}

	summary := PredicateSummary{
		HasTimeFilter:     opts.StartTime > 0 || opts.EndTime > 0,
		HasColumnFilters:  hasColumnFilters,
		HasDedicatedIndex: hasDedicatedIndex,
		HasOROperations:   predicates != nil && predicates.HasOROperations,
	}

	if predicates != nil {
		for col := range predicates.AttributeEquals {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
		}
		for col := range predicates.DedicatedColumns {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
		for col := range predicates.DedicatedColumnsRegex {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
		for col := range predicates.DedicatedRanges {
			summary.ColumnFilters = append(summary.ColumnFilters, col)
			summary.DedicatedColumns = append(summary.DedicatedColumns, col)
		}
	}

	stages := aslice.Make[PruningStageStats](arena, 0)

	// Step 1: Time range filtering
	candidate := pruneBlocksByTimeRange(blocks, opts, decisions, arena)
	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "time_range",
		BlocksRemaining: candidate.Len(),
		BlocksPruned:    totalBlocks - candidate.Len(),
	})

	if predicates == nil {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	// Step 2: Value statistics pruning (v10 feature)
	beforeValueStats := candidate.Len()
	candidate = pruneBlocksByValueStats(reader, predicates, candidate, decisions, arena)

	// Only add stage if we actually did some pruning
	if beforeValueStats != candidate.Len() {
		stages = stages.AppendOne(arena, PruningStageStats{
			StageName:       "value_statistics",
			BlocksRemaining: candidate.Len(),
			BlocksPruned:    beforeValueStats - candidate.Len(),
		})
	}

	// Step 3: Column name bloom filter and value range pruning for AttributeEquals predicates
	beforeColumnBloom := candidate.Len()
	candidate = pruneBlocksByBloomFilter(reader, predicates, candidate, decisions, arena)

	// Only add stage if we actually did some pruning
	if beforeColumnBloom != candidate.Len() || len(predicates.AttributeEquals) > 0 {
		stages = stages.AppendOne(arena, PruningStageStats{
			StageName:       "column_bloom_and_value_filters",
			BlocksRemaining: candidate.Len(),
			BlocksPruned:    beforeColumnBloom - candidate.Len(),
		})
	}

	return selectBlocksWithDedicatedIndexPlan(reader, predicates, candidate, decisions, stages, totalBlocks, summary, arena)
}

func selectBlocksWithDedicatedIndexPlan(
	reader *blockpackio.Reader,
	predicates *QueryPredicates,
	candidate aslice.Slice[int],
	decisions []BlockPruningDecision,
	stages aslice.Slice[PruningStageStats],
	totalBlocks int,
	summary PredicateSummary,
	arena *arena.Arena,
) ([]int, *QueryPlan) {
	// Step 4: Dedicated column index filtering
	if len(predicates.DedicatedColumns) == 0 && len(predicates.DedicatedColumnsRegex) == 0 && len(predicates.DedicatedRanges) == 0 {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	// Check if ANY columns actually have dedicated indexes in the file
	// If none do, skip the dedicated index optimization entirely
	if !hasDedicatedIndexInFile(reader, predicates) {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	beforeDedicated := candidate.Len()
	current := make(map[int]struct{}, candidate.Len())
	for _, id := range candidate.Raw() {
		current[id] = struct{}{}
	}

	// Step 4a: Regex predicates on dedicated columns
	// Skip if using union logic (OR operations) - regex blocks will be handled in Step 4b
	current, appliedDedicatedRegex := pruneBlocksByRegexPredicates(reader, predicates, current, decisions, arena)

	// Step 4b: Exact value predicates on dedicated columns
	// Use union logic if query has OR operations or multiple dedicated columns
	if selected, plan := selectBlocksWithUnionLogic(reader, predicates, current, decisions, stages, beforeDedicated, totalBlocks, summary, arena); plan != nil {
		return selected, plan
	}

	// Step 4c: Intersection logic for dedicated columns
	appliedDedicated, appliedDedicatedRanges := pruneBlocksByDedicatedIntersection(reader, predicates, current, decisions, arena)

	if !appliedDedicated && !appliedDedicatedRegex && !appliedDedicatedRanges {
		plan := &QueryPlan{
			TotalBlocks:       totalBlocks,
			BlocksSelected:    candidate.Len(),
			BlocksPruned:      totalBlocks - candidate.Len(),
			PruningStages:     stages.Raw(),
			BlockDecisions:    decisions,
			PredicatesSummary: summary,
		}
		return candidate.Raw(), plan
	}

	out := aslice.Make[int](arena, 0)
	for id := range current {
		out = out.AppendOne(arena, id)
	}
	outRaw := out.Raw()
	sort.Ints(outRaw)

	stages = stages.AppendOne(arena, PruningStageStats{
		StageName:       "dedicated_index",
		BlocksRemaining: len(outRaw),
		BlocksPruned:    beforeDedicated - len(outRaw),
	})

	plan := &QueryPlan{
		TotalBlocks:       totalBlocks,
		BlocksSelected:    len(outRaw),
		BlocksPruned:      totalBlocks - len(outRaw),
		PruningStages:     stages.Raw(),
		BlockDecisions:    decisions,
		PredicatesSummary: summary,
	}
	return outRaw, plan
}

// isArrayColumn checks if a column name represents an array column that stores multiple values
// Array columns include:
// - Intrinsic array fields: event:name, link:span_id, instrumentation:name (with colons)
// - Attribute array fields: event.message, link.opentracing.ref_type, instrumentation.scope-attr-str (with dots after scope prefix)
// - Explicit arrays: resource.str-array, span.str-array (ending with -array)
func isArrayColumn(columnName string) bool {
	// Check for colon in column name (event:, link:, instrumentation: intrinsics)
	if strings.Contains(columnName, ":") {
		return true
	}
	// Check for array attribute prefixes (event., link., instrumentation. attributes)
	if strings.HasPrefix(columnName, "event.") ||
		strings.HasPrefix(columnName, "link.") ||
		strings.HasPrefix(columnName, "instrumentation.") {
		return true
	}
	// Check for -array suffix (resource/span arrays)
	if strings.HasSuffix(columnName, "-array") {
		return true
	}
	return false
}

// valueToStringKey converts a VM string value to a dedicated string key.
func valueToStringKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeString {
		if s, ok := v.Data.(string); ok {
			return blockpack.StringValueKey(s), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToUint64Key converts a VM int/duration value to a dedicated uint64 key.
func valueToUint64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.UintValueKey(uint64(val)), true
		case int:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.UintValueKey(uint64(val)), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToInt64Key converts a VM int value to a dedicated int64 key.
func valueToInt64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt {
		switch val := v.Data.(type) {
		case int64:
			return blockpack.IntValueKey(val), true
		case int:
			return blockpack.IntValueKey(int64(val)), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToRangeInt64Key converts a VM int/duration value to a range-bucketed int64 key.
func valueToRangeInt64Key(v vm.Value, colType blockpack.ColumnType) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			return blockpack.RangeInt64ValueKey(val, colType), true
		case int:
			return blockpack.RangeInt64ValueKey(int64(val), colType), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToRangeUint64Key converts a VM int/duration value to a range-bucketed uint64 key.
func valueToRangeUint64Key(v vm.Value, colType blockpack.ColumnType) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeInt || v.Type == vm.TypeDuration {
		switch val := v.Data.(type) {
		case int64:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.RangeInt64ValueKey(val, colType), true
		case int:
			if val < 0 {
				return blockpack.DedicatedValueKey{}, false
			}
			return blockpack.RangeInt64ValueKey(int64(val), colType), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToBoolKey converts a VM bool value to a dedicated bool key.
func valueToBoolKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeBool {
		if b, ok := v.Data.(bool); ok {
			return blockpack.BoolValueKey(b), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToFloat64Key converts a VM float value to a dedicated float64 key.
func valueToFloat64Key(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeFloat {
		if f, ok := v.Data.(float64); ok {
			return blockpack.FloatValueKey(f), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

// valueToBytesKey converts a VM bytes value to a dedicated bytes key.
func valueToBytesKey(v vm.Value) (blockpack.DedicatedValueKey, bool) {
	if v.Type == vm.TypeBytes {
		if b, ok := v.Data.([]byte); ok {
			return blockpack.BytesValueKey(b), true
		}
	}
	return blockpack.DedicatedValueKey{}, false
}

func valueToDedicatedKey(colType blockpack.ColumnType, v vm.Value, columnName string) (blockpack.DedicatedValueKey, bool) {
	// Special handling for trace:id and span:id with RangeInt64 type
	// These are byte fields but stored as range-bucketed int64 (first 8 bytes)
	if (columnName == "trace:id" || columnName == "span:id") && colType == blockpack.ColumnTypeRangeInt64 {
		if v.Type == vm.TypeBytes {
			if bytes, ok := v.Data.([]byte); ok && len(bytes) >= 8 {
				// Extract first 8 bytes as int64
				val := int64(binary.LittleEndian.Uint64(bytes[:8]))
				return blockpack.RangeInt64ValueKey(val, colType), true
			}
		}
	}

	switch colType {
	case blockpack.ColumnTypeString:
		return valueToStringKey(v)
	case blockpack.ColumnTypeUint64:
		return valueToUint64Key(v)
	case blockpack.ColumnTypeRangeUint64:
		return valueToRangeUint64Key(v, colType)
	case blockpack.ColumnTypeInt64:
		return valueToInt64Key(v)
	case blockpack.ColumnTypeRangeInt64, blockpack.ColumnTypeRangeDuration:
		return valueToRangeInt64Key(v, colType)
	case blockpack.ColumnTypeBool:
		return valueToBoolKey(v)
	case blockpack.ColumnTypeFloat64:
		return valueToFloat64Key(v)
	case blockpack.ColumnTypeBytes:
		return valueToBytesKey(v)
	}

	return blockpack.DedicatedValueKey{}, false
}

// blockpackAttributeProvider mirrors the benchmark VM provider but keeps IDs handy.
// updateBucketCount increments the count in a bucket
func updateBucketCount(bucket *vm.AggBucket) {
	bucket.Count++
}

// updateBucketSum adds a value to the sum in a bucket
func updateBucketSum(bucket *vm.AggBucket, value float64) {
	bucket.Sum += value
}

// updateBucketMin updates the minimum value in a bucket
func updateBucketMin(bucket *vm.AggBucket, value float64) {
	// First value sets MIN (detect uninitialized via Min==0 && Max==0), or update if smaller
	if (bucket.Min == 0 && bucket.Max == 0) || value < bucket.Min {
		bucket.Min = value
	}
}

// updateBucketMax updates the maximum value in a bucket
func updateBucketMax(bucket *vm.AggBucket, value float64) {
	// First value sets MAX (detect uninitialized via Min==0 && Max==0), or update if larger
	if (bucket.Min == 0 && bucket.Max == 0) || value > bucket.Max {
		bucket.Max = value
	}
}

// finalizeAvg computes the average from sum and count
func finalizeAvg(bucket *vm.AggBucket) float64 {
	if bucket.Count == 0 {
		return math.NaN()
	}
	return bucket.Sum / float64(bucket.Count)
}

// finalizeRate computes the rate from count and time window
func finalizeRate(bucket *vm.AggBucket, timeWindowSeconds float64) float64 {
	if timeWindowSeconds == 0 {
		return 0.0
	}
	return float64(bucket.Count) / timeWindowSeconds
}

// sketchMap maps field names to actual quantile sketches
// We need this because vm.QuantileSketch is a stub and we use the real implementation
var sketchMap = make(map[*vm.AggBucket]map[string]*quantile.QuantileSketch)

// initBucketQuantile initializes a quantile sketch for a field in a bucket
func initBucketQuantile(bucket *vm.AggBucket, fieldName string) {
	if bucket.Quantiles == nil {
		bucket.Quantiles = make(map[string]*vm.QuantileSketch)
	}
	// Store a placeholder in the bucket
	bucket.Quantiles[fieldName] = &vm.QuantileSketch{}

	// Store the real sketch in our map
	if sketchMap[bucket] == nil {
		sketchMap[bucket] = make(map[string]*quantile.QuantileSketch)
	}
	sketchMap[bucket][fieldName] = quantile.NewQuantileSketch(0.01)
}

// updateBucketQuantile adds a value to the quantile sketch for a field
func updateBucketQuantile(bucket *vm.AggBucket, fieldName string, value float64) {
	if bucket.Quantiles == nil || bucket.Quantiles[fieldName] == nil {
		initBucketQuantile(bucket, fieldName)
	}
	// Get the real sketch from our map
	if realSketch := sketchMap[bucket][fieldName]; realSketch != nil {
		realSketch.Add(value)
	}
}

// extractBucketQuantile extracts a quantile value from the sketch for a field
func extractBucketQuantile(bucket *vm.AggBucket, fieldName string, q float64) float64 {
	if bucket.Quantiles == nil || bucket.Quantiles[fieldName] == nil {
		return 0.0
	}
	// Get the real sketch from our map
	if realSketch := sketchMap[bucket][fieldName]; realSketch != nil {
		return realSketch.Quantile(q)
	}
	return 0.0
}

// updateBucketHistogram adds a value to the histogram for a field
func updateBucketHistogram(bucket *vm.AggBucket, fieldName string, value float64) {
	if bucket.Histograms == nil {
		bucket.Histograms = make(map[string]*vm.HistogramData)
	}
	if bucket.Histograms[fieldName] == nil {
		// Initialize histogram with exponential buckets
		// Buckets: 0, 1ms, 10ms, 100ms, 1s, 10s, 100s, ...
		bucket.Histograms[fieldName] = &vm.HistogramData{
			Buckets: []float64{0, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, math.MaxFloat64},
			Counts:  make([]int64, 9), // One more than buckets for overflow
		}
	}

	hist := bucket.Histograms[fieldName]
	// Find the bucket for this value
	for i, boundary := range hist.Buckets {
		if value < boundary {
			hist.Counts[i]++
			return
		}
	}
	// Overflow bucket (last one)
	hist.Counts[len(hist.Counts)-1]++
}

// updateBucketStddev adds a value for standard deviation calculation
func updateBucketStddev(bucket *vm.AggBucket, value float64) {
	bucket.Sum += value
	bucket.SumSq += value * value
	bucket.Count++
}

// finalizeStddev computes the standard deviation from sum, sum of squares, and count
func finalizeStddev(bucket *vm.AggBucket) float64 {
	if bucket.Count == 0 {
		return 0.0
	}
	// Variance = E[X^2] - E[X]^2
	mean := bucket.Sum / float64(bucket.Count)
	meanSq := bucket.SumSq / float64(bucket.Count)
	variance := meanSq - mean*mean
	if variance < 0 {
		variance = 0 // Handle floating point errors
	}
	return math.Sqrt(variance)
}

// readPrecomputedMetricStreams reads pre-computed metric stream results from a metric stream.
// This is the fast path for queries that match pre-computed metric streams.
// Implementation will be added in Phase 6 when Writer support is complete.
func (e *BlockpackExecutor) readPrecomputedMetricStreams(stream *MetricStream, opts QueryOptions) (*BlockpackResult, error) {
	if stream == nil {
		return nil, fmt.Errorf("metric stream cannot be nil")
	}
	if stream.MetricStreamPath == "" {
		return nil, fmt.Errorf("metric stream path is required for precomputed metric streams")
	}
	if stream.Spec == nil {
		return nil, fmt.Errorf("metric stream spec is required for precomputed metric streams")
	}

	providerStorage, ok := e.storage.(ProviderStorage)
	if !ok {
		return nil, fmt.Errorf("storage must implement ProviderStorage interface")
	}

	provider, err := providerStorage.GetProvider(stream.MetricStreamPath)
	if err != nil {
		return nil, fmt.Errorf("read blockpack file: %w", err)
	}

	var tracking *blockpackio.TrackingReaderProvider
	if opts.IOLatency > 0 {
		tracking = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
	} else {
		tracking = blockpackio.NewTrackingReaderProvider(provider)
	}

	reader, err := blockpackio.NewReaderFromProvider(tracking)
	if err != nil {
		return nil, err
	}

	rawData, err := reader.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}
	if len(rawData) == 0 {
		return nil, fmt.Errorf("no metric stream blocks found")
	}

	streamData, err := findMetricStreamByID(rawData, stream.StreamID)
	if err != nil {
		return nil, err
	}

	parsed, err := parseMetricStreamData(streamData)
	if err != nil {
		return nil, err
	}

	aggSpec, err := aggSpecFromQuerySpec(stream.Spec)
	if err != nil {
		return nil, err
	}

	result, err := buildPrecomputedResult(parsed, aggSpec, opts)
	if err != nil {
		return nil, err
	}

	result.BytesRead = tracking.BytesRead()
	result.IOOperations = tracking.IOOperations()
	return result, nil
}

// executeAggregationQuery executes a query with GROUP BY and/or aggregate functions
func (e *BlockpackExecutor) executeAggregationQuery(path string, program *vm.Program, opts QueryOptions) (*BlockpackResult, error) {
	// All storage implementations must provide ProviderStorage
	providerStorage, ok := e.storage.(ProviderStorage)
	if !ok {
		return nil, fmt.Errorf("storage must implement ProviderStorage interface")
	}

	provider, err := providerStorage.GetProvider(path)
	if err != nil {
		return nil, fmt.Errorf("read blockpack file: %w", err)
	}

	// Close provider after query execution to prevent file descriptor leaks
	if closeable, ok := provider.(blockpackio.CloseableReaderProvider); ok {
		defer func() {
			_ = closeable.Close() // Ignore close errors
		}()
	}

	// Wrap provider with byte tracking and optional latency simulation
	var tracker blockpackio.ReaderProvider
	var detailedTracker *blockpackio.DetailedTrackingReader
	var standardTracker *blockpackio.TrackingReaderProvider

	if opts.DetailedIOLogging {
		// Use detailed tracking for analysis
		detailedTracker = blockpackio.NewDetailedTrackingReaderWithLatency(provider, opts.IOLatency, true)
		tracker = detailedTracker
	} else {
		// Use standard tracking
		if opts.IOLatency > 0 {
			standardTracker = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
		} else {
			standardTracker = blockpackio.NewTrackingReaderProvider(provider)
		}
		tracker = standardTracker
	}

	reader, err := blockpackio.NewReaderFromProvider(tracker)
	if err != nil {
		return nil, fmt.Errorf("decode blockpack: %w", err)
	}

	// Note: File structure info would improve categorization accuracy,
	// but Reader doesn't expose these fields. The detailed tracker will use
	// caller-based heuristics instead (good enough for identifying bottlenecks).

	// Create arena for temporary allocations during query execution
	var queryArena arena.Arena
	defer queryArena.Free()

	startTime := opts.StartTime
	endTime := opts.EndTime

	// Time bucket configuration for automatic time bucketing
	// When StartTime and EndTime are set, automatically bucket results by time intervals
	// to produce time-series results matching TraceQL metrics behavior
	var useTimeBuckets bool
	var stepSizeNs int64
	if startTime > 0 && endTime > 0 {
		useTimeBuckets = true
		stepSizeNs = 60 * 1e9 // 1 minute = 60 seconds in nanoseconds (matches TraceQL metrics)
	}

	// Use predicates extracted directly from AST during compilation
	predicates := program.Predicates
	if predicates == nil {
		predicates = &vm.QueryPredicates{
			AttributeEquals:       make(map[string][]vm.Value),
			DedicatedColumns:      make(map[string][]vm.Value),
			DedicatedColumnsRegex: make(map[string]string),
		}
	}

	// Extract time range from WHERE clause predicates and intersect with opts time range
	// This allows block pruning to use the narrowest time range from both sources:
	// 1. opts.StartTime/EndTime (time bucketing range)
	// 2. WHERE span:start >= X AND span:start < Y (actual filter predicates)
	pruneOpts := opts
	if timeRange, ok := predicates.DedicatedRanges["span:start"]; ok {
		// Intersect with WHERE clause time predicates for block pruning
		if timeRange.MinValue != nil {
			if minTime, ok := timeRange.MinValue.Data.(int64); ok {
				// Use the later of the two start times (narrower range)
				if pruneOpts.StartTime == 0 || minTime > pruneOpts.StartTime {
					pruneOpts.StartTime = minTime
				}
			}
		}
		if timeRange.MaxValue != nil {
			if maxTime, ok := timeRange.MaxValue.Data.(int64); ok {
				// Use the earlier of the two end times (narrower range)
				if pruneOpts.EndTime == 0 || maxTime < pruneOpts.EndTime {
					pruneOpts.EndTime = maxTime
				}
			}
		}
	}

	blockOrder, queryPlan := selectBlocksWithPlan(reader, predicates, pruneOpts, &queryArena)

	// Copy queryPlan slices out of arena before arena is freed
	queryPlanCopy := copyQueryPlanFromArena(queryPlan)

	blocksScanned := 0
	executionSkips := make([]ExecutionSkip, 0)

	// Extract columns needed for filtering and aggregation
	// For aggregation queries, we need ALL columns upfront (not two-pass like non-aggregation)
	// because we need both filter columns and aggregation columns for every matching row
	filterColumns := extractFilterColumns(predicates, startTime, endTime)
	aggregationColumns := extractAggregationColumns(program, opts)

	// Merge filter and aggregation columns into single set
	allColumnsNeeded := make(map[string]struct{})
	for col := range filterColumns {
		allColumnsNeeded[col] = struct{}{}
	}
	for col := range aggregationColumns {
		allColumnsNeeded[col] = struct{}{}
	}

	// Map to store aggregation buckets: groupKey -> bucket
	buckets := make(map[string]*vm.AggBucket)

	// Initialize string interner for group keys (reduces duplicate string allocations)
	stringInterner := NewStringInterner()

	// Reusable buffer for group key extraction (avoids per-span allocations)
	maxGroupByFields := len(program.AggregationPlan.GroupByFields)
	groupKeyBuffer := make([]string, maxGroupByFields)

	err = e.scanAggregationBlocks(
		reader,
		blockOrder,
		predicates,
		program,
		opts,
		allColumnsNeeded,
		startTime,
		endTime,
		useTimeBuckets,
		stepSizeNs,
		groupKeyBuffer,
		stringInterner,
		buckets,
		&queryArena,
		&blocksScanned,
		&executionSkips,
		maxNestedBlockpackDepth,
	)
	if err != nil {
		return nil, err
	}

	// Add execution-time skips to the query plan
	if queryPlanCopy != nil {
		queryPlanCopy.ExecutionSkips = executionSkips
	}

	// Collect tracking stats
	var bytesRead int64
	var ioOps int64
	var detailedStats *blockpackio.IOStats
	var analysisText string
	if detailedTracker != nil {
		bytesRead = detailedTracker.BytesRead()
		ioOps = detailedTracker.IOOperations()
		stats := detailedTracker.AnalyzeIOLog()
		detailedStats = &stats
		analysisText = detailedTracker.PrintIOAnalysis()
	} else if standardTracker != nil {
		bytesRead = standardTracker.BytesRead()
		ioOps = standardTracker.IOOperations()
	}

	// Build final result from buckets
	result := buildAggregateResult(buckets, program.AggregationPlan, queryPlanCopy, blocksScanned, bytesRead, ioOps, startTime, endTime, stepSizeNs)
	result.DetailedIOStats = detailedStats
	result.IOAnalysisText = analysisText

	return result, nil
}

// processAggregationRow handles time filtering, group key extraction, and aggregate updates for a single row.
// Returns true if the row was processed, false if filtered out.
func processAggregationRow(
	block *blockpackio.Block,
	rowIdx int,
	startTime, endTime int64,
	useTimeBuckets bool,
	stepSizeNs int64,
	groupByFields []string,
	groupKeyBuffer []string,
	stringInterner *StringInterner,
	aggregates []vm.AggSpec,
	buckets map[string]*vm.AggBucket,
) bool {
	// Apply time filtering and get span start time for bucketing
	var spanStartTime uint64
	if startTime > 0 || endTime > 0 || useTimeBuckets {
		col := block.GetColumn("span:start")
		if col != nil {
			if st, ok := col.Uint64Value(rowIdx); ok {
				spanStartTime = st
				stInt := int64(st)
				// Apply time range filter
				if (startTime > 0 && stInt < startTime) || (endTime > 0 && stInt > endTime) {
					return false
				}
			}
		}
	}

	// Extract group key values using reusable buffer
	extractGroupKeyValuesReuse(block, rowIdx, groupByFields, groupKeyBuffer)

	// Build group key with string interning
	var groupKey string
	if useTimeBuckets {
		timeBucket := (int64(spanStartTime) - startTime) / stepSizeNs
		groupKey = stringInterner.InternConcat(
			fmt.Sprintf("%d", timeBucket),
			"\x00",
			concatenateGroupKeyFromSlice(groupKeyBuffer[:len(groupByFields)]),
		)
	} else {
		groupKey = stringInterner.Intern(
			concatenateGroupKeyFromSlice(groupKeyBuffer[:len(groupByFields)]),
		)
	}

	// Get or create bucket for this group
	bucket, exists := buckets[groupKey]
	if !exists {
		bucket = &vm.AggBucket{
			Min: math.MaxFloat64,  // Initialize to +Inf for MIN aggregation
			Max: -math.MaxFloat64, // Initialize to -Inf for MAX aggregation
		}
		buckets[groupKey] = bucket
	}

	// Update aggregates for this row
	updateAggregatesForRow(block, rowIdx, aggregates, bucket)

	return true
}

// extractGroupKeyValues extracts values for GROUP BY fields from a row
func extractGroupKeyValues(block *blockpackio.Block, rowIdx int, groupByFields []string) []string {
	values := make([]string, len(groupByFields))
	for i, field := range groupByFields {
		col := block.GetColumn(field)
		if col == nil {
			// Try alternative column names
			for _, altName := range GetPossibleColumnNames(field) {
				col = block.GetColumn(altName)
				if col != nil {
					break
				}
			}
		}
		if col == nil {
			values[i] = "" // NULL value
			continue
		}

		// Extract value based on column type
		switch col.Type {
		case blockpack.ColumnTypeString:
			if v, ok := col.StringValue(rowIdx); ok {
				values[i] = v
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeInt64:
			if v, ok := col.Int64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeUint64:
			if v, ok := col.Uint64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBool:
			if v, ok := col.BoolValue(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeFloat64:
			if v, ok := col.Float64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBytes:
			if v, ok := col.BytesValueView(rowIdx); ok {
				values[i] = hex.EncodeToString(v)
			} else {
				values[i] = ""
			}
		default:
			values[i] = ""
		}
	}
	return values
}

// extractGroupKeyValuesReuse extracts values for GROUP BY fields into provided buffer
// Reuses the values slice to avoid allocation
func extractGroupKeyValuesReuse(block *blockpackio.Block, rowIdx int, groupByFields []string, values []string) {
	// Ensure buffer is large enough
	if len(values) < len(groupByFields) {
		panic(fmt.Sprintf("extractGroupKeyValuesReuse: values buffer too small (got %d, need %d)", len(values), len(groupByFields)))
	}

	for i, field := range groupByFields {
		col := block.GetColumn(field)
		if col == nil {
			// Try alternative column names
			for _, altName := range GetPossibleColumnNames(field) {
				col = block.GetColumn(altName)
				if col != nil {
					break
				}
			}
		}
		if col == nil {
			values[i] = "" // NULL value
			continue
		}

		// Extract value based on column type
		switch col.Type {
		case blockpack.ColumnTypeString:
			if v, ok := col.StringValue(rowIdx); ok {
				values[i] = v
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeInt64:
			if v, ok := col.Int64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeUint64:
			if v, ok := col.Uint64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBool:
			if v, ok := col.BoolValue(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeFloat64:
			if v, ok := col.Float64Value(rowIdx); ok {
				values[i] = formatValueAsString(v)
			} else {
				values[i] = ""
			}
		case blockpack.ColumnTypeBytes:
			if v, ok := col.BytesValueView(rowIdx); ok {
				values[i] = hex.EncodeToString(v)
			} else {
				values[i] = ""
			}
		default:
			values[i] = ""
		}
	}
}

// concatenateGroupKey creates a unique string key from group values
func concatenateGroupKey(values []string) string {
	if len(values) == 0 {
		return "" // Global aggregation (no GROUP BY)
	}
	// Use a delimiter that's unlikely to appear in values
	return strings.Join(values, "\x00")
}

// concatenateGroupKeyFromSlice builds a group key from string slice (no allocation)
func concatenateGroupKeyFromSlice(values []string) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) == 1 {
		return values[0]
	}

	// Calculate total length
	totalLen := len(values) - 1 // separators (null bytes)
	for _, v := range values {
		totalLen += len(v)
	}

	// Build string efficiently
	var b strings.Builder
	b.Grow(totalLen)
	for i, v := range values {
		if i > 0 {
			b.WriteByte('\x00')
		}
		b.WriteString(v)
	}
	return b.String()
}

// formatValueAsString converts a value to string representation
func formatValueAsString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		return hex.EncodeToString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// updateAggregatesForRow updates all aggregate values for a single row
func updateAggregatesForRow(block *blockpackio.Block, rowIdx int, aggregates []vm.AggSpec, bucket *vm.AggBucket) {
	for _, agg := range aggregates {
		switch agg.Function {
		case vm.AggCount:
			updateBucketCount(bucket)

		case vm.AggRate:
			// RATE is count-based like COUNT, but finalized as count/time_range
			updateBucketCount(bucket)

		case vm.AggSum, vm.AggAvg, vm.AggMin, vm.AggMax:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}

			switch agg.Function {
			case vm.AggSum, vm.AggAvg:
				updateBucketSum(bucket, value)
				updateBucketCount(bucket) // Need count for AVG
			case vm.AggMin:
				updateBucketMin(bucket, value)
			case vm.AggMax:
				updateBucketMax(bucket, value)
			}

		case vm.AggQuantile:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketQuantile(bucket, agg.Field, value)

		case vm.AggHistogram:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketHistogram(bucket, agg.Field, value)

		case vm.AggStddev:
			// Extract field value and convert to float64
			value, ok := convertToFloat64(block, rowIdx, agg.Field)
			if !ok {
				continue // Skip NULL values
			}
			updateBucketStddev(bucket, value)
		}
	}
}

// convertToFloat64 extracts a column value and converts it to float64
func convertToFloat64(block *blockpackio.Block, rowIdx int, field string) (float64, bool) {
	col := block.GetColumn(field)
	if col == nil {
		// Try alternative column names
		for _, altName := range GetPossibleColumnNames(field) {
			col = block.GetColumn(altName)
			if col != nil {
				break
			}
		}
	}
	if col == nil {
		// Special case: if span:duration column doesn't exist, compute it from span:end - span:start
		if field == "span:duration" {
			startCol := block.GetColumn("span:start")
			endCol := block.GetColumn("span:end")
			if startCol != nil && endCol != nil {
				if start, ok := startCol.Uint64Value(rowIdx); ok {
					if end, ok := endCol.Uint64Value(rowIdx); ok {
						duration := int64(end) - int64(start)
						return float64(duration), true
					}
				}
			}
		}
		return 0, false
	}

	switch col.Type {
	case blockpack.ColumnTypeInt64:
		if v, ok := col.Int64Value(rowIdx); ok {
			return float64(v), true
		}
	case blockpack.ColumnTypeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return float64(v), true
		}
	case blockpack.ColumnTypeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return v, true
		}
	case blockpack.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			if v {
				return 1.0, true
			}
			return 0.0, true
		}
	}

	return 0, false
}

// buildAggregateResult constructs the final BlockpackResult from aggregation buckets
func buildAggregateResult(buckets map[string]*vm.AggBucket, plan *vm.AggregationPlan, queryPlan *QueryPlan, blocksScanned int, bytesRead int64, ioOperations int64, startTime int64, endTime int64, stepSizeNs int64) *BlockpackResult {
	// Determine if time bucketing was used
	useTimeBuckets := (startTime > 0 && endTime > 0)

	// For dense time-series, we need to generate ALL time buckets, not just those with data
	if useTimeBuckets {
		return buildDenseTimeSeriesResult(buckets, plan, queryPlan, blocksScanned, bytesRead, ioOperations, startTime, endTime, stepSizeNs)
	}

	// Original sparse implementation for non-time-series queries
	rows := make([]AggregateRow, 0, len(buckets))

	for groupKey, bucket := range buckets {
		// Parse group key back into values
		var groupKeyValues []string
		if groupKey == "" {
			// Global aggregation (no GROUP BY, no time bucketing)
			groupKeyValues = []string{}
		} else {
			parts := strings.Split(groupKey, "\x00")
			// Filter out empty strings (can occur for global aggregation with time bucketing)
			// Global aggregation with time bucketing: groupKey = "0\x00" -> split to ["0", ""]
			// We want just ["0"]
			groupKeyValues = make([]string, 0, len(parts))
			for _, part := range parts {
				if part != "" {
					groupKeyValues = append(groupKeyValues, part)
				}
			}
		}

		// Build values map from aggregates
		values := make(map[string]float64)
		for _, agg := range plan.Aggregates {
			// Determine output name (use alias if provided, otherwise generate default)
			outputName := agg.Alias
			if outputName == "" {
				// Generate default name like "count", "sum_duration", etc.
				// Strip scope prefixes from field names for cleaner output (span.duration -> duration)
				fieldName := agg.Field
				if strings.HasPrefix(fieldName, "span:") {
					fieldName = strings.TrimPrefix(fieldName, "span:")
				} else if strings.HasPrefix(fieldName, "span.") {
					fieldName = strings.TrimPrefix(fieldName, "span.")
				} else if strings.HasPrefix(fieldName, "resource:") {
					fieldName = strings.TrimPrefix(fieldName, "resource:")
				} else if strings.HasPrefix(fieldName, "resource.") {
					fieldName = strings.TrimPrefix(fieldName, "resource.")
				}

				switch agg.Function {
				case vm.AggCount:
					outputName = "count"
				case vm.AggRate:
					outputName = "rate"
				case vm.AggSum:
					outputName = "sum_" + fieldName
				case vm.AggAvg:
					outputName = "avg_" + fieldName
				case vm.AggMin:
					outputName = "min_" + fieldName
				case vm.AggMax:
					outputName = "max_" + fieldName
				case vm.AggQuantile:
					outputName = fmt.Sprintf("quantile_%s_%g", fieldName, agg.Quantile)
				case vm.AggHistogram:
					outputName = "histogram_" + fieldName
				case vm.AggStddev:
					outputName = "stddev_" + fieldName
				}
			}

			// Extract final value
			var finalValue float64
			switch agg.Function {
			case vm.AggCount:
				finalValue = float64(bucket.Count)
			case vm.AggRate:
				// RATE = count / time_range_seconds
				// Use entire query range for rate calculation (no time bucketing)
				timeRangeSeconds := float64(endTime-startTime) / 1e9
				finalValue = finalizeRate(bucket, timeRangeSeconds)
			case vm.AggSum:
				finalValue = bucket.Sum
			case vm.AggAvg:
				finalValue = finalizeAvg(bucket)
			case vm.AggMin:
				// If Min was never updated, it's still MaxFloat64 - return 0 or NaN
				if bucket.Min == math.MaxFloat64 {
					finalValue = 0
				} else {
					finalValue = bucket.Min
				}
			case vm.AggMax:
				// If Max was never updated, it's still -MaxFloat64 - return 0 or NaN
				if bucket.Max == -math.MaxFloat64 {
					finalValue = 0
				} else {
					finalValue = bucket.Max
				}
			case vm.AggQuantile:
				finalValue = extractBucketQuantile(bucket, agg.Field, agg.Quantile)
			case vm.AggHistogram:
				// For histogram, we return 0 as a placeholder
				// Real histogram data would be returned in a different format
				finalValue = 0
			case vm.AggStddev:
				finalValue = finalizeStddev(bucket)
			}

			values[outputName] = finalValue
		}

		rows = append(rows, AggregateRow{
			GroupKey: groupKeyValues,
			Values:   values,
		})
	}

	// Sort rows by group key for deterministic output
	sort.Slice(rows, func(i, j int) bool {
		// Compare group keys lexicographically
		for k := 0; k < len(rows[i].GroupKey) && k < len(rows[j].GroupKey); k++ {
			if rows[i].GroupKey[k] != rows[j].GroupKey[k] {
				return rows[i].GroupKey[k] < rows[j].GroupKey[k]
			}
		}
		return len(rows[i].GroupKey) < len(rows[j].GroupKey)
	})

	return &BlockpackResult{
		Matches:       nil, // No individual matches for aggregation queries
		BlocksScanned: blocksScanned,
		BytesRead:     bytesRead,
		IOOperations:  ioOperations,
		QueryPlan:     queryPlan,
		AggregateRows: rows,
		IsAggregated:  true,
	}
}

// buildDenseTimeSeriesResult generates dense time-series with ALL time buckets (including empty ones)
// This matches TraceQL metrics behavior which always returns a value for every time interval
func buildDenseTimeSeriesResult(buckets map[string]*vm.AggBucket, plan *vm.AggregationPlan, queryPlan *QueryPlan, blocksScanned int, bytesRead int64, ioOperations int64, startTime int64, endTime int64, stepSizeNs int64) *BlockpackResult {
	numBuckets := denseBucketCount(startTime, endTime, stepSizeNs)
	uniqueAttributeGroups := extractDenseAttributeGroups(buckets)
	rows := buildDenseAggregateRows(buckets, plan, numBuckets, uniqueAttributeGroups, stepSizeNs)
	sortDenseAggregateRows(rows)

	return buildDenseAggregateResult(rows, queryPlan, blocksScanned, bytesRead, ioOperations)
}

func denseBucketCount(startTime, endTime, stepSizeNs int64) int64 {
	return (endTime - startTime + stepSizeNs - 1) / stepSizeNs
}

func extractDenseAttributeGroups(buckets map[string]*vm.AggBucket) map[string]struct{} {
	uniqueAttributeGroups := make(map[string]struct{})
	for groupKey := range buckets {
		attrGroupKey := denseAttributeGroupKey(groupKey)
		uniqueAttributeGroups[attrGroupKey] = struct{}{}
	}
	return uniqueAttributeGroups
}

func denseAttributeGroupKey(groupKey string) string {
	if groupKey == "" {
		return ""
	}
	parts := strings.Split(groupKey, "\x00")
	if len(parts) > 1 {
		return strings.Join(parts[1:], "\x00")
	}
	return ""
}

func buildDenseAggregateRows(buckets map[string]*vm.AggBucket, plan *vm.AggregationPlan, numBuckets int64, uniqueAttributeGroups map[string]struct{}, stepSizeNs int64) []AggregateRow {
	rows := make([]AggregateRow, 0, int(numBuckets)*len(uniqueAttributeGroups))
	for attrGroupKey := range uniqueAttributeGroups {
		attrGroupValues := denseAttributeGroupValues(attrGroupKey)
		for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
			fullGroupKey := denseFullGroupKey(bucketIdx, attrGroupKey)
			bucket := buckets[fullGroupKey]
			if bucket == nil {
				bucket = emptyAggBucket()
			}

			groupKeyValues := make([]string, 0, 1+len(attrGroupValues))
			groupKeyValues = append(groupKeyValues, fmt.Sprintf("%d", bucketIdx))
			groupKeyValues = append(groupKeyValues, attrGroupValues...)

			values := buildDenseAggregateValues(bucket, plan, stepSizeNs)
			rows = append(rows, AggregateRow{
				GroupKey: groupKeyValues,
				Values:   values,
			})
		}
	}
	return rows
}

func denseAttributeGroupValues(attrGroupKey string) []string {
	if attrGroupKey == "" {
		return []string{}
	}
	parts := strings.Split(attrGroupKey, "\x00")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func denseFullGroupKey(bucketIdx int64, attrGroupKey string) string {
	if attrGroupKey == "" {
		return fmt.Sprintf("%d\x00", bucketIdx)
	}
	return fmt.Sprintf("%d\x00%s", bucketIdx, attrGroupKey)
}

func emptyAggBucket() *vm.AggBucket {
	return &vm.AggBucket{
		Count: 0,
		Sum:   0,
		Min:   math.MaxFloat64,
		Max:   -math.MaxFloat64,
	}
}

func buildDenseAggregateValues(bucket *vm.AggBucket, plan *vm.AggregationPlan, stepSizeNs int64) map[string]float64 {
	values := make(map[string]float64, len(plan.Aggregates))
	for _, agg := range plan.Aggregates {
		outputName := aggregationOutputName(agg)
		values[outputName] = aggregationValue(bucket, agg, stepSizeNs)
	}
	return values
}

func aggregationOutputName(agg vm.AggSpec) string {
	if agg.Alias != "" {
		return agg.Alias
	}
	fieldName := strings.TrimPrefix(agg.Field, "span:")
	fieldName = strings.TrimPrefix(fieldName, "span.")
	fieldName = strings.TrimPrefix(fieldName, "resource:")
	fieldName = strings.TrimPrefix(fieldName, "resource.")

	switch agg.Function {
	case vm.AggCount:
		return "count"
	case vm.AggRate:
		return "rate"
	case vm.AggSum:
		return "sum_" + fieldName
	case vm.AggAvg:
		return "avg_" + fieldName
	case vm.AggMin:
		return "min_" + fieldName
	case vm.AggMax:
		return "max_" + fieldName
	case vm.AggQuantile:
		return fmt.Sprintf("quantile_%s_%g", fieldName, agg.Quantile)
	case vm.AggHistogram:
		return "histogram_" + fieldName
	case vm.AggStddev:
		return "stddev_" + fieldName
	}
	functionName := strings.ToLower(agg.Function.String())
	if fieldName == "" {
		return "agg_" + functionName
	}
	return fieldName + "_" + functionName
}

func aggregationValue(bucket *vm.AggBucket, agg vm.AggSpec, stepSizeNs int64) float64 {
	switch agg.Function {
	case vm.AggCount:
		return float64(bucket.Count)
	case vm.AggRate:
		timeRangeSeconds := float64(stepSizeNs) / 1e9
		return finalizeRate(bucket, timeRangeSeconds)
	case vm.AggSum:
		return bucket.Sum
	case vm.AggAvg:
		return finalizeAvg(bucket)
	case vm.AggMin:
		if bucket.Min == math.MaxFloat64 {
			return 0
		}
		return bucket.Min
	case vm.AggMax:
		if bucket.Max == -math.MaxFloat64 {
			return 0
		}
		return bucket.Max
	case vm.AggQuantile:
		return extractBucketQuantile(bucket, agg.Field, agg.Quantile)
	case vm.AggHistogram:
		return 0
	case vm.AggStddev:
		return finalizeStddev(bucket)
	}
	return 0
}

func sortDenseAggregateRows(rows []AggregateRow) {
	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(rows[i].GroupKey) && k < len(rows[j].GroupKey); k++ {
			if rows[i].GroupKey[k] != rows[j].GroupKey[k] {
				if k == 0 {
					iVal, err := strconv.ParseInt(rows[i].GroupKey[k], 10, 64)
					if err != nil {
						return false
					}
					jVal, err := strconv.ParseInt(rows[j].GroupKey[k], 10, 64)
					if err != nil {
						return false
					}
					return iVal < jVal
				}
				return rows[i].GroupKey[k] < rows[j].GroupKey[k]
			}
		}
		return len(rows[i].GroupKey) < len(rows[j].GroupKey)
	})
}

func buildDenseAggregateResult(rows []AggregateRow, queryPlan *QueryPlan, blocksScanned int, bytesRead int64, ioOperations int64) *BlockpackResult {
	return &BlockpackResult{
		Matches:       nil,
		BlocksScanned: blocksScanned,
		BytesRead:     bytesRead,
		IOOperations:  ioOperations,
		QueryPlan:     queryPlan,
		AggregateRows: rows,
		IsAggregated:  true,
	}
}
