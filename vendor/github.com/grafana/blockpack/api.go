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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	minio "github.com/minio/minio-go/v7"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_compaction "github.com/grafana/blockpack/internal/modules/blockio/compaction"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_filecache "github.com/grafana/blockpack/internal/modules/filecache"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/blockpack/internal/s3provider"
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
type DataType = modules_rw.DataType

// DataType constants for read optimization hints.
// Priority order (highest → lowest): Footer ≈ Header > Metadata ≈ TraceBloomFilter >
// TimestampIndex > Block. Caching layers use this ordering for eviction decisions.
const (
	DataTypeUnknown          = modules_rw.DataTypeUnknown
	DataTypeFooter           = modules_rw.DataTypeFooter
	DataTypeHeader           = modules_rw.DataTypeHeader
	DataTypeMetadata         = modules_rw.DataTypeMetadata
	DataTypeTraceBloomFilter = modules_rw.DataTypeTraceBloomFilter
	DataTypeTimestampIndex   = modules_rw.DataTypeTimestampIndex
	DataTypeBlock            = modules_rw.DataTypeBlock
)

// SharedLRUCache is a byte-bounded, priority-tiered LRU cache shared across readers.
// Higher-priority data types (Footer, Header, TraceBloomFilter) survive cache pressure
// from block reads and are evicted last.
type SharedLRUCache = modules_rw.SharedLRUCache

// NewSharedLRUCache creates a SharedLRUCache with the given total byte capacity.
// The cache is safe for concurrent use and can be shared across multiple readers.
func NewSharedLRUCache(maxBytes int64) *SharedLRUCache {
	return modules_rw.NewSharedLRUCache(maxBytes)
}

// NewSharedLRUProvider wraps underlying with a caching layer backed by a shared LRU cache.
// readerID uniquely identifies this reader within the cache (e.g. file path or object key).
// The returned provider satisfies ReaderProvider and is safe for concurrent use.
func NewSharedLRUProvider(underlying ReaderProvider, readerID string, cache *SharedLRUCache) ReaderProvider {
	adapted := &readerProviderAdapter{provider: underlying}
	return modules_rw.NewSharedLRUProvider(adapted, readerID, cache)
}

// FileCache is a disk-backed, size-bounded byte cache for blockpack file sections
// (footer, header, metadata, blocks). It deduplicates concurrent fetches for the
// same key so that many goroutines opening the same file share a single I/O.
// A nil *FileCache is safe to use; all operations become pass-throughs.
type FileCache = modules_filecache.FileCache

// FileCacheConfig configures a disk-backed FileCache.
type FileCacheConfig struct {
	// Path is the file path of the bbolt database used for storage.
	Path string

	// MaxBytes is the maximum total bytes stored on disk.
	// Oldest entries (FIFO) are evicted when the limit is exceeded.
	MaxBytes int64

	// Enabled controls whether the cache is active.
	// When false, OpenFileCache returns (nil, nil) and readers skip all caching.
	Enabled bool
}

// OpenFileCache opens (or creates) a FileCache with the given configuration.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call FileCache.Close() when done.
func OpenFileCache(cfg FileCacheConfig) (*FileCache, error) {
	return modules_filecache.Open(modules_filecache.Config{
		Enabled:  cfg.Enabled,
		MaxBytes: cfg.MaxBytes,
		Path:     cfg.Path,
	})
}

// Signal type constants for blockpack file discrimination.
// SignalTypeLog is returned by Reader.SignalType() for log blockpack files.
// SignalTypeTrace is the default for trace blockpack files (version < 12).
const (
	SignalTypeTrace = modules_shared.SignalTypeTrace
	SignalTypeLog   = modules_shared.SignalTypeLog
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

// NewReaderWithCache creates a Reader that caches footer, header, metadata, and block
// reads in the provided FileCache. fileID must uniquely identify the file within the
// cache namespace — typically the file path or object storage key.
// A nil cache falls back to uncached reads.
func NewReaderWithCache(provider ReaderProvider, fileID string, cache *FileCache) (*Reader, error) {
	wrapped := &readerProviderAdapter{provider: provider}
	return modules_reader.NewReaderFromProviderWithOptions(wrapped, modules_reader.Options{
		Cache:  cache,
		FileID: fileID,
	})
}

// NewLeanReaderFromProvider creates a lean Reader using only 2 I/Os (footer + compact
// trace index). Ideal for GetTraceByID workloads. Falls back to NewReaderFromProvider
// for files without a compact trace index (v3 footer).
func NewLeanReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	wrapped := &readerProviderAdapter{provider: provider}
	return modules_reader.NewLeanReaderFromProvider(wrapped)
}

// NewLeanReaderWithCache creates a lean Reader with disk caching. Uses the same 2-I/O
// path as NewLeanReaderFromProvider but caches footer and compact index reads.
// fileID must uniquely identify the file within the cache namespace.
func NewLeanReaderWithCache(provider ReaderProvider, fileID string, cache *FileCache) (*Reader, error) {
	wrapped := &readerProviderAdapter{provider: provider}
	return modules_reader.NewLeanReaderFromProviderWithOptions(wrapped, modules_reader.Options{
		Cache:  cache,
		FileID: fileID,
	})
}

// GetTraceByID looks up all spans for the given trace ID and returns them.
// traceIDHex must be a 32-character lowercase hex string (16 bytes).
// Returns an empty slice (not an error) when the trace is not found.
// Use NewLeanReaderFromProvider for the lowest-I/O path.
func GetTraceByID(r *Reader, traceIDHex string) (results []SpanMatch, err error) {
	if r == nil {
		return nil, fmt.Errorf("GetTraceByID: reader cannot be nil")
	}

	if len(traceIDHex) != 32 {
		return nil, fmt.Errorf("GetTraceByID: traceIDHex must be 32 hex chars, got %d", len(traceIDHex))
	}

	traceIDBytes, decErr := hex.DecodeString(traceIDHex)
	if decErr != nil {
		return nil, fmt.Errorf("GetTraceByID: invalid trace ID hex: %w", decErr)
	}

	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	entries := r.TraceEntries(traceID)
	blockIDs := make([]int, len(entries))
	for i, e := range entries {
		blockIDs[i] = e.BlockID
	}
	rawMap := make(map[int][]byte)
	for _, group := range r.CoalescedGroups(blockIDs) {
		groupRaw, fetchErr := r.ReadGroup(group)
		if fetchErr != nil {
			return nil, fmt.Errorf("GetTraceByID: read group: %w", fetchErr)
		}
		for bi, raw := range groupRaw {
			rawMap[bi] = raw
		}
	}

	for _, entry := range entries {
		raw, ok := rawMap[entry.BlockID]
		if !ok {
			continue
		}
		bwb, blockErr := r.ParseBlockFromBytes(raw, nil, r.BlockMeta(entry.BlockID))
		if blockErr != nil {
			return nil, fmt.Errorf("GetTraceByID: block %d: %w", entry.BlockID, blockErr)
		}

		// NOTE-37: scan the trace:id column for rows belonging to this trace.
		traceIDCol := bwb.Block.GetColumn(
			"trace:id",
		) // matches traceIDColumnName in internal/modules/blockio/writer/constants.go
		if traceIDCol == nil {
			continue
		}
		for rowIdx := range bwb.Block.SpanCount() {
			v, ok := traceIDCol.BytesValue(rowIdx)
			if !ok || !bytes.Equal(v, traceID[:]) {
				continue
			}
			fields := modules_blockio.NewSpanFieldsAdapter(bwb.Block, rowIdx)
			traceIDStr, spanIDStr := extractIDs(bwb.Block, rowIdx)
			match := SpanMatch{
				Fields:  fields,
				TraceID: traceIDStr,
				SpanID:  spanIDStr,
			}
			results = append(results, match.Clone())
		}
	}

	return results, nil
}

// readerProviderAdapter adapts the public ReaderProvider to modules_rw.ReaderProvider.
type readerProviderAdapter struct {
	provider ReaderProvider
}

func (a *readerProviderAdapter) Size() (int64, error) {
	return a.provider.Size()
}

func (a *readerProviderAdapter) ReadAt(p []byte, off int64, dataType modules_rw.DataType) (int, error) {
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
	// MostRecent controls block traversal order. Always use keyed struct literals:
	// QueryOptions{Limit: 10, MostRecent: true}.
	// With Limit > 0, uses CollectTopK for globally newest-first ordering by span:start.
	// With Limit == 0, ordering is per-block newest-first; not globally sorted when block ranges overlap.
	MostRecent bool
	// StartNano is the inclusive lower bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range ends before StartNano are skipped entirely.
	// 0 means no lower bound.
	StartNano uint64
	// EndNano is the inclusive upper bound for block-level time pruning (unix nanoseconds).
	// Internal blocks whose span:start range begins after EndNano are skipped entirely.
	// 0 means no upper bound.
	EndNano uint64
	// StartBlock is the first internal block index to scan (0-based, inclusive).
	// Used by the frontend sharder to partition a single blockpack file into
	// multiple sub-file jobs. 0 means start from the first block.
	StartBlock int
	// BlockCount is the number of internal blocks to scan starting from StartBlock.
	// 0 means scan all blocks (no sub-file sharding).
	BlockCount int
}

// validateQueryOptions checks that sharding and time range parameters are valid.
func validateQueryOptions(opts QueryOptions) error {
	if opts.StartBlock < 0 {
		return fmt.Errorf("invalid StartBlock %d: must be >= 0", opts.StartBlock)
	}
	if opts.BlockCount < 0 {
		return fmt.Errorf("invalid BlockCount %d: must be >= 0", opts.BlockCount)
	}
	if opts.BlockCount > 0 {
		end := opts.StartBlock + opts.BlockCount
		if end < opts.StartBlock { // overflow
			return fmt.Errorf("invalid shard range: StartBlock + BlockCount overflows int")
		}
	}
	if opts.StartBlock != 0 && opts.BlockCount == 0 {
		return fmt.Errorf(
			"invalid shard range: StartBlock=%d has no effect when BlockCount=0 (scan all)",
			opts.StartBlock,
		)
	}
	if opts.StartNano > 0 && opts.EndNano > 0 && opts.StartNano > opts.EndNano {
		return fmt.Errorf("invalid time range: StartNano (%d) > EndNano (%d)", opts.StartNano, opts.EndNano)
	}
	return nil
}

// normalizeTimeRange converts StartNano/EndNano into a TimeRange, treating
// EndNano==0 as "no upper bound" (math.MaxUint64) so the TS-index fast path
// does not incorrectly prune all blocks.
func normalizeTimeRange(startNano, endNano uint64) modules_queryplanner.TimeRange {
	if startNano > 0 && endNano == 0 {
		endNano = math.MaxUint64
	}
	return modules_queryplanner.TimeRange{MinNano: startNano, MaxNano: endNano}
}

// SpanMatch represents a single span that matched the query.
// Fields is safe to retain after QueryTraceQL or QueryLogQL returns.
type SpanMatch struct {
	Fields  SpanFieldsProvider
	TraceID string
	SpanID  string
}

// Clone materializes all fields from the lazy provider and returns a deep copy
// with stable ownership, safe to hold beyond any internal callback or function return.
// If Fields is nil (e.g. structural queries), the clone has nil Fields.
func (m *SpanMatch) Clone() SpanMatch {
	out := SpanMatch{TraceID: m.TraceID, SpanID: m.SpanID}
	if m.Fields == nil {
		return out
	}
	materialized := make(map[string]any, 32) // pre-size to avoid grow/rehash for typical label counts
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

// spanMatchFn is the internal callback type used by streaming query helpers.
// match is valid only for the duration of the call; more=false signals end of results.
type spanMatchFn func(match *SpanMatch, more bool) bool

// QueryTraceQL executes a TraceQL query against a modules-format blockpack file
// and returns all matching spans.
//
// Supported query types:
//   - Filter expressions: `{ span.http.method = "GET" }`
//   - Structural queries: `{ expr } OP { expr }` where OP is >>, >, ~, <<, <, !~
//
// For filter queries, each SpanMatch.Fields supports GetField and IterateFields.
// For structural queries, SpanMatch.Fields is nil — only TraceID and SpanID are set.
// Metrics queries return an error.
// ColumnNames returns all column names known to this reader — the union of
// range-indexed and sketch columns. Derived from file header metadata; no
// block I/O is required. Returns a sorted slice of column name strings
// (e.g. "span:status", "resource.service.name", "span.http.method").
func ColumnNames(r *Reader) []string {
	return r.ColumnNames()
}

func QueryTraceQL(r *Reader, traceqlQuery string, opts QueryOptions) (results []SpanMatch, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("QueryTraceQL: reader cannot be nil")
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, fmt.Errorf("QueryTraceQL: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse TraceQL: %w", parseErr)
	}

	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}

	switch q := parsed.(type) {
	case *traceqlparser.FilterExpression:
		err = streamFilterQuery(r, q, opts, collector)
	case *traceqlparser.StructuralQuery:
		err = streamStructuralQuery(r, q, opts, collector)
	case *traceqlparser.MetricsQuery:
		err = streamPipelineQuery(r, q, opts, collector)
	default:
		err = fmt.Errorf(
			"QueryTraceQL: query type %T is not supported",
			parsed,
		)
	}
	return results, err
}

// streamPipelineQuery executes a TraceQL pipeline query ({filter} | aggregate() > threshold).
// It runs the filter part, groups matching spans into spansets by trace ID, applies the
// aggregate function per spanset, filters by threshold, and emits qualifying spans.
func streamPipelineQuery(r *Reader, mq *traceqlparser.MetricsQuery, opts QueryOptions, fn spanMatchFn) error {
	// Compile and execute the filter part to get all matching spans.
	program, compileErr := vm.CompileTraceQLFilter(mq.Filter)
	if compileErr != nil {
		return fmt.Errorf("compile pipeline filter: %w", compileErr)
	}

	// Run with no limit — we need all matching spans to compute aggregates.
	filterOpts := opts
	filterOpts.Limit = 0

	var allSpans []SpanMatch
	streamErr := streamFilterProgram(r, program, filterOpts, func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		allSpans = append(allSpans, match.Clone())
		return true
	})
	if streamErr != nil {
		return streamErr
	}

	const maxPipelineSpans = 1_000_000
	if len(allSpans) > maxPipelineSpans {
		return fmt.Errorf(
			"pipeline query matched %d spans (max %d); narrow your filter",
			len(allSpans),
			maxPipelineSpans,
		)
	}

	pipeline := mq.Pipeline

	if pipeline == nil {
		// No pipeline stages — emit all spans, respecting limit.
		remaining := opts.Limit
		for i := range allSpans {
			if !fn(&allSpans[i], true) {
				break
			}
			if remaining > 0 {
				remaining--
				if remaining == 0 {
					break
				}
			}
		}
		fn(nil, false)
		return nil
	}

	// If pipeline has no aggregate (just by/select), emit all spans, respecting limit.
	if pipeline.Aggregate.Name == "" {
		remaining := opts.Limit
		for i := range allSpans {
			if !fn(&allSpans[i], true) {
				break
			}
			if remaining > 0 {
				remaining--
				if remaining == 0 {
					break
				}
			}
		}
		fn(nil, false)
		return nil
	}

	// Group spans by trace ID into spansets.
	type spanset struct {
		spans []int // indices into allSpans
	}
	traceGroups := make(map[string]*spanset)
	traceOrder := make([]string, 0)
	for i, sp := range allSpans {
		tid := sp.TraceID
		if tid == "" {
			tid = "__no_trace_id__"
		}
		ss, ok := traceGroups[tid]
		if !ok {
			ss = &spanset{}
			traceGroups[tid] = ss
			traceOrder = append(traceOrder, tid)
		}
		ss.spans = append(ss.spans, i)
	}

	// Compute aggregate per spanset and filter by threshold.
	aggName := pipeline.Aggregate.Name
	aggField := pipeline.Aggregate.Field

	// Track limit globally across all traces, not per-trace.
	limit := opts.Limit
	for _, tid := range traceOrder {
		ss := traceGroups[tid]

		aggVal, ok := computeSpansetAggregate(aggName, aggField, ss.spans, allSpans)
		if !ok {
			continue
		}

		if pipeline.HasThreshold {
			if !compareThreshold(aggVal, pipeline.ThresholdOp, pipeline.ThresholdVal) {
				continue
			}
		}

		// Emit all spans from this qualifying spanset.
		for _, idx := range ss.spans {
			if !fn(&allSpans[idx], true) {
				fn(nil, false)
				return nil
			}
			if limit > 0 {
				limit--
				if limit == 0 {
					fn(nil, false)
					return nil
				}
			}
		}
	}

	fn(nil, false)
	return nil
}

// computeSpansetAggregate computes an aggregate value over a spanset.
func computeSpansetAggregate(aggName, aggField string, indices []int, allSpans []SpanMatch) (float64, bool) {
	switch aggName {
	case "count", "count_over_time":
		return float64(len(indices)), true

	case "avg", "min", "max", "sum":
		var values []float64
		for _, idx := range indices {
			v, ok := getSpanFieldNumeric(allSpans[idx], aggField)
			if ok {
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			return 0, false
		}
		switch aggName {
		case "avg":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum / float64(len(values)), true
		case "min":
			minVal := values[0]
			for _, v := range values[1:] {
				if v < minVal {
					minVal = v
				}
			}
			return minVal, true
		case "max":
			maxVal := values[0]
			for _, v := range values[1:] {
				if v > maxVal {
					maxVal = v
				}
			}
			return maxVal, true
		case "sum":
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum, true
		}
	default:
		slog.Warn("unsupported aggregate in pipeline query", "aggregate", aggName)
	}
	return 0, false
}

// getSpanFieldNumeric extracts a numeric value from a span's fields.
// It matches fieldName exactly first, then falls back to unscoped/scoped variants
// (e.g. "duration" matches "span:duration", and "span.duration" matches "duration").
func getSpanFieldNumeric(sp SpanMatch, fieldName string) (float64, bool) {
	if sp.Fields == nil {
		return 0, false
	}
	var result float64
	var found bool
	sp.Fields.IterateFields(func(name string, value any) bool {
		if !fieldNameMatches(name, fieldName) {
			return true
		}
		switch v := value.(type) {
		case int64:
			result = float64(v)
			found = true
		case uint64:
			result = float64(v)
			found = true
		case float64:
			result = v
			found = true
		case int:
			result = float64(v)
			found = true
		}
		return false // stop iteration
	})
	return result, found
}

// fieldNameMatches returns true if emitted matches the requested field name,
// accounting for scope prefix differences (e.g. "span:duration" vs "duration",
// "span.http.method" vs "http.method").
func fieldNameMatches(emitted, requested string) bool {
	if emitted == requested {
		return true
	}
	// Try stripping "span:" or "span." prefix from emitted name.
	for _, prefix := range []string{"span:", "span."} {
		if strings.HasPrefix(emitted, prefix) {
			if emitted[len(prefix):] == requested {
				return true
			}
		}
		if strings.HasPrefix(requested, prefix) {
			if requested[len(prefix):] == emitted {
				return true
			}
		}
	}
	return false
}

// compareThreshold compares an aggregate value against a threshold.
func compareThreshold(aggVal float64, op traceqlparser.BinaryOp, threshold interface{}) bool {
	var thresholdF float64
	switch v := threshold.(type) {
	case int64:
		thresholdF = float64(v)
	case float64:
		thresholdF = v
	default:
		return false
	}

	switch op {
	case traceqlparser.OpGt:
		return aggVal > thresholdF
	case traceqlparser.OpGte:
		return aggVal >= thresholdF
	case traceqlparser.OpLt:
		return aggVal < thresholdF
	case traceqlparser.OpLte:
		return aggVal <= thresholdF
	case traceqlparser.OpEq:
		return math.Abs(aggVal-thresholdF) < 1e-9
	case traceqlparser.OpNeq:
		return math.Abs(aggVal-thresholdF) >= 1e-9
	default:
		return false
	}
}

// streamFilterQuery executes a TraceQL filter query against a modules-format reader.
func streamFilterQuery(r *Reader, filterExpr *traceqlparser.FilterExpression, opts QueryOptions, fn spanMatchFn) error {
	program, compileErr := vm.CompileTraceQLFilter(filterExpr)
	if compileErr != nil {
		return fmt.Errorf("compile TraceQL filter: %w", compileErr)
	}

	return streamFilterProgram(r, program, opts, fn)
}

// streamFilterProgram executes a compiled filter program against a modules-format reader.
func streamFilterProgram(r *Reader, program *vm.Program, opts QueryOptions, fn spanMatchFn) error {
	// SPEC-STREAM-8: MostRecent maps to Backward direction with span:start timestamp sorting.
	// span:start is always present in searchMetaColumns so no extra I/O is needed.
	// When MostRecent+Limit, CollectTopK gives globally top-K results by span:start.
	collectOpts := modules_executor.CollectOptions{
		TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
		Limit:      opts.Limit,
		StartBlock: opts.StartBlock,
		BlockCount: opts.BlockCount,
		// NOTE-028: AllColumns defaults to false — for Collect this means the second pass decodes only
		// searchMetaColumns ∪ predicate columns. The MostRecent+Limit path uses CollectTopK, which
		// currently performs a full decode on the second pass.
	}
	if opts.MostRecent {
		collectOpts.Direction = modules_queryplanner.Backward
		collectOpts.TimestampColumn = "span:start"
	}
	ex := modules_executor.New()
	var (
		rows []modules_executor.MatchedRow
		err  error
	)
	if opts.MostRecent && opts.Limit > 0 {
		rows, err = ex.CollectTopK(r, program, collectOpts)
	} else {
		rows, err = ex.Collect(r, program, collectOpts)
	}
	if err != nil {
		return err
	}
	for _, row := range rows {
		fields := modules_blockio.NewSpanFieldsAdapter(row.Block, row.RowIdx)
		traceIDHex, spanIDHex := extractIDs(row.Block, row.RowIdx)
		match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
	return nil
}

// LogQueryStats contains block selection and I/O statistics for a log query.
// Reported via LogQueryOptions.OnStats after query execution completes.
type LogQueryStats struct {
	Explain        string // ASCII trace of predicate tree → block set resolution
	TotalBlocks    int    // total blocks in the file
	PrunedByTime   int    // blocks eliminated by time-range comparison
	PrunedByIndex  int    // blocks eliminated by range index lookups
	PrunedByFuse   int    // blocks eliminated by BinaryFuse8 membership checks
	PrunedByCMS    int    // blocks eliminated by Count-Min Sketch zero-estimate checks
	SelectedBlocks int    // blocks selected for reading after all pruning
	FetchedBlocks  int    // blocks actually fetched (≤ SelectedBlocks when limit causes early stop)
}

// LogQueryOptions configures log query execution.
type LogQueryOptions struct {
	// OnStats is an optional callback invoked after query execution with block
	// selection and I/O statistics. FetchedBlocks reflects actual I/O done,
	// which is less than SelectedBlocks when a Limit causes early termination.
	OnStats   func(LogQueryStats)
	StartNano uint64 // Inclusive start (unix nanos); 0 = no lower bound.
	EndNano   uint64 // Inclusive end (unix nanos); 0 = no upper bound.
	Limit     int    // Max matches; 0 = unlimited.
	Forward   bool   // If true, traverse forward (oldest first). Default is backward (newest first).
}

// QueryLogQL executes a LogQL filter query against a blockpack log file
// and returns all matching log records.
//
// Supported syntax:
//   - Label matchers: {key="val", key=~"regex", key!="val", key!~"regex"}
//   - Line filters: |= "text" != "text" |~ "regex" !~ "regex"
//   - Pipeline stages: | json, | logfmt, | label_format, | line_format, | drop, | keep
//   - Label filter stages: | field = "val", | field > 42, etc.
//   - Unwrap: | unwrap field
//
// When no pipeline stages are present, queries are executed via the low-level
// path. When pipeline stages are present, the native LogQL engine is used.
//
// Direction defaults to backward, traversing blocks from newest to oldest.
func QueryLogQL(r *Reader, logqlQuery string, opts LogQueryOptions) (results []SpanMatch, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryLogQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("QueryLogQL: reader cannot be nil")
	}

	sel, parseErr := logqlparser.Parse(logqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse LogQL: %w", parseErr)
	}

	program, pipeline, compileErr := logqlparser.CompileAll(sel)
	if compileErr != nil {
		return nil, fmt.Errorf("compile LogQL: %w", compileErr)
	}

	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}

	if pipeline != nil {
		err = streamLogQLWithPipeline(r, program, pipeline, opts, collector)
	} else {
		err = streamLogProgram(r, program, opts, collector)
	}
	return results, err
}

// streamLogQLWithPipeline streams log entries applying a compiled pipeline.
// Delegates to CollectLogs which uses a heap with block-level timestamp pruning
// for limited queries, and a collect-sort-deliver path for unlimited queries.
func streamLogQLWithPipeline(
	r *Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	opts LogQueryOptions,
	fn spanMatchFn,
) error {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}

	entries, err := modules_executor.CollectLogs(
		r,
		program,
		pipeline,
		modules_executor.CollectOptions{
			Limit:     opts.Limit,
			TimeRange: modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction: direction,
			OnStats: func(s modules_executor.CollectStats) {
				if opts.OnStats != nil {
					opts.OnStats(LogQueryStats{
						TotalBlocks:    s.TotalBlocks,
						PrunedByTime:   s.PrunedByTime,
						PrunedByIndex:  s.PrunedByIndex,
						PrunedByFuse:   s.PrunedByFuse,
						PrunedByCMS:    s.PrunedByCMS,
						SelectedBlocks: s.SelectedBlocks,
						FetchedBlocks:  s.FetchedBlocks,
						Explain:        s.Explain,
					})
				}
			},
		},
	)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		match := &SpanMatch{Fields: &logEntryFields{
			labelsMap: entry.Labels,
			logAttrs:  entry.LogAttrs,
			line:      entry.Line,
			timestamp: entry.TimestampNanos,
		}}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
	return nil
}

// logEntryFields adapts a log entry's labels to SpanFieldsProvider.
// logAttrs holds log.* ColumnTypeString values (original LogRecord attributes) keyed
// by full column name (e.g. "log.detected_level") so that extractStructuredMetadata
// can find them via their "log." prefix. These are distinct from pipeline-derived labels
// which are stored in labels with the prefix stripped.
type logEntryFields struct {
	labelsMap map[string]string
	logAttrs  map[string]string
	line      string
	timestamp uint64
}

func (f *logEntryFields) GetField(name string) (any, bool) {
	switch name {
	case "log:body":
		return f.line, true
	case "log:timestamp":
		return f.timestamp, true
	}
	if f.logAttrs != nil {
		if v, ok := f.logAttrs[name]; ok {
			return v, true
		}
	}
	v, ok := f.labelsMap[name]
	return v, ok
}

func (f *logEntryFields) IterateFields(fn func(name string, value any) bool) {
	if !fn("log:body", f.line) {
		return
	}
	if !fn("log:timestamp", f.timestamp) {
		return
	}
	for k, v := range f.logAttrs {
		if !fn(k, v) {
			return
		}
	}
	for k, v := range f.labelsMap {
		if !fn(k, v) {
			return
		}
	}
}

// LogMetricsResult is the output of ExecuteMetricsLogQL.
type LogMetricsResult = modules_executor.LogMetricsResult

// LogMetricsRow is one row in a LogMetricsResult time-series grid.
type LogMetricsRow = modules_executor.LogMetricsRow

// LogMetricOptions configures a LogQL metrics query.
type LogMetricOptions struct {
	// GroupBy lists label names to group the time series by.
	GroupBy []string
	// StartNano is the inclusive start of the query time window (unix nanoseconds).
	StartNano int64
	// EndNano is the exclusive end of the query time window (unix nanoseconds).
	EndNano int64
	// StepNano is the time bucket step size in nanoseconds (default: 60 seconds).
	StepNano int64
}

// ExecuteMetricsLogQL executes a LogQL metric query against a blockpack log file and
// returns dense time-bucketed results.
//
// Supported metric functions: count_over_time, rate, bytes_over_time, bytes_rate,
// sum_over_time, avg_over_time, min_over_time, max_over_time.
//
// For unwrap-based functions (sum_over_time, avg_over_time, min_over_time,
// max_over_time), include a pipeline with | unwrap <field> to provide numeric values.
//
// Returns a LogMetricsResult with one row per (time-bucket × group-by-key) pair.
// GroupKey[0] is the 0-indexed bucket number; GroupKey[1..] are group-by label values.
func ExecuteMetricsLogQL(r *Reader, logqlQuery string, opts LogMetricOptions) (result *LogMetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsLogQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsLogQL: reader cannot be nil")
	}

	lq, parseErr := logqlparser.ParseQuery(logqlQuery)
	if parseErr != nil {
		return nil, fmt.Errorf("parse LogQL metric query: %w", parseErr)
	}

	if lq.VectorAgg != nil {
		return nil, fmt.Errorf(
			"ExecuteMetricsLogQL: vector aggregation operators (topk, sum by, etc.) not yet implemented",
		)
	}

	var metric *logqlparser.MetricExpr
	switch {
	case lq.Metric != nil:
		metric = lq.Metric
	default:
		return nil, fmt.Errorf("ExecuteMetricsLogQL: query is not a metric expression")
	}

	// Reinsert the unwrap stage into the pipeline stages before compiling.
	// parseMetricExpr extracts unwrap into MetricExpr.Unwrap and removes it from
	// Pipeline; we must add it back so the pipeline can populate UnwrapValueKey.
	pipelineStages := metric.Pipeline
	if metric.Unwrap != "" {
		pipelineStages = append(pipelineStages, logqlparser.PipelineStage{
			Type:   logqlparser.StageUnwrap,
			Params: []string{metric.Unwrap},
		})
	}

	// Build a combined selector with the full pipeline for CompileAll pushdown.
	combinedSel := &logqlparser.LogSelector{
		Matchers:    metric.Selector.Matchers,
		LineFilters: metric.Selector.LineFilters,
		Pipeline:    pipelineStages,
	}
	program, pipeline, compileErr := logqlparser.CompileAll(combinedSel)
	if compileErr != nil {
		return nil, fmt.Errorf("compile LogQL: %w", compileErr)
	}

	stepNano := opts.StepNano
	if stepNano <= 0 {
		stepNano = 60 * 1_000_000_000 // default: 1 minute
	}

	querySpec := &vm.QuerySpec{
		TimeBucketing: vm.TimeBucketSpec{
			Enabled:       true,
			StartTime:     opts.StartNano,
			EndTime:       opts.EndNano,
			StepSizeNanos: stepNano,
		},
		Filter: vm.FilterSpec{IsMatchAll: true},
	}

	return modules_executor.ExecuteLogMetrics(r, program, pipeline, querySpec, metric.Function, opts.GroupBy)
}

// TraceMetricsResult is the output of ExecuteMetricsTraceQL.
type TraceMetricsResult = modules_executor.TraceMetricsResult

// TraceTimeSeries is one time series in a TraceMetricsResult.
type TraceTimeSeries = modules_executor.TraceTimeSeries

// TraceMetricLabel is one label key-value pair in a TraceTimeSeries.
type TraceMetricLabel = modules_executor.TraceMetricLabel

// TraceMetricOptions configures a TraceQL metrics query.
type TraceMetricOptions struct {
	// StartNano is the inclusive start of the query time window (unix nanoseconds).
	StartNano int64
	// EndNano is the exclusive end of the query time window (unix nanoseconds).
	EndNano int64
	// StepNano is the time bucket step size in nanoseconds (default: 60 seconds).
	StepNano int64
}

// ExecuteMetricsTraceQL executes a TraceQL metrics query against a blockpack trace file
// and returns dense time-bucketed results.
//
// Supported metric functions: count_over_time(), rate(), sum(field), avg(field), min(field),
// max(field), histogram_over_time(field), quantile_over_time(field, phi), stddev(field).
//
// Returns a TraceMetricsResult with one TraceTimeSeries per unique group-by key combination.
// Each series has len(Values) == ceil((EndNano-StartNano)/StepNano) buckets.
// COUNT/RATE: missing buckets are 0. Other functions: missing buckets are NaN.
func ExecuteMetricsTraceQL(r *Reader, query string, opts TraceMetricOptions) (result *TraceMetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: reader cannot be nil")
	}

	stepNano := opts.StepNano
	if stepNano <= 0 {
		stepNano = 60 * 1_000_000_000 // default: 1 minute
	}

	prog, spec, compileErr := vm.CompileTraceQLMetrics(query, opts.StartNano, opts.EndNano)
	if compileErr != nil {
		return nil, fmt.Errorf("compile TraceQL metrics query: %w", compileErr)
	}

	// Override the step size with the caller-provided value (compiler uses a fixed default).
	spec.TimeBucketing.StepSizeNanos = stepNano

	return modules_executor.ExecuteTraceMetrics(r, prog, spec)
}

// streamLogProgram executes a compiled log program against a modules-format reader.
// Blocks are fetched lazily in ~8 MB coalesced batches; fetching stops once opts.Limit
// matches have been delivered, so I/O is proportional to results returned.
func streamLogProgram(r *Reader, program *vm.Program, opts LogQueryOptions, fn spanMatchFn) error {
	direction := modules_queryplanner.Backward
	if opts.Forward {
		direction = modules_queryplanner.Forward
	}
	rows, err := modules_executor.New().CollectTopK(
		r,
		program,
		modules_executor.CollectOptions{
			Limit:           opts.Limit,
			TimeRange:       modules_queryplanner.TimeRange{MinNano: opts.StartNano, MaxNano: opts.EndNano},
			Direction:       direction,
			TimestampColumn: "log:timestamp",
			OnStats: func(s modules_executor.CollectStats) {
				if opts.OnStats != nil {
					opts.OnStats(LogQueryStats{
						TotalBlocks:    s.TotalBlocks,
						PrunedByTime:   s.PrunedByTime,
						PrunedByIndex:  s.PrunedByIndex,
						PrunedByFuse:   s.PrunedByFuse,
						PrunedByCMS:    s.PrunedByCMS,
						SelectedBlocks: s.SelectedBlocks,
						FetchedBlocks:  s.FetchedBlocks,
						Explain:        s.Explain,
					})
				}
			},
		},
	)
	if err != nil {
		return err
	}
	for _, row := range rows {
		fields := modules_blockio.NewSpanFieldsAdapter(row.Block, row.RowIdx)
		traceIDHex, spanIDHex := extractIDs(row.Block, row.RowIdx)
		match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
		if !fn(match, true) {
			break
		}
	}
	fn(nil, false)
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
func streamStructuralQuery(r *Reader, q *traceqlparser.StructuralQuery, opts QueryOptions, fn spanMatchFn) error {
	if r == nil {
		return fmt.Errorf("streamStructuralQuery: reader cannot be nil")
	}

	leftProg, rightProg, err := compileStructuralPrograms(q)
	if err != nil {
		return err
	}

	traceSpans, err := collectStructuralSpans(r, leftProg, rightProg, opts)
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

// collectStructuralSpans scans blocks and accumulates per-trace span records.
// Returns a map: traceIDHex → []structSpanRec (in block scan order).
// opts.StartBlock/BlockCount restrict scanning to the assigned sub-file shard.
func collectStructuralSpans(
	r *Reader,
	leftProg, rightProg *vm.Program,
	opts QueryOptions,
) (map[string][]structSpanRec, error) {
	planner := modules_queryplanner.NewPlanner(r)
	plan := planner.Plan(nil, normalizeTimeRange(opts.StartNano, opts.EndNano))

	// Sub-file sharding: restrict to assigned block range.
	if opts.BlockCount > 0 {
		endBlock := opts.StartBlock + opts.BlockCount
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if bi >= opts.StartBlock && bi < endBlock {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
	}

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
	// Union predicate columns from both programs plus the identity columns appendBlockSpanRecs reads.
	wantColumns := modules_executor.ProgramWantColumns(leftProg, "trace:id", "span:id", "span:parent_id")
	if right := modules_executor.ProgramWantColumns(rightProg, "trace:id", "span:id", "span:parent_id"); right != nil {
		if wantColumns == nil {
			wantColumns = right
		} else {
			for c := range right {
				wantColumns[c] = struct{}{}
			}
		}
	}
	bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
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
	fn spanMatchFn,
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
			if !fn(match, true) {
				fn(nil, false)
				return nil
			}
			matched++
			if opts.Limit > 0 && matched >= opts.Limit {
				fn(nil, false)
				return nil
			}
		}
	}
	fn(nil, false)
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
	case traceqlparser.OpNotDescendant:
		return evalOpNotDescendant(spans)
	case traceqlparser.OpNotChild:
		return evalOpNotChild(spans)
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

// evalOpNotDescendant: R is rightMatch but NOT a descendant of any leftMatch span (!>>).
func evalOpNotDescendant(spans []structSpanRec) []int {
	// Build set of all leftMatch span indices.
	leftSet := make(map[int]bool)
	for i, sp := range spans {
		if sp.leftMatch {
			leftSet[i] = true
		}
	}
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		// Walk ancestor chain of R; if any ancestor is leftMatch, R IS a descendant → skip.
		isDescendant := false
		cur := r.parentIdx
		for cur >= 0 {
			if leftSet[cur] {
				isDescendant = true
				break
			}
			cur = spans[cur].parentIdx
		}
		if !isDescendant {
			result = append(result, ri)
		}
	}
	return result
}

// evalOpNotChild: R is rightMatch but its direct parent is NOT leftMatch (!>).
func evalOpNotChild(spans []structSpanRec) []int {
	var result []int
	for ri, r := range spans {
		if !r.rightMatch {
			continue
		}
		// R qualifies if it has no parent, or its parent is not leftMatch.
		if r.parentIdx < 0 || !spans[r.parentIdx].leftMatch {
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

// AGENT: Block metadata API - provides file-level metadata for query planning.

// BlockMeta contains metadata about a blockpack file for block selection.
// Tempo uses this information to determine which files to query based on time range overlap.
type BlockMeta struct {
	MinStartNanos uint64 // Earliest span start time (unix nanos)
	MaxStartNanos uint64 // Latest span start time (unix nanos)
	TotalSpans    int    // Total number of spans across all blocks
	TotalTraces   int    // Total number of unique traces (from trace index)
	BlockCount    int    // Number of blocks in the file
	Size          int64  // File size in bytes
}

// GetBlockMeta returns metadata about a blockpack file including time range,
// span/trace counts, and file size. This is used by Tempo for block selection
// to determine which files to query based on time range overlap.
//
// The returned metadata includes:
//   - MinStartNanos: Earliest span start time in the file (unix nanoseconds)
//   - MaxStartNanos: Latest span start time in the file (unix nanoseconds)
//   - TotalSpans: Total number of spans across all blocks
//   - TotalTraces: Number of unique traces (from trace index), consistent with Tempo's TotalObjects
//   - BlockCount: Number of blocks in the file
//   - Size: File size in bytes
//
// This function reads only the file header and block index metadata (typically <1KB),
// making it suitable for frequent calls during query planning.
func GetBlockMeta(path string, storage Storage) (meta *BlockMeta, err error) {
	return GetBlockMetaWithCache(path, storage, nil)
}

// GetBlockMetaWithCache is like GetBlockMeta but caches footer, header, and metadata
// reads in cache. Pass nil for cache to disable caching (equivalent to GetBlockMeta).
// path is used as the fileID within the cache namespace.
func GetBlockMetaWithCache(path string, storage Storage, cache *FileCache) (meta *BlockMeta, err error) {
	defer func() {
		if r := recover(); r != nil {
			meta = nil
			err = fmt.Errorf("internal error in GetBlockMeta: %v", r)
		}
	}()

	provider := &storageReaderProvider{storage: storage, path: path}
	r, readerErr := modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  cache,
		FileID: path,
	})
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

// fileEntry holds a path, its resolved metadata, and whether metadata resolution failed.
type fileEntry struct {
	path   string
	meta   BlockMeta
	failed bool
}

// FilePlan holds an ordered list of blockpack files with their resolved metadata,
// intended for use by query planners that need file-level priority ordering.
type FilePlan struct {
	files []fileEntry
}

// PlanFiles resolves BlockMeta for each path and returns a FilePlan.
// Paths that fail metadata resolution are retained in the plan with failed=true
// and a zero BlockMeta; the error is logged via slog. A nil or empty paths slice
// returns an empty FilePlan. cache may be nil.
func PlanFiles(paths []string, storage Storage, cache *FileCache) *FilePlan {
	if len(paths) == 0 {
		return &FilePlan{}
	}

	entries := make([]fileEntry, len(paths))
	for i, p := range paths {
		meta, err := GetBlockMetaWithCache(p, storage, cache)
		if err != nil {
			slog.Error("PlanFiles: failed to get block meta", "path", p, "error", err)
			entries[i] = fileEntry{path: p, failed: true}
			continue
		}
		entries[i] = fileEntry{path: p, meta: *meta}
	}

	return &FilePlan{files: entries}
}

// Between returns the paths of files whose time range overlaps [minNanos, maxNanos],
// sorted newest-first (by MaxStartNanos descending). Files that failed metadata
// resolution are always included and sorted after all non-failed files. Returns nil
// when no files qualify.
func (p *FilePlan) Between(minNanos, maxNanos uint64) []string {
	if p == nil || len(p.files) == 0 {
		return nil
	}

	type candidate struct {
		path     string
		maxStart uint64
		failed   bool
	}

	var candidates []candidate
	for _, fe := range p.files {
		if fe.failed {
			candidates = append(candidates, candidate{path: fe.path, failed: true})
			continue
		}
		// Files with unknown time (both 0) are conservatively included, matching
		// the block-level planner behavior in queryplanner/planner.go.
		unknownTime := fe.meta.MinStartNanos == 0 && fe.meta.MaxStartNanos == 0
		if unknownTime || (fe.meta.MaxStartNanos >= minNanos && fe.meta.MinStartNanos <= maxNanos) {
			candidates = append(candidates, candidate{path: fe.path, maxStart: fe.meta.MaxStartNanos})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		// Failed files sort after non-failed
		if ci.failed != cj.failed {
			return !ci.failed
		}
		// Both failed: stable by path
		if ci.failed {
			return ci.path < cj.path
		}
		// Both non-failed: newest first; tiebreak by path
		if ci.maxStart != cj.maxStart {
			return ci.maxStart > cj.maxStart
		}
		return ci.path < cj.path
	})

	paths := make([]string, len(candidates))
	for i, c := range candidates {
		paths[i] = c.path
	}

	return paths
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

// ConvertLogsProtoToBlockpack reads an OTLP protobuf-encoded LogsData file and writes
// a blockpack log file to output.
// The input file must contain a single wire-encoded logsv1.LogsData protobuf message.
// maxRecordsPerBlock controls block granularity (0 uses the default of 2000).
func ConvertLogsProtoToBlockpack(inputPath string, output io.Writer, maxRecordsPerBlock int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error in ConvertLogsProtoToBlockpack: %v", r)
		}
	}()
	return otlpconvert.ConvertLogsProtoFile(inputPath, output, maxRecordsPerBlock)
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

// storageReaderProvider adapts a Storage + path to modules_rw.ReaderProvider.
type storageReaderProvider struct {
	storage Storage
	path    string
}

func (p *storageReaderProvider) Size() (int64, error) {
	return p.storage.Size(p.path)
}

func (p *storageReaderProvider) ReadAt(buf []byte, off int64, dataType modules_rw.DataType) (int, error) {
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

// AGENT: S3 / MinIO storage provider.

// MinIOProvider is a ReaderProvider backed by a single MinIO/S3 object.
// Construct with NewMinIOProvider and pass to NewReaderFromProvider.
// All methods are safe for concurrent use.
type MinIOProvider = s3provider.MinIOProvider

// NewMinIOProvider returns a ReaderProvider that reads a blockpack file from
// the given MinIO/S3-compatible object storage.
//
// client must be a connected *minio.Client.
// bucket is the bucket name; object is the full object path within that bucket.
//
// Size() is cached after the first call — StatObject is issued at most once.
// Each ReadAt call issues an independent HTTP range request.
func NewMinIOProvider(client *minio.Client, bucket, object string) *MinIOProvider {
	return s3provider.NewMinIOProvider(client, bucket, object)
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
	converted := make([]modules_rw.ReaderProvider, len(providers))
	for i, p := range providers {
		converted[i] = p
	}
	paths, _, err := modules_compaction.CompactBlocks(ctx, converted, cfg, output)
	return paths, err
}
