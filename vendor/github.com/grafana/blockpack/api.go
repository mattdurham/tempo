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
	"math"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_queryplanner "github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

// SetIntrinsicCacheBytes sets the byte budget for the process-level decoded intrinsic
// column cache (trace:id, span:id, span:duration etc.). Must be called before the first
// query. Pass 0 to use the default (20% of GOMEMLIMIT, or 256 MiB).
//
// Querier processes should set this to 256-512 MiB. The default 20% of GOMEMLIMIT is
// generous for compaction workers but excessive for queriers with large memory limits.
func SetIntrinsicCacheBytes(n int64) {
	modules_reader.SetIntrinsicCacheBytes(n)
}

// AGENT: Query execution - this is the main public API for querying.
// Keep this minimal - just TraceQL filter query function.

// SpanFieldsProvider gives access to all attributes for a single span row.
// Use GetField for known attribute names, IterateFields to enumerate all.
type SpanFieldsProvider = modules_shared.SpanFieldsProvider

// QueryStats contains per-phase execution metrics returned by all query functions.
// It replaces the LogQueryStats type and the OnStats callback pattern.
type QueryStats = modules_executor.QueryStats

// StepStats holds per-phase metrics for a single execution phase within a QueryStats.
type StepStats = modules_executor.StepStats

// QueryOptions configures query execution.

// Embedder enables VECTOR_AI() predicates in TraceQL filter queries. When non-nil,
// a VECTOR_AI("query text") expression is embedded at compile time and matched against
// spans by cosine similarity. If nil and the query contains VECTOR_AI(), QueryTraceQL
// returns an error. Any type implementing vm.TextEmbedder is accepted — this keeps
// blockpack decoupled from the concrete embedder implementation.
// VECTOR_ALL() does not require an Embedder.

// SelectColumns limits which column names appear in SpanMatch.Fields.
// When non-empty, only columns whose names are present in this slice are
// returned by GetField and IterateFields. nil or empty means all columns
// are returned (no projection applied).
// A nil slice and a non-nil empty slice are equivalent: both mean all columns are returned.

// StartNano is the inclusive lower bound for block-level time pruning (unix nanoseconds).
// Internal blocks whose span:start range ends before StartNano are skipped entirely.
// 0 means no lower bound.

// EndNano is the inclusive upper bound for block-level time pruning (unix nanoseconds).
// Internal blocks whose span:start range begins after EndNano are skipped entirely.
// 0 means no upper bound.

// Limit is the maximum number of spans to return (0 = unlimited).
// Negative values are treated as 0 (unlimited) — the executor does not validate sign.

// StartBlock is the first internal block index to scan (0-based, inclusive).
// Used by the frontend sharder to partition a single blockpack file into
// multiple sub-file jobs. 0 means start from the first block.

// BlockCount is the number of internal blocks to scan starting from StartBlock.
// 0 means scan all blocks (no sub-file sharding).

// MostRecent controls block traversal order. Always use keyed struct literals:
// QueryOptions{Limit: 10, MostRecent: true}.
// Uses Backward direction with span:start timestamp sorting. For intrinsic-only queries,
// top-K selection uses only the intrinsic column blobs (no full block I/O).

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

// Program is a compiled TraceQL filter program. It is safe to reuse across
// multiple QueryTraceQLWithProgram calls concurrently. Programs are immutable
// after creation. Use CompileTraceQL to create a Program; use
// QueryTraceQLWithProgram to execute it.
//
// SPEC-VM-001: A *Program is safe to reuse concurrently across multiple
// QueryTraceQLWithProgram calls. Callers need no external synchronization.
type Program = vm.Program

// CompileTraceQL parses and compiles a TraceQL filter expression to a *Program.
// The returned Program is safe to reuse across multiple QueryTraceQLWithProgram
// calls — compile once, query many times.
//
// Only filter expressions are supported (e.g., `{ span.http.method = "GET" }`).
// Structural queries (A >> B) and pipeline queries (filter | aggregate > threshold)
// return an error — use QueryTraceQL for those query types.
//
// SPEC-VM-001: The returned *Program is immutable after creation; no external
// synchronization is required for concurrent QueryTraceQLWithProgram calls.
func CompileTraceQL(traceqlQuery string, opts QueryOptions) (*Program, error) {
	parsed, err := traceqlparser.ParseTraceQL(traceqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse TraceQL: %w", err)
	}
	fe, ok := parsed.(*traceqlparser.FilterExpression)
	if !ok {
		return nil, fmt.Errorf(
			"CompileTraceQL: only filter expressions are supported, got %T",
			parsed,
		)
	}
	if opts.Embedder != nil || opts.Limit != 0 {
		return vm.CompileTraceQLFilterWithOptions(fe, vm.CompileOptions{
			Embedder: opts.Embedder,
			Limit:    opts.Limit,
		})
	}
	return vm.CompileTraceQLFilter(fe)
}

// QueryTraceQLWithProgram executes a pre-compiled TraceQL filter program against
// a modules-format blockpack file. Use CompileTraceQL to build the program once
// and reuse it across multiple *Reader instances to avoid repeated parse and
// DFA-construction cost.
//
// Nil-safety: returns an error immediately for nil r or nil program.
// Panic safety: internal panics are recovered and returned as errors.
//
// SPEC-VM-001: Program immutability and safe reuse.
func QueryTraceQLWithProgram(
	ctx context.Context,
	r *Reader,
	program *Program,
	opts QueryOptions,
) (results []SpanMatch, stats QueryStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryTraceQLWithProgram: %v", rec)
		}
	}()
	if r == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQLWithProgram: reader cannot be nil")
	}
	if program == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQLWithProgram: program cannot be nil")
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQLWithProgram: %w", shardErr)
	}
	collector := func(match *SpanMatch, more bool) bool {
		if !more {
			return false
		}
		results = append(results, match.Clone())
		return true
	}
	stats, err = streamFilterProgram(ctx, r, program, opts, collector)
	return results, stats, err
}

// QueryTraceQLFromIndex executes a TraceQL filter query using the value index for
// block pruning (NOTE-VI-035, issue #459). It resolves the matching spans from the
// pre-populated ValueIndexSource, then fetches only the blocks that contain matches
// from r and materializes their fields — no full-file scan.
//
// The querier (issue #461) supplies the source by discovering, downloading, and
// applying the per-leaf predicate to the value-index files for r's SourceRef, exactly
// as it does for the metrics path (NOTE-VI-033). sourceRef identifies which data file
// r was opened against so results for other files are ignored.
//
// Returns (matches, true, nil) when the query is fully answerable from the index.
// Returns (nil, false, nil) when the caller must fall back to QueryTraceQL (full scan):
//   - source is nil (caller did no discovery)
//   - any leaf column has no index coverage
//   - the index produced more than maxIndexHits results (<= 0 uses the default)
//   - the query is not a filter expression (structural/metrics use their own paths)
//
// Each returned SpanMatch.Fields is materialized and safe to retain after return.
func QueryTraceQLFromIndex(
	ctx context.Context,
	r *Reader,
	source ValueIndexSource,
	traceqlQuery string,
	sourceRef string,
	opts QueryOptions,
	maxIndexHits int,
) (results []SpanMatch, ok bool, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			results = nil
			ok = false
			err = fmt.Errorf("internal error in QueryTraceQLFromIndex: %v", rec)
		}
	}()

	if r == nil {
		return nil, false, fmt.Errorf("QueryTraceQLFromIndex: reader cannot be nil")
	}
	if source == nil {
		// No index coverage supplied: caller must fall back to a full scan.
		return nil, false, nil
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, false, fmt.Errorf("QueryTraceQLFromIndex: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, false, fmt.Errorf("parse TraceQL: %w", parseErr)
	}
	filterExpr, isFilter := parsed.(*traceqlparser.FilterExpression)
	if !isFilter {
		// Only filter expressions use the index search path; structural and metrics
		// queries have their own execution paths.
		return nil, false, nil
	}

	var program *vm.Program
	var compileErr error
	if opts.Embedder != nil {
		program, compileErr = vm.CompileTraceQLFilterWithOptions(filterExpr, vm.CompileOptions{
			Embedder: opts.Embedder,
			Limit:    opts.Limit,
		})
	} else {
		program, compileErr = vm.CompileTraceQLFilter(filterExpr)
	}
	if compileErr != nil {
		return nil, false, fmt.Errorf("compile TraceQL filter: %w", compileErr)
	}

	matches, indexOK, execErr := modules_executor.QueryTraceQLFromIndex(
		ctx, source, r, program, sourceRef,
		modules_executor.ComputeSecondPassCols(program, opts.SelectColumns),
		maxIndexHits,
	)
	if execErr != nil {
		return nil, false, execErr
	}
	if !indexOK {
		return nil, false, nil
	}

	// Convert executor SpanMatch → public SpanMatch, materializing fields via the
	// reader (same conversion as the structural path).
	wantCols := modules_executor.ComputeSecondPassCols(program, opts.SelectColumns)
	results = make([]SpanMatch, 0, len(matches))
	for i := range matches {
		m := &matches[i]
		rawAdapter := modules_blockio.NewSpanFieldsAdapterWithReader(m.Block, r, m.BlockIdx, m.RowIdx, wantCols)
		fields := rawAdapter
		if len(opts.SelectColumns) > 0 {
			fields = newFilteredSpanFields(rawAdapter, opts.SelectColumns)
		}
		match := SpanMatch{
			TraceID: hex.EncodeToString(m.TraceID[:]),
			SpanID:  hex.EncodeToString(m.SpanID),
			Fields:  fields,
		}
		results = append(results, match.Clone())
		// NOTE-ALLOC-4: release after Clone materializes the fields.
		modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
	}
	return results, true, nil
}

// QueryTraceQL executes a TraceQL query against a modules-format blockpack file
// and returns all matching spans along with per-phase execution statistics.
// QueryStats is populated for filter queries; structural and pipeline queries
// return an empty QueryStats.
//
// Supported query types:
//   - Filter expressions: `{ span.http.method = "GET" }`
//   - Structural queries: `{ expr } OP { expr }` where OP is >>, >, ~, <<, <, !~
//   - Pipeline queries: `{ filter } | aggregate() > threshold`
//
// For filter queries, each SpanMatch.Fields supports GetField and IterateFields.
// For structural queries, SpanMatch.Fields is non-nil and backed by the intrinsic section.
// Pipeline queries group matching spans into spansets, compute aggregates, and
// filter by threshold before returning qualifying spans.
func QueryTraceQL(
	ctx context.Context,
	r *Reader,
	traceqlQuery string,
	opts QueryOptions,
) (results []SpanMatch, stats QueryStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("internal error in QueryTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQL: reader cannot be nil")
	}
	if shardErr := validateQueryOptions(opts); shardErr != nil {
		return nil, QueryStats{}, fmt.Errorf("QueryTraceQL: %w", shardErr)
	}

	parsed, parseErr := traceqlparser.ParseTraceQL(traceqlQuery)
	if parseErr != nil {
		return nil, QueryStats{}, fmt.Errorf("parse TraceQL: %w", parseErr)
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
		stats, err = streamFilterQuery(ctx, r, q, opts, collector)
	case *traceqlparser.StructuralQuery:
		execOpts := modules_executor.Options{
			Limit:      opts.Limit,
			TimeRange:  normalizeTimeRange(opts.StartNano, opts.EndNano),
			StartBlock: opts.StartBlock,
			BlockCount: opts.BlockCount,
		}
		var execResult *modules_executor.StructuralResult
		execResult, err = modules_executor.ExecuteStructural(ctx, r, q, execOpts)
		if err == nil {
			// SPEC-ROOT-017: pass secondPassCols as wantCols to restrict intrinsic decoding
			structuralWantCols := modules_executor.ComputeSecondPassCols(nil, opts.SelectColumns)
			for i := range execResult.Matches {
				m := &execResult.Matches[i]
				rawAdapter := modules_blockio.NewSpanFieldsAdapterWithReader(m.Block, r, m.BlockIdx, m.RowIdx, structuralWantCols)
				fields := rawAdapter
				if len(opts.SelectColumns) > 0 {
					fields = newFilteredSpanFields(rawAdapter, opts.SelectColumns)
				}
				match := &SpanMatch{
					TraceID: hex.EncodeToString(m.TraceID[:]),
					SpanID:  hex.EncodeToString(m.SpanID),
					Fields:  fields,
				}
				if !collector(match, true) {
					modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
					break
				}
				// NOTE-ALLOC-4: release after collector calls match.Clone().
				modules_blockio.ReleaseSpanFieldsAdapter(rawAdapter)
			}
			collector(nil, false)
		}
	case *traceqlparser.MetricsQuery:
		err = streamPipelineQuery(ctx, r, q, opts, collector)
	default:
		err = fmt.Errorf(
			"QueryTraceQL: query type %T is not supported",
			parsed,
		)
	}
	return results, stats, err
}

// TraceMetricsResult is the output of ExecuteMetricsTraceQL.
type TraceMetricsResult = modules_executor.TraceMetricsResult

// TraceTimeSeries is one time series in a TraceMetricsResult.
type TraceTimeSeries = modules_executor.TraceTimeSeries

// TraceMetricLabel is one label key-value pair in a TraceTimeSeries.
type TraceMetricLabel = modules_executor.TraceMetricLabel

// ValueIndexSource provides value-index data for the zero-block-read metrics path
// (count_over_time/rate without group-by). See ExecuteMetricsTraceQL.
type ValueIndexSource = modules_executor.ValueIndexSource

// VILookupResult is one matching span from a value-index lookup.
type VILookupResult = modules_executor.VILookupResult

// SliceValueIndexSource is a ValueIndexSource backed by pre-downloaded, predicate-
// matched value-index results grouped by column.
type SliceValueIndexSource = modules_executor.SliceValueIndexSource

// NewSliceValueIndexSource builds an empty SliceValueIndexSource. Populate it with Add.
func NewSliceValueIndexSource() *SliceValueIndexSource {
	return modules_executor.NewSliceValueIndexSource()
}

// TraceMetricOptions configures a TraceQL metrics query.

// StartNano is the approximate start of the query time window (unix nanoseconds).
// Internally aligned down to the nearest StepNano boundary before query execution.
// The effective interval is right-closed: spans at exactly alignedStart are excluded.
// Zero means the Unix epoch (1970-01-01 00:00:00 UTC), NOT "no lower bound".
// Contrast with QueryOptions.StartNano (uint64) where 0 is treated as unbounded.

// EndNano is the approximate end of the query time window (unix nanoseconds).
// Internally aligned up to the nearest StepNano boundary before query execution.
// The effective interval is right-closed: spans at exactly alignedEnd are included.
// Zero means the Unix epoch (1970-01-01 00:00:00 UTC), NOT "no upper bound".
// Contrast with QueryOptions.EndNano (uint64) where 0 is treated as unbounded.

// StepNano is the time bucket step size in nanoseconds (default: 60 seconds).
// Values <= 0 are treated as the default (60 seconds). Negative values are not an error.

// ExecuteMetricsTraceQL executes a TraceQL metrics query against a blockpack trace file
// and returns dense time-bucketed results.
//
// Supported metric functions: count_over_time(), rate(), sum(field), avg(field), min(field),
// max(field), histogram_over_time(field), quantile_over_time(field, phi), stddev(field).
//
// StartNano and EndNano are aligned to StepNano boundaries before execution (matching Tempo
// IntervalMapperQueryRange semantics). The actual bucket count is
// ceil((alignedEnd-alignedStart)/StepNano), which may exceed ceil((EndNano-StartNano)/StepNano)
// when the inputs are not already step-aligned.
// COUNT/RATE: missing buckets are 0. Other functions: missing buckets are NaN.
func ExecuteMetricsTraceQL(
	ctx context.Context,
	r *Reader,
	query string,
	opts TraceMetricOptions,
) (result *TraceMetricsResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = nil
			err = fmt.Errorf("internal error in ExecuteMetricsTraceQL: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: reader cannot be nil")
	}

	// Normalize nil context to Background so downstream ctx.Err()/ctx.Done() calls
	// don't panic. Callers should pass a real context; this guard is a safety net
	// for the public API boundary.
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stepNano := opts.StepNano
	if stepNano <= 0 {
		stepNano = 60 * 1_000_000_000 // default: 1 minute
	}

	if opts.StartNano > opts.EndNano {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: StartNano (%d) must not exceed EndNano (%d)",
			opts.StartNano, opts.EndNano)
	}

	// Align start/end to step boundaries — matches Tempo IntervalMapperQueryRange semantics.
	// alignedStart = start rounded down to nearest step (floor division).
	// alignedEnd   = end rounded up to nearest step (ceiling division).
	// Use explicit floor/ceil to handle negative timestamps correctly: Go's % keeps the sign
	// of the dividend, so for negative values plain modulo rounds toward zero, not toward -∞.
	startMod := opts.StartNano % stepNano
	if startMod < 0 {
		startMod += stepNano
	}
	if opts.StartNano < math.MinInt64+startMod {
		return nil, fmt.Errorf("ExecuteMetricsTraceQL: StartNano (%d) too close to int64 min for step alignment",
			opts.StartNano)
	}
	alignedStart := opts.StartNano - startMod
	endMod := opts.EndNano % stepNano
	if endMod < 0 {
		endMod += stepNano
	}
	alignedEnd := opts.EndNano
	if endMod != 0 {
		bump := stepNano - endMod
		if opts.EndNano > math.MaxInt64-bump {
			return nil, fmt.Errorf("ExecuteMetricsTraceQL: EndNano (%d) too close to int64 max for step alignment",
				opts.EndNano)
		}
		alignedEnd = opts.EndNano + bump
	}

	prog, spec, compileErr := vm.CompileTraceQLMetrics(query, alignedStart, alignedEnd)
	if compileErr != nil {
		return nil, fmt.Errorf("compile TraceQL metrics query: %w", compileErr)
	}

	// Override the step size with the caller-provided value (compiler uses a fixed default).
	spec.TimeBucketing.StepSizeNanos = stepNano

	// NOTE-VI-033 (issue #460): try the zero-block-read value-index path first.
	// count_over_time()/rate() without group-by are answerable from index TimeSec
	// alone. ExecuteTraceMetricsFromVI returns ok=false for unsupported queries or
	// missing index coverage, in which case we fall back to the full block scan.
	if opts.ValueIndex != nil {
		viResult, ok, viErr := modules_executor.ExecuteTraceMetricsFromVI(ctx, opts.ValueIndex, prog, *spec)
		if viErr != nil {
			return nil, viErr
		}
		if ok {
			return viResult, nil
		}
	}

	return modules_executor.ExecuteTraceMetrics(ctx, r, prog, spec)
}
