package vblockpack

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// metricsOpRe extracts the metric function name from a TraceQL metrics query.
// For example: "{}|rate()" → "rate", "{...}|count_over_time() by (x)" → "count_over_time".
var metricsOpRe = regexp.MustCompile(`\|(\w+)\(`)

// tempoReaderProvider implements blockpack.ReaderProvider directly against
// Tempo's backend.Reader for a single fixed object (DataFileName).
// All methods are safe for concurrent use — ReadRange is stateless.
type tempoReaderProvider struct {
	reader    backend.Reader
	tenantID  string
	blockID   uuid.UUID
	knownSize int64 // populated from BlockMeta.Size_; avoids a full S3 download
}

func (p *tempoReaderProvider) Size() (int64, error) {
	if p.knownSize > 0 {
		return p.knownSize, nil
	}
	// Fallback: stream to get size (expensive — always populate knownSize at construction time).
	rc, size, err := p.reader.StreamReader(context.Background(), DataFileName, p.blockID, p.tenantID)
	if err != nil {
		return 0, err
	}
	rc.Close()
	return size, nil
}

func (p *tempoReaderProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	err := p.reader.ReadRange(context.Background(), DataFileName, p.blockID, p.tenantID, uint64(off), buf, nil)
	if err != nil {
		size, sizeErr := p.Size()
		if sizeErr == nil && off >= size {
			return 0, io.EOF
		}
		return 0, err
	}
	return len(buf), nil
}

// ConfigureLRU is a no-op retained for API compatibility.
// The SharedLRU raw-byte cache was removed — pprof showed it was allocating
// ~76 GB/s under load while providing no benefit: blocks (10-200 MB) exceeded
// the 32 MB limit and were never stored; objectcache already caches parsed
// metadata/sketches/intrinsic columns at a higher level.
func ConfigureLRU(_ int64) {}

// blockpackFileCache is a process-level disk-backed cache for blockpack block bytes.
// A nil *FileCache is safe — all reads fall through to the provider.
var (
	blockpackFileCache     *blockpack.FileCache
	blockpackFileCacheOnce sync.Once
	blockpackFileCachePath string
	blockpackFileCacheSize int64
)

// ConfigureFileCache sets the disk cache path and max size for blockpack blocks.
// Must be called before the first block is opened for disk caching to be active.
// Safe to call multiple times; only the first invocation of getFileCache takes effect.
func ConfigureFileCache(path string, maxBytes int64) {
	blockpackFileCachePath = path
	blockpackFileCacheSize = maxBytes
}

// getFileCache initializes (once) and returns the process-level disk cache.
// Returns nil if no path was configured or if opening the cache failed.
func getFileCache() *blockpack.FileCache {
	blockpackFileCacheOnce.Do(func() {
		path := blockpackFileCachePath
		maxBytes := blockpackFileCacheSize
		if path == "" || maxBytes <= 0 {
			return
		}
		c, err := blockpack.OpenFileCache(blockpack.FileCacheConfig{
			Enabled:  true,
			Path:     path,
			MaxBytes: maxBytes,
		})
		if err != nil {
			return
		}
		blockpackFileCache = c
	})
	return blockpackFileCache
}

type blockpackBlock struct {
	meta   *backend.BlockMeta
	reader backend.Reader
}

// newBackendBlock creates a new blockpack backend block
func newBackendBlock(meta *backend.BlockMeta, r backend.Reader) *blockpackBlock {
	return &blockpackBlock{
		meta:   meta,
		reader: r,
	}
}

// newReaderProvider returns a provider bound to this block's S3 object.
// Parsed metadata, sketches, and intrinsic columns are cached at the
// objectcache layer inside blockpack (GC-cooperative weak.Pointer cache),
// which supersedes the former SharedLRU raw-byte cache.
func (b *blockpackBlock) newReaderProvider() blockpack.ReaderProvider {
	return &tempoReaderProvider{
		reader:    b.reader,
		tenantID:  b.meta.TenantID,
		blockID:   uuid.UUID(b.meta.BlockID),
		knownSize: int64(b.meta.Size_),
	}
}

// newReader creates a Reader with both in-memory LRU and disk FileCache layers.
// Each call returns a new Reader — Reader is not safe for concurrent use.
func (b *blockpackBlock) newReader() (*blockpack.Reader, error) {
	fileID := b.meta.TenantID + "/" + b.meta.BlockID.String()
	return blockpack.NewReaderWithCache(b.newReaderProvider(), fileID, getFileCache())
}

// executeQuery creates a reader and executes a TraceQL query, returning all matching spans.
func (b *blockpackBlock) executeQuery(query string, opts blockpack.QueryOptions) ([]blockpack.SpanMatch, blockpack.QueryStats, error) {
	r, err := b.newReader()
	if err != nil {
		return nil, blockpack.QueryStats{}, fmt.Errorf("failed to create blockpack reader: %w", err)
	}
	return blockpack.QueryTraceQL(r, query, opts)
}

// BlockMeta returns the block metadata
func (b *blockpackBlock) BlockMeta() *backend.BlockMeta {
	return b.meta
}

// QueryRange implements the nativeMetricsQuerier optional interface.
// It delegates to blockpack.ExecuteMetricsTraceQL which uses the intrinsic
// column fast path — reading only ~10 MB of sorted flat blobs instead of
// fetching the entire block (~200 MB) via the generic Fetch path.
func (b *blockpackBlock) QueryRange(_ context.Context, req *tempopb.QueryRangeRequest, _ common.SearchOptions) (*tempopb.QueryRangeResponse, error) {
	r, err := b.newReader()
	if err != nil {
		return nil, fmt.Errorf("blockpack QueryRange: new reader: %w", err)
	}

	result, err := blockpack.ExecuteMetricsTraceQL(r, req.Query, blockpack.TraceMetricOptions{
		StartNano: int64(req.Start),
		EndNano:   int64(req.End),
		StepNano:  int64(req.Step),
	})
	if err != nil {
		return nil, fmt.Errorf("blockpack QueryRange: %w", err)
	}

	return convertTraceMetricsResult(result, req), nil
}

// convertTraceMetricsResult maps a blockpack TraceMetricsResult to a Tempo QueryRangeResponse.
// Timestamps are generated using the same IntervalMapper as Tempo's parquet path so that
// downstream combiners receive structurally identical series regardless of backend.
func convertTraceMetricsResult(result *blockpack.TraceMetricsResult, req *tempopb.QueryRangeRequest) *tempopb.QueryRangeResponse {
	mapper := traceql.NewIntervalMapperFromReq(req)
	intervals := mapper.IntervalCount()

	// For ungrouped queries, Tempo's parquet path adds {__name__=<op>} to match Prometheus
	// convention. Blockpack returns empty labels for ungrouped queries, so we synthesize
	// the __name__ label here using the op name extracted from the query string.
	var ungroupedName string
	if m := metricsOpRe.FindStringSubmatch(req.Query); len(m) > 1 {
		ungroupedName = m[1]
	}

	series := make([]*tempopb.TimeSeries, 0, len(result.Series))
	for _, s := range result.Series {
		var labels []tempocommon.KeyValue
		if len(s.Labels) == 0 && ungroupedName != "" {
			// Ungrouped query — add synthetic __name__ label to match parquet output.
			labels = []tempocommon.KeyValue{{
				Key:   "__name__",
				Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: ungroupedName}},
			}}
		} else {
			labels = make([]tempocommon.KeyValue, 0, len(s.Labels))
			for _, lbl := range s.Labels {
				labels = append(labels, tempocommon.KeyValue{
					Key:   lbl.Name,
					Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: lbl.Value}},
				})
			}
		}

		samples := make([]tempopb.Sample, 0, len(s.Values))
		for i, v := range s.Values {
			if i >= intervals || math.IsNaN(v) {
				continue
			}
			ts := mapper.TimestampOf(i)
			samples = append(samples, tempopb.Sample{
				TimestampMs: time.Unix(0, int64(ts)).UnixMilli(),
				Value:       v,
			})
		}
		if len(samples) == 0 {
			continue
		}
		series = append(series, &tempopb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		})
	}

	return &tempopb.QueryRangeResponse{
		Series: series,
		Metrics: &tempopb.SearchMetrics{
			InspectedBytes: uint64(result.BytesRead), //nolint:gosec
		},
	}
}

// FindTraceByID finds a trace by ID.
func (b *blockpackBlock) FindTraceByID(_ context.Context, id common.ID, _ common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	if len(id) != 16 {
		return nil, fmt.Errorf("trace ID must be 16 bytes, got %d", len(id))
	}

	r, err := b.newReader()
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack reader: %w", err)
	}

	traceIDHex := hex.EncodeToString(id)

	matches, err := blockpack.GetTraceByID(r, traceIDHex)
	if err != nil {
		return nil, fmt.Errorf("GetTraceByID: %w", err)
	}

	if len(matches) == 0 {
		return nil, nil
	}

	trace, err := reconstructTrace(id, matches)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct trace: %w", err)
	}

	return &tempopb.TraceByIDResponse{Trace: trace}, nil
}

// Search performs a search across the blockpack block
// Uses blockpack's query engine for tag/duration filtering
func (b *blockpackBlock) Search(ctx context.Context, req *tempopb.SearchRequest,
	_ common.SearchOptions) (*tempopb.SearchResponse, error) {
	// Build TraceQL query from SearchRequest
	query := buildSearchQuery(req)

	// Execute TraceQL query using public API
	matches, _, err := b.executeQuery(query, blockpack.QueryOptions{
		Limit: int(req.Limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Group matches by trace ID
	traceMatches := make(map[string][]blockpack.SpanMatch)
	for _, match := range matches {
		traceMatches[match.TraceID] = append(traceMatches[match.TraceID], match)
	}

	// Build search response with trace metadata
	traces := make([]*tempopb.TraceSearchMetadata, 0, len(traceMatches))
	for traceID, spans := range traceMatches {
		metadata := buildTraceMetadata(traceID, spans)
		traces = append(traces, metadata)
	}

	return &tempopb.SearchResponse{
		Traces: traces,
	}, nil
}

// SearchTags implements the Searcher interface.
// Column names are read from the file header index (no block I/O required).
func (b *blockpackBlock) SearchTags(_ context.Context, scope traceql.AttributeScope, cb common.TagsCallback, _ common.MetricsCallback, _ common.SearchOptions) error {
	r, err := b.newReader()
	if err != nil {
		return fmt.Errorf("SearchTags: open reader: %w", err)
	}

	seen := make(map[string]struct{})
	for _, col := range blockpack.ColumnNames(r) {
		tag := columnNameToTag(col, scope)
		if tag != "" {
			seen[tag] = struct{}{}
		}
	}
	for tag := range seen {
		cb(tag, scope)
	}
	return nil
}

// SearchTagValues implements the Searcher interface
// Extracts unique values for a given tag
func (b *blockpackBlock) SearchTagValues(ctx context.Context, tag string, cb common.TagValuesCallback, _ common.MetricsCallback, _ common.SearchOptions) error {
	// Use empty TraceQL query to match all spans, then extract tag values
	// Tag names like "service.name" become column names like "resource.service.name"
	colName := tagToColumnName(tag)
	query := "{}" // Match all spans

	// Execute TraceQL query using public API
	matches, _, err := b.executeQuery(query, blockpack.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract unique values and call callback (deduplicate in code)
	seen := make(map[string]struct{})
	for _, match := range matches {
		if val, ok := match.Fields.GetField(colName); ok {
			valStr := fmt.Sprintf("%v", val)
			if _, exists := seen[valStr]; !exists {
				seen[valStr] = struct{}{}
				cb(valStr)
			}
		}
	}

	return nil
}

// SearchTagValuesV2 implements the Searcher interface
func (b *blockpackBlock) SearchTagValuesV2(ctx context.Context, tag traceql.Attribute, cb common.TagValuesCallbackV2, _ common.MetricsCallback, _ common.SearchOptions) error {
	// Convert traceql.Attribute to column name
	colName := tagToColumnName(tag.Name)
	// Use match-all TraceQL query
	query := "{}"

	// Execute TraceQL query using public API
	matches, _, err := b.executeQuery(query, blockpack.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract unique values and call V2 callback (deduplicate in code)
	seen := make(map[string]struct{})
	for _, match := range matches {
		if val, ok := match.Fields.GetField(colName); ok {
			// Convert to string for deduplication
			valStr := fmt.Sprintf("%v", val)
			if _, exists := seen[valStr]; !exists {
				seen[valStr] = struct{}{}
				// Convert to traceql.StaticType
				staticVal := toStaticType(val)
				cb(staticVal)
			}
		}
	}

	return nil
}

// Fetch implements the Searcher interface, enabling TraceQL query execution.
// Blockpack evaluates the TraceQL filter natively, so we only need to stream
// matching spans, group them by trace ID, and convert to the output format.
func (b *blockpackBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	// Use the original TraceQL query when available — blockpack evaluates it natively
	// with full AND/OR structure preserved. Fall back to conditionsToTraceQL only when
	// called from a non-TraceQL path (tag search, etc.) that has no original query.
	query := conditionsToTraceQL(req.Conditions, req.AllConditions)
	if orig, ok := common.OriginalTraceQLQuery(ctx); ok {
		query = orig
	}

	// Open the reader once. newReaderProvider wraps the S3 backend in SharedLRUProvider so
	// footer, compact trace index, and metadata reads are served from the process-level LRU
	// cache after the first request. Each Fetch call gets its own *Reader (single-goroutine
	// use required — Reader.internStrings is not thread-safe).
	r, err := b.newReader()
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: open reader: %w", err)
	}

	// Time-range pre-filter: use the reader's TS index to skip this block entirely when
	// no internal blocks overlap the query window. BlocksInTimeRange does an O(log n)
	// binary search over the per-file timestamp index (populated at NewReader time, so no
	// extra I/O). A non-nil empty result means the TS index exists and nothing overlaps —
	// we can skip all block I/O. A nil result means the index is absent (old file format);
	// in that case we fall through and let QueryTraceQL scan all blocks via BlockMeta.
	//
	// QueryOptions.StartNano/EndNano are also passed to QueryTraceQL below, which wires
	// them into CollectOptions.TimeRange for internal block-level pruning during the query.
	if req.StartTimeUnixNanos > 0 && req.EndTimeUnixNanos > 0 {
		if blocks := r.BlocksInTimeRange(req.StartTimeUnixNanos, req.EndTimeUnixNanos); blocks != nil && len(blocks) == 0 {
			return traceql.FetchSpansResponse{
				Results: &sliceSpansetIterator{},
				Bytes:   func() uint64 { return 0 },
			}, nil
		}
	}

	type traceEntry struct {
		traceID  []byte
		spans    []traceql.Span
		rawSpans []blockpack.SpanMatch
	}
	traceMap := make(map[string]*traceEntry)
	var traceOrder []string

	maxTraces := opts.MaxTraces

	// spanLimit lets Collect stop reading block groups early once enough spans have been
	// found. We request maxTraces * 20 spans to ensure diverse trace coverage while
	// bounding I/O: Collect fetches blocks lazily in ~8 MB coalesced batches and stops
	// as soon as spanLimit spans are accumulated (SPEC-STREAM-2 / SPEC-STREAM-4).
	spanLimit := 0
	if maxTraces > 0 {
		spanLimit = maxTraces * 20
	}

	// Build the select-column list: query conditions + search metadata columns.
	// The metadata columns are required by SpanMatchesMetadata (service.name, span
	// name, timestamps) and trace/span IDs for result building. Limiting to this
	// set matches parquet's selective fetch behavior and prevents Grafana data frame
	// type panics when spans have inconsistent attribute sets across results.
	// Always include all intrinsic columns — they are needed by AttributeFor
	// (kind, status, duration, etc.) and SpanMatchesMetadata (service.name,
	// span name, timestamps). User-defined span/resource attributes are added
	// below from query conditions only.
	selectCols := []string{
		"resource.service.name",
		blockpack.IntrinsicColumnName("name"),
		blockpack.IntrinsicColumnName("start"),
		blockpack.IntrinsicColumnName("end"),
		blockpack.IntrinsicColumnName("duration"),
		blockpack.IntrinsicColumnName("status"),
		blockpack.IntrinsicColumnName("status_message"),
		blockpack.IntrinsicColumnName("kind"),
		blockpack.IntrinsicColumnName("id"),
		blockpack.IntrinsicColumnName("parent_id"),
		blockpack.TraceIDColumnName,
	}
	for _, cond := range req.Conditions {
		if col := attributeToColumnName(cond.Attribute); col != "" {
			selectCols = append(selectCols, col)
		}
	}

	var fetchErr error
	var matches []blockpack.SpanMatch
	matches, _, fetchErr = blockpack.QueryTraceQL(r, query, blockpack.QueryOptions{
		Limit:         spanLimit,
		MostRecent:    common.TraceQLMostRecent(ctx),
		StartNano:     req.StartTimeUnixNanos,
		EndNano:       req.EndTimeUnixNanos,
		SelectColumns: selectCols, // nil when empty = return all (match-all queries)
		Embedder:      getProcessEmbedder(configuredEmbedURL),
		// Do not use StartBlock/BlockCount: Tempo's TotalPages concept maps to parquet
		// row groups, not blockpack internal blocks. Passing TotalPages=1 as BlockCount
		// would scan only 1 internal block out of potentially hundreds. Blockpack files
		// are already one job per file at the Tempo level, so scan all internal blocks.
	})
	if fetchErr == nil {
		for i := range matches {
			match := &matches[i]
			if _, exists := traceMap[match.TraceID]; !exists {
				if maxTraces > 0 && len(traceOrder) >= maxTraces {
					continue
				}
				traceIDBytes, decErr := hex.DecodeString(match.TraceID)
				if decErr != nil {
					continue
				}
				traceMap[match.TraceID] = &traceEntry{traceID: traceIDBytes}
				traceOrder = append(traceOrder, match.TraceID)
			}
			traceMap[match.TraceID].spans = append(traceMap[match.TraceID].spans, &blockpackSpan{match: *match})
			traceMap[match.TraceID].rawSpans = append(traceMap[match.TraceID].rawSpans, *match)
		}
	}
	if fetchErr != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: %w", fetchErr)
	}

	if ctx.Err() != nil {
		return traceql.FetchSpansResponse{}, ctx.Err()
	}

	spansets := make([]*traceql.Spanset, 0, len(traceMap))
	for _, traceIDHex := range traceOrder {
		entry := traceMap[traceIDHex]
		rootSpanName, rootServiceName, startNanos, durationNanos := blockpack.SpanMatchesMetadata(entry.rawSpans)
		// Cap returned spans at DefaultSpansPerSpanSet (3) to match standard Tempo
		// search behaviour — large traces would otherwise produce huge payloads.
		// AttributeMatched must reflect the *total* count before the cap.
		totalSpans := len(entry.spans)
		returnedSpans := entry.spans
		if len(returnedSpans) > traceql.DefaultSpansPerSpanSet {
			returnedSpans = returnedSpans[:traceql.DefaultSpansPerSpanSet]
		}
		ss := &traceql.Spanset{
			TraceID:            entry.traceID,
			Spans:              returnedSpans,
			RootSpanName:       rootSpanName,
			RootServiceName:    rootServiceName,
			StartTimeUnixNanos: startNanos,
			DurationNanos:      durationNanos,
			ServiceStats:       spanMatchesServiceStats(entry.rawSpans),
		}
		// Set matched = total span count so asTraceSearchMetadata populates SpanSet.Matched.
		// The engine's SecondPass normally does this, but blockpack returns a pre-built
		// iterator and SecondPass is never called.
		ss.AddAttribute(traceql.AttributeMatched, traceql.NewStaticInt(totalSpans))
		spansets = append(spansets, ss)
	}

	return traceql.FetchSpansResponse{
		Results: &sliceSpansetIterator{spansets: spansets},
		Bytes:   func() uint64 { return b.meta.Size_ },
	}, nil
}

// blockpackSpan implements traceql.Span using a cloned blockpack.SpanMatch.
// Field selectivity is handled upstream by QueryOptions.SelectColumns, which
// limits SpanMatch.Fields to only the queried columns before reaching this type.
type blockpackSpan struct {
	match blockpack.SpanMatch
}

func (s *blockpackSpan) ID() []byte {
	if s.match.SpanID == "" {
		return nil
	}
	b, _ := hex.DecodeString(s.match.SpanID)
	return b
}

func (s *blockpackSpan) hasFields() bool {
	return s.match.Fields != nil
}

func (s *blockpackSpan) StartTimeUnixNanos() uint64 {
	v, _ := s.match.StartNano()
	return v
}

func (s *blockpackSpan) DurationNanos() uint64 {
	v, _ := s.match.DurationNano()
	return v
}

func (s *blockpackSpan) AttributeFor(attr traceql.Attribute) (traceql.Static, bool) {
	if !s.hasFields() {
		return traceql.Static{}, false
	}
	switch attr.Intrinsic {
	case traceql.IntrinsicDuration:
		if u, ok := s.match.DurationNano(); ok {
			return traceql.NewStaticDuration(time.Duration(u)), true
		}
		return traceql.Static{}, false
	case traceql.IntrinsicName:
		if str, ok := s.match.Name(); ok {
			return traceql.NewStaticString(str), true
		}
		return traceql.Static{}, false
	case traceql.IntrinsicStatus:
		if i, ok := s.match.StatusCode(); ok {
			return traceql.NewStaticStatus(otlpStatusToTempoStatus(i)), true
		}
		return traceql.Static{}, false
	case traceql.IntrinsicStatusMessage:
		if v, ok := s.match.Fields.GetField("span:status_message"); ok {
			if str, ok := v.(string); ok {
				return traceql.NewStaticString(str), true
			}
		}
		return traceql.Static{}, false
	case traceql.IntrinsicKind:
		if i, ok := s.match.KindCode(); ok {
			return traceql.NewStaticKind(otlpKindToTempoKind(i)), true
		}
		return traceql.Static{}, false
	case traceql.IntrinsicNone:
		// Fall through to column lookup below.
	default:
		return traceql.Static{}, false
	}

	colName := attributeToColumnName(attr)
	if v, ok := s.match.Fields.GetField(colName); ok {
		return toStaticType(v), true
	}
	return traceql.Static{}, false
}

func (s *blockpackSpan) AllAttributes() map[traceql.Attribute]traceql.Static {
	attrs := make(map[traceql.Attribute]traceql.Static)
	s.AllAttributesFunc(func(a traceql.Attribute, st traceql.Static) {
		attrs[a] = st
	})
	return attrs
}

func (s *blockpackSpan) AllAttributesFunc(cb func(traceql.Attribute, traceql.Static)) {
	if !s.hasFields() {
		return
	}
	s.match.Fields.IterateFields(func(name string, value any) bool {
		attr, ok := columnNameToAttribute(name)
		if !ok {
			return true
		}
		var st traceql.Static
		switch attr.Intrinsic {
		case traceql.IntrinsicDuration:
			if u, ok := value.(uint64); ok {
				st = traceql.NewStaticDuration(time.Duration(u))
			} else {
				return true
			}
		case traceql.IntrinsicStatus:
			if i, ok := value.(int64); ok {
				st = traceql.NewStaticStatus(otlpStatusToTempoStatus(i))
			} else {
				return true
			}
		case traceql.IntrinsicKind:
			if i, ok := value.(int64); ok {
				st = traceql.NewStaticKind(otlpKindToTempoKind(i))
			} else {
				return true
			}
		default:
			// Convert bool to string for display consistency.
			// Grafana's data frame requires uniform types per column; mixed bool/string
			// attribute values across spans cause a type panic in the Tempo datasource plugin.
			if b, ok := value.(bool); ok {
				if b {
					st = traceql.NewStaticString("true")
				} else {
					st = traceql.NewStaticString("false")
				}
			} else {
				st = toStaticType(value)
			}
		}
		cb(attr, st)
		return true
	})
}

// Structural methods are not supported by blockpack (no nested-set indices stored).
func (s *blockpackSpan) SiblingOf(lhs, rhs []traceql.Span, falseForAll, union bool, buffer []traceql.Span) []traceql.Span {
	return nil
}
func (s *blockpackSpan) DescendantOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	return nil
}
func (s *blockpackSpan) ChildOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	return nil
}

// sliceSpansetIterator iterates over a pre-built slice of spansets.
type sliceSpansetIterator struct {
	spansets []*traceql.Spanset
	pos      int
}

func (i *sliceSpansetIterator) Next(_ context.Context) (*traceql.Spanset, error) {
	if i.pos >= len(i.spansets) {
		return nil, nil
	}
	ss := i.spansets[i.pos]
	i.pos++
	return ss, nil
}

func (i *sliceSpansetIterator) Close() {}

// columnNameToAttribute converts a blockpack column name to a traceql.Attribute.
// Returns false for internal columns (IDs, timestamps) that don't map to attributes.
func columnNameToAttribute(colName string) (traceql.Attribute, bool) {
	scope, attrName, isIntrinsic := blockpack.ColumnScope(colName)
	if isIntrinsic {
		switch attrName {
		case "duration":
			return traceql.NewIntrinsic(traceql.IntrinsicDuration), true
		case "name":
			return traceql.NewIntrinsic(traceql.IntrinsicName), true
		case "status":
			return traceql.NewIntrinsic(traceql.IntrinsicStatus), true
		case "status_message":
			return traceql.NewIntrinsic(traceql.IntrinsicStatusMessage), true
		case "kind":
			return traceql.NewIntrinsic(traceql.IntrinsicKind), true
		}
		return traceql.Attribute{}, false // id, parent_id, start, end — not user-visible
	}
	switch scope {
	case "span":
		return traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, attrName), true
	case "resource":
		return traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, attrName), true
	}
	return traceql.Attribute{}, false
}

// FetchTagValues implements the Searcher interface
func (b *blockpackBlock) FetchTagValues(ctx context.Context, req traceql.FetchTagValuesRequest, cb traceql.FetchTagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// Build TraceQL query from conditions
	// If no conditions, match all spans
	query := conditionsToTraceQL(req.Conditions, true) // Use AND for multiple conditions

	// Execute TraceQL query using public API
	matches, _, err := b.executeQuery(query, blockpack.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract column name for the requested tag
	colName := attributeToColumnName(req.TagName)

	// Track unique values to avoid duplicates
	seen := make(map[string]struct{})

	// Call callback for each unique value
	for _, match := range matches {
		if val, ok := match.Fields.GetField(colName); ok {
			staticVal := toStaticType(val)
			// Use string representation as key for deduplication
			key := staticVal.EncodeToString(false)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				if cb(staticVal) {
					break // Callback returned true = stop
				}
			}
		}
	}

	return nil
}

// FetchTagNames implements the Searcher interface
func (b *blockpackBlock) FetchTagNames(ctx context.Context, req traceql.FetchTagsRequest, cb traceql.FetchTagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// If conditions are specified, execute query to filter spans first
	if len(req.Conditions) > 0 {
		query := conditionsToTraceQL(req.Conditions, true)

		// Execute TraceQL query using public API
		matches, _, err := b.executeQuery(query, blockpack.QueryOptions{})
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		// Extract unique tag names from matching spans
		seen := make(map[string]struct{})
		for _, match := range matches {
			match.Fields.IterateFields(func(colName string, _ any) bool {
				tag := columnNameToTag(colName, req.Scope)
				if tag != "" {
					if _, exists := seen[tag]; !exists {
						seen[tag] = struct{}{}
						if cb(tag, req.Scope) {
							return true // Stop iteration
						}
					}
				}
				return false // Continue iteration
			})
		}
	} else {
		// No conditions - get all column names from block schema
		// Read blockpack file to access schema
		rc, size, err := b.reader.StreamReader(ctx, DataFileName, uuid.UUID(b.meta.BlockID), b.meta.TenantID)
		if err != nil {
			return fmt.Errorf("failed to open blockpack file: %w", err)
		}
		defer rc.Close()

		data := make([]byte, size)
		_, err = io.ReadFull(rc, data)
		if err != nil {
			return fmt.Errorf("failed to read blockpack file: %w", err)
		}

		provider := &bytesReaderProvider{data: data}
		bpr, err := blockpack.NewReaderFromProvider(provider)
		if err != nil {
			return fmt.Errorf("failed to create blockpack reader: %w", err)
		}

		// Extract tag names from all blocks, deduplicated
		seen := make(map[string]struct{})
		done := false
		allBlockIDs := make([]int, bpr.BlockCount())
		for i := range allBlockIDs {
			allBlockIDs[i] = i
		}
	groupLoop:
		for _, group := range bpr.CoalescedGroups(allBlockIDs) {
			rawMap, fetchErr := bpr.ReadGroup(group)
			if fetchErr != nil {
				return fmt.Errorf("failed to read block group: %w", fetchErr)
			}
			for _, blockIdx := range group.BlockIDs {
				raw, ok := rawMap[blockIdx]
				if !ok {
					continue
				}
				bwb, err := bpr.ParseBlockFromBytes(raw, nil, bpr.BlockMeta(blockIdx))
				if err != nil {
					return fmt.Errorf("failed to parse block %d: %w", blockIdx, err)
				}
				for colKey := range bwb.Block.Columns() {
					tag := columnNameToTag(colKey.Name, req.Scope)
					if tag == "" {
						continue
					}
					if _, exists := seen[tag]; exists {
						continue
					}
					seen[tag] = struct{}{}
					if cb(tag, req.Scope) {
						done = true
						break groupLoop
					}
				}
			}
		}
		_ = done
	}

	return nil
}

// Validate validates the blockpack block
// Ensures the blockpack file is readable and well-formed
func (b *blockpackBlock) Validate(ctx context.Context) error {
	// Convert backend.UUID (array) to uuid.UUID
	blockUUID := uuid.UUID(b.meta.BlockID)

	// Read blockpack file header to verify it's valid
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	// Read file data
	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Attempt to open as blockpack - this validates the file format
	provider := &bytesReaderProvider{data: data}
	_, err = blockpack.NewReaderFromProvider(provider)
	if err != nil {
		return fmt.Errorf("invalid blockpack file: %w", err)
	}

	return nil
}

// conditionsToTraceQL converts traceql.Condition objects to a TraceQL query string
// allConditions determines if conditions are combined with && (true) or || (false)
func conditionsToTraceQL(conditions []traceql.Condition, allConditions bool) string {
	if len(conditions) == 0 {
		return "{}" // Match all spans
	}

	expressions := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		expr := conditionToTraceQLExpr(cond)
		if expr != "" {
			expressions = append(expressions, expr)
		}
	}

	if len(expressions) == 0 {
		return "{}" // No valid conditions, match all
	}

	// Combine expressions with && or ||
	combiner := "||"
	if allConditions {
		combiner = "&&"
	}

	return fmt.Sprintf("{ %s }", strings.Join(expressions, fmt.Sprintf(" %s ", combiner)))
}

// conditionToTraceQLExpr converts a single Condition to a TraceQL expression
func conditionToTraceQLExpr(cond traceql.Condition) string {
	// Handle special case: OpNone means just select the attribute without filtering
	if cond.Op == traceql.OpNone {
		return ""
	}

	// Build attribute name with scope
	attrName := attributeToTraceQLName(cond.Attribute)

	// Handle operators
	switch cond.Op {
	case traceql.OpEqual:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s = %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpNotEqual:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s != %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpRegex:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s =~ %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpNotRegex:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s !~ %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpGreater:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s > %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpGreaterEqual:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s >= %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpLess:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s < %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	case traceql.OpLessEqual:
		if len(cond.Operands) > 0 {
			return fmt.Sprintf(`%s <= %s`, attrName, staticToTraceQLValue(cond.Operands[0]))
		}
	}

	return ""
}

// attributeToTraceQLName converts a traceql.Attribute to a TraceQL field name
func attributeToTraceQLName(attr traceql.Attribute) string {
	// Handle intrinsics
	if attr.Intrinsic != traceql.IntrinsicNone {
		switch attr.Intrinsic {
		case traceql.IntrinsicDuration:
			return "duration"
		case traceql.IntrinsicName:
			return "name"
		case traceql.IntrinsicStatus:
			return "status"
		case traceql.IntrinsicKind:
			return "kind"
		case traceql.IntrinsicTraceID:
			return "trace:id"
		case traceql.IntrinsicSpanID:
			return "span:id"
		}
	}

	// Handle scoped attributes
	scope := attr.Scope.String()
	if scope == "none" || scope == "" {
		// Unscoped attributes (e.g. .http.method, .service.name): emit with a leading dot.
		// Blockpack's compiler expands these to Union(resource.X, span.X), matching both
		// resource and span columns — consistent with TraceQL semantics.
		return "." + attr.Name
	}

	return fmt.Sprintf("%s.%s", scope, attr.Name)
}

// staticToTraceQLValue converts a Static value to a TraceQL literal string
func staticToTraceQLValue(s traceql.Static) string {
	switch s.Type {
	case traceql.TypeString:
		// Get string value from Static (uses EncodeToString but fix quotes)
		str := s.EncodeToString(false) // Without backticks
		return fmt.Sprintf(`"%s"`, str)
	case traceql.TypeInt:
		if i, ok := s.Int(); ok {
			return fmt.Sprintf("%d", i)
		}
	case traceql.TypeFloat:
		return fmt.Sprintf("%f", s.Float())
	case traceql.TypeBoolean:
		if b, ok := s.Bool(); ok {
			return fmt.Sprintf("%t", b)
		}
	case traceql.TypeDuration:
		if d, ok := s.Duration(); ok {
			return d.String()
		}
	case traceql.TypeStatus:
		if status, ok := s.Status(); ok {
			return status.String()
		}
	case traceql.TypeKind:
		if kind, ok := s.Kind(); ok {
			return kind.String()
		}
	}
	return ""
}

// bytesReaderProvider implements blockpack.ReaderProvider for in-memory data
type bytesReaderProvider struct {
	data []byte
}

func (p *bytesReaderProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

func (p *bytesReaderProvider) ReadAt(b []byte, off int64, dataType blockpack.DataType) (int, error) {
	// dataType is a hint for caching optimization - we ignore it for in-memory data
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *bytesReaderProvider) Delete() error { return nil }

// rowFieldsProvider implements blockpack.SpanFieldsProvider for a single row within a Block.
// It mirrors modulesSpanFieldsAdapter (internal to blockpack) but is safe to construct
// from outside the package since it works via the exported Column accessors.
type rowFieldsProvider struct {
	block  *blockpack.Block
	rowIdx int
}

func (p *rowFieldsProvider) GetField(name string) (any, bool) {
	col := p.block.GetColumn(name)
	if col == nil {
		return nil, false
	}
	return columnValue(col, p.rowIdx)
}

func (p *rowFieldsProvider) IterateFields(fn func(name string, value any) bool) {
	seen := make(map[string]struct{})
	for key, col := range p.block.Columns() {
		if _, already := seen[key.Name]; already {
			continue
		}
		v, ok := columnValue(col, p.rowIdx)
		if !ok {
			continue
		}
		seen[key.Name] = struct{}{}
		if !fn(key.Name, v) {
			return
		}
	}
}

// columnValue extracts the typed value from a blockpack column at rowIdx.
// Each column has exactly one type populated, so we try each accessor in order.
func columnValue(col *blockpack.Column, rowIdx int) (any, bool) {
	if v, ok := col.BytesValue(rowIdx); ok {
		return v, true
	}
	if v, ok := col.StringValue(rowIdx); ok {
		return v, true
	}
	if v, ok := col.Uint64Value(rowIdx); ok {
		return v, true
	}
	if v, ok := col.Int64Value(rowIdx); ok {
		return v, true
	}
	if v, ok := col.Float64Value(rowIdx); ok {
		return v, true
	}
	if v, ok := col.BoolValue(rowIdx); ok {
		return v, true
	}
	return nil, false
}

// blockpackValueToAnyValue converts a blockpack field value to an OTLP AnyValue.
// Returns nil for unrecognised types so callers can skip unsupported values.
func blockpackValueToAnyValue(value any) *tempocommon.AnyValue {
	switch v := value.(type) {
	case string:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}
	case int64:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: v}}
	case uint64:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: int64(v)}}
	case float64:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_DoubleValue{DoubleValue: v}}
	case bool:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BoolValue{BoolValue: v}}
	case []byte:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BytesValue{BytesValue: v}}
	}
	return nil
}

// reconstructTrace rebuilds a tempopb.Trace from blockpack span matches
// Groups spans by resource and scope to create proper OTLP hierarchy
func reconstructTrace(traceID common.ID, matches []blockpack.SpanMatch) (*tempopb.Trace, error) {
	// Group spans by resource, then by scope
	type spanWithAttrs struct {
		span          *tempotrace.Span
		resourceAttrs []*tempocommon.KeyValue
		scopeName     string
		scopeVersion  string
	}

	spansData := make([]spanWithAttrs, 0, len(matches))

	for _, match := range matches {
		span := &tempotrace.Span{
			TraceId: traceID,
		}
		var resourceAttrs []*tempocommon.KeyValue
		var scopeName, scopeVersion string

		// Decode span ID — use SpanID directly from the match struct (hex-encoded 8-byte span ID).
		// match.Fields.GetField("span:id") returns []byte, not string, so we use match.SpanID.
		if match.SpanID != "" {
			if spanIDBytes, err := hex.DecodeString(match.SpanID); err == nil {
				span.SpanId = spanIDBytes
			}
		}

		// Extract fields from blockpack data
		match.Fields.IterateFields(func(name string, value interface{}) bool {
			switch name {
			case "span:name":
				if v, ok := value.(string); ok {
					span.Name = v
				}
			case "span:parent_id":
				if v, ok := value.([]byte); ok && len(v) > 0 {
					span.ParentSpanId = v
				}
			case "span:start":
				if v, ok := value.(uint64); ok {
					span.StartTimeUnixNano = v
				}
			case "span:end":
				if v, ok := value.(uint64); ok {
					span.EndTimeUnixNano = v
				}
			case "span:kind":
				if v, ok := value.(int64); ok {
					span.Kind = tempotrace.Span_SpanKind(v)
				}
			case "span:status":
				if v, ok := value.(int64); ok {
					span.Status = &tempotrace.Status{
						Code: tempotrace.Status_StatusCode(v),
					}
				}
			case "span:status_message":
				if v, ok := value.(string); ok {
					if span.Status == nil {
						span.Status = &tempotrace.Status{}
					}
					span.Status.Message = v
				}
			case "resource.service.name":
				if v, ok := value.(string); ok {
					resourceAttrs = append(resourceAttrs, &tempocommon.KeyValue{
						Key: "service.name",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: v},
						},
					})
				}
			case "scope.name":
				if v, ok := value.(string); ok {
					scopeName = v
				}
			case "scope.version":
				if v, ok := value.(string); ok {
					scopeVersion = v
				}
			default:
				// span.* → span attributes; resource.* → resource attributes (service.name handled above)
				if strings.HasPrefix(name, "span.") {
					key := strings.TrimPrefix(name, "span.")
					if av := blockpackValueToAnyValue(value); av != nil {
						span.Attributes = append(span.Attributes, &tempocommon.KeyValue{Key: key, Value: av})
					}
				} else if strings.HasPrefix(name, "resource.") {
					key := strings.TrimPrefix(name, "resource.")
					if av := blockpackValueToAnyValue(value); av != nil {
						resourceAttrs = append(resourceAttrs, &tempocommon.KeyValue{Key: key, Value: av})
					}
				}
			}
			return true
		})

		// Always set a non-nil Status — Grafana's trace_transform.go dereferences it unconditionally.
		if span.Status == nil {
			span.Status = &tempotrace.Status{}
		}

		// Set defaults for required OTLP fields
		span.DroppedAttributesCount = 0
		span.DroppedEventsCount = 0
		span.DroppedLinksCount = 0

		// Default scope name
		if scopeName == "" {
			scopeName = "blockpack"
		}

		spansData = append(spansData, spanWithAttrs{
			span:          span,
			resourceAttrs: resourceAttrs,
			scopeName:     scopeName,
			scopeVersion:  scopeVersion,
		})
	}

	// Group spans by resource and scope
	type resourceScope struct {
		resourceKey string
		scopeName   string
		scopeVer    string
	}

	groupedSpans := make(map[resourceScope]struct {
		resourceAttrs []*tempocommon.KeyValue
		spans         []*tempotrace.Span
	})

	for _, sd := range spansData {
		// Create resource key from attributes
		resourceKey := ""
		for _, attr := range sd.resourceAttrs {
			if attr.Key == "service.name" {
				if sv, ok := attr.Value.Value.(*tempocommon.AnyValue_StringValue); ok {
					resourceKey = sv.StringValue
				}
			}
		}

		key := resourceScope{
			resourceKey: resourceKey,
			scopeName:   sd.scopeName,
			scopeVer:    sd.scopeVersion,
		}
		group := groupedSpans[key]
		if group.resourceAttrs == nil {
			group.resourceAttrs = sd.resourceAttrs
		}
		group.spans = append(group.spans, sd.span)
		groupedSpans[key] = group
	}

	// Build ResourceSpans
	resourceSpans := make([]*tempotrace.ResourceSpans, 0, len(groupedSpans))
	for key, group := range groupedSpans {
		rs := &tempotrace.ResourceSpans{
			Resource: &temporesource.Resource{
				Attributes: group.resourceAttrs,
			},
			ScopeSpans: []*tempotrace.ScopeSpans{
				{
					Scope: &tempocommon.InstrumentationScope{
						Name:    key.scopeName,
						Version: key.scopeVer,
					},
					Spans: group.spans,
				},
			},
		}
		resourceSpans = append(resourceSpans, rs)
	}

	return &tempopb.Trace{
		ResourceSpans: resourceSpans,
	}, nil
}

// columnNameToTag converts a blockpack column name to a tag name based on scope
// Returns empty string if the column doesn't match the requested scope
func columnNameToTag(colName string, scope traceql.AttributeScope) string {
	// Blockpack column naming:
	// - span.* for span attributes
	// - resource.* for resource attributes
	// - span:* for intrinsic span fields
	// - trace:* for trace fields
	// - scope.* for scope attributes

	switch scope {
	case traceql.AttributeScopeSpan:
		if len(colName) > 5 && colName[:5] == "span." {
			return colName[5:] // Remove "span." prefix
		}
		// Also include intrinsic span fields
		if len(colName) > 5 && colName[:5] == "span:" {
			return colName // Keep full name for intrinsics
		}
	case traceql.AttributeScopeResource:
		if len(colName) > 9 && colName[:9] == "resource." {
			return colName[9:] // Remove "resource." prefix
		}
	case traceql.AttributeScopeNone:
		// Return all attributes
		if len(colName) > 5 && (colName[:5] == "span." || colName[:5] == "span:") {
			return colName
		}
		if len(colName) > 9 && colName[:9] == "resource." {
			return colName
		}
		if len(colName) > 6 && colName[:6] == "scope." {
			return colName
		}
	}

	return ""
}

// tagToColumnName converts a tag name back to a blockpack column name
// e.g., "service.name" -> "resource.service.name", "name" -> "span:name"
// Blockpack uses colons (:) not dots (.) for intrinsic span fields
func tagToColumnName(tag string) string {
	// Common resource attributes
	if tag == "service.name" || tag == "service.namespace" || tag == "deployment.environment" {
		return "resource." + tag
	}

	// If it already has a prefix, return as-is
	if len(tag) > 5 && (tag[:5] == "span." || tag[:5] == "span:") {
		return tag
	}
	if len(tag) > 9 && tag[:9] == "resource." {
		return tag
	}
	if len(tag) > 6 && tag[:6] == "scope." {
		return tag
	}

	// Default to span attribute with colon (blockpack format)
	return "span:" + tag
}

// attributeToColumnName converts a traceql.Attribute to a blockpack column name.
func attributeToColumnName(attr traceql.Attribute) string {
	if attr.Intrinsic != traceql.IntrinsicNone {
		switch attr.Intrinsic {
		case traceql.IntrinsicDuration:
			return blockpack.IntrinsicColumnName("duration")
		case traceql.IntrinsicName:
			return blockpack.IntrinsicColumnName("name")
		case traceql.IntrinsicStatus:
			return blockpack.IntrinsicColumnName("status")
		case traceql.IntrinsicKind:
			return blockpack.IntrinsicColumnName("kind")
		case traceql.IntrinsicTraceID:
			return blockpack.TraceIDColumnName
		case traceql.IntrinsicSpanID:
			return blockpack.IntrinsicColumnName("id")
		case traceql.IntrinsicParentID:
			return blockpack.IntrinsicColumnName("parent_id")
		}
	}

	switch attr.Scope {
	case traceql.AttributeScopeResource:
		return blockpack.AttributeColumnName("resource", attr.Name)
	default: // span or unscoped — default to span
		return blockpack.AttributeColumnName("span", attr.Name)
	}
}

// otlpKindToTempoKind converts an OTLP SpanKind int64 to Tempo's traceql.Kind enum.
// OTLP: UNSPECIFIED=0, INTERNAL=1, SERVER=2, CLIENT=3, PRODUCER=4, CONSUMER=5
// Tempo: KindUnspecified=0, KindInternal=1, KindClient=2, KindServer=3, KindProducer=4, KindConsumer=5
// CLIENT and SERVER are swapped between OTLP and Tempo, requiring explicit conversion.
func otlpKindToTempoKind(otlp int64) traceql.Kind {
	switch otlp {
	case 0:
		return traceql.KindUnspecified
	case 1:
		return traceql.KindInternal
	case 2:
		return traceql.KindServer
	case 3:
		return traceql.KindClient
	case 4:
		return traceql.KindProducer
	case 5:
		return traceql.KindConsumer
	default:
		return traceql.KindUnspecified
	}
}

// otlpStatusToTempoStatus converts an OTLP StatusCode int64 to Tempo's traceql.Status enum.
// OTLP: UNSET=0, OK=1, ERROR=2
// Tempo: StatusError=0, StatusOk=1, StatusUnset=2
// The orderings differ, requiring explicit conversion.
func otlpStatusToTempoStatus(otlp int64) traceql.Status {
	switch otlp {
	case 0:
		return traceql.StatusUnset
	case 1:
		return traceql.StatusOk
	case 2:
		return traceql.StatusError
	default:
		return traceql.StatusUnset
	}
}

// toStaticType converts a Go value to traceql.Static
func toStaticType(val interface{}) traceql.Static {
	switch v := val.(type) {
	case string:
		return traceql.NewStaticString(v)
	case int:
		return traceql.NewStaticInt(v)
	case int64:
		return traceql.NewStaticInt(int(v))
	case uint64:
		return traceql.NewStaticInt(int(v))
	case float64:
		return traceql.NewStaticFloat(v)
	case bool:
		return traceql.NewStaticBool(v)
	default:
		return traceql.NewStaticString(fmt.Sprintf("%v", v))
	}
}

// columnNameToTraceQLAttr converts a blockpack column name to TraceQL attribute format
// e.g., "span:name" -> "name", "resource.service.name" -> "resource.service.name"
func columnNameToTraceQLAttr(tag string) string {
	// If already a column name with colon, convert to TraceQL format
	if strings.HasPrefix(tag, "span:") {
		return strings.TrimPrefix(tag, "span:")
	}
	if strings.HasPrefix(tag, "resource.") {
		return tag // Keep resource attributes as-is
	}
	// Otherwise assume it's already in TraceQL format
	return tag
}

// buildSearchQuery converts a SearchRequest to TraceQL
func buildSearchQuery(req *tempopb.SearchRequest) string {
	conditions := make([]string, 0)

	// Add tag filters
	for key, value := range req.Tags {
		// TraceQL uses dot notation for attributes
		// Convert column name format to TraceQL attribute format
		attr := columnNameToTraceQLAttr(key)
		// TraceQL string comparison uses =~ for regex/contains
		conditions = append(conditions, fmt.Sprintf(`%s =~ ".*%s.*"`, attr, value))
	}

	// Add duration filters
	if req.MinDurationMs > 0 {
		conditions = append(conditions, fmt.Sprintf(`duration >= %dms`, req.MinDurationMs))
	}
	if req.MaxDurationMs > 0 {
		conditions = append(conditions, fmt.Sprintf(`duration <= %dms`, req.MaxDurationMs))
	}

	// Build final TraceQL query
	if len(conditions) == 0 {
		return "" // Empty query matches all spans
	}

	return fmt.Sprintf("{ %s }", strings.Join(conditions, " && "))
}

// buildTraceMetadata creates trace search metadata from span matches
func buildTraceMetadata(traceID string, spans []blockpack.SpanMatch) *tempopb.TraceSearchMetadata {
	metadata := &tempopb.TraceSearchMetadata{
		TraceID: traceID,
	}

	if len(spans) == 0 {
		return metadata
	}

	// Find min/max times
	var minStart, maxEnd uint64 = ^uint64(0), 0
	for _, span := range spans {
		if start, ok := span.Fields.GetField("span:start"); ok {
			if st, ok := start.(uint64); ok && st < minStart {
				minStart = st
			}
		}
		if end, ok := span.Fields.GetField("span:end"); ok {
			if et, ok := end.(uint64); ok && et > maxEnd {
				maxEnd = et
			}
		}
	}

	metadata.StartTimeUnixNano = minStart
	if maxEnd > minStart {
		metadata.DurationMs = uint32((maxEnd - minStart) / 1000000)
	}

	return metadata
}

// computeSpansetMetadata derives the four trace-level Spanset metadata fields from
// the raw blockpack.SpanMatch values for a single trace.
//
// Root span detection: a span is the root if its "span:parent_id" field is absent.
// The blockpack writer only marks the column present when ParentSpanId is non-empty,
// so column absence is the sole reliable root signal. If no root span is found in the
// filtered result set (e.g. the TraceQL filter only matched child spans), the span with
// the minimum span:start is used as a proxy for RootSpanName and RootServiceName, and
// DurationNanos falls back to max(span:end) - min(span:start) across all matched spans.
// spanMatchesServiceStats converts blockpack.SpanMatchesServiceStats result to traceql.ServiceStats.
// The two types have identical fields; the conversion is necessary because traceql.ServiceStats
// is defined in the Tempo module and cannot be used in blockpack directly.
func spanMatchesServiceStats(spans []blockpack.SpanMatch) map[string]traceql.ServiceStats {
	bpStats := blockpack.SpanMatchesServiceStats(spans)
	if len(bpStats) == 0 {
		return nil
	}
	out := make(map[string]traceql.ServiceStats, len(bpStats))
	for svc, s := range bpStats {
		out[svc] = traceql.ServiceStats{SpanCount: s.SpanCount, ErrorCount: s.ErrorCount}
	}
	return out
}
