package vblockpack

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
	"github.com/mattdurham/blockpack/executor"
	"github.com/mattdurham/blockpack/sql"
)

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

// BlockMeta returns the block metadata
func (b *blockpackBlock) BlockMeta() *backend.BlockMeta {
	return b.meta
}

// FindTraceByID finds a trace by ID in the blockpack block
//
// Implementation: Queries blockpack file for all spans matching trace ID,
// then reconstructs the full trace with proper OTLP hierarchy.
func (b *blockpackBlock) FindTraceByID(ctx context.Context, id common.ID, _ common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	// Convert trace ID to hex string for query
	traceIDHex := hex.EncodeToString(id)

	// Convert backend.UUID (array) to uuid.UUID
	blockUUID := uuid.UUID(b.meta.BlockID)

	// Read blockpack file from backend storage
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	// Read all data into memory
	data := make([]byte, size)
	n, err := io.ReadFull(rc, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read blockpack file: %w", err)
	}
	if int64(n) != size {
		return nil, fmt.Errorf("incomplete read: %d != %d", n, size)
	}

	// Build SQL query to find all spans for this trace
	query := fmt.Sprintf(`SELECT * FROM spans WHERE "trace:id" = '%s'`, traceIDHex)

	// Compile query
	program, err := sql.CompileTraceQLOrSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile query: %w", err)
	}

	// Create blockpack executor with in-memory storage
	storage := &memoryStorage{data: data}
	exec := executor.NewBlockpackExecutor(storage)

	// Execute query
	result, err := exec.ExecuteQuery("", program, nil, executor.QueryOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// No matching spans found
	if len(result.Matches) == 0 {
		return nil, nil // Trace not found
	}

	// Reconstruct trace from spans
	trace, err := reconstructTrace(id, result.Matches)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct trace: %w", err)
	}

	return &tempopb.TraceByIDResponse{
		Trace: trace,
	}, nil
}

// Search performs a search across the blockpack block
// Uses blockpack's query engine for tag/duration filtering
func (b *blockpackBlock) Search(ctx context.Context, req *tempopb.SearchRequest,
	_ common.SearchOptions) (*tempopb.SearchResponse, error) {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Build SQL query from SearchRequest
	query := buildSearchQuery(req)

	// Compile and execute
	program, err := sql.CompileTraceQLOrSQL(query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile query: %w", err)
	}

	storage := &memoryStorage{data: data}
	exec := executor.NewBlockpackExecutor(storage)

	result, err := exec.ExecuteQuery("", program, nil, executor.QueryOptions{
		Limit: int(req.Limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Group matches by trace ID
	traceMatches := make(map[string][]executor.BlockpackSpanMatch)
	for _, match := range result.Matches {
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

// SearchTags implements the Searcher interface
// Extracts unique tag names from blockpack metadata
func (b *blockpackBlock) SearchTags(ctx context.Context, scope traceql.AttributeScope, cb common.TagsCallback, _ common.MetricsCallback, _ common.SearchOptions) error {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Open blockpack reader
	bpr, err := blockpackio.NewReader(data)
	if err != nil {
		return fmt.Errorf("failed to create blockpack reader: %w", err)
	}

	// Collect unique column names from first block
	tags := make(map[string]struct{})
	block, err := bpr.GetBlock(0) // Get first block
	if err != nil {
		return fmt.Errorf("failed to read first block: %w", err)
	}

	// Extract column names and filter by scope
	for colName := range block.Columns() {
		tag := columnNameToTag(colName, scope)
		if tag != "" {
			tags[tag] = struct{}{}
		}
	}

	// Call callback for each tag
	for tag := range tags {
		cb(tag, scope)
	}

	return nil
}

// SearchTagValues implements the Searcher interface
// Extracts unique values for a given tag
func (b *blockpackBlock) SearchTagValues(ctx context.Context, tag string, cb common.TagValuesCallback, _ common.MetricsCallback, _ common.SearchOptions) error {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Build SQL query - use SELECT * since blockpack only supports SELECT * queries
	// Tag names like "service.name" become column names like "resource.service.name"
	colName := tagToColumnName(tag)
	query := "SELECT * FROM spans"

	// Compile and execute query
	program, err := sql.CompileTraceQLOrSQL(query)
	if err != nil {
		return fmt.Errorf("failed to compile query: %w", err)
	}

	storage := &memoryStorage{data: data}
	exec := executor.NewBlockpackExecutor(storage)

	result, err := exec.ExecuteQuery("", program, nil, executor.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract unique values and call callback (deduplicate in code)
	seen := make(map[string]struct{})
	for _, match := range result.Matches {
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
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Convert traceql.Attribute to column name
	colName := tagToColumnName(tag.Name)
	// Use SELECT * since blockpack only supports SELECT * queries
	query := "SELECT * FROM spans"

	// Compile and execute query
	program, err := sql.CompileTraceQLOrSQL(query)
	if err != nil {
		return fmt.Errorf("failed to compile query: %w", err)
	}

	storage := &memoryStorage{data: data}
	exec := executor.NewBlockpackExecutor(storage)

	result, err := exec.ExecuteQuery("", program, nil, executor.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract unique values and call V2 callback (deduplicate in code)
	seen := make(map[string]struct{})
	for _, match := range result.Matches {
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

// Fetch implements the Searcher interface
// Executes TraceQL query and returns matching spans
func (b *blockpackBlock) Fetch(ctx context.Context, _ traceql.FetchSpansRequest, _ common.SearchOptions) (traceql.FetchSpansResponse, error) {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// TODO: Build SQL query from req.Conditions
	// For now, return an error indicating not fully implemented
	// The parquet implementation uses conditions to build iterators
	// We need to convert conditions to SQL WHERE clauses
	return traceql.FetchSpansResponse{}, fmt.Errorf("Fetch not yet implemented for blockpack - requires condition-to-SQL conversion")
}

// FetchTagValues implements the Searcher interface
func (b *blockpackBlock) FetchTagValues(ctx context.Context, req traceql.FetchTagValuesRequest, cb traceql.FetchTagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Build query for distinct tag values
	colName := tagToColumnName(req.TagName.Name)
	query := fmt.Sprintf(`SELECT DISTINCT "%s" FROM spans`, colName)

	// TODO: If req.Conditions is not empty, build WHERE clause
	// For now, we only handle the simple case with no conditions

	// Execute query
	program, err := sql.CompileTraceQLOrSQL(query)
	if err != nil {
		return fmt.Errorf("failed to compile query: %w", err)
	}

	storage := &memoryStorage{data: data}
	exec := executor.NewBlockpackExecutor(storage)

	result, err := exec.ExecuteQuery("", program, nil, executor.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Call callback for each unique value
	for _, match := range result.Matches {
		if val, ok := match.Fields.GetField(colName); ok {
			staticVal := toStaticType(val)
			if cb(staticVal) {
				break // Callback returned true = stop
			}
		}
	}

	return nil
}

// FetchTagNames implements the Searcher interface
func (b *blockpackBlock) FetchTagNames(ctx context.Context, req traceql.FetchTagsRequest, cb traceql.FetchTagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// Read blockpack file
	blockUUID := uuid.UUID(b.meta.BlockID)
	rc, size, err := b.reader.StreamReader(ctx, DataFileName, blockUUID, b.meta.TenantID)
	if err != nil {
		return fmt.Errorf("failed to open blockpack file: %w", err)
	}
	defer rc.Close()

	data := make([]byte, size)
	_, err = io.ReadFull(rc, data)
	if err != nil {
		return fmt.Errorf("failed to read blockpack file: %w", err)
	}

	// Open reader to get column names
	bpr, err := blockpackio.NewReader(data)
	if err != nil {
		return fmt.Errorf("failed to create blockpack reader: %w", err)
	}

	// Get first block to extract column names
	block, err := bpr.GetBlock(0)
	if err != nil {
		return fmt.Errorf("failed to read first block: %w", err)
	}

	// Extract tag names based on scope
	for colName := range block.Columns() {
		tag := columnNameToTag(colName, req.Scope)
		if tag != "" {
			if cb(tag, req.Scope) {
				break // Callback returned true = stop
			}
		}
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
	_, err = blockpackio.NewReader(data)
	if err != nil {
		return fmt.Errorf("invalid blockpack file: %w", err)
	}

	return nil
}

// memoryStorage implements executor.FileStorage and executor.ProviderStorage
// for in-memory blockpack data
type memoryStorage struct {
	data []byte
}

func (m *memoryStorage) Get(_ string) ([]byte, error) {
	return m.data, nil
}

func (m *memoryStorage) GetProvider(_ string) (blockpackio.ReaderProvider, error) {
	return &bytesReaderProvider{data: m.data}, nil
}

// bytesReaderProvider implements blockpackio.ReaderProvider for in-memory data
type bytesReaderProvider struct {
	data []byte
}

func (p *bytesReaderProvider) Size() (int64, error) {
	return int64(len(p.data)), nil
}

func (p *bytesReaderProvider) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(b, p.data[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

// reconstructTrace rebuilds a tempopb.Trace from blockpack span matches
// Groups spans by resource and scope to create proper OTLP hierarchy
func reconstructTrace(traceID common.ID, matches []executor.BlockpackSpanMatch) (*tempopb.Trace, error) {
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

		// Decode span ID
		if spanIDHex, ok := match.Fields.GetField("span:id"); ok {
			if spanIDStr, ok := spanIDHex.(string); ok {
				if spanIDBytes, err := hex.DecodeString(spanIDStr); err == nil {
					span.SpanId = spanIDBytes
				}
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
				if v, ok := value.(string); ok && v != "" {
					if parentIDBytes, err := hex.DecodeString(v); err == nil {
						span.ParentSpanId = parentIDBytes
					}
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
			}
			return true
		})

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

// buildSearchQuery converts a SearchRequest to SQL
func buildSearchQuery(req *tempopb.SearchRequest) string {
	conditions := make([]string, 0)

	// Add tag filters
	for key, value := range req.Tags {
		colName := tagToColumnName(key)
		conditions = append(conditions, fmt.Sprintf(`"%s" LIKE '%%%s%%'`, colName, value))
	}

	// Add duration filters
	if req.MinDurationMs > 0 {
		minNanos := uint64(req.MinDurationMs) * 1000000
		conditions = append(conditions, fmt.Sprintf(`("span:end" - "span:start") >= %d`, minNanos))
	}
	if req.MaxDurationMs > 0 {
		maxNanos := uint64(req.MaxDurationMs) * 1000000
		conditions = append(conditions, fmt.Sprintf(`("span:end" - "span:start") <= %d`, maxNanos))
	}

	// Build final query
	query := "SELECT * FROM spans"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	return query
}

// buildTraceMetadata creates trace search metadata from span matches
func buildTraceMetadata(traceID string, spans []executor.BlockpackSpanMatch) *tempopb.TraceSearchMetadata {
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

// convertToSpansets converts BlockpackSpanMatch to traceql spansets
// Note: This would be used if Fetch were fully implemented
// func convertToSpansets(matches []executor.BlockpackSpanMatch) traceql.SpansetIterator {
//	// Would need to implement a SpansetIterator that returns *Spanset from matches
//	// This is complex and requires building full Span implementations
// }
