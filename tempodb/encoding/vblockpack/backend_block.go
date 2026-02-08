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
	"github.com/mattdurham/blockpack"
)

// tempoStorage adapts Tempo's backend.Reader to blockpack.Storage interface
type tempoStorage struct {
	reader   backend.Reader
	tenantID string
	blockID  uuid.UUID
}

func (s *tempoStorage) Size(path string) (int64, error) {
	// Path is ignored - we always read from our specific block
	// Use StreamReader to get size, then close immediately
	rc, size, err := s.reader.StreamReader(context.Background(), DataFileName, s.blockID, s.tenantID)
	if err != nil {
		return 0, err
	}
	rc.Close()
	return size, nil
}

func (s *tempoStorage) ReadAt(path string, p []byte, off int64, dataType blockpack.DataType) (int, error) {
	// Path is ignored - we always read from our specific block
	// dataType is a hint for caching optimization - we ignore it for now
	// Validate offset
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}

	// Use ReadRange for efficient partial reads
	err := s.reader.ReadRange(context.Background(), DataFileName, s.blockID, s.tenantID, uint64(off), p, nil)
	if err != nil {
		// Check if this is an EOF condition
		if off >= 0 {
			// Get size to check if offset is beyond file
			size, sizeErr := s.Size(path)
			if sizeErr == nil && off >= size {
				return 0, io.EOF
			}
		}
		return 0, err
	}

	// ReadRange fills the entire buffer or returns an error
	return len(p), nil
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

	// Build TraceQL query to find all spans for this trace
	// Use explicit column name with colon notation (passes through normalization)
	query := fmt.Sprintf(`{ trace:id = "%s" }`, traceIDHex)

	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// Execute TraceQL query using public API
	result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{})
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
	// Build TraceQL query from SearchRequest
	query := buildSearchQuery(req)

	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// Execute TraceQL query using public API
	result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{
		Limit: int(req.Limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Group matches by trace ID
	traceMatches := make(map[string][]blockpack.SpanMatch)
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

	// Create blockpack reader from bytes using public API
	provider := &bytesReaderProvider{data: data}
	bpr, err := blockpack.NewReaderFromProvider(provider)
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
	// Use empty TraceQL query to match all spans, then extract tag values
	// Tag names like "service.name" become column names like "resource.service.name"
	colName := tagToColumnName(tag)
	query := "{}" // Match all spans

	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// Execute TraceQL query using public API
	result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{})
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
	// Convert traceql.Attribute to column name
	colName := tagToColumnName(tag.Name)
	// Use match-all TraceQL query
	query := "{}"

	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// Execute TraceQL query using public API
	result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{})
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
func (b *blockpackBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, _ common.SearchOptions) (traceql.FetchSpansResponse, error) {
	// TODO: Full Fetch implementation requires implementing:
	// 1. Span interface for blockpack spans
	// 2. SpansetIterator for iterating through matching spans
	// 3. Converting blockpack matches to Spansets with proper metadata
	// For now, return empty response. Most TraceQL operations use Search instead.
	return traceql.FetchSpansResponse{}, nil
}

// FetchTagValues implements the Searcher interface
func (b *blockpackBlock) FetchTagValues(ctx context.Context, req traceql.FetchTagValuesRequest, cb traceql.FetchTagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// Build TraceQL query from conditions
	// If no conditions, match all spans
	query := conditionsToTraceQL(req.Conditions, true) // Use AND for multiple conditions

	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// Execute TraceQL query using public API
	result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Extract column name for the requested tag
	colName := attributeToColumnName(req.TagName)

	// Track unique values to avoid duplicates
	seen := make(map[string]struct{})

	// Call callback for each unique value
	for _, match := range result.Matches {
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
	// Create storage adapter for blockpack query
	blockUUID := uuid.UUID(b.meta.BlockID)
	storage := &tempoStorage{
		reader:   b.reader,
		tenantID: b.meta.TenantID,
		blockID:  blockUUID,
	}

	// If conditions are specified, execute query to filter spans first
	if len(req.Conditions) > 0 {
		query := conditionsToTraceQL(req.Conditions, true)

		// Execute TraceQL query using public API
		result, err := blockpack.ExecuteTraceQL(DataFileName, query, storage, blockpack.QueryOptions{})
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		// Extract unique tag names from matching spans
		seen := make(map[string]struct{})
		for _, match := range result.Matches {
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

		provider := &bytesReaderProvider{data: data}
		bpr, err := blockpack.NewReaderFromProvider(provider)
		if err != nil {
			return fmt.Errorf("failed to create blockpack reader: %w", err)
		}

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
		return attr.Name
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

// attributeToColumnName converts a traceql.Attribute to a blockpack column name
func attributeToColumnName(attr traceql.Attribute) string {
	// Handle intrinsics
	if attr.Intrinsic != traceql.IntrinsicNone {
		switch attr.Intrinsic {
		case traceql.IntrinsicDuration:
			return "span:duration"
		case traceql.IntrinsicName:
			return "span:name"
		case traceql.IntrinsicStatus:
			return "span:status"
		case traceql.IntrinsicKind:
			return "span:kind"
		case traceql.IntrinsicTraceID:
			return "trace:id"
		case traceql.IntrinsicSpanID:
			return "span:id"
		case traceql.IntrinsicParentID:
			return "span:parent_id"
		}
	}

	// Handle scoped attributes
	scope := attr.Scope.String()
	switch scope {
	case "span":
		return "span." + attr.Name
	case "resource":
		return "resource." + attr.Name
	case "none", "":
		// For unscoped, default to span attribute
		return "span." + attr.Name
	}

	return attr.Name
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

// convertToSpansets converts BlockpackSpanMatch to traceql spansets
// Note: This would be used if Fetch were fully implemented
// func convertToSpansets(matches []blockpack.SpanMatch) traceql.SpansetIterator {
//	// Would need to implement a SpansetIterator that returns *Spanset from matches
//	// This is complex and requires building full Span implementations
// }
