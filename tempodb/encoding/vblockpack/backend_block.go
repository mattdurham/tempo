package vblockpack

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

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
func (b *blockpackBlock) FindTraceByID(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
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
// Uses blockpack's query engine for TraceQL/SQL queries
func (b *blockpackBlock) Search(ctx context.Context, req *tempopb.SearchRequest,
	opts common.SearchOptions) (*tempopb.SearchResponse, error) {
	// TODO: Implement using blockpack query engine
	// - Parse search request
	// - Compile to blockpack query
	// - Execute against blockpack file
	// - Convert results to SearchResponse format
	return &tempopb.SearchResponse{}, nil
}

// SearchTags implements the Searcher interface
// Extracts unique tag names from blockpack metadata
func (b *blockpackBlock) SearchTags(ctx context.Context, scope traceql.AttributeScope, cb common.TagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// TODO: Read blockpack column names and convert to tags
	// Blockpack tracks all column names in metadata
	return nil
}

// SearchTagValues implements the Searcher interface
// Extracts unique values for a given tag
func (b *blockpackBlock) SearchTagValues(ctx context.Context, tag string, cb common.TagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// TODO: Query blockpack for distinct values of column
	// May use dedicated index if available
	return nil
}

// SearchTagValuesV2 implements the Searcher interface
func (b *blockpackBlock) SearchTagValuesV2(ctx context.Context, tag traceql.Attribute, cb common.TagValuesCallbackV2, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// TODO: Same as SearchTagValues but with V2 callback
	return nil
}

// Fetch implements the Searcher interface
// Executes TraceQL query and returns matching spans
func (b *blockpackBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	// TODO: Execute TraceQL query using blockpack executor
	// - Compile TraceQL to blockpack query
	// - Execute query
	// - Convert results to FetchSpansResponse
	return traceql.FetchSpansResponse{}, nil
}

// FetchTagValues implements the Searcher interface
func (b *blockpackBlock) FetchTagValues(ctx context.Context, req traceql.FetchTagValuesRequest, cb traceql.FetchTagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// TODO: Fetch tag values with TraceQL filtering
	return nil
}

// FetchTagNames implements the Searcher interface
func (b *blockpackBlock) FetchTagNames(ctx context.Context, req traceql.FetchTagsRequest, cb traceql.FetchTagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	// TODO: Fetch tag names with TraceQL filtering
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

func (m *memoryStorage) Get(path string) ([]byte, error) {
	return m.data, nil
}

func (m *memoryStorage) GetProvider(path string) (blockpackio.ReaderProvider, error) {
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
