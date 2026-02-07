package vblockpack

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type walBlock struct {
	meta           *backend.BlockMeta
	path           string
	ingestionSlack time.Duration

	// In-memory trace accumulation (simplified - production would use blockpack writer)
	mu     sync.Mutex
	traces map[string]*tempopb.Trace
}

// createWALBlock creates a new WAL block
func createWALBlock(meta *backend.BlockMeta, filepath string, ingestionSlack time.Duration) (*walBlock, error) {
	return &walBlock{
		meta:           meta,
		path:           filepath,
		ingestionSlack: ingestionSlack,
		traces:         make(map[string]*tempopb.Trace),
	}, nil
}

// BlockMeta returns the block metadata
func (w *walBlock) BlockMeta() *backend.BlockMeta {
	return w.meta
}

// Append appends a trace (as bytes) to the WAL block
func (w *walBlock) Append(id common.ID, b []byte, start, end uint32, adjustIngestionSlack bool) error {
	// Decode trace from bytes
	trace := &tempopb.Trace{}
	if err := trace.Unmarshal(b); err != nil {
		return fmt.Errorf("failed to unmarshal trace: %w", err)
	}

	return w.AppendTrace(id, trace, start, end, adjustIngestionSlack)
}

// AppendTrace appends a trace object to the WAL block
func (w *walBlock) AppendTrace(id common.ID, tr *tempopb.Trace, start, end uint32, adjustIngestionSlack bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Store trace in memory (simplified - production would write to blockpack)
	traceID := string(id)
	w.traces[traceID] = tr

	// Update metadata
	w.meta.ObjectAdded(start, end)

	return nil
}

// IngestionSlack returns the ingestion slack duration
func (w *walBlock) IngestionSlack() time.Duration {
	return w.ingestionSlack
}

// Flush writes accumulated traces to disk (stub - needs blockpack writer integration)
func (w *walBlock) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Integrate with blockpack writer to serialize traces
	// For now, this is a no-op stub
	return nil
}

// DataLength returns the current data length
func (w *walBlock) DataLength() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Approximate size
	size := uint64(0)
	for _, trace := range w.traces {
		size += uint64(trace.Size())
	}
	return size
}

// Iterator returns an iterator over all traces in the WAL
func (w *walBlock) Iterator() (common.Iterator, error) {
	w.mu.Lock()
	traces := make(map[string]*tempopb.Trace, len(w.traces))
	for k, v := range w.traces {
		traces[k] = v
	}
	w.mu.Unlock()

	return &walIterator{
		traces: traces,
		ids:    make([]string, 0, len(traces)),
		idx:    0,
	}, nil
}

// Clear clears the WAL block
func (w *walBlock) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.traces = make(map[string]*tempopb.Trace)
	return nil
}

// FindTraceByID finds a trace by ID
func (w *walBlock) FindTraceByID(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	trace, ok := w.traces[string(id)]
	if !ok {
		return nil, nil
	}

	return &tempopb.TraceByIDResponse{Trace: trace}, nil
}

// Search performs a search (stub - not implemented for WAL)
func (w *walBlock) Search(ctx context.Context, req *tempopb.SearchRequest, opts common.SearchOptions) (*tempopb.SearchResponse, error) {
	return &tempopb.SearchResponse{}, nil
}

// SearchTags implements the Searcher interface
func (w *walBlock) SearchTags(ctx context.Context, scope traceql.AttributeScope, cb common.TagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	return nil
}

// SearchTagValues implements the Searcher interface
func (w *walBlock) SearchTagValues(ctx context.Context, tag string, cb common.TagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	return nil
}

// SearchTagValuesV2 implements the Searcher interface
func (w *walBlock) SearchTagValuesV2(ctx context.Context, tag traceql.Attribute, cb common.TagValuesCallbackV2, mcb common.MetricsCallback, opts common.SearchOptions) error {
	return nil
}

// Fetch implements the Searcher interface
func (w *walBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	return traceql.FetchSpansResponse{}, nil
}

// FetchTagValues implements the Searcher interface
func (w *walBlock) FetchTagValues(ctx context.Context, req traceql.FetchTagValuesRequest, cb traceql.FetchTagValuesCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	return nil
}

// FetchTagNames implements the Searcher interface
func (w *walBlock) FetchTagNames(ctx context.Context, req traceql.FetchTagsRequest, cb traceql.FetchTagsCallback, mcb common.MetricsCallback, opts common.SearchOptions) error {
	return nil
}

// Validate validates the WAL block
func (w *walBlock) Validate(ctx context.Context) error {
	return nil
}

// walIterator iterates over traces in memory
type walIterator struct {
	traces map[string]*tempopb.Trace
	ids    []string
	idx    int
	mu     sync.Mutex
}

// Next returns the next trace
func (i *walIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Initialize IDs on first call
	if len(i.ids) == 0 {
		for id := range i.traces {
			i.ids = append(i.ids, id)
		}
	}

	if i.idx >= len(i.ids) {
		return nil, nil, io.EOF
	}

	traceID := i.ids[i.idx]
	trace := i.traces[traceID]
	i.idx++

	return []byte(traceID), trace, nil
}

// Close closes the iterator
func (i *walIterator) Close() {
	// Nothing to close
}
