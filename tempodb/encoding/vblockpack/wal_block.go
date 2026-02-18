package vblockpack

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/mattdurham/blockpack"
)

type walBlock struct {
	meta           *backend.BlockMeta
	path           string
	ingestionSlack time.Duration

	// Blockpack writer for serialization
	mu     sync.Mutex
	writer *blockpack.Writer
	file   *os.File
}

// createWALBlock creates a new WAL block
func createWALBlock(meta *backend.BlockMeta, filepath string, ingestionSlack time.Duration) (*walBlock, error) {
	return &walBlock{
		meta:           meta,
		path:           filepath,
		ingestionSlack: ingestionSlack,
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
// Writes trace spans to blockpack format
func (w *walBlock) AppendTrace(id common.ID, tr *tempopb.Trace, start, end uint32, adjustIngestionSlack bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Lazy initialize blockpack writer on first append
	if w.writer == nil {
		if err := w.initWriter(); err != nil {
			return fmt.Errorf("failed to initialize writer: %w", err)
		}
	}

	// Convert tempopb.Trace to OTLP TracesData format for blockpack
	td := tempoTraceToOTLP(tr)

	// Write to blockpack
	if err := w.writer.AddTracesData(td); err != nil {
		return fmt.Errorf("failed to add trace to blockpack: %w", err)
	}

	// Update metadata
	w.meta.ObjectAdded(start, end)

	return nil
}

// initWriter creates the blockpack writer
func (w *walBlock) initWriter() error {
	// Ensure directory exists
	if err := os.MkdirAll(w.path, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create blockpack writer
	// Use reasonable defaults: 2000 spans per block
	writer, err := blockpack.NewWriter(2000)
	if err != nil {
		return fmt.Errorf("failed to create blockpack writer: %w", err)
	}
	w.writer = writer

	return nil
}

// IngestionSlack returns the ingestion slack duration
func (w *walBlock) IngestionSlack() time.Duration {
	return w.ingestionSlack
}

// Flush writes accumulated traces to disk as blockpack format
func (w *walBlock) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Nothing to flush if no writer
	if w.writer == nil {
		return nil
	}

	// Serialize blockpack to bytes
	data, err := w.writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush blockpack writer: %w", err)
	}

	// Write to disk
	filepath := w.path + "/" + DataFileName
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("failed to write blockpack file: %w", err)
	}

	// Update metadata with actual size
	w.meta.Size_ = uint64(len(data))

	return nil
}

// DataLength returns the current data length
func (w *walBlock) DataLength() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer == nil {
		return 0
	}

	return uint64(w.writer.CurrentSize())
}

// Iterator returns an iterator over all traces in the WAL
func (w *walBlock) Iterator() (common.Iterator, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush to get final data
	if w.writer == nil {
		return &emptyIterator{}, nil
	}

	data, err := w.writer.Flush()
	if err != nil {
		return nil, fmt.Errorf("failed to flush for iterator: %w", err)
	}

	// Open as reader to iterate
	provider := &bytesReaderProvider{data: data}
	reader, err := blockpack.NewReaderFromProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return &blockpackIterator{
		reader: reader,
		// TODO: Initialize iterator state for block traversal
	}, nil
}

// Clear clears the WAL block
func (w *walBlock) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close and reset writer
	w.writer = nil
	w.file = nil

	return nil
}

// FindTraceByID finds a trace by ID
func (w *walBlock) FindTraceByID(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: Query blockpack writer's in-memory data for trace
	// For now, return not found
	return nil, nil
}

// Search performs a search (not implemented for WAL - WAL is write-only during ingestion)
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

// blockpackIterator iterates through blockpack data
type blockpackIterator struct {
	reader *blockpack.Reader
	// TODO: Add state for iterating through blocks and spans
	// Will need to reconstruct traces from spans during iteration
}

// Next returns the next trace
func (i *blockpackIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	// TODO: Iterate through blockpack blocks and spans
	// Reconstruct traces from spans
	// Group spans by trace ID
	return nil, nil, io.EOF
}

// Close closes the iterator
func (i *blockpackIterator) Close() {
	// Nothing to close for in-memory reader
}

// emptyIterator is used when there's no data
type emptyIterator struct{}

func (i *emptyIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	return nil, nil, io.EOF
}

func (i *emptyIterator) Close() {}
