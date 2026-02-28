package vblockpack

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/blockpack"
)

type walBlock struct {
	meta           *backend.BlockMeta
	path           string
	ingestionSlack time.Duration

	// Blockpack writer for serialization
	mu     sync.Mutex
	writer *blockpack.Writer
	buf    *bytes.Buffer
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

	// Pre-allocate 32 MiB to reduce buffer doubling during span ingestion.
	w.buf = bytes.NewBuffer(make([]byte, 0, 32<<20))
	writer, err := blockpack.NewWriter(w.buf, 2000)
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

	// Flush blockpack writer — data is written to w.buf
	if _, err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush blockpack writer: %w", err)
	}
	data := w.buf.Bytes()

	// Write to disk
	filePath := w.path + "/" + DataFileName
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write blockpack file: %w", err)
	}

	// Update metadata with actual size
	w.meta.Size_ = uint64(len(data))

	// Release the in-memory buffer and writer — data is now on disk.
	// Setting writer = nil means a subsequent Flush() call returns early
	// at the w.writer == nil guard instead of panicking on w.buf.Bytes().
	w.buf = nil
	w.writer = nil

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

	if w.writer == nil {
		return &emptyIterator{}, nil
	}

	var data []byte

	if w.buf != nil {
		// Buffer still live — flush and read from it directly.
		if _, err := w.writer.Flush(); err != nil {
			return nil, fmt.Errorf("failed to flush for iterator: %w", err)
		}
		data = w.buf.Bytes()
	} else {
		// Buffer was released by Flush() — read the file from disk.
		var err error
		data, err = os.ReadFile(w.path + "/" + DataFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to read blockpack file for iterator: %w", err)
		}
	}

	reader, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data})
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}

	return newBlockpackIterator(reader)
}

// Clear clears the WAL block
func (w *walBlock) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close and reset writer
	w.writer = nil
	w.buf = nil
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

// blockpackTrace holds pre-collected spans for a single trace.
type blockpackTrace struct {
	id      common.ID
	matches []blockpack.SpanMatch
}

// blockpackIterator iterates through blockpack data, yielding one trace per Next call.
type blockpackIterator struct {
	traces []blockpackTrace
	idx    int
}

// newBlockpackIterator eagerly loads all spans from reader, groups by trace ID,
// and returns an iterator ready for sequential consumption.
func newBlockpackIterator(reader *blockpack.Reader) (*blockpackIterator, error) {
	byTrace := make(map[string][]blockpack.SpanMatch)
	var traceOrder []string

	err := blockpack.StreamTraceQL(reader, "{}", blockpack.QueryOptions{}, func(match *blockpack.SpanMatch) bool {
		if _, exists := byTrace[match.TraceID]; !exists {
			traceOrder = append(traceOrder, match.TraceID)
		}
		byTrace[match.TraceID] = append(byTrace[match.TraceID], match.Clone())
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to stream spans: %w", err)
	}

	traces := make([]blockpackTrace, 0, len(byTrace))
	for _, traceIDHex := range traceOrder {
		traceIDBytes, decErr := hex.DecodeString(traceIDHex)
		if decErr != nil || len(traceIDBytes) != 16 {
			continue
		}
		traces = append(traces, blockpackTrace{
			id:      common.ID(traceIDBytes),
			matches: byTrace[traceIDHex],
		})
	}

	return &blockpackIterator{traces: traces}, nil
}

// Next returns the next trace from the iterator.
func (i *blockpackIterator) Next(_ context.Context) (common.ID, *tempopb.Trace, error) {
	if i.idx >= len(i.traces) {
		return nil, nil, io.EOF
	}

	t := i.traces[i.idx]
	i.idx++

	trace, err := reconstructTrace(t.id, t.matches)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to reconstruct trace: %w", err)
	}

	return t.id, trace, nil
}

// Close releases iterator resources.
func (i *blockpackIterator) Close() {}

// emptyIterator is used when there's no data
type emptyIterator struct{}

func (i *emptyIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	return nil, nil, io.EOF
}

func (i *emptyIterator) Close() {}
