package vblockpack

import (
	"bytes"
	"context"
	"encoding/hex" //nolint:depguard
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

type walBlock struct {
	meta           *backend.BlockMeta
	path           string
	ingestionSlack time.Duration

	// Blockpack writer for serialization
	mu             sync.Mutex
	writer         *blockpack.Writer
	buf            *bytes.Buffer
	dirty          bool     // true if AppendTrace called since last sealCurrent()
	flushed        [][]byte // sealed, complete blockpack files (one per sealCurrent() call)
	nextSegmentIdx int      // monotonically increasing; used as on-disk segment file number
}

// createWALBlock creates a new WAL block and ensures its directory exists.
func createWALBlock(meta *backend.BlockMeta, dirPath string, ingestionSlack time.Duration) (*walBlock, error) {
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		return nil, fmt.Errorf("createWALBlock: mkdir: %w", err)
	}
	return &walBlock{
		meta:           meta,
		path:           dirPath,
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

	if err := w.writer.AddTempoTrace(tr); err != nil {
		return fmt.Errorf("failed to add trace to blockpack: %w", err)
	}
	w.dirty = true

	// Update metadata
	w.meta.ObjectAdded(start, end)

	return nil
}

// initWriter creates a fresh blockpack writer, assigning w.buf and w.writer
// only after both succeed so that a failure leaves the block in a consistent state.
func (w *walBlock) initWriter() error {
	// Pre-allocate 32 MiB to reduce buffer doubling during span ingestion.
	buf := bytes.NewBuffer(make([]byte, 0, 32<<20))
	cfg := blockpack.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: 2000,
	}
	// Pass embedder if configured — blockpack handles field assembly + embedding internally.
	if emb := getProcessEmbedder(configuredEmbedURL); emb != nil {
		cfg.Embedder = emb
	}
	writer, err := blockpack.NewWriterWithConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to create blockpack writer: %w", err)
	}
	// Assign only after both succeed — prevents broken state if NewWriterWithConfig fails.
	w.buf = buf
	w.writer = writer
	return nil
}

// sealCurrent finalizes the active writer segment into w.flushed.
// Must be called with w.mu held.
// Returns nil immediately when dirty==false (nothing pending).
// Returns an error when dirty==true but writer==nil (prior initWriter failure — stuck state).
func (w *walBlock) sealCurrent() error {
	if !w.dirty {
		return nil // nothing pending
	}
	if w.writer == nil {
		// dirty==true but writer==nil means a prior initWriter call failed.
		// Surface the error rather than silently skipping.
		return fmt.Errorf("sealCurrent: writer is nil but dirty flag set (prior initWriter failure)")
	}
	if _, err := w.writer.Flush(); err != nil {
		return fmt.Errorf("sealCurrent: flush: %w", err)
	}
	if w.buf.Len() > 0 {
		seg := make([]byte, w.buf.Len())
		copy(seg, w.buf.Bytes())
		w.flushed = append(w.flushed, seg)
	}
	// Clear dirty BEFORE reinit: if reinit fails, the next sealCurrent call will
	// hit the writer==nil guard above and surface an error rather than silently
	// re-entering this flush path with stale data.
	w.dirty = false
	if err := w.initWriter(); err != nil {
		w.writer = nil
		w.buf = nil
		return fmt.Errorf("sealCurrent: reinit writer: %w", err)
	}
	return nil
}

// IngestionSlack returns the ingestion slack duration
func (w *walBlock) IngestionSlack() time.Duration {
	return w.ingestionSlack
}

// Flush writes accumulated traces to disk as numbered blockpack segment files.
func (w *walBlock) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seal any pending dirty data into w.flushed
	if err := w.sealCurrent(); err != nil {
		return fmt.Errorf("Flush: sealCurrent: %w", err)
	}

	if len(w.flushed) == 0 {
		return nil
	}

	var totalSize uint64
	var written []string
	for _, seg := range w.flushed {
		w.nextSegmentIdx++
		filePath := fmt.Sprintf("%s/%010d", w.path, w.nextSegmentIdx)
		if err := os.WriteFile(filePath, seg, 0o644); err != nil {
			// Best-effort cleanup of segments written in this call to avoid partial state.
			for _, p := range written {
				_ = os.Remove(p)
			}
			return fmt.Errorf("Flush: write segment %d: %w", w.nextSegmentIdx, err)
		}
		written = append(written, filePath)
		totalSize += uint64(len(seg))
	}

	w.meta.Size_ = totalSize

	// Release in-memory segments and writer — data is now on disk
	w.flushed = nil
	w.buf = nil
	w.writer = nil

	return nil
}

// DataLength returns the current data length
func (w *walBlock) DataLength() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	var total uint64
	for _, seg := range w.flushed {
		total += uint64(len(seg))
	}
	if w.writer != nil {
		total += uint64(w.writer.CurrentSize())
	}
	return total
}

// Iterator returns an iterator over all traces in the WAL.
func (w *walBlock) Iterator() (common.Iterator, error) {
	w.mu.Lock()

	if w.writer != nil {
		// In-memory path: seal current batch first
		if err := w.sealCurrent(); err != nil {
			w.mu.Unlock()
			return nil, fmt.Errorf("Iterator: sealCurrent: %w", err)
		}
	}

	if len(w.flushed) > 0 {
		// Deep-copy slice header and pointer list so concurrent appends to w.flushed
		// or future buffer pooling cannot alias the iterator's view.
		// Underlying []byte arrays are immutable after sealing (allocated via make+copy in sealCurrent).
		segments := make([][]byte, len(w.flushed))
		copy(segments, w.flushed)
		w.mu.Unlock()
		return newMultiSegmentIterator(segments)
	}
	w.mu.Unlock()

	// Flushed to disk: enumerate numbered segment files
	return newDiskIterator(w.path)
}

// newMultiSegmentIterator builds a blockpackIterator covering all traces from all segments.
func newMultiSegmentIterator(segments [][]byte) (*blockpackIterator, error) {
	byTrace := make(map[string][]blockpack.SpanMatch)
	var traceOrder []string

	for _, seg := range segments {
		r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: seg})
		if err != nil {
			return nil, fmt.Errorf("newMultiSegmentIterator: create reader: %w", err)
		}
		allMatches, _, err := blockpack.QueryTraceQL(r, "{}", blockpack.QueryOptions{})
		if err != nil {
			return nil, fmt.Errorf("newMultiSegmentIterator: query: %w", err)
		}
		for i := range allMatches {
			match := &allMatches[i]
			if _, exists := byTrace[match.TraceID]; !exists {
				traceOrder = append(traceOrder, match.TraceID)
			}
			byTrace[match.TraceID] = append(byTrace[match.TraceID], *match)
		}
	}

	traces := make([]blockpackTrace, 0, len(byTrace))
	for _, traceIDHex := range traceOrder {
		traceIDBytes, err := hex.DecodeString(traceIDHex)
		if err != nil || len(traceIDBytes) != 16 {
			continue
		}
		traces = append(traces, blockpackTrace{
			id:      common.ID(traceIDBytes),
			matches: byTrace[traceIDHex],
		})
	}
	return &blockpackIterator{traces: traces}, nil
}

// newDiskIterator reads all numbered segment files from path and returns an iterator.
func newDiskIterator(path string) (common.Iterator, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &emptyIterator{}, nil
		}
		return nil, fmt.Errorf("newDiskIterator: readdir: %w", err)
	}

	var segments [][]byte
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Only load numbered data files (format: %010d), skip meta.json and others.
		name := entry.Name()
		if len(name) != 10 {
			continue
		}
		if _, err := strconv.ParseUint(name, 10, 64); err != nil {
			continue // not a numbered segment file
		}
		data, err := os.ReadFile(filepath.Join(path, name))
		if err != nil {
			return nil, fmt.Errorf("newDiskIterator: read %s: %w", name, err)
		}
		if len(data) > 0 {
			segments = append(segments, data)
		}
	}

	if len(segments) == 0 {
		return &emptyIterator{}, nil
	}
	return newMultiSegmentIterator(segments)
}

// Clear clears the WAL block
func (w *walBlock) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer = nil
	w.buf = nil
	w.flushed = nil
	w.dirty = false

	return nil
}

// FindTraceByID finds a trace by ID in the WAL block by iterating all sealed segments.
func (w *walBlock) FindTraceByID(ctx context.Context, id common.ID, opts common.SearchOptions) (*tempopb.TraceByIDResponse, error) {
	if len(id) != 16 {
		return nil, fmt.Errorf("trace ID must be 16 bytes, got %d", len(id))
	}

	w.mu.Lock()
	if err := w.sealCurrent(); err != nil {
		w.mu.Unlock()
		return nil, fmt.Errorf("walBlock FindTraceByID: %w", err)
	}
	segments := w.flushed // slice header copy; underlying arrays are immutable after sealing
	w.mu.Unlock()

	if len(segments) == 0 {
		return nil, nil
	}

	traceIDHex := hex.EncodeToString(id)
	var allMatches []blockpack.SpanMatch

	for _, seg := range segments {
		r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: seg})
		if err != nil {
			return nil, fmt.Errorf("walBlock FindTraceByID: create reader: %w", err)
		}
		matches, err := blockpack.GetTraceByID(r, traceIDHex)
		if err != nil {
			return nil, fmt.Errorf("walBlock FindTraceByID: %w", err)
		}
		allMatches = append(allMatches, matches...)
	}

	if len(allMatches) == 0 {
		return nil, nil
	}

	trace, err := reconstructTrace(id, allMatches)
	if err != nil {
		return nil, fmt.Errorf("walBlock FindTraceByID: reconstruct: %w", err)
	}
	return &tempopb.TraceByIDResponse{Trace: trace}, nil
}

// Search returns an empty response. Tag search operations are not served from WAL blocks.
// The query pipeline falls through to backend blocks for all tag/search operations.
// Only FindTraceByID and Fetch are used against the WAL during active ingestion.
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

// Fetch implements the Searcher interface by querying the WAL block's accumulated data.
// It iterates all sealed segments and collects matches from each.
func (w *walBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	emptyResp := traceql.FetchSpansResponse{
		Results: &sliceSpansetIterator{},
		Bytes:   func() uint64 { return 0 },
	}

	w.mu.Lock()
	if err := w.sealCurrent(); err != nil {
		w.mu.Unlock()
		return traceql.FetchSpansResponse{}, fmt.Errorf("walBlock Fetch: %w", err)
	}
	segments := w.flushed // slice header copy; underlying arrays are immutable after sealing
	w.mu.Unlock()

	if len(segments) == 0 {
		return emptyResp, nil
	}

	query := conditionsToTraceQL(req.Conditions, req.AllConditions)
	if orig, ok := common.OriginalTraceQLQuery(ctx); ok {
		query = orig
	}

	// Collect matches from all segments
	var allMatches []blockpack.SpanMatch
	for _, seg := range segments {
		r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: seg})
		if err != nil {
			return traceql.FetchSpansResponse{}, fmt.Errorf("walBlock Fetch: create reader: %w", err)
		}
		matches, _, fetchErr := blockpack.QueryTraceQL(r, query, blockpack.QueryOptions{
			Limit:     opts.MaxTraces,
			StartNano: req.StartTimeUnixNanos,
			EndNano:   req.EndTimeUnixNanos,
			Embedder:  getProcessEmbedder(configuredEmbedURL),
		})
		if fetchErr != nil {
			return traceql.FetchSpansResponse{}, fmt.Errorf("walBlock Fetch: query: %w", fetchErr)
		}
		allMatches = append(allMatches, matches...)
	}

	// Build requestedAttrs map from query conditions to filter AllAttributesFunc output.
	requestedAttrs := make(map[traceql.Attribute]struct{}, len(req.Conditions))
	for _, cond := range req.Conditions {
		requestedAttrs[cond.Attribute] = struct{}{}
	}

	// Convert matches to spansets grouped by trace (mirrors backend_block.Fetch).
	type traceEntry struct {
		traceID  []byte
		spans    []traceql.Span
		rawSpans []blockpack.SpanMatch
	}
	traceMap := make(map[string]*traceEntry)
	var traceOrder []string
	for i := range allMatches {
		m := &allMatches[i]
		if _, exists := traceMap[m.TraceID]; !exists {
			traceIDBytes, decErr := hex.DecodeString(m.TraceID)
			if decErr != nil {
				continue
			}
			traceMap[m.TraceID] = &traceEntry{traceID: traceIDBytes}
			traceOrder = append(traceOrder, m.TraceID)
		}
		traceMap[m.TraceID].spans = append(traceMap[m.TraceID].spans, &blockpackSpan{match: *m, requestedAttrs: requestedAttrs})
		traceMap[m.TraceID].rawSpans = append(traceMap[m.TraceID].rawSpans, *m)
	}

	spansets := make([]*traceql.Spanset, 0, len(traceOrder))
	for _, tid := range traceOrder {
		entry := traceMap[tid]
		rootSpanName, rootServiceName, startNanos, durationNanos := blockpack.SpanMatchesMetadata(entry.rawSpans)
		// Cap returned spans at DefaultSpansPerSpanSet (3) to match standard Tempo
		// search behaviour. AttributeMatched reflects the total before the cap.
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
		ss.AddAttribute(traceql.AttributeMatched, traceql.NewStaticInt(totalSpans))
		spansets = append(spansets, ss)
	}

	totalBytes := uint64(0)
	for _, seg := range segments {
		totalBytes += uint64(len(seg))
	}

	return traceql.FetchSpansResponse{
		Results: &sliceSpansetIterator{spansets: spansets},
		Bytes:   func() uint64 { return totalBytes },
	}, nil
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

	allMatches, _, err := blockpack.QueryTraceQL(reader, "{}", blockpack.QueryOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to query spans: %w", err)
	}
	for i := range allMatches {
		match := &allMatches[i]
		if _, exists := byTrace[match.TraceID]; !exists {
			traceOrder = append(traceOrder, match.TraceID)
		}
		byTrace[match.TraceID] = append(byTrace[match.TraceID], *match)
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
