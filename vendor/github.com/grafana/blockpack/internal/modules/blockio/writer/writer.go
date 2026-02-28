package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"io"
	"sync/atomic"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Writer encodes OTLP spans into the blockpack format.
// NOT thread-safe — concurrent calls will panic (NOTES §9).
type Writer struct {
	enc *zstdEncoder

	// trace_id → list of {blockID, rowIdx} across all blocks
	traceIndex map[[16]byte][]traceBlockRef

	// UUID column detection: column name → detected as UUID
	uuidColumns map[string]bool

	// Range index built incrementally in buildAndWriteBlock, consumed at Flush.
	// Replaces the old flat log + O(n log n) sort approach.
	rangeIdx rangeIndex

	out countingWriter

	// pending holds lightweight pendingSpan records awaiting the next flushBlocks call.
	// Each pendingSpan stores sort keys and proto pointers only; full OTLP→column
	// decoding is deferred to addRowFromProto at flush time.
	pending    []pendingSpan
	blockMetas []shared.BlockMeta

	// pending holds lightweight sort records (~88 bytes each: sort keys + proto pointers).
	// protoRoots holds the TracesData proto roots that keep the sub-objects (rs/ss/span)
	// referenced by pending entries alive in the GC. Without protoRoots, the GC could
	// collect the proto tree while pending still holds pointers into it.
	// After flushBlocks() clears pending, protoRoots is also cleared to release memory.

	// protoRoots anchors TracesData protos (and synthetic ResourceSpans for AddSpan)
	// until flushBlocks() processes all pending spans referencing them.
	// After flushBlocks() clears w.pending, protoRoots is also cleared:
	// - w.pending entries held the rs/ss/span pointers that kept sub-objects alive
	// - w.rangeIdx map keys keep necessary string data alive independently (GC traces map keys)
	// This clearing is the key RSS fix: without it, protoRoots accumulates for the entire
	// WAL block lifetime, causing 3× RSS vs. parquet.
	protoRoots []*tracev1.TracesData

	cfg Config

	// inUse is a concurrency guard: AddSpan, AddTracesData, and Flush each do
	// CompareAndSwap(false, true) on entry and panic if the swap fails, detecting
	// concurrent callers. The Writer is documented as NOT thread-safe (NOTES §9).
	inUse atomic.Bool
}

// countingWriter wraps io.Writer and tracks total bytes written.
// This is needed because io.Writer has no built-in byte count; Flush() returns
// the total bytes written as its first return value.
type countingWriter struct {
	w     io.Writer
	total int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.total += int64(n)
	return n, err
}

// NewWriterWithConfig validates the config and returns a new Writer.
// Returns error if OutputStream is nil or MaxBlockSpans > 65535.
func NewWriterWithConfig(cfg Config) (*Writer, error) {
	if cfg.OutputStream == nil {
		return nil, fmt.Errorf("writer: OutputStream is required")
	}
	if cfg.MaxBlockSpans > 65535 {
		return nil, fmt.Errorf("writer: MaxBlockSpans %d exceeds maximum 65535", cfg.MaxBlockSpans)
	}
	if cfg.MaxBlockSpans == 0 {
		cfg.MaxBlockSpans = defaultMaxBlockSpans
	}
	// Default auto-flush at 5× block size. Caps live proto memory to one batch of
	// 5 blocks while preserving enough lookahead for MinHash sort quality.
	if cfg.MaxBufferedSpans == 0 {
		cfg.MaxBufferedSpans = 5 * cfg.MaxBlockSpans
	}
	enc, err := newZstdEncoder()
	if err != nil {
		return nil, fmt.Errorf("writer: zstd init: %w", err)
	}
	return &Writer{
		cfg:         cfg,
		out:         countingWriter{w: cfg.OutputStream},
		enc:         enc,
		traceIndex:  make(map[[16]byte][]traceBlockRef),
		uuidColumns: make(map[string]bool),
		rangeIdx:    make(rangeIndex),
		// Pre-allocate pending to MaxBufferedSpans to avoid growslice on the hot path.
		// After each flushBlocks(), w.pending is reset to length 0 (capacity retained).
		pending: make([]pendingSpan, 0, cfg.MaxBufferedSpans),
	}, nil
}

// AddSpan buffers a single OTLP span.
// resourceSchemaURL and scopeSchemaURL are the SchemaUrl fields from
// ResourceSpans and ScopeSpans respectively; pass "" when not present.
// Panics if called concurrently (NOTES §9).
func (w *Writer) AddSpan(
	traceID []byte,
	span *tracev1.Span,
	resourceAttrs map[string]any,
	resourceSchemaURL string,
	scopeAttrs map[string]any,
	scopeSchemaURL string,
) error {
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	// Synthesize proto containers for the attribute maps so addRowFromProto can
	// read them uniformly. These synthetic protos are anchored in protoRoots until
	// the next flushBlocks() processes this span.
	rs, ss := synthesizeResourceSpans(span, resourceAttrs, resourceSchemaURL, scopeAttrs, scopeSchemaURL)
	td := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{rs},
	}
	w.protoRoots = append(w.protoRoots, td)

	var tid [16]byte
	if len(traceID) == 16 {
		copy(tid[:], traceID)
	}

	ps := pendingSpan{
		traceID: tid,
		svcName: extractSvcNameFromMap(resourceAttrs),
		rs:      rs,
		ss:      ss,
		span:    span,
	}
	computeMinHashSigFromProto(&ps)
	w.pending = append(w.pending, ps)

	// Auto-flush when buffer reaches MaxBufferedSpans.
	if w.cfg.MaxBufferedSpans > 0 && len(w.pending) >= w.cfg.MaxBufferedSpans {
		if err := w.flushBlocks(); err != nil {
			return fmt.Errorf("writer: auto-flush: %w", err)
		}
	}

	return nil
}

// AddTracesData buffers all spans from a TracesData message.
// Use this when you already have a TracesData proto (e.g. from an OTLP pipeline) —
// it avoids synthesizing a wrapper proto. Use AddSpan when building spans
// individually or from non-proto sources (maps, structs).
func (w *Writer) AddTracesData(td *tracev1.TracesData) error {
	if td == nil {
		return nil
	}
	// Anchor the proto until flushBlocks() processes all pending spans.
	// After flushBlocks() clears w.pending, protoRoots is also cleared —
	// but w.rangeIdx map keys keep string data alive independently (GC traces them).
	w.protoRoots = append(w.protoRoots, td)
	for _, rs := range td.ResourceSpans {
		if rs == nil {
			continue
		}
		svcName := extractSvcNameFromProto(rs.Resource)
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				continue
			}
			for _, span := range ss.Spans {
				if span == nil {
					continue
				}
				if !w.inUse.CompareAndSwap(false, true) {
					panic("writer: concurrent use detected")
				}

				var tid [16]byte
				if len(span.TraceId) == 16 {
					copy(tid[:], span.TraceId)
				}

				ps := pendingSpan{
					traceID: tid,
					svcName: svcName,
					rs:      rs,
					ss:      ss,
					span:    span,
				}
				computeMinHashSigFromProto(&ps)
				w.pending = append(w.pending, ps)

				// Auto-flush when buffer reaches MaxBufferedSpans.
				if w.cfg.MaxBufferedSpans > 0 && len(w.pending) >= w.cfg.MaxBufferedSpans {
					if flushErr := w.flushBlocks(); flushErr != nil {
						w.inUse.Store(false)
						return fmt.Errorf("writer: auto-flush: %w", flushErr)
					}
				}
				w.inUse.Store(false)
			}
		}
	}
	return nil
}

// Flush sorts spans, encodes blocks, writes all structures, and returns bytes written.
// Panics if called concurrently.
func (w *Writer) Flush() (int64, error) {
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	if len(w.pending) == 0 && len(w.blockMetas) == 0 {
		// Nothing has ever been written — produce a valid empty file.
		return w.writeEmptyFile()
	}

	// 1. Flush any remaining buffered spans into blocks.
	if err := w.flushBlocks(); err != nil {
		return w.out.total, err
	}

	// 2. Record metadata offset.
	metadataOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets

	// 3. Apply KLL bucket boundaries to the range index.
	// KLL sketches were built incrementally in buildAndWriteBlock (one Add per
	// block min and max), so no re-scan is needed here.
	applyRangeBuckets(w.rangeIdx, defaultRangeBuckets)

	// 4. Write metadata section.
	metaBytes, err := buildMetadataSectionBytes(
		shared.VersionV11,
		w.blockMetas,
		w.rangeIdx,
		w.traceIndex,
	)
	if err != nil {
		return w.out.total, fmt.Errorf("writer: build metadata: %w", err)
	}

	metadataLen := uint64(len(metaBytes))
	if _, err = w.out.Write(metaBytes); err != nil {
		return w.out.total, fmt.Errorf("writer: write metadata: %w", err)
	}

	// 5. Write file header.
	headerOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets
	if err = writeFileHeader(&w.out, shared.VersionV11, metadataOffset, metadataLen); err != nil {
		return w.out.total, fmt.Errorf("writer: write file header: %w", err)
	}

	// 6. Write compact trace index.
	compactOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets
	compactN, err := writeCompactTraceIndex(&w.out, w.blockMetas, w.traceIndex)
	if err != nil {
		return w.out.total, fmt.Errorf("writer: write compact index: %w", err)
	}

	// 7. Write footer.
	if err = writeFooter(&w.out, headerOffset, compactOffset, uint32(compactN)); err != nil { //nolint:gosec // safe: compact index size bounded by MaxCompactSectionSize (50MB) fits uint32
		return w.out.total, fmt.Errorf("writer: write footer: %w", err)
	}

	total := w.out.total

	// 8. Reset ALL state.
	w.pending = w.pending[:0]
	w.blockMetas = nil
	for k := range w.rangeIdx {
		delete(w.rangeIdx, k)
	}
	for k := range w.traceIndex {
		delete(w.traceIndex, k)
	}
	// protoRoots is already cleared by flushBlocks(); this is a defensive no-op.
	clear(w.protoRoots)
	w.protoRoots = w.protoRoots[:0]

	return total, nil
}

// writeEmptyFile writes a minimal valid blockpack file with zero blocks.
func (w *Writer) writeEmptyFile() (int64, error) {
	metadataOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets

	rIdx := make(rangeIndex)
	metaBytes, err := buildMetadataSectionBytes(
		shared.VersionV11,
		nil,
		rIdx,
		w.traceIndex,
	)
	if err != nil {
		return w.out.total, err
	}

	metadataLen := uint64(len(metaBytes))
	if _, err = w.out.Write(metaBytes); err != nil {
		return w.out.total, err
	}

	headerOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets
	if err = writeFileHeader(&w.out, shared.VersionV11, metadataOffset, metadataLen); err != nil {
		return w.out.total, err
	}

	compactOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets
	compactN, err := writeCompactTraceIndex(&w.out, nil, w.traceIndex)
	if err != nil {
		return w.out.total, err
	}

	if err = writeFooter(&w.out, headerOffset, compactOffset, uint32(compactN)); err != nil { //nolint:gosec // safe: compact index size bounded by MaxCompactSectionSize (50MB) fits uint32
		return w.out.total, err
	}

	return w.out.total, nil
}

// buildAndWriteBlock builds one block from pending[0:end], writes it to the output, and
// records BlockMeta, range index entries, and traceIndex refs.
func (w *Writer) buildAndWriteBlock(blockID int, pending []pendingSpan) error {
	blockOffset := uint64(w.out.total) //nolint:gosec // safe: total bytes written fits in int64 and int64 fits in uint64 for file offsets

	built, err := buildBlock(pending, w.enc)
	if err != nil {
		return fmt.Errorf("writer: block %d finalize: %w", blockID, err)
	}

	if _, err = w.out.Write(built.payload); err != nil {
		return fmt.Errorf("writer: block %d write: %w", blockID, err)
	}

	// Record BlockMeta.
	meta := shared.BlockMeta{
		Offset:          blockOffset,
		Length:          uint64(len(built.payload)),
		Kind:            shared.BlockKindLeaf,
		SpanCount:       uint32(built.spanCount), //nolint:gosec // safe: spanCount bounded by MaxBlockSpans (65535)
		MinStart:        built.minStart,
		MaxStart:        built.maxStart,
		MinTraceID:      built.minTraceID,
		MaxTraceID:      built.maxTraceID,
		ColumnNameBloom: built.bloom,
	}
	w.blockMetas = append(w.blockMetas, meta)

	// Update range index: one entry per column per block (O(columns_per_block)).
	// Feed min and max into the column's KLL sketch and record the blockRange.
	bid := uint32(blockID) //nolint:gosec // safe: blockID bounded by MaxBlocks (100_000), fits uint32
	for _, mm := range built.colMinMax {
		cd, ok := w.rangeIdx[mm.colName]
		if !ok {
			cd = newRangeColumnData(mm.colType)
			w.rangeIdx[mm.colName] = cd
		}
		addBlockRangeToColumn(cd, mm, bid)
	}

	// Update the file-level trace index using built.traceRows, which is already
	// populated by addRowFromProto. This avoids the redundant blockTraceRows map that
	// previously duplicated the same data.
	for tid, rowIdxs := range built.traceRows {
		w.traceIndex[tid] = append(w.traceIndex[tid], traceBlockRef{
			blockID:     uint16(blockID), //nolint:gosec // safe: blockID bounded by MaxBlocks (100_000) fits uint16 per block limit
			spanIndices: rowIdxs,
		})
	}

	return nil
}

// flushBlocks sorts w.pending, writes all pending blocks to the output, and resets w.pending to empty.
// Appends to w.blockMetas/traceIndex and updates w.rangeIdx incrementally.
//
// Called automatically by AddSpan/AddTracesData when len(w.pending) >= cfg.MaxBufferedSpans,
// and called by Flush() to process any remaining buffered spans before writing metadata.
//
// INVARIANT (NOTES §17): Does NOT write metadata/header/footer. That happens only in Flush().
// INVARIANT: rangeIdx is updated here but KLL/range-buckets are applied at Flush().
// INVARIANT: Block IDs are globally sequential: blockID := len(w.blockMetas) before each block.
//
// RSS bound: After processing all blocks, protoRoots is cleared. The w.rangeIdx map keys
// independently keep string backing bytes alive (GC traces map key pointers), so clearing
// protoRoots is safe and releases the batch's proto memory back to the GC.
func (w *Writer) flushBlocks() error {
	if len(w.pending) == 0 {
		return nil
	}

	sortPending(w.pending)

	blockStart := 0
	for blockStart < len(w.pending) {
		blockEnd := min(blockStart+w.cfg.MaxBlockSpans, len(w.pending))

		blockID := len(w.blockMetas) // globally sequential: blockMetas accumulates across calls
		if err := w.buildAndWriteBlock(blockID, w.pending[blockStart:blockEnd]); err != nil {
			// Partial blocks may have been written; those are unrecoverable.
			// Reset buffered state so the Writer is not stuck in a partial state.
			clear(w.pending)
			w.pending = w.pending[:0]
			clear(w.protoRoots)
			w.protoRoots = w.protoRoots[:0]
			return err
		}
		blockStart = blockEnd
	}

	// Clear pending span buffer. blockMetas, traceIndex, rangeIdx
	// accumulate across flushBlocks() calls and are consumed once at Flush().
	clear(w.pending)
	w.pending = w.pending[:0]

	// Release proto anchors for this batch. The rangeIdx map keys independently
	// keep necessary string data alive (GC traces string pointers in map keys).
	// This is the key RSS fix: prevents protoRoots from accumulating for the entire
	// WAL block lifetime (which caused 3× RSS vs. parquet in prior versions).
	clear(w.protoRoots)
	w.protoRoots = w.protoRoots[:0]

	return nil
}

// buildMetadataSectionBytes serializes the metadata section to a byte slice.
// Sections are concatenated directly with no length prefixes; the reader parses
// them sequentially using the counts embedded in each section header.
// Column index section (§5.3) is written as block_count × uint32(0) for format
// compatibility with existing readers; the per-column entry data is no longer written.
func buildMetadataSectionBytes(
	version uint8,
	blockMetas []shared.BlockMeta,
	rIdx rangeIndex,
	traceIndex map[[16]byte][]traceBlockRef,
) ([]byte, error) {
	blockIdxData, err := writeBlockIndexSection(nil, version, blockMetas)
	if err != nil {
		return nil, err
	}

	dedIdxData, err := writeRangeIndexSection(nil, rIdx)
	if err != nil {
		return nil, err
	}

	traceIdxData, err := writeTraceBlockIndexSection(nil, traceIndex)
	if err != nil {
		return nil, err
	}

	// Column index (§5.3): write block_count × col_count=0 for format compatibility.
	// Per-column entry data is no longer written (dead data removed 2026-02-27).
	colIdxSize := len(blockMetas) * 4
	totalSize := len(blockIdxData) + len(dedIdxData) + colIdxSize + len(traceIdxData)
	buf := make([]byte, 0, totalSize)
	buf = append(buf, blockIdxData...)
	buf = append(buf, dedIdxData...)
	for range len(blockMetas) {
		buf = append(buf, 0, 0, 0, 0) // col_count[4 LE] = 0
	}
	buf = append(buf, traceIdxData...)

	return buf, nil
}

// CurrentSize returns estimated buffered size in bytes.
func (w *Writer) CurrentSize() int64 {
	return int64(len(w.pending)) * estimatedBytesPerSpan
}
