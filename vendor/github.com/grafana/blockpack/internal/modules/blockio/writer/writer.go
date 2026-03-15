package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/golang/snappy"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Writer encodes OTLP spans into the blockpack format.
// NOT thread-safe — concurrent calls will panic (NOTES §9).
type Writer struct {
	enc *zstdEncoder

	// bb is the reusable block builder. Lazily created on first buildAndWriteBlock,
	// then reset and reused for subsequent blocks to eliminate per-block allocations.
	bb *blockBuilder

	// trace_id → list of block IDs (uint16) across all blocks
	traceIndex map[[16]byte][]uint16

	// UUID column detection: column name → detected as UUID
	uuidColumns map[string]bool

	// Range index built incrementally in buildAndWriteBlock, consumed at Flush.
	// Replaces the old flat log + O(n log n) sort approach.
	rangeIdx rangeIndex

	// sketchIdx accumulates per-block sketch sets across all blocks.
	// Indexed parallel to blockMetas: sketchIdx[i] is the sketch for block i.
	// Consumed at Flush() by buildMetadataSectionBytes.
	sketchIdx []blockSketchSet

	// intrinsicAccum accumulates file-level columnar data for intrinsic columns.
	// Fed row-by-row during block building via blockBuilder.intrinsicAccum.
	// Consumed at Flush() by writeIntrinsicSection to produce the footer V4 extension.
	intrinsicAccum *intrinsicAccumulator

	out countingWriter

	cfg Config

	// pending holds lightweight pendingSpan records awaiting the next flushBlocks call.
	// Each pendingSpan stores sort keys and proto pointers only; full OTLP→column
	// decoding is deferred to addRowFromProto at flush time.
	pending    []pendingSpan
	blockMetas []shared.BlockMeta

	// protoRoots anchors TracesData protos (and synthetic ResourceSpans for AddSpan)
	// until flushBlocks() processes all pending spans referencing them.
	// After flushBlocks() clears w.pending, protoRoots is also cleared:
	// - w.pending entries held the rs/ss/span pointers that kept sub-objects alive
	// - w.rangeIdx map keys keep necessary string data alive independently (GC traces map keys)
	// This clearing is the key RSS fix: without it, protoRoots accumulates for the entire
	// WAL block lifetime, causing 3× RSS vs. parquet.
	protoRoots []*tracev1.TracesData

	// pendingLogs holds lightweight log records awaiting the next flushLogBlocks call.
	// Parallel to w.pending (trace path). Protected by the same inUse guard.
	pendingLogs []pendingLogRecord

	// logProtoRoots anchors LogsData protos until flushLogBlocks() processes all pending
	// log records referencing them. Cleared after flushLogBlocks(). Mirrors protoRoots.
	logProtoRoots []*logsv1.LogsData

	// inUse is a concurrency guard: AddSpan, AddTracesData, and Flush each do
	// CompareAndSwap(false, true) on entry and panic if the swap fails, detecting
	// concurrent callers. The Writer is documented as NOT thread-safe (NOTES §9).
	inUse atomic.Bool

	// signalType identifies whether this Writer produces trace or log files.
	// Set implicitly on the first AddLogsData or AddTracesData/AddSpan call.
	// 0 means unset; shared.SignalTypeTrace = 0x01; shared.SignalTypeLog = 0x02.
	signalType uint8
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
		cfg:            cfg,
		out:            countingWriter{w: cfg.OutputStream},
		enc:            enc,
		traceIndex:     make(map[[16]byte][]uint16),
		uuidColumns:    make(map[string]bool),
		rangeIdx:       make(rangeIndex),
		intrinsicAccum: newIntrinsicAccumulator(),
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

	if w.signalType == shared.SignalTypeLog {
		return fmt.Errorf("writer: cannot mix trace and log records in the same Writer")
	}
	w.signalType = shared.SignalTypeTrace

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
// Panics if called concurrently (NOTES §9).
func (w *Writer) AddTracesData(td *tracev1.TracesData) error {
	if td == nil {
		return nil
	}
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	if w.signalType == shared.SignalTypeLog {
		return fmt.Errorf("writer: cannot mix trace and log records in the same Writer")
	}
	w.signalType = shared.SignalTypeTrace
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
						return fmt.Errorf("writer: auto-flush: %w", flushErr)
					}
				}
			}
		}
	}
	return nil
}

// AddLogsData buffers all log records from an OTLP LogsData message.
// The Writer must not have had AddTracesData or AddSpan called on it (signal types cannot
// be mixed in a single Writer). Returns an error if signalType is already SignalTypeTrace
// and there are buffered trace records.
func (w *Writer) AddLogsData(ld *logsv1.LogsData) error {
	if ld == nil {
		return nil
	}
	// Guard against mixing signal types.
	if w.signalType == shared.SignalTypeTrace {
		return fmt.Errorf("writer: cannot mix trace and log records in the same Writer")
	}
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)
	w.signalType = shared.SignalTypeLog
	w.logProtoRoots = append(w.logProtoRoots, ld)

	for _, rl := range ld.ResourceLogs {
		if rl == nil {
			continue
		}
		svcName := extractSvcNameFromProto(rl.Resource)
		for _, sl := range rl.ScopeLogs {
			if sl == nil {
				continue
			}
			for _, record := range sl.LogRecords {
				if record == nil {
					continue
				}

				plr := pendingLogRecord{
					svcName:   svcName,
					rl:        rl,
					sl:        sl,
					record:    record,
					timestamp: record.TimeUnixNano,
				}
				computeMinHashSigFromLog(&plr)
				w.pendingLogs = append(w.pendingLogs, plr)

				if w.cfg.MaxBufferedSpans > 0 && len(w.pendingLogs) >= w.cfg.MaxBufferedSpans {
					if flushErr := w.flushLogBlocks(); flushErr != nil {
						return fmt.Errorf("writer: auto-flush: %w", flushErr)
					}
				}
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

	if len(w.pending) == 0 && len(w.pendingLogs) == 0 && len(w.blockMetas) == 0 {
		// Nothing has ever been written — produce a valid empty file.
		return w.writeEmptyFile()
	}

	// 1. Flush any remaining buffered spans/records into blocks.
	if w.signalType == shared.SignalTypeLog {
		if err := w.flushLogBlocks(); err != nil {
			return w.out.total, err
		}
	} else {
		if err := w.flushBlocks(); err != nil {
			return w.out.total, err
		}
	}

	// 2. Record metadata offset.
	metadataOffset := uint64(w.out.total) //nolint:gosec

	// 3. Apply KLL bucket boundaries to the range index.
	// KLL sketches were built incrementally in buildAndWriteBlock (one Add per
	// block min and max), so no re-scan is needed here.
	applyRangeBuckets(w.rangeIdx, defaultRangeBuckets)

	// 4. Write metadata section (snappy-compressed, V13 format).
	// V13 removes MinTraceID/MaxTraceID from block index entries and
	// stats_offset/stats_len from column metadata. V12 added snappy-compressed
	// metadata and the signal_type byte in the file header.
	metaBytes, err := buildMetadataSectionBytes(
		shared.VersionV13, // V13: block index omits MinTraceID[16]+MaxTraceID[16] per entry
		w.blockMetas,
		w.rangeIdx,
		w.traceIndex,
		w.sketchIdx,
	)
	if err != nil {
		return w.out.total, fmt.Errorf("writer: build metadata: %w", err)
	}

	compressedMeta := snappy.Encode(nil, metaBytes)
	metadataLen := uint64(len(compressedMeta))
	if _, err = w.out.Write(compressedMeta); err != nil {
		return w.out.total, fmt.Errorf("writer: write metadata: %w", err)
	}

	// 5. Write file header (V13).
	signalTypeForHeader := w.signalType
	if signalTypeForHeader == 0 {
		signalTypeForHeader = shared.SignalTypeTrace
	}
	headerOffset := uint64(w.out.total) //nolint:gosec
	if err = writeFileHeader(&w.out, shared.VersionV13, metadataOffset, metadataLen, signalTypeForHeader); err != nil {
		return w.out.total, fmt.Errorf("writer: write file header: %w", err)
	}

	// 6. Write compact trace index.
	compactOffset := uint64(w.out.total) //nolint:gosec
	compactN, err := writeCompactTraceIndex(&w.out, w.blockMetas, w.traceIndex)
	if err != nil {
		return w.out.total, fmt.Errorf("writer: write compact index: %w", err)
	}

	// 7. Write intrinsic section (always — footer V4).
	intrinsicOffset, intrinsicLen, err := w.writeIntrinsicSection()
	if err != nil {
		return w.out.total, fmt.Errorf("writer: write intrinsic section: %w", err)
	}

	// 8. Write footer V4 (always includes intrinsic section offsets).
	if err = writeFooterV4(&w.out, headerOffset, compactOffset, uint32(compactN), intrinsicOffset, intrinsicLen); err != nil { //nolint:gosec // safe: compact index size bounded by MaxCompactSectionSize (50MB) fits uint32
		return w.out.total, fmt.Errorf("writer: write footer: %w", err)
	}

	total := w.out.total

	// 9. Reset ALL state.
	w.pending = w.pending[:0]
	w.blockMetas = nil
	w.sketchIdx = w.sketchIdx[:0]
	for k := range w.rangeIdx {
		delete(w.rangeIdx, k)
	}
	for k := range w.traceIndex {
		delete(w.traceIndex, k)
	}
	// protoRoots is already cleared by flushBlocks(); this is a defensive no-op.
	clear(w.protoRoots)
	w.protoRoots = w.protoRoots[:0]

	// Reset log state (parallel to trace state reset above).
	w.pendingLogs = w.pendingLogs[:0]
	clear(w.logProtoRoots)
	w.logProtoRoots = w.logProtoRoots[:0]
	w.signalType = 0

	// Reset intrinsic accumulator for reuse.
	w.intrinsicAccum = newIntrinsicAccumulator()

	return total, nil
}

// writeEmptyFile writes a minimal valid blockpack file with zero blocks.
func (w *Writer) writeEmptyFile() (int64, error) {
	metadataOffset := uint64(w.out.total) //nolint:gosec

	rIdx := make(rangeIndex)
	metaBytes, err := buildMetadataSectionBytes(
		shared.VersionV13, // V13: block index omits MinTraceID[16]+MaxTraceID[16] per entry
		nil,
		rIdx,
		w.traceIndex,
		nil, // no sketches for empty file
	)
	if err != nil {
		return w.out.total, err
	}

	// All empty files use V13.
	compressedEmptyMeta := snappy.Encode(nil, metaBytes)
	metadataLen := uint64(len(compressedEmptyMeta))
	if _, err = w.out.Write(compressedEmptyMeta); err != nil {
		return w.out.total, err
	}

	signalTypeForHeader := w.signalType
	if signalTypeForHeader == 0 {
		signalTypeForHeader = shared.SignalTypeTrace
	}
	headerOffset := uint64(w.out.total) //nolint:gosec
	if err = writeFileHeader(&w.out, shared.VersionV13, metadataOffset, metadataLen, signalTypeForHeader); err != nil {
		return w.out.total, err
	}

	compactOffset := uint64(w.out.total) //nolint:gosec
	compactN, err := writeCompactTraceIndex(&w.out, nil, w.traceIndex)
	if err != nil {
		return w.out.total, err
	}

	// Empty files also get a V4 footer with an empty intrinsic section (TOC with 0 columns).
	intrinsicOffset, intrinsicLen, err := w.writeIntrinsicSection()
	if err != nil {
		return w.out.total, err
	}

	if err = writeFooterV4(&w.out, headerOffset, compactOffset, uint32(compactN), intrinsicOffset, intrinsicLen); err != nil { //nolint:gosec // safe: compact index size bounded by MaxCompactSectionSize (50MB) fits uint32
		return w.out.total, err
	}

	return w.out.total, nil
}

// writeIntrinsicSection encodes the intrinsic accumulator, writes all column blobs and the
// TOC blob to the output stream, and returns the TOC's absolute file offset and byte length.
//
// If the accumulator is over cap (MaxIntrinsicRows), an empty TOC (0 columns) is written
// to preserve the V4 footer invariant without occupying significant space.
//
// The on-disk layout is:
//
//	col_blob_0[...]
//	col_blob_1[...]
//	...
//	toc_blob[...]    ← intrinsicIndexOffset points here; intrinsicIndexLen = len(toc_blob)
func (w *Writer) writeIntrinsicSection() (intrinsicOffset uint64, intrinsicLen uint32, err error) {
	a := w.intrinsicAccum
	if a == nil || a.overCap() {
		// Write empty TOC to satisfy V4 footer format.
		tocBlob, encErr := encodeTOC(nil)
		if encErr != nil {
			return 0, 0, encErr
		}
		off := uint64(w.out.total) //nolint:gosec
		if _, writeErr := w.out.Write(tocBlob); writeErr != nil {
			return 0, 0, writeErr
		}
		return off, uint32(len(tocBlob)), nil //nolint:gosec // safe: TOC blob bounded by column count × entry size, well within uint32
	}

	names := a.columnNames()

	// Encode and write each column blob, collecting metadata.
	metas := make([]shared.IntrinsicColMeta, 0, len(names))
	for _, name := range names {
		blob, encErr := a.encodeColumn(name)
		if encErr != nil {
			return 0, 0, fmt.Errorf("intrinsic: encode column %q: %w", name, encErr)
		}
		if len(blob) == 0 {
			continue
		}
		colOffset := uint64(w.out.total) //nolint:gosec
		if _, writeErr := w.out.Write(blob); writeErr != nil {
			return 0, 0, fmt.Errorf("intrinsic: write column %q: %w", name, writeErr)
		}
		minVal, maxVal := a.computeMinMax(name)
		colType, format := a.colTypeFor(name)
		metas = append(metas, shared.IntrinsicColMeta{
			Name:   name,
			Type:   colType,
			Format: format,
			Offset: colOffset,
			Length: uint32(len(blob)), //nolint:gosec
			Count:  a.rowCount(name),
			Min:    minVal,
			Max:    maxVal,
		})
	}

	// Encode and write TOC blob.
	tocBlob, encErr := encodeTOC(metas)
	if encErr != nil {
		return 0, 0, fmt.Errorf("intrinsic: encode TOC: %w", encErr)
	}
	tocOffset := uint64(w.out.total) //nolint:gosec
	if _, writeErr := w.out.Write(tocBlob); writeErr != nil {
		return 0, 0, fmt.Errorf("intrinsic: write TOC: %w", writeErr)
	}
	return tocOffset, uint32(len(tocBlob)), nil //nolint:gosec // safe: TOC blob bounded by column count × entry size, well within uint32
}

// buildAndWriteBlock builds one block from pending[0:end], writes it to the output, and
// records BlockMeta, range index entries, and traceIndex refs.
// Returns an error if blockID exceeds the uint16 trace-index wire format limit.
// Block IDs are 0-based and encoded as uint16, so the maximum valid ID is 65534
// (allowing at most 65535 blocks with IDs 0–65534; this keeps block_ref_count within uint16).
func (w *Writer) buildAndWriteBlock(blockID int, pending []pendingSpan) error {
	if blockID >= 65535 {
		return fmt.Errorf(
			"writer: block %d exceeds trace-index limit: block IDs are 0-based and encoded as uint16 (max ID 65534)",
			blockID,
		)
	}
	blockOffset := uint64(w.out.total) //nolint:gosec

	built, bb, err := buildBlock(pending, w.enc, w.bb, shared.VersionBlockV12, w.intrinsicAccum, blockID)
	w.bb = bb
	if err != nil {
		return fmt.Errorf("writer: block %d finalize: %w", blockID, err)
	}

	if _, err = w.out.Write(built.payload); err != nil {
		return fmt.Errorf("writer: block %d write: %w", blockID, err)
	}

	// Record BlockMeta.
	meta := shared.BlockMeta{
		Offset:     blockOffset,
		Length:     uint64(len(built.payload)),
		Kind:       shared.BlockKindLeaf,
		SpanCount:  uint32(built.spanCount), //nolint:gosec // safe: spanCount bounded by MaxBlockSpans (65535)
		MinStart:   built.minStart,
		MaxStart:   built.maxStart,
		MinTraceID: built.minTraceID,
		MaxTraceID: built.maxTraceID,
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

	// Collect sketch set for this block (parallel to blockMetas).
	w.sketchIdx = append(w.sketchIdx, built.colSketches)

	// Update the file-level trace index: record the block ID for each trace present.
	for tid := range built.traceRows {
		w.traceIndex[tid] = append(
			w.traceIndex[tid],
			uint16(blockID), //nolint:gosec // safe: blockID <= 65535 enforced by guard above
		)
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

// buildAndWriteLogBlock builds one log block from pending[0:end], writes it to the output,
// and records BlockMeta and range index entries. Does NOT update w.traceIndex.
func (w *Writer) buildAndWriteLogBlock(blockID int, pending []pendingLogRecord) error {
	blockOffset := uint64(w.out.total) //nolint:gosec

	built, err := buildLogBlock(pending, w.enc)
	if err != nil {
		return fmt.Errorf("writer: log block %d finalize: %w", blockID, err)
	}

	if _, err = w.out.Write(built.payload); err != nil {
		return fmt.Errorf("writer: log block %d write: %w", blockID, err)
	}

	meta := shared.BlockMeta{
		Offset:    blockOffset,
		Length:    uint64(len(built.payload)),
		Kind:      shared.BlockKindLeaf,
		SpanCount: uint32(built.spanCount), //nolint:gosec // safe: spanCount bounded by MaxBlockSpans (65535)
		MinStart:  built.minStart,
		MaxStart:  built.maxStart,
		// MinTraceID and MaxTraceID are zero ([16]byte zero value) for log blocks.
	}
	w.blockMetas = append(w.blockMetas, meta)

	bid := uint32(blockID) //nolint:gosec // safe: blockID bounded by MaxBlocks (100_000), fits uint32
	for _, mm := range built.colMinMax {
		cd, ok := w.rangeIdx[mm.colName]
		if !ok {
			cd = newRangeColumnData(mm.colType)
			w.rangeIdx[mm.colName] = cd
		}
		addBlockRangeToColumn(cd, mm, bid)
	}

	// Collect sketch set for this log block (parallel to blockMetas).
	w.sketchIdx = append(w.sketchIdx, built.colSketches)

	// No trace index update for log blocks.
	return nil
}

// flushLogBlocks sorts w.pendingLogs, writes all pending log blocks, and resets the buffer.
// Mirrors flushBlocks for the log signal path.
func (w *Writer) flushLogBlocks() error {
	if len(w.pendingLogs) == 0 {
		return nil
	}

	sortPendingLogs(w.pendingLogs)

	blockStart := 0
	for blockStart < len(w.pendingLogs) {
		blockEnd := min(blockStart+w.cfg.MaxBlockSpans, len(w.pendingLogs))

		blockID := len(w.blockMetas)
		if err := w.buildAndWriteLogBlock(blockID, w.pendingLogs[blockStart:blockEnd]); err != nil {
			clear(w.pendingLogs)
			w.pendingLogs = w.pendingLogs[:0]
			clear(w.logProtoRoots)
			w.logProtoRoots = w.logProtoRoots[:0]
			return err
		}
		blockStart = blockEnd
	}

	clear(w.pendingLogs)
	w.pendingLogs = w.pendingLogs[:0]

	clear(w.logProtoRoots)
	w.logProtoRoots = w.logProtoRoots[:0]

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
	traceIndex map[[16]byte][]uint16,
	sketchIdx []blockSketchSet,
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

	// TS index section — per-file timestamp index for direction-aware block ordering.
	tsBytes := writeTSIndexSection(blockMetas)
	buf = append(buf, tsBytes...)

	// Sketch index section — per-block HLL/CMS/BinaryFuse8 sketch data.
	if len(sketchIdx) > 0 {
		sketchBytes, err := writeSketchIndexSection(sketchIdx)
		if err != nil {
			return nil, fmt.Errorf("sketch_index: %w", err)
		}
		buf = append(buf, sketchBytes...)
	}

	return buf, nil
}

// AddRow buffers one row from a decoded Block for the columnar compaction path.
// This avoids all OTLP proto allocations — values are read directly from block columns
// at flush time by addRowFromBlock. The caller owns the block; it must remain valid
// until Flush() is called.
// Panics if called concurrently (NOTES §9).
func (w *Writer) AddRow(block *reader.Block, rowIdx int) error {
	if block == nil {
		return fmt.Errorf("writer: AddRow: block is nil")
	}
	if rowIdx < 0 || rowIdx >= block.SpanCount() {
		return fmt.Errorf("writer: AddRow: rowIdx %d out of range [0, %d)", rowIdx, block.SpanCount())
	}
	// Validate required intrinsic columns before acquiring the lock.
	traceCol := block.GetColumn("trace:id")
	if traceCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", "trace:id")
	}
	traceBytes, traceOK := traceCol.BytesValue(rowIdx)
	if !traceOK || len(traceBytes) != 16 {
		return fmt.Errorf("writer: AddRow: required column %q must have 16 bytes, got %d", "trace:id", len(traceBytes))
	}
	spanIDCol := block.GetColumn("span:id")
	if spanIDCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", "span:id")
	}
	spanIDBytes, spanIDOK := spanIDCol.BytesValue(rowIdx)
	if !spanIDOK || len(spanIDBytes) != 8 {
		return fmt.Errorf("writer: AddRow: required column %q must have 8 bytes, got %d", "span:id", len(spanIDBytes))
	}
	startCol := block.GetColumn("span:start")
	if startCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", "span:start")
	}
	if _, ok := startCol.Uint64Value(rowIdx); !ok {
		return fmt.Errorf("writer: AddRow: required column %q must have uint64 value at row %d", "span:start", rowIdx)
	}
	endCol := block.GetColumn("span:end")
	if endCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", "span:end")
	}
	if _, ok := endCol.Uint64Value(rowIdx); !ok {
		return fmt.Errorf("writer: AddRow: required column %q must have uint64 value at row %d", "span:end", rowIdx)
	}

	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	var tid [16]byte
	copy(tid[:], traceBytes)

	var svcName string
	if col := block.GetColumn("resource.service.name"); col != nil {
		if v, ok := col.StringValue(rowIdx); ok {
			svcName = v
		}
	}

	ps := pendingSpan{
		traceID:   tid,
		svcName:   svcName,
		srcBlock:  block,
		srcRowIdx: rowIdx,
	}
	computeMinHashSigFromBlock(&ps, block)
	w.pending = append(w.pending, ps)

	if w.cfg.MaxBufferedSpans > 0 && len(w.pending) >= w.cfg.MaxBufferedSpans {
		if err := w.flushBlocks(); err != nil {
			return fmt.Errorf("writer: auto-flush: %w", err)
		}
	}

	return nil
}

// CurrentSize returns estimated buffered size in bytes.
func (w *Writer) CurrentSize() int64 {
	return int64(len(w.pending)+len(w.pendingLogs)) * estimatedBytesPerSpan
}
