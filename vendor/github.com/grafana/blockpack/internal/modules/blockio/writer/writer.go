package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/golang/snappy"
	"github.com/grafana/tempo/pkg/tempopb"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Writer encodes OTLP spans into the blockpack format.
// NOT thread-safe — concurrent calls will panic (NOTE-004).
type Writer struct {
	// bbPool is a pool of reusable blockBuilders for parallel block construction.
	// Each goroutine in flushBlocks checks out one builder, builds, then returns it.
	// The pool starts empty; builders are created on first use by newBlockBuilder.
	bbPool sync.Pool

	// trace_id → list of block IDs (uint16) across all blocks
	traceIndex map[[16]byte][]uint16

	// UUID column detection: column name → detected as UUID
	uuidColumns map[string]bool

	// Range index built incrementally in flushBlocks serial pass, consumed at Flush.
	// Replaces the old flat log + O(n log n) sort approach.
	rangeIdx rangeIndex

	// sketchIdx accumulates per-block sketch sets across all blocks.
	// Indexed parallel to blockMetas: sketchIdx[i] is the sketch for block i.
	// Consumed at Flush() by writeV14Sections.
	sketchIdx []blockSketchSet

	// intrinsicAccum accumulates file-level columnar data for intrinsic columns.
	// Fed row-by-row during block building via blockBuilder.intrinsicAccum.
	// Consumed at Flush() by writeV14Sections to produce the V7 footer intrinsic columns.
	intrinsicAccum *intrinsicAccumulator

	// fileBloomSvcNames accumulates unique service names for file-level bloom construction.
	// Fed from flushBlocks and flushLogBlocks; consumed at Flush by writeV14Sections.
	fileBloomSvcNames map[string]struct{}

	// addRowIntrinsicCache caches per-block intrinsic indexes built during AddRowFromReader
	// calls. Key: (srcReader pointer, srcBlockIdx). Value: pre-built row→field map.
	// Avoids O(N) IntrinsicBytesAt/IntrinsicDictStringAt scans on every per-row call,
	// reducing AddRowFromReader from O(N^2) to O(N) per block.
	addRowIntrinsicCache map[addRowCacheKey]intrinsicRowFields

	// vectorAccum accumulates per-block vector data for building the VectorIndex section.
	// Nil when cfg.VectorDimension == 0 (no vector support requested).
	vectorAccum *vectorAccumulator

	// dedicatedCols is the pre-built set of full column names (e.g. "span.http.method")
	// configured as dedicated in cfg.DedicatedColumns. Built once in NewWriterWithConfig
	// and passed to each buildBlock call so attribute loops can feed matching columns
	// into the intrinsic section alongside the normal block columns.
	// Nil when cfg.DedicatedColumns is empty.
	dedicatedCols map[string]struct{}

	out countingWriter

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

	// tempoProtoRoots anchors *tempopb.Trace protos for the Tempo-native ingest path.
	// Cleared after flushBlocks() processes all pending spans referencing them.
	// Mirrors protoRoots for the AddTempoTrace path.
	tempoProtoRoots []*tempopb.Trace

	// pendingLogs holds lightweight log records awaiting the next flushLogBlocks call.
	// Parallel to w.pending (trace path). Protected by the same inUse guard.
	pendingLogs []pendingLogRecord

	// logProtoRoots anchors LogsData protos until flushLogBlocks() processes all pending
	// log records referencing them. Cleared after flushLogBlocks(). Mirrors protoRoots.
	logProtoRoots []*logsv1.LogsData

	cfg Config

	// inUse is a concurrency guard: AddSpan, AddTracesData, and Flush each do
	// CompareAndSwap(false, true) on entry and panic if the swap fails, detecting
	// concurrent callers. The Writer is documented as NOT thread-safe (NOTE-004).
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
	// Auto-detect vector dimension from the embedder when one is configured but
	// VectorDimension is not explicitly set. A probe embedding of a short string
	// determines the actual output dimension; this avoids requiring callers to
	// hard-code the dimension when using an embedder.
	if cfg.Embedder != nil && cfg.VectorDimension == 0 {
		probeVec, probeErr := cfg.Embedder.Embed("probe")
		if probeErr != nil {
			return nil, fmt.Errorf("writer: embedder probe failed: %w", probeErr)
		}
		cfg.VectorDimension = len(probeVec)
	}

	var va *vectorAccumulator
	if cfg.VectorDimension > 0 {
		va = newVectorAccumulator(cfg.VectorDimension)
	}
	var dedicatedCols map[string]struct{}
	if len(cfg.DedicatedColumns) > 0 {
		dedicatedCols = make(map[string]struct{}, len(cfg.DedicatedColumns))
		for _, dc := range cfg.DedicatedColumns {
			if dc.Name != "" {
				dedicatedCols[dc.Name] = struct{}{}
			}
		}
	}
	return &Writer{
		cfg:               cfg,
		out:               countingWriter{w: cfg.OutputStream},
		traceIndex:        make(map[[16]byte][]uint16),
		uuidColumns:       make(map[string]bool),
		rangeIdx:          make(rangeIndex),
		intrinsicAccum:    newIntrinsicAccumulator(),
		fileBloomSvcNames: make(map[string]struct{}),
		vectorAccum:       va,
		dedicatedCols:     dedicatedCols,
		// Pre-allocate pending to MaxBufferedSpans to avoid growslice on the hot path.
		// After each flushBlocks(), w.pending is reset to length 0 (capacity retained).
		pending: make([]pendingSpan, 0, cfg.MaxBufferedSpans),
	}, nil
}

// AddSpan buffers a single OTLP span.
// resourceSchemaURL and scopeSchemaURL are the SchemaUrl fields from
// ResourceSpans and ScopeSpans respectively; pass "" when not present.
// Panics if called concurrently (NOTE-004).
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
// Panics if called concurrently (NOTE-004).
//
//nolint:dupl // intentional mirror of AddTempoTrace for OTLP types; different proto types prevent sharing
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

// AddTempoTrace buffers all spans from a Tempo-native *tempopb.Trace message.
// Use this when consuming data directly from Tempo's storage layer to avoid
// the round-trip conversion through OTLP types.
// Panics if called concurrently (NOTE-004).
//
//nolint:dupl // intentional mirror of AddTracesData for Tempo-native types; different proto types prevent sharing
func (w *Writer) AddTempoTrace(trace *tempopb.Trace) error {
	if trace == nil {
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
	w.tempoProtoRoots = append(w.tempoProtoRoots, trace)

	for _, rs := range trace.ResourceSpans {
		if rs == nil {
			continue
		}
		svcName := extractSvcNameFromTempoProto(rs.Resource)
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
					traceID:   tid,
					svcName:   svcName,
					tempoRS:   rs,
					tempoSS:   ss,
					tempoSpan: span,
				}
				computeMinHashSigFromTempoProto(&ps)
				w.pending = append(w.pending, ps)

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

	// 2. Apply KLL bucket boundaries to the range index.
	// KLL sketches were built incrementally in flushBlocks (one Add per block min
	// and max), so no re-scan is needed here.
	applyRangeBuckets(w.rangeIdx, defaultRangeBuckets)

	// 3–8. Write V14 sections + Footer V7.
	if err := w.writeV14Sections(); err != nil {
		return w.out.total, fmt.Errorf("writer: write V14 sections: %w", err)
	}

	total := w.out.total

	// 9. Reset ALL state.
	w.pending = w.pending[:0]
	w.blockMetas = nil
	for _, bs := range w.sketchIdx {
		releaseBlockSketchSet(bs)
	}
	w.sketchIdx = w.sketchIdx[:0]
	for k := range w.rangeIdx {
		delete(w.rangeIdx, k)
	}
	for k := range w.traceIndex {
		delete(w.traceIndex, k)
	}
	// protoRoots and tempoProtoRoots are already cleared by flushBlocks(); these are defensive no-ops.
	clear(w.protoRoots)
	w.protoRoots = w.protoRoots[:0]
	clear(w.tempoProtoRoots)
	w.tempoProtoRoots = w.tempoProtoRoots[:0]

	// Reset log state (parallel to trace state reset above).
	w.pendingLogs = w.pendingLogs[:0]
	clear(w.logProtoRoots)
	w.logProtoRoots = w.logProtoRoots[:0]
	w.signalType = 0

	// Reset intrinsic accumulator for reuse.
	w.intrinsicAccum = newIntrinsicAccumulator()

	// Reset file-level bloom service names.
	for k := range w.fileBloomSvcNames {
		delete(w.fileBloomSvcNames, k)
	}

	// Reset vector accumulator for reuse.
	if w.vectorAccum != nil {
		w.vectorAccum = newVectorAccumulator(w.cfg.VectorDimension)
	}

	return total, nil
}

// writeEmptyFile writes a minimal valid V14 blockpack file with zero blocks.
func (w *Writer) writeEmptyFile() (int64, error) {
	if err := w.writeV14Sections(); err != nil {
		return w.out.total, err
	}
	return w.out.total, nil
}

// writeV14Sections writes all V14 metadata sections, section directory, and Footer V7.
//
// Type-keyed sections (each snappy-compressed independently):
//   - SectionBlockIndex  (0x01): block index
//   - SectionRangeIndex  (0x02): range column index
//   - SectionTraceIndex  (0x03): compact trace index (same format as V13 compact index)
//   - SectionTSIndex     (0x04): timestamp index
//   - SectionSketchIndex (0x05): sketch index
//   - SectionFileBloom   (0x06): file-level bloom filter
//
// Name-keyed entries (DirEntryName, one per file-level intrinsic column blob) are written
// after the type-keyed sections. Each intrinsic column blob is snappy-compressed independently.
//
// After all sections: writes snappy-compressed section directory + Footer V7.
func (w *Writer) writeV14Sections() error {
	var typeEntries []shared.DirEntryType
	var nameEntries []shared.DirEntryName

	// Helper: compress raw bytes, write, record a type-keyed DirEntryType.
	writeSection := func(sectionType uint8, raw []byte) error {
		compressed := snappy.Encode(nil, raw)
		offset := uint64(w.out.total) //nolint:gosec
		if _, err := w.out.Write(compressed); err != nil {
			return fmt.Errorf("section 0x%02X write: %w", sectionType, err)
		}
		typeEntries = append(typeEntries, shared.DirEntryType{
			SectionType:   sectionType,
			Offset:        offset,
			CompressedLen: uint32(len(compressed)), //nolint:gosec // safe: section size fits uint32
		})
		return nil
	}

	// SectionBlockIndex (0x01).
	blockIdxRaw, err := writeBlockIndexSection(nil, w.blockMetas)
	if err != nil {
		return fmt.Errorf("block_index: %w", err)
	}
	if err = writeSection(shared.SectionBlockIndex, blockIdxRaw); err != nil {
		return err
	}

	// SectionRangeIndex (0x02).
	rangeIdxRaw, err := writeRangeIndexSection(nil, w.rangeIdx)
	if err != nil {
		return fmt.Errorf("range_index: %w", err)
	}
	if err = writeSection(shared.SectionRangeIndex, rangeIdxRaw); err != nil {
		return err
	}

	// SectionTraceIndex (0x03): compact trace index (same binary format as V13 compact section).
	var compactBuf bytes.Buffer
	if _, err = writeCompactTraceIndex(&compactBuf, w.blockMetas, w.traceIndex); err != nil {
		return fmt.Errorf("trace_index: %w", err)
	}
	if err = writeSection(shared.SectionTraceIndex, compactBuf.Bytes()); err != nil {
		return err
	}

	// SectionTSIndex (0x04): timestamp index.
	tsRaw := writeTSIndexSection(w.blockMetas)
	if err = writeSection(shared.SectionTSIndex, tsRaw); err != nil {
		return err
	}

	// SectionSketchIndex (0x05): sketch index (may be empty).
	if len(w.sketchIdx) > 0 {
		sketchRaw, sketchErr := writeSketchIndexSection(w.sketchIdx)
		if sketchErr != nil {
			return fmt.Errorf("sketch_index: %w", sketchErr)
		}
		if err = writeSection(shared.SectionSketchIndex, sketchRaw); err != nil {
			return err
		}
	}

	// SectionFileBloom (0x06): file-level bloom filter (may be empty).
	if len(w.fileBloomSvcNames) > 0 {
		bloomRaw, bloomErr := writeFileBloomSection(w.fileBloomSvcNames)
		if bloomErr != nil {
			return fmt.Errorf("file_bloom: %w", bloomErr)
		}
		if err = writeSection(shared.SectionFileBloom, bloomRaw); err != nil {
			return err
		}
	}

	// File-level intrinsic column blobs: each column is one snappy-compressed blob
	// with a name-keyed DirEntryName in the section directory.
	a := w.intrinsicAccum
	if a != nil && !a.overCap() {
		for _, name := range a.columnNames() {
			blob, encErr := a.encodeColumn(name)
			if encErr != nil {
				return fmt.Errorf("intrinsic column %q encode: %w", name, encErr)
			}
			if len(blob) == 0 {
				continue
			}
			// blob is already snappy-compressed by encodeColumn/encodePagedXxxColumn.
			// Do NOT re-compress; store directly so GetIntrinsicColumnBlob returns
			// a valid snappy stream that DecodeIntrinsicColumnBlob can decode.
			colOffset := uint64(w.out.total) //nolint:gosec
			if _, writeErr := w.out.Write(blob); writeErr != nil {
				return fmt.Errorf("intrinsic column %q write: %w", name, writeErr)
			}
			nameEntries = append(nameEntries, shared.DirEntryName{
				Name:          name,
				Offset:        colOffset,
				CompressedLen: uint32(len(blob)), //nolint:gosec // bounded by MaxIntrinsicRows
			})
		}
	}

	// Build + write section directory (snappy-compressed).
	// entry_count[4] + signal-kind entry (2 bytes) + type-keyed entries (14 bytes each) + name-keyed entries (variable).
	// Always include signal_type as a DirEntryKindSignal entry (kind=0x02, signal_type=1 byte).
	signalType := w.signalType
	if signalType == 0 {
		signalType = shared.SignalTypeTrace
	}
	const signalEntryCount = 1 // one DirEntryKindSignal entry
	totalEntries := signalEntryCount + len(typeEntries) + len(nameEntries)
	dirRaw := make([]byte, 0, 4+2+totalEntries*shared.DirEntryTypeWireSize)
	dirRaw = appendUint32LE(dirRaw, uint32(totalEntries)) //nolint:gosec // safe: section count is small
	// Signal-kind entry: entry_kind[1]=0x02 + signal_type[1]
	dirRaw = append(dirRaw, shared.DirEntryKindSignal, signalType)
	for _, e := range typeEntries {
		dirRaw = append(dirRaw, e.Marshal()...)
	}
	for _, e := range nameEntries {
		dirRaw = append(dirRaw, e.Marshal()...)
	}

	compressedDir := snappy.Encode(nil, dirRaw)
	dirOffset := uint64(w.out.total) //nolint:gosec
	if _, err = w.out.Write(compressedDir); err != nil {
		return fmt.Errorf("section directory write: %w", err)
	}
	dirLen := uint32(len(compressedDir)) //nolint:gosec // safe: directory size fits uint32

	// Write Footer V7 (V14 section-directory footer).
	return writeFooterV7(&w.out, dirOffset, dirLen)
}

// flushBlocks sorts w.pending, builds all blocks concurrently, writes payloads and
// updates indexes in deterministic block-ID order, then resets w.pending to empty.
// Appends to w.blockMetas/traceIndex and updates w.rangeIdx incrementally.
//
// Called automatically by AddSpan/AddTracesData when len(w.pending) >= cfg.MaxBufferedSpans,
// and called by Flush() to process any remaining buffered spans before writing metadata.
//
// INVARIANT (NOTES §17): Does NOT write metadata/header/footer. That happens only in Flush().
// INVARIANT: rangeIdx is updated here but KLL/range-buckets are applied at Flush().
// INVARIANT: Block IDs are globally sequential: blockID := len(w.blockMetas) + i.
//
// RSS bound: After processing all blocks, protoRoots is cleared. The w.rangeIdx map keys
// independently keep string backing bytes alive (GC traces map key pointers), so clearing
// protoRoots is safe and releases the batch's proto memory back to the GC.
func (w *Writer) flushBlocks() error {
	if len(w.pending) == 0 {
		return nil
	}

	sortPending(w.pending)

	// Pre-compute block boundaries and pre-assign block IDs so goroutines can
	// write to results[i] without any coordination.
	type blockSlice struct {
		spans   []pendingSpan
		blockID int
	}
	var slices []blockSlice
	blockStart := 0
	for blockStart < len(w.pending) {
		blockEnd := min(blockStart+w.cfg.MaxBlockSpans, len(w.pending))
		slices = append(slices, blockSlice{
			spans:   w.pending[blockStart:blockEnd],
			blockID: len(w.blockMetas) + len(slices),
		})
		blockStart = blockEnd
	}

	// Validate all block IDs up-front so goroutines don't need error coordination
	// for the uint16 overflow check.
	for _, s := range slices {
		if s.blockID >= 65535 {
			return fmt.Errorf(
				"writer: block %d exceeds trace-index limit: block IDs are 0-based and encoded as uint16 (max ID 65534)",
				s.blockID,
			)
		}
	}

	// Pre-compute embedding vectors for all pending spans when auto-embedding is enabled.
	// This runs sequentially before the parallel block-build phase because Embed() is
	// typically a network/compute call that must not be parallelised without external
	// fan-in coordination. The result is a flat slice parallel to w.pending; each block
	// goroutine receives its own subslice (no per-goroutine allocation needed).
	var allSpanVectors [][]float32
	if w.cfg.Embedder != nil {
		var embedErr error
		allSpanVectors, embedErr = w.embedPendingSpans(w.pending)
		if embedErr != nil {
			clear(w.pending)
			w.pending = w.pending[:0]
			clear(w.protoRoots)
			w.protoRoots = w.protoRoots[:0]
			clear(w.tempoProtoRoots)
			w.tempoProtoRoots = w.tempoProtoRoots[:0]
			return fmt.Errorf("writer: embed spans: %w", embedErr)
		}
	}

	results := make([]builtBlock, len(slices))

	// Parallel build phase: each goroutine builds one block independently.
	// localAccum is per-goroutine; merged serially below.
	var g errgroup.Group
	g.SetLimit(runtime.NumCPU())
	for i, s := range slices {
		i, s := i, s // capture loop variables
		g.Go(func() error {
			// Compute the vector subslice for this block. When allSpanVectors is nil
			// (no embedder configured), blockVecs is nil and buildBlock skips injection.
			var blockVecs [][]float32
			if allSpanVectors != nil {
				spanOffset := 0
				for j := 0; j < i; j++ {
					spanOffset += len(slices[j].spans)
				}
				blockVecs = allSpanVectors[spanOffset : spanOffset+len(s.spans)]
			}
			localAccum := newIntrinsicAccumulator()
			bb, _ := w.bbPool.Get().(*blockBuilder)
			built, bb, err := buildBlock(
				s.spans,
				bb,
				shared.VersionBlockV14,
				localAccum,
				s.blockID,
				blockVecs,
				w.dedicatedCols,
			)
			if err != nil {
				w.bbPool.Put(bb)
				return fmt.Errorf("writer: block %d finalize: %w", s.blockID, err)
			}
			// Extract vectors from bb BEFORE returning it to the pool.
			// extractBlockVectors reads bb.columns which will be reset on pool reuse.
			if w.vectorAccum != nil {
				built.blockVectors = extractBlockVectors(bb)
			}
			results[i] = built
			// Put bb AFTER results[i] is written — traceRows and colMinMax inside built
			// are direct map references into bb. Returning bb to the pool before this
			// assignment would allow a concurrent goroutine's reset() call to clear those
			// maps while the serial merge pass is still reading them.
			w.bbPool.Put(bb)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		// At least one block failed. Reset buffered state to avoid a stuck Writer.
		clear(w.pending)
		w.pending = w.pending[:0]
		clear(w.protoRoots)
		w.protoRoots = w.protoRoots[:0]
		clear(w.tempoProtoRoots)
		w.tempoProtoRoots = w.tempoProtoRoots[:0]
		return err
	}

	// Serial merge pass (in block-ID order): write payloads, merge accumulators,
	// update indexes. Order is deterministic because results[i] corresponds to
	// slices[i] which has a pre-assigned blockID.
	for i, built := range results {
		s := slices[i]
		blockOffset := uint64(w.out.total) //nolint:gosec

		if _, err := w.out.Write(built.payload); err != nil {
			// Clear buffered state so the Writer is not left in a partially-flushed limbo.
			// Blocks written before this failure are unrecoverable, but clearing prevents
			// a subsequent Flush()/auto-flush from re-processing already-consumed spans.
			clear(w.pending)
			w.pending = w.pending[:0]
			clear(w.protoRoots)
			w.protoRoots = w.protoRoots[:0]
			clear(w.tempoProtoRoots)
			w.tempoProtoRoots = w.tempoProtoRoots[:0]
			return fmt.Errorf("writer: block %d write: %w", s.blockID, err)
		}

		meta := shared.BlockMeta{
			Offset:     blockOffset,
			Length:     uint64(len(built.payload)),
			Kind:       shared.BlockKindLeaf,
			SpanCount:  uint32(built.spanCount), //nolint:gosec
			MinStart:   built.minStart,
			MaxStart:   built.maxStart,
			MinTraceID: built.minTraceID,
			MaxTraceID: built.maxTraceID,
		}
		// Release payload memory immediately after writing to bound peak RSS.
		results[i].payload = nil

		w.blockMetas = append(w.blockMetas, meta)

		// Merge per-block intrinsic accumulator into the file-level one, then release.
		if built.localAccum != nil {
			w.intrinsicAccum.merge(built.localAccum)
			results[i].localAccum = nil
		}

		// Accumulate vectors for PQ training (serial to avoid concurrent map writes).
		if w.vectorAccum != nil && len(built.blockVectors) > 0 {
			w.vectorAccum.accumulateBlock(s.blockID, built.blockVectors)
			results[i].blockVectors = nil // release memory after accumulation
		}

		// Update range index.
		bid := uint32(s.blockID) //nolint:gosec
		for _, mm := range built.colMinMax {
			cd, ok := w.rangeIdx[mm.colName]
			if !ok {
				cd = newRangeColumnData(mm.colType)
				w.rangeIdx[mm.colName] = cd
			}
			addBlockRangeToColumn(cd, mm, bid)
		}

		// Collect sketch set for this block.
		w.sketchIdx = append(w.sketchIdx, built.colSketches)

		// Update file-level trace index.
		for tid := range built.traceRows {
			w.traceIndex[tid] = append(
				w.traceIndex[tid],
				uint16(s.blockID), //nolint:gosec
			)
		}
	}

	// Collect service names for file-level bloom filter.
	for _, ps := range w.pending {
		if ps.svcName != "" {
			w.fileBloomSvcNames[ps.svcName] = struct{}{}
		}
	}

	// Clear pending and proto anchors (same invariant as before).
	clear(w.pending)
	w.pending = w.pending[:0]
	clear(w.protoRoots)
	w.protoRoots = w.protoRoots[:0]
	clear(w.tempoProtoRoots)
	w.tempoProtoRoots = w.tempoProtoRoots[:0]

	return nil
}

// flushLogBlocks sorts w.pendingLogs, builds all log blocks concurrently, writes payloads
// and updates indexes in deterministic block-ID order, then resets the buffer.
// Mirrors flushBlocks for the log signal path. Log blocks have no intrinsicAccum.
func (w *Writer) flushLogBlocks() error {
	if len(w.pendingLogs) == 0 {
		return nil
	}

	sortPendingLogs(w.pendingLogs)

	// Pre-compute block boundaries and pre-assign block IDs.
	type logBlockSlice struct {
		records []pendingLogRecord
		blockID int
	}
	var slices []logBlockSlice
	blockStart := 0
	for blockStart < len(w.pendingLogs) {
		blockEnd := min(blockStart+w.cfg.MaxBlockSpans, len(w.pendingLogs))
		slices = append(slices, logBlockSlice{
			records: w.pendingLogs[blockStart:blockEnd],
			blockID: len(w.blockMetas) + len(slices),
		})
		blockStart = blockEnd
	}

	results := make([]builtBlock, len(slices))

	// Parallel build phase.
	var g errgroup.Group
	g.SetLimit(runtime.NumCPU())
	for i, s := range slices {
		i, s := i, s
		g.Go(func() error {
			built, err := buildLogBlock(s.records)
			if err != nil {
				return fmt.Errorf("writer: log block %d finalize: %w", s.blockID, err)
			}
			results[i] = built
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		clear(w.pendingLogs)
		w.pendingLogs = w.pendingLogs[:0]
		clear(w.logProtoRoots)
		w.logProtoRoots = w.logProtoRoots[:0]
		return err
	}

	// Serial merge pass.
	for i, built := range results {
		s := slices[i]
		blockOffset := uint64(w.out.total) //nolint:gosec

		if _, err := w.out.Write(built.payload); err != nil {
			// Clear buffered state so the Writer is not left in a partially-flushed limbo.
			clear(w.pendingLogs)
			w.pendingLogs = w.pendingLogs[:0]
			clear(w.logProtoRoots)
			w.logProtoRoots = w.logProtoRoots[:0]
			return fmt.Errorf("writer: log block %d write: %w", s.blockID, err)
		}
		results[i].payload = nil

		meta := shared.BlockMeta{
			Offset:    blockOffset,
			Length:    uint64(len(built.payload)),
			Kind:      shared.BlockKindLeaf,
			SpanCount: uint32(built.spanCount), //nolint:gosec
			MinStart:  built.minStart,
			MaxStart:  built.maxStart,
			// MinTraceID and MaxTraceID are zero ([16]byte zero value) for log blocks.
		}
		w.blockMetas = append(w.blockMetas, meta)

		bid := uint32(s.blockID) //nolint:gosec
		for _, mm := range built.colMinMax {
			cd, ok := w.rangeIdx[mm.colName]
			if !ok {
				cd = newRangeColumnData(mm.colType)
				w.rangeIdx[mm.colName] = cd
			}
			addBlockRangeToColumn(cd, mm, bid)
		}

		w.sketchIdx = append(w.sketchIdx, built.colSketches)
		// No trace index update for log blocks.
	}

	// Collect service names for file-level bloom filter.
	for _, pl := range w.pendingLogs {
		if pl.svcName != "" {
			w.fileBloomSvcNames[pl.svcName] = struct{}{}
		}
	}

	clear(w.pendingLogs)
	w.pendingLogs = w.pendingLogs[:0]
	clear(w.logProtoRoots)
	w.logProtoRoots = w.logProtoRoots[:0]

	return nil
}

// AddRow buffers one row from a decoded Block for the columnar compaction path.
// This avoids all OTLP proto allocations — values are read directly from block columns
// at flush time by addRowFromBlock. The caller owns the block; it must remain valid
// until Flush() is called.
// Panics if called concurrently (NOTE-004).
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
	spanIDCol := block.GetColumn(spanIDColumnName)
	if spanIDCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", spanIDColumnName)
	}
	spanIDBytes, spanIDOK := spanIDCol.BytesValue(rowIdx)
	if !spanIDOK || len(spanIDBytes) != 8 {
		return fmt.Errorf(
			"writer: AddRow: required column %q must have 8 bytes, got %d",
			spanIDColumnName,
			len(spanIDBytes),
		)
	}
	startCol := block.GetColumn(spanStartColumnName)
	if startCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", spanStartColumnName)
	}
	if _, ok := startCol.Uint64Value(rowIdx); !ok {
		return fmt.Errorf(
			"writer: AddRow: required column %q must have uint64 value at row %d",
			spanStartColumnName,
			rowIdx,
		)
	}
	endCol := block.GetColumn(spanEndColumnName)
	if endCol == nil {
		return fmt.Errorf("writer: AddRow: required column %q missing", spanEndColumnName)
	}
	if _, ok := endCol.Uint64Value(rowIdx); !ok {
		return fmt.Errorf(
			"writer: AddRow: required column %q must have uint64 value at row %d",
			spanEndColumnName,
			rowIdx,
		)
	}

	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	var tid [16]byte
	copy(tid[:], traceBytes)

	var svcName string
	if col := block.GetColumn(svcNameColumnName); col != nil {
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

// addRowCacheKey identifies a unique (reader, blockIdx) pair for the AddRowFromReader
// per-block intrinsic index cache.
type addRowCacheKey struct {
	r        *reader.Reader
	blockIdx int
}

// AddRowFromReader adds one row from the source block at rowIdx, reading required
// identity fields (trace:id, span:id, span:start) from the source reader's intrinsic
// section when block columns are absent (new storage format where intrinsic columns
// live exclusively in the intrinsic section, not block columns).
// Uses a per-Writer cache to build the intrinsic index once per (reader, blockIdx) pair,
// reducing the trace:id and svcName lookups from O(N) per row to O(1).
func (w *Writer) AddRowFromReader(block *reader.Block, rowIdx int, srcReader *reader.Reader, srcBlockIdx int) error {
	if block == nil {
		return fmt.Errorf("writer: AddRowFromReader: block is nil")
	}
	if rowIdx < 0 || rowIdx >= block.SpanCount() {
		return fmt.Errorf("writer: AddRowFromReader: rowIdx %d out of range [0, %d)", rowIdx, block.SpanCount())
	}

	// Acquire single-use guard before any access to w.addRowIntrinsicCache.
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)

	// PATTERN: block-column-first with intrinsic-section fallback (shared across
	// compaction/compaction.go, writer.go, executor/executor.go, executor/metrics_trace.go).
	// v3 files store identity columns in block payloads; v4 files store them exclusively
	// in the intrinsic section. Try the block column first for backwards compat.

	// Resolve trace:id — block column if present, otherwise O(1) index lookup.
	var traceBytes []byte
	if col := block.GetColumn("trace:id"); col != nil {
		traceBytes, _ = col.BytesValue(rowIdx)
	}
	if len(traceBytes) != 16 && srcReader != nil {
		idx := w.getOrBuildAddRowIndex(srcReader, srcBlockIdx)
		if idx != nil {
			if v, ok := idx[uint16(rowIdx)]["trace:id"]; ok { //nolint:gosec // rowIdx bounded by SpanCount (≤65535)
				traceBytes, _ = v.([]byte)
			}
		}
	}
	if len(traceBytes) != 16 {
		return fmt.Errorf("writer: AddRowFromReader: trace:id missing at row %d", rowIdx)
	}

	var tid [16]byte
	copy(tid[:], traceBytes)

	// svcName — block column if present, otherwise O(1) index lookup.
	var svcName string
	if col := block.GetColumn(svcNameColumnName); col != nil {
		svcName, _ = col.StringValue(rowIdx)
	}
	if svcName == "" && srcReader != nil {
		idx := w.getOrBuildAddRowIndex(srcReader, srcBlockIdx)
		if idx != nil {
			if v, ok := idx[uint16(rowIdx)][svcNameColumnName]; ok { //nolint:gosec // rowIdx bounded by SpanCount (≤65535)
				svcName, _ = v.(string)
			}
		}
	}

	ps := pendingSpan{
		traceID:     tid,
		svcName:     svcName,
		srcBlock:    block,
		srcReader:   srcReader,
		srcBlockIdx: srcBlockIdx,
		srcRowIdx:   rowIdx,
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

// getOrBuildAddRowIndex returns the cached per-block intrinsic index for the given
// (srcReader, srcBlockIdx) pair, building it on first access via buildIntrinsicBlockIndex.
// Called by AddRowFromReader to avoid O(N) IntrinsicBytesAt/IntrinsicDictStringAt scans.
// Must be called while w.inUse is held (AddRowFromReader acquires the CAS guard first).
func (w *Writer) getOrBuildAddRowIndex(r *reader.Reader, blockIdx int) intrinsicRowFields {
	k := addRowCacheKey{r, blockIdx}
	if w.addRowIntrinsicCache != nil {
		if idx, ok := w.addRowIntrinsicCache[k]; ok {
			return idx
		}
	}
	idx := buildIntrinsicBlockIndex(r, blockIdx)
	if w.addRowIntrinsicCache == nil {
		w.addRowIntrinsicCache = make(map[addRowCacheKey]intrinsicRowFields)
	}
	w.addRowIntrinsicCache[k] = idx
	return idx
}

// CurrentSize returns estimated buffered size in bytes.
func (w *Writer) CurrentSize() int64 {
	return int64(len(w.pending)+len(w.pendingLogs)) * estimatedBytesPerSpan
}
