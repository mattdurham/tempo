package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/grafana/tempo/pkg/tempopb"
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
	// Consumed at Flush() by writeV8Sections.
	sketchIdx []blockSketchSet

	// colStatsByBlock accumulates per-block per-column statistics (NOTE-446, issue #364).
	// Fed in the serial flush pass from built.colStats; consumed at Flush() by
	// writeV8Sections to write the ToCSubTypeColStats section.
	colStatsByBlock []shared.BlockColStats

	// intrinsicAccum accumulates file-level columnar data for intrinsic columns.
	// NOTE-461 (issue #380): backed by per-column on-disk spill files (tempFileAccum) so peak
	// RSS during compaction is bounded by the largest single column, not all columns × all
	// spans. Per-block accumulation still uses an in-memory intrinsicAccumulator (localAccum);
	// the serial merge pass spills each block's rows here via spillMerge. Created lazily on
	// the first flushBlocks() (see ensureIntrinsicAccum) and released at Flush().
	intrinsicAccum *tempFileAccum

	// spanTreeAccum accumulates one structural record per span for the SpanTree section
	// (NOTE-462, issue #381). Backed by sorted on-disk run files (external sort) so peak RSS
	// is bounded by one run buffer during the spill phase and by the largest single trace
	// during the merge/DFS phase — never by total span count. Fed in the serial flush pass
	// from each block's intrinsic identity columns; consumed at Flush() by writeV8FileSections.
	// Created lazily on the first flushBlocks() (see ensureSpanTreeAccum) and released at Flush().
	spanTreeAccum *spanTreeAccum

	// fileBloomSvcNames accumulates unique service names for file-level bloom construction.
	// Fed from flushBlocks; consumed at Flush by writeV8Sections.
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

	cfg Config

	// inUse is a concurrency guard: AddSpan, AddTracesData, and Flush each do
	// CompareAndSwap(false, true) on entry and panic if the swap fails, detecting
	// concurrent callers. The Writer is documented as NOT thread-safe (NOTE-004).
	inUse atomic.Bool

	// signalType identifies the file's signal type. Blockpack stores traces only
	// (NOTE-460, issue #376), so this is always shared.SignalTypeTrace once any data
	// has been added. 0 means unset; shared.SignalTypeTrace = 0x01.
	signalType uint8
}

// countingWriter wraps io.Writer and tracks total bytes written.
// This is needed because io.Writer has no built-in byte count; Flush() returns
// the total bytes written as its first return value.

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
	if cfg.MinBlockSpans == 0 {
		cfg.MinBlockSpans = defaultMinBlockSpans
	}
	// NOTE-AP-001: apply the AllPresent encoding rollout flag. Default is enabled; setting
	// Config.DisableAllPresentEncoding forces the legacy presence-RLE form for every column.
	setAllPresentEncodingEnabled(!cfg.DisableAllPresentEncoding)
	// NOTE-215: apply the bit-packed DeltaUint64 rollout flag. Default is enabled; setting
	// Config.DisableBitPackedDelta forces the legacy byte-width form (kind 5).
	setBitPackedDeltaEnabled(!cfg.DisableBitPackedDelta)
	// NOTE-218: apply the per-page DeltaUint64 rollout flag. Default is enabled; setting
	// Config.DisablePagedDelta forces the single-page forms (kinds 5/22).
	setPagedDeltaEnabled(!cfg.DisablePagedDelta)
	// NOTE-217: apply the uniform-length XORBytes rollout flag. Default is enabled; setting
	// Config.DisableUniformBytes forces the legacy variable-length form (kinds 8/9/19).
	setUniformBytesEnabled(!cfg.DisableUniformBytes)
	// NOTE-219: apply the Gorilla-XOR Float64 rollout flag. Default is enabled; setting
	// Config.DisableGorillaFloat64 forces the Dictionary form (kinds 1/2) for float columns.
	setGorillaFloat64Enabled(!cfg.DisableGorillaFloat64)
	// NOTE-220: apply the V15 inline-column rollout flag. Default is DISABLED (V14 blocks);
	// setting Config.EnableInlineColumns emits V15 blocks with inline tiny columns.
	setInlineColumnsEnabled(cfg.EnableInlineColumns)
	// NOTE-405 (issue #355): apply the per-column zstd rollout flag. Default is DISABLED
	// (every column blob snappy); setting Config.EnableZstdColumns lets the writer pick zstd
	// per-column when it beats snappy by the benefit margin. Requires V15 (EnableInlineColumns)
	// since the codec is signaled by a V15 flags-byte bit; gated to that combination below.
	setZstdColumnsEnabled(cfg.EnableZstdColumns && cfg.EnableInlineColumns)
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
					traceID:  tid,
					svcName:  svcName,
					spanName: span.Name,
					rs:       rs,
					ss:       ss,
					span:     span,
				}
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
					spanName:  span.Name,
					tempoRS:   rs,
					tempoSS:   ss,
					tempoSpan: span,
				}
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

// Flush sorts spans, encodes blocks, writes all structures, and returns bytes written.
// Panics if called concurrently.
func (w *Writer) Flush() (int64, error) {
	if !w.inUse.CompareAndSwap(false, true) {
		panic("writer: concurrent use detected")
	}
	defer w.inUse.Store(false)
	// Always release the on-disk intrinsic spill files / temp dir on any Flush exit path,
	// including errors, so a failed compaction does not leak scratch files (NOTE-461). close
	// is idempotent; the success path's explicit reset below nils the reference first.
	defer func() {
		if w.intrinsicAccum != nil {
			_ = w.intrinsicAccum.close()
			w.intrinsicAccum = nil
		}
		// NOTE-462: release the SpanTree spill run files / temp dir on every Flush exit path.
		if w.spanTreeAccum != nil {
			_ = w.spanTreeAccum.close()
			w.spanTreeAccum = nil
		}
	}()

	if len(w.pending) == 0 && len(w.blockMetas) == 0 {
		// Nothing has ever been written — produce a valid empty file.
		return w.writeEmptyFile()
	}

	// 1. Flush any remaining buffered spans into blocks.
	if err := w.flushBlocks(); err != nil {
		return w.out.total, err
	}

	// 2. Apply KLL bucket boundaries to the range index.
	// KLL sketches were built incrementally in flushBlocks (one Add per block min
	// and max), so no re-scan is needed here.
	applyRangeBuckets(w.rangeIdx, defaultRangeBuckets)

	// 3–8. Write V8 sections + Footer.
	if err := w.writeV8Sections(); err != nil {
		return w.out.total, fmt.Errorf("writer: write V8 sections: %w", err)
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
	w.signalType = 0

	// Intrinsic accumulator is closed and nil'd by the deferred cleanup at the top of Flush()
	// (NOTE-461); the next write lazily re-creates it via ensureIntrinsicAccum.

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

// writeEmptyFile writes a minimal valid blockpack file with zero blocks.
func (w *Writer) writeEmptyFile() (int64, error) {
	if err := w.writeV8Sections(); err != nil {
		return w.out.total, err
	}
	return w.out.total, nil
}

// ensureIntrinsicAccum lazily creates the file-level on-disk intrinsic accumulator
// (NOTE-461). Created on first use so a Writer that is constructed but never written
// (or only used for an empty file) does not touch the filesystem, and so the spill
// directory is created under w.cfg.ScratchDir when configured by the caller (compaction).
func (w *Writer) ensureIntrinsicAccum() error {
	if w.intrinsicAccum != nil {
		return nil
	}
	a, err := newTempFileAccum(w.cfg.ScratchDir)
	if err != nil {
		return err
	}
	// NOTE-476 (issue #394): when configured, drop the identity columns from the persisted
	// IntrinsicTOC. The SpanTree (fed independently from the per-block accumulator) is the
	// sole identity store for these blocks; readers fall back to it via SpanTreeIdentityForBlock.
	if w.cfg.OmitIntrinsicIdentityColumns {
		a.skipCols = map[string]struct{}{
			traceIDColumnName:      {},
			spanIDColumnName:       {},
			spanParentIDColumnName: {},
		}
	}
	w.intrinsicAccum = a
	return nil
}

// ensureSpanTreeAccum lazily creates the file-level on-disk SpanTree accumulator
// (NOTE-462, issue #381). Like ensureIntrinsicAccum it is created on first use and spills
// under w.cfg.ScratchDir when the caller configures one.
func (w *Writer) ensureSpanTreeAccum() error {
	if w.spanTreeAccum != nil {
		return nil
	}
	a, err := newSpanTreeAccum(w.cfg.ScratchDir)
	if err != nil {
		return err
	}
	w.spanTreeAccum = a
	return nil
}

// feedSpanTreeFromAccum extracts (traceID, spanID, parentID) per row from a per-block
// intrinsic accumulator and feeds one SpanTree record per span. trace:id is always present;
// span:id and span:parent_id are present only when non-empty (root spans have no parent).
// Rows missing a span:id are skipped — a structural index entry without a span identity is
// not addressable. NOTE-462.
func feedSpanTreeFromAccum(acc *spanTreeAccum, local *intrinsicAccumulator, blockID uint16) error {
	traceCol := local.flatCols[traceIDColumnName]
	spanCol := local.flatCols[spanIDColumnName]
	if traceCol == nil || spanCol == nil {
		return nil
	}
	// Index trace:id and span:parent_id by rowIdx so they can be joined to each span:id row.
	traceByRow := make(map[uint16][]byte, len(traceCol.refs))
	for i, ref := range traceCol.refs {
		traceByRow[ref.RowIdx] = traceCol.bytesValues[i]
	}
	var parentByRow map[uint16][]byte
	if parentCol := local.flatCols[spanParentIDColumnName]; parentCol != nil {
		parentByRow = make(map[uint16][]byte, len(parentCol.refs))
		for i, ref := range parentCol.refs {
			parentByRow[ref.RowIdx] = parentCol.bytesValues[i]
		}
	}
	for i, ref := range spanCol.refs {
		tid := traceByRow[ref.RowIdx]
		if len(tid) != 16 {
			// trace:id is always 16 bytes; skip malformed rows.
			continue
		}
		var traceID [16]byte
		copy(traceID[:], tid)
		var parent []byte
		if parentByRow != nil {
			parent = parentByRow[ref.RowIdx]
		}
		if err := acc.add(traceID, spanCol.bytesValues[i], parent, blockID, ref.RowIdx); err != nil {
			return err
		}
	}
	return nil
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
	// NOTE-474: boundary-aware block slicing.
	// Flush at a (service.name, span.name) group boundary once MinBlockSpans is reached.
	// Keeps homogeneous groups in their own blocks (better dictionary/RLE compression)
	// while batching rare operations into reasonably-sized blocks. MaxBlockSpans is the
	// hard cap regardless of group boundaries.
	// NOTE-091 caveat: this splits multi-service traces across blocks; structural queries
	// may have false negatives for traces whose ancestor chain spans pruned blocks.
	var slices []blockSlice
	blockStart := 0
	for blockStart < len(w.pending) {
		blockEnd := blockStart + 1
		for blockEnd < len(w.pending) {
			size := blockEnd - blockStart
			if size >= w.cfg.MaxBlockSpans {
				break
			}
			prev := &w.pending[blockEnd-1]
			next := &w.pending[blockEnd]
			if size >= w.cfg.MinBlockSpans &&
				(next.svcName != prev.svcName || next.spanName != prev.spanName) {
				break
			}
			blockEnd++
		}
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
				emittedBlockVersion(),
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

		// Spill per-block intrinsic accumulator into the file-level on-disk accumulator,
		// then release. NOTE-461: spillMerge streams the block's rows to disk instead of
		// growing in-memory maps, bounding peak RSS to one block's worth of intrinsic data.
		if built.localAccum != nil {
			if err := w.ensureIntrinsicAccum(); err != nil {
				return fmt.Errorf("writer: intrinsic accumulator: %w", err)
			}
			if err := w.intrinsicAccum.spillMerge(built.localAccum); err != nil {
				return fmt.Errorf("writer: block %d intrinsic spill: %w", s.blockID, err)
			}
			// NOTE-462 (issue #381): feed the SpanTree accumulator from the same per-block
			// intrinsic identity columns before releasing localAccum, so structural records
			// are spilled in the same single write pass.
			if err := w.ensureSpanTreeAccum(); err != nil {
				return fmt.Errorf("writer: spantree accumulator: %w", err)
			}
			if err := feedSpanTreeFromAccum(w.spanTreeAccum, built.localAccum, uint16(s.blockID)); err != nil { //nolint:gosec // blockID bounded above by 65534
				return fmt.Errorf("writer: block %d spantree spill: %w", s.blockID, err)
			}
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
			if mm.colType == shared.ColumnTypeBool {
				// NOTE-452 (issue #373): bool min/max is tracked in colMinMax only to feed
				// the ColStats numeric [0,1] range; there is no RangeBool index type, so it
				// must be excluded from the on-disk range index.
				continue
			}
			cd, ok := w.rangeIdx[mm.colName]
			if !ok {
				cd = newRangeColumnData(mm.colType)
				w.rangeIdx[mm.colName] = cd
			}
			addBlockRangeToColumn(cd, mm, bid)
		}

		// NOTE-446: collect per-block column statistics for the ColStats section.
		if len(built.colStats) > 0 {
			w.colStatsByBlock = append(w.colStatsByBlock, shared.BlockColStats{
				BlockIdx: uint16(s.blockID), //nolint:gosec
				Cols:     built.colStats,
			})
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

// addRowCacheKey identifies a unique (reader, blockIdx) pair for the AddRowFromReader
// per-block intrinsic index cache.

// AddRowFromReader adds one row from the source block at rowIdx, reading required
// identity fields (trace:id, span:id, span:start) from the source block columns when
// present and falling back to the source reader's intrinsic section only for blocks that
// lack those columns.
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
		if entry, ok := idx.get(rowIdx); ok {
			traceBytes = entry.traceID
		}
	}
	if len(traceBytes) != 16 {
		return fmt.Errorf("writer: AddRowFromReader: trace:id missing at row %d", rowIdx)
	}

	var tid [16]byte
	copy(tid[:], traceBytes)

	// svcName and spanName come from block columns only. NOTE-471 (issue #391): the
	// intrinsic index carries identity columns exclusively (trace:id, span:id,
	// span:parent_id), so there is no index fallback for svcName — the prior fallback
	// could never hit and was removed with the per-row map.
	var svcName, spanName string
	if col := block.GetColumn(svcNameColumnName); col != nil {
		svcName, _ = col.StringValue(rowIdx)
	}
	if col := block.GetColumn(spanNameColumnName); col != nil {
		spanName, _ = col.StringValue(rowIdx)
	}

	ps := pendingSpan{
		traceID:     tid,
		svcName:     svcName,
		spanName:    spanName,
		srcBlock:    block,
		srcReader:   srcReader,
		srcBlockIdx: srcBlockIdx,
		srcRowIdx:   rowIdx,
	}
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
	return int64(len(w.pending)) * estimatedBytesPerSpan
}

// FlushedBytes returns the total number of bytes the writer has written to its
// OutputStream so far. Unlike CurrentSize — which only reflects the spans still
// pending in the in-memory buffer and resets to ~0 after each internal flush —
// this counter is monotonically increasing across the writer's whole lifetime.
//
// NOTE-458 (issue #377): callers that need to decide when to cut a new block by
// on-disk size (e.g. Tempo's block-builder via walBlock.DataLength) must use
// this, not a span-count × bytes-per-span estimate. After the WAL switched to
// counting one object per trace (TotalObjects = trace count, aligning blockpack
// with parquet's trace-based max_compaction_objects), the old
// TotalObjects × estimatedBytesPerSpan heuristic under-counted by the
// spans-per-trace factor and let blocks grow far past the configured byte limit.
func (w *Writer) FlushedBytes() int64 {
	return w.out.total
}
