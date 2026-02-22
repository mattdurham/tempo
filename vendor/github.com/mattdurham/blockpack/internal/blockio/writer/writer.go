package writer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/arena"
)

// Metric stream types are now in writer_metrics.go

// WriterConfig configures Writer behavior.
type WriterConfig struct { //nolint:revive
	// OutputStream enables streaming mode: blocks are written immediately instead of accumulated
	OutputStream  io.Writer
	MaxBlockSpans int
	// MinHashCacheSize controls the max number of token sets cached for MinHash.
	// Set to 0 for default sizing, or negative to disable caching.
	MinHashCacheSize int
}

// blockReference holds lightweight metadata for a block in streaming mode
type blockReference struct {
	meta   blockIndexEntry // Block metadata (bloom, timestamps, etc)
	offset int64           // File offset where block data starts
	length int64           // Length of serialized block data
}

// Writer builds a multi-block blockpack file in-memory.
//
// Thread Safety: Writer is NOT thread-safe. All methods (including AddSpan) must be called
// from a single goroutine. Concurrent calls to AddSpan or other methods will cause data races
// and may result in panics or corrupted data, especially when using metric streams.
//
//nolint:govet // Field order optimized for readability over memory alignment
type Writer struct {
	// Streaming mode fields
	output       io.Writer     // Output stream for immediate writes
	outputBuffer *bufio.Writer // Buffered wrapper around output (streaming mode only)
	current      *blockBuilder
	// Reusable zstd encoder (128 MB, reused for all blocks to avoid pool accumulation)
	zstdEncoder *zstd.Encoder
	// dedicated column index: column -> encoded value -> set of block IDs
	dedicatedIndex map[string]map[string]map[int]struct{}
	// UUID column tracking: column name -> detected as UUID
	uuidColumns map[string]bool
	// UUID column sampling: column name -> sample values for detection
	uuidSamples map[string][]string

	// MinHash cache to reduce repeated hashing for identical token sets
	minhashCache *MinHashCache
	// Reusable sorting buckets for two-level sorting
	sortBuckets map[string][]*BufferedSpan
	// Scratch map for deduplicating attribute keys without per-span allocation
	attrKeyScratch map[string]struct{}

	// Min/max value per block for range-bucketed columns (O(blocks) memory)
	// Used by high-cardinality bucketing to assign blocks to all buckets they span
	rangeBlockMinMax map[string]map[int][2]int64
	// KLL sketches for range bucketing (uses O(k + log n) memory, provably optimal)
	// Maps column name -> KLL sketch
	kllSketches             map[string]*KLL[int64]
	kllFloat64Sketches      map[string]*KLL[float64]
	kllStringSketches       map[string]*KLL[string] // used for both string and bytes columns
	rangeFloat64BlockMinMax map[string]map[int][2]float64
	rangeStringBlockMinMax  map[string]map[int][2]string
	// Computed bucket metadata: column -> metadata (for int64/uint64 range columns)
	rangeBucketMeta map[string]*RangeBucketMetadata
	// Range metadata for float64/bytes/string range columns
	rangeMetadata *RangeMetadata
	// Pre-computed bucket boundaries from reservoir sampling (column -> boundaries)
	precomputedBoundaries map[string][]int64
	// Bucket index for pre-computed boundaries (column -> bucket_id -> block_ids)
	rangeBucketIndex map[string]map[uint16]map[int]struct{}
	// Auto-detected dedicated column types (for columns not in explicit list)
	autoDedicatedTypes map[string]ColumnType

	// Trace index for fast trace-by-ID lookups
	// Maps trace ID -> block ID -> span indices within that block
	traceBlockIndex map[[16]byte]map[int][]uint16

	statsCollector *blockStatsCollector // Current block's stats collector
	blockRefs      []*blockReference    // Lightweight block references (streaming mode)

	// Span buffering for sorting (added for clustering)
	spanBuffer   []*BufferedSpan
	sortServices []string

	// Metric stream support
	metricStreams []*MetricStreamBuilder

	// Value statistics collection (v10 feature)
	config WriterConfig
	// Arena for MinHash token string construction during sorting
	sortArena arena.Arena

	maxBlockSpans int
	currentPos    int64 // Current write position in output stream
	totalSpans    int
	version       uint8 // File format version (v10/v11)
}

// NewWriterWithConfig creates a new blockpack writer with custom configuration.
func NewWriterWithConfig(config WriterConfig) (*Writer, error) {
	if config.OutputStream == nil {
		return nil, fmt.Errorf("WriterConfig.OutputStream is required (streaming-only mode)")
	}
	if config.MaxBlockSpans <= 0 {
		config.MaxBlockSpans = 2000 // Default: 2000 spans per block
	}
	// CRITICAL: Cap MaxBlockSpans at math.MaxUint16 (65535) because span indices are stored as uint16
	// in the trace block index. Values larger than 65535 would silently truncate causing corrupt index entries.
	if config.MaxBlockSpans > 65535 {
		config.MaxBlockSpans = 65535
	}
	if config.MinHashCacheSize == 0 {
		config.MinHashCacheSize = 5000
	}
	if config.MinHashCacheSize < 0 {
		config.MinHashCacheSize = 0
	}

	// Use v11 format (current version)
	version := versionV11

	// Create reusable zstd encoder (128 MB, reused for all blocks)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	w := &Writer{
		maxBlockSpans:           config.MaxBlockSpans,
		dedicatedIndex:          make(map[string]map[string]map[int]struct{}),
		uuidColumns:             make(map[string]bool),
		uuidSamples:             make(map[string][]string),
		spanBuffer:              make([]*BufferedSpan, 0, 100000), // Buffer up to 100K spans
		minhashCache:            NewMinHashCache(config.MinHashCacheSize),
		sortBuckets:             make(map[string][]*BufferedSpan),
		attrKeyScratch:          make(map[string]struct{}),
		rangeBlockMinMax:        make(map[string]map[int][2]int64),
		kllSketches:             make(map[string]*KLL[int64]),
		kllFloat64Sketches:      make(map[string]*KLL[float64]),
		kllStringSketches:       make(map[string]*KLL[string]),
		rangeFloat64BlockMinMax: make(map[string]map[int][2]float64),
		rangeStringBlockMinMax:  make(map[string]map[int][2]string),
		rangeBucketMeta:         make(map[string]*RangeBucketMetadata),
		rangeMetadata:           NewRangeMetadata(),
		precomputedBoundaries:   make(map[string][]int64),
		rangeBucketIndex:        make(map[string]map[uint16]map[int]struct{}),
		autoDedicatedTypes:      make(map[string]ColumnType),
		traceBlockIndex:         make(map[[16]byte]map[int][]uint16),
		output:                  config.OutputStream,
		blockRefs:               make([]*blockReference, 0),
		currentPos:              0,
		zstdEncoder:             encoder,
		config:                  config,
		version:                 version,
	}

	// Wrap output stream in a 64 KB buffered writer (streaming mode only).
	// This eliminates per-block Write syscalls; a single Sync() at end of
	// flushStreaming() provides durability.
	w.outputBuffer = bufio.NewWriterSize(config.OutputStream, 64*1024)

	// Pre-set column types for special columns that need specific range types
	// span:duration must be ColumnTypeRangeDuration (not RangeInt64) for proper query filtering
	w.autoDedicatedTypes["span:duration"] = ColumnTypeRangeDuration
	// span:start and span:end are high-cardinality timestamp fields - always use range types
	w.autoDedicatedTypes["span:start"] = ColumnTypeRangeUint64
	w.autoDedicatedTypes["span:end"] = ColumnTypeRangeUint64

	// Initialize stats collector to automatically track all columns (v11 format)
	w.statsCollector = newBlockStatsCollector()

	return w, nil
}

// Helper functions (isUUID, parseUUID, recordDedicatedValue, etc.) are now in writer_helpers.go
// Span processing functions (AddTracesData, addSpan, setSpan*, addSpanToBlock, etc.) are now in writer_span.go
// Buffer and sorting functions (closeCurrentBlock, sortSpanBuffer) are now in writer_buffers.go

// CurrentSize returns an estimate of the current size in bytes.
func (w *Writer) CurrentSize() int {
	if w == nil {
		return 0
	}
	return w.totalSpans * 2048
}

// Column operations (setFromAnyValue, setInstrumentation/Event/Link, set*, getOrCreateColumn) are now in writer_columns.go

// Flush writes all buffered spans to blocks and returns the serialized blockpack data.
func (w *Writer) Flush() ([]byte, error) {
	if w == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	// Step 1: Sort buffered spans
	// Compute sort keys
	w.sortArena.Free()
	for _, buffered := range w.spanBuffer {
		buffered.ComputeSortKey(w.minhashCache, &w.sortArena)
	}
	w.sortArena.Free()

	// Sort: primary by service.name, secondary by MinHash
	w.sortSpanBuffer(w.spanBuffer)

	// Step 2: Write sorted spans to blocks
	w.current = nil
	w.dedicatedIndex = make(map[string]map[string]map[int]struct{}) // Clear dedicated index
	w.traceBlockIndex = make(map[[16]byte]map[int][]uint16)         // Clear trace block index

	for _, buffered := range w.spanBuffer {
		// Ensure block exists
		if w.current == nil {
			w.current = newBlockBuilder()
		}

		// Add span to current block
		if err := w.addSpanToBlock(w.current, buffered); err != nil {
			return nil, fmt.Errorf("add span to block: %w", err)
		}

		// Close block if full
		if w.current.spanCount >= w.maxBlockSpans {
			if err := w.closeCurrentBlock(); err != nil {
				return nil, fmt.Errorf("close block: %w", err)
			}
		}
	}

	// Close final block
	if err := w.closeCurrentBlock(); err != nil {
		return nil, fmt.Errorf("close final block: %w", err)
	}

	// Step 3: Compute range buckets and build bucket index
	if err := w.computeRangeBuckets(); err != nil {
		return nil, fmt.Errorf("compute range buckets: %w", err)
	}

	// Step 4: Write metadata, header, and footer for the streaming output.
	return w.flushStreaming()
}

// flushStreaming writes metadata, header, and footer for streaming mode
// File structure: [blocks (already written)] [metadata] [header] [footer]
func (w *Writer) flushStreaming() (_ []byte, retErr error) {
	defer func() {
		if retErr != nil {
			_ = w.outputBuffer.Flush() // best-effort: push buffered bytes before returning error
		}
	}()
	// Build block index from blockRefs
	blockIndex := make([]blockIndexEntry, len(w.blockRefs))
	for i, ref := range w.blockRefs {
		blockIndex[i] = ref.meta
		//nolint:gosec
		blockIndex[i].Offset = uint64(ref.offset)
		//nolint:gosec
		blockIndex[i].Length = uint64(ref.length)
	}

	// Build dedicated index buffer
	dedicatedBuf, err := buildDedicatedIndexBuffer(
		w.dedicatedIndex,
		w.rangeBucketMeta,
		w.autoDedicatedTypes,
		w.rangeMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("build dedicated index: %w", err)
	}

	// Build trace index buffer
	traceIndexBuf, err := buildTraceBlockIndexBuffer(w.traceBlockIndex)
	if err != nil {
		return nil, fmt.Errorf("build trace index: %w", err)
	}

	// Build aggregate blocks if needed
	var aggregateBytes []byte
	if len(w.metricStreams) > 0 {
		aggBytes, err := w.BuildMetricStreamBlocks() //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("build aggregate blocks: %w", err)
		}
		aggregateBytes = aggBytes
	}

	// Write metadata section
	metadataBuf := &bytes.Buffer{}

	// Write block index
	if err := binary.Write(metadataBuf, binary.LittleEndian, uint32(len(blockIndex))); err != nil { //nolint:gosec,govet
		return nil, fmt.Errorf("write block index count: %w", err)
	}
	for _, entry := range blockIndex {
		if err := entry.write(metadataBuf, w.version); err != nil { //nolint:govet
			return nil, err
		}
	}

	// Write dedicated index
	if _, err := metadataBuf.Write(dedicatedBuf.Bytes()); err != nil { //nolint:govet
		return nil, err
	}

	// CRITICAL: Write column index for selective I/O (must match non-streaming order)
	if err := writeColumnIndex(metadataBuf, blockIndex); err != nil { //nolint:govet
		return nil, fmt.Errorf("write column index: %w", err)
	}

	// Write trace index
	if _, err := metadataBuf.Write(traceIndexBuf.Bytes()); err != nil { //nolint:govet
		return nil, err
	}

	// Write metadata section (without aggregates)
	metadataOffset := w.currentPos
	metadataBytes := metadataBuf.Bytes()
	metadataLen := uint64(len(metadataBytes))
	if _, err := w.outputBuffer.Write(metadataBytes); err != nil { //nolint:govet
		return nil, fmt.Errorf("write metadata: %w", err)
	}
	w.currentPos += int64(len(metadataBytes))

	// Write aggregate blocks separately after metadata
	//nolint:gosec
	aggregateOffset := uint64(w.currentPos)
	aggregateLen := uint64(len(aggregateBytes))
	if len(aggregateBytes) > 0 {
		if _, err := w.outputBuffer.Write(aggregateBytes); err != nil { //nolint:govet
			return nil, fmt.Errorf("write aggregates: %w", err)
		}
		w.currentPos += int64(len(aggregateBytes))
	}

	// Write header: magic(4) + version(1) + metadata_offset(8) + metadata_len(8) + aggregate_offset(8) + aggregate_len(8) = 37 bytes
	headerOffset := w.currentPos
	headerBuf := &bytes.Buffer{}
	if err := binary.Write(headerBuf, binary.LittleEndian, magicNumber); err != nil { //nolint:govet
		return nil, fmt.Errorf("write header magic: %w", err)
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, w.version); err != nil { //nolint:govet
		return nil, err
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, uint64(metadataOffset)); err != nil { //nolint:gosec,govet
		return nil, err
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, metadataLen); err != nil { //nolint:govet
		return nil, err
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, aggregateOffset); err != nil { //nolint:govet
		return nil, err
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, aggregateLen); err != nil { //nolint:govet
		return nil, err
	}
	if _, err := w.outputBuffer.Write(headerBuf.Bytes()); err != nil { //nolint:govet
		return nil, fmt.Errorf("write header: %w", err)
	}
	w.currentPos += int64(headerBuf.Len())

	// Write footer (streaming format: version + offset)
	footerBuf := &bytes.Buffer{}
	// Write footer version (2 bytes)
	if err := binary.Write(footerBuf, binary.LittleEndian, FooterVersion); err != nil { //nolint:govet
		return nil, fmt.Errorf("write footer version: %w", err)
	}
	// Write header offset (8 bytes)
	if err := binary.Write(footerBuf, binary.LittleEndian, uint64(headerOffset)); err != nil { //nolint:gosec,govet
		return nil, fmt.Errorf("write footer offset: %w", err)
	}

	footerSize, err := w.outputBuffer.Write(footerBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("write footer: %w", err)
	}
	w.currentPos += int64(footerSize)

	// Flush bufio.Writer to push all buffered bytes to the underlying writer
	// before issuing the single Sync() call that ensures durability.
	if err := w.outputBuffer.Flush(); err != nil {
		return nil, fmt.Errorf("flush output buffer: %w", err)
	}
	// One Sync() per Flush() -- not per block (IOPS optimization).
	if syncer, ok := w.output.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return nil, fmt.Errorf("sync output: %w", err)
		}
	}

	// Clear span buffer and reset
	w.spanBuffer = w.spanBuffer[:0]
	w.totalSpans = 0

	// In streaming mode, return a marker with the total bytes written
	// The actual data is already in the output stream
	marker := []byte(fmt.Sprintf("STREAM:%d", w.currentPos))
	return marker, nil
}

// Block building and serialization functions are now in writer_blocks.go
// Metadata helpers (writeDedicatedValue, buildDedicatedIndexBuffer, buildTraceBlockIndexBuffer, writeFooter) are now in writer_metadata.go
// Aggregate serialization types and functions are now in writer_aggregate.go

// buildTraceTable serializes trace-level columns and returns the trace table data plus stats.
// The encoder is passed through to column builders for dictionary compression.
