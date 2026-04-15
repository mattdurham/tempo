// Package compaction merges and deduplicates multiple modules-format blockpack files.
//
// NOTE: Core invariant — spans are copied via Writer.AddRow (native columnar path),
// not via OTLP object reconstruction. Deduplication is keyed on (trace:id, span:id).
// Spans with missing or invalid IDs are silently dropped and counted in droppedSpans.
package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// blockIDPair holds the pre-fetched trace:id and span:id for a single row.
// Built once per block by buildDedupeIndex; looked up O(1) by dedupeKey.
type blockIDPair struct {
	traceID []byte
	spanID  []byte
}

// buildDedupeIndex builds a per-row deduplication index for the given (reader, blockIdx) pair.
// Iterates the trace:id and span:id intrinsic columns once (O(N)), eliminating the
// O(N) per-row linear scan that IntrinsicBytesAt performs.
// Returns nil when r is nil or has no intrinsic section (callers fall back to block columns only).
func buildDedupeIndex(r *modules_reader.Reader, blockIdx int) map[uint16]blockIDPair {
	if r == nil {
		return nil
	}
	names := r.IntrinsicColumnNames()
	if len(names) == 0 {
		return nil
	}
	out := make(map[uint16]blockIDPair)
	for _, colName := range []string{"trace:id", "span:id"} {
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			continue
		}
		if col.Format != modules_shared.IntrinsicFormatFlat {
			continue
		}
		for i, ref := range col.BlockRefs {
			if int(ref.BlockIdx) != blockIdx {
				continue
			}
			if i >= len(col.BytesValues) {
				continue
			}
			entry := out[ref.RowIdx]
			if colName == "trace:id" {
				entry.traceID = col.BytesValues[i]
			} else {
				entry.spanID = col.BytesValues[i]
			}
			out[ref.RowIdx] = entry
		}
	}
	return out
}

// Config configures the compaction operation.
type Config struct {
	// StagingDir is a local directory for staging output files.
	// If empty, os.TempDir() is used.
	StagingDir string
	// MaxOutputFileSize is the maximum size in bytes of each output file (estimated).
	// Zero means no size limit.
	MaxOutputFileSize int64
	// MaxSpansPerBlock controls how many spans are written per block.
	// Defaults to 2000 if zero.
	MaxSpansPerBlock int
}

// OutputStorage provides write access for pushing output files.
//
// The interface is intentionally narrow (Put only) because compaction is an
// append-only operation: output blocks are written exactly once and never read
// back by the compactor itself. Keeping Delete and any read methods out of scope
// makes test doubles trivial to implement — a single-method interface requires a
// single-method fake. Using blockpack.WritableStorage here would pull in Delete,
// which compaction has no reason to call and which would widen the contract
// unnecessarily.
type OutputStorage interface {
	Put(path string, data []byte) error
}

// writerState holds an active output writer and its accumulated span count.
//
//nolint:govet // Field order optimized for readability
type writerState struct {
	w         *modules_blockio.Writer
	buf       *bytes.Buffer
	spanCount int
}

// compactionState holds mutable state during a single CompactBlocks call.
type compactionState struct {
	current      *writerState
	stagingDir   string
	stagedFiles  []string
	seenSpans    map[[24]byte]struct{}
	cfg          Config
	maxSpans     int
	outputSeq    int
	droppedSpans int64 // spans dropped due to missing trace:id or span:id
}

// CompactBlocks reads input blockpack providers, merges spans, deduplicates them,
// and writes compacted output to outputStorage.
// Returns relative paths of all output files written and the count of spans dropped
// due to missing trace:id or span:id columns.
func CompactBlocks(
	ctx context.Context,
	providers []modules_rw.ReaderProvider,
	cfg Config,
	outputStorage OutputStorage,
) ([]string, int64, error) {
	if len(providers) == 0 {
		return nil, 0, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, fmt.Errorf("context canceled before compaction: %w", err)
	}

	if outputStorage == nil {
		return nil, 0, fmt.Errorf("outputStorage cannot be nil")
	}

	stagingDir, cleanup, err := prepareStagingDir(cfg.StagingDir)
	if err != nil {
		return nil, 0, fmt.Errorf("prepare staging dir: %w", err)
	}
	defer cleanup()

	maxSpansPerBlock := cfg.MaxSpansPerBlock
	if maxSpansPerBlock <= 0 {
		maxSpansPerBlock = 2000
	}

	state := &compactionState{
		cfg:         cfg,
		stagingDir:  stagingDir,
		maxSpans:    maxSpansPerBlock,
		stagedFiles: make([]string, 0),
		seenSpans:   make(map[[24]byte]struct{}),
	}

	for i, provider := range providers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, state.droppedSpans, fmt.Errorf("context canceled: %w", ctxErr)
		}

		if processErr := state.processProvider(provider); processErr != nil {
			return nil, state.droppedSpans, fmt.Errorf("process provider %d: %w", i, processErr)
		}
	}

	if flushErr := state.flushCurrentWriter(); flushErr != nil {
		return nil, state.droppedSpans, fmt.Errorf("flush final writer: %w", flushErr)
	}

	outputPaths, err := pushStagedFiles(state.stagedFiles, outputStorage)
	if err != nil {
		return nil, state.droppedSpans, fmt.Errorf("push staged files: %w", err)
	}

	return outputPaths, state.droppedSpans, nil
}

// processProvider feeds all spans from the given provider into the current writer.
func (s *compactionState) processProvider(provider modules_rw.ReaderProvider) (retErr error) {
	if closer, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			if cErr := closer.Close(); cErr != nil && retErr == nil {
				retErr = fmt.Errorf("close provider: %w", cErr)
			}
		}()
	}

	r, err := modules_reader.NewReaderFromProvider(provider)
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}

	for blockIdx := range r.BlockCount() {
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil)
		if getErr != nil {
			return fmt.Errorf("get block %d: %w", blockIdx, getErr)
		}
		if bwb == nil {
			continue
		}

		if processErr := s.processBlock(r, blockIdx, bwb.Block); processErr != nil {
			return fmt.Errorf("process block %d: %w", blockIdx, processErr)
		}
	}

	return nil
}

// processBlock iterates all rows in block and adds each span to the current writer.
// Builds a per-block deduplication index once (O(N)) to avoid O(N^2) IntrinsicBytesAt
// calls across the per-row loop.
func (s *compactionState) processBlock(r *modules_reader.Reader, blockIdx int, block *modules_reader.Block) error {
	// Build intrinsic ID index once per block; O(N) over intrinsic columns.
	// dedupeKey uses this for O(1) per-row lookups instead of O(N) linear scans.
	idIndex := buildDedupeIndex(r, blockIdx)
	for rowIdx := range block.SpanCount() {
		if err := s.addSpanFromBlock(r, blockIdx, block, rowIdx, idIndex); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}
	return nil
}

// dedupeKey builds a 24-byte deduplication key from trace:id (16 bytes) and span:id (8 bytes).
// Falls back to the pre-built idIndex (O(1) lookup) when block columns are absent (new storage
// format where intrinsic columns live exclusively in the intrinsic section).
// idIndex is built once per block by buildDedupeIndex; pass nil to disable intrinsic fallback.
// Returns the key and true if both IDs are present and non-empty; false otherwise.
func dedupeKey(block *modules_reader.Block, rowIdx int, idIndex map[uint16]blockIDPair) ([24]byte, bool) {
	var key [24]byte

	// PATTERN: block-column-first with intrinsic-section fallback (shared across
	// compaction.go, writer/writer.go, executor/executor.go, executor/metrics_trace.go).
	// v3 files store identity columns in block payloads; v4 files store them exclusively
	// in the intrinsic section. Try the block column first for backwards compat.

	// trace:id — try block column first, then O(1) index lookup.
	var traceID []byte
	if traceCol := block.GetColumn("trace:id"); traceCol != nil && traceCol.IsPresent(rowIdx) {
		traceID, _ = traceCol.BytesValue(rowIdx)
	} else if idIndex != nil {
		traceID = idIndex[uint16(rowIdx)].traceID //nolint:gosec // rowIdx bounded by SpanCount (≤65535)
	}
	if len(traceID) != 16 {
		return key, false
	}

	// span:id — try block column first, then O(1) index lookup.
	var spanID []byte
	if spanCol := block.GetColumn("span:id"); spanCol != nil && spanCol.IsPresent(rowIdx) {
		spanID, _ = spanCol.BytesValue(rowIdx)
	} else if idIndex != nil {
		spanID = idIndex[uint16(rowIdx)].spanID //nolint:gosec // rowIdx bounded by SpanCount (≤65535)
	}
	if len(spanID) != 8 {
		return key, false
	}

	copy(key[0:16], traceID)
	copy(key[16:24], spanID)
	return key, true
}

// addSpanFromBlock adds one row from block at rowIdx to the current writer via the
// native columnar path, deduplicating by (trace:id, span:id) and respecting the
// output file size limit. idIndex is the pre-built per-block dedup index (may be nil).
func (s *compactionState) addSpanFromBlock(
	r *modules_reader.Reader,
	blockIdx int,
	block *modules_reader.Block,
	rowIdx int,
	idIndex map[uint16]blockIDPair,
) error {
	if err := s.ensureWriter(); err != nil {
		return fmt.Errorf("ensure writer: %w", err)
	}

	key, ok := dedupeKey(block, rowIdx, idIndex)
	if !ok {
		s.droppedSpans++
		return nil // skip rows without valid trace:id or span:id
	}
	if _, seen := s.seenSpans[key]; seen {
		return nil
	}
	s.seenSpans[key] = struct{}{}

	if err := s.current.w.AddRowFromReader(block, rowIdx, r, blockIdx); err != nil {
		return fmt.Errorf("add row: %w", err)
	}

	s.current.spanCount++

	if s.cfg.MaxOutputFileSize > 0 &&
		s.current.w.CurrentSize() >= s.cfg.MaxOutputFileSize {
		if err := s.flushCurrentWriter(); err != nil {
			return fmt.Errorf("flush on size limit: %w", err)
		}
	}

	return nil
}

// ensureWriter initializes the current writer if it is nil.
func (s *compactionState) ensureWriter() error {
	if s.current != nil {
		return nil
	}

	buf := &bytes.Buffer{}
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  buf,
		MaxBlockSpans: s.maxSpans,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	s.current = &writerState{w: w, buf: buf}
	return nil
}

// flushCurrentWriter flushes the current writer to the staging directory.
// Does nothing if no writer is active or no spans were added.
func (s *compactionState) flushCurrentWriter() error {
	if s.current == nil || s.current.spanCount == 0 {
		s.current = nil
		return nil
	}

	if _, err := s.current.w.Flush(); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}

	data := s.current.buf.Bytes()
	filename := fmt.Sprintf("compacted-%05d.blockpack", s.outputSeq)
	s.outputSeq++
	stagedPath := filepath.Join(s.stagingDir, filename)

	if err := os.WriteFile(stagedPath, data, 0o600); err != nil { //nolint:gosec
		return fmt.Errorf("write staged file %s: %w", stagedPath, err)
	}

	s.stagedFiles = append(s.stagedFiles, stagedPath)
	s.current = nil
	return nil
}

// prepareStagingDir creates a unique subdirectory for staging compaction output.
// Returns the directory path and a cleanup function to remove it on completion.
func prepareStagingDir(baseDir string) (string, func(), error) {
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	dir, err := os.MkdirTemp(baseDir, "blockpack-compaction-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create staging dir under %s: %w", baseDir, err)
	}

	cleanup := func() { _ = os.RemoveAll(dir) }
	return dir, cleanup, nil
}

// pushStagedFiles reads each staged file and pushes it to outputStorage.
// Returns the relative output paths (just the filenames).
func pushStagedFiles(stagedPaths []string, output OutputStorage) ([]string, error) {
	outputPaths := make([]string, 0, len(stagedPaths))

	for _, stagedPath := range stagedPaths {
		data, err := os.ReadFile(stagedPath) //nolint:gosec
		if err != nil {
			return nil, fmt.Errorf("read staged file %s: %w", stagedPath, err)
		}

		relPath := filepath.Base(stagedPath)
		if err := output.Put(relPath, data); err != nil {
			return nil, fmt.Errorf("push file %s: %w", relPath, err)
		}

		outputPaths = append(outputPaths, relPath)
	}

	return outputPaths, nil
}
