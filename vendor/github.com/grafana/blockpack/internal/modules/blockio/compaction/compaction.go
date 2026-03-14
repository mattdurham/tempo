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
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

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

	allBlocks := make([]int, r.BlockCount())
	for i := range allBlocks {
		allBlocks[i] = i
	}
	for _, group := range r.CoalescedGroups(allBlocks) {
		rawMap, fetchErr := r.ReadGroup(group)
		if fetchErr != nil {
			return fmt.Errorf("read group: %w", fetchErr)
		}
		for _, blockIdx := range group.BlockIDs {
			raw, ok := rawMap[blockIdx]
			if !ok {
				continue
			}
			bwb, parseErr := r.ParseBlockFromBytes(raw, nil, r.BlockMeta(blockIdx))
			if parseErr != nil {
				return fmt.Errorf("parse block %d: %w", blockIdx, parseErr)
			}
			if processErr := s.processBlock(bwb.Block); processErr != nil {
				return fmt.Errorf("process block %d: %w", blockIdx, processErr)
			}
		}
	}

	return nil
}

// processBlock iterates all rows in block and adds each span to the current writer.
func (s *compactionState) processBlock(block *modules_reader.Block) error {
	for rowIdx := range block.SpanCount() {
		if err := s.addSpanFromBlock(block, rowIdx); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}
	return nil
}

// dedupeKey builds a 24-byte deduplication key from trace:id (16 bytes) and span:id (8 bytes).
// Returns the key and true if both IDs are present and non-empty; false otherwise.
func dedupeKey(block *modules_reader.Block, rowIdx int) ([24]byte, bool) {
	var key [24]byte

	traceCol := block.GetColumn("trace:id")
	if traceCol == nil || !traceCol.IsPresent(rowIdx) {
		return key, false
	}
	traceID, ok := traceCol.BytesValue(rowIdx)
	if !ok || len(traceID) != 16 {
		return key, false
	}

	spanCol := block.GetColumn("span:id")
	if spanCol == nil || !spanCol.IsPresent(rowIdx) {
		return key, false
	}
	spanID, ok := spanCol.BytesValue(rowIdx)
	if !ok || len(spanID) != 8 {
		return key, false
	}

	copy(key[0:16], traceID)
	copy(key[16:24], spanID)
	return key, true
}

// addSpanFromBlock adds one row from block at rowIdx to the current writer via the
// native columnar path, deduplicating by (trace:id, span:id) and respecting the
// output file size limit.
func (s *compactionState) addSpanFromBlock(block *modules_reader.Block, rowIdx int) error {
	if err := s.ensureWriter(); err != nil {
		return fmt.Errorf("ensure writer: %w", err)
	}

	key, ok := dedupeKey(block, rowIdx)
	if !ok {
		s.droppedSpans++
		return nil // skip rows without valid trace:id or span:id
	}
	if _, seen := s.seenSpans[key]; seen {
		return nil
	}
	s.seenSpans[key] = struct{}{}

	if err := s.current.w.AddRow(block, rowIdx); err != nil {
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
