// Package compaction provides blockpack file compaction. It reads spans from
// multiple blockpack files and rewrites them into fewer, well-packed output files.
package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	spanconv "github.com/mattdurham/blockpack/internal/blockio/spanconv"
	"github.com/mattdurham/blockpack/internal/blockio/writer"
)

// Config configures the compaction operation.
type Config struct {
	// StagingDir is a local directory for staging output files.
	// If empty, os.TempDir() is used.
	StagingDir string
	// MaxOutputFileSize is the maximum size in bytes of each output file.
	// Zero means no size limit.
	MaxOutputFileSize int64

	// MaxSpansPerBlock controls how many spans are written per block.
	// Defaults to 2000 if zero.
	MaxSpansPerBlock int
}

// OutputStorage provides write access for pushing output files.
type OutputStorage interface {
	// Put writes data to the given path.
	Put(path string, data []byte) error
}

// writerState holds an active output writer and its accumulated span count.
//
//nolint:govet // Field order optimized for readability
type writerState struct {
	w         *writer.Writer
	buf       *bytes.Buffer
	spanCount int
}

// compactionState holds mutable state during a single CompactBlocks call.
//
//nolint:govet // Field order optimized for readability
type compactionState struct {
	cfg         Config
	stagingDir  string
	current     *writerState
	stagedFiles []string
	maxSpans    int
	outputSeq   int
}

// CompactBlocks reads input blockpack providers, merges spans, and writes
// compacted output to outputStorage via a local staging directory.
// Returns relative paths of all output files written.
func CompactBlocks(
	ctx context.Context,
	providers []blockpackio.ReaderProvider,
	cfg Config,
	outputStorage OutputStorage,
) ([]string, error) {
	if len(providers) == 0 {
		return nil, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context canceled before compaction: %w", err)
	}

	if outputStorage == nil {
		return nil, fmt.Errorf("outputStorage cannot be nil")
	}

	stagingDir, cleanup, err := prepareStagingDir(cfg.StagingDir)
	if err != nil {
		return nil, fmt.Errorf("prepare staging dir: %w", err)
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
		outputSeq:   0,
	}

	for i, provider := range providers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("context canceled: %w", ctxErr)
		}

		if processErr := state.processProvider(provider); processErr != nil {
			return nil, fmt.Errorf("process provider %d: %w", i, processErr)
		}
	}

	if flushErr := state.flushCurrentWriter(); flushErr != nil {
		return nil, fmt.Errorf("flush final writer: %w", flushErr)
	}

	outputPaths, err := pushStagedFiles(state.stagedFiles, outputStorage)
	if err != nil {
		return nil, fmt.Errorf("push staged files: %w", err)
	}

	return outputPaths, nil
}

// processProvider feeds all spans from the given provider into the current writer,
// flushing to a new file when size limit is reached.
func (s *compactionState) processProvider(provider blockpackio.ReaderProvider) (retErr error) {
	if closer, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			if cErr := closer.Close(); cErr != nil && retErr == nil {
				retErr = fmt.Errorf("close provider: %w", cErr)
			}
		}()
	}

	r, err := reader.NewReaderFromProvider(provider)
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}

	blockCount := len(r.Blocks())
	for blockIdx := 0; blockIdx < blockCount; blockIdx++ {
		if r.IsBlockpackEntry(blockIdx) {
			subReader, subErr := r.SubReader(blockIdx)
			if subErr != nil {
				return fmt.Errorf("open sub-reader for nested block %d: %w", blockIdx, subErr)
			}

			subBlocks := subReader.Blocks()
			for subIdx := range subBlocks {
				if subReader.IsBlockpackEntry(subIdx) {
					return fmt.Errorf(
						"nested blockpack depth exceeded at block %d, sub-block %d: only 2 levels of nesting are supported",
						blockIdx,
						subIdx,
					)
				}

				subBlock, subBlockErr := subReader.GetBlock(subIdx)
				if subBlockErr != nil {
					return fmt.Errorf("get sub-block %d from nested block %d: %w", subIdx, blockIdx, subBlockErr)
				}

				if processErr := s.processBlock(subBlock); processErr != nil {
					return fmt.Errorf("process sub-block %d from nested block %d: %w", subIdx, blockIdx, processErr)
				}
			}

			continue
		}

		block, err := r.GetBlock(blockIdx)
		if err != nil {
			return fmt.Errorf("get block %d: %w", blockIdx, err)
		}

		if err := s.processBlock(block); err != nil {
			return fmt.Errorf("process block %d: %w", blockIdx, err)
		}
	}

	return nil
}

// processBlock iterates all rows in block and adds each span to the current writer.
func (s *compactionState) processBlock(block *reader.Block) error {
	spanCount := block.SpanCount()
	for rowIdx := 0; rowIdx < spanCount; rowIdx++ {
		if err := s.addSpanFromBlock(block, rowIdx); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}

	return nil
}

// addSpanFromBlock reconstructs one span from block row rowIdx and adds it
// to the current writer. Flushes and starts a new writer if size limit is reached.
func (s *compactionState) addSpanFromBlock(block *reader.Block, rowIdx int) error {
	if err := s.ensureWriter(); err != nil {
		return fmt.Errorf("ensure writer: %w", err)
	}

	sd, err := spanconv.ReconstructSpan(block, rowIdx)
	if err != nil {
		return fmt.Errorf("reconstruct span: %w", err)
	}

	if err := s.current.w.AddSpan(sd.Span, sd.ResourceAttrs, sd.Resource, sd.ResourceSchema, sd.Scope, sd.ScopeSchema); err != nil {
		return fmt.Errorf("add span: %w", err)
	}

	s.current.spanCount++

	if s.cfg.MaxOutputFileSize > 0 &&
		int64(s.current.w.CurrentSize()) >= s.cfg.MaxOutputFileSize { //nolint:gosec
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

	w, err := writer.NewWriterWithConfig(writer.WriterConfig{
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
