// Package compaction merges and deduplicates multiple modules-format blockpack files.
//
// NOTE: Core invariant — spans are copied via Writer.AddRow (native columnar path),
// not via OTLP object reconstruction. Deduplication is keyed on (trace:id, span:id).
// A source block whose trace:id/span:id identity is missing or malformed (NOTE-469
// legacy shape, or a malformed row) returns a typed error rather than being silently
// dropped — see NOTE-104 in NOTES.md. droppedSpans counts ONLY genuine duplicate
// (trace:id, span:id) drops during normal operation.
package compaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// Config configures the compaction operation.

// StagingDir is a local directory for staging output files.
// If empty, os.TempDir() is used.

// DedicatedColumns lists attribute columns to be written into the intrinsic section
// of output blocks, enabling the zero-block-read fast path for metrics queries.
// When non-empty, these columns are passed to each output Writer created during
// compaction. See writer.DedicatedColumn for documentation on the Name format.

// MaxOutputFileSize is the maximum size in bytes of each output file (estimated).
// Zero means no size limit.

// MaxSpansPerBlock controls how many spans are written per block.
// Defaults to 2000 if zero.

// OutputStorage provides write access for pushing output files.
//
// The interface is intentionally narrow (Put only) because compaction is an
// append-only operation: output blocks are written exactly once and never read
// back by the compactor itself. Keeping Delete and any read methods out of scope
// makes test doubles trivial to implement — a single-method interface requires a
// single-method fake. Using blockpack.WritableStorage here would pull in Delete,
// which compaction has no reason to call and which would widen the contract
// unnecessarily.

// writerState holds an active output writer and its accumulated span count.
//
//nolint:govet // Field order optimized for readability

// compactionState holds mutable state during a single CompactBlocks call.

// spans dropped due to missing trace:id or span:id

// CompactBlocks reads input blockpack providers, merges spans, deduplicates them,
// and writes compacted output to outputStorage.
// Returns relative paths of all output files written and the count of spans dropped
// due to genuine (trace:id, span:id) duplication. A source block with missing or
// malformed trace:id/span:id identity returns an error instead (see NOTE-104).
//
// All providers are passed already materialized; if the caller holds every input
// block fully in memory before calling this, peak memory is sum(all blocks). To
// bound peak memory to ~max(single block), use CompactBlocksStreaming with lazy
// provider factories that download just-in-time and release after consumption.
func CompactBlocks(
	ctx context.Context,
	providers []modules_rw.ReaderProvider,
	cfg Config,
	outputStorage OutputStorage,
) ([]string, int64, error) {
	// NOTE-459: Adapt pre-materialized providers to the streaming API by wrapping
	// each in a factory that returns the already-open provider. This keeps a single
	// span-merge/dedup code path; memory bounding is the caller's responsibility
	// (they already hold all providers, so peak is unchanged for this entry point).
	if len(providers) == 0 {
		return nil, 0, nil
	}
	factories := make([]ProviderFunc, len(providers))
	for i := range providers {
		p := providers[i]
		factories[i] = func() (modules_rw.ReaderProvider, error) { return p, nil }
	}
	return CompactBlocksStreaming(ctx, factories, cfg, outputStorage)
}

// ProviderFunc lazily opens a single input blockpack provider on demand.
// CompactBlocksStreaming invokes each ProviderFunc immediately before consuming its
// block and discards the returned provider before invoking the next, so the caller
// can download one block at a time and release its bytes between blocks.
type ProviderFunc func() (modules_rw.ReaderProvider, error)

// CompactBlocksStreaming merges and deduplicates spans from input blockpack files,
// consuming one provider at a time to bound peak memory.
//
// NOTE-459: Unlike CompactBlocks (which takes all providers already materialized),
// this opens each provider via its ProviderFunc just-in-time, feeds all of its spans
// into the output writer, then drops the provider reference before opening the next.
// Peak input-side memory is therefore ~max(largest single block) rather than
// sum(all blocks), regardless of how many inputs are compacted. The only state that
// spans all inputs is the dedup set of (trace:id, span:id) keys — far smaller than
// the raw block bytes — so raising the input-block count no longer scales peak memory.
func CompactBlocksStreaming(
	ctx context.Context,
	providers []ProviderFunc,
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

	for i, open := range providers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, state.droppedSpans, fmt.Errorf("context canceled: %w", ctxErr)
		}

		if procErr := openAndProcess(open, state, i); procErr != nil {
			return nil, state.droppedSpans, procErr
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

// openAndProcess opens one lazy provider, feeds its spans into state, and ensures the
// provider reference does not outlive this call so its block bytes become GC-eligible
// before the next provider is opened (the core memory-bounding guarantee of NOTE-459).
func openAndProcess(open ProviderFunc, state *compactionState, idx int) error {
	provider, err := open()
	if err != nil {
		return fmt.Errorf("open provider %d: %w", idx, err)
	}
	if provider == nil {
		return fmt.Errorf("open provider %d: nil provider", idx)
	}
	if processErr := state.processProvider(provider); processErr != nil {
		return fmt.Errorf("process provider %d: %w", idx, processErr)
	}
	return nil
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
func (s *compactionState) processBlock(r *modules_reader.Reader, blockIdx int, block *modules_reader.Block) error {
	for rowIdx := range block.SpanCount() {
		if err := s.addSpanFromBlock(r, blockIdx, block, rowIdx); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}
	return nil
}

// dedupeKey builds a 24-byte deduplication key from trace:id (16 bytes) and span:id (8 bytes).
//
// NOTE-469/#490 A-10 (re-scoped per .bob/state/identity-investigation.md): identity is
// read exclusively from block columns — every block written since the v2 self-contained
// format (issue #420) carries trace:id/span:id as block columns. The prior "intrinsic
// index" fallback (buildDedupeIndex) was provably dead: it unconditionally returned nil
// after #433 (IntrinsicTOC removal)/#434 (SpanTree removal)/#436 (intrinsic/attribute
// distinction removal), so this branch could never fire; deleted along with it.
//
// NOTE-104 (holistic-review Fix 2): mirrors writer.go's AddRowFromReader two-branch
// treatment of the identical NOTE-469 legacy condition exactly, rather than silently
// folding it into a per-row skip. A source block that entirely lacks the trace:id block
// column (the #389-#420 intrinsic-only shape) returns the shared greppable family error
// so it aborts loudly and is retried via re-compaction, instead of vanishing into the
// dedupe-drop counter indistinguishably from ordinary duplicates. A block that carries the
// trace:id column but has a malformed/absent value for this one row returns a distinct,
// row-scoped error. span:id is validated the same way (column-absent or malformed value)
// since a key cannot be built without it, but is not part of the NOTE-469 legacy-block
// family (that family is defined solely by trace:id's block-column presence, matching
// writer_block.go's buildBlock guard).
func dedupeKey(block *modules_reader.Block, rowIdx int) ([24]byte, error) {
	var key [24]byte

	traceCol := block.GetColumn("trace:id")
	if traceCol == nil {
		return key, fmt.Errorf(
			"compaction: dedupeKey: legacy intrinsic-only source block unsupported (missing trace:id block column, see NOTES.md NOTE-469) — re-compact",
		)
	}
	traceID, _ := traceCol.BytesValue(rowIdx)
	if !traceCol.IsPresent(rowIdx) || len(traceID) != 16 {
		return key, fmt.Errorf("compaction: dedupeKey: trace:id missing or malformed at row %d", rowIdx)
	}

	spanCol := block.GetColumn("span:id")
	if spanCol == nil {
		return key, fmt.Errorf("compaction: dedupeKey: span:id missing or malformed at row %d", rowIdx)
	}
	spanID, _ := spanCol.BytesValue(rowIdx)
	if !spanCol.IsPresent(rowIdx) || len(spanID) != 8 {
		return key, fmt.Errorf("compaction: dedupeKey: span:id missing or malformed at row %d", rowIdx)
	}

	copy(key[0:16], traceID)
	copy(key[16:24], spanID)
	return key, nil
}

// addSpanFromBlock adds one row from block at rowIdx to the current writer via the
// native columnar path, deduplicating by (trace:id, span:id) and respecting the
// output file size limit.
//
// NOTE-104 (holistic-review Fix 2): droppedSpans now counts ONLY genuine dedupe drops
// (the (trace:id, span:id) pair was already seen earlier in this compaction) — normal,
// expected operation. It no longer absorbs legacy-input loss: dedupeKey's two error
// branches (block-level identity absent, row-level identity malformed) propagate as
// real errors instead.
func (s *compactionState) addSpanFromBlock(
	r *modules_reader.Reader,
	blockIdx int,
	block *modules_reader.Block,
	rowIdx int,
) error {
	if err := s.ensureWriter(); err != nil {
		return fmt.Errorf("ensure writer: %w", err)
	}

	key, err := dedupeKey(block, rowIdx)
	if err != nil {
		return err
	}
	if _, seen := s.seenSpans[key]; seen {
		s.droppedSpans++
		return nil // genuine duplicate: same (trace:id, span:id) already written
	}
	s.seenSpans[key] = struct{}{}

	if err := s.current.w.AddRowFromReader(block, rowIdx, r, blockIdx); err != nil {
		return fmt.Errorf("add row: %w", err)
	}

	s.current.spanCount++

	return nil
}

// ensureWriter initializes the current writer if it is nil.
// The writer streams directly to a staging file on disk so that each internal
// block flush (triggered by the writer's MaxBufferedSpans threshold) writes to
// disk rather than to an in-memory buffer. This bounds peak output-side memory
// to ~one block's worth of pending spans regardless of total input size.
func (s *compactionState) ensureWriter() error {
	if s.current != nil {
		return nil
	}

	filename := fmt.Sprintf("compacted-%05d.blockpack", s.outputSeq)
	s.outputSeq++
	stagedPath := filepath.Join(s.stagingDir, filename)

	f, err := os.OpenFile(stagedPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec
	if err != nil {
		return fmt.Errorf("create staging file %s: %w", stagedPath, err)
	}

	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:     f,
		MaxBlockSpans:    s.maxSpans,
		DedicatedColumns: s.cfg.DedicatedColumns,
		// NOTE-461 (issue #380): keep the intrinsic spill files on the same staging volume as
		// the output block so all of compaction's disk I/O stays on the configured scratch
		// volume rather than the default /tmp.
		ScratchDir: s.stagingDir,
		// NOTE: EnableV2Format removed (2026-06-29, v2 unconditional).
	})
	if err != nil {
		_ = f.Close()
		_ = os.Remove(stagedPath)
		return fmt.Errorf("new writer: %w", err)
	}

	s.current = &writerState{w: w, f: f, stagedPath: stagedPath}
	return nil
}

// flushCurrentWriter finalizes the current writer, closing the staging file.
// The writer has been streaming blocks directly to disk throughout processing,
// so Flush() only writes any remaining pending spans. Does nothing if no writer
// is active or no spans were added.
func (s *compactionState) flushCurrentWriter() error {
	if s.current == nil || s.current.spanCount == 0 {
		if s.current != nil {
			_ = s.current.f.Close()
			_ = os.Remove(s.current.stagedPath)
		}
		s.current = nil
		return nil
	}

	if _, err := s.current.w.Flush(); err != nil {
		_ = s.current.f.Close()
		return fmt.Errorf("flush writer: %w", err)
	}

	// Optional inline value-index sink: open the staged file as a Reader and call the sink
	// before closing the file so callers can extract value-index entries synchronously.
	if s.cfg.ValueIndexSink != nil {
		if err := s.callValueIndexSink(s.current.stagedPath); err != nil {
			_ = s.current.f.Close()
			return fmt.Errorf("value index sink: %w", err)
		}
	}

	if err := s.current.f.Close(); err != nil {
		return fmt.Errorf("close staging file: %w", err)
	}

	stagedPath := s.current.stagedPath

	s.stagedFiles = append(s.stagedFiles, stagedPath)
	s.current = nil
	return nil
}

// callValueIndexSink reads the staged blockpack file, opens a Reader, and calls cfg.ValueIndexSink.
func (s *compactionState) callValueIndexSink(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return fmt.Errorf("read staged file for value index sink: %w", err)
	}
	provider := modules_rw.NewBytesProvider(data)
	r, err := modules_reader.NewReaderFromProvider(provider)
	if err != nil {
		return fmt.Errorf("open reader for value index sink: %w", err)
	}
	return s.cfg.ValueIndexSink(r)
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
