// Package compaction merges and deduplicates multiple modules-format blockpack files.
package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
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
	current     *writerState
	stagingDir  string
	stagedFiles []string
	seenSpans   map[[24]byte]struct{}
	cfg         Config
	maxSpans    int
	outputSeq   int
}

// CompactBlocks reads input blockpack providers, merges spans, deduplicates them,
// and writes compacted output to outputStorage.
// Returns relative paths of all output files written.
func CompactBlocks(
	ctx context.Context,
	providers []modules_shared.ReaderProvider,
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
		seenSpans:   make(map[[24]byte]struct{}),
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

// processProvider feeds all spans from the given provider into the current writer.
func (s *compactionState) processProvider(provider modules_shared.ReaderProvider) (retErr error) {
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
		bwb, getErr := r.GetBlockWithBytes(blockIdx, nil, nil)
		if getErr != nil {
			return fmt.Errorf("get block %d: %w", blockIdx, getErr)
		}
		if bwb == nil {
			continue
		}

		if processErr := s.processBlock(bwb.Block); processErr != nil {
			return fmt.Errorf("process block %d: %w", blockIdx, processErr)
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

// addSpanFromBlock reconstructs one span from block row rowIdx and adds it
// to the current writer, flushing to a new file if the size limit is reached.
func (s *compactionState) addSpanFromBlock(block *modules_reader.Block, rowIdx int) error {
	if err := s.ensureWriter(); err != nil {
		return fmt.Errorf("ensure writer: %w", err)
	}

	traceID, span, resAttrs, resSchemaURL, scopeAttrs, scopeSchemaURL := spanFromRow(block, rowIdx)
	if len(traceID) == 0 || len(span.SpanId) == 0 {
		return nil // skip rows without valid IDs
	}

	var key [24]byte
	copy(key[0:16], traceID)
	copy(key[16:24], span.SpanId)
	if _, seen := s.seenSpans[key]; seen {
		return nil
	}
	s.seenSpans[key] = struct{}{}

	if err := s.current.w.AddSpan(traceID, span, resAttrs, resSchemaURL, scopeAttrs, scopeSchemaURL); err != nil {
		return fmt.Errorf("add span: %w", err)
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

// spanFromRow reconstructs span data from all columns at rowIdx.
func spanFromRow(
	block *modules_reader.Block,
	rowIdx int,
) (traceID []byte, span *tracev1.Span, resAttrs map[string]any, resSchemaURL string, scopeAttrs map[string]any, scopeSchemaURL string) {
	span = &tracev1.Span{}
	resAttrs = make(map[string]any)
	scopeAttrs = make(map[string]any)

	for name, col := range block.Columns() {
		if !col.IsPresent(rowIdx) {
			continue
		}
		switch name {
		case "trace:id":
			if v, ok := col.BytesValue(rowIdx); ok {
				traceID = cloneBytes(v)
			}
		case "span:id":
			if v, ok := col.BytesValue(rowIdx); ok {
				span.SpanId = cloneBytes(v)
			}
		case "span:parent_id":
			if v, ok := col.BytesValue(rowIdx); ok {
				span.ParentSpanId = cloneBytes(v)
			}
		case "span:name":
			if v, ok := col.StringValue(rowIdx); ok {
				span.Name = v
			}
		case "span:kind":
			if v, ok := col.Int64Value(rowIdx); ok {
				span.Kind = tracev1.Span_SpanKind(v) //nolint:gosec
			}
		case "span:start":
			if v, ok := col.Uint64Value(rowIdx); ok {
				span.StartTimeUnixNano = v
			}
		case "span:end":
			if v, ok := col.Uint64Value(rowIdx); ok {
				span.EndTimeUnixNano = v
			}
		case "span:duration":
			// computed from start/end; skip
		case "span:status":
			if v, ok := col.Int64Value(rowIdx); ok {
				if span.Status == nil {
					span.Status = &tracev1.Status{}
				}
				span.Status.Code = tracev1.Status_StatusCode(v) //nolint:gosec
			}
		case "span:status_message":
			if v, ok := col.StringValue(rowIdx); ok {
				if span.Status == nil {
					span.Status = &tracev1.Status{}
				}
				span.Status.Message = v
			}
		case "trace:state":
			if v, ok := col.StringValue(rowIdx); ok {
				span.TraceState = v
			}
		case "resource:schema_url":
			if v, ok := col.StringValue(rowIdx); ok {
				resSchemaURL = v
			}
		case "scope:schema_url":
			if v, ok := col.StringValue(rowIdx); ok {
				scopeSchemaURL = v
			}
		default:
			v := columnAny(col, rowIdx)
			if v == nil {
				continue
			}
			switch {
			case strings.HasPrefix(name, "resource."):
				resAttrs[name[len("resource."):]] = v
			case strings.HasPrefix(name, "span."):
				span.Attributes = append(span.Attributes, anyToKV(name[len("span."):], v))
			case strings.HasPrefix(name, "scope."):
				scopeAttrs[name[len("scope."):]] = v
			}
		}
	}

	return traceID, span, resAttrs, resSchemaURL, scopeAttrs, scopeSchemaURL
}

// columnAny returns the typed value from col at rowIdx as any, or nil if absent.
func columnAny(col *modules_reader.Column, rowIdx int) any {
	if col == nil || !col.IsPresent(rowIdx) {
		return nil
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			return v
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			return v
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return v
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return v
		}
	case modules_shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			return v
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			return cloneBytes(v)
		}
	}
	return nil
}

// anyToKV converts a (key, any) pair to an OTLP KeyValue.
func anyToKV(key string, v any) *commonv1.KeyValue {
	kv := &commonv1.KeyValue{Key: key}
	switch val := v.(type) {
	case string:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}}
	case int64:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}}
	case uint64:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(val)}} //nolint:gosec
	case float64:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: val}}
	case bool:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: val}}
	case []byte:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: val}}
	default:
		kv.Value = &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprint(val)}}
	}
	return kv
}

// cloneBytes returns a copy of b to avoid aliasing into block memory.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
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
