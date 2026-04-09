package vblockpack

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// fileReaderProvider implements blockpack.ReaderProvider for an *os.File.
// Used by setBlockTimeRangeFromFile to read the written blockpack without
// loading the full file into memory.
type fileReaderProvider struct {
	f    *os.File
	size int64
}

func (p *fileReaderProvider) Size() (int64, error) {
	return p.size, nil
}

func (p *fileReaderProvider) ReadAt(b []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(b, off)
}

// CreateBlock creates a new blockpack block from an iterator.
// Writes blockpack data to a temp file to avoid buffering the entire block in
// memory, then streams the file to backend storage with a known size.
// When cfg.Blockpack.VectorDimension > 0, the writer builds a VectorIndex
// section in the V5 footer for any __embedding__ columns present in the data.
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {

	// Initialize multi-tier cache on first block creation (no-op if already initialized).
	ConfigureCache(
		cfg.Blockpack.FileCachePath,
		cfg.Blockpack.FileCacheMaxBytes,
		cfg.Blockpack.MemCacheServers,
		cfg.Blockpack.MemCacheMaxBytes,
		cfg.Blockpack.MemoryCacheBytes,
	)
	ConfigureLRU(cfg.Blockpack.LRUCacheBytes)
	ConfigureEmbedding(cfg.Blockpack.EmbeddingURL)

	// Write to a temp file so we get a known size for StreamWriter and avoid
	// holding the entire encoded block in RAM.
	tmp, err := os.CreateTemp("", "vblockpack-*.bp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	writerCfg := blockpack.WriterConfig{
		OutputStream:  tmp,
		MaxBlockSpans: cfg.Blockpack.MaxSpansPerBlock,
	}
	// Pass embedder to blockpack writer — it handles field assembly and embedding internally.
	// Must guard against nil *Embedder assigned to interface (Go nil interface trap).
	if emb := getProcessEmbedder(cfg.Blockpack.EmbeddingURL); emb != nil {
		writerCfg.Embedder = emb
	}
	if cfg.Blockpack.VectorDimension > 0 {
		writerCfg.VectorDimension = cfg.Blockpack.VectorDimension
	}

	writer, err := blockpack.NewWriterWithConfig(writerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack writer: %w", err)
	}

	var traceCount int

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		id, tr, nextErr := i.Next(ctx)
		if nextErr == io.EOF || (tr == nil && nextErr == nil) {
			break
		}
		if nextErr != nil {
			return nil, fmt.Errorf("failed to read from iterator: %w", nextErr)
		}
		if tr == nil {
			continue
		}

		if addErr := writer.AddTempoTrace(tr); addErr != nil {
			return nil, fmt.Errorf("failed to add trace to blockpack: %w", addErr)
		}

		traceCount++
		_ = id // Trace ID is embedded in the trace data
	}

	if _, err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush blockpack writer: %w", err)
	}

	// Get file size and rewind for streaming.
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get temp file size: %w", err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to rewind temp file: %w", err)
	}

	meta.TotalObjects = int64(traceCount)
	meta.Size_ = uint64(size)
	meta.TotalRecords = 1
	// Read the actual span time range back from the written blockpack file.
	// This is the authoritative source for StartTime/EndTime — reading from
	// the intrinsic span:start column avoids the off-by-one where maxStart
	// (latest span start) was incorrectly used as EndTime, and correctly
	// handles any span whose StartTimeUnixNano differs from EndTimeUnixNano.
	setBlockTimeRangeFromFile(meta, tmp, size)

	blockUUID := uuid.UUID(meta.BlockID)
	if err := to.StreamWriter(ctx, DataFileName, blockUUID, meta.TenantID, tmp, size); err != nil {
		return nil, fmt.Errorf("failed to stream blockpack to backend: %w", err)
	}

	if err := to.WriteBlockMeta(ctx, meta); err != nil {
		return nil, fmt.Errorf("failed to write block metadata: %w", err)
	}

	return meta, nil
}

// setBlockTimeRangeFromFile populates meta.StartTime and meta.EndTime from the
// blockpack file pointed to by f (size bytes). Unlike setBlockTimeRange which
// requires the full file in memory, this reads directly from the file using
// random-access I/O. If the file cannot be parsed or contains no spans,
// StartTime/EndTime are left unchanged.
func setBlockTimeRangeFromFile(meta *backend.BlockMeta, f *os.File, size int64) {
	if size == 0 {
		return
	}
	r, err := blockpack.NewReaderFromProvider(&fileReaderProvider{f: f, size: size})
	if err != nil {
		return
	}
	setBlockTimeRangeFromReader(meta, r)
}

// setBlockTimeRange populates meta.StartTime and meta.EndTime from the actual
// span timestamps in the blockpack data. This allows Tempo's block selector to
// skip blocks outside the query time range, reducing blocks scanned per query.
// If the data cannot be parsed or contains no spans, StartTime/EndTime are unchanged.
func setBlockTimeRange(meta *backend.BlockMeta, data []byte) {
	r, err := blockpack.NewReaderFromProvider(&bytesReaderProvider{data: data})
	if err != nil {
		return
	}
	setBlockTimeRangeFromReader(meta, r)
}

// setBlockTimeRangeFromReader applies span timestamp bounds from a blockpack Reader
// to meta.StartTime and meta.EndTime. It reads the intrinsic span:start column
// (a sorted flat uint64 column whose first value is the minimum and last value
// is the maximum start timestamp across all spans). Falls back to per-block
// BlockMeta.MinStart/MaxStart for older files that predate the intrinsic section.
func setBlockTimeRangeFromReader(meta *backend.BlockMeta, r *blockpack.Reader) {
	// PR #172 (dual-storage intrinsic format) stores span:start exclusively in
	// the intrinsic section — it is no longer present in block columns. Read the
	// time range from the intrinsic column, which is a sorted flat uint64 column:
	// Uint64Values[0] is the minimum start time, Uint64Values[len-1] the maximum.
	col, colErr := r.GetIntrinsicColumn("span:start")
	if colErr == nil && col != nil && len(col.Uint64Values) > 0 {
		minStart := col.Uint64Values[0]
		maxStart := col.Uint64Values[len(col.Uint64Values)-1]
		meta.StartTime = time.Unix(0, int64(minStart)) //nolint:gosec
		meta.EndTime = time.Unix(0, int64(maxStart))   //nolint:gosec
		meta.TotalRecords = 1
		return
	}

	// Fallback: older format files store span:start in block columns and populate
	// BlockMeta.MinStart / BlockMeta.MaxStart during compaction.
	var minStart uint64 = ^uint64(0)
	var maxEnd uint64
	for i := range r.BlockCount() {
		bm := r.BlockMeta(i)
		if bm.MinStart < minStart {
			minStart = bm.MinStart
		}
		if bm.MaxStart > maxEnd {
			maxEnd = bm.MaxStart
		}
	}
	if r.BlockCount() > 0 && minStart != ^uint64(0) {
		meta.StartTime = time.Unix(0, int64(minStart)) //nolint:gosec
		meta.EndTime = time.Unix(0, int64(maxEnd))     //nolint:gosec
	}
	meta.TotalRecords = 1
}
