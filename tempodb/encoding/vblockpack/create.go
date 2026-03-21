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

// CreateBlock creates a new blockpack block from an iterator.
// Writes blockpack data to a temp file to avoid buffering the entire block in
// memory, then streams the file to backend storage with a known size.
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {

	// Initialize disk cache on first block creation (no-op if already initialized).
	ConfigureFileCache(cfg.Blockpack.FileCachePath, cfg.Blockpack.FileCacheMaxBytes)
	ConfigureLRU(cfg.Blockpack.LRUCacheBytes)

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

	writer, err := blockpack.NewWriter(tmp, cfg.Blockpack.MaxSpansPerBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack writer: %w", err)
	}

	var (
		traceCount int
		minStart   uint64 = ^uint64(0)
		maxStart   uint64
	)

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

		// Track time range from span timestamps to populate meta without
		// re-parsing the block bytes after writing.
		for _, rs := range tr.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					if span.StartTimeUnixNano < minStart {
						minStart = span.StartTimeUnixNano
					}
					if span.StartTimeUnixNano > maxStart {
						maxStart = span.StartTimeUnixNano
					}
				}
			}
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
	if minStart != ^uint64(0) {
		meta.StartTime = time.Unix(0, int64(minStart))
		meta.EndTime = time.Unix(0, int64(maxStart))
	}

	blockUUID := uuid.UUID(meta.BlockID)
	if err := to.StreamWriter(ctx, DataFileName, blockUUID, meta.TenantID, tmp, size); err != nil {
		return nil, fmt.Errorf("failed to stream blockpack to backend: %w", err)
	}

	if err := to.WriteBlockMeta(ctx, meta); err != nil {
		return nil, fmt.Errorf("failed to write block metadata: %w", err)
	}

	return meta, nil
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
		meta.StartTime = time.Unix(0, int64(minStart))
		meta.EndTime = time.Unix(0, int64(maxEnd))
	}
	// TotalRecords = 1: one job per file (no sub-file sharding).
	// Blockpack's intrinsic fast path evaluates all internal blocks using
	// intrinsic column blobs (~10 MB) instead of reading full blocks (~180 MB).
	// Sub-file sharding disables this fast path by restricting each job to a
	// block range, forcing full block reads. Setting TotalRecords=1 ensures the
	// frontend creates exactly one job per file.
	meta.TotalRecords = 1
}
