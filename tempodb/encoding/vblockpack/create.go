package vblockpack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/blockpack"
)

// CreateBlock creates a new blockpack block from an iterator
// Converts trace iterator to blockpack format and writes to backend storage
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {

	// Initialize disk cache on first block creation (no-op if already initialized).
	ConfigureFileCache(cfg.Blockpack.FileCachePath, cfg.Blockpack.FileCacheMaxBytes)

	var buf bytes.Buffer
	writer, err := blockpack.NewWriter(&buf, cfg.Blockpack.MaxSpansPerBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack writer: %w", err)
	}

	// Iterate through all traces and add to blockpack
	traceCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		id, tr, err := i.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read from iterator: %w", err)
		}

		// Convert tempopb.Trace to OTLP TracesData
		td := tempoTraceToOTLP(tr)

		// Add to blockpack writer
		if err := writer.AddTracesData(td); err != nil {
			return nil, fmt.Errorf("failed to add trace to blockpack: %w", err)
		}

		traceCount++
		_ = id // Trace ID is embedded in the trace data
	}
	// Flush blockpack writer — data is written to buf
	if _, err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush blockpack writer: %w", err)
	}
	data := buf.Bytes()

	// Update metadata with actual stats.
	// TotalRecords is set by setBlockTimeRange below to the internal block count,
	// allowing the frontend sharder to split this file into sub-file jobs.
	meta.TotalObjects = int64(traceCount)
	meta.Size_ = uint64(len(data))

	// Populate StartTime/EndTime from actual span timestamps in the blockpack file.
	// This allows Tempo's block selector to skip blocks outside the query time range.
	setBlockTimeRange(meta, data)

	// Write blockpack file to backend storage
	blockUUID := uuid.UUID(meta.BlockID)
	if err := to.Write(ctx, DataFileName, blockUUID, meta.TenantID, data, nil); err != nil {
		return nil, fmt.Errorf("failed to write blockpack to backend: %w", err)
	}

	// Write block metadata
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
	// TotalRecords = internal block count. The frontend sharder uses this to split
	// a single blockpack file into multiple sub-file jobs, each scanning a subset
	// of internal blocks. Must be >= 1 or the sharder skips the block entirely.
	if bc := r.BlockCount(); bc > 0 {
		meta.TotalRecords = uint32(bc)
	}
}
