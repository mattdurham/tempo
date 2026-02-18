package vblockpack

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/mattdurham/blockpack"
)

// CreateBlock creates a new blockpack block from an iterator
// Converts trace iterator to blockpack format and writes to backend storage
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {

	// Create blockpack writer with configured block size
	// Default to 2000 spans per block, but use cfg.RowGroupSizeBytes as hint
	maxSpans := 2000
	if cfg.RowGroupSizeBytes > 0 {
		// Rough estimate: 1KB per span average
		maxSpans = cfg.RowGroupSizeBytes / 1024
		if maxSpans < 100 {
			maxSpans = 100
		}
		if maxSpans > 10000 {
			maxSpans = 10000
		}
	}

	writer, err := blockpack.NewWriter(maxSpans)
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
	// Flush blockpack writer to get serialized bytes
	data, err := writer.Flush()
	if err != nil {
		return nil, fmt.Errorf("failed to flush blockpack writer: %w", err)
	}

	// Update metadata with actual stats
	meta.TotalObjects = int64(traceCount)
	meta.Size_ = uint64(len(data))

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
