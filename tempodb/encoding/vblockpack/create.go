package vblockpack

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// CreateBlock creates a new blockpack block from an iterator.
// Streams blockpack data directly to backend storage to avoid buffering
// the entire block in memory.
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {

	// Initialize disk cache on first block creation (no-op if already initialized).
	ConfigureFileCache(cfg.Blockpack.FileCachePath, cfg.Blockpack.FileCacheMaxBytes)

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)

	var (
		traceCount   int
		minStart     uint64 = ^uint64(0)
		maxStart     uint64
		bytesWritten int64
	)

	// Write blockpack data into the pipe in a goroutine so StreamWriter can
	// consume it concurrently, flushing each internal block to S3 as it fills
	// up rather than accumulating the entire dataset in memory.
	go func() {
		var err error
		defer func() {
			if err != nil {
				pw.CloseWithError(err)
			} else {
				pw.Close()
			}
			errCh <- err
		}()

		cw := &countingWriter{w: pw}
		writer, err := blockpack.NewWriter(cw, cfg.Blockpack.MaxSpansPerBlock)
		if err != nil {
			err = fmt.Errorf("failed to create blockpack writer: %w", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
			}

			id, tr, nextErr := i.Next(ctx)
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				err = fmt.Errorf("failed to read from iterator: %w", nextErr)
				return
			}

			// Track time range from span timestamps so we can populate meta
			// without re-parsing the block bytes after writing.
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

			td := tempoTraceToOTLP(tr)
			if addErr := writer.AddTracesData(td); addErr != nil {
				err = fmt.Errorf("failed to add trace to blockpack: %w", addErr)
				return
			}

			traceCount++
			_ = id // Trace ID is embedded in the trace data
		}

		if _, flushErr := writer.Flush(); flushErr != nil {
			err = fmt.Errorf("failed to flush blockpack writer: %w", flushErr)
			return
		}
		bytesWritten = cw.n
	}()

	// Stream blockpack output directly to backend storage.
	// size=-1 enables chunked/multipart upload without requiring the size upfront.
	blockUUID := uuid.UUID(meta.BlockID)
	streamErr := to.StreamWriter(ctx, DataFileName, blockUUID, meta.TenantID, pr, -1)
	goroutineErr := <-errCh

	if streamErr != nil {
		return nil, fmt.Errorf("failed to stream blockpack to backend: %w", streamErr)
	}
	if goroutineErr != nil {
		return nil, goroutineErr
	}

	meta.TotalObjects = int64(traceCount)
	meta.Size_ = uint64(bytesWritten)
	// TotalRecords=1: one job per file — see setBlockTimeRange for rationale.
	meta.TotalRecords = 1
	if minStart != ^uint64(0) {
		meta.StartTime = time.Unix(0, int64(minStart))
		meta.EndTime = time.Unix(0, int64(maxStart))
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

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
