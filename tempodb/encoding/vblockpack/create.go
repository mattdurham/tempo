package vblockpack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// CreateBlock creates a new blockpack block from an iterator.
// Writes blockpack data to a temp file to avoid buffering the entire block in
// memory, then streams the file to backend storage with a known size.
func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, _ backend.Reader, to backend.Writer,
) (*backend.BlockMeta, error) {
	// Initialize multi-tier cache on first block creation (no-op if already initialized).
	ConfigureCache(
		cfg.Blockpack.FileCachePath,
		cfg.Blockpack.FileCacheMaxBytes,
		cfg.Blockpack.MemCacheServers,
	)

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
		// blockpack NOTE-220: emit V15 blocks with inline tiny columns. The reader accepts
		// both V14 and V15, and this binary's reader was updated in the same change, so it
		// is safe to enable here. Tiny low-cardinality columns are stored inline in the TOC
		// entry, skipping the offset indirection and per-column outer snappy.
		EnableInlineColumns: true,
		// blockpack NOTE-405 (issue #355): per-column zstd codec, benefit-gated. The writer
		// keeps zstd only when it beats snappy by a margin (large dict/ID columns like
		// trace:id/span:id/span:parent_id), leaving incompressible/bit-packed columns on
		// snappy. The reader is always codec-aware (additive flag on the V15 flags byte), so
		// any V15 reader decodes zstd blobs. Shrinks on-disk size on the I/O-bound read path.
		EnableZstdColumns: true,
	}
	// Map Tempo dedicated columns to blockpack dedicated columns.
	// span-scope → "span." prefix; resource-scope → "resource." prefix.
	// Event-scope and unknown scopes are not yet supported by blockpack and are skipped.
	// Prefer per-tenant override in meta; fall back to block config default (which carries
	// DefaultDedicatedColumns for all tenants without an explicit per-tenant override).
	dedicatedCols := meta.DedicatedColumns
	if len(dedicatedCols) == 0 {
		dedicatedCols = cfg.DedicatedColumns
	}
	if len(dedicatedCols) > 0 {
		writerCfg.DedicatedColumns = dedicatedColumnsToBlockpack(dedicatedCols)
	}

	writer, err := blockpack.NewWriterWithConfig(writerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create blockpack writer: %w", err)
	}

	// Accumulate VCNT counts and cube counts in parallel with block writing.
	var vcntAcc *vcntAccumulator
	if getVCNTSink() != nil {
		vcntAcc = newVCNTAccumulator()
	}
	cm := getCubeManager()
	if cm != nil {
		cm.maybeRefresh()
	}

	var traceCount int

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		id, tr, nextErr := i.Next(ctx)
		if errors.Is(nextErr, io.EOF) || (tr == nil && nextErr == nil) {
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
		if vcntAcc != nil {
			vcntAcc.addTrace(tr)
		}
		if cm != nil {
			cm.addTrace(tr)
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
	// Do not set meta.StartTime/EndTime here — the caller (tenant_store.go)
	// runs adjustTimeRangeForSlack after CreateBlock returns, which clamps the
	// time range to the consume-cycle window. Overwriting here with raw span
	// timestamps bypasses that clamping and produces blocks with wide time
	// ranges that prevent compaction (blocks span multiple compaction windows).
	// This matches vParquet4's CreateBlock which passes meta timestamps through
	// unchanged (vparquet4/create.go:144-145).

	blockUUID := uuid.UUID(meta.BlockID)
	if err := to.StreamWriter(ctx, DataFileName, blockUUID, meta.TenantID, tmp, size); err != nil {
		return nil, fmt.Errorf("failed to stream blockpack to backend: %w", err)
	}

	if err := to.WriteBlockMeta(ctx, meta); err != nil {
		return nil, fmt.Errorf("failed to write block metadata: %w", err)
	}

	// Write VCNT value-count files — one .vcnt per column with (col, value, count)
	// for tag autocomplete and cardinality queries. Best-effort; skipped when
	// value_index_enabled is false.
	if vcntAcc != nil {
		vcntAcc.flush(getVCNTSink(), meta.TenantID)
	}
	if cm != nil {
		cm.flush(meta.TenantID)
	}

	// blockpack NOTE-VI-042 (issue #464): synchronously write per-column L0
	// value-index files for this block, with no Redis broker. Best-effort — a
	// failure is logged but never fails the block write, since the index can
	// always be rebuilt from the source block. Skipped entirely (no S3 client,
	// no extra work) when value_index_enabled is false.
	if store, prefix := getValueIndexSink(); store != nil {
		sourceRef := blockObjectKey(meta.TenantID, blockUUID.String())
		if _, serr := tmp.Seek(0, io.SeekStart); serr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 skipped: seek failed", "block", sourceRef, "err", serr)
		} else if r, rerr := blockpack.NewReaderFromProvider(&fileReaderProvider{f: tmp}); rerr != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 skipped: open reader failed", "block", sourceRef, "err", rerr)
		} else {
			policy := BuildViColumnPolicyForTenant(ctx, cfg.Blockpack.ViUsage, meta.TenantID)
			if werr := blockpack.WriteValueIndexL0(r, store, sourceRef, meta.TenantID, prefix, policy); werr != nil {
				level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index L0 write failed", "block", sourceRef, "err", werr)
			}
		}
	}

	return meta, nil
}

// dedicatedColumnsToBlockpack converts Tempo backend DedicatedColumns to blockpack
// DedicatedColumn entries. Only span and resource scopes are supported; event-scope
// and unknown scopes are skipped. The full column name is constructed by prepending
// the scope prefix: "span." for span scope, "resource." for resource scope.
func dedicatedColumnsToBlockpack(cols backend.DedicatedColumns) []blockpack.DedicatedColumn {
	out := make([]blockpack.DedicatedColumn, 0, len(cols))
	for _, dc := range cols {
		switch dc.Scope {
		case backend.DedicatedColumnScopeSpan:
			out = append(out, blockpack.DedicatedColumn{Name: "span." + dc.Name})
		case backend.DedicatedColumnScopeResource:
			out = append(out, blockpack.DedicatedColumn{Name: "resource." + dc.Name})
		}
		// Event scope and unknown scopes are not yet supported by blockpack.
	}
	return out
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

	// blockpack #436: span:start is a regular per-row block column; the block time
	// range is derived from BlockMeta.MinStart / BlockMeta.MaxStart, which the writer
	// populates from the span:start column.
	minStart := ^uint64(0)
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
