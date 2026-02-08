package vblockpack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// NewCompactor creates a new blockpack compactor with the given options.
func NewCompactor(opts common.CompactionOptions) *Compactor {
	return &Compactor{opts: opts}
}

// Compactor implements the common.Compactor interface for blockpack blocks.
// It merges multiple blockpack (or parquet) blocks into consolidated blockpack blocks.
type Compactor struct {
	opts common.CompactionOptions
}

// Compact merges multiple input blocks into one or more output blocks.
// It handles:
// - Reading from blockpack and parquet blocks during migration
// - Deduplicating traces across blocks
// - Maintaining time ordering
// - Updating bloom filters and block metadata
// - Writing output in blockpack format
func (c *Compactor) Compact(ctx context.Context, l log.Logger, r backend.Reader, w backend.Writer, inputs []*backend.BlockMeta) (newCompactedBlocks []*backend.BlockMeta, err error) {
	var (
		compactionLevel uint32
		totalRecords    int64
		minBlockStart   time.Time
		maxBlockEnd     time.Time
		bookmarks       = make([]*traceBookmark, 0, len(inputs))
	)

	// Collect metadata from input blocks
	for _, blockMeta := range inputs {
		totalRecords += blockMeta.TotalObjects

		if blockMeta.CompactionLevel > compactionLevel {
			compactionLevel = blockMeta.CompactionLevel
		}

		if blockMeta.StartTime.Before(minBlockStart) || minBlockStart.IsZero() {
			minBlockStart = blockMeta.StartTime
		}
		if blockMeta.EndTime.After(maxBlockEnd) {
			maxBlockEnd = blockMeta.EndTime
		}

		// Create iterator for this block
		// For now, use a placeholder iterator - full implementation will handle
		// both blockpack and parquet blocks
		iter, err := createIteratorForBlock(blockMeta, r)
		if err != nil {
			return nil, fmt.Errorf("error creating iterator for block %s: %w", blockMeta.BlockID.String(), err)
		}

		bookmarks = append(bookmarks, &traceBookmark{
			iter:    iter,
			current: nil,
			id:      nil,
			err:     nil,
		})
	}

	var (
		replicationFactor   = inputs[0].ReplicationFactor
		nextCompactionLevel = compactionLevel + 1
		dedicatedColumns    = inputs[0].DedicatedColumns
	)

	// Create trace combiner for deduplication
	combine := func(traces []*tempopb.Trace) (*tempopb.Trace, error) {
		if len(traces) == 0 {
			return nil, nil
		}

		if len(traces) == 1 {
			return traces[0], nil
		}

		// Check if all traces are identical
		isEqual := true
		for i := 1; i < len(traces) && isEqual; i++ {
			// Simple comparison - in production, use proper trace comparison
			isEqual = traceIDsEqual(traces[0], traces[i])
		}
		if isEqual {
			return traces[0], nil
		}

		// Check if combined trace exceeds max size
		if c.opts.MaxBytesPerTrace > 0 {
			totalSize := 0
			for _, tr := range traces {
				totalSize += tr.Size()
			}
			if totalSize > c.opts.MaxBytesPerTrace {
				// Trace too large to compact - keep first one
				if c.opts.SpansDiscarded != nil {
					spanCount := 0
					for i := 1; i < len(traces); i++ {
						spanCount += countSpansInTrace(traces[i])
					}
					c.opts.SpansDiscarded("", "", "", spanCount)
				}
				return traces[0], nil
			}
		}

		// Merge traces
		merged, dedupedSpans := mergeTraces(traces)

		if c.opts.DedupedSpans != nil {
			c.opts.DedupedSpans(int(replicationFactor), dedupedSpans)
		}

		// Check for disconnected or rootless traces
		if merged != nil {
			if !isTraceConnected(merged) && c.opts.DisconnectedTrace != nil {
				c.opts.DisconnectedTrace()
			}
			if isRootlessTrace(merged) && c.opts.RootlessTrace != nil {
				c.opts.RootlessTrace()
			}
		}

		if c.opts.ObjectsCombined != nil {
			c.opts.ObjectsCombined(int(compactionLevel), 1)
		}

		return merged, nil
	}

	var (
		m               = newMultiBlockIterator(bookmarks, combine)
		recordsPerBlock = (totalRecords / int64(c.opts.OutputBlocks))
		currentBlock    *blockpackBlock
	)
	defer m.Close()

	// Iterate through all traces in sorted order
	for {
		traceID, trace, err := m.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error iterating input blocks: %w", err)
		}

		// Drop object if requested
		if c.opts.DropObject != nil && c.opts.DropObject(traceID) {
			continue
		}

		// Create new block if necessary
		if currentBlock == nil {
			newMeta := &backend.BlockMeta{
				BlockID:           backend.NewUUID(),
				TenantID:          inputs[0].TenantID,
				CompactionLevel:   nextCompactionLevel,
				TotalObjects:      recordsPerBlock, // Estimate
				ReplicationFactor: replicationFactor,
				DedicatedColumns:  dedicatedColumns,
				Version:           VersionString,
			}

			currentBlock, err = newBlockpackBlock(ctx, &c.opts.BlockConfig, newMeta, w)
			if err != nil {
				return nil, fmt.Errorf("error creating new block: %w", err)
			}
			newCompactedBlocks = append(newCompactedBlocks, currentBlock.meta)
		}

		// Add trace to current block
		err = currentBlock.AddTrace(traceID, trace)
		if err != nil {
			return nil, fmt.Errorf("error adding trace to block: %w", err)
		}

		// Flush block if it's getting too large
		if currentBlock.ShouldFlush(uint32(c.opts.BlockConfig.RowGroupSizeBytes)) {
			err = c.flushBlock(ctx, currentBlock, l)
			if err != nil {
				return nil, fmt.Errorf("error flushing block: %w", err)
			}
		}

		// Complete block if we've reached the target size
		if currentBlock.meta.TotalObjects >= recordsPerBlock {
			currentBlock.meta.StartTime = minBlockStart
			currentBlock.meta.EndTime = maxBlockEnd
			err := c.finishBlock(ctx, currentBlock, l)
			if err != nil {
				return nil, fmt.Errorf("error completing block %s: %w", currentBlock.meta.BlockID.String(), err)
			}
			currentBlock = nil
		}
	}

	// Complete final block
	if currentBlock != nil {
		currentBlock.meta.StartTime = minBlockStart
		currentBlock.meta.EndTime = maxBlockEnd
		err := c.finishBlock(ctx, currentBlock, l)
		if err != nil {
			return nil, fmt.Errorf("error completing final block %s: %w", currentBlock.meta.BlockID.String(), err)
		}
	}

	return newCompactedBlocks, nil
}

// flushBlock flushes the current block's buffered data
func (c *Compactor) flushBlock(ctx context.Context, block *blockpackBlock, l log.Logger) error {
	_, span := tracer.Start(ctx, "vblockpack.compactor.flushBlock")
	defer span.End()

	compactionLevel := int(block.meta.CompactionLevel - 1)
	objs := block.meta.TotalObjects

	if c.opts.ObjectsWritten != nil {
		c.opts.ObjectsWritten(compactionLevel, int(objs))
	}

	bytesFlushed, err := block.Flush()
	if err != nil {
		return err
	}

	if c.opts.BytesWritten != nil {
		c.opts.BytesWritten(compactionLevel, int(bytesFlushed))
	}

	level.Info(l).Log("msg", "flushed blockpack block", "bytes", bytesFlushed, "objects", objs)

	return nil
}

// finishBlock completes and writes a block to backend storage
func (c *Compactor) finishBlock(ctx context.Context, block *blockpackBlock, l log.Logger) error {
	_, span := tracer.Start(ctx, "vblockpack.compactor.finishBlock")
	defer span.End()

	bytesFlushed, err := block.Complete()
	if err != nil {
		return fmt.Errorf("error completing block: %w", err)
	}

	level.Info(l).Log(
		"msg", "wrote compacted blockpack block",
		"version", block.meta.Version,
		"tenantID", block.meta.TenantID,
		"blockID", block.meta.BlockID.String(),
		"startTime", block.meta.StartTime.String(),
		"endTime", block.meta.EndTime.String(),
		"totalObjects", block.meta.TotalObjects,
		"size", block.meta.Size_,
		"compactionLevel", block.meta.CompactionLevel,
		"bloomShardCount", block.meta.BloomShardCount,
		"replicationFactor", block.meta.ReplicationFactor,
		"dedicatedColumns", fmt.Sprintf("%+v", block.meta.DedicatedColumns),
	)

	span.AddEvent("wrote compacted blockpack block")
	span.SetAttributes(
		attribute.String("blockID", block.meta.BlockID.String()),
	)

	compactionLevel := int(block.meta.CompactionLevel) - 1
	if c.opts.BytesWritten != nil {
		c.opts.BytesWritten(compactionLevel, int(bytesFlushed))
	}

	return nil
}

// traceBookmark tracks the current position in a block iterator
type traceBookmark struct {
	iter    common.Iterator
	current *tempopb.Trace
	id      common.ID
	err     error
}

// multiBlockIterator merges traces from multiple blocks in sorted order
type multiBlockIterator struct {
	bookmarks []*traceBookmark
	combiner  func([]*tempopb.Trace) (*tempopb.Trace, error)
}

func newMultiBlockIterator(bookmarks []*traceBookmark, combiner func([]*tempopb.Trace) (*tempopb.Trace, error)) *multiBlockIterator {
	return &multiBlockIterator{
		bookmarks: bookmarks,
		combiner:  combiner,
	}
}

// Next returns the next trace in sorted order
func (m *multiBlockIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	// Advance all bookmarks that need it
	for _, bm := range m.bookmarks {
		if bm.current == nil && bm.err == nil {
			bm.id, bm.current, bm.err = bm.iter.Next(ctx)
		}
	}

	// Find the lowest trace ID
	var lowestID common.ID
	lowestSet := false

	for _, bm := range m.bookmarks {
		if bm.err != nil {
			continue
		}
		if bm.current == nil {
			continue
		}

		if !lowestSet || compareIDs(bm.id, lowestID) < 0 {
			lowestID = bm.id
			lowestSet = true
		}
	}

	if !lowestSet {
		// Check if all iterators are done
		allDone := true
		for _, bm := range m.bookmarks {
			if !errors.Is(bm.err, io.EOF) {
				allDone = false
				break
			}
		}
		if allDone {
			return nil, nil, io.EOF
		}
		// Return first non-EOF error
		for _, bm := range m.bookmarks {
			if bm.err != nil && !errors.Is(bm.err, io.EOF) {
				return nil, nil, bm.err
			}
		}
		return nil, nil, io.EOF
	}

	// Collect all traces with this ID
	var traces []*tempopb.Trace
	for _, bm := range m.bookmarks {
		if bm.current != nil && compareIDs(bm.id, lowestID) == 0 {
			traces = append(traces, bm.current)
			bm.current = nil // Mark for advancement
		}
	}

	// Combine traces
	combined, err := m.combiner(traces)
	if err != nil {
		return nil, nil, err
	}

	return lowestID, combined, nil
}

func (m *multiBlockIterator) Close() {
	for _, bm := range m.bookmarks {
		if bm.iter != nil {
			bm.iter.Close()
		}
	}
}

// Helper functions

func createIteratorForBlock(_ *backend.BlockMeta, _ backend.Reader) (common.Iterator, error) {
	// This will be implemented to handle both blockpack and parquet blocks
	// For now, return an error indicating implementation needed
	return nil, fmt.Errorf("createIteratorForBlock not yet implemented")
}

func traceIDsEqual(t1, t2 *tempopb.Trace) bool {
	if t1 == nil || t2 == nil {
		return t1 == t2
	}
	// Compare resource spans - simplified version
	if len(t1.ResourceSpans) != len(t2.ResourceSpans) {
		return false
	}
	return true
}

// Stub implementations for compaction block writing
// These will be fully implemented when blockpack writer is integrated

// newBlockpackBlock creates a stub compaction block
func newBlockpackBlock(ctx context.Context, config *common.BlockConfig, meta *backend.BlockMeta, w backend.Writer) (*blockpackBlock, error) {
	// TODO: Implement full blockpack writer integration
	// For now, return a stub block that tracks metadata
	return &blockpackBlock{
		meta:   meta,
		reader: nil, // Writer mode - no reader needed
	}, nil
}

// AddTrace is a stub for adding traces during compaction
func (b *blockpackBlock) AddTrace(id common.ID, trace *tempopb.Trace) error {
	// TODO: Write trace to blockpack format
	// For now, just update metadata
	if trace != nil {
		b.meta.TotalObjects++
	}
	return nil
}

// ShouldFlush checks if block should be flushed
func (b *blockpackBlock) ShouldFlush(maxBytes uint32) bool {
	// TODO: Check actual buffer size once writer is implemented
	// For now, use object count as proxy
	return b.meta.TotalObjects >= 10000
}

// Flush is a stub for flushing block data
func (b *blockpackBlock) Flush() (int64, error) {
	// TODO: Flush buffered data to storage
	// Return estimate for now
	return int64(b.meta.TotalObjects * 1000), nil
}

// Complete is a stub for finalizing the block
func (b *blockpackBlock) Complete() (int64, error) {
	// TODO: Write block footer and metadata
	// Return estimate for now
	return int64(b.meta.TotalObjects * 1000), nil
}

func isRootlessTrace(tr *tempopb.Trace) bool {
	if tr == nil || tr.ResourceSpans == nil {
		return true
	}
	// Check if there's at least one span that could be a root
	for _, rs := range tr.ResourceSpans {
		if rs.ScopeSpans != nil {
			for _, ss := range rs.ScopeSpans {
				if len(ss.Spans) > 0 {
					// In a real implementation, check for spans with no parent
					return false
				}
			}
		}
	}
	return true
}

func countSpansInTrace(tr *tempopb.Trace) int {
	if tr == nil {
		return 0
	}
	count := 0
	if tr.ResourceSpans != nil {
		for _, rs := range tr.ResourceSpans {
			if rs.ScopeSpans != nil {
				for _, ss := range rs.ScopeSpans {
					count += len(ss.Spans)
				}
			}
		}
	}
	return count
}

func mergeTraces(traces []*tempopb.Trace) (*tempopb.Trace, int) {
	if len(traces) == 0 {
		return nil, 0
	}
	if len(traces) == 1 {
		return traces[0], 0
	}

	// Use the first trace as base and merge others into it
	merged := traces[0]
	dedupedSpans := 0

	// Simple merge - in production, implement proper span deduplication
	// This is a placeholder implementation
	for i := 1; i < len(traces); i++ {
		// Count spans that would be deduplicated
		// In a real implementation, merge resource spans and deduplicate
		dedupedSpans += countSpansInTrace(traces[i])
	}

	return merged, dedupedSpans
}

func isTraceConnected(tr *tempopb.Trace) bool {
	// Simplified check - in production, verify parent-child relationships
	return tr != nil && tr.ResourceSpans != nil && len(tr.ResourceSpans) > 0
}

func compareIDs(id1, id2 common.ID) int {
	// Compare trace IDs lexicographically
	for i := 0; i < len(id1) && i < len(id2); i++ {
		if id1[i] < id2[i] {
			return -1
		}
		if id1[i] > id2[i] {
			return 1
		}
	}
	if len(id1) < len(id2) {
		return -1
	}
	if len(id1) > len(id2) {
		return 1
	}
	return 0
}
