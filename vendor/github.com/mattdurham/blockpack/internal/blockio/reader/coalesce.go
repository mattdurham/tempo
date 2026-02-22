package reader

import (
	"fmt"

	"github.com/mattdurham/blockpack/internal/arena"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// AggressiveCoalesceConfig returns config that minimizes IO operations for S3/object storage.
// This is the recommended default since S3 latency (~20ms/IO) dominates over bandwidth costs.
// Trades bandwidth efficiency for fewer round-trips.
func AggressiveCoalesceConfig() shared.CoalesceConfig {
	return shared.CoalesceConfig{
		MaxReadSize:   4 * 1024 * 1024, // 4MB max per read
		MaxGapSize:    4 * 1024 * 1024, // Allow large gaps (up to MaxReadSize)
		MaxWasteRatio: 1.0,             // Allow 100% waste (no limit)
	}
}

// CoalesceBlocks groups adjacent blocks into larger reads to reduce IO operations.
// Uses aggressive coalescing by default to minimize IOs for S3/object storage.
// For backward compatibility, maxReadSize parameter is still accepted.
//
// For S3 workloads: fewer IOs is more important than bandwidth savings (use aggressive).
// For bandwidth-constrained scenarios: use CoalesceBlocksWithConfig with DefaultCoalesceConfig.
func (r *Reader) CoalesceBlocks(blockOrder []int, maxReadSize uint64) []shared.CoalescedRead {
	config := AggressiveCoalesceConfig()
	if maxReadSize > 0 {
		config.MaxReadSize = maxReadSize
	}
	return r.CoalesceBlocksWithConfig(blockOrder, config)
}

// CoalesceBlocksWithConfig groups blocks into larger reads with configurable strategy.
// Blocks that are not adjacent or would exceed limits are placed in separate reads.
// This is critical for object storage (S3) where each IO operation has ~20ms latency.
//
// Adaptive coalescing skips large gaps to avoid reading excessive unwanted data:
// - Adjacent blocks (gap=0) are always coalesced
// - Small gaps (< MaxGapSize) are coalesced if waste ratio is acceptable
// - Large gaps cause separate reads
//
// Example: blocks [10, 50, 51, 90]
// - Aggressive: reads 10-90 (huge waste reading 11-49, 52-89)
// - Adaptive: reads [10], [50-51], [90] (3 IOs, no waste)
func (r *Reader) CoalesceBlocksWithConfig(blockOrder []int, config shared.CoalesceConfig) []shared.CoalescedRead {
	if len(blockOrder) == 0 {
		return nil
	}

	if config.MaxReadSize == 0 {
		config.MaxReadSize = 4 * 1024 * 1024 // Default 4MB
	}

	reads := make([]shared.CoalescedRead, 0, len(blockOrder)/10) // Estimate ~10 blocks per read

	// Start first coalesced read
	startBlockIdx := blockOrder[0]
	startEntry := r.blockEntries[startBlockIdx]
	currentOffset := startEntry.Offset
	currentLength := startEntry.Length
	lastBlockIdx := startBlockIdx

	for i := 1; i < len(blockOrder); i++ {
		currBlockIdx := blockOrder[i]
		currEntry := r.blockEntries[currBlockIdx]
		lastEntry := r.blockEntries[lastBlockIdx]

		// Calculate gap between last block and current block
		expectedOffset := lastEntry.Offset + lastEntry.Length
		//nolint:gosec
		gapSize := uint64(0)
		if currEntry.Offset > expectedOffset {
			gapSize = currEntry.Offset - expectedOffset
		}
		isAdjacent := gapSize == 0

		// Decide whether to coalesce based on gap size and waste ratio
		shouldCoalesce := false
		if isAdjacent {
			// Always coalesce adjacent blocks
			shouldCoalesce = true
		} else if gapSize <= config.MaxGapSize {
			// Gap is reasonable, check waste ratio
			// Waste ratio = gap / (current data + gap + next block)
			totalWithGap := currentLength + gapSize + currEntry.Length
			wasteRatio := float64(gapSize) / float64(totalWithGap)

			shouldCoalesce = wasteRatio <= config.MaxWasteRatio
		}
		// If gap > MaxGapSize, shouldCoalesce stays false

		// Check if adding this block (with gap) would exceed size limit
		wouldExceedLimit := (currentLength + gapSize + currEntry.Length) > config.MaxReadSize

		if !shouldCoalesce || wouldExceedLimit {
			// Flush current coalesced read
			reads = append(reads, shared.CoalescedRead{
				StartBlockIdx: startBlockIdx,
				EndBlockIdx:   lastBlockIdx + 1, // exclusive
				Offset:        currentOffset,
				Length:        currentLength,
			})

			// Start new coalesced read
			startBlockIdx = currBlockIdx
			currentOffset = currEntry.Offset
			currentLength = currEntry.Length
		} else {
			// Coalesce: include gap and next block
			currentLength += gapSize + currEntry.Length
		}

		lastBlockIdx = currBlockIdx
	}

	// Flush final coalesced read
	reads = append(reads, shared.CoalescedRead{
		StartBlockIdx: startBlockIdx,
		EndBlockIdx:   lastBlockIdx + 1, // exclusive
		Offset:        currentOffset,
		Length:        currentLength,
	})

	return reads
}

// ReadCoalescedBlocks reads a single coalesced chunk and returns individual block bytes.
// Returns a map of blockIdx -> rawBytes for each block in the coalesced read.
//
// Caching strategy:
// 1. First checks cache for individual blocks (file:offset:length keys)
// 2. For cache misses, reads the entire coalesced chunk in one I/O
// 3. Caches both the coalesced chunk AND individual blocks within it
//
// This enables future queries needing just one block to hit cache instead of re-reading.
// Expected impact: 30-50% better cache hit rate for queries with overlapping block access.
func (r *Reader) ReadCoalescedBlocks(cr shared.CoalescedRead) (map[int][]byte, error) {
	blockBytes := make(map[int][]byte, cr.EndBlockIdx-cr.StartBlockIdx)

	// First pass: check cache for individual blocks
	missingBlocks := make([]int, 0, cr.EndBlockIdx-cr.StartBlockIdx)
	for blockIdx := cr.StartBlockIdx; blockIdx < cr.EndBlockIdx; blockIdx++ {
		entry := r.blockEntries[blockIdx]

		// Try to get individual block from cache
		if r.cache != nil && r.name != "" {
			key := fmt.Sprintf("%s:%d:%d", r.name, entry.Offset, entry.Length)
			if data, found := r.cache.Get(key); found {
				blockBytes[blockIdx] = data
				continue
			}
		}

		// Cache miss - need to read this block
		missingBlocks = append(missingBlocks, blockIdx)
	}

	// If all blocks were cached, we're done
	if len(missingBlocks) == 0 {
		return blockBytes, nil
	}

	// Read the entire coalesced chunk in one I/O operation
	coalescedBytes, err := r.readRange(int64(cr.Offset), int(cr.Length), shared.DataTypeColumn) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("read coalesced range [%d, %d): %w", cr.Offset, cr.Offset+cr.Length, err)
	}

	// Second pass: extract and cache individual blocks from coalesced chunk
	//nolint:gosec
	currentOffset := uint64(0)
	for blockIdx := cr.StartBlockIdx; blockIdx < cr.EndBlockIdx; blockIdx++ {
		entry := r.blockEntries[blockIdx]
		blockLength := entry.Length

		// Extract this block's bytes from the coalesced chunk
		// Note: this creates a slice backed by coalescedBytes, sharing memory
		blockData := coalescedBytes[currentOffset : currentOffset+blockLength]

		// Only add if not already in blockBytes (from cache hit)
		if _, exists := blockBytes[blockIdx]; !exists {
			blockBytes[blockIdx] = blockData

			// Cache individual block for future queries
			// Make a copy since blockData is a slice of coalescedBytes
			if r.cache != nil && r.name != "" {
				blockCopy := make([]byte, len(blockData))
				copy(blockCopy, blockData)
				key := fmt.Sprintf("%s:%d:%d", r.name, entry.Offset, entry.Length)
				r.cache.Put(key, blockCopy)
			}
		}

		currentOffset += blockLength
	}

	return blockBytes, nil
}

// AddColumnsToBlock adds additional columns to an existing block without re-reading from disk.
// This allows incremental column loading: load filter columns first, then add result columns only for matching blocks.
// If arena is provided, decoded column data is allocated on the arena.
func (r *Reader) AddColumnsToBlock(
	bwb *BlockWithBytes,
	additionalColumns map[string]struct{},
	queryArena *arena.Arena,
) error {
	if bwb == nil || bwb.Block == nil {
		return fmt.Errorf("nil block")
	}

	// If additionalColumns is empty, nothing to add
	if len(additionalColumns) == 0 {
		return nil
	}

	// Filter out columns that are already loaded
	columnsToRead := make(map[string]struct{})
	for colName := range additionalColumns {
		if _, exists := bwb.Block.columns[colName]; !exists {
			columnsToRead[colName] = struct{}{}
		}
	}

	if len(columnsToRead) == 0 {
		// All requested columns already loaded
		return nil
	}

	// Reuse already-read bytes if available to avoid re-reading from storage.
	// RawBytes is populated by GetBlockWithBytes when cachedBytes are provided
	// (i.e., from the coalesced first-pass read in scanStreamingBlocks).
	blockBytes := bwb.RawBytes
	if len(blockBytes) == 0 {
		entry := r.blockEntries[bwb.BlockIdx]
		var err error
		blockBytes, err = r.readRange(int64(entry.Offset), int(entry.Length), shared.DataTypeColumn) //nolint:gosec
		if err != nil {
			return fmt.Errorf("read block for additional columns: %w", err)
		}
	}

	// Parse only the additional columns
	additionalBlock, err := parseBlockColumnsReuse(blockBytes, columnsToRead, nil, queryArena)
	if err != nil {
		return fmt.Errorf("parse additional columns: %w", err)
	}

	// Merge additional columns into existing block
	if bwb.Block.columns == nil {
		bwb.Block.columns = make(map[string]*Column)
	}
	for name, col := range additionalBlock.columns {
		// Only add if not already present
		if _, exists := bwb.Block.columns[name]; !exists {
			bwb.Block.columns[name] = col
		}
	}

	return nil
}
