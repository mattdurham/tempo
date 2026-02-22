package tempoapi

import (
	"fmt"

	"github.com/mattdurham/blockpack/internal/blockio/reader"
)

// BlockMeta contains metadata about a blockpack file for block selection.
type BlockMeta struct {
	MinStartNanos uint64 // Earliest span start time (unix nanos)
	MaxStartNanos uint64 // Latest span start time (unix nanos)
	TotalSpans    int    // Total number of spans across all blocks
	BlockCount    int    // Number of blocks in the file
	Size          int64  // File size in bytes
}

// GetBlockMeta returns metadata about a blockpack file including time range,
// span count, and file size. This is used by Tempo for block selection
// to determine which files to query based on time range overlap.
func GetBlockMeta(path string, storage Storage) (*BlockMeta, error) {
	// Create reader using storage provider
	provider, err := storage.GetProvider(path)
	if err != nil {
		return nil, fmt.Errorf("get storage provider: %w", err)
	}

	// Close provider if closeable
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close()
		}()
	}

	rdr, err := reader.NewReaderFromProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}

	// Get file size from provider
	size, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("get file size: %w", err)
	}

	// Get block metadata from reader
	blocks := rdr.Blocks()

	// Calculate min/max start times across all blocks
	minStart := ^uint64(0) // max uint64
	var maxStart uint64

	for _, block := range blocks {
		if block.MinStart < minStart {
			minStart = block.MinStart
		}
		if block.MaxStart > maxStart {
			maxStart = block.MaxStart
		}
	}

	// Get total span count
	totalSpans := rdr.SpanCount()

	return &BlockMeta{
		MinStartNanos: minStart,
		MaxStartNanos: maxStart,
		TotalSpans:    totalSpans,
		BlockCount:    len(blocks),
		Size:          size,
	}, nil
}
