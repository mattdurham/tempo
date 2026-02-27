package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ReaderProvider is the storage backend interface.
// All implementations must be safe for concurrent use.
type ReaderProvider = shared.ReaderProvider

// DataType is a hint passed to ReaderProvider.ReadAt for caching layers.
type DataType = shared.DataType

// BlockMeta holds the parsed block index entry.
type BlockMeta = shared.BlockMeta

// CoalesceConfig controls how adjacent block reads are merged.
type CoalesceConfig = shared.CoalesceConfig

// CoalescedRead describes a single merged I/O request.
type CoalescedRead = shared.CoalescedRead

// AggressiveCoalesceConfig merges within 4 MB with no waste limit.
var AggressiveCoalesceConfig = shared.AggressiveCoalesceConfig

// Reader reads and decodes a blockpack file.
type Reader = reader.Reader

// Block holds decoded columns for a single block.
type Block = reader.Block

// BlockWithBytes bundles a decoded Block with its raw bytes for AddColumnsToBlock.
type BlockWithBytes = reader.BlockWithBytes

// Column holds a decoded column ready for query evaluation.
type Column = reader.Column

// DefaultProvider is the standard provider composition:
// outer range cache, inner tracking, innermost user storage.
type DefaultProvider = reader.DefaultProvider

// TrackingReaderProvider wraps a ReaderProvider and counts I/O calls and bytes.
type TrackingReaderProvider = reader.TrackingReaderProvider

// RangeCachingProvider wraps a ReaderProvider with sub-range caching.
type RangeCachingProvider = reader.RangeCachingProvider

// ReaderOption is a functional option for NewReaderFromProvider.
type ReaderOption = reader.Option

// NewReaderFromProvider constructs a Reader from any storage backend.
func NewReaderFromProvider(provider ReaderProvider, opts ...ReaderOption) (*Reader, error) {
	return reader.NewReaderFromProvider(provider, opts...)
}

// NewDefaultProvider wraps a storage provider with tracking + range caching.
func NewDefaultProvider(underlying ReaderProvider) *DefaultProvider {
	return reader.NewDefaultProvider(underlying)
}

// CoalesceBlocks merges adjacent block reads per cfg.
func CoalesceBlocks(metas []BlockMeta, blockOrder []int, cfg CoalesceConfig) []CoalescedRead {
	return reader.CoalesceBlocks(metas, blockOrder, cfg)
}

// ReadCoalescedBlocks executes merged I/O and returns per-block byte slices.
func ReadCoalescedBlocks(provider ReaderProvider, cr []CoalescedRead) (map[int][]byte, error) {
	return reader.ReadCoalescedBlocks(provider, cr)
}

// ReadBlocks is a convenience wrapper: it coalesces and reads the given block
// indices in as few I/O operations as possible. Equivalent to calling
// CoalesceBlocks + ReadCoalescedBlocks with AggressiveCoalesceConfig.
func ReadBlocks(r *Reader, blockIndices []int) (map[int][]byte, error) {
	return r.ReadBlocks(blockIndices)
}
