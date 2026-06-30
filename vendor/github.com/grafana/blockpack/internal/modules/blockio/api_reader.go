package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

// ReaderProvider is the storage backend interface.
// All implementations must be safe for concurrent use.
type ReaderProvider = modules_rw.ReaderProvider

// DataType is a hint passed to ReaderProvider.ReadAt for caching layers.
type DataType = modules_rw.DataType

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
type DefaultProvider = modules_rw.DefaultProvider

// TrackingReaderProvider wraps a ReaderProvider and counts I/O calls and bytes.
type TrackingReaderProvider = modules_rw.TrackingReaderProvider

// RangeCachingProvider wraps a ReaderProvider with sub-range caching.
type RangeCachingProvider = modules_rw.RangeCachingProvider

// IOHealth is the guardrail classification of read-path I/O efficiency
// (io_ops and bytes/io) against the documented bands.
type IOHealth = modules_rw.IOHealth

// IOBand classifies an I/O metric against the documented efficiency bands
// (good / warning / critical).
type IOBand = modules_rw.IOBand

// IOBand values, ordered good < warning < critical so callers can take the
// worst band across metrics and gate on a single threshold.
const (
	BandGood     = modules_rw.BandGood
	BandWarning  = modules_rw.BandWarning
	BandCritical = modules_rw.BandCritical
)

// EvaluateIOHealth classifies raw io_ops / bytes_read counters against the
// documented bands. Pure: callers may evaluate per-query-phase counter deltas.
func EvaluateIOHealth(ioOps, bytesRead int64) IOHealth {
	return modules_rw.EvaluateIOHealth(ioOps, bytesRead)
}

// NewReaderFromProvider constructs a Reader from any storage backend.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return reader.NewReaderFromProvider(provider)
}

// NewBytesProvider returns a ReaderProvider backed by data.
// The caller must not modify data after this call.
func NewBytesProvider(data []byte) ReaderProvider {
	return modules_rw.NewBytesProvider(data)
}

// ReadBlockByRef fetches a v2 block directly by its page-addressed reference without
// a TOC lookup (NOTE-VI-027, issue #417 PR6). Only valid for v2 files (IsV2Format).
func ReadBlockByRef(r *Reader, pageNum uint32, lenPages uint16) ([]byte, error) {
	return r.ReadBlockByRef(pageNum, lenPages)
}

// NewDefaultProvider wraps a storage provider with tracking + range caching.
func NewDefaultProvider(underlying ReaderProvider) *DefaultProvider {
	return modules_rw.NewDefaultProvider(underlying)
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
