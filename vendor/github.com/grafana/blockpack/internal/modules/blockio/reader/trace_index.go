package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// compactBlockEntry holds file location for one block as stored in the compact block table.
type compactBlockEntry struct {
	fileOffset uint64
	fileLength uint32
}

// BlocksForTraceID returns block indices containing the given trace ID.
// Uses the compact index as a fast path when available. If the compact index
// returns nil (bloom rejected the ID or ID not in hash map), falls through to
// the full trace index — which is the authoritative answer for full readers.
// For lean readers (NewLeanReaderFromProvider), r.traceIndex is always empty so
// the compact index result is final.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	if r.compactLen > 0 {
		if err := r.ensureCompactIndexParsed(); err == nil {
			blocks := r.BlocksForTraceIDCompact(traceID)
			if blocks != nil {
				return blocks
			}
		}
	}

	blockIDs, ok := r.traceIndex[traceID]
	if !ok {
		return nil
	}

	return blockIDsToInts(blockIDs)
}

// BlocksForTraceIDCompact searches the compact trace index.
// Checks the trace ID bloom filter first — returns nil immediately if the bloom
// reports the ID as definitely absent, avoiding a hash map lookup.
func (r *Reader) BlocksForTraceIDCompact(traceID [16]byte) []int {
	if r.compactParsed == nil {
		return nil
	}

	// NOTE-36 / SPEC-10.2: bloom filter check before hash map — fast path for absent trace IDs.
	if !shared.TestTraceIDBloom(r.compactParsed.traceIDBloom, traceID) {
		return nil
	}

	blockIDs, ok := r.compactParsed.traceIndex[traceID]
	if !ok {
		return nil
	}

	return blockIDsToInts(blockIDs)
}

// blockIDsToInts converts a []uint16 block ID list to a deduplicated []int slice.
// The v2 writer never produces duplicate block IDs, but v1 files could theoretically
// contain them; the dedup map is retained for safety when reading old files.
func blockIDsToInts(blockIDs []uint16) []int {
	blocks := make([]int, 0, len(blockIDs))
	seen := make(map[int]struct{}, len(blockIDs))
	for _, bid := range blockIDs {
		bi := int(bid)
		if _, dup := seen[bi]; !dup {
			seen[bi] = struct{}{}
			blocks = append(blocks, bi)
		}
	}
	return blocks
}

// ensureCompactIndexParsed lazily parses the compact trace index.
// Supports version 1 (no bloom) and version 2 (with trace ID bloom filter).
func (r *Reader) ensureCompactIndexParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

	cacheKey := r.fileID + "/compact"

	data, err := r.cache.GetOrFetch(cacheKey, func() ([]byte, error) {
		return r.readRange(r.compactOffset, uint64(r.compactLen), rw.DataTypeTraceBloomFilter)
	})
	if err != nil {
		return fmt.Errorf("compact index: read: %w", err)
	}

	if len(data) < 9 {
		return fmt.Errorf("compact index: too short (%d bytes)", len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.CompactIndexMagic {
		return fmt.Errorf("compact index: bad magic 0x%08X", magic)
	}

	version := data[4]
	if version != shared.CompactIndexVersion && version != shared.CompactIndexVersion2 {
		return fmt.Errorf("compact index: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(data[5:])) //nolint:gosec // validated below before use
	if blockCount > shared.MaxBlocks {
		return fmt.Errorf("compact index: block_count %d exceeds maximum %d", blockCount, shared.MaxBlocks)
	}
	pos := 9

	// Version 2: parse trace ID bloom filter before the block table.
	var traceIDBloom []byte
	if version == shared.CompactIndexVersion2 {
		if pos+4 > len(data) {
			return fmt.Errorf("compact index: short for bloom_bytes")
		}
		bloomBytes := int(binary.LittleEndian.Uint32(data[pos:])) //nolint:gosec // validated below before use
		pos += 4
		if bloomBytes > shared.TraceIDBloomMaxBytes {
			return fmt.Errorf(
				"compact index: bloom_bytes %d exceeds maximum %d",
				bloomBytes, shared.TraceIDBloomMaxBytes,
			)
		}
		if pos+bloomBytes > len(data) {
			return fmt.Errorf("compact index: short for bloom_data (need %d bytes)", bloomBytes)
		}
		traceIDBloom = make([]byte, bloomBytes)
		copy(traceIDBloom, data[pos:pos+bloomBytes])
		pos += bloomBytes
	}

	// Parse block_table: block_count × 12 bytes (file_offset[8] + file_length[4]).
	need := blockCount * 12
	if pos+need > len(data) {
		return fmt.Errorf("compact index: short for block_table (need %d)", need)
	}

	blockTable := make([]compactBlockEntry, blockCount)
	for i := range blockCount {
		blockTable[i] = compactBlockEntry{
			fileOffset: binary.LittleEndian.Uint64(data[pos:]),
			fileLength: binary.LittleEndian.Uint32(data[pos+8:]),
		}
		pos += 12
	}

	// Parse trace index in the same format as the main trace block index.
	traceIdx, _, err := parseTraceBlockIndex(data[pos:])
	if err != nil {
		return fmt.Errorf("compact index: trace_index: %w", err)
	}

	r.compactParsed = &compactTraceIndex{
		blockTable:   blockTable,
		traceIndex:   traceIdx,
		traceIDBloom: traceIDBloom,
	}
	return nil
}
