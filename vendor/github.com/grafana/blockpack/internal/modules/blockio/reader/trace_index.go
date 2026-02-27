package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// compactBlockEntry holds file location for one block as stored in the compact block table.
type compactBlockEntry struct {
	fileOffset uint64
	fileLength uint32
}

// BlocksForTraceID returns block indices containing the given trace ID.
// Uses the compact index if available, otherwise the main trace index.
func (r *Reader) BlocksForTraceID(traceID [16]byte) []int {
	if r.compactLen > 0 {
		if err := r.ensureCompactIndexParsed(); err == nil {
			blocks := r.BlocksForTraceIDCompact(traceID)
			if blocks != nil {
				return blocks
			}
		}
	}

	refs, ok := r.traceIndex[traceID]
	if !ok {
		return nil
	}

	blocks := make([]int, 0, len(refs))
	seen := make(map[int]struct{}, len(refs))
	for _, ref := range refs {
		bi := int(ref.blockID)
		if _, dup := seen[bi]; !dup {
			seen[bi] = struct{}{}
			blocks = append(blocks, bi)
		}
	}

	return blocks
}

// BlocksForTraceIDCompact searches the compact trace index.
func (r *Reader) BlocksForTraceIDCompact(traceID [16]byte) []int {
	if r.compactParsed == nil {
		return nil
	}

	refs, ok := r.compactParsed.traceIndex[traceID]
	if !ok {
		return nil
	}

	blocks := make([]int, 0, len(refs))
	seen := make(map[int]struct{}, len(refs))
	for _, ref := range refs {
		bi := int(ref.blockID)
		if _, dup := seen[bi]; !dup {
			seen[bi] = struct{}{}
			blocks = append(blocks, bi)
		}
	}

	return blocks
}

// GetTraceSpanIndices returns span row indices for a trace within a specific block.
func (r *Reader) GetTraceSpanIndices(traceID [16]byte, blockIdx int) []uint16 {
	refs, ok := r.traceIndex[traceID]
	if !ok {
		return nil
	}

	for _, ref := range refs {
		if int(ref.blockID) == blockIdx {
			return ref.spanIndices
		}
	}

	return nil
}

// ensureCompactIndexParsed lazily parses the compact trace index.
func (r *Reader) ensureCompactIndexParsed() error {
	if r.compactParsed != nil {
		return nil
	}

	if r.compactLen == 0 {
		return fmt.Errorf("compact index: not present")
	}

	data, err := r.readRange(r.compactOffset, uint64(r.compactLen), shared.DataTypeCompact)
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
	if version != shared.CompactIndexVersion {
		return fmt.Errorf("compact index: unsupported version %d", version)
	}

	blockCount := int(binary.LittleEndian.Uint32(data[5:]))
	pos := 9

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

	r.compactParsed = &compactTraceIndex{blockTable: blockTable, traceIndex: traceIdx}
	return nil
}
