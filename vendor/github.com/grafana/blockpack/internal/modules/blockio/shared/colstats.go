package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md
// of the writer/reader modules (NOTE-446, issue #364).

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// ColStats wire format (ToCSubTypeColStats = 9, issue #364).
//
// The section holds one entry per BLOCK, each entry holding packed statistics for the
// columns present in that block. It is consumed by the executor to prune column fetches
// and whole blocks before any per-block column I/O.
//
// Layout:
//
//	entry_count[4 LE]           // number of block entries
//	per block entry:
//	  block_idx[2 LE]           // which block
//	  col_count[2 LE]           // number of column stats entries
//	  per column:
//	    name_len[2 LE] + name[name_len]
//	    stats_flags[1]          // which optional fields follow (see ColStatFlag* below)
//	    present_count[4 LE]     // non-null row count; 0 = column absent → skip entirely
//	    if stats_flags & ColStatFlagNumRange: min_uint64[8 LE] + max_uint64[8 LE]
//
// Forward compatibility: a reader that does not understand a stats_flags bit MUST be able
// to skip the optional payload it implies. The only optional payload currently defined is
// the numeric range (ColStatFlagNumRange). Unknown high bits with no defined payload are
// ignored. To keep this safe, the writer never sets flags whose payload size the current
// decoder cannot compute.
const (
	// ColStatFlagNumRange indicates min_uint64[8 LE] + max_uint64[8 LE] follow present_count.
	// The two uint64s are the encoded numeric range key bits for the column in this block
	// (same encoding used by the range index), enabling per-block range pruning without
	// decoding the column blob.
	ColStatFlagNumRange uint8 = 0x01
)

// ColStat holds the per-block statistics for one column.
// Field order minimizes padding (betteralign): uint64s, then string, then uint32, then bool.
type ColStat struct {
	Name         string
	MinNum       uint64
	MaxNum       uint64
	PresentCount uint32
	HasNumRange  bool
}

// BlockColStats is the parsed per-block column statistics for one block.
// Slice first, then the small uint16 (betteralign).
type BlockColStats struct {
	Cols     []ColStat
	BlockIdx uint16
}

// EncodeColStatsSection serializes per-block column statistics into the ColStats wire format.
// blocks must be sorted by BlockIdx ascending; columns within each block are sorted by Name
// ascending for deterministic output. The caller owns ordering of the input; this function
// does not mutate the input slices but sorts a local copy of each block's columns.
func EncodeColStatsSection(blocks []BlockColStats) []byte {
	// Pre-size: header + per-block fixed + per-column estimate.
	size := 4
	for i := range blocks {
		size += 4 // block_idx[2] + col_count[2]
		for _, c := range blocks[i].Cols {
			size += 2 + len(c.Name) + 1 + 4
			if c.HasNumRange {
				size += 16
			}
		}
	}

	out := make([]byte, 0, size)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(blocks))) //nolint:gosec
	for i := range blocks {
		b := &blocks[i]
		out = binary.LittleEndian.AppendUint16(out, b.BlockIdx)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Cols))) //nolint:gosec

		// Sort columns by name for deterministic output without mutating the caller's slice.
		cols := make([]ColStat, len(b.Cols))
		copy(cols, b.Cols)
		sort.Slice(cols, func(a, c int) bool { return cols[a].Name < cols[c].Name })

		for j := range cols {
			c := &cols[j]
			out = binary.LittleEndian.AppendUint16(out, uint16(len(c.Name))) //nolint:gosec
			out = append(out, c.Name...)
			var flags uint8
			if c.HasNumRange {
				flags |= ColStatFlagNumRange
			}
			out = append(out, flags)
			out = binary.LittleEndian.AppendUint32(out, c.PresentCount)
			if c.HasNumRange {
				out = binary.LittleEndian.AppendUint64(out, c.MinNum)
				out = binary.LittleEndian.AppendUint64(out, c.MaxNum)
			}
		}
	}
	return out
}

// DecodeColStatsSection parses a ColStats wire blob into a map from block index to that
// block's column statistics. Returns an error on truncation or malformed length prefixes.
func DecodeColStatsSection(raw []byte) (map[int]*BlockColStats, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) < 4 {
		return nil, fmt.Errorf("colstats: section too short (%d bytes)", len(raw))
	}
	entryCount := binary.LittleEndian.Uint32(raw[0:])
	pos := 4
	out := make(map[int]*BlockColStats, entryCount)
	for e := uint32(0); e < entryCount; e++ {
		if pos+4 > len(raw) {
			return nil, fmt.Errorf("colstats: truncated block header at entry %d", e)
		}
		blockIdx := binary.LittleEndian.Uint16(raw[pos:])
		colCount := binary.LittleEndian.Uint16(raw[pos+2:])
		pos += 4
		bcs := &BlockColStats{BlockIdx: blockIdx, Cols: make([]ColStat, 0, colCount)}
		for c := uint16(0); c < colCount; c++ {
			if pos+2 > len(raw) {
				return nil, fmt.Errorf("colstats: truncated name_len in block %d", blockIdx)
			}
			nameLen := int(binary.LittleEndian.Uint16(raw[pos:]))
			pos += 2
			if pos+nameLen > len(raw) {
				return nil, fmt.Errorf("colstats: truncated name in block %d", blockIdx)
			}
			name := string(raw[pos : pos+nameLen])
			pos += nameLen
			if pos+1+4 > len(raw) {
				return nil, fmt.Errorf("colstats: truncated stats for %q in block %d", name, blockIdx)
			}
			flags := raw[pos]
			pos++
			cs := ColStat{Name: name, PresentCount: binary.LittleEndian.Uint32(raw[pos:])}
			pos += 4
			if flags&ColStatFlagNumRange != 0 {
				if pos+16 > len(raw) {
					return nil, fmt.Errorf("colstats: truncated num range for %q in block %d", name, blockIdx)
				}
				cs.MinNum = binary.LittleEndian.Uint64(raw[pos:])
				cs.MaxNum = binary.LittleEndian.Uint64(raw[pos+8:])
				cs.HasNumRange = true
				pos += 16
			}
			bcs.Cols = append(bcs.Cols, cs)
		}
		out[int(blockIdx)] = bcs
	}
	return out, nil
}

// Lookup returns the ColStat for the named column in this block, or nil if absent.
func (b *BlockColStats) Lookup(name string) *ColStat {
	for i := range b.Cols {
		if b.Cols[i].Name == name {
			return &b.Cols[i]
		}
	}
	return nil
}
