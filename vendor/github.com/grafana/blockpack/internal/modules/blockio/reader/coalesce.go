package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// blockExtent is an internal tuple used during coalescing.
type blockExtent struct {
	offset   int64
	length   int64
	blockIdx int
}

// CoalesceBlocks merges adjacent block extents using cfg.
// blockOrder lists block indices to read (need not be sorted).
// Returns a slice of CoalescedRead structs ready for I/O.
func CoalesceBlocks(metas []shared.BlockMeta, blockOrder []int, cfg shared.CoalesceConfig) []shared.CoalescedRead {
	if len(blockOrder) == 0 {
		return nil
	}

	extents := make([]blockExtent, 0, len(blockOrder))
	for _, bi := range blockOrder {
		if bi < 0 || bi >= len(metas) {
			continue
		}

		extents = append(extents, blockExtent{
			offset:   int64(metas[bi].Offset), //nolint:gosec // safe: file offsets fit in int64
			length:   int64(metas[bi].Length), //nolint:gosec // safe: block lengths fit in int64
			blockIdx: bi,
		})
	}

	sort.Slice(extents, func(i, j int) bool {
		return extents[i].offset < extents[j].offset
	})

	var result []shared.CoalescedRead
	if len(extents) == 0 {
		return result
	}

	// Start the first merged range.
	cur := shared.CoalescedRead{
		Offset:       extents[0].offset,
		Length:       extents[0].length,
		BlockIDs:     []int{extents[0].blockIdx},
		BlockOffsets: []int64{extents[0].offset},
		BlockLengths: []int64{extents[0].length},
	}

	for _, e := range extents[1:] {
		curEnd := cur.Offset + cur.Length
		eEnd := e.offset + e.length

		// Determine the candidate merged end.
		mergedEnd := max(eEnd, curEnd)

		mergedLen := mergedEnd - cur.Offset
		usefulBytes := min(cur.Length+e.length, mergedLen) // approximate: may double-count overlaps

		gap := e.offset - curEnd
		wasteBytes := mergedLen - usefulBytes
		wasteRatio := float64(0)
		if mergedLen > 0 {
			wasteRatio = float64(wasteBytes) / float64(mergedLen)
		}

		canMerge := gap <= cfg.MaxGapBytes && wasteRatio <= cfg.MaxWasteRatio

		if canMerge {
			cur.Length = max(cur.Length, mergedEnd-cur.Offset)

			cur.BlockIDs = append(cur.BlockIDs, e.blockIdx)
			cur.BlockOffsets = append(cur.BlockOffsets, e.offset)
			cur.BlockLengths = append(cur.BlockLengths, e.length)
		} else {
			result = append(result, cur)
			cur = shared.CoalescedRead{
				Offset:       e.offset,
				Length:       e.length,
				BlockIDs:     []int{e.blockIdx},
				BlockOffsets: []int64{e.offset},
				BlockLengths: []int64{e.length},
			}
		}
	}

	result = append(result, cur)
	return result
}

// ReadCoalescedBlocks executes the merged I/O requests.
// Returns a map from block index to raw block bytes.
func ReadCoalescedBlocks(provider shared.ReaderProvider, cr []shared.CoalescedRead) (map[int][]byte, error) {
	result := make(map[int][]byte)

	for i := range cr {
		c := &cr[i]
		buf := make([]byte, c.Length)
		n, err := provider.ReadAt(buf, c.Offset, shared.DataTypeBlock)
		if err != nil {
			return nil, fmt.Errorf("coalesced read at offset %d length %d: %w", c.Offset, c.Length, err)
		}

		if int64(n) != c.Length {
			return nil, fmt.Errorf(
				"coalesced read at offset %d: short read, got %d want %d",
				c.Offset, n, c.Length,
			)
		}

		for j, blockID := range c.BlockIDs {
			bOff := c.BlockOffsets[j] - c.Offset
			bLen := c.BlockLengths[j]
			end := bOff + bLen
			if bOff < 0 || end > int64(len(buf)) {
				return nil, fmt.Errorf(
					"coalesced read: block %d slice [%d:%d] out of range for buf len %d",
					blockID, bOff, end, len(buf),
				)
			}

			blockBuf := make([]byte, bLen)
			copy(blockBuf, buf[bOff:end])
			result[blockID] = blockBuf
		}
	}

	return result, nil
}
