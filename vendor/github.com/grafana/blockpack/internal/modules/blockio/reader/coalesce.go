package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/rw"
)

// coalescedReadPool holds reusable read buffers for ReadCoalescedBlocks.
// Each buffer is returned immediately after per-block copies are made,
// eliminating the ~90 GB/s allocation hot spot from repeated large mallocs.
// Buffers larger than coalescedReadPoolMaxBytes are not returned to the pool —
// an oversized allocation from a pathological block would otherwise sit in the
// pool indefinitely, inflating steady-state RSS.
var coalescedReadPool = &sync.Pool{
	New: func() any {
		b := make([]byte, 0, 8*1024*1024) // 8 MB initial capacity
		return &b
	},
}

// coalescedReadPoolMaxBytes is the largest buffer returned to the pool.
// Reads larger than this are handled by a fresh allocation that is GC'd normally.
const coalescedReadPoolMaxBytes = 32 * 1024 * 1024 // 32 MB

// returnToPool returns a pooled buffer if it is within the size limit.
func returnToPool(p *[]byte) {
	if int64(cap(*p)) <= coalescedReadPoolMaxBytes {
		coalescedReadPool.Put(p)
	}
}

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

	slices.SortFunc(extents, func(a, b blockExtent) int { return cmp.Compare(a.offset, b.offset) })

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

		canMerge := gap <= cfg.MaxGapBytes && wasteRatio <= cfg.MaxWasteRatio &&
			(cfg.MaxReadBytes <= 0 || mergedLen <= cfg.MaxReadBytes)

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
//
// Each coalesced read uses a pooled buffer for the S3 fetch. Block data is
// copied into independent per-block allocations so the pooled buffer can be
// returned immediately — this eliminates the large-buffer allocation hot spot
// without retaining ~8 MB per coalesced group for the caller's lifetime.
func ReadCoalescedBlocks(provider rw.ReaderProvider, cr []shared.CoalescedRead) (map[int][]byte, error) {
	result := make(map[int][]byte)

	for i := range cr {
		c := &cr[i]

		// Acquire a pooled read buffer; grow if needed.
		bufPtr := coalescedReadPool.Get().(*[]byte)
		if int64(cap(*bufPtr)) < c.Length {
			*bufPtr = make([]byte, c.Length)
		} else {
			*bufPtr = (*bufPtr)[:c.Length]
		}
		buf := *bufPtr

		n, err := provider.ReadAt(buf, c.Offset, rw.DataTypeBlock)
		if err != nil {
			returnToPool(bufPtr)
			return nil, fmt.Errorf("coalesced read at offset %d length %d: %w", c.Offset, c.Length, err)
		}

		if int64(n) != c.Length {
			returnToPool(bufPtr)
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
				returnToPool(bufPtr)
				return nil, fmt.Errorf(
					"coalesced read: block %d slice [%d:%d] out of range for buf len %d",
					blockID, bOff, end, len(buf),
				)
			}

			// Copy into an independent per-block allocation so the pooled buf
			// can be returned immediately below (no shared sub-slice aliases).
			block := make([]byte, bLen)
			copy(block, buf[bOff:end])
			result[blockID] = block
		}

		// Return pool buffer now — all block data has been copied out.
		returnToPool(bufPtr)
	}

	return result, nil
}
