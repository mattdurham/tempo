package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"container/heap"
	"fmt"
	"maps"
	"slices"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// topKEntry holds one candidate result buffered during a StreamTopK scan.
type topKEntry struct {
	block    *modules_reader.Block
	ts       uint64
	blockIdx int
	rowIdx   int
}

// topKHeap implements heap.Interface.
// backward=true → min-heap (root = oldest entry, evicted first when full).
// backward=false → max-heap (root = newest entry, evicted first when full).
type topKHeap struct {
	entries  []topKEntry
	backward bool
}

func (h *topKHeap) Len() int { return len(h.entries) }
func (h *topKHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *topKHeap) Less(i, j int) bool {
	if h.backward {
		return h.entries[i].ts < h.entries[j].ts // min-heap: oldest at root
	}
	return h.entries[i].ts > h.entries[j].ts // max-heap: newest at root
}

func (h *topKHeap) Push(x any) { h.entries = append(h.entries, x.(topKEntry)) }
func (h *topKHeap) Pop() any {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return x
}

// worstTS returns the timestamp of the heap root (the entry evicted when full).
func (h *topKHeap) worstTS() uint64 { return h.entries[0].ts }

// topKSkipBlock returns true when a block's timestamp range cannot contain an entry
// better than the current heap root. Safe to skip when this returns true.
func topKSkipBlock(buf *topKHeap, limit int, backward bool, meta shared.BlockMeta) bool {
	if buf.Len() < limit {
		return false
	}
	worst := buf.worstTS()
	if backward {
		return meta.MaxStart != 0 && meta.MaxStart <= worst
	}
	return meta.MinStart != 0 && meta.MinStart >= worst
}

// topKInsert inserts entry into the heap, evicting the worst entry if the heap is full
// and entry is a better fit.
func topKInsert(buf *topKHeap, limit int, backward bool, entry topKEntry) {
	if buf.Len() < limit {
		heap.Push(buf, entry)
		return
	}
	worst := buf.worstTS()
	if backward && entry.ts > worst {
		heap.Pop(buf)
		heap.Push(buf, entry)
	} else if !backward && entry.ts < worst {
		heap.Pop(buf)
		heap.Push(buf, entry)
	}
}

// topKScanRows iterates rowIndices from the given block, applying time filtering and
// inserting qualifying rows into the heap.
func topKScanRows(
	buf *topKHeap,
	limit int,
	backward bool,
	block *modules_reader.Block,
	blockIdx int,
	tsCol *modules_reader.Column,
	timeRange queryplanner.TimeRange,
	rowIndices []int,
) {
	for _, rowIdx := range rowIndices {
		ts, ok := tsCol.Uint64Value(rowIdx)
		if !ok {
			continue
		}
		if timeRange.MinNano > 0 && ts < timeRange.MinNano {
			continue
		}
		if timeRange.MaxNano > 0 && ts > timeRange.MaxNano {
			continue
		}
		topKInsert(buf, limit, backward, topKEntry{
			ts: ts, blockIdx: blockIdx, rowIdx: rowIdx, block: block,
		})
	}
}

// topKScanRowsFromIntrinsic is the fallback for topKScanRows when the timestamp column
// is not stored in block columns (intrinsic-only storage format). It reads per-row
// timestamps directly from the reader's intrinsic section.
func topKScanRowsFromIntrinsic(
	buf *topKHeap,
	limit int,
	backward bool,
	r *modules_reader.Reader,
	block *modules_reader.Block,
	blockIdx int,
	tsColName string,
	timeRange queryplanner.TimeRange,
	rowIndices []int,
) {
	for _, rowIdx := range rowIndices {
		ts, ok := r.IntrinsicUint64At(tsColName, blockIdx, rowIdx)
		if !ok {
			continue
		}
		if timeRange.MinNano > 0 && ts < timeRange.MinNano {
			continue
		}
		if timeRange.MaxNano > 0 && ts > timeRange.MaxNano {
			continue
		}
		topKInsert(buf, limit, backward, topKEntry{
			ts: ts, blockIdx: blockIdx, rowIdx: rowIdx, block: block,
		})
	}
}

// topKScanBlocks iterates selected blocks, fetches them lazily, and fills buf.
// Returns (fetchedGroups, fetchedBlocks, bytesRead, error): fetchedGroups counts ReadGroup
// calls (IOOps); fetchedBlocks counts individual blocks fetched.
func topKScanBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	secondPassCols map[string]struct{},
	opts CollectOptions,
	plan *queryplanner.Plan,
	buf *topKHeap,
	groups []shared.CoalescedRead,
	blockToGroup map[int]int,
	backward bool,
) (int, int, int64, error) {
	fetched := make(map[int][]byte, len(plan.SelectedBlocks))
	fetchedGroupsSeen := make(map[int]struct{}, len(groups))
	skippedBlocks := make(map[int]struct{}, len(plan.SelectedBlocks))
	fetchedGroups := 0
	fetchCount := 0
	var bytesRead int64

	for _, blockIdx := range plan.SelectedBlocks {
		meta := r.BlockMeta(blockIdx)
		if topKSkipBlock(buf, opts.Limit, backward, meta) {
			gi2, ok2 := blockToGroup[blockIdx]
			if _, seen := fetchedGroupsSeen[gi2]; ok2 && seen {
				delete(fetched, blockIdx)
			} else {
				skippedBlocks[blockIdx] = struct{}{}
			}
			continue
		}

		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}
		if _, seen := fetchedGroupsSeen[gi]; !seen {
			groupRaw, fetchErr := r.ReadGroup(groups[gi])
			if fetchErr != nil {
				return fetchedGroups, fetchCount, bytesRead, fmt.Errorf("ReadGroup: %w", fetchErr)
			}
			maps.Copy(fetched, groupRaw)
			// Count bytes for entire fetched group — ReadGroup reads all blocks in the group.
			for _, bi := range groups[gi].BlockIDs {
				if _, skip := skippedBlocks[bi]; skip {
					delete(fetched, bi)
				}
				bytesRead += int64(r.BlockMeta(bi).Length) //nolint:gosec // Length is block size, safe to cast
			}
			fetchedGroups++
			fetchCount += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = struct{}{}
		}

		raw, rawOK := fetched[blockIdx]
		if !rawOK {
			continue
		}
		delete(fetched, blockIdx)

		r.ResetInternStrings()
		bwb, parseErr := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if parseErr != nil {
			return fetchedGroups, fetchCount, bytesRead, fmt.Errorf(
				"ParseBlockFromBytes block %d: %w",
				blockIdx,
				parseErr,
			)
		}

		provider := newBlockColumnProvider(bwb.Block)
		rowSet, evalErr := program.ColumnPredicate(provider)
		if evalErr != nil {
			return fetchedGroups, fetchCount, bytesRead, fmt.Errorf("ColumnPredicate block %d: %w", blockIdx, evalErr)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// Second pass: decode secondPassCols; the block is stored in the heap and
		// accessed by the caller when delivering results.
		if wantColumns != nil {
			bwb, parseErr = r.ParseBlockFromBytes(bwb.RawBytes, secondPassCols, meta)
			if parseErr != nil {
				return fetchedGroups, fetchCount, bytesRead, fmt.Errorf(
					"ParseBlockFromBytes (full) block %d: %w",
					blockIdx,
					parseErr,
				)
			}
		}

		tsCol := bwb.Block.GetColumn(opts.TimestampColumn)
		if tsCol == nil {
			// Timestamp column absent from block (intrinsic-only format): read from intrinsic section.
			topKScanRowsFromIntrinsic(buf, opts.Limit, backward, r, bwb.Block, blockIdx,
				opts.TimestampColumn, opts.TimeRange, rowSet.ToSlice())
		} else {
			topKScanRows(buf, opts.Limit, backward, bwb.Block, blockIdx, tsCol, opts.TimeRange, rowSet.ToSlice())
		}
	}
	return fetchedGroups, fetchCount, bytesRead, nil
}

// topKDeliver sorts the heap contents and returns them in direction order.
func topKDeliver(buf *topKHeap, backward bool) []MatchedRow {
	entries := buf.entries
	if backward {
		slices.SortFunc(entries, func(a, b topKEntry) int { return cmp.Compare(b.ts, a.ts) })
	} else {
		slices.SortFunc(entries, func(a, b topKEntry) int { return cmp.Compare(a.ts, b.ts) })
	}
	results := make([]MatchedRow, len(entries))
	for i, entry := range entries {
		results[i] = MatchedRow{Block: entry.block, BlockIdx: entry.blockIdx, RowIdx: entry.rowIdx}
	}
	return results
}
