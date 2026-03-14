package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"container/heap"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// logTopKEntry holds one pipeline-passing log row buffered during a CollectLogs scan.
// The processed LogEntry is stored (not block/row references) because the pipeline may
// mutate labels in-place and the block may not be available at delivery time.
type logTopKEntry struct {
	entry *LogEntry
	ts    uint64
}

// logTopKHeap implements heap.Interface over logTopKEntry.
// backward=true  → min-heap (root = oldest entry, evicted first when full).
// backward=false → max-heap (root = newest entry, evicted first when full).
type logTopKHeap struct {
	entries  []logTopKEntry
	backward bool
}

func (h *logTopKHeap) Len() int { return len(h.entries) }
func (h *logTopKHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *logTopKHeap) Less(i, j int) bool {
	if h.backward {
		return h.entries[i].ts < h.entries[j].ts // min-heap: oldest at root
	}
	return h.entries[i].ts > h.entries[j].ts // max-heap: newest at root
}

func (h *logTopKHeap) Push(x any) { h.entries = append(h.entries, x.(logTopKEntry)) }
func (h *logTopKHeap) Pop() any {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return x
}

// logTopKCanSkipBlock returns true when the heap is full and the block's timestamp
// range contains no entry that can displace the current heap root.
func logTopKCanSkipBlock(buf *logTopKHeap, limit int, backward bool, meta shared.BlockMeta) bool {
	if buf.Len() < limit {
		return false
	}
	worst := buf.entries[0].ts
	if backward {
		// min-heap root is oldest; skip if even the block's newest entry is no better.
		return meta.MaxStart != 0 && meta.MaxStart <= worst
	}
	// max-heap root is newest; skip if even the block's oldest entry is no better.
	return meta.MinStart != 0 && meta.MinStart >= worst
}

// logTopKInsert inserts entry into buf. When full, evicts the worst entry only if
// the new entry is a better fit.
func logTopKInsert(buf *logTopKHeap, limit int, backward bool, entry logTopKEntry) {
	if buf.Len() < limit {
		heap.Push(buf, entry)
		return
	}
	worst := buf.entries[0].ts
	if backward && entry.ts > worst {
		heap.Pop(buf)
		heap.Push(buf, entry)
	} else if !backward && entry.ts < worst {
		heap.Pop(buf)
		heap.Push(buf, entry)
	}
}

// CollectLogs collects the globally top opts.Limit log rows by per-row timestamp,
// applying the pipeline per row before inserting into the heap. Returns results as a
// sorted slice after the full scan completes.
//
// For Backward direction: opts.Limit newest entries (descending timestamp).
// For Forward direction: opts.Limit oldest entries (ascending timestamp).
//
// Block-level early termination: once the heap is full, blocks whose entire
// timestamp range cannot contain a better entry than the heap root are skipped.
// For Backward: blocks with MaxStart <= heap.root.ts are skipped.
// For Forward: blocks with MinStart >= heap.root.ts are skipped.
//
// When opts.Limit == 0, all pipeline-passing rows are collected, sorted, and delivered.
//
// Block-level time pruning: passes opts.TimeRange to planner.Plan when provided,
// allowing blocks outside the time window to be eliminated before fetch.
// When opts.TimeRange is zero, no block-level pruning is applied (same as before).
// Per-row time filtering via opts.TimeRange.MinNano/MaxNano (NOTE-021) applies
// in addition for sub-block granularity.
func CollectLogs(
	r *modules_reader.Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
) ([]*LogEntry, error) {
	if r == nil {
		return nil, nil
	}
	if program == nil {
		return nil, fmt.Errorf("CollectLogs: program cannot be nil")
	}

	planner := queryplanner.NewPlanner(r)
	predicates := buildPredicates(r, program)
	// NOTE-023: forward opts.TimeRange for block-level pruning when provided.
	// When opts.TimeRange is zero, this is identical to the previous behavior.
	plan := planner.Plan(predicates, queryplanner.TimeRange{
		MinNano: opts.TimeRange.MinNano,
		MaxNano: opts.TimeRange.MaxNano,
	})

	fetchedBlocks := 0
	if opts.OnStats != nil {
		defer func() {
			opts.OnStats(CollectStats{
				TotalBlocks:    plan.TotalBlocks,
				PrunedByTime:   plan.PrunedByTime,
				PrunedByIndex:  plan.PrunedByIndex,
				PrunedByFuse:   plan.PrunedByFuse,
				PrunedByCMS:    plan.PrunedByCMS,
				SelectedBlocks: len(plan.SelectedBlocks),
				FetchedBlocks:  fetchedBlocks,
				Explain:        plan.Explain,
			})
		}()
	}

	if len(plan.SelectedBlocks) == 0 {
		return nil, nil
	}

	wantColumns := ProgramWantColumns(program)
	// NOTE-021: ensure log:timestamp is always in the first-pass block for time pre-filtering.
	// Also include resource.__loki_labels__ so callers accessing stream-label metadata via
	// GetField("resource.__loki_labels__") or IterateFields always find a decoded value
	// regardless of whether it appeared in a query predicate.
	if wantColumns != nil {
		wantCopy := make(map[string]struct{}, len(wantColumns)+2)
		maps.Copy(wantCopy, wantColumns)
		wantCopy["log:timestamp"] = struct{}{}
		wantCopy["resource.__loki_labels__"] = struct{}{}
		wantColumns = wantCopy
	}

	backward := opts.Direction == queryplanner.Backward
	var fb int
	var scanErr error

	if opts.Limit > 0 {
		buf := &logTopKHeap{
			entries:  make([]logTopKEntry, 0, opts.Limit),
			backward: backward,
		}
		fb, scanErr = logTopKScan(r, program, wantColumns, pipeline, opts, plan, buf, backward)
		if scanErr != nil {
			return nil, scanErr
		}
		fetchedBlocks = fb
		return logTopKDeliver(buf, backward), nil
	}

	// Limit == 0: collect all, sort globally, deliver.
	var all []logTopKEntry
	fb, scanErr = logCollectAll(r, program, wantColumns, pipeline, opts, plan, &all)
	if scanErr != nil {
		return nil, scanErr
	}
	fetchedBlocks = fb
	return logDeliverAll(all, backward), nil
}

// logTopKScan iterates selected blocks, applies block-level skip checks, fetches
// lazily, and fills buf with the top-limit pipeline-passing rows.
func logTopKScan(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
	plan *queryplanner.Plan,
	buf *logTopKHeap,
	backward bool,
) (int, error) {
	groups := r.CoalescedGroups(plan.SelectedBlocks)
	blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			blockToGroup[bi] = gi
		}
	}

	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)
	fetchCount := 0

	for _, blockIdx := range plan.SelectedBlocks {
		meta := r.BlockMeta(blockIdx)
		if logTopKCanSkipBlock(buf, opts.Limit, backward, meta) {
			delete(fetched, blockIdx) // release bytes if already fetched in this group
			continue
		}

		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}
		if !fetchedGroupsSeen[gi] {
			groupRaw, err := r.ReadGroup(groups[gi])
			if err != nil {
				return fetchCount, fmt.Errorf("CollectLogs ReadGroup: %w", err)
			}
			maps.Copy(fetched, groupRaw)
			fetchCount += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = true
		}

		raw, ok := fetched[blockIdx]
		if !ok {
			continue
		}
		delete(fetched, blockIdx)

		r.ResetInternStrings()
		bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if err != nil {
			return fetchCount, fmt.Errorf("CollectLogs ParseBlockFromBytes block %d: %w", blockIdx, err)
		}

		rowSet, err := program.ColumnPredicate(NewColumnProvider(bwb.Block))
		if err != nil {
			return fetchCount, fmt.Errorf("CollectLogs ColumnPredicate block %d: %w", blockIdx, err)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-021: pre-filter by time using first-pass block before full second-pass decode.
		// log:timestamp is guaranteed present (injected into wantColumns above).
		var keptByTime []int
		if opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0 {
			firstPassTSCol := bwb.Block.GetColumn("log:timestamp")
			for _, rowIdx := range rowSet.ToSlice() {
				ts := uint64(0)
				if firstPassTSCol != nil {
					if v, ok := firstPassTSCol.Uint64Value(rowIdx); ok {
						ts = v
					}
				}
				if opts.TimeRange.MinNano > 0 && ts < opts.TimeRange.MinNano {
					continue
				}
				if opts.TimeRange.MaxNano > 0 && ts > opts.TimeRange.MaxNano {
					continue
				}
				keptByTime = append(keptByTime, rowIdx)
			}
			if len(keptByTime) == 0 {
				continue // skip second-pass decode entirely
			}
		} else {
			keptByTime = rowSet.ToSlice()
		}

		// NOTE-001: Lazy registration in ParseBlockFromBytes registers all columns with
		// presence-only decode. Full decode is triggered on first value access - no AddColumnsToBlock needed.

		// Cache column pointers for the row loop.
		tsCol := bwb.Block.GetColumn("log:timestamp")
		bodyCol := bwb.Block.GetColumn("log:body")
		colNames, colMap, colCols := buildBlockColMaps(bwb.Block)
		logStrNames, logStrCols := buildLogStringColCache(bwb.Block)
		skipParsers := pipeline != nil && blockHasBodyParsed(bwb.Block)
		for _, rowIdx := range keptByTime {
			var ts uint64
			if tsCol != nil {
				if v, ok := tsCol.Uint64Value(rowIdx); ok {
					ts = v
				}
			}
			var line string
			if bodyCol != nil {
				if v, ok := bodyCol.StringValue(rowIdx); ok {
					line = v
				}
			}

			// NOTE-SL-016: acquire from pool - zero map allocation for dropped rows.
			bls := acquireBlockLabelSet(bwb.Block, rowIdx, colNames, colMap, colCols)
			var labels logqlparser.LabelSet = bls
			if pipeline != nil {
				var keep bool
				if skipParsers {
					line, labels, keep = pipeline.ProcessSkipParsers(ts, line, bls)
				} else {
					line, labels, keep = pipeline.Process(ts, line, bls)
				}
				if !keep {
					releaseBlockLabelSet(bls)
					continue
				}
			}
			// NOTE-031: Early skip - if heap is full and this row's ts cannot displace the
			// current worst entry, skip materialization entirely. Pipeline must still run
			// above (for keep/drop decisions) but map allocations are avoided for the ~99%
			// of rows that don't make it into the top-K.
			if buf.Len() >= opts.Limit {
				worst := buf.entries[0].ts
				if (backward && ts <= worst) || (!backward && ts >= worst) {
					releaseBlockLabelSet(bls)
					continue
				}
			}
			// Materialize to stable map before storing in heap (bls will be released).
			matLabels := labels.Materialize()
			logAttrs := collectLogStringAttrs(logStrNames, logStrCols, rowIdx)
			releaseBlockLabelSet(bls)
			entry := &LogEntry{TimestampNanos: ts, Line: line, Labels: matLabels, LogAttrs: logAttrs}
			logTopKInsert(buf, opts.Limit, backward, logTopKEntry{entry: entry, ts: ts})
		}
	}
	return fetchCount, nil
}

// logCollectAll scans all selected blocks without heap pruning, collecting every
// pipeline-passing row. Used by CollectLogs when opts.Limit == 0.
func logCollectAll(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
	plan *queryplanner.Plan,
	all *[]logTopKEntry,
) (int, error) {
	groups := r.CoalescedGroups(plan.SelectedBlocks)
	blockToGroup := make(map[int]int, len(plan.SelectedBlocks))
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			blockToGroup[bi] = gi
		}
	}

	fetched := make(map[int][]byte)
	fetchedGroupsSeen := make(map[int]bool)
	fetchCount := 0

	for _, blockIdx := range plan.SelectedBlocks {
		gi, ok := blockToGroup[blockIdx]
		if !ok {
			continue
		}
		if !fetchedGroupsSeen[gi] {
			groupRaw, err := r.ReadGroup(groups[gi])
			if err != nil {
				return fetchCount, fmt.Errorf("CollectLogs ReadGroup: %w", err)
			}
			maps.Copy(fetched, groupRaw)
			fetchCount += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = true
		}

		meta := r.BlockMeta(blockIdx)
		raw, ok := fetched[blockIdx]
		if !ok {
			continue
		}
		delete(fetched, blockIdx)

		r.ResetInternStrings()
		bwb, err := r.ParseBlockFromBytes(raw, wantColumns, meta)
		if err != nil {
			return fetchCount, fmt.Errorf("CollectLogs ParseBlockFromBytes block %d: %w", blockIdx, err)
		}

		rowSet, err := program.ColumnPredicate(NewColumnProvider(bwb.Block))
		if err != nil {
			return fetchCount, fmt.Errorf("CollectLogs ColumnPredicate block %d: %w", blockIdx, err)
		}

		if rowSet.Size() == 0 {
			continue
		}

		// NOTE-021: pre-filter by time using first-pass block before full second-pass decode.
		// log:timestamp is guaranteed present (injected into wantColumns above).
		var keptByTime []int
		if opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0 {
			firstPassTSCol := bwb.Block.GetColumn("log:timestamp")
			for _, rowIdx := range rowSet.ToSlice() {
				ts := uint64(0)
				if firstPassTSCol != nil {
					if v, ok := firstPassTSCol.Uint64Value(rowIdx); ok {
						ts = v
					}
				}
				if opts.TimeRange.MinNano > 0 && ts < opts.TimeRange.MinNano {
					continue
				}
				if opts.TimeRange.MaxNano > 0 && ts > opts.TimeRange.MaxNano {
					continue
				}
				keptByTime = append(keptByTime, rowIdx)
			}
			if len(keptByTime) == 0 {
				continue // skip second-pass decode entirely
			}
		} else {
			keptByTime = rowSet.ToSlice()
		}

		// NOTE-001: Lazy registration in ParseBlockFromBytes registers all columns with
		// presence-only decode. Full decode is triggered on first value access - no AddColumnsToBlock needed.

		// Cache column pointers for the row loop.
		tsCol := bwb.Block.GetColumn("log:timestamp")
		bodyCol := bwb.Block.GetColumn("log:body")
		colNames, colMap, colCols := buildBlockColMaps(bwb.Block)
		logStrNames, logStrCols := buildLogStringColCache(bwb.Block)
		skipParsers := pipeline != nil && blockHasBodyParsed(bwb.Block)
		for _, rowIdx := range keptByTime {
			var ts uint64
			if tsCol != nil {
				if v, ok := tsCol.Uint64Value(rowIdx); ok {
					ts = v
				}
			}
			var line string
			if bodyCol != nil {
				if v, ok := bodyCol.StringValue(rowIdx); ok {
					line = v
				}
			}

			// NOTE-SL-016: acquire from pool - zero map allocation for dropped rows.
			bls := acquireBlockLabelSet(bwb.Block, rowIdx, colNames, colMap, colCols)
			var labels logqlparser.LabelSet = bls
			if pipeline != nil {
				var keep bool
				if skipParsers {
					line, labels, keep = pipeline.ProcessSkipParsers(ts, line, bls)
				} else {
					line, labels, keep = pipeline.Process(ts, line, bls)
				}
				if !keep {
					releaseBlockLabelSet(bls)
					continue
				}
			}
			// Materialize to stable map before storing in slice (bls will be released).
			matLabels := labels.Materialize()
			logAttrs := collectLogStringAttrs(logStrNames, logStrCols, rowIdx)
			releaseBlockLabelSet(bls)
			entry := &LogEntry{TimestampNanos: ts, Line: line, Labels: matLabels, LogAttrs: logAttrs}
			*all = append(*all, logTopKEntry{entry: entry, ts: ts})
		}
	}
	return fetchCount, nil
}

// logTopKDeliver sorts the heap entries and returns them in direction order.
func logTopKDeliver(buf *logTopKHeap, backward bool) []*LogEntry {
	return logDeliverAll(buf.entries, backward)
}

// buildLogStringColCache builds a per-block cache of log.* string-valued columns.
// Called once per block; the result is passed to collectLogStringAttrs per row.
// Iterates block.Columns() directly (not buildBlockColMaps output) so that ALL original
// LogRecord attributes are collected regardless of resource.* precedence rules -
// a "log.foo" column must appear in SM even when "resource.foo" exists in the same block.
// Includes ColumnTypeString and ColumnTypeUUID (writer may auto-detect UUID-shaped strings).
// Body-auto-parsed columns (ColumnTypeRangeString) are intentionally excluded.
func buildLogStringColCache(block *modules_reader.Block) (names []string, cols []*modules_reader.Column) {
	for key, col := range block.Columns() {
		if !strings.HasPrefix(key.Name, "log.") {
			continue
		}
		if key.Type == shared.ColumnTypeString || key.Type == shared.ColumnTypeUUID {
			names = append(names, key.Name)
			cols = append(cols, col)
		}
	}
	return names, cols
}

// collectLogStringAttrs collects log.* string-valued column values for rowIdx
// using the per-block cache built by buildLogStringColCache.
// Returns nil if no columns are present at this row.
// Empty-but-present string values are included (IsPresent controls absence, not value content).
func collectLogStringAttrs(names []string, cols []*modules_reader.Column, rowIdx int) map[string]string {
	var result map[string]string
	for i, name := range names {
		col := cols[i]
		if col == nil || !col.IsPresent(rowIdx) {
			continue
		}
		v, ok := col.StringValue(rowIdx)
		if !ok {
			continue
		}
		if result == nil {
			result = make(map[string]string, len(names))
		}
		result[name] = v
	}
	return result
}

// logDeliverAll sorts entries and returns them in direction order.
func logDeliverAll(entries []logTopKEntry, backward bool) []*LogEntry {
	if backward {
		slices.SortFunc(entries, func(a, b logTopKEntry) int { return cmp.Compare(b.ts, a.ts) })
	} else {
		slices.SortFunc(entries, func(a, b logTopKEntry) int { return cmp.Compare(a.ts, b.ts) })
	}
	results := make([]*LogEntry, len(entries))
	for i, e := range entries {
		results[i] = e.entry
	}
	return results
}
