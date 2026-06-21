package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// rowIndexScratchPool pools *[]int scratch slices used in filterRowsByTimeRange and
// collectMixedTopK to avoid per-block []int allocations in the log hot path.
//
// NOTE-101: Acquire with acquireRowIndexScratch(); release with releaseRowIndexScratch().
// Cap guard: slices grown beyond rowIndexScratchMaxCap entries are replaced on return to bound pool memory.
// ~512 KiB max per pooled item (65536 int64 elements × 8 bytes).

const (
	rowIndexScratchDefaultCap = 256
	rowIndexScratchMaxCap     = 65536
)

//nolint:gochecknoglobals
var rowIndexScratchPool = &sync.Pool{
	New: func() any {
		s := make([]int, 0, rowIndexScratchDefaultCap)
		return &s
	},
}

func acquireRowIndexScratch() *[]int {
	//nolint:forcetypeassert
	return rowIndexScratchPool.Get().(*[]int)
}

func releaseRowIndexScratch(p *[]int) {
	if cap(*p) > rowIndexScratchMaxCap {
		*p = make([]int, 0, rowIndexScratchDefaultCap)
	} else {
		*p = (*p)[:0]
	}
	rowIndexScratchPool.Put(p)
}

// logTopKHeap implements heap.Interface over LogEntry.
// backward=true  → min-heap (root = oldest entry, evicted first when full).
// backward=false → max-heap (root = newest entry, evicted first when full).

func (h *logTopKHeap) Len() int { return len(h.entries) }

func (h *logTopKHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *logTopKHeap) Less(i, j int) bool {
	if h.backward {
		return h.entries[i].TimestampNanos < h.entries[j].TimestampNanos // min-heap: oldest at root
	}
	return h.entries[i].TimestampNanos > h.entries[j].TimestampNanos // max-heap: newest at root
}

func (h *logTopKHeap) Push(x any) {
	entry, ok := x.(LogEntry)
	if !ok {
		slog.Error("logTopKHeap.Push: unexpected type", "type", fmt.Sprintf("%T", x))
		return
	}
	h.entries = append(h.entries, entry)
}

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
	worst := buf.entries[0].TimestampNanos
	if backward {
		// min-heap root is oldest; skip if even the block's newest entry is no better.
		return meta.MaxStart != 0 && meta.MaxStart <= worst
	}
	// max-heap root is newest; skip if even the block's oldest entry is no better.
	return meta.MinStart != 0 && meta.MinStart >= worst
}

// logTopKInsert inserts entry into buf. When full, evicts the worst entry only if
// the new entry is a better fit.
func logTopKInsert(buf *logTopKHeap, limit int, backward bool, entry LogEntry) {
	if buf.Len() < limit {
		heap.Push(buf, entry)
		return
	}
	worst := buf.entries[0].TimestampNanos
	if backward && entry.TimestampNanos > worst {
		heap.Pop(buf)
		heap.Push(buf, entry)
	} else if !backward && entry.TimestampNanos < worst {
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
	ctx context.Context,
	r *modules_reader.Reader,
	program *vm.Program,
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
) ([]LogEntry, QueryStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return nil, QueryStats{}, nil
	}
	if program == nil {
		return nil, QueryStats{}, fmt.Errorf("CollectLogs: program cannot be nil")
	}

	queryStart := time.Now()
	var qs QueryStats

	// NOTE-023: forward opts.TimeRange for block-level pruning when provided.
	// When opts.TimeRange is zero, this is identical to the previous behavior.
	planStart := time.Now()
	plan := planBlocks(r, program, queryplanner.TimeRange{
		MinNano: opts.TimeRange.MinNano,
		MaxNano: opts.TimeRange.MaxNano,
	}, queryplanner.PlanOptions{})
	emitPlannerSpan(ctx, plan) // NOTE-456
	qs.Steps = append(qs.Steps, StepStats{
		Name:     stepNamePlan,
		Duration: time.Since(planStart),
		Metadata: map[string]any{
			"total_blocks":    plan.TotalBlocks,
			"pruned_by_time":  plan.PrunedByTime,
			"pruned_by_index": plan.PrunedByIndex,
			"pruned_by_fuse":  plan.PrunedByFuse,
			"selected_blocks": len(plan.SelectedBlocks),
			"explain":         plan.Explain,
		},
	})

	if len(plan.SelectedBlocks) == 0 {
		qs.ExecutionPath = ExecPathBlockPruned
		qs.TotalDuration = time.Since(queryStart)
		return nil, qs, nil
	}

	wantColumns := ProgramWantColumns(program)
	wantColumns = injectLogRequiredColumns(wantColumns)

	backward := opts.Direction == queryplanner.Backward
	var fetchedGroups int
	var fetchedBlocks int
	var bytesRead int64
	var scanErr error

	scanStart := time.Now()

	if opts.Limit > 0 {
		qs.ExecutionPath = ExecPathBlockTopK
		buf := &logTopKHeap{
			entries:  make([]LogEntry, 0, opts.Limit),
			backward: backward,
		}
		fetchedGroups, fetchedBlocks, bytesRead, scanErr = logTopKScan(
			r,
			program,
			wantColumns,
			pipeline,
			opts,
			plan,
			buf,
			backward,
		)
		if scanErr != nil {
			qs.TotalDuration = time.Since(queryStart)
			return nil, qs, scanErr
		}
		qs.Steps = append(qs.Steps, StepStats{
			Name:      stepNameBlockScan,
			Duration:  time.Since(scanStart),
			BytesRead: bytesRead,
			IOOps:     fetchedGroups,
			Metadata: map[string]any{
				"fetched_blocks": fetchedBlocks,
			},
		})
		qs.TotalDuration = time.Since(queryStart)
		return logDeliverAll(buf.entries, backward), qs, nil
	}

	// Limit == 0: collect all, sort globally, deliver.
	// Pre-allocate with a capacity hint: min(totalSpanCount, 4096) to avoid repeated
	// slice growth copies while not over-allocating for selective queries.
	qs.ExecutionPath = ExecPathBlockPlain
	var totalRows int
	for _, blockIdx := range plan.SelectedBlocks {
		totalRows += int(r.BlockMeta(blockIdx).SpanCount)
	}
	if totalRows > maxLogPreallocRows {
		totalRows = maxLogPreallocRows
	}
	all := make([]LogEntry, 0, totalRows)
	fetchedGroups, fetchedBlocks, bytesRead, scanErr = logCollectAll(
		r,
		program,
		wantColumns,
		pipeline,
		opts,
		plan,
		&all,
	)
	if scanErr != nil {
		qs.TotalDuration = time.Since(queryStart)
		return nil, qs, scanErr
	}
	qs.Steps = append(qs.Steps, StepStats{
		Name:      stepNameBlockScan,
		Duration:  time.Since(scanStart),
		BytesRead: bytesRead,
		IOOps:     fetchedGroups,
		Metadata: map[string]any{
			"fetched_blocks": fetchedBlocks,
		},
	})
	qs.TotalDuration = time.Since(queryStart)
	return logDeliverAll(all, backward), qs, nil
}

// filterRowsByTimeRange filters rowIndices to those within [minNano, maxNano].
// Zero values for minNano/maxNano mean open bounds.
// NOTE-021 (executor): called after first-pass ColumnPredicate to skip second-pass decode for out-of-range rows.
// NOTE-101: scratch is optional (*[]int from acquireRowIndexScratch). If nil or if no
// time range filtering is needed (minNano == maxNano == 0), rows is returned as-is without
// pool interaction. When scratch is non-nil and filtering is needed, the returned slice is
// backed by scratch and must not be used after releaseRowIndexScratch(scratch).
func filterRowsByTimeRange(tsCol *modules_reader.Column, rows []int, minNano, maxNano uint64, scratch *[]int) []int {
	if minNano == 0 && maxNano == 0 {
		// Fast path: no filtering needed, scratch never touched.
		return rows
	}
	if scratch == nil {
		// Fallback: allocate inline (test or non-hot-path callers that skip pool acquire).
		kept := make([]int, 0, len(rows))
		for _, rowIdx := range rows {
			var ts uint64
			if tsCol != nil {
				if v, ok := tsCol.Uint64Value(rowIdx); ok {
					ts = v
				}
			}
			if minNano > 0 && ts < minNano {
				continue
			}
			if maxNano > 0 && ts > maxNano {
				continue
			}
			kept = append(kept, rowIdx)
		}
		return kept
	}
	// Pool path: use scratch to avoid per-block allocation.
	kept := (*scratch)[:0]
	for _, rowIdx := range rows {
		var ts uint64
		if tsCol != nil {
			if v, ok := tsCol.Uint64Value(rowIdx); ok {
				ts = v
			}
		}
		if minNano > 0 && ts < minNano {
			continue
		}
		if maxNano > 0 && ts > maxNano {
			continue
		}
		kept = append(kept, rowIdx)
	}
	*scratch = kept
	return kept
}

// iterateLogRows iterates all selected blocks, applying block-level pruning, time
// pre-filtering (NOTE-021), and per-row pipeline processing, then calls fn for each
// pipeline-passing row.
//
// canSkipBlock, if non-nil, is called before issuing any I/O for a block. If it returns
// true the block is skipped. Used by logTopKScan for heap-based block pruning (SPEC-SLK-2).
// logCollectAll passes nil (never skip).
//
// canSkip, if non-nil, is called after the row timestamp is read but BEFORE lokiLabels
// and logAttrs are materialized. If it returns true, the row is skipped cheaply without
// incurring materialization cost. Used by logTopKScan for NOTE-031 per-row early skip.
// logCollectAll passes nil (no per-row skip needed).
//
// fn receives the row timestamp and the materialized LogEntry. fn returns true to
// continue iteration, false to stop.
//
// Returns the number of blocks fetched and approximate bytes read.
// processLogRows applies the per-row pipeline and callback for one block's kept rows.
// Returns true if the callback requested early stop.
func processLogRows(
	rows []int,
	tsCol, bodyCol *modules_reader.Column,
	colNames []string,
	colMap map[string]int,
	colCols []*modules_reader.Column,
	logStrNames []string,
	logStrCols []*modules_reader.Column,
	block *modules_reader.Block,
	pipeline *logqlparser.Pipeline,
	skipParsers bool,
	canSkip func(ts uint64) bool,
	fn func(ts uint64, entry LogEntry) bool,
) bool {
	for _, rowIdx := range rows {
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

		// NOTE-031: per-row early skip — called after ts is read but before
		// lokiLabels/logAttrs are materialized, avoiding unnecessary allocation.
		if canSkip != nil && canSkip(ts) {
			continue
		}

		// NOTE-SL-017: acquire from pool - zero map allocation for dropped rows.
		bls := acquireBlockLabelSet(block, rowIdx, colNames, colMap, colCols)
		if pipeline != nil {
			var keep bool
			if skipParsers {
				line, _, keep = pipeline.ProcessSkipParsers(ts, line, bls)
			} else {
				line, _, keep = pipeline.Process(ts, line, bls)
			}
			if !keep {
				releaseBlockLabelSet(bls)
				continue
			}
		}
		// Read __loki_labels__ via the post-pipeline LabelSet so that mutations
		// (drop/keep/label_format) are respected if they affect this field.
		lokiLabels := bls.Get("__loki_labels__")
		logAttrs := collectLogStringAttrs(logStrNames, logStrCols, rowIdx)
		releaseBlockLabelSet(bls)
		entry := LogEntry{TimestampNanos: ts, Line: line, LokiLabels: lokiLabels, LogAttrs: logAttrs}
		if !fn(ts, entry) {
			return true
		}
	}
	return false
}

// iterateLogRows returns (fetchedGroups, fetchedBlocks, bytesRead, error):
// fetchedGroups counts ReadGroup calls (IOOps); fetchedBlocks counts individual blocks fetched.
func iterateLogRows(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
	plan *queryplanner.Plan,
	canSkipBlock func(meta shared.BlockMeta) bool,
	canSkip func(ts uint64) bool,
	fn func(ts uint64, entry LogEntry) bool,
) (int, int, int64, error) {
	groups := r.CoalescedGroups(plan.SelectedBlocks)
	// NOTE-105: flat []int replaces map[int]int — block IDs are bounded by r.BlockCount().
	blockCount := r.BlockCount()
	blockToGroupSlice := make([]int, blockCount)
	for i := range blockToGroupSlice {
		blockToGroupSlice[i] = -1
	}
	for gi, g := range groups {
		for _, bi := range g.BlockIDs {
			if bi < blockCount {
				blockToGroupSlice[bi] = gi
			}
		}
	}

	fetched := make(map[int][]byte, len(plan.SelectedBlocks))
	fetchedGroupsSeen := make(map[int]bool, len(groups))
	skippedBlocks := make(map[int]bool, len(plan.SelectedBlocks))
	fetchedGroups := 0
	fetchCount := 0
	var bytesRead int64

	for _, blockIdx := range plan.SelectedBlocks {
		meta := r.BlockMeta(blockIdx)
		if canSkipBlock != nil && canSkipBlock(meta) {
			// If the group is already fetched, release bytes immediately.
			// Otherwise record the block as skipped so we can delete it after
			// maps.Copy brings the whole group into fetched.
			gi2 := -1
			if blockIdx < blockCount {
				gi2 = blockToGroupSlice[blockIdx]
			}
			if gi2 != -1 && fetchedGroupsSeen[gi2] {
				delete(fetched, blockIdx)
			} else {
				skippedBlocks[blockIdx] = true
			}
			continue
		}

		gi := -1
		if blockIdx < blockCount {
			gi = blockToGroupSlice[blockIdx]
		}
		if gi == -1 {
			continue
		}
		if !fetchedGroupsSeen[gi] {
			groupRaw, err := r.ReadGroup(groups[gi])
			if err != nil {
				return fetchedGroups, fetchCount, bytesRead, fmt.Errorf("CollectLogs ReadGroup: %w", err)
			}
			maps.Copy(fetched, groupRaw)
			// Count bytes for entire fetched group — ReadGroup reads all blocks in the group.
			// Remove skipped blocks from fetched but still count their bytes.
			for _, bi := range groups[gi].BlockIDs {
				if skippedBlocks[bi] {
					delete(fetched, bi)
				}
				bytesRead += int64(r.BlockMeta(bi).Length) //nolint:gosec // Length is block size, safe to cast
			}
			fetchedGroups++
			fetchCount += len(groups[gi].BlockIDs)
			fetchedGroupsSeen[gi] = true
		}

		raw, ok := fetched[blockIdx]
		if !ok {
			continue
		}
		delete(fetched, blockIdx)

		r.ResetInternStrings()
		bwb, err := r.ParseBlockFromBytes(raw, modules_reader.WantOnly(wantColumns), meta)
		if err != nil {
			return fetchedGroups, fetchCount, bytesRead, fmt.Errorf(
				"CollectLogs ParseBlockFromBytes block %d: %w",
				blockIdx,
				err,
			)
		}

		cp := acquireBlockColumnProvider(bwb.Block)
		rowSet, err := program.ColumnPredicate(cp)
		if err != nil {
			releaseBlockColumnProvider(cp)
			return fetchedGroups, fetchCount, bytesRead, fmt.Errorf(
				"CollectLogs ColumnPredicate block %d: %w",
				blockIdx,
				err,
			)
		}

		if rowSet.Size() == 0 {
			releaseBlockColumnProvider(cp)
			continue
		}

		// NOTE-021: pre-filter by time using first-pass block before full second-pass decode.
		// log:timestamp is guaranteed present (injected into wantColumns above).
		// NOTE-101: use pooled scratch slice to avoid per-block allocation.
		// The scratch is acquired and released inside an immediately-invoked closure so
		// that defer fires per-block rather than at function return, protecting against
		// leaks on panic inside buildBlockColMapsWithLogCache, blockHasBodyParsed, or processLogRows.
		rows := rowSet.ToSlice()
		// NOTE-107: rows may point into cp.scratch; release after processLogRows returns so
		// keptByTime (which may alias rows when minNano==maxNano==0) is not clobbered by a
		// concurrent pool user before processLogRows finishes reading it.
		stopped := func() bool {
			scratchPtr := acquireRowIndexScratch()
			defer releaseRowIndexScratch(scratchPtr)
			keptByTime := filterRowsByTimeRange(
				bwb.Block.GetColumn("log:timestamp"), rows,
				opts.TimeRange.MinNano, opts.TimeRange.MaxNano,
				scratchPtr,
			)
			if len(keptByTime) == 0 {
				releaseBlockColumnProvider(cp)
				return false // skip second-pass decode entirely
			}

			// NOTE-001: Columns registered by ParseBlockFromBytes hold compressed bytes only;
			// no decode happens at registration. Full decode is deferred to first accessor call
			// via ensureDecompressed() + decodeNow().

			// Cache column pointers for the row loop.
			tsCol := bwb.Block.GetColumn("log:timestamp")
			bodyCol := bwb.Block.GetColumn("log:body")
			colNames, colMap, colCols, logStrNames, logStrCols := buildBlockColMapsWithLogCache(bwb.Block)
			skipParsers := pipeline != nil && blockHasBodyParsed(bwb.Block) && !pipeline.HasLineFormat
			result := processLogRows(
				keptByTime,
				tsCol,
				bodyCol,
				colNames,
				colMap,
				colCols,
				logStrNames,
				logStrCols,
				bwb.Block,
				pipeline,
				skipParsers,
				canSkip,
				fn,
			)
			releaseBlockColumnProvider(cp)
			return result
		}()
		if stopped {
			return fetchedGroups, fetchCount, bytesRead, nil
		}
	}
	return fetchedGroups, fetchCount, bytesRead, nil
}

// logTopKScan iterates selected blocks, applies block-level skip checks, fetches
// lazily, and fills buf with the top-limit pipeline-passing rows.
// Returns (fetchedGroups, fetchedBlocks, bytesRead, error).
func logTopKScan(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
	plan *queryplanner.Plan,
	buf *logTopKHeap,
	backward bool,
) (int, int, int64, error) {
	return iterateLogRows(
		r, program, wantColumns, pipeline, opts, plan,
		func(meta shared.BlockMeta) bool {
			return logTopKCanSkipBlock(buf, opts.Limit, backward, meta)
		},
		// NOTE-031: per-row early skip — called before lokiLabels/logAttrs materialization.
		// If the heap is full and this ts cannot displace the worst entry, skip cheaply.
		func(ts uint64) bool {
			if buf.Len() < opts.Limit {
				return false
			}
			worst := buf.entries[0].TimestampNanos
			return (backward && ts <= worst) || (!backward && ts >= worst)
		},
		func(ts uint64, entry LogEntry) bool {
			logTopKInsert(buf, opts.Limit, backward, entry)
			return true
		},
	)
}

// logCollectAll scans all selected blocks without heap pruning, collecting every
// pipeline-passing row. Used by CollectLogs when opts.Limit == 0.
// Returns (fetchedGroups, fetchedBlocks, bytesRead, error).
func logCollectAll(
	r *modules_reader.Reader,
	program *vm.Program,
	wantColumns map[string]struct{},
	pipeline *logqlparser.Pipeline,
	opts CollectOptions,
	plan *queryplanner.Plan,
	all *[]LogEntry,
) (int, int, int64, error) {
	return iterateLogRows(
		r, program, wantColumns, pipeline, opts, plan,
		nil, // no block-level skip for collect-all
		nil, // no per-row skip for collect-all
		func(_ uint64, entry LogEntry) bool {
			*all = append(*all, entry)
			return true
		},
	)
}

// collectLogStringAttrs collects log.* string-valued column values for rowIdx
// using the per-block log column cache produced by buildBlockColMapsWithLogCache.
// Returns a zero LogAttrs if no columns are present at this row.
// Empty-but-present string values are included (IsPresent controls absence, not value content).
// Names and Values slices are allocated with len(names) capacity on the first present
// column found, avoiding both zero-allocation overhead for absent rows and growth
// reallocs for rows where all columns are present.
func collectLogStringAttrs(names []string, cols []*modules_reader.Column, rowIdx int) LogAttrs {
	var result LogAttrs
	for i, name := range names {
		col := cols[i]
		if col == nil || !col.IsPresent(rowIdx) {
			continue
		}
		v, ok := col.StringValue(rowIdx)
		if !ok {
			continue
		}
		if result.Names == nil {
			result.Names = make([]string, 0, len(names))
			result.Values = make([]string, 0, len(names))
		}
		result.Names = append(result.Names, name)
		result.Values = append(result.Values, v)
	}
	return result
}

// logDeliverAll sorts entries and returns them in direction order.
func logDeliverAll(entries []LogEntry, backward bool) []LogEntry {
	if backward {
		slices.SortFunc(entries, func(a, b LogEntry) int {
			return cmp.Compare(b.TimestampNanos, a.TimestampNanos)
		})
	} else {
		slices.SortFunc(entries, func(a, b LogEntry) int {
			return cmp.Compare(a.TimestampNanos, b.TimestampNanos)
		})
	}
	results := make([]LogEntry, len(entries))
	copy(results, entries)
	return results
}
