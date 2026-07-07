package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// stream_compaction.go — streaming compaction merge for the v2 BucketGroup file format
// (NOTE-VI-043, #427). This is the streaming companion to bucketmerge.go's non-streaming
// MergeBucketFiles: instead of materializing every input file's every group into an
// in-memory map-of-maps before cutting output blocks, it performs a heap-based k-way merge
// across per-file BucketFileIterators, bounding peak memory to the K already-decoded input
// files plus one in-progress output block plus a small per-key merge buffer.

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// GroupIterator is the interface StreamCompactBucketFiles' k-way merge requires: a
// forward-only, sorted stream of BucketGroups plus the StringTable needed to resolve each
// group's SourceID references. BucketFileIterator (whole-file, in-memory decode) and
// diskBucketFileIterator (lazy, block-at-a-time decode from a local temp file) both satisfy
// it — confirmed by reading bucketIteratorHeap.Less/Push/Pop and StreamCompactBucketFiles'
// merge loop directly: Peek/Advance/StringTable are the only methods ever called on an
// iterator by the merge algorithm itself (Err/Close are called only by the algorithm's
// bookkeeping, never by mergeGroupsAtKey or Less).
type GroupIterator interface {
	// Peek returns the current group without advancing, or (nil, false) if exhausted OR if
	// Err() is non-nil (an errored iterator looks like an exhausted one to Peek/heap
	// ordering — callers MUST check Err() to distinguish the two).
	Peek() (*BucketGroup, bool)
	// Advance moves to the next group. For a disk-backed iterator this may perform local
	// disk I/O (decoding the next block) and may set Err(); ctx allows that I/O (and any
	// retention-checker calls it triggers) to respect cancellation.
	Advance(ctx context.Context)
	// StringTable returns the table needed to resolve the group most recently returned by
	// Peek. Never nil for a live (non-exhausted, non-errored) iterator.
	StringTable() *StringTable
	// Err returns the first error encountered while advancing, or nil. Once non-nil, Peek
	// always returns (nil, false).
	Err() error
	// Close releases resources (e.g. an open local temp file). Idempotent — safe to call
	// more than once. BucketFileIterator's Close is a no-op.
	Close() error
}

// BucketFileIterator walks one BucketFile's groups in sorted order via a sequential
// block-by-block cursor.
//
// SPEC-VI-1: blocks within a BucketFile are globally ordered end-to-end (see
// SplitIntoBlocks, bucketmerge.go), so a plain sequential block-by-block walk yields fully
// sorted output — no cross-block lookahead or re-sort needed.
type BucketFileIterator struct {
	file     *BucketFile
	blockIdx int
	groupIdx int
}

// var _ GroupIterator = (*BucketFileIterator)(nil) is a compile-time assertion that
// BucketFileIterator satisfies GroupIterator — catches interface drift at compile time
// forever after, not just once at test-run time.
var _ GroupIterator = (*BucketFileIterator)(nil)

// NewBucketFileIterator returns an iterator positioned at the first group of f (skipping
// past any leading empty blocks).
func NewBucketFileIterator(f *BucketFile) *BucketFileIterator {
	it := &BucketFileIterator{file: f}
	it.skipEmptyBlocks()
	return it
}

// Peek returns the current group without advancing, or (nil, false) if the iterator is
// exhausted.
func (it *BucketFileIterator) Peek() (*BucketGroup, bool) {
	if it.file == nil || it.blockIdx >= len(it.file.Blocks) {
		return nil, false
	}
	return &it.file.Blocks[it.blockIdx].Groups[it.groupIdx], true
}

// Advance moves to the next group, skipping over any now-exhausted or empty blocks. ctx is
// unused — in-memory iteration performs no I/O — but is required by the GroupIterator
// interface (see GroupIterator's doc comment for why Advance takes a ctx).
func (it *BucketFileIterator) Advance(_ context.Context) {
	if it.file == nil || it.blockIdx >= len(it.file.Blocks) {
		return
	}
	it.groupIdx++
	it.skipEmptyBlocks()
}

// StringTable returns the table needed to resolve the current file's SourceID references.
func (it *BucketFileIterator) StringTable() *StringTable {
	if it.file == nil {
		return nil
	}
	return it.file.StringTable
}

// Err always returns nil — in-memory iteration cannot fail.
func (it *BucketFileIterator) Err() error { return nil }

// Close is a no-op — BucketFileIterator holds no resources to release.
func (it *BucketFileIterator) Close() error { return nil }

// skipEmptyBlocks advances blockIdx/groupIdx past any block that has no groups left,
// defensively tolerating a block with zero groups (SPEC-ROOT-001 — no infinite loop, no
// panic).
func (it *BucketFileIterator) skipEmptyBlocks() {
	for it.blockIdx < len(it.file.Blocks) && it.groupIdx >= len(it.file.Blocks[it.blockIdx].Groups) {
		it.blockIdx++
		it.groupIdx = 0
	}
}

// DecodeFilteredBucketFile decodes data and applies retention filtering (the per-input
// decode-and-filter step feeding StreamCompactBucketFiles' k-way merge). It returns
// (nil, stats, nil) — not an error —
// ONLY for a bad-magic file (ErrNotBucketFile), a legacy pre-v2 input that holds no v2
// postings and so can be skipped without dropping any live posting. Every other decode
// failure is a genuine corrupt v2 file and is returned as an error: silently skipping it
// would permanently drop its postings from a compacted output, silently under-counting the
// index — the same silent-partial-result bug class the trace-by-id review caught
// (NOTE-VI-046). Mirrors NewDiskBucketFileIterator's discipline.
func DecodeFilteredBucketFile(ctx context.Context, data []byte, checker RefChecker) (*BucketFile, CompactStats, error) {
	f, err := DecodeBucketFile(data)
	if err != nil {
		if errors.Is(err, ErrNotBucketFile) {
			return nil, CompactStats{}, nil //nolint:nilerr // intentional: legacy bad-magic pre-v2 files are skipped, not aborted
		}
		return nil, CompactStats{}, fmt.Errorf("valueindex: DecodeFilteredBucketFile: decode: %w", err)
	}
	if checker != nil {
		filtered, stats, ferr := filterDeadRefs(ctx, f, checker)
		if ferr != nil {
			return nil, CompactStats{}, ferr
		}
		return filtered, stats, nil
	}
	return f, CompactStats{Retained: countBucketRefs(f)}, nil
}

// bucketIteratorHeap implements heap.Interface over a set of GroupIterators, ordered
// by each iterator's current Peek() key (TimeSec, CanonicalValue). The root is always the
// iterator with the smallest current key.
type bucketIteratorHeap struct {
	iters []GroupIterator
}

func (h *bucketIteratorHeap) Len() int { return len(h.iters) }

func (h *bucketIteratorHeap) Less(i, j int) bool {
	gi, iok := h.iters[i].Peek()
	gj, jok := h.iters[j].Peek()
	if !iok || !jok {
		// Defensive: the heap only ever holds non-exhausted iterators; treat an
		// exhausted iterator (which should never occur here) as never less than a
		// live one rather than deref a nil group (SPEC-ROOT-001).
		return jok && !iok
	}
	if gi.TimeSec != gj.TimeSec {
		return gi.TimeSec < gj.TimeSec
	}
	return compareCanonicalBytes(gi.CanonicalValue, gj.CanonicalValue) < 0
}

func (h *bucketIteratorHeap) Swap(i, j int) {
	h.iters[i], h.iters[j] = h.iters[j], h.iters[i]
}

func (h *bucketIteratorHeap) Push(x any) {
	it, ok := x.(GroupIterator)
	if !ok {
		slog.Error("bucketIteratorHeap.Push: unexpected type", "type", fmt.Sprintf("%T", x))
		return
	}
	h.iters = append(h.iters, it)
}

func (h *bucketIteratorHeap) Pop() any {
	n := len(h.iters)
	x := h.iters[n-1]
	h.iters = h.iters[:n-1]
	return x
}

// popIterator pops the heap root and asserts its concrete type (SPEC-ROOT-001 — guarded,
// no unchecked type assertion; the heap only ever holds GroupIterator values, so a mismatch
// here signals a programming error in this file, not malformed external input).
func popIterator(h *bucketIteratorHeap) (GroupIterator, error) {
	x := heap.Pop(h)
	it, ok := x.(GroupIterator)
	if !ok {
		return nil, fmt.Errorf("valueindex: bucketIteratorHeap: unexpected pop type %T", x)
	}
	return it, nil
}

// popStaleIterator pops a heap root whose Peek() already reported exhaustion (SPEC-ROOT-001
// defensive case — the heap should only ever hold non-exhausted iterators). It returns an
// error either if the pop itself fails, or if the popped iterator recorded a real error —
// distinguishing a genuinely-errored iterator from a merely-exhausted one, per
// GroupIterator's Peek/Err contract (an errored iterator looks exhausted to Peek alone).
func popStaleIterator(h *bucketIteratorHeap) error {
	it, err := popIterator(h)
	if err != nil {
		return err
	}
	return it.Err()
}

// mergeGroupsAtKey merges N same-key BucketGroups (one per contributing iterator, each
// resolved against its own file's StringTable) into one output BucketGroup, unioning
// BlockRefs by (sourcePath, page) and SpanRefs by TraceID (NOTE-VI-045 dedup semantics —
// same rule as MergeBucketFiles, verified by the equivalence tests, not by shared code,
// since the two accumulators operate at different scopes: whole-file vs single-key).
func mergeGroupsAtKey(
	timeSec uint64,
	value []byte,
	groups []*BucketGroup,
	tables []*StringTable,
	outTable *StringTable,
) (BucketGroup, error) {
	type spanAcc struct {
		idx     map[uint16]struct{}
		traceID [16]byte
	}
	type refAcc struct {
		spans      map[[16]byte]*spanAcc
		sourcePath string
		ref        BlockRef
	}

	refs := make(map[string]*refAcc, len(groups))
	refOrder := make([]string, 0, len(groups))
	for gi, g := range groups {
		if g == nil || gi >= len(tables) || tables[gi] == nil {
			// Defensive: groups/tables must be parallel slices supplied by the caller;
			// skip a malformed entry rather than index out of range or deref nil
			// (SPEC-ROOT-001).
			continue
		}
		table := tables[gi]
		for ri := range g.Refs {
			r := &g.Refs[ri]
			path := table.Lookup(r.SourceID)
			rk := formatRefKey(path, r.Ref)
			ra := refs[rk]
			if ra == nil {
				ra = &refAcc{sourcePath: path, ref: r.Ref, spans: make(map[[16]byte]*spanAcc, len(r.Spans))}
				refs[rk] = ra
				refOrder = append(refOrder, rk)
			}
			for si := range r.Spans {
				s := &r.Spans[si]
				sa := ra.spans[s.TraceID]
				if sa == nil {
					sa = &spanAcc{traceID: s.TraceID, idx: make(map[uint16]struct{}, len(s.SpanIndexes))}
					ra.spans[s.TraceID] = sa
				}
				for _, idx := range s.SpanIndexes {
					sa.idx[idx] = struct{}{}
				}
			}
		}
	}

	out := BucketGroup{
		TimeSec:        timeSec,
		CanonicalValue: value,
		Refs:           make([]BucketBlockRef, 0, len(refOrder)),
	}
	for _, rk := range refOrder {
		ra := refs[rk]
		if ra == nil {
			// Defensive: refOrder only ever holds keys inserted alongside a non-nil
			// value in the loop above (SPEC-ROOT-001 — guard rather than deref nil).
			continue
		}
		id, ok := outTable.Intern(ra.sourcePath)
		if !ok {
			return BucketGroup{}, fmt.Errorf("valueindex: mergeGroupsAtKey: %w", ErrStringTableOverflow)
		}
		br := BucketBlockRef{SourceID: id, Ref: ra.ref, Spans: make([]SpanRef, 0, len(ra.spans))}
		for _, sa := range ra.spans {
			idxs := make([]uint16, 0, len(sa.idx))
			for idx := range sa.idx {
				idxs = append(idxs, idx)
			}
			sortUint16(idxs)
			br.Spans = append(br.Spans, SpanRef{TraceID: sa.traceID, SpanIndexes: idxs})
		}
		out.Refs = append(out.Refs, br)
	}

	sort.Slice(out.Refs, func(i, j int) bool {
		if out.Refs[i].SourceID != out.Refs[j].SourceID {
			return out.Refs[i].SourceID < out.Refs[j].SourceID
		}
		return out.Refs[i].Ref.PageNum < out.Refs[j].Ref.PageNum
	})
	for ri := range out.Refs {
		spans := out.Refs[ri].Spans
		sort.Slice(spans, func(i, j int) bool {
			return compareTraceID(spans[i].TraceID, spans[j].TraceID) < 0
		})
	}
	return out, nil
}

// streamOutputWriter accumulates BucketGroups into fixed-size blocks and writes each block
// directly to a bufio.Writer as it is cut (plan.md Decision 3), tracking the block directory
// and file-level min/max time needed for the eventual footer. Extracted out of
// StreamCompactBucketFiles to keep that function's cyclomatic complexity under the repo's
// gocyclo gate.
type streamOutputWriter struct {
	bw             *bufio.Writer
	dir            []BlockDirEntry
	pending        []BucketGroup
	groupsPerBlock int
	bodyEnd        uint64
	fileMin        uint64
	fileMax        uint64
	haveFileTime   bool
}

func newStreamOutputWriter(bw *bufio.Writer, groupsPerBlock int, headerLen uint64) *streamOutputWriter {
	return &streamOutputWriter{
		bw:             bw,
		dir:            make([]BlockDirEntry, 0, 8),
		pending:        make([]BucketGroup, 0, groupsPerBlock),
		groupsPerBlock: groupsPerBlock,
		bodyEnd:        headerLen,
	}
}

// add appends g to the pending block, flushing automatically once groupsPerBlock is
// reached. It reports whether a block was just cut (flushed) by this call, so the caller
// can safely evaluate an output-file-size split boundary only at a block boundary — never
// mid-block, since a block's groups reference the current file's string table (NOTE-VI-077).
func (w *streamOutputWriter) add(g BucketGroup) (blockCut bool, err error) {
	w.pending = append(w.pending, g)
	if len(w.pending) >= w.groupsPerBlock {
		if err := w.flush(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// flush writes the pending block (if non-empty) to bw and records its directory entry.
func (w *streamOutputWriter) flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	blk := BucketBlock{Groups: w.pending}
	blk.ComputeBlockMeta()
	raw := encodeBucketBlock(&blk)
	compressed := snappy.Encode(nil, raw)
	if _, err := w.bw.Write(compressed); err != nil {
		return fmt.Errorf("valueindex: StreamCompactBucketFiles: write block: %w", err)
	}
	w.dir = append(w.dir, BlockDirEntry{
		CompOff:    w.bodyEnd,
		CompLen:    uint64(len(compressed)),
		MinTimeSec: blk.MinTimeSec,
		MaxTimeSec: blk.MaxTimeSec,
		MinValue:   blk.MinValue,
		MaxValue:   blk.MaxValue,
	})
	w.bodyEnd += uint64(len(compressed))
	// pending is only ever flushed non-empty (guarded above), so this block always
	// contributes to the file-level time range — mirrors EncodeBucketFile's
	// len(b.Groups) > 0 guard for the same footer field.
	if !w.haveFileTime || blk.MinTimeSec < w.fileMin {
		w.fileMin = blk.MinTimeSec
	}
	if !w.haveFileTime || blk.MaxTimeSec > w.fileMax {
		w.fileMax = blk.MaxTimeSec
	}
	w.haveFileTime = true
	w.pending = make([]BucketGroup, 0, w.groupsPerBlock)
	return nil
}

// projectedFileSize returns an estimate of the eventual on-disk size of the output file if it
// were finalized right now, given the writer's flushed body (bodyEnd) plus the tail it will
// grow: the string table (sized from table) + block index (one entry per flushed block) +
// fixed footer. Used only by the output-size split heuristic (NOTE-VI-077) — it is an
// estimate, not a guarantee, because the not-yet-flushed pending block and any future blocks
// still add to the body, and the string table may still gain SourceRefs; but evaluated only
// at a block boundary (pending empty) it is exact for everything already committed, which is
// enough to bound each rotated file within a small multiple of the cap.
func projectedFileSize(w *streamOutputWriter, table *StringTable) uint64 {
	// Block index: 4-byte count prefix + per-entry fixed fields (4×uint64) + two
	// length-prefixed values, mirroring appendBlockIndex's layout exactly.
	blockIdxSize := uint64(4)
	for i := range w.dir {
		d := &w.dir[i]
		blockIdxSize += 8 + 8 + 8 + 8 + 2 + uint64(len(d.MinValue)) + 2 + uint64(len(d.MaxValue))
	}
	//nolint:gosec // G115: EncodedSize is a byte length, always >= 0, never overflows uint64.
	return w.bodyEnd + uint64(table.EncodedSize()) + blockIdxSize + bucketFooterSize
}

// populateMergeHeap pushes every non-exhausted iterator onto a fresh heap, propagating any
// error reported by an iterator that starts out already-exhausted (an errored iterator looks
// exhausted to Peek alone — GroupIterator's Peek/Err contract).
func populateMergeHeap(iterators []GroupIterator) (*bucketIteratorHeap, error) {
	h := &bucketIteratorHeap{iters: make([]GroupIterator, 0, len(iterators))}
	for _, it := range iterators {
		if _, ok := it.Peek(); ok {
			heap.Push(h, it)
			continue
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// collectContributionsAtKey pops every iterator whose Peek() reports the same (keyTS,
// keyValue) key currently parked at the heap root into the caller-owned groups/tables/iters
// buffers (reset via [:0] here, reused across outer-loop iterations to avoid a fresh
// allocation per merged key).
//
// SPEC-VI-3: this coalescing loop only merges groups simultaneously parked at the heap root
// during this one outer-loop pass. It relies on (TimeSec, CanonicalValue) keys being unique
// within each input file (not just globally ordered per SPEC-VI-1) — a second same-key group
// later in the same file would surface as a spurious extra output group instead of being
// merged here. This is guaranteed today by every producer's pre-dedup construction and is not
// defended against here.
func collectContributionsAtKey(
	h *bucketIteratorHeap,
	keyTS uint64,
	keyValue []byte,
	groups []*BucketGroup,
	tables []*StringTable,
	iters []GroupIterator,
) ([]*BucketGroup, []*StringTable, []GroupIterator, error) {
	groups = groups[:0]
	tables = tables[:0]
	iters = iters[:0]
	for h.Len() > 0 {
		g, gok := h.iters[0].Peek()
		if !gok {
			if err := popStaleIterator(h); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		if g.TimeSec != keyTS || compareCanonicalBytes(g.CanonicalValue, keyValue) != 0 {
			break
		}
		it, err := popIterator(h)
		if err != nil {
			return nil, nil, nil, err
		}
		groups = append(groups, g)
		tables = append(tables, it.StringTable())
		iters = append(iters, it)
	}
	return groups, tables, iters, nil
}

// advanceContributors advances every iterator that contributed to the just-merged key,
// re-pushing it onto the heap if it still has more groups, and aborting on the first error.
func advanceContributors(ctx context.Context, h *bucketIteratorHeap, contribIters []GroupIterator) error {
	for _, it := range contribIters {
		it.Advance(ctx)
		if err := it.Err(); err != nil {
			return err
		}
		if _, ok := it.Peek(); ok {
			heap.Push(h, it)
		}
	}
	return nil
}

// StreamCompactBucketFiles performs a heap-based k-way merge across N already-decoded,
// already-filtered GroupIterators, emitting merged BucketGroups in sorted order and
// cutting them into output blocks of at most groupsPerBlock groups (SPEC-VI-2). It never
// materializes the full merged group set: peak memory is bounded by the K input files
// (already decoded by the caller) plus one in-progress output block plus a transient
// per-key merge buffer sized to however many of the K files currently share one key —
// replacing MergeBucketFiles+SplitIntoBlocks' map-of-maps for this call path. groupsPerBlock
// <= 0 defaults to shared.ValueIndexBucketGroupsPerBlock. output is called at most once,
// with the path to the fully assembled output file, or not at all if there is nothing to
// emit.
//
// The merged output is staged through a local temp file (plan.md Decision 3): a
// bufio.Writer over a fresh os.CreateTemp file, written block-by-block as each output block
// is cut, never accumulating the full output in memory. The temp file (input to `output`) is
// removed via defer after `output` returns (Go's LIFO defer ordering guarantees the caller's
// read of the file completes first), regardless of success or error.
func StreamCompactBucketFiles(
	ctx context.Context,
	iterators []GroupIterator,
	groupsPerBlock int,
	maxOutputBytes int64,
	output func(path string) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if groupsPerBlock <= 0 {
		groupsPerBlock = shared.ValueIndexBucketGroupsPerBlock
	}

	h, err := populateMergeHeap(iterators)
	if err != nil {
		return err
	}

	of, err := newBucketOutputFile(groupsPerBlock)
	if err != nil {
		return err
	}
	// Only the current (in-progress) output file needs cleanup on error: a file that has
	// been finalized+handed to `output` is the caller's responsibility per the callback
	// contract, and finalizeBucketOutputFile clears of.file after a successful handoff.
	defer func() { of.cleanup() }()

	contribGroups := make([]*BucketGroup, 0, len(iterators))
	contribTables := make([]*StringTable, 0, len(iterators))
	contribIters := make([]GroupIterator, 0, len(iterators))

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		keyGroup, ok := h.iters[0].Peek()
		if !ok {
			// Defensive: the heap only ever holds non-exhausted iterators; drop a
			// stale entry rather than deref a nil group (SPEC-ROOT-001) and keep going —
			// unless the iterator reported a real error, which must abort the merge
			// rather than be silently treated as exhaustion (GroupIterator's Err/Peek
			// contract).
			if err := popStaleIterator(h); err != nil {
				return err
			}
			continue
		}
		keyTS := keyGroup.TimeSec
		keyValue := keyGroup.CanonicalValue

		var cerr error
		contribGroups, contribTables, contribIters, cerr = collectContributionsAtKey(
			h, keyTS, keyValue, contribGroups, contribTables, contribIters,
		)
		if cerr != nil {
			return cerr
		}

		merged, err := mergeGroupsAtKey(keyTS, keyValue, contribGroups, contribTables, of.table)
		if err != nil {
			return fmt.Errorf("valueindex: StreamCompactBucketFiles: %w", err)
		}
		blockCut, addErr := of.out.add(merged)
		if addErr != nil {
			return addErr
		}

		// Advance the contributing iterators back onto the heap BEFORE evaluating the split
		// boundary: collectContributionsAtKey popped every contributor out of the heap, so
		// h.Len() would be 0 here for a single-input merge even when that input still has more
		// groups. Advancing first restores the heap to reflect the true remaining work, so the
		// "more input remaining" guard below is accurate.
		if advErr := advanceContributors(ctx, h, contribIters); advErr != nil {
			return advErr
		}

		// NOTE-VI-077 (#482): honor maxOutputBytes by rotating to a fresh output file at a
		// block boundary once the projected finalized file size has grown past the cap.
		// projectedFileSize (not just bodyEnd) accounts for the tail — string table + block
		// index + footer — which for a many-block file dominates the compressed body, so a
		// bodyEnd-only check would never trigger. Rotation is only ever evaluated right after
		// a block was cut (blockCut) so the split lands on a block boundary — never mid-block,
		// since a block's groups reference the current file's string table (a mid-block split
		// would strand references to interned SourceRefs the new file's table does not carry).
		// The h.Len() > 0 guard (evaluated after advanceContributors above) guarantees
		// rotation never produces a trailing empty file.
		//nolint:gosec // G115: maxOutputBytes > 0 is guarded here, so the uint64 conversion is safe.
		if blockCut && maxOutputBytes > 0 && h.Len() > 0 &&
			projectedFileSize(of.out, of.table) >= uint64(maxOutputBytes) {
			if finErr := finalizeBucketOutputFile(of, output); finErr != nil {
				return finErr
			}
			var newErr error
			if of, newErr = newBucketOutputFile(groupsPerBlock); newErr != nil {
				return newErr
			}
		}
	}

	if err := of.out.flush(); err != nil {
		return err
	}

	if len(of.out.dir) == 0 {
		// Nothing left to emit in the trailing file (all remaining groups, if any, were
		// already flushed into a prior rotated file). cleanup (deferred) removes the empty
		// temp file.
		return nil
	}

	return finalizeBucketOutputFile(of, output)
}

// bucketOutputFile bundles the per-output-file state StreamCompactBucketFiles cuts blocks
// into: the temp file + buffered writer, this file's own string table (SourceRefs are
// interned per file — a rotated file starts with a fresh table, NOTE-VI-077), and the
// block-cutting streamOutputWriter. One instance is live at a time; multi-file output
// (maxOutputBytes splitting) replaces it with a fresh instance after each finalize.
type bucketOutputFile struct {
	file  *os.File
	bw    *bufio.Writer
	table *StringTable
	out   *streamOutputWriter
}

// newBucketOutputFile creates a fresh temp output file, writes its header, and returns the
// bundled per-file writer state.
func newBucketOutputFile(groupsPerBlock int) (*bucketOutputFile, error) {
	f, err := os.CreateTemp("", "vi-merge-out-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("valueindex: StreamCompactBucketFiles: create temp output: %w", err)
	}
	bw := bufio.NewWriterSize(f, 256<<10)
	header := binary.LittleEndian.AppendUint32(make([]byte, 0, 5), BucketFileMagic)
	header = append(header, BucketFileVersion)
	if _, err := bw.Write(header); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name()) //nolint:gosec // G703: f is our own os.CreateTemp file, not user-supplied.
		return nil, fmt.Errorf("valueindex: StreamCompactBucketFiles: write header: %w", err)
	}
	return &bucketOutputFile{
		file:  f,
		bw:    bw,
		table: NewStringTable(),
		out:   newStreamOutputWriter(bw, groupsPerBlock, uint64(len(header))),
	}, nil
}

// cleanup closes and removes the current (not-yet-finalized) temp file. It is a no-op once
// the file has been handed off to `output` (finalizeBucketOutputFile clears of.file), so a
// deferred cleanup only ever removes an in-progress file, never one the caller now owns.
func (of *bucketOutputFile) cleanup() {
	if of == nil || of.file == nil {
		return
	}
	name := of.file.Name()
	_ = of.file.Close()
	//nolint:gosec // G703: name is our own os.CreateTemp file, not user-supplied.
	_ = os.Remove(name)
	of.file = nil
}

// finalizeBucketOutputFile flushes any pending block, writes the string table / block index /
// footer, flushes+closes the file, and hands its path to `output`. On success it clears
// of.file so the deferred cleanup will not remove the file the caller now owns. `output` must
// be called at most once per finalized file; multiple finalized files (from maxOutputBytes
// splitting) invoke it once each, in emission order.
func finalizeBucketOutputFile(of *bucketOutputFile, output func(path string) error) error {
	if err := of.out.flush(); err != nil {
		return err
	}
	if err := writeBucketFileTail(of.bw, of.out.dir, of.table, of.out.fileMin, of.out.fileMax, of.out.bodyEnd); err != nil {
		return err
	}
	if err := of.bw.Flush(); err != nil {
		return fmt.Errorf("valueindex: StreamCompactBucketFiles: flush output: %w", err)
	}
	name := of.file.Name()
	if err := of.file.Close(); err != nil {
		return fmt.Errorf("valueindex: StreamCompactBucketFiles: close output: %w", err)
	}
	of.file = nil // handed off; deferred cleanup must not remove it now
	defer func() { _ = os.Remove(name) }()
	return output(name)
}
