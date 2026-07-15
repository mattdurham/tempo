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

// mergeGroupsAtKey merges N same-key BucketGroups into one OR MORE output BucketGroups sharing
// the key, capping each output group's total span-ref count at maxSpansPerGroup (SPEC-VI-16,
// issue #501).
//
// SPEC-VI-18 (issue #503) replaced the original map-of-maps union-building implementation
// (which held the WHOLE key's cross-file union in memory before ever cutting a sibling
// group) with a disk-spill design: contributing (ref, span) pairs accumulate into a flat
// in-memory buffer (accumulateAndSpillKeyRecords) that spills to a sorted temp chunk
// (keymerge_spill.go, mirroring runspill.go's external sort-merge shape) once its estimated
// size crosses shared.ValueIndexMergeBufferSpillBytes, then a streaming k-way merge
// (reduceKeySpillRecords) reduces the spilled chunks plus the in-memory tail back into
// output groups, feeding completed refs into a streamingSplitPacker incrementally rather
// than materializing the full merged result first. Net peak memory is bounded by O(one
// spill-chunk's worth of records) + O(maxSpansPerGroup, one sibling group being packed) —
// mutation-confirmed load-bearing, independent of K (contributing file/iterator count) and
// independent of the key's cumulative real total fan-in — plus a third, honestly-scoped
// term (one ref's own distinct-TraceID union bounded by shared.MaxSpans, a real but derived
// cross-package fact, not mutation-provable at this bound; see SPEC-VI-18), closing the
// residual risk NOTES.md NOTE-VI-111/112 (issue #501) left open. See SPEC-VI-2's Addendum
// and SPEC-VI-18 for the full citation. maxSpansPerGroup <= 0 is a no-op (single output
// group, today's behavior) for callers/tests that don't care about the cap.
//
// Unions BlockRefs by (sourcePath, page) and SpanRefs by TraceID (NOTE-VI-045 dedup semantics —
// same rule as MergeBucketFiles, verified by the equivalence tests, not by shared code,
// since the two accumulators operate at different scopes: whole-file vs single-key).
func mergeGroupsAtKey(
	ctx context.Context,
	timeSec uint64,
	value []byte,
	groups []*BucketGroup,
	tables []*StringTable,
	outTable *StringTable,
	maxSpansPerGroup int,
	tmpDir string,
) ([]BucketGroup, error) {
	return mergeGroupsAtKeyWithSpillThreshold(
		ctx, timeSec, value, groups, tables, outTable, maxSpansPerGroup, tmpDir,
		shared.ValueIndexMergeBufferSpillBytes,
	)
}

// mergeGroupsAtKeyWithSpillThreshold is mergeGroupsAtKey's implementation, parameterized by the
// spill-trigger byte threshold (issue #503) so in-package tests can force spilling with a tiny
// threshold without needing megabyte-scale fixtures — mirrors streamCompactBucketFilesWithCap's
// own production/test-parameterized-entry-point split (issue #501). mergeGroupsAtKey (the
// production entry point) always calls this with the real shared.ValueIndexMergeBufferSpillBytes
// constant; there is one implementation, not two copies.
func mergeGroupsAtKeyWithSpillThreshold(
	ctx context.Context,
	timeSec uint64,
	value []byte,
	groups []*BucketGroup,
	tables []*StringTable,
	outTable *StringTable,
	maxSpansPerGroup int,
	tmpDir string,
	spillThresholdBytes uint64,
) ([]BucketGroup, error) {
	chunks, tail, err := accumulateAndSpillKeyRecords(ctx, groups, tables, outTable, tmpDir, spillThresholdBytes)
	// Every spill chunk created for this key must be removed once its processing completes,
	// success or not (plan.md Phase 6) — registered immediately after the call so a partial
	// failure partway through accumulation (e.g. ErrStringTableOverflow or ctx cancellation on
	// a later ref, after earlier refs already spilled) still cleans up whatever was already
	// written to disk. accumulateAndSpillKeyRecords always returns every chunk it created so
	// far on EVERY exit path, error or not, so this defer never misses one.
	defer func() {
		for _, c := range chunks {
			c.remove()
		}
	}()
	if err != nil {
		return nil, err
	}
	return reduceKeySpillRecords(ctx, chunks, tail, timeSec, value, maxSpansPerGroup)
}

// testHookAccumBufLen is a zero-cost-in-production instrumentation hook (nil by default),
// wired to a counter ONLY by TestMergeGroupsAtKey_NoInMemoryStructureExceedsStructuralBound
// (Phase 8, #503) to observe the in-memory accumulation buffer's length immediately after
// each record is appended, before the spill-threshold check evaluates — a structural
// observation point, not heap sampling (rejected twice already for this exact class of
// claim, NOTE-VI-112).
var testHookAccumBufLen func(n int)

// accumulateAndSpillKeyRecords walks groups/tables in the same nested order mergeGroupsAtKey has
// always used, interning each contributing ref's sourcePath into outTable EAGERLY, in
// first-arrival order (this is what makes spilled and non-spilled accumulation produce the
// identical total order — see plan.md's Design decision section), and appends one
// keySpillRecord per (ref, span) pair to a running in-memory buffer. The spill check runs after
// EVERY record (not just after each ref) so a single pathological ref with millions of spans
// still spills incrementally rather than defeating the memory bound. Once the buffer's
// estimated byte size crosses spillThresholdBytes, it is spilled to a new sorted disk chunk
// (writeKeySpillChunk, mirroring runspill.go's writeRun) and reset. ctx.Err() is checked once
// per ref boundary (Edge Case 4, plan.md) so a long-running hot-key spill can still be
// canceled promptly. Returns every spill chunk created SO FAR on every exit path — including
// error paths — so the caller's cleanup defer never leaks a chunk that was already written to
// disk before a later ref failed.
func accumulateAndSpillKeyRecords(
	ctx context.Context,
	groups []*BucketGroup,
	tables []*StringTable,
	outTable *StringTable,
	tmpDir string,
	spillThresholdBytes uint64,
) ([]*keySpillChunk, []keySpillRecord, error) {
	var (
		chunks   []*keySpillChunk
		buf      []keySpillRecord
		bufBytes uint64
	)

	for gi, g := range groups {
		if g == nil || gi >= len(tables) || tables[gi] == nil {
			// Defensive: groups/tables must be parallel slices supplied by the caller;
			// skip a malformed entry rather than index out of range or deref nil
			// (SPEC-ROOT-001).
			continue
		}
		table := tables[gi]
		for ri := range g.Refs {
			if err := ctx.Err(); err != nil {
				return chunks, nil, err
			}
			r := &g.Refs[ri]
			path := table.Lookup(r.SourceID)
			id, ok := outTable.Intern(path)
			if !ok {
				return chunks, nil, fmt.Errorf("valueindex: mergeGroupsAtKey: %w", ErrStringTableOverflow)
			}
			for si := range r.Spans {
				s := &r.Spans[si]
				rec := keySpillRecord{sourceID: id, ref: r.Ref, traceID: s.TraceID, spanIndexes: s.SpanIndexes}
				buf = append(buf, rec)
				if testHookAccumBufLen != nil {
					testHookAccumBufLen(len(buf))
				}
				bufBytes += uint64(estimateKeySpillRecordBytes(&rec)) //nolint:gosec // bounded by real record sizes
				if bufBytes >= spillThresholdBytes {
					chunk, werr := writeKeySpillChunk(tmpDir, buf)
					if werr != nil {
						return chunks, nil, fmt.Errorf("valueindex: mergeGroupsAtKey: %w", werr)
					}
					chunks = append(chunks, chunk)
					buf = nil
					bufBytes = 0
				}
			}
		}
	}
	return chunks, buf, nil
}

// streamOutputWriter accumulates BucketGroups into fixed-size blocks and writes each block
// directly to a bufio.Writer as it is cut (plan.md Decision 3), tracking the block directory
// and file-level min/max time needed for the eventual footer. Extracted out of
// StreamCompactBucketFiles to keep that function's cyclomatic complexity under the repo's
// gocyclo gate. maxBlockBytes/pendingBytes implement the SPEC-VI-17 byte-size block cap.
type streamOutputWriter struct {
	bw             *bufio.Writer
	dir            []BlockDirEntry
	pending        []BucketGroup
	groupsPerBlock int
	maxBlockBytes  uint64
	pendingBytes   uint64
	bodyEnd        uint64
	fileMin        uint64
	fileMax        uint64
	haveFileTime   bool
}

func newStreamOutputWriter(
	bw *bufio.Writer,
	groupsPerBlock int,
	headerLen, maxBlockBytes uint64,
) *streamOutputWriter {
	return &streamOutputWriter{
		bw:             bw,
		dir:            make([]BlockDirEntry, 0, 8),
		pending:        make([]BucketGroup, 0, groupsPerBlock),
		groupsPerBlock: groupsPerBlock,
		maxBlockBytes:  maxBlockBytes,
		bodyEnd:        headerLen,
	}
}

// estimateBucketGroupBytes is a cheap, directionally-correct (not exact) estimate of g's
// encoded size, used only by the SPEC-VI-17 block-byte-size cap (Approach B, issue #501) —
// never a full re-encode per add() call. perRefBytes/perSpanBytes are rough fixed-size
// approximations of BucketBlockRef's/SpanRef's on-wire shape (bucketfile.go's block layout
// comment).
func estimateBucketGroupBytes(g *BucketGroup) uint64 {
	const perRefBytes = 32
	const perSpanBytes = 24
	// G115: len()/count values here are bounded by in-memory slice sizes, never negative.
	size := uint64(len(g.CanonicalValue))
	size += uint64(len(g.Refs)) * perRefBytes
	size += uint64(bucketGroupSpanCount(g)) * perSpanBytes //nolint:gosec // G115: see above
	return size
}

// add appends g to the pending block, flushing automatically once EITHER groupsPerBlock or
// maxBlockBytes (SPEC-VI-17, Approach B, issue #501) is reached. It reports whether a block
// was just cut (flushed) by this call, so the caller can safely evaluate an output-file-size
// split boundary only at a block boundary — never mid-block, since a block's groups reference
// the current file's string table (NOTE-VI-077).
func (w *streamOutputWriter) add(g BucketGroup) (blockCut bool, err error) {
	w.pending = append(w.pending, g)
	w.pendingBytes += estimateBucketGroupBytes(&g)
	if len(w.pending) >= w.groupsPerBlock || (w.maxBlockBytes > 0 && w.pendingBytes >= w.maxBlockBytes) {
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
	w.pendingBytes = 0
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

// collectContributionsAtKey pops every group currently sharing (keyTS, keyValue) into the
// caller-owned groups/tables/iters buffers (reset via [:0] here, reused across outer-loop
// iterations to avoid a fresh allocation per merged key) — including MULTIPLE groups from the
// SAME iterator, via a combined pop-advance-repush loop mirroring
// collectTraceContributionsAtKey's (stream_trace_compaction.go): after popping an iterator
// whose Peek() matches the key, it is immediately advanced; if it is still positioned at the
// same key it is pushed straight back onto the heap and popped again by this same loop, rather
// than deferred to a later outer-loop pass (NOTE-VI-111 design rationale; regression-guarded by
// TEST-VI-28).
//
// SPEC-VI-3 (amended, issue #501): (TimeSec, CanonicalValue) keys are no longer guaranteed
// unique within a single input file — assembleBucketWithCap/mergeGroupsAtKey/
// splitBucketGroupBySpanCap deliberately emit multiple sibling BucketGroups sharing one key
// when that key's span-ref count would exceed shared.ValueIndexBucketGroupMaxSpanRefs. This
// combined loop (not the old two-phase collect-then-advance split) is what re-consolidates
// those siblings down to the cap across successive compaction generations instead of letting
// them proliferate.
func collectContributionsAtKey(
	ctx context.Context,
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

		it.Advance(ctx)
		if aerr := it.Err(); aerr != nil {
			return nil, nil, nil, aerr
		}
		if _, ok := it.Peek(); ok {
			heap.Push(h, it)
		}
	}
	return groups, tables, iters, nil
}

// StreamCompactBucketFiles performs a heap-based k-way merge across N already-decoded,
// already-filtered GroupIterators, emitting merged BucketGroups in sorted order and
// cutting them into output blocks of at most groupsPerBlock groups (SPEC-VI-2). It never
// materializes the full merged group set: peak memory is bounded by (a) the K input files
// (already decoded by the caller), (b) one in-progress output block, and (c) a transient
// per-key merge-union buffer -- NOT bounded to a fixed K * shared.ValueIndexBucketGroupMaxSpanRefs
// constant. Every OUTPUT BucketGroup is capped at shared.ValueIndexBucketGroupMaxSpanRefs
// (mergeGroupsAtKey, SPEC-VI-16), bounding decode-side cost and preventing unbounded-forever
// single-group growth, but the transient buffer's real size is K * (each contributing iterator's
// own real total fan-in for the key) -- mitigated by CompactMaxInputFiles/CompactBatchBytes
// bounding K and by retention filtering, not by a single unconditional bound (SPEC-VI-2, amended;
// NOTES.md NOTE-VI-111) — replacing MergeBucketFiles+SplitIntoBlocks' map-of-maps for this call
// path. groupsPerBlock <= 0 defaults to shared.ValueIndexBucketGroupsPerBlock. output is called
// at most once per emitted file, or not at all if there is nothing to emit.
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
	tmpDir string,
	output func(path string) error,
) error {
	return streamCompactBucketFilesWithCap(
		ctx, iterators, groupsPerBlock, maxOutputBytes, shared.ValueIndexBucketGroupMaxSpanRefs, tmpDir, output,
	)
}

// streamCompactBucketFilesWithCap is StreamCompactBucketFiles' implementation, parameterized
// by the per-key span-ref cap (SPEC-VI-16, mergeGroupsAtKey, issue #501) so in-package tests
// can exercise hot-key sibling-splitting end-to-end with a small cap without needing
// shared.ValueIndexBucketGroupMaxSpanRefs-sized input. StreamCompactBucketFiles (the
// production entry point) always calls this with the real default; the cap is not
// caller-configurable at this call site (no <= 0 escape hatch), unlike groupsPerBlock.
func streamCompactBucketFilesWithCap(
	ctx context.Context,
	iterators []GroupIterator,
	groupsPerBlock int,
	maxOutputBytes int64,
	maxSpansPerGroup int,
	tmpDir string,
	output func(path string) error,
) error {
	return streamCompactBucketFilesWithCapAndSpillThreshold(
		ctx, iterators, groupsPerBlock, maxOutputBytes, maxSpansPerGroup, tmpDir,
		shared.ValueIndexMergeBufferSpillBytes, output,
	)
}

// streamCompactBucketFilesWithCapAndSpillThreshold is streamCompactBucketFilesWithCap's
// implementation, further parameterized by the per-key merge-buffer spill-trigger byte
// threshold (issue #503) so in-package tests can force spilling end-to-end through the FULL
// StreamCompactBucketFiles call path — mirrors mergeGroupsAtKey's own
// production/mergeGroupsAtKeyWithSpillThreshold test-parameterized-entry-point split.
// streamCompactBucketFilesWithCap (called by the production entry point,
// StreamCompactBucketFiles) always calls this with the real
// shared.ValueIndexMergeBufferSpillBytes constant; there is one implementation, not two
// copies.
func streamCompactBucketFilesWithCapAndSpillThreshold(
	ctx context.Context,
	iterators []GroupIterator,
	groupsPerBlock int,
	maxOutputBytes int64,
	maxSpansPerGroup int,
	tmpDir string,
	spillThresholdBytes uint64,
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

	of, err := newBucketOutputFile(groupsPerBlock, tmpDir)
	if err != nil {
		return err
	}
	// Only the current (in-progress) output file needs cleanup on error: a file that has
	// been finalized+handed to `output` is the caller's responsibility per the callback
	// contract, and finalizeBucketOutputFile clears of.file after a successful handoff. This
	// closure captures `of` by reference, so it always cleans up whichever output file is
	// current at defer-execution time even after addMergedGroupAndMaybeRotate replaces it.
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

		// collectContributionsAtKey advances every contributor inline (issue #501) — there is
		// no separate advance-contributors step here, unlike the pre-#501 two-phase split.
		var cerr error
		contribGroups, contribTables, contribIters, cerr = collectContributionsAtKey(
			ctx, h, keyTS, keyValue, contribGroups, contribTables, contribIters,
		)
		if cerr != nil {
			return cerr
		}

		mergedSiblings, err := mergeGroupsAtKeyWithSpillThreshold(
			ctx, keyTS, keyValue, contribGroups, contribTables, of.table, maxSpansPerGroup, tmpDir, spillThresholdBytes,
		)
		if err != nil {
			return fmt.Errorf("valueindex: StreamCompactBucketFiles: %w", err)
		}
		// One key can now produce more than one sibling group (issue #501): the add+rotate
		// step runs once per sibling, unlike contributor advancement above, which already ran
		// exactly once for this key.
		for _, mg := range mergedSiblings {
			if err := addMergedGroupAndMaybeRotate(&of, mg, maxOutputBytes, groupsPerBlock, tmpDir, h, output); err != nil {
				return err
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

// addMergedGroupAndMaybeRotate adds one merged sibling group to the current output file and,
// at a block boundary, rotates to a fresh output file once the projected finalized size has
// grown past maxOutputBytes (NOTE-VI-077, #482). Extracted out of
// streamCompactBucketFilesWithCap's main loop because mergeGroupsAtKey can now return more
// than one sibling per key (issue #501) — this must run once PER SIBLING, both to keep
// streamCompactBucketFilesWithCap's cyclomatic complexity from growing and because rotation
// must still only ever land on a block boundary regardless of how many siblings share a key.
// ofp is threaded by pointer-to-pointer so a rotation can replace the caller's
// *bucketOutputFile in place.
func addMergedGroupAndMaybeRotate(
	ofp **bucketOutputFile,
	mg BucketGroup,
	maxOutputBytes int64,
	groupsPerBlock int,
	tmpDir string,
	h *bucketIteratorHeap,
	output func(path string) error,
) error {
	of := *ofp
	blockCut, addErr := of.out.add(mg)
	if addErr != nil {
		return addErr
	}

	// NOTE-VI-077 (#482): honor maxOutputBytes by rotating to a fresh output file at a
	// block boundary once the projected finalized file size has grown past the cap.
	// projectedFileSize (not just bodyEnd) accounts for the tail — string table + block
	// index + footer — which for a many-block file dominates the compressed body, so a
	// bodyEnd-only check would never trigger. Rotation is only ever evaluated right after
	// a block was cut (blockCut) so the split lands on a block boundary — never mid-block,
	// since a block's groups reference the current file's string table (a mid-block split
	// would strand references to interned SourceRefs the new file's table does not carry).
	// h.Len() > 0 reflects the heap's true remaining-work state: collectContributionsAtKey
	// advances contributors inline (issue #501) before this function is ever called, so no
	// separate "advance before checking h.Len()" step is needed here.
	//nolint:gosec // G115: maxOutputBytes > 0 is guarded here, so the uint64 conversion is safe.
	if blockCut && maxOutputBytes > 0 && h.Len() > 0 &&
		projectedFileSize(of.out, of.table) >= uint64(maxOutputBytes) {
		if finErr := finalizeBucketOutputFile(of, output); finErr != nil {
			return finErr
		}
		newOf, newErr := newBucketOutputFile(groupsPerBlock, tmpDir)
		if newErr != nil {
			return newErr
		}
		*ofp = newOf
	}
	return nil
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

// newBucketOutputFile creates a fresh temp output file inside tmpDir (issue #503 —
// previously hardcoded os.CreateTemp("", ...), which os.CreateTemp already resolves to
// os.TempDir(), so passing os.TempDir() explicitly here is behavior-preserving for every
// existing caller), writes its header, and returns the bundled per-file writer state.
func newBucketOutputFile(groupsPerBlock int, tmpDir string) (*bucketOutputFile, error) {
	f, err := os.CreateTemp(tmpDir, "vi-merge-out-*.tmp")
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
		out:   newStreamOutputWriter(bw, groupsPerBlock, uint64(len(header)), shared.ValueIndexBucketBlockMaxBytes),
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
