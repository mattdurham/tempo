package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// stream_trace_compaction.go — streaming compaction merge for the v2 "VTG2" TraceGroup file
// format (issue #500, NOTE-VI-109). This is the streaming companion to traceindex.go's
// non-streaming MergeTraceGroups: instead of materializing every input file's every group into
// an in-memory map before writing the output, it performs a heap-based k-way merge across
// per-file TraceGroupIterators, bounding peak memory to the K already-decoded input files' one
// current block each, plus one in-progress output block.
//
// Merge-key shape vs. stream_compaction.go (SPEC-VI-14/NOTE-VI-109): BucketGroup's merge key
// (TimeSec, CanonicalValue) was originally also each input file's own per-group uniqueness key
// (SPEC-VI-3), which once let stream_compaction.go's collectContributionsAtKey use a simpler
// two-phase split (pop every iterator currently AT the key, then advance all of them once).
// SPEC-VI-3 was amended by issue #501: a single input file can now hold multiple consecutive
// sibling groups sharing one key (splitBucketGroupBySpanCap's overflow siblings), so
// collectContributionsAtKey now uses the same combined pop-advance-repush loop as
// collectTraceContributionsAtKey below — the two modules converge on this shape. TraceGroup's
// merge key is TraceID alone; TimeSec is deliberately excluded (MergeTraceGroups collapses
// every group sharing a TraceID, regardless of TimeSec, into one output group), so a single
// input file can hold MULTIPLE consecutive groups for the same TraceID (differing only in
// TimeSec) for a structurally different reason. collectTraceContributionsAtKey below is a
// single combined pop-advance-repush loop: after advancing a just-popped iterator, if it is
// still positioned at the same TraceID it is pushed straight back onto the heap and will be
// popped again by the same loop iteration, rather than deferred to a later outer-loop pass.

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// traceIterEntry pairs a TraceGroupIterator with its priority — the original index of its
// input file within the []TraceGroupIterator slice passed to StreamCompactTraceGroups. priority
// breaks ties between iterators currently peeking the same TraceID, and doubles as the
// dedup-order tiebreaker in mergeTraceGroupsAtKey: contributions are always collected in
// ascending priority order (see collectTraceContributionsAtKey), so "first occurrence wins" span
// dedup there matches MergeTraceGroups' own input-order iteration exactly (MergeTraceGroups
// processes `inputs` in slice order, and each input's own groups/spans in their existing
// (TraceID, TimeSec)-sorted order).
type traceIterEntry struct {
	it       TraceGroupIterator
	priority int
}

// traceIteratorHeap implements heap.Interface over a set of traceIterEntry, ordered by
// (current Peek() TraceID ASC, priority ASC). The root is always the lowest-TraceID entry,
// with ties broken by input order.
type traceIteratorHeap struct {
	entries []*traceIterEntry
}

func (h *traceIteratorHeap) Len() int { return len(h.entries) }

func (h *traceIteratorHeap) Less(i, j int) bool {
	gi, iok := h.entries[i].it.Peek()
	gj, jok := h.entries[j].it.Peek()
	if !iok || !jok {
		// Defensive: the heap only ever holds non-exhausted entries; treat an exhausted
		// entry (which should never occur here) as never less than a live one.
		return jok && !iok
	}
	if gi.TraceID != gj.TraceID {
		return less16(gi.TraceID, gj.TraceID)
	}
	return h.entries[i].priority < h.entries[j].priority
}

func (h *traceIteratorHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *traceIteratorHeap) Push(x any) {
	e, ok := x.(*traceIterEntry)
	if !ok {
		slog.Error("traceIteratorHeap.Push: unexpected type", "type", fmt.Sprintf("%T", x))
		return
	}
	h.entries = append(h.entries, e)
}

func (h *traceIteratorHeap) Pop() any {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[:n-1]
	return x
}

// popTraceEntry pops the heap root and asserts its concrete type (SPEC-ROOT-001 — guarded, no
// unchecked type assertion; the heap only ever holds *traceIterEntry values, so a mismatch here
// signals a programming error in this file, not malformed external input).
func popTraceEntry(h *traceIteratorHeap) (*traceIterEntry, error) {
	x := heap.Pop(h)
	e, ok := x.(*traceIterEntry)
	if !ok {
		return nil, fmt.Errorf("valueindex: traceIteratorHeap: unexpected pop type %T", x)
	}
	return e, nil
}

// popStaleTraceEntry pops a heap root whose Peek() already reported exhaustion, returning an
// error either if the pop itself fails, or if the popped iterator recorded a real error.
func popStaleTraceEntry(h *traceIteratorHeap) error {
	e, err := popTraceEntry(h)
	if err != nil {
		return err
	}
	return e.it.Err()
}

// populateTraceMergeHeap pushes every non-exhausted iterator onto a fresh heap (tagged with its
// slice index as priority), propagating any error reported by an iterator that starts out
// already-exhausted.
func populateTraceMergeHeap(iterators []TraceGroupIterator) (*traceIteratorHeap, error) {
	h := &traceIteratorHeap{entries: make([]*traceIterEntry, 0, len(iterators))}
	for i, it := range iterators {
		if _, ok := it.Peek(); ok {
			heap.Push(h, &traceIterEntry{it: it, priority: i})
			continue
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// collectTraceContributionsAtKey pops every group across every iterator currently sharing
// TraceID key into buf, in ascending priority order (see traceIterEntry's doc comment) —
// including MULTIPLE groups from the SAME iterator when one input file holds several groups
// for this TraceID at different TimeSec values (the combined pop-advance-repush loop this
// function's own doc comment above describes, required because TraceGroup's merge key omits
// TimeSec unlike BucketGroup's).
func collectTraceContributionsAtKey(
	ctx context.Context,
	h *traceIteratorHeap,
	key [16]byte,
	buf []*TraceGroup,
) ([]*TraceGroup, error) {
	buf = buf[:0]
	for h.Len() > 0 {
		g, ok := h.entries[0].it.Peek()
		if !ok {
			if err := popStaleTraceEntry(h); err != nil {
				return nil, err
			}
			continue
		}
		if g.TraceID != key {
			break
		}
		e, err := popTraceEntry(h)
		if err != nil {
			return nil, err
		}
		buf = append(buf, g)
		e.it.Advance(ctx)
		if aerr := e.it.Err(); aerr != nil {
			return nil, aerr
		}
		if _, ok := e.it.Peek(); ok {
			heap.Push(h, e)
		}
	}
	return buf, nil
}

// mergeTraceGroupsAtKey merges every same-TraceID contribution (already collected in ascending
// priority order by collectTraceContributionsAtKey) into one output TraceGroup: TimeSec is the
// minimum across contributions, and spans are deduplicated by SpanID with first-occurrence-wins
// semantics — identical to MergeTraceGroups (traceindex.go), verified by the equivalence test
// (TestStreamCompactTraceGroups_MatchesMergeTraceGroups, TEST-VI-24).
func mergeTraceGroupsAtKey(traceID [16]byte, contributions []*TraceGroup) TraceGroup {
	seenSpan := make(map[[8]byte]struct{})
	out := TraceGroup{TraceID: traceID}
	haveTime := false
	for _, g := range contributions {
		if g == nil {
			continue
		}
		if !haveTime || g.TimeSec < out.TimeSec {
			out.TimeSec = g.TimeSec
		}
		haveTime = true
		for i := range g.Spans {
			s := g.Spans[i]
			if _, dup := seenSpan[s.SpanID]; dup {
				continue
			}
			seenSpan[s.SpanID] = struct{}{}
			out.Spans = append(out.Spans, s)
		}
	}
	sortSpanEntries(out.Spans)
	return out
}

// traceStreamWriter accumulates TraceGroups into fixed-size blocks and writes each block
// directly to a bufio.Writer as it is cut, tracking the block directory and file-level min/max
// time needed for the eventual footer. Mirrors streamOutputWriter (stream_compaction.go).
type traceStreamWriter struct {
	bw             *bufio.Writer
	table          *StringTable
	dir            []traceBlockDirEntry
	pending        []TraceGroup
	groupsPerBlock int
	bodyEnd        uint64
	fileMin        uint64
	fileMax        uint64
	haveFileTime   bool
}

func newTraceStreamWriter(bw *bufio.Writer, table *StringTable, groupsPerBlock int, headerLen uint64) *traceStreamWriter {
	return &traceStreamWriter{
		bw:             bw,
		table:          table,
		dir:            make([]traceBlockDirEntry, 0, 8),
		pending:        make([]TraceGroup, 0, groupsPerBlock),
		groupsPerBlock: groupsPerBlock,
		bodyEnd:        headerLen,
	}
}

// add appends g to the pending block, flushing automatically once groupsPerBlock is reached. It
// reports whether a block was just cut (flushed) by this call, mirroring streamOutputWriter.add.
func (w *traceStreamWriter) add(g TraceGroup) (blockCut bool, err error) {
	w.pending = append(w.pending, g)
	if len(w.pending) >= w.groupsPerBlock {
		if err := w.flush(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// flush writes the pending block (if non-empty) to bw and records its directory entry, interning
// every pending span's SourceRef into w.table first — encodeTraceBlock's own doc comment notes
// its SourceRef table.Intern call assumes the caller already interned every value, exactly as
// encodeTraceGroups' outer pre-intern loop does for the non-streaming path (traceindex.go).
func (w *traceStreamWriter) flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	for i := range w.pending {
		for j := range w.pending[i].Spans {
			if _, ok := w.table.Intern(w.pending[i].Spans[j].SourceRef); !ok {
				return fmt.Errorf("valueindex: StreamCompactTraceGroups: %w", ErrStringTableOverflow)
			}
		}
	}
	raw := encodeTraceBlock(w.pending, w.table)
	compressed := snappy.Encode(nil, raw)
	if _, err := w.bw.Write(compressed); err != nil {
		return fmt.Errorf("valueindex: StreamCompactTraceGroups: write block: %w", err)
	}
	bMin, bMax := traceBlockTimeRange(w.pending)
	w.dir = append(w.dir, traceBlockDirEntry{
		compOff:    w.bodyEnd,
		compLen:    uint64(len(compressed)),
		minTraceID: w.pending[0].TraceID,
		maxTraceID: w.pending[len(w.pending)-1].TraceID,
		minTimeSec: bMin,
		maxTimeSec: bMax,
	})
	w.bodyEnd += uint64(len(compressed))
	if !w.haveFileTime || bMin < w.fileMin {
		w.fileMin = bMin
	}
	if !w.haveFileTime || bMax > w.fileMax {
		w.fileMax = bMax
	}
	w.haveFileTime = true
	w.pending = make([]TraceGroup, 0, w.groupsPerBlock)
	return nil
}

// projectedTraceFileSize estimates the eventual on-disk size of the output file if finalized
// right now, mirroring projectedFileSize (stream_compaction.go)'s role in the NOTE-VI-077-style
// output-size split heuristic. Unlike BucketGroup's BlockDirEntry (variable-length MinValue/
// MaxValue), traceBlockDirEntry is fixed-size (traceBlockDirEntrySize), so no per-entry variable
// length needs summing.
func projectedTraceFileSize(w *traceStreamWriter, table *StringTable) uint64 {
	blockIdxSize := uint64(4) + uint64(len(w.dir))*traceBlockDirEntrySize
	//nolint:gosec // G115: EncodedSize is a byte length, always >= 0, never overflows uint64.
	return w.bodyEnd + uint64(table.EncodedSize()) + blockIdxSize + traceFooterSize
}

// traceOutputFile bundles the per-output-file state StreamCompactTraceGroups cuts blocks into.
// Mirrors bucketOutputFile (stream_compaction.go).
type traceOutputFile struct {
	file  *os.File
	bw    *bufio.Writer
	table *StringTable
	out   *traceStreamWriter
}

// newTraceOutputFile creates a fresh temp output file under tmpDir, writes its header, and
// returns the bundled per-file writer state. Uses the same "vi-merge-out-*.tmp" naming
// convention as newBucketOutputFile — both are already covered by the single
// sweepOrphanedMergeTempFilesIn glob (temp_cleanup.go), so no changes are needed there. tmpDir
// is caller-supplied (mirrors writeLocalTempInput's own dir parameter, valueindexcompactor/
// diskstage.go) rather than hardcoded to the empty string (which os.CreateTemp resolves to
// os.TempDir()), so a caller can point compaction's local staging at a specific mount path
// once one exists, instead of always assuming the container's default /tmp.
func newTraceOutputFile(groupsPerBlock int, tmpDir string) (*traceOutputFile, error) {
	f, err := os.CreateTemp(tmpDir, "vi-merge-out-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("valueindex: StreamCompactTraceGroups: create temp output: %w", err)
	}
	bw := bufio.NewWriterSize(f, 256<<10)
	header := binary.LittleEndian.AppendUint32(make([]byte, 0, 5), TraceFileMagic)
	header = append(header, TraceFileVersion)
	if _, err := bw.Write(header); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name()) //nolint:gosec // G703: f is our own os.CreateTemp file, not user-supplied.
		return nil, fmt.Errorf("valueindex: StreamCompactTraceGroups: write header: %w", err)
	}
	table := NewStringTable()
	return &traceOutputFile{
		file:  f,
		bw:    bw,
		table: table,
		out:   newTraceStreamWriter(bw, table, groupsPerBlock, uint64(len(header))),
	}, nil
}

// cleanup closes and removes the current (not-yet-finalized) temp file. No-op once the file has
// been handed off to `output` (finalizeTraceOutputFile clears of.file).
func (of *traceOutputFile) cleanup() {
	if of == nil || of.file == nil {
		return
	}
	name := of.file.Name()
	_ = of.file.Close()
	//nolint:gosec // G703: name is our own os.CreateTemp file, not user-supplied.
	_ = os.Remove(name)
	of.file = nil
}

// writeTraceFileTail writes the string table, block index, and footer directly to bw,
// completing a file whose header and block body were already written by the caller. Mirrors
// encodeTraceGroups' own tail-assembly (traceindex.go) byte-for-byte — reusing
// EncodeStringTable and appendTraceBlockIndex directly rather than duplicating their logic —
// but writes incrementally to bw instead of building one in-memory []byte, exactly as
// writeBucketFileTail (bucketfile.go) does for the sibling BucketGroup format.
//
// format; different magic/footer-size constants and block-index encoders (this format's
// TraceFileMagic/traceFooterSize/appendTraceBlockIndex vs. the sibling's
// BucketFileMagic/bucketFooterSize/appendBlockIndex) prevent sharing without adding an
// abstraction layer that serves only these two call sites.
//
//nolint:dupl // intentional mirror of writeBucketFileTail (bucketfile.go) for the BucketGroup
func writeTraceFileTail(
	bw *bufio.Writer,
	dir []traceBlockDirEntry,
	table *StringTable,
	fileMin, fileMax, bodyEnd uint64,
) error {
	if table == nil {
		table = NewStringTable()
	}

	strOff := bodyEnd
	strBytes := EncodeStringTable(table)
	if _, err := bw.Write(strBytes); err != nil {
		return fmt.Errorf("valueindex: writeTraceFileTail: write string table: %w", err)
	}
	strLen := uint64(len(strBytes))

	blockIdxOff := strOff + strLen
	blockIdxBytes := appendTraceBlockIndex(nil, dir)
	if _, err := bw.Write(blockIdxBytes); err != nil {
		return fmt.Errorf("valueindex: writeTraceFileTail: write block index: %w", err)
	}
	blockIdxLen := uint64(len(blockIdxBytes))

	footer := make([]byte, 0, traceFooterSize)
	footer = binary.LittleEndian.AppendUint32(footer, TraceFileMagic)
	footer = binary.LittleEndian.AppendUint64(footer, blockIdxOff)
	footer = binary.LittleEndian.AppendUint64(footer, blockIdxLen)
	footer = binary.LittleEndian.AppendUint64(footer, strOff)
	footer = binary.LittleEndian.AppendUint64(footer, strLen)
	footer = binary.LittleEndian.AppendUint64(footer, fileMin)
	footer = binary.LittleEndian.AppendUint64(footer, fileMax)
	footer = append(footer, TraceFileVersion)
	if _, err := bw.Write(footer); err != nil {
		return fmt.Errorf("valueindex: writeTraceFileTail: write footer: %w", err)
	}
	return nil
}

// finalizeTraceOutputFile flushes any pending block, writes the tail, flushes+closes the file,
// and hands its path to `output`. On success it clears of.file so the deferred cleanup will not
// remove the file the caller now owns. Mirrors finalizeBucketOutputFile.
func finalizeTraceOutputFile(of *traceOutputFile, output func(path string) error) error {
	if err := of.out.flush(); err != nil {
		return err
	}
	if err := writeTraceFileTail(of.bw, of.out.dir, of.table, of.out.fileMin, of.out.fileMax, of.out.bodyEnd); err != nil {
		return err
	}
	if err := of.bw.Flush(); err != nil {
		return fmt.Errorf("valueindex: StreamCompactTraceGroups: flush output: %w", err)
	}
	name := of.file.Name()
	if err := of.file.Close(); err != nil {
		return fmt.Errorf("valueindex: StreamCompactTraceGroups: close output: %w", err)
	}
	of.file = nil // handed off; deferred cleanup must not remove it now
	defer func() { _ = os.Remove(name) }()
	return output(name)
}

// StreamCompactTraceGroups performs a heap-based k-way merge across N already-decoded,
// already-filtered TraceGroupIterators, emitting merged TraceGroups in (TraceID ASC, TimeSec
// ASC) order and cutting them into output blocks of at most groupsPerBlock groups. It never
// materializes the full merged group set: peak memory is bounded by the K input files' one
// current decoded block each plus one in-progress output block plus a transient per-key merge
// buffer. groupsPerBlock <= 0 defaults to shared.ValueIndexTraceGroupsPerBlock. output is called
// at most once per finalized file (more than once only if maxOutputBytes splitting rotates), or
// not at all if there is nothing to emit.
//
// The merged output is staged through a local temp file under tmpDir — a bufio.Writer over a
// fresh os.CreateTemp(tmpDir, ...) file, written block-by-block as each output block is cut,
// never accumulating the full output in memory — removed via defer after `output` returns,
// regardless of success or error. tmpDir is caller-supplied rather than hardcoded to the empty
// string (unlike StreamCompactBucketFiles' sibling os.CreateTemp("", ...) call, which this
// function otherwise mirrors exactly for this contract) so a caller (mergeTraceLevel) can point
// local staging at a specific mount path once one exists, rather than always assuming the
// container's default /tmp; pass "" to fall back to os.TempDir(), matching os.CreateTemp's own
// empty-string convention.
func StreamCompactTraceGroups(
	ctx context.Context,
	iterators []TraceGroupIterator,
	groupsPerBlock int,
	maxOutputBytes int64,
	tmpDir string,
	output func(path string) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if groupsPerBlock <= 0 {
		groupsPerBlock = shared.ValueIndexTraceGroupsPerBlock
	}

	h, err := populateTraceMergeHeap(iterators)
	if err != nil {
		return err
	}

	of, err := newTraceOutputFile(groupsPerBlock, tmpDir)
	if err != nil {
		return err
	}
	defer func() { of.cleanup() }()

	contribs := make([]*TraceGroup, 0, len(iterators))

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		keyGroup, ok := h.entries[0].it.Peek()
		if !ok {
			if err := popStaleTraceEntry(h); err != nil {
				return err
			}
			continue
		}
		key := keyGroup.TraceID

		var cerr error
		contribs, cerr = collectTraceContributionsAtKey(ctx, h, key, contribs)
		if cerr != nil {
			return cerr
		}

		merged := mergeTraceGroupsAtKey(key, contribs)
		blockCut, addErr := of.out.add(merged)
		if addErr != nil {
			return addErr
		}

		// h.Len() > 0 here already reflects every remaining iterator correctly:
		// collectTraceContributionsAtKey re-pushes each contributor immediately after
		// advancing it (unlike stream_compaction.go's two-phase split), so no separate
		// "advance contributors before evaluating the split boundary" step is needed.
		//nolint:gosec // G115: maxOutputBytes > 0 is guarded here, so the uint64 conversion is safe.
		if blockCut && maxOutputBytes > 0 && h.Len() > 0 &&
			projectedTraceFileSize(of.out, of.table) >= uint64(maxOutputBytes) {
			if finErr := finalizeTraceOutputFile(of, output); finErr != nil {
				return finErr
			}
			var newErr error
			if of, newErr = newTraceOutputFile(groupsPerBlock, tmpDir); newErr != nil {
				return newErr
			}
		}
	}

	if err := of.out.flush(); err != nil {
		return err
	}

	if len(of.out.dir) == 0 {
		return nil
	}

	return finalizeTraceOutputFile(of, output)
}
