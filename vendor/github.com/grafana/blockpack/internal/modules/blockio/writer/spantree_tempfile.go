package writer

// NOTE-462 (issue #381): spanTreeAccum is the temp-file-backed accumulator for the SpanTree
// structural index. Like the intrinsic tempFileAccum (NOTE-461), it spills one record per span
// to disk as spans are built, so peak RSS does not grow with total span count.
//
// A SpanTree entry must be emitted for every span in the output file. Naively grouping by
// traceID to compute DFS order would hold every record in RAM (the O(spans) accumulator the
// streaming model exists to avoid). Instead, at Flush() this accumulator runs a bounded
// EXTERNAL SORT:
//
//  1. Spill phase (during block build): append fixed-stride spill records
//     (traceID[16] spanID[8] parentID[8] blockIdx[2] rowIdx[2]) to runs of at most
//     spanTreeRunRecords records. Each full run is sorted in memory by (traceID, spanID) and
//     written to its own sorted run file, then the in-memory buffer is released. Peak RAM is
//     O(spanTreeRunRecords), not O(spans).
//  2. Merge phase (at Flush): a k-way merge over the sorted run files yields records in global
//     (traceID, spanID) order. Consecutive same-traceID records form one trace; that trace is
//     held in memory (bounded by the largest single trace, not the file), DFS-numbered, and
//     emitted as sorted SpanTree records into independently-snappy-compressed chunks.
//
// Section framing mirrors the chunked trace index: raw (un-snappy) section = header +
// chunk directory + concatenated snappy chunks + trace-ID bloom. Within a chunk the decoded
// records are fixed-stride (shared.SpanTreeRecordSize) so the reader can binary-search on
// traceID without a full decode.

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/klauspost/compress/snappy"
)

// spanTreeRunRecords bounds the number of spill records held in memory before a sorted run is
// flushed to disk. 1<<18 records × spanTreeSpillRecordSize (36 B) ≈ 9.4 MiB per run buffer,
// which keeps the spill phase memory-bounded regardless of total span count.
//
// Declared as a var (not const) so tests can lower it to exercise the multi-run external-merge
// path without writing hundreds of thousands of spans. Production code must never modify it.
var spanTreeRunRecords = 1 << 18

// spanTreeSpillRecordSize is the fixed stride of a spill record on disk:
// traceID[16] + spanID[8] + parentID[8] + blockIdx[2] + rowIdx[2] = 36 bytes.
const spanTreeSpillRecordSize = 36

// spillRecord is one in-memory spill record during the spill/sort phases.
type spillRecord struct {
	traceID  [16]byte
	spanID   [8]byte
	parentID [8]byte
	blockIdx uint16
	rowIdx   uint16
}

// spanTreeAccum spills span records to sorted run files and, at Flush, external-merges them
// into a SpanTree section.
type spanTreeAccum struct {
	dir      string
	tmpDir   string // non-empty if owned (removed on close)
	runs     []string
	buf      []spillRecord
	spanSeen int
}

// newSpanTreeAccum creates a spanTreeAccum spilling into scratchDir. When scratchDir is empty
// a unique owned temp directory is created (removed on close). When non-empty the caller owns
// the directory; only the run files this accumulator creates are removed on close.
func newSpanTreeAccum(scratchDir string) (*spanTreeAccum, error) {
	dir := scratchDir
	owned := ""
	if dir == "" {
		d, err := os.MkdirTemp("", "blockpack-spantree-*")
		if err != nil {
			return nil, fmt.Errorf("spantree spill: create temp dir: %w", err)
		}
		dir = d
		owned = d
	}
	return &spanTreeAccum{
		dir:    dir,
		tmpDir: owned,
		buf:    make([]spillRecord, 0, spanTreeRunRecords),
	}, nil
}

// add buffers one span record. spanID is required; traceID identifies the trace. A zero
// parentID marks a root span. When the in-memory buffer fills, a sorted run is flushed.
func (s *spanTreeAccum) add(traceID [16]byte, spanID, parentID []byte, blockIdx, rowIdx uint16) error {
	var rec spillRecord
	rec.traceID = traceID
	copy(rec.spanID[:], spanID)
	copy(rec.parentID[:], parentID)
	rec.blockIdx = blockIdx
	rec.rowIdx = rowIdx
	s.buf = append(s.buf, rec)
	s.spanSeen++
	if len(s.buf) >= spanTreeRunRecords {
		return s.flushRun()
	}
	return nil
}

// flushRun sorts the in-memory buffer by (traceID, spanID) and writes it to a new run file,
// then releases the buffer's contents (capacity retained for reuse).
func (s *spanTreeAccum) flushRun() error {
	if len(s.buf) == 0 {
		return nil
	}
	slices.SortFunc(s.buf, compareSpillRecord)

	f, err := os.CreateTemp(s.dir, "spantree-run-*")
	if err != nil {
		return fmt.Errorf("spantree spill: create run: %w", err)
	}
	w := bufio.NewWriterSize(f, 1<<16)
	var rb [spanTreeSpillRecordSize]byte
	for i := range s.buf {
		encodeSpillRecord(rb[:], s.buf[i])
		if _, werr := w.Write(rb[:]); werr != nil {
			_ = f.Close()
			return fmt.Errorf("spantree spill: write run: %w", werr)
		}
	}
	if ferr := w.Flush(); ferr != nil {
		_ = f.Close()
		return fmt.Errorf("spantree spill: flush run: %w", ferr)
	}
	if cerr := f.Close(); cerr != nil {
		return fmt.Errorf("spantree spill: close run: %w", cerr)
	}
	s.runs = append(s.runs, f.Name())
	s.buf = s.buf[:0]
	return nil
}

// compareSpillRecord orders spill records by (traceID, spanID).
func compareSpillRecord(a, b spillRecord) int {
	if c := bytes.Compare(a.traceID[:], b.traceID[:]); c != 0 {
		return c
	}
	return bytes.Compare(a.spanID[:], b.spanID[:])
}

func encodeSpillRecord(dst []byte, r spillRecord) {
	copy(dst[0:16], r.traceID[:])
	copy(dst[16:24], r.spanID[:])
	copy(dst[24:32], r.parentID[:])
	binary.LittleEndian.PutUint16(dst[32:34], r.blockIdx)
	binary.LittleEndian.PutUint16(dst[34:36], r.rowIdx)
}

func decodeSpillRecord(src []byte) spillRecord {
	var r spillRecord
	copy(r.traceID[:], src[0:16])
	copy(r.spanID[:], src[16:24])
	copy(r.parentID[:], src[24:32])
	r.blockIdx = binary.LittleEndian.Uint16(src[32:34])
	r.rowIdx = binary.LittleEndian.Uint16(src[34:36])
	return r
}

// runReader streams sorted records from one run file.
type runReader struct {
	r   *bufio.Reader
	f   *os.File
	cur spillRecord
	buf [spanTreeSpillRecordSize]byte
	ok  bool
}

func openRunReader(name string) (*runReader, error) {
	f, err := os.Open(name) //nolint:gosec // path produced by os.CreateTemp in our own dir
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, r: bufio.NewReaderSize(f, 1<<16)}
	rr.advance()
	return rr, nil
}

// advance loads the next record into rr.cur, setting rr.ok=false at EOF.
func (rr *runReader) advance() {
	if _, err := io.ReadFull(rr.r, rr.buf[:]); err != nil {
		rr.ok = false
		return
	}
	rr.cur = decodeSpillRecord(rr.buf[:])
	rr.ok = true
}

func (rr *runReader) close() { _ = rr.f.Close() }

// runHeap is a min-heap over run readers ordered by their current record.
type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return compareSpillRecord(h[i].cur, h[j].cur) < 0 }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *runHeap) Push(x any) { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

// build runs the external merge and serializes the SpanTree section. Returns nil (no section)
// when no spans were fed. The caller (writeV8FileSections) writes the returned blob raw.
func (s *spanTreeAccum) build(blockCount int) ([]byte, error) {
	// Flush any tail buffer into a final run.
	if err := s.flushRun(); err != nil {
		return nil, err
	}
	if s.spanSeen == 0 || len(s.runs) == 0 {
		return nil, nil
	}

	readers := make([]*runReader, 0, len(s.runs))
	for _, name := range s.runs {
		rr, err := openRunReader(name)
		if err != nil {
			for _, r := range readers {
				r.close()
			}
			return nil, fmt.Errorf("spantree merge: open run: %w", err)
		}
		readers = append(readers, rr)
	}
	defer func() {
		for _, rr := range readers {
			rr.close()
		}
	}()

	h := make(runHeap, 0, len(readers))
	for _, rr := range readers {
		if rr.ok {
			h = append(h, rr)
		}
	}
	heap.Init(&h)

	enc := newSpanTreeEncoder(blockCount)
	var trace []spillRecord
	var curTrace [16]byte
	haveTrace := false

	for h.Len() > 0 {
		top := h[0]
		rec := top.cur
		if !haveTrace {
			curTrace = rec.traceID
			haveTrace = true
		} else if rec.traceID != curTrace {
			if err := enc.emitTrace(trace); err != nil {
				return nil, err
			}
			trace = trace[:0]
			curTrace = rec.traceID
		}
		trace = append(trace, rec)

		top.advance()
		if top.ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	if len(trace) > 0 {
		if err := enc.emitTrace(trace); err != nil {
			return nil, err
		}
	}
	return enc.finish(), nil
}

// close removes all run files and, if owned, the temp directory. Idempotent.
func (s *spanTreeAccum) close() error {
	var firstErr error
	for _, name := range s.runs {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	s.runs = nil
	s.buf = nil
	if s.tmpDir != "" {
		if err := os.RemoveAll(s.tmpDir); err != nil && firstErr == nil {
			firstErr = err
		}
		s.tmpDir = ""
	}
	return firstErr
}

// ---- section encoder ----

// spanTreeEncoder accumulates DFS-numbered records one trace at a time into snappy chunks and
// finalizes the section framing (header + directory + chunks + bloom).
type spanTreeEncoder struct {
	chunkBuf   []byte // current uncompressed chunk (concatenated fixed-stride records)
	dir        []spanTreeDirEnt
	traceIDs   [][16]byte   // for the bloom (one per trace)
	compChunks bytes.Buffer // concatenated compressed chunks
	blockCount int
	spanCount  int
	traceCount int
	chunkN     int      // records in current chunk
	firstID    [16]byte // first traceID of the current open chunk
	firstIDSet bool
}

type spanTreeDirEnt struct {
	firstID   [16]byte
	compOff   uint32
	compLen   uint32
	spanCount uint32
}

func newSpanTreeEncoder(blockCount int) *spanTreeEncoder {
	return &spanTreeEncoder{
		blockCount: blockCount,
		chunkBuf:   make([]byte, 0, shared.SpanTreeRecordsPerChunk*shared.SpanTreeRecordSize),
	}
}

// emitTrace DFS-numbers one trace's records and appends them to the current chunk. A chunk is
// sealed once it reaches SpanTreeRecordsPerChunk, but never mid-trace, so a trace's records are
// always contiguous in one chunk (the reader relies on this).
func (e *spanTreeEncoder) emitTrace(recs []spillRecord) error {
	if len(recs) == 0 {
		return nil
	}
	dfs, err := dfsNumber(recs)
	if err != nil {
		return err
	}
	e.traceCount++
	e.traceIDs = append(e.traceIDs, recs[0].traceID)

	if !e.firstIDSet {
		e.firstID = recs[0].traceID
		e.firstIDSet = true
	}

	var rb [shared.SpanTreeRecordSize]byte
	for i := range dfs {
		shared.EncodeSpanTreeRecord(rb[:], dfs[i])
		e.chunkBuf = append(e.chunkBuf, rb[:]...)
		e.chunkN++
		e.spanCount++
	}

	if e.chunkN >= shared.SpanTreeRecordsPerChunk {
		e.sealChunk()
	}
	return nil
}

// sealChunk snappy-compresses the current chunk buffer, records a directory entry, and resets
// the chunk buffer for the next trace.
func (e *spanTreeEncoder) sealChunk() {
	if e.chunkN == 0 {
		return
	}
	compressed := snappy.Encode(nil, e.chunkBuf)
	e.dir = append(e.dir, spanTreeDirEnt{
		firstID:   e.firstID,
		compOff:   0,                       // backfilled in finish()
		compLen:   uint32(len(compressed)), //nolint:gosec // bounded by chunk size
		spanCount: uint32(e.chunkN),        //nolint:gosec
	})
	e.compChunks.Write(compressed)
	e.chunkBuf = e.chunkBuf[:0]
	e.chunkN = 0
	e.firstIDSet = false
}

// finish seals the last chunk, builds the trace-ID bloom, and serializes the full section.
func (e *spanTreeEncoder) finish() []byte {
	e.sealChunk()

	bloomSize := shared.TraceIDBloomSize(e.traceCount)
	bloom := make([]byte, bloomSize)
	for _, tid := range e.traceIDs {
		shared.AddTraceIDToBloom(bloom, tid)
	}

	chunkCount := len(e.dir)
	dirOff := shared.SpanTreeHeaderSize
	chunksOff := dirOff + chunkCount*shared.SpanTreeDirEntrySize
	bloomOff := chunksOff + e.compChunks.Len()

	var running int
	for i := range e.dir {
		e.dir[i].compOff = uint32(chunksOff + running) //nolint:gosec
		running += int(e.dir[i].compLen)
	}

	var out bytes.Buffer
	out.Grow(bloomOff + bloomSize)
	var tmp4 [4]byte

	binary.LittleEndian.PutUint32(tmp4[:], shared.SpanTreeMagic)
	out.Write(tmp4[:])
	out.WriteByte(shared.SpanTreeVersion)
	out.Write([]byte{0, 0, 0})                                   // reserved
	binary.LittleEndian.PutUint32(tmp4[:], uint32(e.blockCount)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(e.traceCount)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(e.spanCount)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(chunkCount)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(dirOff)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(bloomOff)) //nolint:gosec
	out.Write(tmp4[:])
	binary.LittleEndian.PutUint32(tmp4[:], uint32(bloomSize)) //nolint:gosec
	out.Write(tmp4[:])

	for i := range e.dir {
		out.Write(e.dir[i].firstID[:])
		binary.LittleEndian.PutUint32(tmp4[:], e.dir[i].compOff)
		out.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], e.dir[i].compLen)
		out.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], e.dir[i].spanCount)
		out.Write(tmp4[:])
	}
	out.Write(e.compChunks.Bytes())
	out.Write(bloom)

	return out.Bytes()
}

// dfsNumber assigns DFS in/out counters to one trace's spans and returns the records sorted by
// (dfsIn) — i.e. parent always before its children. recs may be in any order on input.
//
// Tree reconstruction: index spans by spanID, then link each span to its parent. Spans whose
// parentID is zero, or whose parentID is not present in this set (cross-block/cross-trace
// dangling parent), are treated as roots. A single iterative DFS over the forest assigns the
// counters. Children are visited in (spanID) order for determinism.
func dfsNumber(recs []spillRecord) ([]shared.SpanTreeRecord, error) {
	n := len(recs)
	bySpan := make(map[[8]byte]int, n)
	for i := range recs {
		bySpan[recs[i].spanID] = i
	}

	children := make(map[int][]int, n)
	roots := make([]int, 0, 1)
	for i := range recs {
		pid := recs[i].parentID
		if pid == ([8]byte{}) {
			roots = append(roots, i)
			continue
		}
		if p, ok := bySpan[pid]; ok && p != i {
			children[p] = append(children[p], i)
		} else {
			// Parent not in this trace's record set (dangling) — treat as a root so the
			// span still gets a valid DFS interval and is reachable.
			roots = append(roots, i)
		}
	}

	// Deterministic child ordering by spanID.
	for p := range children {
		cs := children[p]
		slices.SortFunc(cs, func(a, b int) int { return bytes.Compare(recs[a].spanID[:], recs[b].spanID[:]) })
		children[p] = cs
	}
	slices.SortFunc(roots, func(a, b int) int { return bytes.Compare(recs[a].spanID[:], recs[b].spanID[:]) })

	out := make([]shared.SpanTreeRecord, 0, n)
	var counter uint32

	// Iterative DFS to avoid deep recursion on pathological chains. Each stack frame tracks
	// the node and the next child index to visit; dfsIn is assigned on first visit, dfsOut
	// after all children are exhausted.
	type frame struct {
		node     int
		childPos int
		dfsIn    uint32
	}
	dfsIn := make([]uint32, n)
	visited := make([]bool, n)

	for _, root := range roots {
		stack := []frame{{node: root, childPos: 0, dfsIn: 0}}
		// Assign root's dfsIn.
		dfsIn[root] = counter
		counter++
		visited[root] = true
		stack[0].dfsIn = dfsIn[root]
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			cs := children[top.node]
			if top.childPos < len(cs) {
				child := cs[top.childPos]
				top.childPos++
				if visited[child] {
					// Cycle guard: a malformed parent chain could revisit a node.
					continue
				}
				visited[child] = true
				dfsIn[child] = counter
				counter++
				stack = append(stack, frame{node: child, childPos: 0, dfsIn: dfsIn[child]})
				continue
			}
			// Exhausted children: assign dfsOut and pop.
			dfsOut := counter
			counter++
			r := recs[top.node]
			out = append(out, shared.SpanTreeRecord{
				TraceID:  r.traceID,
				SpanID:   r.spanID,
				ParentID: r.parentID,
				DFSIn:    top.dfsIn,
				DFSOut:   dfsOut,
				BlockIdx: r.blockIdx,
				RowIdx:   r.rowIdx,
			})
			stack = stack[:len(stack)-1]
		}
	}

	// Any unvisited records (e.g. part of an isolated cycle) get a degenerate self-interval so
	// the section still contains a row for every span.
	for i := range recs {
		if !visited[i] {
			in := counter
			counter++
			out2 := counter
			counter++
			r := recs[i]
			out = append(out, shared.SpanTreeRecord{
				TraceID:  r.traceID,
				SpanID:   r.spanID,
				ParentID: r.parentID,
				DFSIn:    in,
				DFSOut:   out2,
				BlockIdx: r.blockIdx,
				RowIdx:   r.rowIdx,
			})
		}
	}

	if len(out) != n {
		return nil, fmt.Errorf("spantree dfs: produced %d records for %d spans", len(out), n)
	}
	// Sort by dfsIn so records are stored parent-before-children (single-scan reconstruction).
	slices.SortFunc(out, func(a, b shared.SpanTreeRecord) int {
		return int(a.DFSIn) - int(b.DFSIn)
	})
	return out, nil
}
