package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
//
// NOTE-VI-026 (issue #413): external sort-merge for the value-index writer.
//
// AddEntry buffers rawEntry in memory until ValueIndexWriterSpillEntries is reached,
// then sorts the run and spills it to a temp file. Flush k-way merges all spilled
// runs plus the in-memory tail, deduplicating on the fly and streaming the result
// through assemble(). This caps the writer's peak memory to one run plus one output
// chunk regardless of how many entries a single column accumulates, eliminating the
// OOM kills observed on high-volume low-cardinality columns (e.g. resource.k8s
// namespace/cluster names with millions of refs each).

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// runFile is a single sorted run spilled to disk during AddEntry.
type runFile struct {
	f    *os.File
	path string
}

// writeRun sorts the given entries (by the column's canonical order) and writes them
// to a new temp file as a sorted run. The returned runFile is positioned at offset 0
// for subsequent reading.
func writeRun(colType shared.ColumnType, entries []rawEntry) (*runFile, error) {
	sortRawSlice(colType, entries)

	f, err := os.CreateTemp("", "vi-run-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("valueindex: writeRun: create temp: %w", err)
	}
	bw := bufio.NewWriterSize(f, 256<<10)
	name := f.Name()
	for i := range entries {
		if err := writeRawEntry(bw, &entries[i]); err != nil {
			_ = f.Close()
			_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
			return nil, fmt.Errorf("valueindex: writeRun: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
		return nil, fmt.Errorf("valueindex: writeRun: flush: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(name) //nolint:gosec // G703: name comes from os.CreateTemp, not user input
		return nil, fmt.Errorf("valueindex: writeRun: seek: %w", err)
	}
	return &runFile{f: f, path: f.Name()}, nil
}

// remove closes and deletes the run's temp file.
func (r *runFile) remove() {
	if r.f != nil {
		_ = r.f.Close()
	}
	if r.path != "" {
		_ = os.Remove(r.path)
	}
}

// spillV2Flag is the flag byte that prefixes v2 rawEntry spill records.
// 0 = v1 (BlockID uint32), 1 = v2 (BlockRef 5-byte).
const spillV2Flag byte = 1

// spillV4Flag is the flag byte for v4 rawEntry spill records (BlockRef + SpanID + RowIdx).
const spillV4Flag byte = 2

// writeRawEntry serializes one rawEntry in the spill wire format. The valueHash is
// recomputed on read from canonicalValue, so it is not persisted.
//
// v1 wire: flag[1]=0 + val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_id[4]+ref_len[2]+ref[N]
// v2 wire: flag[1]=1 + val_len[2]+val[N]+time_sec[8]+trace_id[16]+block_page[3]+block_len_pages[2]+ref_len[2]+ref[N]
func writeRawEntry(w io.Writer, e *rawEntry) error {
	// Version flag: 0=v1 (BlockID), 1=v2 (BlockRef).
	var ver [1]byte
	if e.spanID != ([8]byte{}) {
		ver[0] = spillV4Flag
	} else if e.blockRef.PageNum > 0 || e.blockRef.LenPages > 0 {
		ver[0] = spillV2Flag
	}
	if _, err := w.Write(ver[:]); err != nil {
		return err
	}
	var hdr [2]byte
	binary.LittleEndian.PutUint16(hdr[:], uint16(len(e.canonicalValue))) //nolint:gosec // bounded
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(e.canonicalValue); err != nil {
		return err
	}
	var fixed [29]byte
	binary.LittleEndian.PutUint64(fixed[0:], e.timeSec)
	copy(fixed[8:24], e.traceID[:])
	var fixedLen int
	//nolint:gosec // intentional little-endian byte masks (the encoding), not overflow
	switch ver[0] {
	case spillV4Flag:
		fixed[24] = byte(e.blockRef.PageNum)
		fixed[25] = byte(e.blockRef.PageNum >> 8)
		fixed[26] = byte(e.blockRef.PageNum >> 16)
		fixed[27] = byte(e.blockRef.LenPages)
		fixed[28] = byte(e.blockRef.LenPages >> 8)
		fixedLen = 29
	case spillV2Flag:
		fixed[24] = byte(e.blockRef.PageNum)
		fixed[25] = byte(e.blockRef.PageNum >> 8)
		fixed[26] = byte(e.blockRef.PageNum >> 16)
		fixed[27] = byte(e.blockRef.LenPages)
		fixed[28] = byte(e.blockRef.LenPages >> 8)
		fixedLen = 29
	default:
		binary.LittleEndian.PutUint32(fixed[24:], e.blockID)
		fixedLen = 28
	}
	if _, err := w.Write(fixed[:fixedLen]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint16(hdr[:], uint16(len(e.sourceRef))) //nolint:gosec // bounded
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, e.sourceRef); err != nil {
		return err
	}
	// v4: append SpanID[8]+RowIdx[2].
	if ver[0] == spillV4Flag {
		var identity [10]byte
		copy(identity[0:8], e.spanID[:])
		binary.LittleEndian.PutUint16(identity[8:10], e.rowIdx)
		if _, err := w.Write(identity[:]); err != nil {
			return err
		}
	}
	return nil
}

// readRawEntry reads one rawEntry from a run file, recomputing valueHash. Returns
// io.EOF at clean end of stream.
func readRawEntry(r *bufio.Reader) (rawEntry, error) {
	// Version flag byte.
	verByte, err := r.ReadByte()
	if err != nil {
		return rawEntry{}, err // io.EOF propagates on clean boundary
	}
	isV2 := verByte == spillV2Flag || verByte == spillV4Flag
	isV4 := verByte == spillV4Flag

	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: val_len: %w", err)
	}
	valLen := int(binary.LittleEndian.Uint16(hdr[:]))
	cv := make([]byte, valLen)
	if _, err := io.ReadFull(r, cv); err != nil {
		return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: value: %w", err)
	}
	var fixed [29]byte
	fixedLen := 28
	if isV2 {
		fixedLen = 29
	}
	if _, err := io.ReadFull(r, fixed[:fixedLen]); err != nil {
		return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: fixed: %w", err)
	}
	var re rawEntry
	re.canonicalValue = cv
	re.timeSec = binary.LittleEndian.Uint64(fixed[0:])
	copy(re.traceID[:], fixed[8:24])
	if isV2 {
		re.blockRef = BlockRef{
			PageNum:  uint32(fixed[24]) | uint32(fixed[25])<<8 | uint32(fixed[26])<<16,
			LenPages: uint16(fixed[27]) | uint16(fixed[28])<<8,
		}
	} else {
		re.blockID = binary.LittleEndian.Uint32(fixed[24:])
	}
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: ref_len: %w", err)
	}
	refLen := int(binary.LittleEndian.Uint16(hdr[:]))
	ref := make([]byte, refLen)
	if _, err := io.ReadFull(r, ref); err != nil {
		return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: ref: %w", err)
	}
	re.sourceRef = string(ref)
	// v4: read SpanID[8]+RowIdx[2].
	if isV4 {
		var identity [10]byte
		if _, err := io.ReadFull(r, identity[:]); err != nil {
			return rawEntry{}, fmt.Errorf("valueindex: readRawEntry: v4 identity: %w", err)
		}
		copy(re.spanID[:], identity[0:8])
		re.rowIdx = binary.LittleEndian.Uint16(identity[8:10])
	}
	re.valueHash = ValueHash16(cv)
	return re, nil
}

// runReader is one input stream of the k-way merge: a buffered reader over a run
// file (or the in-memory tail) holding the next entry not yet emitted.
type runReader struct {
	br    *bufio.Reader // nil for the in-memory tail source
	tail  []rawEntry    // in-memory tail (already sorted); nil for disk runs
	cur   rawEntry      // current head entry
	tailI int           // cursor into tail
	valid bool          // whether cur holds a live entry
}

// advance loads the next entry into cur, setting valid=false at end of stream.
func (rr *runReader) advance() error {
	if rr.br != nil {
		e, err := readRawEntry(rr.br)
		if err != nil {
			if err == io.EOF {
				rr.valid = false
				return nil
			}
			return err
		}
		rr.cur = e
		rr.valid = true
		return nil
	}
	if rr.tailI < len(rr.tail) {
		rr.cur = rr.tail[rr.tailI]
		rr.tailI++
		rr.valid = true
		return nil
	}
	rr.valid = false
	return nil
}

// mergeRuns performs a streaming k-way merge of the spilled runs plus the sorted
// in-memory tail, yielding deduplicated entries in final sorted order. Dedup mirrors
// deduplicateEntries: entries equal on (valueHash, traceID, sourceRef, blockRef,
// timeSec) collapse to one. The yield callback may return an error to abort.
func mergeRuns(
	colType shared.ColumnType,
	runs []*runFile,
	tail []rawEntry,
	yield func(rawEntry) error,
) error {
	sortRawSlice(colType, tail)

	readers := make([]*runReader, 0, len(runs)+1)
	for _, rf := range runs {
		rr := &runReader{br: bufio.NewReaderSize(rf.f, 256<<10)}
		if err := rr.advance(); err != nil {
			return err
		}
		readers = append(readers, rr)
	}
	if len(tail) > 0 {
		rr := &runReader{tail: tail}
		if err := rr.advance(); err != nil {
			return err
		}
		readers = append(readers, rr)
	}

	var (
		prev     rawEntry
		havePrev bool
	)
	for {
		// Find the reader with the smallest current entry.
		minIdx := -1
		for i, rr := range readers {
			if !rr.valid {
				continue
			}
			if minIdx == -1 || compareRawEntry(colType, &rr.cur, &readers[minIdx].cur) < 0 {
				minIdx = i
			}
		}
		if minIdx == -1 {
			break // all readers drained
		}
		cur := readers[minIdx].cur

		if !havePrev || !sameEntry(&cur, &prev) {
			if err := yield(cur); err != nil {
				return err
			}
			prev = cur
			havePrev = true
		}
		if err := readers[minIdx].advance(); err != nil {
			return err
		}
	}
	return nil
}

// compareRawEntry orders two entries by (canonicalValue, timeSec, traceID), matching
// sortRawSlice so the merge yields the same total order.
func compareRawEntry(colType shared.ColumnType, a, b *rawEntry) int {
	if c := compareCanonical(colType, a.canonicalValue, b.canonicalValue); c != 0 {
		return c
	}
	if a.timeSec < b.timeSec {
		return -1
	}
	if a.timeSec > b.timeSec {
		return 1
	}
	if c := bytes.Compare(a.traceID[:], b.traceID[:]); c != 0 {
		return c
	}
	// NOTE-VI-045 (#429): tiebreak on rowIdx to match sortRawSlice.
	if a.rowIdx < b.rowIdx {
		return -1
	}
	if a.rowIdx > b.rowIdx {
		return 1
	}
	return 0
}

// sameEntry reports whether two entries are duplicates for dedup purposes, matching
// deduplicateEntries.
func sameEntry(a, b *rawEntry) bool {
	return a.valueHash == b.valueHash &&
		a.traceID == b.traceID &&
		a.sourceRef == b.sourceRef &&
		a.blockRef == b.blockRef &&
		a.timeSec == b.timeSec &&
		a.rowIdx == b.rowIdx &&
		a.spanID == b.spanID
}
