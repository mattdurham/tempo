package writer

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// NOTE-461 (issue #380): tempFileAccum is the temp-file-backed replacement for the
// file-level intrinsicAccumulator. The old in-memory accumulator held
// map[column]→[]entry for EVERY span in the output file until Flush(); a pyroscope
// profile during compaction showed (*intrinsicAccumulator).merge as the dominant live
// allocation (tens of GB across workers), causing OOMKills and forcing max_input_blocks=2.
//
// Instead of accumulating all columns × all spans in RAM, tempFileAccum appends each fed
// row to a per-column spill file on disk as spans are processed. At Flush(), it rebuilds
// one column's in-memory flatAccum/dictAccum at a time from its spill file, encodes it via
// the existing encodeColumn path (which sorts internally), then releases it before moving to
// the next column. Peak memory becomes O(largest single column's rows) instead of
// O(all columns × all spans).
//
// Append order: rows are spilled in write order — block-by-block in ascending blockID,
// row-by-row within each block — so each column file is naturally ordered by
// (blockIdx ASC, rowIdx ASC). The encode path re-sorts (sortFlatAccum / sortDictEntries)
// per the wire-format requirement (value order for flat, value-grouped for dict), so
// correctness does not depend on the spill order; the spill order only documents the
// natural sequential-block-access property described in issue #380.
//
// Wire format per spill record (column kind/type fixed for the whole file, stored once in
// the columnSpill header — not per record):
//
//	flat uint64 / dict int64:  blockIdx[2 LE] rowIdx[2 LE] value[8 LE]
//	flat bytes  / dict string: blockIdx[2 LE] rowIdx[2 LE] valueLen[4 LE] value[valueLen]

// spillKind classifies how a column's rows are encoded in its spill file and which
// in-memory accumulator (flat vs dict) is rebuilt at encode time.
type spillKind uint8

const (
	spillFlatUint64 spillKind = iota
	spillFlatBytes
	spillDictString
	spillDictInt64
)

// columnSpill owns one column's on-disk spill file plus a buffered writer.
type columnSpill struct {
	w       *bufio.Writer
	f       *os.File
	colType shared.ColumnType
	kind    spillKind
	count   int
}

// tempFileAccum is the file-level intrinsic accumulator backed by per-column spill files.
// It mirrors the feed* / columnNames / encodeColumn / overCap surface of the in-memory
// intrinsicAccumulator that the per-block localAccum still uses.
type tempFileAccum struct {
	dir    string
	cols   map[string]*columnSpill
	tmpDir string // non-empty if this accumulator owns dir and must remove it on close
}

// newTempFileAccum creates a tempFileAccum that spills into scratchDir. When scratchDir is
// empty a unique temp directory under os.TempDir() is created and owned by the accumulator
// (removed by close). When scratchDir is non-empty the caller owns it; the accumulator
// creates per-column files inside it and removes only those files on close.
func newTempFileAccum(scratchDir string) (*tempFileAccum, error) {
	dir := scratchDir
	owned := ""
	if dir == "" {
		d, err := os.MkdirTemp("", "blockpack-intrinsic-*")
		if err != nil {
			return nil, fmt.Errorf("intrinsic spill: create temp dir: %w", err)
		}
		dir = d
		owned = d
	}
	return &tempFileAccum{
		dir:  dir,
		cols: make(map[string]*columnSpill),

		tmpDir: owned,
	}, nil
}

// spillFor returns (creating if needed) the columnSpill for name. The column kind and type
// are taken from the first feed call for that name and assumed stable thereafter (the writer
// always feeds a given intrinsic column with one fixed type).
func (t *tempFileAccum) spillFor(name string, kind spillKind, colType shared.ColumnType) (*columnSpill, error) {
	if cs, ok := t.cols[name]; ok {
		return cs, nil
	}
	f, err := os.CreateTemp(t.dir, "col-*")
	if err != nil {
		return nil, fmt.Errorf("intrinsic spill %q: create file: %w", name, err)
	}
	cs := &columnSpill{
		f:       f,
		w:       bufio.NewWriterSize(f, 1<<16),
		colType: colType,
		kind:    kind,
	}
	t.cols[name] = cs
	return cs, nil
}

// spillEntry writes one complete spill record (ref header + payload) atomically into a
// single 12- or (12+N)-byte buffer so that a write error cannot leave the file mid-entry.
// Returns an error if the underlying bufio.Writer is in a failed state.
func spillEntry(w *bufio.Writer, blockIdx, rowIdx uint16, payload []byte) error {
	// Encode header + payload into a single buffer to avoid a partial write window.
	buf := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint16(buf[0:2], blockIdx)
	binary.LittleEndian.PutUint16(buf[2:4], rowIdx)
	copy(buf[4:], payload)
	_, err := w.Write(buf)
	return err
}

// spillEntryUint64 writes a ref header + 8-byte uint64 payload in one call.
func spillEntryUint64(w *bufio.Writer, blockIdx, rowIdx uint16, v uint64) error {
	var buf [12]byte
	binary.LittleEndian.PutUint16(buf[0:2], blockIdx)
	binary.LittleEndian.PutUint16(buf[2:4], rowIdx)
	binary.LittleEndian.PutUint64(buf[4:12], v)
	_, err := w.Write(buf[:])
	return err
}

// addUint64 spills one uint64 row (span:start, span:duration) for a flat column.
func (t *tempFileAccum) addUint64(name string, colType shared.ColumnType, val uint64, blockIdx, rowIdx uint16) error {
	cs, err := t.spillFor(name, spillFlatUint64, colType)
	if err != nil {
		return err
	}
	if err := spillEntryUint64(cs.w, blockIdx, rowIdx, val); err != nil {
		return fmt.Errorf("intrinsic spill %q: write: %w", name, err)
	}
	cs.count++
	return nil
}

// addBytes spills one byte-slice row (trace:id, span:id, span:parent_id) for a flat column.
// Empty values are skipped (matching the in-memory accumulator's feedBytes guard).
func (t *tempFileAccum) addBytes(name string, colType shared.ColumnType, val []byte, blockIdx, rowIdx uint16) error {
	if len(val) == 0 {
		return nil
	}
	cs, err := t.spillFor(name, spillFlatBytes, colType)
	if err != nil {
		return err
	}
	// Encode length-prefixed payload.
	payload := make([]byte, 4+len(val))
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(val))) //nolint:gosec // value length bounded by span size
	copy(payload[4:], val)
	if err := spillEntry(cs.w, blockIdx, rowIdx, payload); err != nil {
		return fmt.Errorf("intrinsic spill %q: write: %w", name, err)
	}
	cs.count++
	return nil
}

// addString spills one string row (span:name, resource.service.name) for a dict column.
// Empty values are skipped (matching the in-memory accumulator's feedString guard).
func (t *tempFileAccum) addString(name string, colType shared.ColumnType, val string, blockIdx, rowIdx uint16) error {
	if val == "" {
		return nil
	}
	cs, err := t.spillFor(name, spillDictString, colType)
	if err != nil {
		return err
	}
	// Encode length-prefixed payload.
	b := []byte(val)
	payload := make([]byte, 4+len(b))
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(b))) //nolint:gosec // value length bounded by span size
	copy(payload[4:], b)
	if err := spillEntry(cs.w, blockIdx, rowIdx, payload); err != nil {
		return fmt.Errorf("intrinsic spill %q: write: %w", name, err)
	}
	cs.count++
	return nil
}

// addInt64 spills one int64 row (span:kind, span:status) for a dict column.
func (t *tempFileAccum) addInt64(name string, colType shared.ColumnType, val int64, blockIdx, rowIdx uint16) error {
	cs, err := t.spillFor(name, spillDictInt64, colType)
	if err != nil {
		return err
	}
	if err := spillEntryUint64(cs.w, blockIdx, rowIdx, uint64(val)); err != nil { //nolint:gosec // reinterpreting int64 bits as uint64 for binary encoding
		return fmt.Errorf("intrinsic spill %q: write: %w", name, err)
	}
	cs.count++
	return nil
}

// overCap formerly dropped the intrinsic section when any column exceeded MaxIntrinsicRows.
// Removed (see intrinsic_accum.go). Always returns false.
//
//nolint:unused // mirrors intrinsicAccumulator.overCap interface; intentionally kept as stub
func (t *tempFileAccum) overCap() bool {
	return false
}

// columnNames returns all spilled column names, sorted.
func (t *tempFileAccum) columnNames() []string {
	names := make([]string, 0, len(t.cols))
	for n := range t.cols {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// encodeColumn rebuilds one column's in-memory accumulator from its spill file, encodes it
// via the same wire-format path as the in-memory accumulator, then releases the rebuilt
// accumulator. Peak memory is bounded by this single column's row count.
func (t *tempFileAccum) encodeColumn(name string) ([]byte, error) {
	cs, ok := t.cols[name]
	if !ok {
		return nil, nil
	}
	if err := cs.w.Flush(); err != nil {
		return nil, fmt.Errorf("intrinsic spill %q: flush: %w", name, err)
	}
	if _, err := cs.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("intrinsic spill %q: seek: %w", name, err)
	}
	r := bufio.NewReaderSize(cs.f, 1<<16)

	switch cs.kind {
	case spillFlatUint64:
		c := &flatAccum{colType: cs.colType}
		if err := readFlatUint64Spill(r, cs.count, c); err != nil {
			return nil, fmt.Errorf("intrinsic spill %q: read: %w", name, err)
		}
		return encodeColumnFromFlat(c)
	case spillFlatBytes:
		c := &flatAccum{colType: cs.colType}
		if err := readFlatBytesSpill(r, cs.count, c); err != nil {
			return nil, fmt.Errorf("intrinsic spill %q: read: %w", name, err)
		}
		return encodeColumnFromFlat(c)
	case spillDictString:
		c := &dictAccum{index: make(map[string]int), numIndex: make(map[[8]byte]int), colType: cs.colType}
		if err := readDictStringSpill(r, cs.count, c); err != nil {
			return nil, fmt.Errorf("intrinsic spill %q: read: %w", name, err)
		}
		return encodePagedDictColumn(c)
	case spillDictInt64:
		c := &dictAccum{index: make(map[string]int), numIndex: make(map[[8]byte]int), colType: cs.colType}
		if err := readDictInt64Spill(r, cs.count, c); err != nil {
			return nil, fmt.Errorf("intrinsic spill %q: read: %w", name, err)
		}
		return encodePagedDictColumn(c)
	default:
		return nil, fmt.Errorf("intrinsic spill %q: unknown kind %d", name, cs.kind)
	}
}

// encodeColumnFromFlat mirrors the flat-column arm of intrinsicAccumulator.encodeColumn.
func encodeColumnFromFlat(c *flatAccum) ([]byte, error) {
	if len(c.bytesValues) > 0 {
		return encodeXORBytesIntrinsic(c)
	}
	if len(c.uint64Values) > 0 {
		return encodeDeltaUint64Intrinsic(c)
	}
	return encodeFlatColumn(c)
}

func readRefHeader(r *bufio.Reader) (blockIdx, rowIdx uint16, err error) {
	var b [4]byte
	if _, err = io.ReadFull(r, b[:]); err != nil {
		return 0, 0, err
	}
	return binary.LittleEndian.Uint16(b[0:2]), binary.LittleEndian.Uint16(b[2:4]), nil
}

func readFlatUint64Spill(r *bufio.Reader, count int, c *flatAccum) error {
	c.uint64Values = make([]uint64, 0, count)
	c.refs = make([]shared.BlockRef, 0, count)
	var v [8]byte
	for range count {
		blockIdx, rowIdx, err := readRefHeader(r)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, v[:]); err != nil {
			return err
		}
		c.uint64Values = append(c.uint64Values, binary.LittleEndian.Uint64(v[:]))
		c.refs = append(c.refs, shared.BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
	}
	return nil
}

func readFlatBytesSpill(r *bufio.Reader, count int, c *flatAccum) error {
	c.bytesValues = make([][]byte, 0, count)
	c.refs = make([]shared.BlockRef, 0, count)
	var lb [4]byte
	for range count {
		blockIdx, rowIdx, err := readRefHeader(r)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(lb[:])
		val := make([]byte, n)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}
		c.bytesValues = append(c.bytesValues, val)
		c.refs = append(c.refs, shared.BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
	}
	return nil
}

func readDictStringSpill(r *bufio.Reader, count int, c *dictAccum) error {
	var lb [4]byte
	for range count {
		blockIdx, rowIdx, err := readRefHeader(r)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(lb[:])
		val := make([]byte, n)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}
		key := string(val)
		idx, exists := c.index[key]
		if !exists {
			idx = len(c.entries)
			c.index[key] = idx
			c.entries = append(c.entries, dictEntry{strVal: key})
		}
		c.entries[idx].refs = append(c.entries[idx].refs, shared.BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
	}
	return nil
}

func readDictInt64Spill(r *bufio.Reader, count int, c *dictAccum) error {
	var v [8]byte
	for range count {
		blockIdx, rowIdx, err := readRefHeader(r)
		if err != nil {
			return err
		}
		if _, err := io.ReadFull(r, v[:]); err != nil {
			return err
		}
		val := int64(binary.LittleEndian.Uint64(v[:])) //nolint:gosec // reinterpreting uint64 bits as int64
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(val)) //nolint:gosec
		idx, exists := c.numIndex[tmp]
		if !exists {
			idx = len(c.entries)
			c.numIndex[tmp] = idx
			c.entries = append(c.entries, dictEntry{int64Val: val})
		}
		c.entries[idx].refs = append(c.entries[idx].refs, shared.BlockRef{BlockIdx: blockIdx, RowIdx: rowIdx})
	}
	return nil
}

// spillMerge appends every row from an in-memory per-block accumulator into the spill files.
// This replaces intrinsicAccumulator.merge for the file-level accumulator: instead of growing
// in-memory maps it streams the block's rows to disk, bounding peak RSS to one block.
//
// Dict entries in the source localAccum hold deduplicated values with multiple refs; spilling
// expands them back to one (value, ref) record per ref. Re-deduplication happens at
// encodeColumn time when the single column is rebuilt — this keeps the spill append-only and
// streamable while preserving the final dictionary semantics.
func (t *tempFileAccum) spillMerge(src *intrinsicAccumulator) error {
	for name, c := range src.flatCols {
		if len(c.bytesValues) > 0 {
			for i, v := range c.bytesValues {
				ref := c.refs[i]
				if err := t.addBytes(name, c.colType, v, ref.BlockIdx, ref.RowIdx); err != nil {
					return err
				}
			}
			continue
		}
		for i, v := range c.uint64Values {
			ref := c.refs[i]
			if err := t.addUint64(name, c.colType, v, ref.BlockIdx, ref.RowIdx); err != nil {
				return err
			}
		}
	}
	for name, c := range src.dictCols {
		isInt64 := c.colType == shared.ColumnTypeInt64 || c.colType == shared.ColumnTypeRangeInt64
		for _, e := range c.entries {
			for _, ref := range e.refs {
				if isInt64 {
					if err := t.addInt64(name, c.colType, e.int64Val, ref.BlockIdx, ref.RowIdx); err != nil {
						return err
					}
				} else {
					if err := t.addString(name, c.colType, e.strVal, ref.BlockIdx, ref.RowIdx); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// close flushes and removes all spill files and, if owned, the temp directory.
// Safe to call multiple times.
func (t *tempFileAccum) close() error {
	var firstErr error
	for _, cs := range t.cols {
		if cs.f != nil {
			name := cs.f.Name()
			_ = cs.f.Close()
			if err := os.Remove(name); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
			cs.f = nil
		}
	}
	t.cols = make(map[string]*columnSpill)
	if t.tmpDir != "" {
		if err := os.RemoveAll(t.tmpDir); err != nil && firstErr == nil {
			firstErr = err
		}
		t.tmpDir = ""
	}
	return firstErr
}
