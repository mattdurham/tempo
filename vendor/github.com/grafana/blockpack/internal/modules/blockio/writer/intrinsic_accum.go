package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// flatAccum accumulates values for a flat intrinsic column (uint64 or bytes).
// All appended rows are stored in parallel arrays (values + refs).
// At flush time, rows are sorted by value before encoding.
type flatAccum struct {
	uint64Values []uint64
	bytesValues  [][]byte
	refs         []shared.BlockRef
	colType      shared.ColumnType
}

// dictEntry holds one unique string/int64 value and all block refs for that value.
type dictEntry struct {
	refs     []shared.BlockRef
	strVal   string
	int64Val int64
}

// dictAccum accumulates values for a dictionary intrinsic column (string or int64).
// Deduplicates values via an index map; collects all refs per unique value.
type dictAccum struct {
	index   map[string]int // encoded value → index in entries
	entries []dictEntry
	colType shared.ColumnType
}

// intrinsicAccumulator holds per-column accumulators for a file being written.
// One instance lives on Writer; fed row-by-row during block building.
type intrinsicAccumulator struct {
	flatCols map[string]*flatAccum
	dictCols map[string]*dictAccum
}

// newIntrinsicAccumulator creates an empty accumulator.
func newIntrinsicAccumulator() *intrinsicAccumulator {
	return &intrinsicAccumulator{
		flatCols: make(map[string]*flatAccum),
		dictCols: make(map[string]*dictAccum),
	}
}

// overCap reports whether any single column exceeds MaxIntrinsicRows.
func (a *intrinsicAccumulator) overCap() bool {
	for _, c := range a.flatCols {
		if len(c.uint64Values)+len(c.bytesValues) > shared.MaxIntrinsicRows {
			return true
		}
	}
	for _, c := range a.dictCols {
		total := 0
		for _, e := range c.entries {
			total += len(e.refs)
		}
		if total > shared.MaxIntrinsicRows {
			return true
		}
	}
	return false
}

// feedUint64 adds one uint64 value (span:duration, span:start, span:end, etc.) to the
// named flat column. colType must be ColumnTypeUint64 or ColumnTypeRangeUint64.
func (a *intrinsicAccumulator) feedUint64(name string, colType shared.ColumnType, val uint64, blockIdx uint16, rowIdx int) {
	c, ok := a.flatCols[name]
	if !ok {
		c = &flatAccum{colType: colType}
		a.flatCols[name] = c
	}
	c.uint64Values = append(c.uint64Values, val)
	c.refs = append(c.refs, shared.BlockRef{BlockIdx: blockIdx, RowIdx: uint16(rowIdx)}) //nolint:gosec // safe: rowIdx bounded by MaxBlockSpans (65535)
}

// feedString adds one string value (span:name, span:status_message, etc.) to the named dict column.
func (a *intrinsicAccumulator) feedString(name string, colType shared.ColumnType, val string, blockIdx uint16, rowIdx int) {
	if val == "" {
		return
	}
	c, ok := a.dictCols[name]
	if !ok {
		c = &dictAccum{index: make(map[string]int), colType: colType}
		a.dictCols[name] = c
	}
	idx, exists := c.index[val]
	if !exists {
		idx = len(c.entries)
		c.index[val] = idx
		c.entries = append(c.entries, dictEntry{strVal: val})
	}
	c.entries[idx].refs = append(c.entries[idx].refs, shared.BlockRef{
		BlockIdx: blockIdx,
		RowIdx:   uint16(rowIdx), //nolint:gosec
	})
}

// feedInt64 adds one int64 value (span:kind, span:status, log:severity_number) to the named dict column.
func (a *intrinsicAccumulator) feedInt64(name string, colType shared.ColumnType, val int64, blockIdx uint16, rowIdx int) {
	c, ok := a.dictCols[name]
	if !ok {
		c = &dictAccum{index: make(map[string]int), colType: colType}
		a.dictCols[name] = c
	}
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(val)) //nolint:gosec
	key := string(tmp[:])
	idx, exists := c.index[key]
	if !exists {
		idx = len(c.entries)
		c.index[key] = idx
		c.entries = append(c.entries, dictEntry{int64Val: val})
	}
	c.entries[idx].refs = append(c.entries[idx].refs, shared.BlockRef{
		BlockIdx: blockIdx,
		RowIdx:   uint16(rowIdx), //nolint:gosec
	})
}

// columnNames returns all accumulated column names (flat + dict), sorted.
func (a *intrinsicAccumulator) columnNames() []string {
	names := make([]string, 0, len(a.flatCols)+len(a.dictCols))
	for n := range a.flatCols {
		names = append(names, n)
	}
	for n := range a.dictCols {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// encodeColumn serializes one column's accumulated data into a snappy-compressed blob.
// The blob starts with IntrinsicFormatVersion[1] + format[1] (flat or dict) + colType[1].
func (a *intrinsicAccumulator) encodeColumn(name string) ([]byte, error) {
	if c, ok := a.flatCols[name]; ok {
		return encodeFlatColumn(c)
	}
	if c, ok := a.dictCols[name]; ok {
		return encodeDictColumn(c)
	}
	return nil, nil
}

// refWidths computes the minimum byte widths needed for blockIdx and rowIdx fields.
// Returns 1 if the maximum value fits in a single byte (≤255), 2 otherwise.
func refWidths(refs []shared.BlockRef) (blockW, rowW uint8) {
	var maxBlock, maxRow uint16
	for _, r := range refs {
		if r.BlockIdx > maxBlock {
			maxBlock = r.BlockIdx
		}
		if r.RowIdx > maxRow {
			maxRow = r.RowIdx
		}
	}
	blockW = 1
	if maxBlock > 255 {
		blockW = 2
	}
	rowW = 1
	if maxRow > 255 {
		rowW = 2
	}
	return
}

// dictRefWidths computes the minimum byte widths for blockIdx and rowIdx fields
// across all refs in all entries of a dict column.
func dictRefWidths(entries []dictEntry) (blockW, rowW uint8) {
	var maxBlock, maxRow uint16
	for _, e := range entries {
		for _, r := range e.refs {
			if r.BlockIdx > maxBlock {
				maxBlock = r.BlockIdx
			}
			if r.RowIdx > maxRow {
				maxRow = r.RowIdx
			}
		}
	}
	blockW = 1
	if maxBlock > 255 {
		blockW = 2
	}
	rowW = 1
	if maxRow > 255 {
		rowW = 2
	}
	return
}

// writeRef writes a single BlockRef using the specified byte widths.
func writeRef(buf *bytes.Buffer, ref shared.BlockRef, blockW, rowW uint8) {
	var tmp2 [2]byte
	if blockW == 1 {
		buf.WriteByte(byte(ref.BlockIdx))
	} else {
		binary.LittleEndian.PutUint16(tmp2[:], ref.BlockIdx)
		buf.Write(tmp2[:])
	}
	if rowW == 1 {
		buf.WriteByte(byte(ref.RowIdx))
	} else {
		binary.LittleEndian.PutUint16(tmp2[:], ref.RowIdx)
		buf.Write(tmp2[:])
	}
}

// encodeFlatColumn encodes a flat accumulator to wire format and snappy-compresses it.
//
// Wire format (uncompressed):
//
//	format_version[1]  = IntrinsicFormatVersion (0x01)
//	format[1]          = IntrinsicFormatFlat (0x01)
//	col_type[1]        = ColumnType byte
//	row_count[4 LE]    = total rows
//	block_idx_width[1] = bytes per blockIdx in refs (1 or 2)
//	row_idx_width[1]   = bytes per rowIdx in refs (1 or 2)
//	values[row_count × width] = sorted values:
//	  uint64: delta-encoded (first value absolute, subsequent values are v[i]-v[i-1])
//	  bytes:  length-prefixed (2-byte LE len + raw bytes)
//	refs[row_count × (block_idx_width+row_idx_width)] = (blockIdx, rowIdx) parallel to values
func encodeFlatColumn(c *flatAccum) ([]byte, error) {
	// Sort by value.
	n := len(c.refs)
	if len(c.uint64Values) > 0 {
		// Sort uint64 ascending.
		type row struct {
			val uint64
			ref shared.BlockRef
		}
		rows := make([]row, n)
		for i := range n {
			rows[i] = row{c.uint64Values[i], c.refs[i]}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].val < rows[j].val })
		for i := range n {
			c.uint64Values[i] = rows[i].val
			c.refs[i] = rows[i].ref
		}
	} else if len(c.bytesValues) > 0 {
		// Sort bytes lexicographic.
		type row struct {
			val []byte
			ref shared.BlockRef
		}
		rows := make([]row, n)
		for i := range n {
			rows[i] = row{c.bytesValues[i], c.refs[i]}
		}
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].val, rows[j].val) < 0 })
		for i := range n {
			c.bytesValues[i] = rows[i].val
			c.refs[i] = rows[i].ref
		}
	}

	blockW, rowW := refWidths(c.refs)

	var buf bytes.Buffer
	buf.WriteByte(shared.IntrinsicFormatVersion)
	buf.WriteByte(shared.IntrinsicFormatFlat)
	buf.WriteByte(byte(c.colType))

	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(n)) //nolint:gosec
	buf.Write(tmp4[:])

	buf.WriteByte(blockW)
	buf.WriteByte(rowW)

	var tmp8 [8]byte
	var tmp2 [2]byte
	if len(c.uint64Values) > 0 {
		// Delta encoding: first value is absolute, subsequent values are deltas.
		// Values are sorted ascending so all deltas are non-negative.
		var prev uint64
		for _, v := range c.uint64Values {
			binary.LittleEndian.PutUint64(tmp8[:], v-prev)
			buf.Write(tmp8[:])
			prev = v
		}
	} else {
		for _, v := range c.bytesValues {
			binary.LittleEndian.PutUint16(tmp2[:], uint16(len(v))) //nolint:gosec
			buf.Write(tmp2[:])
			buf.Write(v)
		}
	}
	// Block refs parallel to values, using variable-width encoding.
	for _, ref := range c.refs {
		writeRef(&buf, ref, blockW, rowW)
	}

	return snappy.Encode(nil, buf.Bytes()), nil
}

// encodeDictColumn encodes a dict accumulator to wire format and snappy-compresses it.
//
// Wire format (uncompressed):
//
//	format_version[1]  = IntrinsicFormatVersion (0x01)
//	format[1]          = IntrinsicFormatDict (0x02)
//	col_type[1]        = ColumnType byte
//	value_count[4 LE]  = number of unique values
//	block_idx_width[1] = bytes per blockIdx in all refs (1 or 2)
//	row_idx_width[1]   = bytes per rowIdx in all refs (1 or 2)
//	per value (sorted by value):
//	  value_len[2 LE] = byte length of value string (0 for int64: 8 bytes follow)
//	  value[value_len OR 8 bytes LE int64]
//	  ref_count[4 LE] = number of block refs for this value
//	  refs[ref_count × (block_idx_width+row_idx_width)] = (blockIdx, rowIdx)
func encodeDictColumn(c *dictAccum) ([]byte, error) {
	// Sort entries by value.
	isInt64 := c.colType == shared.ColumnTypeInt64 || c.colType == shared.ColumnTypeRangeInt64
	sort.Slice(c.entries, func(i, j int) bool {
		if isInt64 {
			return c.entries[i].int64Val < c.entries[j].int64Val
		}
		return c.entries[i].strVal < c.entries[j].strVal
	})

	blockW, rowW := dictRefWidths(c.entries)

	var buf bytes.Buffer
	buf.WriteByte(shared.IntrinsicFormatVersion)
	buf.WriteByte(shared.IntrinsicFormatDict)
	buf.WriteByte(byte(c.colType))

	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(c.entries))) //nolint:gosec
	buf.Write(tmp4[:])

	buf.WriteByte(blockW)
	buf.WriteByte(rowW)

	var tmp8 [8]byte
	var tmp2 [2]byte
	for _, e := range c.entries {
		if isInt64 {
			binary.LittleEndian.PutUint16(tmp2[:], 0) // sentinel: 0 len means int64 follows
			buf.Write(tmp2[:])
			binary.LittleEndian.PutUint64(tmp8[:], uint64(e.int64Val)) //nolint:gosec
			buf.Write(tmp8[:])
		} else {
			binary.LittleEndian.PutUint16(tmp2[:], uint16(len(e.strVal))) //nolint:gosec
			buf.Write(tmp2[:])
			buf.WriteString(e.strVal)
		}
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(e.refs))) //nolint:gosec
		buf.Write(tmp4[:])
		for _, ref := range e.refs {
			writeRef(&buf, ref, blockW, rowW)
		}
	}

	return snappy.Encode(nil, buf.Bytes()), nil
}

// computeMinMax extracts the min/max encoded values from a flat or dict column for the TOC.
// Must be called after encodeColumn (which sorts the entries).
func (a *intrinsicAccumulator) computeMinMax(name string) (minVal, maxVal string) {
	if c, ok := a.flatCols[name]; ok {
		if len(c.uint64Values) > 0 {
			var mn, mx [8]byte
			binary.LittleEndian.PutUint64(mn[:], c.uint64Values[0])
			binary.LittleEndian.PutUint64(mx[:], c.uint64Values[len(c.uint64Values)-1])
			return string(mn[:]), string(mx[:])
		}
		if len(c.bytesValues) > 0 {
			return string(c.bytesValues[0]), string(c.bytesValues[len(c.bytesValues)-1])
		}
	}
	if c, ok := a.dictCols[name]; ok {
		if len(c.entries) > 0 {
			first := c.entries[0]
			last := c.entries[len(c.entries)-1]
			isInt64 := c.colType == shared.ColumnTypeInt64 || c.colType == shared.ColumnTypeRangeInt64
			if isInt64 {
				var mn, mx [8]byte
				binary.LittleEndian.PutUint64(mn[:], uint64(first.int64Val)) //nolint:gosec
				binary.LittleEndian.PutUint64(mx[:], uint64(last.int64Val))  //nolint:gosec
				return string(mn[:]), string(mx[:])
			}
			return first.strVal, last.strVal
		}
	}
	return "", ""
}

// rowCount returns the total number of rows for the named column.
func (a *intrinsicAccumulator) rowCount(name string) uint32 {
	if c, ok := a.flatCols[name]; ok {
		return uint32(len(c.refs)) //nolint:gosec
	}
	if c, ok := a.dictCols[name]; ok {
		total := 0
		for _, e := range c.entries {
			total += len(e.refs)
		}
		return uint32(total) //nolint:gosec
	}
	return 0
}

// colTypeFor returns the ColumnType and format byte for the named column.
func (a *intrinsicAccumulator) colTypeFor(name string) (shared.ColumnType, uint8) {
	if c, ok := a.flatCols[name]; ok {
		return c.colType, shared.IntrinsicFormatFlat
	}
	if c, ok := a.dictCols[name]; ok {
		return c.colType, shared.IntrinsicFormatDict
	}
	return 0, 0
}

// encodeTOC serializes a slice of IntrinsicColMeta entries to a snappy-compressed blob.
//
// Wire format (uncompressed):
//
//	toc_version[1]  = 0x01
//	col_count[4 LE] = number of entries
//	per entry:
//	  name_len[2 LE] + name
//	  col_type[1]
//	  format[1]      = IntrinsicFormatFlat or IntrinsicFormatDict
//	  offset[8 LE]
//	  length[4 LE]
//	  count[4 LE]
//	  min_len[2 LE] + min[min_len]
//	  max_len[2 LE] + max[max_len]
func encodeTOC(entries []shared.IntrinsicColMeta) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(0x01) // toc_version

	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(entries))) //nolint:gosec
	buf.Write(tmp4[:])

	var tmp8 [8]byte
	var tmp2 [2]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(e.Name))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.WriteString(e.Name)
		buf.WriteByte(byte(e.Type))
		buf.WriteByte(e.Format)
		binary.LittleEndian.PutUint64(tmp8[:], e.Offset)
		buf.Write(tmp8[:])
		binary.LittleEndian.PutUint32(tmp4[:], e.Length)
		buf.Write(tmp4[:])
		binary.LittleEndian.PutUint32(tmp4[:], e.Count)
		buf.Write(tmp4[:])
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(e.Min))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.WriteString(e.Min)
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(e.Max))) //nolint:gosec
		buf.Write(tmp2[:])
		buf.WriteString(e.Max)
	}
	return snappy.Encode(nil, buf.Bytes()), nil
}

// decodeTOC decompresses a TOC blob and parses it into a slice of IntrinsicColMeta.
// Delegates to shared.DecodeTOC; kept here so writer-internal tests can call it.
func decodeTOC(blob []byte) ([]shared.IntrinsicColMeta, error) {
	return shared.DecodeTOC(blob)
}

// decodeIntrinsicColumnBlob decompresses and decodes a column data blob.
// Delegates to shared.DecodeIntrinsicColumnBlob; kept here so writer-internal tests can call it.
func decodeIntrinsicColumnBlob(blob []byte) (*shared.IntrinsicColumn, error) {
	return shared.DecodeIntrinsicColumnBlob(blob)
}
