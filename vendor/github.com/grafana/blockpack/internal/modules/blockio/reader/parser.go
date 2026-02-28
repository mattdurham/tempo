package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// traceBlockRef describes span indices for one trace in one block.
type traceBlockRef struct {
	spanIndices []uint16
	blockID     uint16
}

// rangeIndexMeta records the byte range within metadataBytes for a
// range column index entry (lazy parsing).
type rangeIndexMeta struct {
	typ    shared.ColumnType
	offset int
	length int
}

// readFooter reads the last 22 bytes (v3 footer).
func (r *Reader) readFooter() error {
	if r.fileSize < int64(shared.FooterV3Size) {
		return fmt.Errorf("file too small for footer: %d bytes", r.fileSize)
	}

	buf := make([]byte, shared.FooterV3Size)
	off := r.fileSize - int64(shared.FooterV3Size)
	n, err := r.provider.ReadAt(buf, off, shared.DataTypeFooter)
	if err != nil {
		return fmt.Errorf("readFooter: %w", err)
	}

	if n != int(shared.FooterV3Size) {
		return fmt.Errorf("readFooter: short read: %d bytes", n)
	}

	ver := binary.LittleEndian.Uint16(buf[0:])
	if ver != shared.FooterV3Version {
		return fmt.Errorf("readFooter: unsupported footer version %d", ver)
	}

	r.footerVersion = ver
	r.footerFields.headerOffset = binary.LittleEndian.Uint64(buf[2:])
	r.footerFields.compactOffset = binary.LittleEndian.Uint64(buf[10:])
	r.footerFields.compactLen = binary.LittleEndian.Uint32(buf[18:])

	if r.footerFields.headerOffset == 0 {
		return fmt.Errorf("readFooter: header_offset is zero")
	}

	r.headerOffset = r.footerFields.headerOffset
	r.compactOffset = r.footerFields.compactOffset
	r.compactLen = r.footerFields.compactLen
	return nil
}

// readHeader reads the 21-byte file header at footer.headerOffset.
func (r *Reader) readHeader() error {
	const headerSize = 21
	buf := make([]byte, headerSize)
	n, err := r.provider.ReadAt(buf, int64(r.headerOffset), shared.DataTypeHeader) //nolint:gosec // safe: headerOffset is a file offset, fits in int64
	if err != nil {
		return fmt.Errorf("readHeader: %w", err)
	}

	if n != headerSize {
		return fmt.Errorf("readHeader: short read: %d bytes", n)
	}

	magic := binary.LittleEndian.Uint32(buf[0:])
	if magic != shared.MagicNumber {
		return fmt.Errorf("readHeader: bad magic 0x%08X", magic)
	}

	version := buf[4] //nolint:gosec // safe: buf is headerSize (21) bytes, validated by short-read check above
	if version != shared.VersionV10 && version != shared.VersionV11 {
		return fmt.Errorf("readHeader: unsupported version %d", version)
	}

	r.fileVersion = version
	r.metadataOffset = binary.LittleEndian.Uint64(buf[5:])
	r.metadataLen = binary.LittleEndian.Uint64(buf[13:])
	return nil
}

// parseV5MetadataLazy reads the metadata section and eagerly parses:
//   - block index entries → r.blockMetas
//   - range column index byte ranges → r.rangeOffsets (lazy)
//   - trace block index → r.traceIndex
func (r *Reader) parseV5MetadataLazy() error {
	if r.metadataLen == 0 {
		return fmt.Errorf("parseMetadata: metadata_len is zero")
	}

	if r.metadataLen > shared.MaxMetadataSize {
		return fmt.Errorf("parseMetadata: metadata too large: %d bytes", r.metadataLen)
	}

	data, err := r.readRange(r.metadataOffset, r.metadataLen, shared.DataTypeMetadata)
	if err != nil {
		return fmt.Errorf("parseMetadata: %w", err)
	}

	r.metadataBytes = data
	pos := 0

	// Block index: block_count[4] + entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: block_index: short for block_count")
	}

	blockCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	metas, newPos, err := parseBlockIndex(data[pos:pos+len(data)-pos], r.fileVersion, blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: block_index: %w", err)
	}

	actualConsumed := newPos
	pos += actualConsumed
	r.blockMetas = metas

	// Range index: range_count[4] + range_count entries.
	if pos+4 > len(data) {
		return fmt.Errorf("parseMetadata: range_index: short for range_count")
	}

	dedCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	rangeStart := pos
	offsets, newPos, err := scanRangeIndexOffsets(data[pos:], dedCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: range_index: %w", err)
	}

	// Adjust offsets to be absolute within data.
	for k, v := range offsets {
		v.offset += rangeStart
		offsets[k] = v
	}

	pos += newPos
	r.rangeOffsets = offsets

	// Column index: block_count × col_count (always 0 in new files; skip without allocating).
	newPos, err = skipColumnIndex(data[pos:], blockCount)
	if err != nil {
		return fmt.Errorf("parseMetadata: column_index: %w", err)
	}

	pos += newPos

	// Trace block index.
	traceIdx, _, err := parseTraceBlockIndex(data[pos:])
	if err != nil {
		return fmt.Errorf("parseMetadata: trace_index: %w", err)
	}

	r.traceIndex = traceIdx
	return nil
}

// parseBlockIndexEntry parses one block index entry for the given version.
// Returns the entry and new position.
func parseBlockIndexEntry(
	data []byte,
	pos int,
	version uint8,
) (shared.BlockMeta, int, error) {
	var meta shared.BlockMeta

	// offset[8] + length[8]
	if pos+16 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for offset/length")
	}

	meta.Offset = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.Length = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// v11 adds kind[1] before span_count.
	if version == shared.VersionV11 {
		if pos+1 > len(data) {
			return meta, pos, fmt.Errorf("block_index entry: short for kind")
		}

		meta.Kind = shared.BlockKind(data[pos])
		pos++
	}

	// span_count[4] + min_start[8] + max_start[8]
	if pos+20 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for span_count/timestamps")
	}

	meta.SpanCount = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	meta.MinStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8
	meta.MaxStart = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	// min_trace_id[16] + max_trace_id[16]
	if pos+32 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for trace IDs")
	}

	copy(meta.MinTraceID[:], data[pos:pos+16])
	pos += 16
	copy(meta.MaxTraceID[:], data[pos:pos+16])
	pos += 16

	// column_name_bloom[32]
	if pos+32 > len(data) {
		return meta, pos, fmt.Errorf("block_index entry: short for bloom")
	}

	copy(meta.ColumnNameBloom[:], data[pos:pos+32])
	pos += 32

	return meta, pos, nil
}

// parseBlockIndex parses block_count block index entries from data.
func parseBlockIndex(data []byte, version uint8, blockCount int) ([]shared.BlockMeta, int, error) {
	metas := make([]shared.BlockMeta, 0, blockCount)
	pos := 0

	for i := range blockCount {
		meta, newPos, err := parseBlockIndexEntry(data, pos, version)
		if err != nil {
			return nil, pos, fmt.Errorf("block[%d]: %w", i, err)
		}

		metas = append(metas, meta)
		pos = newPos
	}

	return metas, pos, nil
}

// skipColumnIndex advances pos past the column index section without allocating.
// For new files, each block has col_count=0 (4 bytes per block).
// For old files, it correctly skips any existing entries.
func skipColumnIndex(data []byte, blockCount int) (int, error) {
	pos := 0

	for b := range blockCount {
		if pos+4 > len(data) {
			return pos, fmt.Errorf("column_index block[%d]: short for col_count", b)
		}

		colCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		for c := range colCount {
			if pos+2 > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name_len", b, c)
			}

			nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
			advance := 2 + nameLen + 8 // name_len[2] + name + offset[4] + length[4]
			if advance < 0 || pos+advance > len(data) {
				return pos, fmt.Errorf("column_index block[%d] col[%d]: short for name/offset/length", b, c)
			}

			pos += advance
		}
	}

	return pos, nil
}

// parseTraceBlockIndex parses the trace block index section.
// Returns the parsed map and bytes consumed.
func parseTraceBlockIndex(data []byte) (map[[16]byte][]traceBlockRef, int, error) {
	if len(data) < 5 {
		return nil, 0, nil
	}

	fmtVersion := data[0]
	if fmtVersion != shared.TraceIndexFmtVersion {
		return nil, 0, fmt.Errorf("trace_index: unsupported fmt_version %d", fmtVersion)
	}

	traceCount := int(binary.LittleEndian.Uint32(data[1:]))
	pos := 5

	result := make(map[[16]byte][]traceBlockRef, traceCount)

	for t := range traceCount {
		if pos+18 > len(data) {
			return nil, pos, fmt.Errorf("trace_index: trace[%d]: short for trace_id+block_count", t)
		}

		var tid [16]byte
		copy(tid[:], data[pos:pos+16])
		pos += 16

		blockCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		refs := make([]traceBlockRef, 0, blockCount)
		for b := range blockCount {
			if pos+4 > len(data) {
				return nil, pos, fmt.Errorf("trace_index: trace[%d] block[%d]: short for block_id+span_count", t, b)
			}

			blockID := binary.LittleEndian.Uint16(data[pos:])
			pos += 2
			spanCount := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2

			if pos+spanCount*2 > len(data) {
				return nil, pos, fmt.Errorf(
					"trace_index: trace[%d] block[%d]: short for span_indices (%d × 2 bytes)",
					t, b, spanCount,
				)
			}

			indices := make([]uint16, spanCount)
			for s := range spanCount {
				indices[s] = binary.LittleEndian.Uint16(data[pos+s*2:])
			}

			pos += spanCount * 2
			refs = append(refs, traceBlockRef{
				blockID:     blockID,
				spanIndices: indices,
			})
		}

		result[tid] = refs
	}

	return result, pos, nil
}

// scanRangeIndexOffsets scans the range index data recording byte offset+length
// for each column entry WITHOUT parsing values.
// data is the slice starting immediately after range_count.
// Returns the offset map and bytes consumed.
func scanRangeIndexOffsets(data []byte, dedCount int) (map[string]rangeIndexMeta, int, error) {
	offsets := make(map[string]rangeIndexMeta, dedCount)
	pos := 0

	for i := range dedCount {
		entryStart := pos

		// name_len[2] + name
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("range[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// column_type[1]
		if pos+1 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for type", i, name)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		// Bucket metadata always present: bucket_min[8] + bucket_max[8] + boundary_count[4]
		if pos+20 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for bucket header", i, name)
		}

		pos += 16 // min + max
		boundaryCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// boundaries[boundary_count × 8]
		if pos+boundaryCount*8 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for boundaries", i, name)
		}

		pos += boundaryCount * 8

		// typed_count[4] + typed boundaries
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for typed_count", i, name)
		}

		typedCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		typedBytes, newPos, err := skipTypedBoundaries(data, pos, colType, typedCount)
		if err != nil {
			return nil, pos, fmt.Errorf("range[%d] %q: typed boundaries: %w", i, name, err)
		}

		_ = typedBytes
		pos = newPos

		// value_count[4]
		if pos+4 > len(data) {
			return nil, pos, fmt.Errorf("range[%d] %q: short for value_count", i, name)
		}

		valueCount := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		// Skip value entries to find end.
		for v := range valueCount {
			newPos, err := skipRangeValueEntry(data, pos, colType)
			if err != nil {
				return nil, pos, fmt.Errorf("range[%d] %q value[%d]: %w", i, name, v, err)
			}

			pos = newPos
		}

		offsets[name] = rangeIndexMeta{
			typ:    colType,
			offset: entryStart,
			length: pos - entryStart,
		}
	}

	return offsets, pos, nil
}

// skipTypedBoundaries skips typed boundary data based on column type.
func skipTypedBoundaries(data []byte, pos int, colType shared.ColumnType, count int) (int, int, error) {
	switch colType {
	case shared.ColumnTypeRangeFloat64:
		// count × float64_bits(8)
		need := count * 8
		if pos+need > len(data) {
			return 0, pos, fmt.Errorf("typed boundaries(float64): need %d bytes at pos %d", need, pos)
		}

		return 0, pos + need, nil

	case shared.ColumnTypeRangeString:
		for i := range count {
			if pos+4 > len(data) {
				return 0, pos, fmt.Errorf("typed boundaries(string)[%d]: short for len", i)
			}

			sLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+sLen > len(data) {
				return 0, pos, fmt.Errorf("typed boundaries(string)[%d]: short for data", i)
			}

			pos += sLen
		}

		return 0, pos, nil

	case shared.ColumnTypeRangeBytes:
		for i := range count {
			if pos+4 > len(data) {
				return 0, pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for len", i)
			}

			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return 0, pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for data", i)
			}

			pos += bLen
		}

		return 0, pos, nil

	default:
		// RangeInt64/RangeUint64/RangeDuration: typed_count must be 0.
		if count != 0 {
			return 0, pos, fmt.Errorf("typed boundaries: non-zero count %d for type %d", count, colType)
		}

		return 0, pos, nil
	}
}

// skipRangeValueEntry skips one RangeValueEntry for the given column type.
func skipRangeValueEntry(data []byte, pos int, colType shared.ColumnType) (int, error) {
	// value_key depends on type.
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(string): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(string): short for data")
		}

		pos += kLen

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if pos+4 > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(bytes): short for data")
		}

		pos += kLen

	case shared.ColumnTypeInt64, shared.ColumnTypeUint64, shared.ColumnTypeFloat64:
		if pos+8 > len(data) {
			return pos, fmt.Errorf("value_key(numeric): short")
		}

		pos += 8

	case shared.ColumnTypeBool:
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(bool): short")
		}

		pos++

	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeUint64, shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeFloat64:
		// length_prefix(1 uint8) + key_data where length_prefix is 2 (bucket ID) or 8 (raw value)
		if pos+1 > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for length_prefix")
		}

		kLen := int(data[pos])
		pos++
		if pos+kLen > len(data) {
			return pos, fmt.Errorf("value_key(range numeric): short for key_data (len=%d)", kLen)
		}

		pos += kLen

	default:
		return pos, fmt.Errorf("skipRangeValueEntry: unknown column type %d", colType)
	}

	// block_id_count[4] + block_ids[N×4]
	if pos+4 > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_id_count")
	}

	bidCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	need := bidCount * 4
	if pos+need > len(data) {
		return pos, fmt.Errorf("value_entry: short for block_ids (count=%d)", bidCount)
	}

	pos += need
	return pos, nil
}

// readRange reads exactly length bytes at absolute byte offset.
func (r *Reader) readRange(offset, length uint64, dt shared.DataType) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	buf := make([]byte, length)
	n, err := r.provider.ReadAt(buf, int64(offset), dt) //nolint:gosec // safe: offset is a file offset, fits in int64
	if err != nil {
		return nil, fmt.Errorf("readRange offset=%d length=%d: %w", offset, length, err)
	}

	if uint64(n) != length { //nolint:gosec // safe: n is bytes read, always non-negative
		return nil, fmt.Errorf("readRange offset=%d: short read %d/%d", offset, n, length)
	}

	return buf, nil
}
