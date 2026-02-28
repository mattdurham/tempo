package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

type traceBlockRef struct {
	spanIndices []uint16
	blockID     uint16
}

// writeBlockIndexSection serializes the block index.
// Returns the serialized bytes (without the length prefix — caller adds it).
func writeBlockIndexSection(_ io.Writer, version uint8, metas []shared.BlockMeta) ([]byte, error) {
	var buf bytes.Buffer

	// block_count[4 LE]
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(metas))) //nolint:gosec // safe: block count bounded by MaxBlocks (100_000)
	buf.Write(tmp[:])

	for _, m := range metas {
		// offset[8 LE]
		var off [8]byte
		binary.LittleEndian.PutUint64(off[:], m.Offset)
		buf.Write(off[:])

		// length[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.Length)
		buf.Write(off[:])

		if version >= shared.VersionV11 {
			// kind[1]
			buf.WriteByte(byte(m.Kind))
		}

		// span_count[4 LE]
		binary.LittleEndian.PutUint32(tmp[:], m.SpanCount)
		buf.Write(tmp[:])

		// min_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MinStart)
		buf.Write(off[:])

		// max_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MaxStart)
		buf.Write(off[:])

		// min_trace_id[16]
		buf.Write(m.MinTraceID[:])

		// max_trace_id[16]
		buf.Write(m.MaxTraceID[:])

		// column_name_bloom[32]
		buf.Write(m.ColumnNameBloom[:])
	}

	return buf.Bytes(), nil
}

// writeRangeIndexSection serializes the range column index.
// Returns the serialized bytes.
func writeRangeIndexSection(_ io.Writer, rIdx rangeIndex) ([]byte, error) {
	var buf bytes.Buffer

	// Sort column names for deterministic output.
	// Skip columns with no bucket entries (cd.values == nil): they carry no pruning
	// information and must not be written — the reader would treat them as indexed
	// with 0 entries, causing the query planner to incorrectly prune all blocks.
	colNames := make([]string, 0, len(rIdx))
	for name, cd := range rIdx {
		if len(cd.values) > 0 {
			colNames = append(colNames, name)
		}
	}
	sort.Strings(colNames)

	// col_count[4 LE]
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(colNames))) //nolint:gosec // safe: column count bounded by MaxColumns
	buf.Write(tmp[:])

	for _, colName := range colNames {
		cd := rIdx[colName]

		// col_name_len[2 LE] + col_name
		buf.Write([]byte{byte(len(colName)), byte(len(colName) >> 8)}) //nolint:gosec // safe: col name len bounded by MaxNameLen (1024)
		buf.WriteString(colName)

		// col_type[1]
		buf.WriteByte(byte(cd.colType))

		writeRangeBucketMeta(&buf, cd)

		// value_count[4 LE]
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(cd.values))) //nolint:gosec // safe: value count bounded by MaxDictionarySize
		buf.Write(tmp[:])

		// Sort value keys for deterministic output.
		keys := make([]string, 0, len(cd.values))
		for k := range cd.values {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			blockIDs := cd.values[key]

			writeRangeValueKey(&buf, cd.colType, key)

			// block_id_count[4 LE]
			binary.LittleEndian.PutUint32(tmp[:], uint32(len(blockIDs))) //nolint:gosec // safe: block ID count bounded by MaxBlocks
			buf.Write(tmp[:])

			// block_ids[N × 4 LE]
			for _, bid := range blockIDs {
				binary.LittleEndian.PutUint32(tmp[:], bid)
				buf.Write(tmp[:])
			}
		}
	}

	return buf.Bytes(), nil
}

// writeRangeBucketMeta writes bucket metadata for a range-bucketed column.
func writeRangeBucketMeta(buf *bytes.Buffer, cd *rangeColumnData) {
	var tmp8 [8]byte
	var tmp4 [4]byte

	// bucket_min[8 LE]
	binary.LittleEndian.PutUint64(tmp8[:], uint64(cd.bucketMin)) //nolint:gosec // safe: storing int64 bits as uint64 for wire format
	buf.Write(tmp8[:])
	// bucket_max[8 LE]
	binary.LittleEndian.PutUint64(tmp8[:], uint64(cd.bucketMax)) //nolint:gosec // safe: storing int64 bits as uint64 for wire format
	buf.Write(tmp8[:])

	// boundary_count[4 LE] + boundaries[N × 8 LE int64]
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(cd.boundaries))) //nolint:gosec // safe: boundary count bounded by defaultRangeBuckets+1
	buf.Write(tmp4[:])
	for _, b := range cd.boundaries {
		binary.LittleEndian.PutUint64(tmp8[:], uint64(b)) //nolint:gosec // safe: storing int64 bits as uint64 for wire format
		buf.Write(tmp8[:])
	}

	// typed_count[4 LE] + typed boundaries
	switch cd.colType {
	case shared.ColumnTypeRangeFloat64:
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(cd.float64Bounds))) //nolint:gosec // safe: float64 boundary count bounded by defaultRangeBuckets+1
		buf.Write(tmp4[:])
		for _, f := range cd.float64Bounds {
			binary.LittleEndian.PutUint64(tmp8[:], math.Float64bits(f))
			buf.Write(tmp8[:])
		}
	case shared.ColumnTypeRangeString:
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(cd.stringBounds))) //nolint:gosec // safe: string boundary count bounded by defaultRangeBuckets+1
		buf.Write(tmp4[:])
		for _, s := range cd.stringBounds {
			binary.LittleEndian.PutUint32(tmp4[:], uint32(len(s))) //nolint:gosec // safe: string boundary length bounded by MaxStringLen
			buf.Write(tmp4[:])
			buf.WriteString(s)
		}
	case shared.ColumnTypeRangeBytes:
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(cd.bytesBounds))) //nolint:gosec // safe: bytes boundary count bounded by defaultRangeBuckets+1
		buf.Write(tmp4[:])
		for _, b := range cd.bytesBounds {
			binary.LittleEndian.PutUint32(tmp4[:], uint32(len(b))) //nolint:gosec // safe: bytes boundary length bounded by MaxBytesLen
			buf.Write(tmp4[:])
			buf.Write(b)
		}
	default:
		// RangeInt64/RangeUint64/RangeDuration: typed_count = 0
		binary.LittleEndian.PutUint32(tmp4[:], 0)
		buf.Write(tmp4[:])
	}
}

// writeRangeValueKey writes the value key in the correct wire format for the given column type.
func writeRangeValueKey(buf *bytes.Buffer, colType shared.ColumnType, key string) {
	var tmp4 [4]byte
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeBytes,
		shared.ColumnTypeRangeString, shared.ColumnTypeRangeBytes:
		// len(4 LE) + key_bytes
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(key))) //nolint:gosec // safe: key length bounded by MaxStringLen
		buf.Write(tmp4[:])
		buf.WriteString(key)
	case shared.ColumnTypeInt64, shared.ColumnTypeUint64, shared.ColumnTypeFloat64:
		// 8 raw bytes LE
		buf.WriteString(key)
	case shared.ColumnTypeBool:
		// 1 byte
		buf.WriteString(key)
	default:
		// RangeInt64/RangeUint64/RangeDuration/RangeFloat64: length_prefix(1 uint8) + key_bytes
		// length_prefix = 8 (boundary value is always 8 bytes for numeric types); see SPECS §5.2.1.
		buf.WriteByte(byte(len(key))) //nolint:gosec // safe: range boundary key is 8 bytes, fits uint8
		buf.WriteString(key)
	}
}

// writeTraceBlockIndexSection serializes the trace block index.
// Returns the serialized bytes.
func writeTraceBlockIndexSection(_ io.Writer, traceIndex map[[16]byte][]traceBlockRef) ([]byte, error) {
	var buf bytes.Buffer

	// Sort trace IDs for deterministic output.
	traceIDs := make([][16]byte, 0, len(traceIndex))
	for tid := range traceIndex {
		traceIDs = append(traceIDs, tid)
	}
	sort.Slice(traceIDs, func(i, j int) bool {
		return bytes.Compare(traceIDs[i][:], traceIDs[j][:]) < 0
	})

	// fmt_version[1] = 0x01
	buf.WriteByte(shared.TraceIndexFmtVersion)

	// trace_count[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(traceIDs))) //nolint:gosec // safe: trace count bounded by MaxTraceCount
	buf.Write(tmp4[:])

	var tmp2 [2]byte

	for _, tid := range traceIDs {
		refs := traceIndex[tid]

		// trace_id[16]
		buf.Write(tid[:])

		// block_ref_count[2 LE]
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(refs))) //nolint:gosec // safe: block ref count bounded by MaxBlocks (100_000) fits uint16
		buf.Write(tmp2[:])

		for _, ref := range refs {
			// block_id[2 LE]
			binary.LittleEndian.PutUint16(tmp2[:], ref.blockID)
			buf.Write(tmp2[:])
			// span_count[2 LE]
			binary.LittleEndian.PutUint16(tmp2[:], uint16(len(ref.spanIndices))) //nolint:gosec // safe: span count <= MaxBlockSpans (65535)
			buf.Write(tmp2[:])
			// span_indices[span_count × uint16 LE]
			for _, idx := range ref.spanIndices {
				binary.LittleEndian.PutUint16(tmp2[:], idx)
				buf.Write(tmp2[:])
			}
		}
	}

	return buf.Bytes(), nil
}

// writeFileHeader writes the 21-byte file header.
func writeFileHeader(w io.Writer, version uint8, metadataOffset, metadataLen uint64) error {
	var buf [21]byte
	binary.LittleEndian.PutUint32(buf[0:], shared.MagicNumber)
	buf[4] = version
	binary.LittleEndian.PutUint64(buf[5:], metadataOffset)
	binary.LittleEndian.PutUint64(buf[13:], metadataLen)
	_, err := w.Write(buf[:])
	return err
}

// writeCompactTraceIndex writes the compact trace index section.
// Returns bytes written.
func writeCompactTraceIndex(
	w io.Writer,
	blockMetas []shared.BlockMeta,
	traceIndex map[[16]byte][]traceBlockRef,
) (int64, error) {
	var buf bytes.Buffer

	// magic[4 LE] = 0xC01DC1DE
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], shared.CompactIndexMagic)
	buf.Write(tmp4[:])

	// version[1] = 1
	buf.WriteByte(shared.CompactIndexVersion)

	// block_count[4 LE]
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(blockMetas))) //nolint:gosec // safe: block count bounded by MaxBlocks
	buf.Write(tmp4[:])

	// block_table: block_count × { file_offset[8 LE] + file_length[4 LE] }
	for _, m := range blockMetas {
		var tmp8 [8]byte
		binary.LittleEndian.PutUint64(tmp8[:], m.Offset)
		buf.Write(tmp8[:])
		binary.LittleEndian.PutUint32(tmp4[:], uint32(m.Length)) //nolint:gosec // safe: block length bounded by MaxBlockSize (1GB) fits uint32
		buf.Write(tmp4[:])
	}

	// Trace index: fmt_version[1] + trace_count[4] + traces
	traceData, err := writeTraceBlockIndexSection(nil, traceIndex)
	if err != nil {
		return 0, err
	}
	buf.Write(traceData)

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// writeFooter writes the 22-byte v3 footer.
func writeFooter(w io.Writer, headerOffset, compactOffset uint64, compactLen uint32) error {
	var buf [22]byte
	binary.LittleEndian.PutUint16(buf[0:], shared.FooterV3Version)
	binary.LittleEndian.PutUint64(buf[2:], headerOffset)
	binary.LittleEndian.PutUint64(buf[10:], compactOffset)
	binary.LittleEndian.PutUint32(buf[18:], compactLen)
	_, err := w.Write(buf[:])
	return err
}
