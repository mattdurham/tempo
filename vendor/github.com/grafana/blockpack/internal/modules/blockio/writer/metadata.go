package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// writeBlockIndexSection serializes the block index.
// Returns the serialized bytes (without the length prefix — caller adds it).
func writeBlockIndexSection(_ io.Writer, version uint8, metas []shared.BlockMeta) ([]byte, error) {
	var buf bytes.Buffer

	// block_count[4 LE]
	var tmp [4]byte
	binary.LittleEndian.PutUint32(
		tmp[:],
		uint32(len(metas)), //nolint:gosec // safe: block count bounded by MaxBlocks (100_000)
	)
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

		if version < shared.VersionV13 {
			// min_trace_id[16] + max_trace_id[16] — omitted in V13+ (never used for pruning)
			buf.Write(m.MinTraceID[:])
			buf.Write(m.MaxTraceID[:])
		}

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
	binary.LittleEndian.PutUint32(
		tmp[:],
		uint32(len(colNames)), //nolint:gosec // safe: column count bounded by MaxColumns
	)
	buf.Write(tmp[:])

	for _, colName := range colNames {
		cd := rIdx[colName]

		// col_name_len[2 LE] + col_name
		buf.Write([]byte{byte(len(colName)), byte(len(colName) >> 8)}) //nolint:gosec
		buf.WriteString(colName)

		// col_type[1]
		buf.WriteByte(byte(cd.colType))

		writeRangeBucketMeta(&buf, cd)

		// value_count[4 LE]
		binary.LittleEndian.PutUint32(
			tmp[:],
			uint32(len(cd.values)), //nolint:gosec // safe: value count bounded by MaxDictionarySize
		)
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
			binary.LittleEndian.PutUint32(
				tmp[:],
				uint32(len(blockIDs)), //nolint:gosec // safe: block ID count bounded by MaxBlocks
			)
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
	binary.LittleEndian.PutUint64(
		tmp8[:],
		uint64(cd.bucketMin), //nolint:gosec // safe: storing int64 bits as uint64 for wire format
	)
	buf.Write(tmp8[:])
	// bucket_max[8 LE]
	binary.LittleEndian.PutUint64(
		tmp8[:],
		uint64(cd.bucketMax), //nolint:gosec // safe: storing int64 bits as uint64 for wire format
	)
	buf.Write(tmp8[:])

	// boundary_count[4 LE] + boundaries[N × 8 LE int64]
	binary.LittleEndian.PutUint32(
		tmp4[:],
		uint32(len(cd.boundaries)), //nolint:gosec // safe: boundary count bounded by defaultRangeBuckets+1
	)
	buf.Write(tmp4[:])
	for _, b := range cd.boundaries {
		binary.LittleEndian.PutUint64(
			tmp8[:],
			uint64(b), //nolint:gosec // safe: storing int64 bits as uint64 for wire format
		)
		buf.Write(tmp8[:])
	}

	// typed_count[4 LE] + typed boundaries
	switch cd.colType {
	case shared.ColumnTypeRangeFloat64:
		uint32FloatLen := uint32(len(cd.float64Bounds)) //nolint:gosec
		binary.LittleEndian.PutUint32(tmp4[:], uint32FloatLen)
		buf.Write(tmp4[:])
		for _, f := range cd.float64Bounds {
			binary.LittleEndian.PutUint64(tmp8[:], math.Float64bits(f))
			buf.Write(tmp8[:])
		}
	case shared.ColumnTypeRangeString:
		binary.LittleEndian.PutUint32(
			tmp4[:],
			uint32(len(cd.stringBounds)), //nolint:gosec // safe: string boundary count bounded by defaultRangeBuckets+1
		)
		buf.Write(tmp4[:])
		for _, s := range cd.stringBounds {
			binary.LittleEndian.PutUint32(
				tmp4[:],
				uint32(len(s)), //nolint:gosec // safe: string boundary length bounded by MaxStringLen
			)
			buf.Write(tmp4[:])
			buf.WriteString(s)
		}
	case shared.ColumnTypeRangeBytes:
		binary.LittleEndian.PutUint32(
			tmp4[:],
			uint32(len(cd.bytesBounds)), //nolint:gosec // safe: bytes boundary count bounded by defaultRangeBuckets+1
		)
		buf.Write(tmp4[:])
		for _, b := range cd.bytesBounds {
			binary.LittleEndian.PutUint32(
				tmp4[:],
				uint32(len(b)), //nolint:gosec // safe: bytes boundary length bounded by MaxBytesLen
			)
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
		binary.LittleEndian.PutUint32(
			tmp4[:],
			uint32(len(key)), //nolint:gosec // safe: key length bounded by MaxStringLen
		)
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

// NOTE-37: trace index v2 — block IDs only, no span indices; reader scans in-block.
// writeTraceBlockIndexSection serializes the trace block index (format version 2).
// Format: fmt_version[1]=0x02 + trace_count[4 LE] +
// per trace: trace_id[16] + block_ref_count[2 LE] + block_id[2 LE] × N.
// Span indices are not stored; the reader scans the trace:id column in-block.
// Returns the serialized bytes.
func writeTraceBlockIndexSection(_ io.Writer, traceIndex map[[16]byte][]uint16) ([]byte, error) {
	var buf bytes.Buffer

	// Sort trace IDs for deterministic output.
	traceIDs := make([][16]byte, 0, len(traceIndex))
	for tid := range traceIndex {
		traceIDs = append(traceIDs, tid)
	}
	sort.Slice(traceIDs, func(i, j int) bool {
		return bytes.Compare(traceIDs[i][:], traceIDs[j][:]) < 0
	})

	// fmt_version[1] = 0x02
	buf.WriteByte(shared.TraceIndexFmtVersion2)

	// trace_count[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(
		tmp4[:],
		uint32(len(traceIDs)), //nolint:gosec // safe: trace count bounded by MaxTraceCount
	)
	buf.Write(tmp4[:])

	var tmp2 [2]byte

	for _, tid := range traceIDs {
		blockIDs := traceIndex[tid]

		if len(blockIDs) > math.MaxUint16 {
			return nil, fmt.Errorf("trace_index: trace %x has %d block refs, exceeds uint16 max", tid, len(blockIDs))
		}

		// trace_id[16]
		buf.Write(tid[:])

		// block_ref_count[2 LE]
		binary.LittleEndian.PutUint16(
			tmp2[:],
			uint16(len(blockIDs)), //nolint:gosec // safe: len(blockIDs) <= math.MaxUint16 checked above
		)
		buf.Write(tmp2[:])

		// block_id[2 LE] × N
		for _, bid := range blockIDs {
			binary.LittleEndian.PutUint16(tmp2[:], bid)
			buf.Write(tmp2[:])
		}
	}

	return buf.Bytes(), nil
}

// writeFileHeader writes the file header.
// For version < 12: 21 bytes (magic[4] + version[1] + metadataOffset[8] + metadataLen[8]).
// For version 12+:  22 bytes (same + signalType[1] at offset 21).
func writeFileHeader(w io.Writer, version uint8, metadataOffset, metadataLen uint64, signalType uint8) error {
	if version >= shared.VersionV12 {
		var buf [22]byte
		binary.LittleEndian.PutUint32(buf[0:], shared.MagicNumber)
		buf[4] = version
		binary.LittleEndian.PutUint64(buf[5:], metadataOffset)
		binary.LittleEndian.PutUint64(buf[13:], metadataLen)
		buf[21] = signalType
		_, err := w.Write(buf[:])
		return err
	}
	var buf [21]byte
	binary.LittleEndian.PutUint32(buf[0:], shared.MagicNumber)
	buf[4] = version
	binary.LittleEndian.PutUint64(buf[5:], metadataOffset)
	binary.LittleEndian.PutUint64(buf[13:], metadataLen)
	_, err := w.Write(buf[:])
	return err
}

// NOTE-36: bloom-enabled compact index (v2) — see SPECS.md §6
// writeCompactTraceIndex writes the compact trace index section (version 2).
// Format: magic[4] + version[1]=2 + block_count[4] + bloom_bytes[4] + bloom_data[N] +
// block_table[block_count×12] + trace_index[...].
// Returns bytes written.
func writeCompactTraceIndex(
	w io.Writer,
	blockMetas []shared.BlockMeta,
	traceIndex map[[16]byte][]uint16,
) (int64, error) {
	var buf bytes.Buffer

	// magic[4 LE] = 0xC01DC1DE
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], shared.CompactIndexMagic)
	buf.Write(tmp4[:])

	// version[1] = 2 (bloom-enabled)
	buf.WriteByte(shared.CompactIndexVersion2)

	// block_count[4 LE]
	binary.LittleEndian.PutUint32(
		tmp4[:],
		uint32(len(blockMetas)), //nolint:gosec // safe: block count bounded by MaxBlocks
	)
	buf.Write(tmp4[:])

	// Build trace ID bloom filter from all trace IDs in the index.
	bloomSize := shared.TraceIDBloomSize(len(traceIndex))
	bloom := make([]byte, bloomSize)
	for tid := range traceIndex {
		shared.AddTraceIDToBloom(bloom, tid)
	}

	// bloom_bytes[4 LE] + bloom_data[bloom_bytes]
	binary.LittleEndian.PutUint32(
		tmp4[:],
		uint32(bloomSize), //nolint:gosec // safe: bloom size bounded by TraceIDBloomMaxBytes (1MB) fits uint32
	)
	buf.Write(tmp4[:])
	buf.Write(bloom)

	// block_table: block_count × { file_offset[8 LE] + file_length[4 LE] }
	for _, m := range blockMetas {
		var tmp8 [8]byte
		binary.LittleEndian.PutUint64(tmp8[:], m.Offset)
		buf.Write(tmp8[:])
		binary.LittleEndian.PutUint32(
			tmp4[:],
			uint32(m.Length), //nolint:gosec // safe: block length bounded by MaxBlockSize (1GB) fits uint32
		)
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

// writeFooterV4 writes the 34-byte v4 footer with intrinsic section offsets.
//
// Wire format:
//
//	version[2 LE]                  = FooterV4Version (4)
//	header_offset[8 LE]
//	compact_offset[8 LE]
//	compact_len[4 LE]
//	intrinsic_index_offset[8 LE]
//	intrinsic_index_len[4 LE]
func writeFooterV4(w io.Writer, headerOffset, compactOffset uint64, compactLen uint32, intrinsicOffset uint64, intrinsicLen uint32) error {
	var buf [34]byte
	binary.LittleEndian.PutUint16(buf[0:], shared.FooterV4Version)
	binary.LittleEndian.PutUint64(buf[2:], headerOffset)
	binary.LittleEndian.PutUint64(buf[10:], compactOffset)
	binary.LittleEndian.PutUint32(buf[18:], compactLen)
	binary.LittleEndian.PutUint64(buf[22:], intrinsicOffset)
	binary.LittleEndian.PutUint32(buf[30:], intrinsicLen)
	_, err := w.Write(buf[:])
	return err
}
