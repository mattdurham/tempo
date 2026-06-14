package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// writeBlockIndexSection serializes the block index.
// Returns the serialized bytes (without the length prefix — caller adds it).
func writeBlockIndexSection(_ io.Writer, metas []shared.BlockMeta) ([]byte, error) {
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

		// kind[1]
		buf.WriteByte(byte(m.Kind))

		// span_count[4 LE]
		binary.LittleEndian.PutUint32(tmp[:], m.SpanCount)
		buf.Write(tmp[:])

		// min_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MinStart)
		buf.Write(off[:])

		// max_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MaxStart)
		buf.Write(off[:])

		// V13: MinTraceID/MaxTraceID omitted (never used for pruning).
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

// V7 footer field offsets (M-25).
// Wire format: magic[4] · version[2] · dir_offset[8] · dir_len[4] = 18 bytes.
const (
	footerV7OffVersion = 4  // uint16 version field within 18-byte V7 footer
	footerV7OffDirOff  = 6  // uint64 dir_offset field
	footerV7OffDirLen  = 14 // uint32 dir_len field
)

// writeFooterV8 writes the 18-byte V8 footer for V8 (unified ToC) files.
//
// Wire format (SPEC-FORMAT-001):
//
//	magic[4]=0xC011FEA1 · version[2]=8 · toc_offset[8] · toc_length[4]
//
// Reuses the same field-offset constants as V7 because the layout is identical.
func writeFooterV8(w io.Writer, tocOffset uint64, tocLen uint32) error {
	var buf [18]byte
	binary.LittleEndian.PutUint32(buf[0:], shared.MagicNumber)
	binary.LittleEndian.PutUint16(buf[footerV7OffVersion:], shared.FooterV8Version)
	binary.LittleEndian.PutUint64(buf[footerV7OffDirOff:], tocOffset)
	binary.LittleEndian.PutUint32(buf[footerV7OffDirLen:], tocLen)
	_, err := w.Write(buf[:])
	return err
}

// writeOneColumnRangeBlob serializes the range data for a single column.
// The returned bytes do NOT include the column name (the ToCEntry.Key.Name carries it).
// Wire format: col_type[1] + bucket_metadata + value_count[4] + value_entries[...].
// Returns nil, nil if cd has no bucket entries (cd.values and cd.numValues are both empty).
func writeOneColumnRangeBlob(cd *rangeColumnData) ([]byte, error) {
	// Numeric columns (int64/uint64/float64) use numValues; string/bytes use values.
	if len(cd.numValues) > 0 {
		return writeOneColumnRangeBlobNum(cd)
	}
	if len(cd.values) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	buf.WriteByte(byte(cd.colType))
	writeRangeBucketMeta(&buf, cd)

	// Sort value keys for deterministic output.
	keys := make([]string, 0, len(cd.values))
	for k := range cd.values {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// value_count[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(
		tmp4[:],
		uint32(len(keys)), //nolint:gosec // safe: value count bounded by MaxDictionarySize
	)
	buf.Write(tmp4[:])

	for _, key := range keys {
		blockIDs := cd.values[key]
		writeRangeValueKey(&buf, cd.colType, key)
		binary.LittleEndian.PutUint32(
			tmp4[:],
			uint32(len(blockIDs)), //nolint:gosec // safe: block ID count bounded by MaxBlocks
		)
		buf.Write(tmp4[:])
		for _, bid := range blockIDs {
			binary.LittleEndian.PutUint32(tmp4[:], bid)
			buf.Write(tmp4[:])
		}
	}
	return buf.Bytes(), nil
}

// writeOneColumnRangeBlobNum serializes range data for numeric columns that store
// bucket entries in numValues ([8]byte keys) rather than values (string keys).
// Wire format is identical to writeOneColumnRangeBlob.
func writeOneColumnRangeBlobNum(cd *rangeColumnData) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(byte(cd.colType))
	writeRangeBucketMeta(&buf, cd)

	// Sort keys for deterministic output by converting to strings.
	keys := make([][8]byte, 0, len(cd.numValues))
	for k := range cd.numValues {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b [8]byte) int {
		return bytes.Compare(a[:], b[:])
	})

	// value_count[4 LE]
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(keys))) //nolint:gosec
	buf.Write(tmp4[:])

	for _, bk := range keys {
		blockIDs := cd.numValues[bk]
		writeRangeValueKey(&buf, cd.colType, string(bk[:]))
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(blockIDs))) //nolint:gosec
		buf.Write(tmp4[:])
		for _, bid := range blockIDs {
			binary.LittleEndian.PutUint32(tmp4[:], bid)
			buf.Write(tmp4[:])
		}
	}
	return buf.Bytes(), nil
}
