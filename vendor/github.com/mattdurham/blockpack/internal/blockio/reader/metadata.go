package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// readFooter parses the streaming footer from the last 10 bytes
// Format: version(2) + header_offset(8) = 10 bytes
func readFooter(data []byte) (fileFooter, error) {
	if len(data) < footerSize {
		return fileFooter{}, fmt.Errorf("data too small for footer (expected %d bytes)", footerSize)
	}

	start := len(data) - footerSize
	footer := fileFooter{
		version:      binary.LittleEndian.Uint16(data[start : start+2]),
		headerOffset: binary.LittleEndian.Uint64(data[start+2 : start+10]),
	}

	// Validate footer version to reject corrupted/invalid data
	if footer.version != shared.FooterVersion {
		return fileFooter{}, fmt.Errorf(
			"invalid footer version: got %d, expected %d",
			footer.version,
			shared.FooterVersion,
		)
	}

	return footer, nil
}

// readHeader parses the file header
// Format: magic(4) + version(1) + metadata_offset(8) + metadata_len(8) + aggregate_offset(8) + aggregate_len(8) = 37 bytes
func readHeader(data []byte) (fileHeader, error) {
	if len(data) < 37 {
		return fileHeader{}, fmt.Errorf("data too small for header (expected 37 bytes, got %d)", len(data))
	}

	header := fileHeader{
		magic:              binary.LittleEndian.Uint32(data[0:4]),
		version:            data[4],
		metadataOffset:     binary.LittleEndian.Uint64(data[5:13]),
		metadataLen:        binary.LittleEndian.Uint64(data[13:21]),
		metricStreamOffset: binary.LittleEndian.Uint64(data[21:29]),
		metricStreamLen:    binary.LittleEndian.Uint64(data[29:37]),
		metadataCRC:        0, // CRC will be computed from metadata, not stored in header
	}
	return header, nil
}

// parseV5MetadataLazy parses block entries but only records offsets for dedicated indexes
// Actual dedicated indexes are parsed on-demand when first accessed
func parseV5MetadataLazy(
	data []byte,
	version uint8,
) ([]blockIndexEntry, map[string]dedicatedIndexMeta, map[[16]byte][]TraceBlockEntry, error) {
	if len(data) < 4 {
		return nil, nil, nil, fmt.Errorf("truncated metadata header")
	}
	blockCount := int(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4

	entries := make([]blockIndexEntry, blockCount)
	for i := 0; i < blockCount; i++ {
		var err error
		reader := bytes.NewReader(data[offset:])
		entries[i], err = readBlockIndexEntry(reader, version)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("block index %d: %w", i, err)
		}
		// Calculate how many bytes were read
		bytesRead := len(data[offset:]) - reader.Len()
		offset += bytesRead
	}

	dedicatedOffsets, offset, err := scanDedicatedIndexOffsets(data, offset)
	if err != nil {
		return nil, nil, nil, err
	}

	// Parse per-column offsets for each block
	offset, err = parseColumnIndexData(data, offset, entries)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse column index: %w", err)
	}

	// Parse trace block index
	traceBlockIndex, err := parseTraceBlockIndex(data, offset)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse trace block index: %w", err)
	}

	return entries, dedicatedOffsets, traceBlockIndex, nil
}

// parseTraceBlockIndex parses the trace block index with span-level indices.
// Format (new v1):
//   - format_version (1 byte) - 0x01 = new format with span indices
//   - trace_count (4 bytes)
//   - For each trace (sorted by trace_id):
//   - trace_id (16 bytes)
//   - block_count (2 bytes)
//   - For each block:
//   - block_id (2 bytes)
//   - span_count (2 bytes)
//   - span_indices (2 bytes each)
func parseTraceBlockIndex(data []byte, offset int) (map[[16]byte][]TraceBlockEntry, error) {
	// Read format version for backward compatibility.
	// New format: leading byte 0x01, followed by traceCount (4 bytes, LE).
	// Old format: no version byte; data starts directly with traceCount.
	if offset < len(data) && data[offset] == 0x01 {
		offset++ // Skip version byte for new format
	}
	// For any other leading byte value, treat as legacy format (no version
	// byte) and fall through to reading traceCount directly.

	// Read trace count
	if offset+4 > len(data) {
		return nil, fmt.Errorf("truncated trace count")
	}
	traceCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	// HIGH: Validate traceCount to prevent OOM from crafted files
	const maxTraceCount = 10_000_000
	if traceCount > maxTraceCount {
		return nil, fmt.Errorf("trace count %d exceeds maximum %d", traceCount, maxTraceCount)
	}

	traceIndex := make(map[[16]byte][]TraceBlockEntry, traceCount)

	for i := 0; i < traceCount; i++ {
		// Read trace_id (16 bytes)
		if offset+16 > len(data) {
			return nil, fmt.Errorf("truncated trace_id at trace %d", i)
		}
		var traceID [16]byte
		copy(traceID[:], data[offset:offset+16])
		offset += 16

		// Read block_count (2 bytes)
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated block_count at trace %d", i)
		}
		blockCount := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		// Read block entries with span indices
		entries := make([]TraceBlockEntry, blockCount)
		for j := 0; j < blockCount; j++ {
			// Read block_id (2 bytes)
			if offset+2 > len(data) {
				return nil, fmt.Errorf("truncated block_id at trace %d, block %d", i, j)
			}
			entries[j].BlockID = int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2

			// Read span_count (2 bytes)
			if offset+2 > len(data) {
				return nil, fmt.Errorf("truncated span_count at trace %d, block %d", i, j)
			}
			spanCount := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2

			// Validate spanCount and required bytes before allocation
			if offset+spanCount*2 > len(data) {
				return nil, fmt.Errorf(
					"truncated span_indices at trace %d, block %d: need %d bytes, have %d",
					i,
					j,
					spanCount*2,
					len(data)-offset,
				)
			}

			// Read span_indices (2 bytes each)
			entries[j].SpanIndices = make([]uint16, spanCount)
			for k := 0; k < spanCount; k++ {
				if offset+2 > len(data) {
					return nil, fmt.Errorf("truncated span_index at trace %d, block %d, span %d", i, j, k)
				}
				entries[j].SpanIndices[k] = binary.LittleEndian.Uint16(data[offset:])
				offset += 2
			}
		}

		traceIndex[traceID] = entries
	}

	return traceIndex, nil
}

// skipTypedBoundaries advances offset past typed boundary data for a range column.
func skipTypedBoundaries(data []byte, offset int, typ ColumnType, typedCount int, name string) (int, error) {
	switch typ {
	case ColumnTypeRangeFloat64:
		// Each float64 boundary is 8 bytes
		if offset+typedCount*8 > len(data) {
			return 0, fmt.Errorf("truncated float64 typed boundaries for %s", name)
		}
		return offset + typedCount*8, nil
	case ColumnTypeRangeString:
		// Each string boundary: uint32(len) + bytes
		const maxBoundaryLen = 1024 * 1024 // 1 MiB per boundary element
		for k := 0; k < typedCount; k++ {
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated string typed boundary length %d for %s", k, name)
			}
			sLen := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if sLen > maxBoundaryLen {
				return 0, fmt.Errorf(
					"string boundary length %d exceeds max %d for column %s",
					sLen,
					maxBoundaryLen,
					name,
				)
			}
			if offset+sLen > len(data) {
				return 0, fmt.Errorf("truncated string typed boundary data %d for %s", k, name)
			}
			offset += sLen
		}
		return offset, nil
	case ColumnTypeRangeBytes:
		// Each bytes boundary: uint32(len) + bytes
		const maxBoundaryLenBytes = 1024 * 1024 // 1 MiB per boundary element
		for k := 0; k < typedCount; k++ {
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated bytes typed boundary length %d for %s", k, name)
			}
			bLen := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if bLen > maxBoundaryLenBytes {
				return 0, fmt.Errorf(
					"bytes boundary length %d exceeds max %d for column %s",
					bLen,
					maxBoundaryLenBytes,
					name,
				)
			}
			if offset+bLen > len(data) {
				return 0, fmt.Errorf("truncated bytes typed boundary data %d for %s", k, name)
			}
			offset += bLen
		}
		return offset, nil
	default:
		// RangeInt64/Uint64/Duration: typedCount should be 0, nothing extra to skip
		return offset, nil
	}
}

// scanDedicatedIndexOffsets scans the metadata and records where each dedicated column's index is located
// Does NOT parse the actual values - just records offsets for lazy loading
// Returns the offsets map and the final offset after scanning all dedicated indexes
func scanDedicatedIndexOffsets(data []byte, offset int) (map[string]dedicatedIndexMeta, int, error) {
	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated dedicated index count")
	}
	dedicatedCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	offsets := make(map[string]dedicatedIndexMeta, dedicatedCount)
	for i := 0; i < dedicatedCount; i++ {
		startOffset := offset

		// Read column name
		if offset+2 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated column name length")
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen+1 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated column name")
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++

		// Skip bucket metadata if present
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("truncated bucket metadata flag for %s", name)
		}
		hasBuckets := data[offset]
		offset++

		if hasBuckets == 1 {
			// Skip bucket metadata
			if offset+8+8+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated bucket metadata header for %s", name)
			}
			offset += 8 // min
			offset += 8 // max
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated boundary count for %s", name)
			}
			boundaryCount := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			const maxBoundaries = 65536
			if boundaryCount > maxBoundaries {
				return nil, 0, fmt.Errorf(
					"boundary count %d exceeds max %d for column %s",
					boundaryCount,
					maxBoundaries,
					name,
				)
			}
			if offset+boundaryCount*8 > len(data) {
				return nil, 0, fmt.Errorf("truncated bucket boundaries for %s", name)
			}
			offset += boundaryCount * 8

			// Skip typed boundaries
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated typed boundary count for column %s", name)
			}
			typedCount := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			const maxTypedBoundaries = 65536
			if typedCount > maxTypedBoundaries {
				return nil, 0, fmt.Errorf(
					"typed boundary count %d exceeds max %d for column %s",
					typedCount,
					maxTypedBoundaries,
					name,
				)
			}
			var err error
			offset, err = skipTypedBoundaries(data, offset, typ, typedCount, name)
			if err != nil {
				return nil, 0, err
			}
		}

		// Read value count but don't parse values
		if offset+4 > len(data) {
			return nil, 0, fmt.Errorf("truncated dedicated value count for %s", name)
		}
		valueCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		// Skip over all values and their block IDs
		for j := 0; j < valueCount; j++ {
			// Skip value key
			switch typ {
			case ColumnTypeString:
				if offset+4 > len(data) {
					return nil, 0, fmt.Errorf("truncated string length")
				}
				strLen := int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4
				if offset+strLen > len(data) {
					return nil, 0, fmt.Errorf("truncated string value data for column %s", name)
				}
				offset += strLen
			case ColumnTypeInt64, ColumnTypeUint64, ColumnTypeFloat64:
				offset += 8
			case ColumnTypeBool:
				offset++
			case ColumnTypeBytes:
				if offset+4 > len(data) {
					return nil, 0, fmt.Errorf("truncated bytes length")
				}
				bytesLen := int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4
				if offset+bytesLen > len(data) {
					return nil, 0, fmt.Errorf("truncated bytes value data for column %s", name)
				}
				offset += bytesLen
			case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration, ColumnTypeRangeFloat64:
				// Numeric range keys have a single-byte length prefix (2 or 8)
				if offset+1 > len(data) {
					return nil, 0, fmt.Errorf("truncated range key length")
				}
				length := int(data[offset])
				offset += 1 + length // length prefix + key data
			case ColumnTypeRangeBytes, ColumnTypeRangeString:
				// Variable-length range keys use varint encoding for the length
				length64, n := binary.Uvarint(data[offset:])
				if n <= 0 {
					return nil, 0, fmt.Errorf("truncated range key length varint")
				}
				offset += n
				length := int(length64) //nolint:gosec
				offset += length        // key data
			default:
				return nil, 0, fmt.Errorf("unsupported dedicated type %d", typ)
			}

			// Skip block IDs
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("truncated block id count")
			}
			idCount := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if offset+idCount*4 > len(data) {
				return nil, 0, fmt.Errorf("truncated block ids")
			}
			offset += idCount * 4
		}

		// Record the offset and length for this column
		length := offset - startOffset
		offsets[name] = dedicatedIndexMeta{
			typ:    typ,
			offset: startOffset,
			length: length,
		}
	}

	return offsets, offset, nil
}

// parseColumnIndexData parses the per-column offset data for all blocks
func parseColumnIndexData(data []byte, offset int, entries []blockIndexEntry) (int, error) {
	for i := range entries {
		// Read column count
		if offset+4 > len(data) {
			return 0, fmt.Errorf("truncated column count for block %d", i)
		}
		columnCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		entries[i].ColumnIndex = make([]columnMetadata, columnCount)
		for j := 0; j < columnCount; j++ {
			// Read column name length
			if offset+2 > len(data) {
				return 0, fmt.Errorf("truncated column name length for block %d, column %d", i, j)
			}
			nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
			offset += 2

			// Read column name
			if offset+nameLen > len(data) {
				return 0, fmt.Errorf("truncated column name for block %d, column %d", i, j)
			}
			name := string(data[offset : offset+nameLen])
			offset += nameLen

			// Read offset
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated column offset for block %d, column %d", i, j)
			}
			colOffset := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			// Read length
			if offset+4 > len(data) {
				return 0, fmt.Errorf("truncated column length for block %d, column %d", i, j)
			}
			colLength := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			// Validate column bounds are within block bounds
			blockSize := entries[i].Length
			if uint64(colOffset)+uint64(colLength) > blockSize {
				return 0, fmt.Errorf("block %d column %d out of bounds: offset=%d length=%d blockSize=%d",
					i, j, colOffset, colLength, blockSize)
			}

			entries[i].ColumnIndex[j] = columnMetadata{
				Name:   name,
				Offset: colOffset,
				Length: colLength,
			}
		}
	}
	return offset, nil
}

// readBlockIndexEntry reads a single block index entry from a bytes.Reader
func readBlockIndexEntry(rd *bytes.Reader, version uint8) (blockIndexEntry, error) {
	var e blockIndexEntry
	if err := binary.Read(rd, binary.LittleEndian, &e.Offset); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.Length); err != nil {
		return e, err
	}
	if version >= versionV11 {
		kind, err := rd.ReadByte()
		if err != nil {
			return e, err
		}
		e.Kind = blockEntryKind(kind)
	} else {
		e.Kind = blockEntryKindLeaf
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.SpanCount); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.MinStart); err != nil {
		return e, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &e.MaxStart); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.MinTraceID[:]); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.MaxTraceID[:]); err != nil {
		return e, err
	}
	if _, err := rd.Read(e.ColumnNameBloom[:]); err != nil {
		return e, err
	}

	// Read value statistics for v10 format
	if version >= versionV10 {
		valueStats, err := shared.ReadValueStats(rd)
		if err != nil {
			return e, fmt.Errorf("read value stats: %w", err)
		}
		e.ValueStats = valueStats
	}

	return e, nil
}
