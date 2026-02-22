package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// writer_metadata.go contains functions for building metadata buffers and footers.

// writeDedicatedValue writes a single dedicated value key to the buffer.
// Handles different column types with appropriate encoding.
func writeDedicatedValue(buf *bytes.Buffer, key DedicatedValueKey) error {
	data := key.Data()
	switch key.Type() {
	case ColumnTypeString, ColumnTypeBytes:
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil { //nolint:gosec
			return fmt.Errorf("write value length for string/bytes: %w", err)
		}
		if len(data) > 0 {
			if _, err := buf.Write(data); err != nil {
				return fmt.Errorf("write value data for string/bytes: %w", err)
			}
		}
		return nil
	case ColumnTypeInt64, ColumnTypeUint64, ColumnTypeFloat64:
		if len(data) != 8 {
			return fmt.Errorf("invalid dedicated numeric key length %d", len(data))
		}
		if _, err := buf.Write(data); err != nil {
			return fmt.Errorf("write numeric value: %w", err)
		}
		return nil
	case ColumnTypeBool:
		if len(data) != 1 {
			return fmt.Errorf("invalid dedicated bool key length %d", len(data))
		}
		if err := buf.WriteByte(data[0]); err != nil {
			return fmt.Errorf("write bool value: %w", err)
		}
		return nil
	case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration, ColumnTypeRangeFloat64:
		// Numeric range keys can be either:
		// - 2 bytes: bucket ID (uint16) for bucketed columns
		// - 8 bytes: raw value (int64/uint64/float64) for direct value indexing (low cardinality)
		if len(data) != 2 && len(data) != 8 {
			return fmt.Errorf("invalid numeric range key length %d (expected 2 or 8)", len(data))
		}
		// Write length prefix to distinguish bucket IDs from raw values
		if err := buf.WriteByte(byte(len(data))); err != nil {
			return fmt.Errorf("write range key length prefix: %w", err)
		}
		if _, err := buf.Write(data); err != nil {
			return fmt.Errorf("write range key data: %w", err)
		}
		return nil
	case ColumnTypeRangeBytes, ColumnTypeRangeString:
		// Variable-length range keys can be:
		// - 2 bytes: bucket ID (uint16) for bucketed columns
		// - Variable length: raw value (bytes/string) for direct value indexing (low cardinality)
		// Use varint encoding for length to support any size (including 0 for empty strings)
		// Write length as varint (1-4 bytes for most cases)
		lenBuf := make([]byte, binary.MaxVarintLen32)
		n := binary.PutUvarint(lenBuf, uint64(len(data)))
		if _, err := buf.Write(lenBuf[:n]); err != nil {
			return fmt.Errorf("write range key length: %w", err)
		}
		if len(data) > 0 {
			if _, err := buf.Write(data); err != nil {
				return fmt.Errorf("write range key data: %w", err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported dedicated column type %d", key.Type())
	}
}

// buildDedicatedIndexBuffer serializes the dedicated column index to a buffer.
// The index maps column names to value keys to block IDs that contain that value.
func buildDedicatedIndexBuffer(
	dedicatedIndex map[string]map[string]map[int]struct{},
	rangeBucketMeta map[string]*RangeBucketMetadata,
	autoDedicatedTypes map[string]ColumnType,
	rangeMetadata *RangeMetadata,
) (*bytes.Buffer, error) {
	dedicatedBuf := &bytes.Buffer{}
	dedicatedCols := make([]string, 0, len(dedicatedIndex))
	for col := range dedicatedIndex {
		dedicatedCols = append(dedicatedCols, col)
	}
	sort.Strings(dedicatedCols)

	if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(dedicatedCols))); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write dedicated column count: %w", err)
	}
	for _, col := range dedicatedCols {
		// Get type from auto-detected types
		typ, ok := autoDedicatedTypes[col]
		if !ok {
			return nil, fmt.Errorf("unknown dedicated column %s (not in auto-detected types)", col)
		}
		if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint16(len(col))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write column name length for %s: %w", col, err)
		}
		if _, err := dedicatedBuf.Write([]byte(col)); err != nil {
			return nil, fmt.Errorf("write column name %s: %w", col, err)
		}
		if err := dedicatedBuf.WriteByte(byte(typ)); err != nil {
			return nil, fmt.Errorf("write column type for %s: %w", col, err)
		}

		// Check if this is a range-bucketed column with metadata
		bucketMeta, hasBuckets := rangeBucketMeta[col]
		if hasBuckets && IsRangeColumnType(typ) {
			// Write bucket metadata flag (1 = has buckets)
			if err := dedicatedBuf.WriteByte(1); err != nil {
				return nil, fmt.Errorf("write bucket metadata flag for %s: %w", col, err)
			}

			// Write min, max, and boundaries
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, bucketMeta.Min); err != nil {
				return nil, fmt.Errorf("write bucket min for %s: %w", col, err)
			}
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, bucketMeta.Max); err != nil {
				return nil, fmt.Errorf("write bucket max for %s: %w", col, err)
			}
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(bucketMeta.Boundaries))); err != nil { //nolint:gosec
				return nil, fmt.Errorf("write boundary count for %s: %w", col, err)
			}
			for i, boundary := range bucketMeta.Boundaries {
				if err := binary.Write(dedicatedBuf, binary.LittleEndian, boundary); err != nil {
					return nil, fmt.Errorf("write boundary %d for %s: %w", i, col, err)
				}
			}

			// Write typed boundaries after int64 boundaries
			if err := writeTypedBoundaries(dedicatedBuf, col, typ, rangeMetadata); err != nil {
				return nil, err
			}
		} else {
			// Write bucket metadata flag (0 = no buckets, normal dedicated column)
			if err := dedicatedBuf.WriteByte(0); err != nil {
				return nil, fmt.Errorf("write no-bucket flag for %s: %w", col, err)
			}
		}

		valueMap := dedicatedIndex[col]
		valueKeys := make([]string, 0, len(valueMap))
		for k := range valueMap {
			valueKeys = append(valueKeys, k)
		}
		sort.Strings(valueKeys)

		if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(valueKeys))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write value count for column %s: %w", col, err)
		}
		for _, encodedKey := range valueKeys {
			key, err := DecodeDedicatedKey(encodedKey)
			if err != nil {
				return nil, fmt.Errorf("decode dedicated key for column %s: %w", col, err)
			}
			if key.Type() != typ {
				return nil, fmt.Errorf(
					"dedicated column %s has mismatched key type %d (expected %d)",
					col,
					key.Type(),
					typ,
				)
			}
			if err := writeDedicatedValue(dedicatedBuf, key); err != nil {
				return nil, fmt.Errorf("write dedicated value for column %s: %w", col, err)
			}
			ids := make([]int, 0, len(valueMap[encodedKey]))
			for id := range valueMap[encodedKey] {
				ids = append(ids, id)
			}
			sort.Ints(ids)
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(ids))); err != nil { //nolint:gosec
				return nil, fmt.Errorf("write block ID count for column %s: %w", col, err)
			}
			for _, id := range ids {
				if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(id)); err != nil { //nolint:gosec
					return nil, fmt.Errorf("write block ID %d for column %s: %w", id, col, err)
				}
			}
		}
	}

	return dedicatedBuf, nil
}

// writeTypedBoundaries writes the typed boundary section for a range column.
// Format: typed_count (4 bytes uint32) + typed data (type-specific encoding)
// For ColumnTypeRangeFloat64: count + float64bits values (8 bytes each)
// For ColumnTypeRangeString: count + uint32(len) + string bytes (each)
// For ColumnTypeRangeBytes: count + uint32(len) + byte data (each)
// For others (RangeInt64/Uint64/Duration): typed_count = 0
func writeTypedBoundaries(buf *bytes.Buffer, col string, typ ColumnType, rangeMetadata *RangeMetadata) error {
	switch typ {
	case ColumnTypeRangeFloat64:
		var f64Boundaries []float64
		if rangeMetadata != nil {
			f64Boundaries, _ = rangeMetadata.GetFloat64Boundaries(col)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(f64Boundaries))); err != nil { //nolint:gosec
			return fmt.Errorf("write float64 boundary count for %s: %w", col, err)
		}
		for i, v := range f64Boundaries {
			if err := binary.Write(buf, binary.LittleEndian, math.Float64bits(v)); err != nil {
				return fmt.Errorf("write float64 boundary %d for %s: %w", i, col, err)
			}
		}
	case ColumnTypeRangeString:
		var strBoundaries []string
		if rangeMetadata != nil {
			strBoundaries, _ = rangeMetadata.GetStringBoundaries(col)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(strBoundaries))); err != nil { //nolint:gosec
			return fmt.Errorf("write string boundary count for %s: %w", col, err)
		}
		for i, s := range strBoundaries {
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil { //nolint:gosec
				return fmt.Errorf("write string boundary length %d for %s: %w", i, col, err)
			}
			if len(s) > 0 {
				if _, err := buf.Write([]byte(s)); err != nil {
					return fmt.Errorf("write string boundary data %d for %s: %w", i, col, err)
				}
			}
		}
	case ColumnTypeRangeBytes:
		var bytesBoundaries [][]byte
		if rangeMetadata != nil {
			bytesBoundaries, _ = rangeMetadata.GetBytesBoundaries(col)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(bytesBoundaries))); err != nil { //nolint:gosec
			return fmt.Errorf("write bytes boundary count for %s: %w", col, err)
		}
		for i, b := range bytesBoundaries {
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(b))); err != nil { //nolint:gosec
				return fmt.Errorf("write bytes boundary length %d for %s: %w", i, col, err)
			}
			if len(b) > 0 {
				if _, err := buf.Write(b); err != nil {
					return fmt.Errorf("write bytes boundary data %d for %s: %w", i, col, err)
				}
			}
		}
	default:
		// ColumnTypeRangeInt64, RangeUint64, RangeDuration: no typed boundaries needed.
		// Write zero typed_count to keep format consistent.
		if err := binary.Write(buf, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("write zero typed boundary count for %s: %w", col, err)
		}
	}
	return nil
}

// buildTraceBlockIndexBuffer serializes the trace block index with span-level indices.
// Format:
//   - format_version (1 byte) - 0x01 = new format with span indices
//   - trace_count (4 bytes)
//   - For each trace (sorted by trace_id):
//   - trace_id (16 bytes)
//   - block_count (2 bytes)
//   - For each block (sorted by block_id):
//   - block_id (2 bytes)
//   - span_count (2 bytes)
//   - span_indices (2 bytes each)
func buildTraceBlockIndexBuffer(traceBlockIndex map[[16]byte]map[int][]uint16) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	// HIGH: Write format version to distinguish from old format (backward compatibility)
	// 0x01 = new format with block IDs + span indices
	if err := buf.WriteByte(0x01); err != nil {
		return nil, err
	}

	// Sort trace IDs for deterministic output and binary search
	traceIDs := make([][16]byte, 0, len(traceBlockIndex))
	for traceID := range traceBlockIndex {
		traceIDs = append(traceIDs, traceID)
	}
	sort.Slice(traceIDs, func(i, j int) bool {
		return bytes.Compare(traceIDs[i][:], traceIDs[j][:]) < 0
	})

	// Write trace count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(traceIDs))); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write trace count: %w", err)
	}

	// Write each trace entry
	for _, traceID := range traceIDs {
		blockMap := traceBlockIndex[traceID]

		// Convert to sorted block IDs
		blockIDs := make([]int, 0, len(blockMap))
		for blockID := range blockMap {
			blockIDs = append(blockIDs, blockID)
		}
		sort.Ints(blockIDs)

		// Write trace_id (16 bytes)
		if _, err := buf.Write(traceID[:]); err != nil {
			return nil, fmt.Errorf("write trace ID: %w", err)
		}

		// Write block_count (2 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(blockIDs))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write block count for trace: %w", err)
		}

		// Write each block entry with span indices
		for _, blockID := range blockIDs {
			spanIndices := blockMap[blockID]

			// Write block_id (2 bytes)
			if err := binary.Write(buf, binary.LittleEndian, uint16(blockID)); err != nil { //nolint:gosec
				return nil, fmt.Errorf("write block ID %d for trace: %w", blockID, err)
			}

			// Write span_count (2 bytes)
			if len(spanIndices) > 65535 {
				return nil, fmt.Errorf("span count %d exceeds uint16 max for block %d", len(spanIndices), blockID)
			}
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(spanIndices))); err != nil { //nolint:gosec
				return nil, err
			}

			// Write span_indices (2 bytes each)
			for _, spanIdx := range spanIndices {
				if err := binary.Write(buf, binary.LittleEndian, spanIdx); err != nil {
					return nil, err
				}
			}
		}
	}

	return buf, nil
}
