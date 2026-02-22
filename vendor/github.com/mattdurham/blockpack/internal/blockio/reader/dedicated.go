package reader

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/mattdurham/blockpack/internal/arena"
	aslice "github.com/mattdurham/blockpack/internal/arena/slice"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
	ondisk "github.com/mattdurham/blockpack/internal/types"
)

// ensureDedicatedColumnParsed parses a specific dedicated column index on-demand if not already parsed
// The arena parameter is optional - if nil, uses heap allocation
func (r *Reader) ensureDedicatedColumnParsed(column string, arena *arena.Arena) error {
	// Already parsed?
	if _, ok := r.dedicatedIndex[column]; ok {
		return nil
	}

	// Get offset metadata
	meta, ok := r.dedicatedIndexOffsets[column]
	if !ok {
		return nil // Column doesn't exist - not an error
	}

	// Parse just this one column
	idx, bucketMeta, err := parseSingleDedicatedColumn(r.metadataBytes, meta.offset, meta.typ, arena)
	if err != nil {
		return fmt.Errorf("parse dedicated column %s: %w", column, err)
	}

	r.dedicatedIndex[column] = idx
	if bucketMeta != nil {
		r.rangeBucketMeta[column] = bucketMeta
	}
	return nil
}

// parseSingleDedicatedColumn parses one dedicated column's index from metadata
// The arena parameter is optional - if nil, uses heap allocation
func parseSingleDedicatedColumn(
	data []byte,
	offset int,
	typ ColumnType,
	arena *arena.Arena,
) (*dedicatedColumnIndex, *shared.RangeBucketMetadata, error) {
	// Skip column name (already read in scan phase)
	if offset+2 > len(data) {
		return nil, nil, fmt.Errorf("truncated name length")
	}
	nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2 + nameLen + 1 // name + type byte

	// Parse bucket metadata if present (for all range column types)
	if offset >= len(data) {
		return nil, nil, fmt.Errorf("truncated bucket flag")
	}
	hasBuckets := data[offset]
	offset++

	var bucketMeta *shared.RangeBucketMetadata
	if hasBuckets == 1 && IsRangeColumnType(typ) {
		if offset+8+8+4 > len(data) {
			return nil, nil, fmt.Errorf("truncated bucket header")
		}
		min := int64(binary.LittleEndian.Uint64(data[offset:])) //nolint:gosec,revive
		offset += 8
		max := int64(binary.LittleEndian.Uint64(data[offset:])) //nolint:gosec,revive
		offset += 8
		boundaryCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		const maxBoundaries = 65536
		if boundaryCount > maxBoundaries {
			return nil, nil, fmt.Errorf("boundary count %d exceeds max %d", boundaryCount, maxBoundaries)
		}
		if offset+boundaryCount*8 > len(data) {
			return nil, nil, fmt.Errorf("truncated bucket boundaries")
		}
		var boundaries []int64
		if arena == nil {
			boundaries = make([]int64, boundaryCount)
		} else {
			boundaries = aslice.Make[int64](arena, boundaryCount).Raw()
		}
		for i := 0; i < boundaryCount; i++ {
			boundaries[i] = int64(binary.LittleEndian.Uint64(data[offset:])) //nolint:gosec
			offset += 8
		}
		bucketMeta = &shared.RangeBucketMetadata{
			Min:        min,
			Max:        max,
			Boundaries: boundaries,
			ColumnType: typ,
		}

		// Parse typed boundaries
		if offset+4 > len(data) {
			return nil, nil, fmt.Errorf("truncated typed boundary count")
		}
		typedCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		const maxTypedBoundaries = 65536
		if typedCount > maxTypedBoundaries {
			return nil, nil, fmt.Errorf("typed boundary count %d exceeds max %d", typedCount, maxTypedBoundaries)
		}
		if typedCount > 0 {
			switch typ {
			case ondisk.ColumnTypeRangeFloat64:
				if offset+typedCount*8 > len(data) {
					return nil, nil, fmt.Errorf("truncated float64 typed boundaries")
				}
				f64Boundaries := make([]float64, typedCount)
				for i := 0; i < typedCount; i++ {
					bits := binary.LittleEndian.Uint64(data[offset:])
					f64Boundaries[i] = math.Float64frombits(bits)
					offset += 8
				}
				bucketMeta.Float64Boundaries = f64Boundaries
			case ondisk.ColumnTypeRangeString:
				const maxBoundaryLen = 1024 * 1024 // 1 MiB per boundary element
				strBoundaries := make([]string, typedCount)
				for i := 0; i < typedCount; i++ {
					if offset+4 > len(data) {
						return nil, nil, fmt.Errorf("truncated string boundary length at %d", i)
					}
					sLen := int(binary.LittleEndian.Uint32(data[offset:]))
					offset += 4
					if sLen > maxBoundaryLen {
						return nil, nil, fmt.Errorf(
							"string boundary length %d exceeds max %d at %d",
							sLen,
							maxBoundaryLen,
							i,
						)
					}
					if offset+sLen > len(data) {
						return nil, nil, fmt.Errorf("truncated string boundary data at %d", i)
					}
					strBoundaries[i] = string(data[offset : offset+sLen])
					offset += sLen
				}
				bucketMeta.StringBoundaries = strBoundaries
			case ondisk.ColumnTypeRangeBytes:
				const maxBoundaryLenBytes = 1024 * 1024 // 1 MiB per boundary element
				bytesBoundaries := make([][]byte, typedCount)
				for i := 0; i < typedCount; i++ {
					if offset+4 > len(data) {
						return nil, nil, fmt.Errorf("truncated bytes boundary length at %d", i)
					}
					bLen := int(binary.LittleEndian.Uint32(data[offset:]))
					offset += 4
					if bLen > maxBoundaryLenBytes {
						return nil, nil, fmt.Errorf(
							"bytes boundary length %d exceeds max %d at %d",
							bLen,
							maxBoundaryLenBytes,
							i,
						)
					}
					if offset+bLen > len(data) {
						return nil, nil, fmt.Errorf("truncated bytes boundary data at %d", i)
					}
					cp := make([]byte, bLen)
					copy(cp, data[offset:offset+bLen])
					bytesBoundaries[i] = cp
					offset += bLen
				}
				bucketMeta.BytesBoundaries = bytesBoundaries
			default:
				// RangeInt64/Uint64/Duration: typedCount should be 0
			}
		}
	}

	// Parse values
	if offset+4 > len(data) {
		return nil, nil, fmt.Errorf("truncated value count")
	}
	valueCount := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	// Cap the map pre-allocation hint. valueCount comes from an untrusted uint32 in the file,
	// so a crafted file could set it to ~4 billion. make(map, n) allocates bucket arrays
	// proportional to n upfront — before the loop's bounds checks have a chance to reject
	// the file. In practice valueCount is at most ~100 (one entry per KLL bucket).
	hint := int(valueCount) //nolint:unconvert
	if hint > 1_000_000 {
		hint = 1_000_000
	}
	values := make(map[string][]int, hint)
	for j := 0; j < valueCount; j++ {
		key, nextOffset, err := readDedicatedValue(typ, data, offset)
		if err != nil {
			return nil, nil, fmt.Errorf("value %d: %w", j, err)
		}
		offset = nextOffset

		if offset+4 > len(data) {
			return nil, nil, fmt.Errorf("truncated block id count")
		}
		idCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		const maxIDCount = 1_000_000
		if idCount > maxIDCount {
			return nil, nil, fmt.Errorf("block ID count %d exceeds max %d", idCount, maxIDCount)
		}
		if offset+idCount*4 > len(data) {
			return nil, nil, fmt.Errorf("truncated block ids")
		}
		var ids []int
		if arena == nil {
			ids = make([]int, idCount)
		} else {
			ids = aslice.Make[int](arena, idCount).Raw()
		}
		for k := 0; k < idCount; k++ {
			ids[k] = int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
		}
		values[key.Encode()] = ids
	}

	return &dedicatedColumnIndex{
		typ:    typ,
		values: values,
	}, bucketMeta, nil
}

// readDedicatedValue reads a single dedicated value key from the metadata
func readDedicatedValue(typ ColumnType, data []byte, offset int) (DedicatedValueKey, int, error) {
	switch typ {
	case ColumnTypeString:
		if offset+4 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated string length")
		}
		l := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+l > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated string value")
		}
		val := string(data[offset : offset+l])
		offset += l
		return StringValueKey(val), offset, nil
	case ColumnTypeBytes:
		if offset+4 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bytes length")
		}
		l := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+l > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bytes value")
		}
		cp := make([]byte, l)
		copy(cp, data[offset:offset+l])
		offset += l
		return BytesValueKey(cp), offset, nil
	case ColumnTypeInt64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated int64 value")
		}
		v := int64(binary.LittleEndian.Uint64(data[offset:])) //nolint:gosec
		return IntValueKey(v), offset + 8, nil
	case ColumnTypeUint64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated uint64 value")
		}
		v := binary.LittleEndian.Uint64(data[offset:])
		return UintValueKey(v), offset + 8, nil
	case ColumnTypeFloat64:
		if offset+8 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated float64 value")
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		return FloatValueKey(v), offset + 8, nil
	case ColumnTypeBool:
		if offset+1 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated bool value")
		}
		return BoolValueKey(data[offset] != 0), offset + 1, nil
	case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration, ColumnTypeRangeFloat64:
		// Numeric range columns can be either:
		// - 2 bytes: bucket ID (uint16) for bucketed columns
		// - 8 bytes: raw value for direct value indexing (low cardinality)
		if offset+1 > len(data) {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated range key length")
		}
		length := int(data[offset])
		offset++

		switch length {
		case 2:
			// Bucket ID
			if offset+2 > len(data) {
				return DedicatedValueKey{}, 0, fmt.Errorf("truncated range bucket id")
			}
			bucketID := binary.LittleEndian.Uint16(data[offset:])
			return RangeBucketValueKey(bucketID, typ), offset + 2, nil
		case 8:
			// Raw value (direct indexing)
			if offset+8 > len(data) {
				return DedicatedValueKey{}, 0, fmt.Errorf("truncated range value")
			}
			value := int64(binary.LittleEndian.Uint64(data[offset:])) //nolint:gosec
			// Use type-specific key constructor
			switch typ {
			case ondisk.ColumnTypeRangeFloat64:
				floatVal := math.Float64frombits(uint64(value)) //nolint:gosec
				return ondisk.RangeFloat64ValueKey(floatVal, typ), offset + 8, nil
			default:
				return ondisk.RangeInt64ValueKey(value, typ), offset + 8, nil
			}
		default:
			return DedicatedValueKey{}, 0, fmt.Errorf("invalid numeric range key length %d", length)
		}
	case ColumnTypeRangeBytes, ColumnTypeRangeString:
		// Variable-length range columns can be:
		// - 2 bytes: bucket ID (uint16) for bucketed columns (encoded with varint but will be 1 byte for value 2)
		// - Variable length: raw value for direct value indexing (low cardinality)
		// Read length as varint
		length64, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return DedicatedValueKey{}, 0, fmt.Errorf("truncated range key length varint")
		}
		offset += n
		length := int(length64) //nolint:gosec

		if length == 2 {
			// Bucket ID
			if offset+2 > len(data) {
				return ondisk.DedicatedValueKey{}, 0, fmt.Errorf("truncated range bucket id")
			}
			bucketID := binary.LittleEndian.Uint16(data[offset:])
			return ondisk.RangeBucketValueKey(bucketID, typ), offset + 2, nil
		}

		// Raw value (direct indexing)
		if offset+length > len(data) {
			return ondisk.DedicatedValueKey{}, 0, fmt.Errorf("truncated range value (need %d bytes)", length)
		}
		switch typ {
		case ondisk.ColumnTypeRangeString:
			value := string(data[offset : offset+length])
			return ondisk.RangeStringValueKey(value, typ), offset + length, nil
		case ondisk.ColumnTypeRangeBytes:
			valueCopy := make([]byte, length)
			copy(valueCopy, data[offset:offset+length])
			return ondisk.RangeBytesValueKey(valueCopy, typ), offset + length, nil
		default:
			return ondisk.DedicatedValueKey{}, 0, fmt.Errorf("unexpected type %v in bytes/string range handler", typ)
		}
	default:
		return DedicatedValueKey{}, 0, fmt.Errorf("unsupported dedicated type %d", typ)
	}
}
