// Package encodings provides blockpack encoding implementations for the ondisk format.
// This package contains encoding-specific logic separated from column type semantics.
//
// Each encoding file implements a specific compression strategy:
//   - dictionary.go: Dictionary encoding for all column types
//   - delta.go: Delta encoding for uint64 (timestamps, counters)
//   - xor.go: XOR encoding for bytes (IDs, hashes)
//   - prefix.go: Prefix compression for bytes (URLs, paths)
//   - inline.go: Inline bytes storage for small/unique values
//
// Encoding decisions are made by the parent ondisk package based on column names
// and data patterns. This package focuses purely on the encoding/compression algorithms.
package encodings

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ChooseIndexWidth returns the minimum byte width needed to store dictionary indexes.
// Returns 1, 2, or 4 bytes based on dictionary size.
func ChooseIndexWidth(dictLen int) uint8 {
	if dictLen <= 0xFF {
		return 1
	}
	if dictLen <= 0xFFFF {
		return 2
	}
	return 4
}

// WriteFixedWidth writes a uint32 value using the specified byte width (1, 2, or 4 bytes).
func WriteFixedWidth(buf *bytes.Buffer, val uint32, width uint8) error {
	switch width {
	case 1:
		return buf.WriteByte(byte(val))
	case 2:
		return binary.Write(buf, binary.LittleEndian, uint16(val))
	case 4:
		return binary.Write(buf, binary.LittleEndian, val)
	default:
		return fmt.Errorf("unsupported width %d", width)
	}
}

// CountPresentBits counts the number of set bits in the first 'rows' bits of the bitmap.
func CountPresentBits(bits []byte, rows int) int {
	count := 0
	for i := 0; i < rows; i++ {
		if isBitSet(bits, i) {
			count++
		}
	}
	return count
}

// isBitSet checks if the bit at position idx is set in the bitmap.
func isBitSet(bits []byte, idx int) bool {
	byteIdx := idx / 8
	bitIdx := idx % 8
	return bits[byteIdx]&(1<<bitIdx) != 0
}

// EncodePresenceRLE compresses presence bits into alternating runs of false/true values.
// It is used by column encodings to avoid storing one bit per row when nulls are clustered.
func EncodePresenceRLE(bits []byte, rows int) []byte {
	const presenceRLEVersion = uint8(1)

	var buf bytes.Buffer
	runCount := uint32(0)
	last := false
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if i == 0 || val != last {
			runCount++
		}
		last = val
	}
	_ = buf.WriteByte(presenceRLEVersion)
	_ = binary.Write(&buf, binary.LittleEndian, runCount)

	currentVal := isBitSet(bits, 0)
	currentLen := uint32(0)
	for i := 0; i < rows; i++ {
		val := isBitSet(bits, i)
		if val == currentVal {
			currentLen++
			continue
		}
		_ = binary.Write(&buf, binary.LittleEndian, currentLen)
		_ = buf.WriteByte(boolToByte(currentVal))
		currentVal = val
		currentLen = 1
	}
	_ = binary.Write(&buf, binary.LittleEndian, currentLen)
	_ = buf.WriteByte(boolToByte(currentVal))
	return buf.Bytes()
}

// EncodeIndexRLE compresses uint32 indexes into runs of the same value.
// Used when dictionary indices are larger than the dictionary itself.
// Format: version(1) + runCount(4) + [length(4) + value(4)]...
func EncodeIndexRLE(indexes []uint32) []byte {
	if len(indexes) == 0 {
		var buf bytes.Buffer
		_ = buf.WriteByte(1) // version
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		return buf.Bytes()
	}

	var buf bytes.Buffer
	runCount := uint32(0)
	last := indexes[0]
	for i := 1; i < len(indexes); i++ {
		if indexes[i] != last {
			runCount++
			last = indexes[i]
		}
	}
	runCount++ // count the final run

	_ = buf.WriteByte(1) // version
	_ = binary.Write(&buf, binary.LittleEndian, runCount)

	currentVal := indexes[0]
	currentLen := uint32(1)
	for i := 1; i < len(indexes); i++ {
		if indexes[i] == currentVal {
			currentLen++
			continue
		}
		_ = binary.Write(&buf, binary.LittleEndian, currentLen)
		_ = binary.Write(&buf, binary.LittleEndian, currentVal)
		currentVal = indexes[i]
		currentLen = 1
	}
	_ = binary.Write(&buf, binary.LittleEndian, currentLen)
	_ = binary.Write(&buf, binary.LittleEndian, currentVal)
	return buf.Bytes()
}

// boolToByte converts a bool to a byte (0 or 1).
func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// ChooseBytesEncodingKind selects the best encoding (dictionary vs inline) for bytes columns.
// It compares the compressed size of dictionary encoding vs inline encoding and returns
// the encoding kind that produces the smallest output.
func ChooseBytesEncodingKind(spanCount, presentCount int, useSparse bool, presenceRLE []byte, width uint8, compressedDict []byte, dictVals [][]byte, indexes []uint32, present []byte) (uint8, error) {
	// Inline bytes encoding stores each value with its length, so size scales with payload bytes.
	// Dictionary encoding stores a compressed dictionary plus fixed-width indexes for each row.
	inlineSize := 4 + 4 + len(presenceRLE)
	if useSparse {
		// Sparse layout writes only present indexes, plus the count of present rows.
		inlineSize += 4
		inlineSize += presentCount * 4
	} else {
		// Dense layout writes an index for every row.
		inlineSize += spanCount * 4
	}
	for i := 0; i < spanCount; i++ {
		if !isBitSet(present, i) {
			continue
		}
		idx := indexes[i]
		if int(idx) >= len(dictVals) {
			return 0, fmt.Errorf("bytes dictionary index %d out of range %d", idx, len(dictVals))
		}
		// Inline encoding embeds the payload bytes directly.
		inlineSize += len(dictVals[idx])
	}

	// Dictionary encoding cost: dict header + compressed bytes + presence bits + indexes.
	dictSize := 1 + 4 + len(compressedDict) + 4 + 4 + len(presenceRLE)
	if useSparse {
		// Sparse dictionary writes present indexes only.
		dictSize += 4
		dictSize += presentCount * int(width)
	} else {
		// Dense dictionary writes an index per row.
		dictSize += spanCount * int(width)
	}

	// Pick the smallest representation, preserving sparse vs dense when necessary.
	if inlineSize < dictSize {
		if useSparse {
			return encodingKindSparseInlineBytes, nil
		}
		return encodingKindInlineBytes, nil
	}
	if useSparse {
		return encodingKindSparseDictionary, nil
	}
	return encodingKindDictionary, nil
}

// Encoding kind constants (must match parent package ondisk.types.go)
const (
	encodingKindDictionary            uint8 = 1  // Dictionary + indexes (default)
	encodingKindSparseDictionary      uint8 = 2  // Dictionary + sparse indexes (>50% nulls)
	encodingKindInlineBytes           uint8 = 3  // Inline values (bytes columns)
	encodingKindSparseInlineBytes     uint8 = 4  // Inline + sparse (bytes + >50% nulls)
	encodingKindDeltaUint64           uint8 = 5  // Delta encoding for uint64 (timestamps, monotonic data)
	encodingKindRLEIndexes            uint8 = 6  // Dictionary + RLE-compressed indexes (low cardinality)
	encodingKindSparseRLEIndexes      uint8 = 7  // Dictionary + RLE-compressed sparse indexes
	encodingKindXORBytes              uint8 = 8  // XOR encoding for bytes (IDs with common patterns)
	encodingKindSparseXORBytes        uint8 = 9  // XOR encoding + sparse (bytes + >50% nulls)
	encodingKindPrefixBytes           uint8 = 10 // Prefix compression for bytes (URLs, paths with common prefixes)
	encodingKindSparsePrefixBytes     uint8 = 11 // Prefix compression + sparse (bytes + >50% nulls)
	encodingKindDeltaDictionary       uint8 = 12 // Dictionary + delta-encoded indexes (trace IDs with locality)
	encodingKindSparseDeltaDictionary uint8 = 13 // Dictionary + delta-encoded sparse indexes
)
