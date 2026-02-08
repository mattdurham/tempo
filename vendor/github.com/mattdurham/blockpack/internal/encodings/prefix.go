package encodings

import (
	"bytes"
	"encoding/binary"
	"github.com/klauspost/compress/zstd"
	"strings"
)

// BuildPrefixBytes encodes a bytes column using prefix compression.
// Prefix compression identifies common prefixes and stores them in a dictionary,
// then stores each value as a prefix index plus suffix.
// This is efficient for URL/path columns with common prefixes.
func BuildPrefixBytes(
	encoder *zstd.Encoder,
	buf *bytes.Buffer,
	spanCount, _ int,
	useSparse bool,
	presenceRLE []byte,
	bytesValues [][]byte,
	present []byte,
) error {
	// Prefix compression for URL/path columns
	encodingKind := encodingKindPrefixBytes
	if useSparse {
		encodingKind = encodingKindSparsePrefixBytes
	}

	_ = buf.WriteByte(encodingKind)
	_ = binary.Write(buf, binary.LittleEndian, uint32(spanCount))
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(presenceRLE)))
	_, _ = buf.Write(presenceRLE)

	// Find common prefixes by analyzing all values
	prefixCounts := make(map[string]int)
	const minPrefixLen = 20 // Minimum prefix length to consider

	// Count prefix occurrences
	for i := 0; i < spanCount; i++ {
		if !isBitSet(present, i) {
			continue
		}
		value := string(bytesValues[i])
		if len(value) > minPrefixLen {
			// Try different prefix lengths and pick the one that repeats most
			for prefixLen := minPrefixLen; prefixLen <= len(value) && prefixLen <= 50; prefixLen++ {
				prefix := value[:prefixLen]
				prefixCounts[prefix]++
			}
		}
	}

	// Build prefix dictionary from most common prefixes
	// Only include prefixes used by at least 2 values
	prefixList := make([]string, 0)
	prefixMap := make(map[string]uint32)
	for prefix, count := range prefixCounts {
		if count >= 2 {
			prefixMap[prefix] = uint32(len(prefixList))
			prefixList = append(prefixList, prefix)
		}
	}

	// Encode prefix dictionary
	var prefixDictBuf bytes.Buffer
	_ = binary.Write(&prefixDictBuf, binary.LittleEndian, uint32(len(prefixList)))
	for _, prefix := range prefixList {
		_ = binary.Write(&prefixDictBuf, binary.LittleEndian, uint32(len(prefix)))
		_, _ = prefixDictBuf.Write([]byte(prefix))
	}
	compressedPrefixDict := CompressZstd(prefixDictBuf.Bytes(), encoder)

	// Encode suffix data with prefix indexes
	var suffixBuf bytes.Buffer
	prefixIndexWidth := ChooseIndexWidth(len(prefixList))
	_ = suffixBuf.WriteByte(prefixIndexWidth)

	for i := 0; i < spanCount; i++ {
		if !isBitSet(present, i) {
			continue
		}
		value := string(bytesValues[i])

		// Find longest matching prefix
		bestPrefixIdx := uint32(0xFFFFFFFF) // No prefix marker
		bestPrefixLen := 0
		for prefix, idx := range prefixMap {
			if len(prefix) > bestPrefixLen && strings.HasPrefix(value, prefix) {
				bestPrefixIdx = idx
				bestPrefixLen = len(prefix)
			}
		}

		// Write prefix index
		_ = WriteFixedWidth(&suffixBuf, bestPrefixIdx, prefixIndexWidth)

		// Write suffix
		var suffix string
		if bestPrefixIdx != 0xFFFFFFFF {
			suffix = value[bestPrefixLen:]
		} else {
			suffix = value
		}
		_ = binary.Write(&suffixBuf, binary.LittleEndian, uint32(len(suffix)))
		_, _ = suffixBuf.Write([]byte(suffix))
	}

	// Compress suffix data
	compressedSuffixes := CompressZstd(suffixBuf.Bytes(), encoder)

	// Write compressed data
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedPrefixDict)))
	_, _ = buf.Write(compressedPrefixDict)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(compressedSuffixes)))
	_, _ = buf.Write(compressedSuffixes)

	return nil
}
