package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// kllSectionHeaderSize: magic[4]+version[1]+col_type[1]+reserved[2]+boundary_count[4] = 12 bytes.
const kllSectionHeaderSize = 12

// kllDefaultBuckets is the number of histogram buckets requested when serializing the KLL sketch.
const kllDefaultBuckets = 200

// EncodeKLLSection serializes a KLL[T] sketch into the VKLL wire format.
// Boundaries are stored as length-prefixed canonical byte slices.
func EncodeKLLSection[T cmp.Ordered](sk *writer.KLL[T], colType shared.ColumnType) ([]byte, error) {
	bounds := sk.Boundaries(kllDefaultBuckets)
	rawBounds := encodeOrderedBounds(bounds)
	return encodeKLLBoundsBytes(colType, rawBounds)
}

// encodeOrderedBounds converts a slice of cmp.Ordered values to [][]byte.
func encodeOrderedBounds[T cmp.Ordered](bounds []T) [][]byte {
	if len(bounds) == 0 {
		return nil
	}
	out := make([][]byte, len(bounds))
	for i, v := range bounds {
		out[i] = encodeOrderedValue(v)
	}
	return out
}

// encodeOrderedValue encodes one ordered value to bytes.
func encodeOrderedValue[T cmp.Ordered](v T) []byte {
	switch a := any(v).(type) {
	case string:
		return []byte(a)
	case int64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(a)) //nolint:gosec // intentional two's complement
		return b
	case uint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, a)
		return b
	case float64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(a))
		return b
	case int:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(a)) //nolint:gosec
		return b
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// encodeKLLBoundsBytes encodes pre-computed boundary byte slices into VKLL wire format.
func encodeKLLBoundsBytes(colType shared.ColumnType, bounds [][]byte) ([]byte, error) {
	totalBoundBytes := 0
	for _, b := range bounds {
		totalBoundBytes += 2 + len(b) // len[2 LE] + bytes
	}
	size := kllSectionHeaderSize + totalBoundBytes
	out := make([]byte, size)

	binary.LittleEndian.PutUint32(out[0:], shared.ValueIndexKLLMagic)
	out[4] = shared.ValueIndexKLLVersion
	out[5] = byte(colType)
	// out[6:8] reserved — zero from make
	binary.LittleEndian.PutUint32(out[8:], uint32(len(bounds))) //nolint:gosec
	pos := kllSectionHeaderSize
	for _, b := range bounds {
		binary.LittleEndian.PutUint16(out[pos:], uint16(len(b))) //nolint:gosec
		pos += 2
		copy(out[pos:], b)
		pos += len(b)
	}
	return out, nil
}

// DecodeKLLStringBounds decodes a VKLL section for a string column and returns boundary strings.
func DecodeKLLStringBounds(b []byte, _ int) ([]string, error) {
	raw, err := decodeKLLRaw(b)
	if err != nil {
		return nil, err
	}
	bounds := make([]string, len(raw))
	for i, r := range raw {
		bounds[i] = string(r)
	}
	return bounds, nil
}

// DecodeKLLUint64Bounds decodes a VKLL section for a uint64 column and returns boundary values.
func DecodeKLLUint64Bounds(b []byte, _ int) ([]uint64, error) {
	raw, err := decodeKLLRaw(b)
	if err != nil {
		return nil, err
	}
	bounds := make([]uint64, 0, len(raw))
	for _, r := range raw {
		if len(r) >= 8 {
			bounds = append(bounds, binary.LittleEndian.Uint64(r[:8]))
		}
	}
	return bounds, nil
}

// decodeKLLRaw parses the VKLL header and returns the raw boundary byte slices.
func decodeKLLRaw(b []byte) ([][]byte, error) {
	if len(b) < kllSectionHeaderSize {
		return nil, fmt.Errorf("valueindex: VKLL blob too short (%d bytes)", len(b))
	}
	if magic := binary.LittleEndian.Uint32(b[:4]); magic != shared.ValueIndexKLLMagic {
		return nil, fmt.Errorf("valueindex: VKLL wrong magic 0x%08X", magic)
	}
	if ver := b[4]; ver != shared.ValueIndexKLLVersion {
		return nil, fmt.Errorf("valueindex: VKLL unsupported version %d", ver)
	}
	count := int(binary.LittleEndian.Uint32(b[8:12]))
	if count == 0 {
		return nil, nil
	}
	pos := kllSectionHeaderSize
	bounds := make([][]byte, 0, count)
	for i := range count {
		if pos+2 > len(b) {
			return nil, fmt.Errorf("valueindex: VKLL boundary %d: truncated at len", i)
		}
		bLen := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if pos+bLen > len(b) {
			return nil, fmt.Errorf("valueindex: VKLL boundary %d: truncated at value", i)
		}
		val := make([]byte, bLen)
		copy(val, b[pos:pos+bLen])
		bounds = append(bounds, val)
		pos += bLen
	}
	return bounds, nil
}
