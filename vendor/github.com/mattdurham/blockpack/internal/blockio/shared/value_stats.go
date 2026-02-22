package shared

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// WriteValueStats writes value statistics to buffer
// This function is shared between writer (blockio) and reader packages
func WriteValueStats(buf *bytes.Buffer, stats map[string]AttributeStats) error {
	count := uint16(len(stats)) //nolint:gosec
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("write stats count: %w", err)
	}

	if count == 0 {
		return nil
	}

	for name, stat := range stats {
		// Write attribute name
		nameLen := uint16(len(name)) //nolint:gosec
		if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
			return fmt.Errorf("write name length: %w", err)
		}
		if _, err := buf.WriteString(name); err != nil {
			return fmt.Errorf("write name: %w", err)
		}

		// Write statistics
		if err := stat.WriteTo(buf); err != nil {
			return fmt.Errorf("write stats for %s: %w", name, err)
		}
	}

	return nil
}

// ReadValueStats reads value statistics from reader
// This function is shared between writer (blockio) and reader packages
func ReadValueStats(rd *bytes.Reader) (map[string]AttributeStats, error) {
	var count uint16
	if err := binary.Read(rd, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read stats count: %w", err)
	}

	if count == 0 {
		return nil, nil
	}

	stats := make(map[string]AttributeStats, count)

	for i := uint16(0); i < count; i++ {
		// Read attribute name
		var nameLen uint16
		if err := binary.Read(rd, binary.LittleEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("read name length: %w", err)
		}

		nameBytes := make([]byte, nameLen)
		if _, err := rd.Read(nameBytes); err != nil {
			return nil, fmt.Errorf("read name: %w", err)
		}
		name := string(nameBytes)

		// Read statistics
		stat, err := ReadAttributeStats(rd)
		if err != nil {
			return nil, fmt.Errorf("read stats for %s: %w", name, err)
		}

		stats[name] = stat
	}

	return stats, nil
}

// WriteTo serializes AttributeStats to a buffer
func (s *AttributeStats) WriteTo(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.LittleEndian, s.Type); err != nil {
		return fmt.Errorf("write stats type: %w", err)
	}
	if _, err := buf.Write(s.ValuesBloom[:]); err != nil {
		return fmt.Errorf("write values bloom: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MinInt); err != nil {
		return fmt.Errorf("write min int: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MaxInt); err != nil {
		return fmt.Errorf("write max int: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MinFloat); err != nil {
		return fmt.Errorf("write min float: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MaxFloat); err != nil {
		return fmt.Errorf("write max float: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, s.ApproxDistinctCount); err != nil {
		return fmt.Errorf("write distinct count: %w", err)
	}
	return nil
}

// ReadAttributeStats deserializes AttributeStats from a reader
func ReadAttributeStats(rd *bytes.Reader) (AttributeStats, error) {
	var stats AttributeStats

	if err := binary.Read(rd, binary.LittleEndian, &stats.Type); err != nil {
		return stats, fmt.Errorf("read stats type: %w", err)
	}
	if _, err := rd.Read(stats.ValuesBloom[:]); err != nil {
		return stats, fmt.Errorf("read values bloom: %w", err)
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MinInt); err != nil {
		return stats, fmt.Errorf("read min int: %w", err)
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MaxInt); err != nil {
		return stats, fmt.Errorf("read max int: %w", err)
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MinFloat); err != nil {
		return stats, fmt.Errorf("read min float: %w", err)
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MaxFloat); err != nil {
		return stats, fmt.Errorf("read max float: %w", err)
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.ApproxDistinctCount); err != nil {
		return stats, fmt.Errorf("read distinct count: %w", err)
	}

	return stats, nil
}

// MayContainValue checks if a value might be in the block based on bloom filter.
// This is a bloom filter check, so false positives are possible but false negatives are not.
func (s *AttributeStats) MayContainValue(value any) bool {
	if value == nil {
		return true // Can't determine, include block
	}

	// Check bloom filter
	bloom := &valueStatsBloom{}
	bloom.bits = s.ValuesBloom

	switch s.Type {
	case StatsTypeString:
		strVal, ok := value.(string)
		if !ok {
			return true
		}
		return bloom.mayContain([]byte(strVal))

	case StatsTypeInt64:
		var intVal int64
		switch v := value.(type) {
		case int64:
			intVal = v
		case int:
			intVal = int64(v)
		case int32:
			intVal = int64(v)
		default:
			return true
		}

		// Check range first (quick rejection)
		if intVal < s.MinInt || intVal > s.MaxInt {
			return false
		}

		// Check bloom filter
		buf := make([]byte, 8)
		for i := 0; i < 8; i++ {
			buf[i] = byte(intVal >> (8 * i))
		}
		return bloom.mayContain(buf)

	case StatsTypeFloat64:
		floatVal, ok := value.(float64)
		if !ok {
			return true
		}

		// Check range first (quick rejection)
		if floatVal < s.MinFloat || floatVal > s.MaxFloat {
			return false
		}

		// Check bloom filter
		//nolint:gosec
		bits := uint64(0)
		if floatVal != 0 {
			// Use binary representation for hashing
			//nolint:gosec
			bits = uint64(floatVal)
		}
		buf := make([]byte, 8)
		for i := 0; i < 8; i++ {
			buf[i] = byte(bits >> (8 * i))
		}
		return bloom.mayContain(buf)

	case StatsTypeBool:
		_, ok := value.(bool)
		if !ok {
			return true
		}
		// Booleans only have 2 values, bloom filter always contains them
		return true

	default:
		return true // Unknown type, include block
	}
}

// valueStatsBloom is a simple bloom filter for value statistics
type valueStatsBloom struct {
	bits [32]byte // 256 bits
}

// mayContain checks if a value might be in the set (can have false positives)
func (b *valueStatsBloom) mayContain(data []byte) bool {
	h1, h2 := b.hash(data)

	// Use double hashing to generate multiple hash functions
	// k=4 hash functions for good false positive rate
	for i := uint32(0); i < 4; i++ {
		pos := (h1 + i*h2) % 256
		byteIdx := pos / 8
		bitIdx := pos % 8
		if (b.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false // Definitely not in set
		}
	}

	return true // Might be in set
}

// hash computes two hash values for double hashing
// IMPORTANT: This must match the hash function used by the writer in blockio/value_stats.go
// The writer uses fnv.New64a(), so we use the same algorithm here
func (b *valueStatsBloom) hash(data []byte) (uint32, uint32) {
	// Use FNV-1a 64-bit hash (matching Go's fnv.New64a())
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	//nolint:gosec
	hash := uint64(offset64)
	for _, c := range data {
		//nolint:gosec
		hash ^= uint64(c)
		hash *= prime64
	}

	// Split 64-bit hash into two 32-bit hashes for double hashing
	h1 := uint32(hash & 0xFFFFFFFF) //nolint:gosec // Reviewed and acceptable
	h2 := uint32(hash >> 32)        //nolint:gosec // Reviewed and acceptable

	// Ensure h2 is never zero (important for double hashing)
	if h2 == 0 {
		h2 = 1
	}

	return h1, h2
}
