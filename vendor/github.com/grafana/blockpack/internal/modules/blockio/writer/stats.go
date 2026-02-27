package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"math"
)

// stringStats accumulates min/max statistics for string column values.
type stringStats struct {
	min, max  string
	hasValues bool
}

// int64Stats accumulates min/max statistics for int64 column values.
type int64Stats struct {
	min, max  int64
	hasValues bool
}

// uint64Stats accumulates min/max statistics for uint64 column values.
type uint64Stats struct {
	min, max  uint64
	hasValues bool
}

// float64Stats accumulates min/max statistics for float64 column values.
type float64Stats struct {
	min, max  float64
	hasValues bool
}

// bytesStats accumulates min/max statistics for bytes column values.
type bytesStats struct {
	min, max  []byte
	hasValues bool
}

// boolStats accumulates min/max statistics for bool column values.
// Ordering: false < true.
type boolStats struct {
	min, max  bool
	hasValues bool
}

// accumulateStringStats updates s with val, expanding the [min, max] range as needed.
func accumulateStringStats(s *stringStats, val string) {
	if !s.hasValues {
		s.min = val
		s.max = val
		s.hasValues = true

		return
	}

	if val < s.min {
		s.min = val
	}

	if val > s.max {
		s.max = val
	}
}

// accumulateInt64Stats updates s with val, expanding the [min, max] range as needed.
func accumulateInt64Stats(s *int64Stats, val int64) {
	if !s.hasValues {
		s.min = val
		s.max = val
		s.hasValues = true

		return
	}

	if val < s.min {
		s.min = val
	}

	if val > s.max {
		s.max = val
	}
}

// accumulateUint64Stats updates s with val, expanding the [min, max] range as needed.
func accumulateUint64Stats(s *uint64Stats, val uint64) {
	if !s.hasValues {
		s.min = val
		s.max = val
		s.hasValues = true

		return
	}

	if val < s.min {
		s.min = val
	}

	if val > s.max {
		s.max = val
	}
}

// accumulateFloat64Stats updates s with val, expanding the [min, max] range as needed.
func accumulateFloat64Stats(s *float64Stats, val float64) {
	if !s.hasValues {
		s.min = val
		s.max = val
		s.hasValues = true

		return
	}

	if val < s.min {
		s.min = val
	}

	if val > s.max {
		s.max = val
	}
}

// accumulateBytesStats updates s with val, expanding the [min, max] range as needed.
// Comparison uses bytes.Compare for lexicographic ordering.
func accumulateBytesStats(s *bytesStats, val []byte) {
	if !s.hasValues {
		s.min = append([]byte(nil), val...)
		s.max = append([]byte(nil), val...)
		s.hasValues = true

		return
	}

	if bytes.Compare(val, s.min) < 0 {
		s.min = append([]byte(nil), val...)
	}

	if bytes.Compare(val, s.max) > 0 {
		s.max = append([]byte(nil), val...)
	}
}

// accumulateBoolStats updates s with val. false < true.
func accumulateBoolStats(s *boolStats, val bool) {
	if !s.hasValues {
		s.min = val
		s.max = val
		s.hasValues = true

		return
	}

	// false < true: min is false if any false seen, max is true if any true seen
	if !val {
		s.min = false
	}

	if val {
		s.max = true
	}
}

// encodeStringStats serializes string min/max stats to the column stats wire format.
// Format: has_values[1] (+ min_len[4] + min + max_len[4] + max if has_values).
func encodeStringStats(s stringStats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	buf := make([]byte, 0, 1+4+len(s.min)+4+len(s.max))
	buf = append(buf, 1)
	buf = appendUint32LE(buf, uint32(len(s.min))) //nolint:gosec // safe: string length bounded by MaxStringLen
	buf = append(buf, s.min...)
	buf = appendUint32LE(buf, uint32(len(s.max))) //nolint:gosec // safe: string length bounded by MaxStringLen
	buf = append(buf, s.max...)

	return buf
}

// encodeInt64Stats serializes int64 min/max stats to the column stats wire format.
// Format: has_values[1] (+ min[8 LE signed] + max[8 LE signed] if has_values).
func encodeInt64Stats(s int64Stats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	buf := make([]byte, 1+8+8)
	buf[0] = 1
	binary.LittleEndian.PutUint64(buf[1:], uint64(s.min)) //nolint:gosec // safe: serializing int64 bits
	binary.LittleEndian.PutUint64(buf[9:], uint64(s.max)) //nolint:gosec // safe: serializing int64 bits

	return buf
}

// encodeUint64Stats serializes uint64 min/max stats to the column stats wire format.
// Format: has_values[1] (+ min[8 LE] + max[8 LE] if has_values).
func encodeUint64Stats(s uint64Stats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	buf := make([]byte, 1+8+8)
	buf[0] = 1
	binary.LittleEndian.PutUint64(buf[1:], s.min)
	binary.LittleEndian.PutUint64(buf[9:], s.max)

	return buf
}

// encodeFloat64Stats serializes float64 min/max stats to the column stats wire format.
// Format: has_values[1] (+ min_bits[8 LE IEEE 754] + max_bits[8 LE IEEE 754] if has_values).
func encodeFloat64Stats(s float64Stats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	buf := make([]byte, 1+8+8)
	buf[0] = 1
	binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(s.min))
	binary.LittleEndian.PutUint64(buf[9:], math.Float64bits(s.max))

	return buf
}

// encodeBytesStats serializes bytes min/max stats to the column stats wire format.
// Format: has_values[1] (+ min_len[4] + min + max_len[4] + max if has_values).
func encodeBytesStats(s bytesStats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	buf := make([]byte, 0, 1+4+len(s.min)+4+len(s.max))
	buf = append(buf, 1)
	buf = appendUint32LE(buf, uint32(len(s.min))) //nolint:gosec // safe: string length bounded by MaxStringLen
	buf = append(buf, s.min...)
	buf = appendUint32LE(buf, uint32(len(s.max))) //nolint:gosec // safe: string length bounded by MaxStringLen
	buf = append(buf, s.max...)

	return buf
}

// encodeBoolStats serializes bool min/max stats to the column stats wire format.
// Format: has_values[1] (+ min[1] + max[1] if has_values). 0=false, 1=true.
func encodeBoolStats(s boolStats) []byte {
	if !s.hasValues {
		return []byte{0}
	}

	minByte := byte(0)
	if s.min {
		minByte = 1
	}

	maxByte := byte(0)
	if s.max {
		maxByte = 1
	}

	return []byte{1, minByte, maxByte}
}

// appendUint32LE appends a uint32 in little-endian byte order to buf.
func appendUint32LE(buf []byte, v uint32) []byte {
	return append(buf,
		byte(v),     //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>8),  //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>16), //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>24), //nolint:gosec // safe: truncating uint32 bytes for LE encoding
	)
}
