package ondiskio

import (
	"bytes"
	"encoding/binary"
	"math"
)

// columnStatsBuilder tracks column statistics during block construction.
type columnStatsBuilder struct {
	hasValue           bool
	stringMin          string
	stringMax          string
	intMin, intMax     int64
	uintMin, uintMax   uint64
	floatMin, floatMax float64
	bytesMin, bytesMax []byte
	boolMin, boolMax   bool
}

func (s *columnStatsBuilder) recordString(v string) {
	if !s.hasValue {
		s.stringMin = v
		s.stringMax = v
		s.hasValue = true
	} else {
		if v < s.stringMin {
			s.stringMin = v
		}
		if v > s.stringMax {
			s.stringMax = v
		}
	}
}

func (s *columnStatsBuilder) recordBytes(columnName string, v []byte) {
	if !s.hasValue {
		s.bytesMin = append([]byte(nil), v...)
		s.bytesMax = append([]byte(nil), v...)
		s.hasValue = true
	} else {
		if bytes.Compare(v, s.bytesMin) < 0 {
			s.bytesMin = append([]byte(nil), v...)
		}
		if bytes.Compare(v, s.bytesMax) > 0 {
			s.bytesMax = append([]byte(nil), v...)
		}
	}

	// Note: Previously added array elements to bloom filter for query optimization.
	// This is no longer needed since all columns have dedicated indexes.
}

func (s *columnStatsBuilder) recordInt(v int64) {
	if !s.hasValue {
		s.intMin = v
		s.intMax = v
		s.hasValue = true
	} else {
		if v < s.intMin {
			s.intMin = v
		}
		if v > s.intMax {
			s.intMax = v
		}
	}
}

func (s *columnStatsBuilder) recordUint(v uint64) {
	if !s.hasValue {
		s.uintMin = v
		s.uintMax = v
		s.hasValue = true
	} else {
		if v < s.uintMin {
			s.uintMin = v
		}
		if v > s.uintMax {
			s.uintMax = v
		}
	}
}

func (s *columnStatsBuilder) recordFloat(v float64) {
	if math.IsNaN(v) {
		return
	}
	if !s.hasValue {
		s.floatMin = v
		s.floatMax = v
		s.hasValue = true
	} else {
		if v < s.floatMin {
			s.floatMin = v
		}
		if v > s.floatMax {
			s.floatMax = v
		}
	}
}

func (s *columnStatsBuilder) recordBool(v bool) {
	if !s.hasValue {
		s.boolMin = v
		s.boolMax = v
		s.hasValue = true
	} else {
		if v {
			s.boolMax = true
		} else {
			s.boolMin = false
		}
	}
}

func encodeColumnStats(typ ColumnType, stats columnStatsBuilder) []byte {
	var buf bytes.Buffer
	has := byte(0)
	if stats.hasValue {
		has = 1
	}
	_ = buf.WriteByte(has)
	if !stats.hasValue {
		return buf.Bytes()
	}
	switch typ {
	case ColumnTypeString:
		writeSizedBytes(&buf, []byte(stats.stringMin))
		writeSizedBytes(&buf, []byte(stats.stringMax))
	case ColumnTypeBytes:
		writeSizedBytes(&buf, stats.bytesMin)
		writeSizedBytes(&buf, stats.bytesMax)
	case ColumnTypeInt64:
		_ = binary.Write(&buf, binary.LittleEndian, stats.intMin)
		_ = binary.Write(&buf, binary.LittleEndian, stats.intMax)
	case ColumnTypeUint64:
		_ = binary.Write(&buf, binary.LittleEndian, stats.uintMin)
		_ = binary.Write(&buf, binary.LittleEndian, stats.uintMax)
	case ColumnTypeFloat64:
		_ = binary.Write(&buf, binary.LittleEndian, stats.floatMin)
		_ = binary.Write(&buf, binary.LittleEndian, stats.floatMax)
	case ColumnTypeBool:
		minVal := byte(0)
		maxVal := byte(0)
		if stats.boolMin {
			minVal = 1
		}
		if stats.boolMax {
			maxVal = 1
		}
		_ = buf.WriteByte(minVal)
		_ = buf.WriteByte(maxVal)
	}
	return buf.Bytes()
}

func writeSizedBytes(buf *bytes.Buffer, data []byte) {
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(data)))
	if len(data) > 0 {
		_, _ = buf.Write(data)
	}
}
