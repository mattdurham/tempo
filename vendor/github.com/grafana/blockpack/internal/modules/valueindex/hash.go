package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ColHash returns the 32-char lower-hex column hash for a column name.
// col_hash = lower_hex(SHA-256(col_name)[:16])
func ColHash(colName string) string {
	sum := sha256.Sum256([]byte(colName))
	return hex.EncodeToString(sum[:16])
}

// ValueHash returns the 32-char lower-hex value hash for a canonical-encoded value.
// value_hash = lower_hex(SHA-256(encoded_value)[:16])
func ValueHash(encodedValue []byte) string {
	sum := sha256.Sum256(encodedValue)
	return hex.EncodeToString(sum[:16])
}

// ValueHash16 returns the raw 16-byte value hash for use in hash index entries.
func ValueHash16(encodedValue []byte) [16]byte {
	sum := sha256.Sum256(encodedValue)
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// CanonicalValue encodes a typed value to its canonical binary form.
// This encoding is used to compute value_hash and as the wire representation of vi:value.
// Range* types are indexed as their scalar equivalents (NOTE-VI-012): callers pass int64
// for RangeDuration/RangeInt64, uint64 for RangeUint64, float64 for RangeFloat64,
// string for RangeString, and []byte for RangeBytes. Only VectorF32 is excluded.
func CanonicalValue(colType shared.ColumnType, v any) ([]byte, error) {
	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeString expects string, got %T", v)
		}
		return []byte(s), nil

	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeInt64/RangeInt64/RangeDuration expects int64, got %T", v)
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(n)) //nolint:gosec // intentional two's complement re-interpretation
		return b, nil

	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		n, ok := v.(uint64)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeUint64/RangeUint64 expects uint64, got %T", v)
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, n)
		return b, nil

	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeFloat64/RangeFloat64 expects float64, got %T", v)
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(f))
		return b, nil

	case shared.ColumnTypeBool:
		bv, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeBool expects bool, got %T", v)
		}
		if bv {
			return []byte{0x01}, nil
		}
		return []byte{0x00}, nil

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		b, ok := v.([]byte)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeBytes/RangeBytes expects []byte, got %T", v)
		}
		return b, nil

	case shared.ColumnTypeUUID:
		uid, ok := v.([16]byte)
		if !ok {
			return nil, fmt.Errorf("valueindex: ColumnTypeUUID expects [16]byte, got %T", v)
		}
		out := make([]byte, 16)
		copy(out, uid[:])
		return out, nil

	default:
		return nil, fmt.Errorf(
			"valueindex: column type %d is not indexable (VectorF32 is excluded)",
			colType,
		)
	}
}
