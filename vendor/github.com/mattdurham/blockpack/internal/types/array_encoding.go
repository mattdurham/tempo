// Package ondisk provides on-disk format types and encoding/decoding utilities for blockpack.
package ondisk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// ArrayValueType represents the type of an array element.
type ArrayValueType byte

// Array element type constants.
const (
	// ArrayTypeString is the string array element type.
	ArrayTypeString ArrayValueType = iota
	// ArrayTypeInt64 is the int64 array element type.
	ArrayTypeInt64
	// ArrayTypeFloat64 is the float64 array element type.
	ArrayTypeFloat64
	// ArrayTypeBool is the bool array element type.
	ArrayTypeBool
	// ArrayTypeBytes is the bytes array element type.
	ArrayTypeBytes
	// ArrayTypeDuration is the duration array element type.
	ArrayTypeDuration
)

// ArrayValue represents a single value in an array.
type ArrayValue struct {
	Str   string
	Bytes []byte
	Int   int64
	Float float64
	Type  ArrayValueType
	Bool  bool
}

// encodeTypedValue encodes a single typed value to the buffer.
// This is the canonical place for all type-specific encoding logic.
func encodeTypedValue(buf *bytes.Buffer, val ArrayValue) {
	buf.WriteByte(byte(val.Type))
	switch val.Type {
	case ArrayTypeString:
		writeBytes(buf, []byte(val.Str))
	case ArrayTypeInt64, ArrayTypeDuration:
		_ = binary.Write(buf, binary.LittleEndian, val.Int)
	case ArrayTypeFloat64:
		bits := math.Float64bits(val.Float)
		_ = binary.Write(buf, binary.LittleEndian, bits)
	case ArrayTypeBool:
		if val.Bool {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case ArrayTypeBytes:
		writeBytes(buf, val.Bytes)
	}
}

// EncodeArray encodes an array of values into a byte slice.
func EncodeArray(values []ArrayValue) []byte {
	if len(values) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writeUvarint(&buf, uint64(len(values)))
	for _, val := range values {
		encodeTypedValue(&buf, val)
	}
	return buf.Bytes()
}

// decodeTypedValue decodes a single typed value from the reader.
// This is the canonical place for all type-specific decoding logic.
func decodeTypedValue(rd *bytes.Reader, valueType ArrayValueType) (ArrayValue, error) {
	switch valueType {
	case ArrayTypeString:
		str, err := readBytes(rd)
		if err != nil {
			return ArrayValue{}, err
		}
		return ArrayValue{Type: ArrayTypeString, Str: string(str)}, nil
	case ArrayTypeInt64:
		var v int64
		if err := binary.Read(rd, binary.LittleEndian, &v); err != nil {
			return ArrayValue{}, fmt.Errorf("read int64: %w", err)
		}
		return ArrayValue{Type: ArrayTypeInt64, Int: v}, nil
	case ArrayTypeDuration:
		var v int64
		if err := binary.Read(rd, binary.LittleEndian, &v); err != nil {
			return ArrayValue{}, fmt.Errorf("read duration: %w", err)
		}
		return ArrayValue{Type: ArrayTypeDuration, Int: v}, nil
	case ArrayTypeFloat64:
		var bits uint64
		if err := binary.Read(rd, binary.LittleEndian, &bits); err != nil {
			return ArrayValue{}, fmt.Errorf("read float64: %w", err)
		}
		return ArrayValue{Type: ArrayTypeFloat64, Float: math.Float64frombits(bits)}, nil
	case ArrayTypeBool:
		b, err := rd.ReadByte()
		if err != nil {
			return ArrayValue{}, fmt.Errorf("read bool: %w", err)
		}
		return ArrayValue{Type: ArrayTypeBool, Bool: b != 0}, nil
	case ArrayTypeBytes:
		b, err := readBytes(rd)
		if err != nil {
			return ArrayValue{}, err
		}
		return ArrayValue{Type: ArrayTypeBytes, Bytes: b}, nil
	default:
		return ArrayValue{}, fmt.Errorf("unsupported array type %d", valueType)
	}
}

// DecodeArray decodes an array from bytes.
func DecodeArray(data []byte) ([]ArrayValue, error) {
	if len(data) == 0 {
		return nil, nil
	}
	rd := bytes.NewReader(data)
	count, err := binary.ReadUvarint(rd)
	if err != nil {
		return nil, fmt.Errorf("read array count: %w", err)
	}
	values := make([]ArrayValue, 0, count)
	for i := uint64(0); i < count; i++ {
		t, err := rd.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read array type: %w", err)
		}
		val, err := decodeTypedValue(rd, ArrayValueType(t))
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// EncodeStringArray encodes a string array to bytes.
func EncodeStringArray(values []string) []byte {
	if len(values) == 0 {
		return nil
	}
	arr := make([]ArrayValue, 0, len(values))
	for _, val := range values {
		arr = append(arr, ArrayValue{Type: ArrayTypeString, Str: val})
	}
	return EncodeArray(arr)
}

// EncodeDurationArray encodes a duration array to bytes.
func EncodeDurationArray(values []int64) []byte {
	if len(values) == 0 {
		return nil
	}
	arr := make([]ArrayValue, 0, len(values))
	for _, val := range values {
		arr = append(arr, ArrayValue{Type: ArrayTypeDuration, Int: val})
	}
	return EncodeArray(arr)
}

// EncodeInt64Array encodes an int64 array to bytes.
func EncodeInt64Array(values []int64) []byte {
	if len(values) == 0 {
		return nil
	}
	arr := make([]ArrayValue, 0, len(values))
	for _, val := range values {
		arr = append(arr, ArrayValue{Type: ArrayTypeInt64, Int: val})
	}
	return EncodeArray(arr)
}

// EncodeAnyValueArray encodes an array of OTEL AnyValue to bytes.
func EncodeAnyValueArray(values []*commonv1.AnyValue) []byte {
	if len(values) == 0 {
		return nil
	}
	arr := make([]ArrayValue, 0, len(values))
	for _, val := range values {
		if val == nil {
			continue
		}
		switch v := val.Value.(type) {
		case *commonv1.AnyValue_StringValue:
			arr = append(arr, ArrayValue{Type: ArrayTypeString, Str: v.StringValue})
		case *commonv1.AnyValue_IntValue:
			arr = append(arr, ArrayValue{Type: ArrayTypeInt64, Int: v.IntValue})
		case *commonv1.AnyValue_DoubleValue:
			arr = append(arr, ArrayValue{Type: ArrayTypeFloat64, Float: v.DoubleValue})
		case *commonv1.AnyValue_BoolValue:
			arr = append(arr, ArrayValue{Type: ArrayTypeBool, Bool: v.BoolValue})
		case *commonv1.AnyValue_BytesValue:
			arr = append(arr, ArrayValue{Type: ArrayTypeBytes, Bytes: v.BytesValue})
		}
	}
	return EncodeArray(arr)
}

func writeUvarint(buf *bytes.Buffer, val uint64) {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], val)
	buf.Write(tmp[:n])
}

func writeBytes(buf *bytes.Buffer, data []byte) {
	writeUvarint(buf, uint64(len(data)))
	if len(data) > 0 {
		buf.Write(data)
	}
}

func readBytes(rd *bytes.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(rd)
	if err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	if _, err := rd.Read(out); err != nil {
		return nil, fmt.Errorf("read bytes: %w", err)
	}
	return out, nil
}

// EncodeKeyValueList encodes an OTEL KeyValueList to bytes.
// Format:
//   - Count (varint)
//   - For each entry:
//   - Key length (varint)
//   - Key bytes
//   - Value type (1 byte - ArrayValueType)
//   - Value data (depending on type)
func EncodeKeyValueList(kvlist *commonv1.KeyValueList) []byte {
	if kvlist == nil || len(kvlist.Values) == 0 {
		return nil
	}
	var buf bytes.Buffer
	writeUvarint(&buf, uint64(len(kvlist.Values)))
	for _, kv := range kvlist.Values {
		if kv == nil {
			continue
		}
		// Write key
		writeBytes(&buf, []byte(kv.Key))

		// Write value
		if kv.Value == nil {
			// NULL value - write special type marker
			buf.WriteByte(0xFF)
			continue
		}

		// Convert AnyValue to ArrayValue and encode using the canonical encoding function
		switch v := kv.Value.Value.(type) {
		case *commonv1.AnyValue_StringValue:
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeString, Str: v.StringValue})
		case *commonv1.AnyValue_IntValue:
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeInt64, Int: v.IntValue})
		case *commonv1.AnyValue_DoubleValue:
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeFloat64, Float: v.DoubleValue})
		case *commonv1.AnyValue_BoolValue:
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeBool, Bool: v.BoolValue})
		case *commonv1.AnyValue_BytesValue:
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeBytes, Bytes: v.BytesValue})
		case *commonv1.AnyValue_ArrayValue:
			// Nested array - encode it and store as bytes
			encoded := EncodeAnyValueArray(v.ArrayValue.Values)
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeBytes, Bytes: encoded})
		case *commonv1.AnyValue_KvlistValue:
			// Nested KVList - encode it and store as bytes
			encoded := EncodeKeyValueList(v.KvlistValue)
			encodeTypedValue(&buf, ArrayValue{Type: ArrayTypeBytes, Bytes: encoded})
		default:
			// Unsupported type - write NULL marker
			buf.WriteByte(0xFF)
		}
	}
	return buf.Bytes()
}
