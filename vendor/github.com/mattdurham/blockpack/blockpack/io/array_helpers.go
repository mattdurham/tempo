package ondiskio

import (
	types "github.com/mattdurham/blockpack/blockpack/types"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// DecodeArray forwards array decoding to the core types package.
func DecodeArray(data []byte) ([]ArrayValue, error) {
	return types.DecodeArray(data)
}

// EncodeStringArray forwards array encoding to the core types package.
func EncodeStringArray(values []string) []byte {
	return types.EncodeStringArray(values)
}

// EncodeDurationArray forwards array encoding to the core types package.
func EncodeDurationArray(values []int64) []byte {
	return types.EncodeDurationArray(values)
}

// EncodeInt64Array forwards array encoding to the core types package.
func EncodeInt64Array(values []int64) []byte {
	return types.EncodeInt64Array(values)
}

// EncodeAnyValueArray forwards array encoding to the core types package.
func EncodeAnyValueArray(values []*commonv1.AnyValue) []byte {
	return types.EncodeAnyValueArray(values)
}

// EncodeKeyValueList forwards array encoding to the core types package.
func EncodeKeyValueList(kvlist *commonv1.KeyValueList) []byte {
	return types.EncodeKeyValueList(kvlist)
}
