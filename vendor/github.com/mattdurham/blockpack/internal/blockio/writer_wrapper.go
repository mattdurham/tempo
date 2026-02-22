package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/writer"
)

// Writer is a backward-compatible type alias for writer.Writer.
type Writer = writer.Writer //nolint:revive

// WriterConfig is a backward-compatible type alias for writer.WriterConfig.
type WriterConfig = writer.WriterConfig //nolint:revive

// BufferedSpan is a backward-compatible type alias for writer.BufferedSpan.
type BufferedSpan = writer.BufferedSpan //nolint:revive

// MetricStreamBuilder is a backward-compatible type alias for writer.MetricStreamBuilder.
type MetricStreamBuilder = writer.MetricStreamBuilder //nolint:revive

// MetricStreamDef is a backward-compatible type alias for writer.MetricStreamDef.
type MetricStreamDef = writer.MetricStreamDef //nolint:revive

// BinaryAggValue is a backward-compatible type alias for writer.BinaryAggValue.
type BinaryAggValue = writer.BinaryAggValue //nolint:revive

// SerializableAgg is a backward-compatible type alias for writer.SerializableAgg.
type SerializableAgg = writer.SerializableAgg //nolint:revive

// GroupKeyIndex is a backward-compatible type alias for writer.GroupKeyIndex.
type GroupKeyIndex = writer.GroupKeyIndex //nolint:revive

// Backward-compatible function wrappers
var (
	NewWriterWithConfig = writer.NewWriterWithConfig
)

// Aggregate value type constants
const (
	ValueTypeString  = writer.ValueTypeString
	ValueTypeInt64   = writer.ValueTypeInt64
	ValueTypeFloat64 = writer.ValueTypeFloat64
	ValueTypeBool    = writer.ValueTypeBool
)

// Aggregate type constants
const (
	AggtypeCount = writer.AggtypeCount
	AggtypeSum   = writer.AggtypeSum
	AggtypeMin   = writer.AggtypeMin
	AggtypeMax   = writer.AggtypeMax
	AggtypeP50   = writer.AggtypeP50
	AggtypeP95   = writer.AggtypeP95
	AggtypeP99   = writer.AggtypeP99
)

// Compression constants
const (
	CompressionZstd = writer.CompressionZstd
)
