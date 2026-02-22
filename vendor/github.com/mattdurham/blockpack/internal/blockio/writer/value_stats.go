package writer

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// Columns that should not be tracked in value statistics because they're
// already handled by dedicated indexes or block metadata
var excludedColumns = map[string]bool{
	"trace:id":    true, // Already in traceBlockIndex
	"trace:index": true, // Internal trace table lookup column
	"span:id":     true, // Unique per span, not useful for filtering
	"span:start":  true, // Already in MinStart/MaxStart
	"span:end":    true, // Derivable from span:start + duration
}

// valueStatsBloom is a simple bloom filter implementation for value statistics
type valueStatsBloom struct {
	bits [32]byte // 256 bits
}

func newValueStatsBloom() *valueStatsBloom {
	return &valueStatsBloom{}
}

// Add adds a value to the bloom filter using two hash functions
func (b *valueStatsBloom) Add(data []byte) {
	h1, h2 := b.hash(data)

	// Use double hashing to generate multiple hash functions
	// k=4 hash functions for good false positive rate
	for i := uint32(0); i < 4; i++ {
		pos := (h1 + i*h2) % 256
		byteIdx := pos / 8
		bitIdx := pos % 8
		b.bits[byteIdx] |= 1 << bitIdx
	}
}

// hash returns two hash values for double hashing
func (b *valueStatsBloom) hash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	sum := h.Sum64()
	h1 := uint32(sum & 0xFFFFFFFF) //nolint:gosec
	h2 := uint32(sum >> 32)        //nolint:gosec
	if h2 == 0 {
		h2 = 1 // Ensure h2 is never 0
	}
	return h1, h2
}

// toBytes returns the bloom filter bits
func (b *valueStatsBloom) toBytes() [32]byte {
	return b.bits
}

// blockStatsCollector accumulates statistics for a single block during write.
// All columns are tracked automatically (except those in excludedColumns).
type blockStatsCollector struct {
	tracked map[string]*attributeStatsBuilder
}

func newBlockStatsCollector() *blockStatsCollector {
	return &blockStatsCollector{
		tracked: make(map[string]*attributeStatsBuilder),
	}
}

// recordValue records a value for a specific attribute
// Automatically tracks all columns except those in excludedColumns
func (c *blockStatsCollector) recordValue(attrName string, value any) {
	// Skip columns that are already handled by special indexes
	if excludedColumns[attrName] {
		return
	}

	// Auto-detect stats type from the value
	statsType := detectStatsType(value)
	if statsType == StatsTypeNone {
		return // Skip nil or unsupported types
	}

	builder := c.getOrCreateBuilder(attrName, statsType)
	if builder == nil {
		return // Type mismatch detected, skip this value
	}
	builder.add(value)
}

// detectStatsType infers the statistics type from a value's runtime type
func detectStatsType(value any) StatsType {
	if value == nil {
		return StatsTypeNone
	}

	switch v := value.(type) {
	case string:
		return StatsTypeString
	case int64, int, int32, int16, int8:
		return StatsTypeInt64
	case uint64:
		// Only treat as signed if it fits in int64 to avoid overflow
		if v > 1<<63-1 {
			return StatsTypeNone
		}
		return StatsTypeInt64
	case uint:
		// On 64-bit platforms, uint can exceed int64; check before treating as signed
		if uint64(v) > 1<<63-1 {
			return StatsTypeNone
		}
		return StatsTypeInt64
	case uint32, uint16, uint8:
		// Always fits within int64
		return StatsTypeInt64
	case float64, float32:
		return StatsTypeFloat64
	case bool:
		return StatsTypeBool
	default:
		return StatsTypeNone
	}
}

// getOrCreateBuilder returns a statistics builder for the given column.
// If a builder already exists but has a different type, it returns nil to indicate
// the column should not be tracked (mixed types within a block are not supported).
// This handles edge cases where a column receives inconsistent types within the same block.
func (c *blockStatsCollector) getOrCreateBuilder(name string, typ StatsType) *attributeStatsBuilder {
	builder, exists := c.tracked[name]
	if !exists {
		builder = newAttributeStatsBuilder(typ)
		c.tracked[name] = builder
		return builder
	}

	// Check for type mismatch - if types don't match, stop tracking this column
	if builder.statsType != typ {
		// Mark builder as invalid without mutating its type
		builder.invalid = true
		return nil
	}

	return builder
}

// finalize returns the collected statistics and resets for next block.
// Builders marked as invalid (due to type mismatches) are excluded from results.
func (c *blockStatsCollector) finalize() map[string]AttributeStats {
	if len(c.tracked) == 0 {
		return nil
	}

	result := make(map[string]AttributeStats, len(c.tracked))
	for attrName, builder := range c.tracked {
		// Skip builders that were invalidated due to type mismatches
		if builder.invalid {
			continue
		}
		result[attrName] = builder.build()
	}
	return result
}

// reset clears accumulated statistics for the next block
func (c *blockStatsCollector) reset() {
	c.tracked = make(map[string]*attributeStatsBuilder)
}

// attributeStatsBuilder accumulates statistics for a single attribute
type attributeStatsBuilder struct {
	bloom *valueStatsBloom

	// Cardinality tracking (exact for small counts)
	seenValues map[string]struct{}

	// Numeric bounds
	minInt   int64
	maxInt   int64
	minFloat float64
	maxFloat float64

	statsType StatsType
	invalid   bool // Set to true when type mismatch detected

	hasValues bool
}

func newAttributeStatsBuilder(typ StatsType) *attributeStatsBuilder {
	return &attributeStatsBuilder{
		statsType:  typ,
		bloom:      newValueStatsBloom(),
		seenValues: make(map[string]struct{}),
		minInt:     math.MaxInt64,
		maxInt:     math.MinInt64,
		minFloat:   math.MaxFloat64,
		maxFloat:   -math.MaxFloat64,
	}
}

func (b *attributeStatsBuilder) add(value any) {
	if value == nil {
		return
	}

	b.hasValues = true

	switch b.statsType {
	case StatsTypeString:
		strVal, ok := value.(string)
		if !ok {
			return
		}
		b.bloom.Add([]byte(strVal))
		if len(b.seenValues) < 1000 {
			b.seenValues[strVal] = struct{}{}
		}

	case StatsTypeInt64:
		intVal := toInt64(value)
		b.bloom.Add(int64ToBytes(intVal))
		if intVal < b.minInt {
			b.minInt = intVal
		}
		if intVal > b.maxInt {
			b.maxInt = intVal
		}

	case StatsTypeFloat64:
		floatVal := toFloat64(value)
		b.bloom.Add(float64ToBytes(floatVal))
		if floatVal < b.minFloat {
			b.minFloat = floatVal
		}
		if floatVal > b.maxFloat {
			b.maxFloat = floatVal
		}

	case StatsTypeBool:
		boolVal, ok := value.(bool)
		if !ok {
			return
		}
		if boolVal {
			b.bloom.Add([]byte{1})
		} else {
			b.bloom.Add([]byte{0})
		}
	}
}

func (b *attributeStatsBuilder) build() AttributeStats {
	stats := AttributeStats{
		Type:                b.statsType,
		ValuesBloom:         b.bloom.toBytes(),
		ApproxDistinctCount: uint16(min(len(b.seenValues), 65535)), //nolint:gosec
	}

	if b.hasValues {
		stats.MinInt = b.minInt
		stats.MaxInt = b.maxInt
		stats.MinFloat = b.minFloat
		stats.MaxFloat = b.maxFloat
	}

	return stats
}

// Helper functions for type conversion

func toInt64(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v) //nolint:gosec
	case uint32:
		return int64(v)
	case uint64:
		return int64(v) //nolint:gosec
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func toFloat64(value any) float64 {
	switch v := value.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func int64ToBytes(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v)) //nolint:gosec
	return buf
}

func float64ToBytes(v float64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
	return buf
}

func min(a, b int) int { //nolint:revive
	if a < b {
		return a
	}
	return b
}

// writeValueStats is now a wrapper for shared.WriteValueStats for backward compatibility
func writeValueStats(buf *bytes.Buffer, stats map[string]AttributeStats) error {
	return shared.WriteValueStats(buf, stats)
}
