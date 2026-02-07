package ondiskio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
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

// StatsType identifies the type of statistics stored for an attribute
type StatsType uint8

const (
	StatsTypeNone StatsType = iota
	StatsTypeString
	StatsTypeInt64
	StatsTypeFloat64
	StatsTypeBool
)

// AttributeStats holds per-block statistics for a single attribute.
// Used for query optimization by allowing block pruning before reading.
type AttributeStats struct {
	Type StatsType

	// Bloom filter for value membership testing (256 bits = 32 bytes)
	// For strings: hash of the string value
	// For numbers: hash of the numeric value
	// ~1% false positive rate for up to 100 distinct values per block
	ValuesBloom [32]byte

	// Numeric range bounds (only valid for Int64/Float64 types)
	MinInt   int64
	MaxInt   int64
	MinFloat float64
	MaxFloat float64

	// Approximate count of distinct values in this block
	// Used for selectivity estimation
	ApproxDistinctCount uint16
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

// MayContain checks if a value might be in the set (can have false positives)
func (b *valueStatsBloom) MayContain(data []byte) bool {
	h1, h2 := b.hash(data)

	for i := uint32(0); i < 4; i++ {
		pos := (h1 + i*h2) % 256
		byteIdx := pos / 8
		bitIdx := pos % 8
		if (b.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

// hash returns two hash values for double hashing
func (b *valueStatsBloom) hash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	sum := h.Sum64()
	h1 := uint32(sum & 0xFFFFFFFF)
	h2 := uint32(sum >> 32)
	if h2 == 0 {
		h2 = 1 // Ensure h2 is never 0
	}
	return h1, h2
}

// toBytes returns the bloom filter bits
func (b *valueStatsBloom) toBytes() [32]byte {
	return b.bits
}

// fromBytes loads bloom filter from bytes
func (b *valueStatsBloom) fromBytes(data [32]byte) {
	b.bits = data
}

func mergeAttributeStats(a, b AttributeStats) (AttributeStats, bool) {
	if a.Type == StatsTypeNone {
		return b, true
	}
	if b.Type == StatsTypeNone {
		return a, true
	}
	if a.Type != b.Type {
		return AttributeStats{}, false
	}

	out := a
	for i := range out.ValuesBloom {
		out.ValuesBloom[i] |= b.ValuesBloom[i]
	}

	switch a.Type {
	case StatsTypeInt64:
		if b.MinInt < out.MinInt {
			out.MinInt = b.MinInt
		}
		if b.MaxInt > out.MaxInt {
			out.MaxInt = b.MaxInt
		}
	case StatsTypeFloat64:
		if b.MinFloat < out.MinFloat {
			out.MinFloat = b.MinFloat
		}
		if b.MaxFloat > out.MaxFloat {
			out.MaxFloat = b.MaxFloat
		}
	}

	distinct := uint32(out.ApproxDistinctCount) + uint32(b.ApproxDistinctCount)
	if distinct > math.MaxUint16 {
		distinct = math.MaxUint16
	}
	out.ApproxDistinctCount = uint16(distinct)

	return out, true
}

// blockStatsCollector accumulates statistics for a single block during write
type blockStatsCollector struct {
	tracked map[string]*attributeStatsBuilder
	config  []TrackedAttribute
}

func newBlockStatsCollector(config []TrackedAttribute) *blockStatsCollector {
	return &blockStatsCollector{
		tracked: make(map[string]*attributeStatsBuilder),
		config:  config,
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
	statsType StatsType
	bloom     *valueStatsBloom
	invalid   bool // Set to true when type mismatch detected

	// Numeric bounds
	minInt   int64
	maxInt   int64
	minFloat float64
	maxFloat float64

	// Cardinality tracking (exact for small counts)
	seenValues map[string]struct{}
	hasValues  bool
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
		ApproxDistinctCount: uint16(min(len(b.seenValues), 65535)),
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
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
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
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}

func float64ToBytes(v float64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
	return buf
}

// Serialization helpers

func (s *AttributeStats) writeTo(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.LittleEndian, s.Type); err != nil {
		return err
	}
	if _, err := buf.Write(s.ValuesBloom[:]); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MinInt); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MaxInt); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MinFloat); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, s.MaxFloat); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, s.ApproxDistinctCount); err != nil {
		return err
	}
	return nil
}

func readAttributeStats(rd *bytes.Reader) (AttributeStats, error) {
	var stats AttributeStats

	if err := binary.Read(rd, binary.LittleEndian, &stats.Type); err != nil {
		return stats, err
	}
	if _, err := rd.Read(stats.ValuesBloom[:]); err != nil {
		return stats, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MinInt); err != nil {
		return stats, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MaxInt); err != nil {
		return stats, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MinFloat); err != nil {
		return stats, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.MaxFloat); err != nil {
		return stats, err
	}
	if err := binary.Read(rd, binary.LittleEndian, &stats.ApproxDistinctCount); err != nil {
		return stats, err
	}

	return stats, nil
}

// valueMatchesStats checks if a value might exist in a block based on statistics
func valueMatchesStats(value any, stats AttributeStats) bool {
	if value == nil {
		return true // Can't determine, include block
	}

	bloom := &valueStatsBloom{}
	bloom.fromBytes(stats.ValuesBloom)

	switch stats.Type {
	case StatsTypeString:
		strVal, ok := value.(string)
		if !ok {
			return true
		}
		return bloom.MayContain([]byte(strVal))

	case StatsTypeInt64:
		intVal := toInt64(value)
		// Check range first (fast)
		if intVal < stats.MinInt || intVal > stats.MaxInt {
			return false
		}
		// Check bloom filter for more precision
		return bloom.MayContain(int64ToBytes(intVal))

	case StatsTypeFloat64:
		floatVal := toFloat64(value)
		if floatVal < stats.MinFloat || floatVal > stats.MaxFloat {
			return false
		}
		return bloom.MayContain(float64ToBytes(floatVal))

	case StatsTypeBool:
		boolVal, ok := value.(bool)
		if !ok {
			return true
		}
		if boolVal {
			return bloom.MayContain([]byte{1})
		}
		return bloom.MayContain([]byte{0})
	}

	return true
}

// TrackedAttribute specifies an attribute to track for block pruning
type TrackedAttribute struct {
	Name string
	Type StatsType
}

// DefaultTrackedAttributes is DEPRECATED and unused.
// All columns are now automatically tracked except those in excludedColumns.
// This variable is kept for backward compatibility only.
var DefaultTrackedAttributes = []TrackedAttribute{
	{"service.name", StatsTypeString},
	{"http.method", StatsTypeString},
	{"http.status_code", StatsTypeInt64},
	{"span.kind", StatsTypeString},
	{"resource.tier", StatsTypeString},
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MayContainValue checks if a value might be in the block based on bloom filter
func (s *AttributeStats) MayContainValue(value any) bool {
	return valueMatchesStats(value, *s)
}

// SerializedSize returns the size in bytes when serialized
func (s *AttributeStats) SerializedSize() int {
	// type(1) + bloom(32) + minInt(8) + maxInt(8) + minFloat(8) + maxFloat(8) + distinctCount(2)
	return 1 + 32 + 8 + 8 + 8 + 8 + 2
}

// writeValueStats writes value statistics to buffer
func writeValueStats(buf *bytes.Buffer, stats map[string]AttributeStats) error {
	count := uint16(len(stats))
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("write stats count: %w", err)
	}

	if count == 0 {
		return nil
	}

	for name, stat := range stats {
		// Write attribute name
		nameLen := uint16(len(name))
		if err := binary.Write(buf, binary.LittleEndian, nameLen); err != nil {
			return fmt.Errorf("write name length: %w", err)
		}
		if _, err := buf.WriteString(name); err != nil {
			return fmt.Errorf("write name: %w", err)
		}

		// Write statistics
		if err := stat.writeTo(buf); err != nil {
			return fmt.Errorf("write stats for %s: %w", name, err)
		}
	}

	return nil
}

// readValueStats reads value statistics from reader
func readValueStats(rd *bytes.Reader) (map[string]AttributeStats, error) {
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
		stat, err := readAttributeStats(rd)
		if err != nil {
			return nil, fmt.Errorf("read stats for %s: %w", name, err)
		}

		stats[name] = stat
	}

	return stats, nil
}
