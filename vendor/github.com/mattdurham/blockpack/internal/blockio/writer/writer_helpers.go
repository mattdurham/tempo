package writer

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

// writer_helpers.go contains utility helper functions for the writer.

// Metric stream methods are now in writer_metrics.go

// isUUID checks if a string matches the UUID format: 8-4-4-4-12 hex digits
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	// Check format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	// Check all other characters are hex digits
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// parseUUID converts a UUID string to 16 bytes
func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	// Remove hyphens
	hexStr := strings.ReplaceAll(s, "-", "")
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return uuid, err
	}
	if len(decoded) != 16 {
		return uuid, fmt.Errorf("invalid UUID length: %d", len(decoded))
	}
	copy(uuid[:], decoded)
	return uuid, nil
}

// Known UUID columns that should always be stored as binary
var knownUUIDColumns = map[string]bool{
	"resource.k8s.pod.uid":        true,
	"resource.k8s.replicaset.uid": true,
	"resource.k8s.deployment.uid": true,
	"span.correlation.id":         true,
	"span.request.id":             true,
}

// skipDedicatedColumns lists columns that have their own purpose-built index
// and should not be tracked in the general dedicated column index.
var skipDedicatedColumns = map[string]bool{
	"trace:id":       true,
	"span:id":        true,
	"span:parent_id": true,
}

func (w *Writer) recordDedicatedValue(b *blockBuilder, name string, key DedicatedValueKey) {
	if w == nil {
		return
	}
	// Skip columns that have their own purpose-built index (trace block index)
	if skipDedicatedColumns[name] {
		return
	}

	keyType := key.Type()

	// Check if we've already detected this column's type
	typ, ok := w.autoDedicatedTypes[name]
	if !ok {
		// Auto-detect type from the first value seen for this column.
		switch keyType {
		case ColumnTypeInt64:
			typ = ColumnTypeInt64
		case ColumnTypeUint64:
			typ = ColumnTypeUint64
		case ColumnTypeBytes:
			typ = ColumnTypeBytes
		case ColumnTypeFloat64:
			typ = ColumnTypeFloat64
		case ColumnTypeString:
			typ = ColumnTypeString
		case ColumnTypeBool:
			typ = ColumnTypeBool
		default:
			// Unknown type, skip
			return
		}
		// Track this auto-detected type
		w.autoDedicatedTypes[name] = typ
	}

	// Check if this is a range-bucketed column OR a regular type that will be upgraded
	// We need to collect values for cardinality detection
	switch typ {
	case ColumnTypeInt64, ColumnTypeRangeInt64:
		// Accept int64/uint64/bytes keys for int64 columns
		if keyType != ColumnTypeInt64 && keyType != ColumnTypeUint64 && keyType != ColumnTypeBytes {
			return
		}
		// Extract int64 value from key (first 8 bytes for Bytes type)
		if len(key.Data()) >= 8 {
			value := int64(binary.LittleEndian.Uint64(key.Data())) //nolint:gosec
			w.recordRangeBucketValue(name, value)
		}
	case ColumnTypeUint64, ColumnTypeRangeUint64, ColumnTypeRangeDuration:
		// Accept int64/uint64/bytes keys for uint64 columns
		if keyType != ColumnTypeInt64 && keyType != ColumnTypeUint64 && keyType != ColumnTypeBytes {
			return
		}
		// Extract int64 value from key
		if len(key.Data()) >= 8 {
			value := int64(binary.LittleEndian.Uint64(key.Data())) //nolint:gosec
			w.recordRangeBucketValue(name, value)
		}
	case ColumnTypeFloat64, ColumnTypeRangeFloat64:
		// Only accept float64 keys
		if keyType != ColumnTypeFloat64 {
			return
		}
		// Extract float64 value from key
		if len(key.Data()) >= 8 {
			bits := binary.LittleEndian.Uint64(key.Data())
			value := math.Float64frombits(bits)
			w.recordRangeFloat64Value(name, value)
		}
	case ColumnTypeBytes:
		// Regular bytes columns use exact-match dedicated index.
		// Also feed KLL sketch so processKLLStringByteSketches can upgrade to
		// ColumnTypeRangeBytes if the column turns out to be high-cardinality.
		if keyType != ColumnTypeBytes {
			return
		}
		b.recordDedicated(name, key)
		w.recordRangeBytesValue(name, key.Data())
	case ColumnTypeRangeBytes:
		// Range-bytes columns use KLL sketches for bucket-based lookups
		if keyType != ColumnTypeBytes {
			return
		}
		w.recordRangeBytesValue(name, key.Data())
	case ColumnTypeString:
		// Regular string columns use exact-match dedicated index.
		// Also feed KLL sketch so processKLLStringByteSketches can upgrade to
		// ColumnTypeRangeString if the column turns out to be high-cardinality.
		if keyType != ColumnTypeString {
			return
		}
		b.recordDedicated(name, key)
		w.recordRangeStringValue(name, string(key.Data()))
	case ColumnTypeRangeString:
		// Range-string columns use KLL sketches for bucket-based lookups
		if keyType != ColumnTypeString {
			return
		}
		w.recordRangeStringValue(name, string(key.Data()))
	case ColumnTypeBool:
		// Regular dedicated column - type must match exactly
		if typ != keyType {
			return
		}
		// Add to block's dedicated map
		b.recordDedicated(name, key)
	default:
		// Unknown type, skip
		return
	}
}

// recordRangeFloat64Value collects a float64 value for range bucketing.
// Uses a KLL sketch for global boundary computation (O(k + log n) memory)
// plus per-block min/max for accurate bucket assignment.
func (w *Writer) recordRangeFloat64Value(columnName string, value float64) {
	// Skip NaN values: they violate sort.Slice's strict-weak-ordering contract
	// (NaN < x == false && x < NaN == false), which causes undefined sort behavior
	// during KLL compaction.
	if math.IsNaN(value) {
		return
	}
	blockID := len(w.blockRefs)

	// KLL sketch for global boundary computation
	sketch, ok := w.kllFloat64Sketches[columnName]
	if !ok {
		sketch = NewKLL[float64](256)
		w.kllFloat64Sketches[columnName] = sketch
	}
	sketch.Update(value)

	// Per-block min/max for high-cardinality bucket assignment
	mm := w.rangeFloat64BlockMinMax[columnName]
	if mm == nil {
		mm = make(map[int][2]float64)
		w.rangeFloat64BlockMinMax[columnName] = mm
	}
	if cur, exists := mm[blockID]; exists {
		if value < cur[0] {
			cur[0] = value
		}
		if value > cur[1] {
			cur[1] = value
		}
		mm[blockID] = cur
	} else {
		mm[blockID] = [2]float64{value, value}
	}
}

// recordRangeBytesValue collects a bytes value for range bucketing.
// Uses a KLL[string] sketch (string representation) for global boundary computation
// plus per-block min/max for accurate bucket assignment.
func (w *Writer) recordRangeBytesValue(columnName string, value []byte) {
	blockID := len(w.blockRefs)
	strVal := string(value)

	// KLL sketch for global boundary computation (bytes stored as string)
	sketch, ok := w.kllStringSketches[columnName]
	if !ok {
		sketch = NewKLL[string](256)
		w.kllStringSketches[columnName] = sketch
	}
	sketch.Update(strVal)

	// Per-block min/max for high-cardinality bucket assignment
	mm := w.rangeStringBlockMinMax[columnName]
	if mm == nil {
		mm = make(map[int][2]string)
		w.rangeStringBlockMinMax[columnName] = mm
	}
	if cur, exists := mm[blockID]; exists {
		if strVal < cur[0] {
			cur[0] = strVal
		}
		if strVal > cur[1] {
			cur[1] = strVal
		}
		mm[blockID] = cur
	} else {
		mm[blockID] = [2]string{strVal, strVal}
	}
}

// recordRangeStringValue collects a string value for range bucketing.
// Uses a KLL[string] sketch for global boundary computation (O(k + log n) memory)
// plus per-block min/max for accurate bucket assignment.
func (w *Writer) recordRangeStringValue(columnName string, value string) {
	blockID := len(w.blockRefs)

	// KLL sketch for global boundary computation
	sketch, ok := w.kllStringSketches[columnName]
	if !ok {
		sketch = NewKLL[string](256)
		w.kllStringSketches[columnName] = sketch
	}
	sketch.Update(value)

	// Per-block min/max for high-cardinality bucket assignment
	mm := w.rangeStringBlockMinMax[columnName]
	if mm == nil {
		mm = make(map[int][2]string)
		w.rangeStringBlockMinMax[columnName] = mm
	}
	if cur, exists := mm[blockID]; exists {
		if value < cur[0] {
			cur[0] = value
		}
		if value > cur[1] {
			cur[1] = value
		}
		mm[blockID] = cur
	} else {
		mm[blockID] = [2]string{value, value}
	}
}

// recordRangeBucketValue collects a value for a range-bucketed column using reservoir sampling.
// This prevents OOM by using O(log n) memory instead of O(n) for high-cardinality columns.
func (w *Writer) recordRangeBucketValue(columnName string, value int64) {
	blockID := len(w.blockRefs) // Current block being built

	// Check if we have pre-computed boundaries for this column (from external two-pass conversion)
	if boundaries, ok := w.precomputedBoundaries[columnName]; ok {
		// External boundaries mode: directly assign to bucket
		bucketID := GetBucketID(value, boundaries)

		// Initialize bucket index for this column if needed
		if w.rangeBucketIndex[columnName] == nil {
			w.rangeBucketIndex[columnName] = make(map[uint16]map[int]struct{})
		}

		// Add block to this bucket
		if w.rangeBucketIndex[columnName][bucketID] == nil {
			w.rangeBucketIndex[columnName][bucketID] = make(map[int]struct{})
		}
		w.rangeBucketIndex[columnName][bucketID][blockID] = struct{}{}

		return
	}

	// Default mode: use KLL sketch to prevent OOM
	// KLL uses O(k + log n) memory, provably optimal for streaming quantiles
	sketch, ok := w.kllSketches[columnName]
	if !ok {
		const k = 256 // Sketch size parameter: controls accuracy vs memory tradeoff
		sketch = NewKLL[int64](k)
		w.kllSketches[columnName] = sketch
	}
	sketch.Update(value)

	// Track min/max value per block (O(blocks) memory, always accurate)
	// Used by high-cardinality bucketing to assign blocks to all buckets they span
	mm, ok := w.rangeBlockMinMax[columnName]
	if !ok {
		mm = make(map[int][2]int64)
		w.rangeBlockMinMax[columnName] = mm
	}
	if cur, exists := mm[blockID]; exists {
		if value < cur[0] {
			cur[0] = value
		}
		if value > cur[1] {
			cur[1] = value
		}
		mm[blockID] = cur
	} else {
		mm[blockID] = [2]int64{value, value}
	}
}
