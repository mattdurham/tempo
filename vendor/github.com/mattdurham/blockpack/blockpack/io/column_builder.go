package ondiskio

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/mattdurham/blockpack/blockpack/encodings"
)

// shouldUseDeltaEncoding analyzes uint64 values to determine if delta encoding
// would be beneficial compared to dictionary encoding.
// Delta encoding is good when the range (max - min) is small, as it stores values
// as offsets from the minimum value using 1, 2, 4, or 8 byte widths.
func shouldUseDeltaEncoding(values []uint64, present []byte, spanCount int, minVal, maxVal uint64) bool {
	if spanCount == 0 {
		return false
	}

	// Count unique values and present count
	uniqueValues := make(map[uint64]bool)
	presentCount := 0

	for i := range spanCount {
		if isBitSet(present, i) {
			uniqueValues[values[i]] = true
			presentCount++
		}
	}

	if presentCount == 0 {
		return false
	}

	cardinality := len(uniqueValues)

	// Calculate the range
	valueRange := maxVal - minVal

	// If very low cardinality (≤2 unique values), dictionary is almost always better
	// Example: [100, 100, 100, 200, 200] -> dictionary uses ~20 bytes, delta uses similar
	if cardinality <= 2 {
		return false
	}

	// Delta encoding is beneficial when range fits in 1-2 bytes (uint8 or uint16)
	// This reduces storage from 8 bytes/value to 1-2 bytes/value
	// Threshold: range <= 65535 (fits in uint16)
	//
	// For ranges that fit in 4 bytes, delta encoding still helps but less dramatically.
	// We use a higher cardinality threshold for these cases.
	if valueRange <= 0xFFFF {
		// Range fits in 1-2 bytes: delta encoding is almost always better
		return true
	}

	// For ranges that fit in 3-4 bytes, delta encoding still helps if cardinality is not very low
	// Dictionary overhead grows with cardinality, while delta overhead is fixed
	// For cardinality > 3, delta with 4-byte width is typically better than dictionary
	// Exception: very small cardinality (≤3) with medium count benefits from dictionary
	if valueRange <= 0xFFFFFFFF {
		// Delta encoding is better for cardinality > 3
		return cardinality > 3
	}

	// For very large ranges (requires 8-byte width), delta encoding can still be beneficial
	// when cardinality is moderate-to-high. Since we already checked cardinality > 2 above,
	// and delta encoding with 8-byte width has fixed overhead, it's typically better than
	// dictionary for any cardinality > 2.
	// Dictionary: ~32 bytes + (cardinality * 8) + (count * index_bytes)
	// Delta: 8 + 1 + (count * 8)
	// Delta has fixed per-value cost while dictionary grows with cardinality
	return true
}

// isIDColumn returns true if the column name indicates it contains ID data
// that would benefit from XOR encoding.
// Note: trace:id is excluded because it's a trace-level column that benefits more
// from dictionary encoding (multiple spans share the same trace ID).
func isIDColumn(name string) bool {
	// Known ID columns in tracing data that benefit from XOR encoding
	// Exclude trace:id - it's shared across spans and benefits from deduplication
	if name == "trace:id" {
		return false
	}
	return name == "span:id" || name == "span:parent_id" ||
		strings.HasSuffix(name, ".id") || strings.HasSuffix(name, "_id") || strings.HasSuffix(name, "-id") ||
		strings.HasSuffix(name, ".uid") || strings.HasSuffix(name, "_uid") ||
		strings.Contains(name, "guid") || strings.Contains(name, "uuid") ||
		name == "trace.index" // Sequential index within trace
}

// isURLColumn returns true if the column name indicates it contains URL/path data
// that would benefit from prefix compression
func isURLColumn(name string) bool {
	return strings.Contains(name, "url") || strings.Contains(name, "path") ||
		strings.Contains(name, "uri") || name == "http.target" ||
		strings.HasSuffix(name, ".endpoint") || strings.HasSuffix(name, ".route")
}

// isArrayColumn returns true if the column stores array data
// Array columns have their own encoding and should not use XOR/prefix compression
func isArrayColumn(name string) bool {
	// Intrinsic array columns (these are known array columns)
	switch name {
	case "event:name", "event:time_since_start", "event:dropped_attributes_count",
		"link:trace_id", "link:span_id", "link:trace_state", "link:dropped_attributes_count",
		"instrumentation:name", "instrumentation:version", "instrumentation:dropped_attributes_count":
		return true
	}

	// Array attribute columns - all event/link/instrumentation attributes are arrays
	// (excluding the intrinsic fields listed above)
	if strings.HasPrefix(name, "event.") ||
		strings.HasPrefix(name, "link.") ||
		strings.HasPrefix(name, "instrumentation.") {
		// If it's not one of the intrinsic fields above, it's an attribute array
		return true
	}

	// Legacy check for explicit array markers
	if strings.HasSuffix(name, "-array") {
		return true
	}

	return false
}

type columnBuilder struct {
	name           string
	typ            ColumnType
	stringDict     map[string]uint32
	stringDictVals []string
	stringIndexes  []uint32
	intDict        map[int64]uint32
	intDictVals    []int64
	intIndexes     []uint32
	uintValues     []uint64 // Raw values - encoding decided at build time
	boolDict       map[uint8]uint32
	boolDictVals   []uint8
	boolIndexes    []uint32
	floatDict      map[uint64]uint32
	floatDictVals  []float64
	floatIndexes   []uint32
	bytesDict      map[string]uint32
	bytesDictVals  [][]byte
	bytesIndexes   []uint32
	bytesValues    [][]byte // Raw values for XOR or prefix encoding
	useXOR         bool     // Use XOR encoding for this bytes column
	usePrefix      bool     // Use prefix compression for this bytes column
	present        []byte
	stats          columnStatsBuilder
}

func newColumnBuilder(name string, typ ColumnType, spanCount int) *columnBuilder {
	cb := &columnBuilder{
		name:    name,
		typ:     typ,
		present: make([]byte, bitsLen(spanCount)),
	}
	switch typ {
	case ColumnTypeString:
		cb.stringDict = make(map[string]uint32)
		cb.stringDictVals = make([]string, 0, 16)
		cb.stringIndexes = make([]uint32, spanCount)
	case ColumnTypeInt64:
		cb.intDict = make(map[int64]uint32)
		cb.intDictVals = make([]int64, 0, 16)
		cb.intIndexes = make([]uint32, spanCount)
	case ColumnTypeUint64:
		// Always collect raw values for uint64 columns
		// We'll analyze at build time whether to use delta or dictionary encoding
		cb.uintValues = make([]uint64, spanCount)
	case ColumnTypeBool:
		cb.boolDict = make(map[uint8]uint32)
		cb.boolDictVals = make([]uint8, 0, 2)
		cb.boolIndexes = make([]uint32, spanCount)
	case ColumnTypeFloat64:
		cb.floatDict = make(map[uint64]uint32)
		cb.floatDictVals = make([]float64, 0, 16)
		cb.floatIndexes = make([]uint32, spanCount)
	case ColumnTypeBytes:
		// Use XOR encoding for ID columns, prefix compression for URLs
		// Array columns use dictionary encoding only
		if !isArrayColumn(name) && isIDColumn(name) {
			cb.useXOR = true
			cb.bytesValues = make([][]byte, spanCount)
		} else if !isArrayColumn(name) && isURLColumn(name) {
			cb.usePrefix = true
			cb.bytesValues = make([][]byte, spanCount)
		} else {
			cb.bytesDict = make(map[string]uint32)
			cb.bytesDictVals = make([][]byte, 0, 16)
			cb.bytesIndexes = make([]uint32, spanCount)
		}
	}
	return cb
}
func (cb *columnBuilder) appendNull() {
	cb.ensureCapacity(1)
	switch cb.typ {
	case ColumnTypeString:
		cb.stringIndexes = append(cb.stringIndexes, 0)
	case ColumnTypeInt64:
		cb.intIndexes = append(cb.intIndexes, 0)
	case ColumnTypeUint64:
		cb.uintValues = append(cb.uintValues, 0)
	case ColumnTypeBool:
		cb.boolIndexes = append(cb.boolIndexes, 0)
	case ColumnTypeFloat64:
		cb.floatIndexes = append(cb.floatIndexes, 0)
	case ColumnTypeBytes:
		if cb.useXOR || cb.usePrefix {
			cb.bytesValues = append(cb.bytesValues, nil)
		} else {
			cb.bytesIndexes = append(cb.bytesIndexes, 0)
		}
	}
}

func (cb *columnBuilder) ensureCapacity(additional int) {
	targetRows := cb.length() + additional
	needBits := bitsLen(targetRows)
	if len(cb.present) < needBits {
		grow := make([]byte, needBits)
		copy(grow, cb.present)
		cb.present = grow
	}
}

func (cb *columnBuilder) length() int {
	switch cb.typ {
	case ColumnTypeString:
		return len(cb.stringIndexes)
	case ColumnTypeInt64:
		return len(cb.intIndexes)
	case ColumnTypeUint64:
		return len(cb.uintValues)
	case ColumnTypeBool:
		return len(cb.boolIndexes)
	case ColumnTypeFloat64:
		return len(cb.floatIndexes)
	case ColumnTypeBytes:
		if cb.useXOR || cb.usePrefix {
			return len(cb.bytesValues)
		}
		return len(cb.bytesIndexes)
	default:
		return 0
	}
}

// ensureIndex grows the column to include idx, backfilling missing rows.
func (cb *columnBuilder) ensureIndex(idx int) {
	if idx < cb.length() {
		return
	}
	missing := idx - cb.length() + 1
	cb.appendMissing(missing)
}

// appendMissing extends the index slices by n entries, leaving presence unset.
func (cb *columnBuilder) appendMissing(n int) {
	cb.ensureCapacity(n)
	switch cb.typ {
	case ColumnTypeString:
		cb.stringIndexes = append(cb.stringIndexes, make([]uint32, n)...)
	case ColumnTypeInt64:
		cb.intIndexes = append(cb.intIndexes, make([]uint32, n)...)
	case ColumnTypeUint64:
		cb.uintValues = append(cb.uintValues, make([]uint64, n)...)
	case ColumnTypeBool:
		cb.boolIndexes = append(cb.boolIndexes, make([]uint32, n)...)
	case ColumnTypeFloat64:
		cb.floatIndexes = append(cb.floatIndexes, make([]uint32, n)...)
	case ColumnTypeBytes:
		if cb.useXOR || cb.usePrefix {
			cb.bytesValues = append(cb.bytesValues, make([][]byte, n)...)
		} else {
			cb.bytesIndexes = append(cb.bytesIndexes, make([]uint32, n)...)
		}
	}
}

// buildData encodes the column into its on-disk representation.
// bytes.Buffer writes never return errors (only panic on out-of-memory).
// Error returns are ignored as they cannot occur in practice.
func (cb *columnBuilder) buildData(spanCount int) ([]byte, columnStatsBuilder, error) {
	if cb.length() != spanCount {
		return nil, columnStatsBuilder{}, fmt.Errorf("column %s length mismatch: %d != %d", cb.name, cb.length(), spanCount)
	}
	buf := &bytes.Buffer{}
	presentCount := encodings.CountPresentBits(cb.present, spanCount)
	nullCount := spanCount - presentCount
	useSparse := nullCount > spanCount/2

	_ = buf.WriteByte(columnEncodingVersion)
	presenceRLE := EncodePresenceRLE(cb.present, spanCount)

	var err error
	switch cb.typ {
	case ColumnTypeString:
		err = cb.buildStringData(buf, spanCount, presentCount, useSparse, presenceRLE)
	case ColumnTypeInt64:
		err = cb.buildInt64Data(buf, spanCount, presentCount, useSparse, presenceRLE)
	case ColumnTypeUint64:
		err = cb.buildUint64Data(buf, spanCount, presentCount, useSparse, presenceRLE)
	case ColumnTypeBool:
		err = cb.buildBoolData(buf, spanCount, presentCount, useSparse, presenceRLE)
	case ColumnTypeFloat64:
		err = cb.buildFloat64Data(buf, spanCount, presentCount, useSparse, presenceRLE)
	case ColumnTypeBytes:
		err = cb.buildBytesData(buf, spanCount, presentCount, useSparse, presenceRLE)
	default:
		return nil, columnStatsBuilder{}, fmt.Errorf("unsupported column type %v", cb.typ)
	}

	if err != nil {
		return nil, columnStatsBuilder{}, err
	}
	return buf.Bytes(), cb.stats, nil
}
