package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ---- stringColumnBuilder ----

type stringColumnBuilder struct {
	colName        string
	values         []string
	present        []bool
	typ            shared.ColumnType // ColumnTypeString or ColumnTypeRangeString; preserved in colType()
	detectedAsUUID bool              // set by buildData when all values are valid UUIDs
}

func (b *stringColumnBuilder) resetForReuse(colName string) {
	clear(b.values)  // zero string headers so GC can collect string data
	clear(b.present) // zero present flags so prepare can extend safely
	b.values = b.values[:0]
	b.present = b.present[:0]
	b.colName = colName
	b.detectedAsUUID = false
}

func (b *stringColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
		clear(b.values) // zero string headers so GC can collect old data
	} else {
		b.values = make([]string, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *stringColumnBuilder) addString(val string, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
}

func (b *stringColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *stringColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *stringColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *stringColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *stringColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *stringColumnBuilder) rowCount() int { return len(b.values) }

func (b *stringColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *stringColumnBuilder) colType() shared.ColumnType {
	if b.detectedAsUUID {
		return shared.ColumnTypeUUID
	}
	return b.typ
}

func (b *stringColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)
	if nRows == 0 {
		return encodeDictionaryKind(KindDictionary, shared.ColumnTypeString, b.values, b.present, 0, enc)
	}

	// UUID auto-detection: convert to bytes column if all sampled values are UUIDs.
	// shouldStoreAsUUID only checks the first uuidSampleCount values for performance;
	// the full scan below may still encounter non-UUID values (e.g. Kubernetes pod names
	// in resource.service.instance.id). On any conversion failure, fall through to standard
	// string encoding rather than returning an error.
	if shouldStoreAsUUID(b.values) {
		byteVals := make([][]byte, len(b.values))
		uuidOK := true
		for i, s := range b.values {
			if !b.present[i] {
				continue
			}
			parsed, err := uuidToBytes(s)
			if err != nil {
				uuidOK = false
				break
			}
			byteVals[i] = parsed[:]
		}
		if uuidOK {
			b.detectedAsUUID = true
			bb := &bytesColumnBuilder{
				values:  byteVals,
				present: b.present,
				colName: b.colName,
			}
			return bb.buildData(enc)
		}
		// Fall through: non-UUID value found in full scan; use string encoding.
	}

	nullRatio := float64(b.nullCount()) / float64(nRows)
	sparse := nullRatio > sparseNullRatioThreshold

	// encodeDictionaryKind auto-upgrades to RLE when dictionary size ≤ rleCardinalityThreshold.
	if sparse {
		return encodeDictionaryKind(
			KindSparseDictionary, shared.ColumnTypeString, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindDictionary, shared.ColumnTypeString, b.values, b.present, nRows, enc,
	)
}

// ---- int64ColumnBuilder ----

type int64ColumnBuilder struct {
	values  []int64
	present []bool
}

func (b *int64ColumnBuilder) resetForReuse(_ string) {
	clear(b.present)
	b.values = b.values[:0]
	b.present = b.present[:0]
}

func (b *int64ColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
	} else {
		b.values = make([]int64, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *int64ColumnBuilder) addString(_ string, _ bool)   {}
func (b *int64ColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *int64ColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *int64ColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *int64ColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *int64ColumnBuilder) addInt64(val int64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
}

func (b *int64ColumnBuilder) rowCount() int { return len(b.values) }

func (b *int64ColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *int64ColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeInt64 }

func (b *int64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	// encodeDictionaryKind auto-upgrades to RLE when dictionary size ≤ rleCardinalityThreshold.
	if sparse {
		return encodeDictionaryKind(
			KindSparseDictionary, shared.ColumnTypeInt64, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindDictionary, shared.ColumnTypeInt64, b.values, b.present, nRows, enc,
	)
}

// ---- uint64ColumnBuilder ----

type uint64ColumnBuilder struct {
	colName string
	values  []uint64
	present []bool
	minVal  uint64
	maxVal  uint64
	hasVals bool
}

func (b *uint64ColumnBuilder) resetForReuse(colName string) {
	clear(b.present)
	b.values = b.values[:0]
	b.present = b.present[:0]
	b.colName = colName
	b.minVal = 0
	b.maxVal = 0
	b.hasVals = false
}

func (b *uint64ColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
	} else {
		b.values = make([]uint64, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *uint64ColumnBuilder) addString(_ string, _ bool)   {}
func (b *uint64ColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *uint64ColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *uint64ColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *uint64ColumnBuilder) addBytes(_ []byte, _ bool)    {}

// trackMinMax updates the running min/max for delta encoding decisions.
// Called from indexed writes where addUint64 (which appends) is not appropriate.
func (b *uint64ColumnBuilder) trackMinMax(val uint64) {
	if !b.hasVals {
		b.minVal = val
		b.maxVal = val
		b.hasVals = true
	} else {
		if val < b.minVal {
			b.minVal = val
		}
		if val > b.maxVal {
			b.maxVal = val
		}
	}
}

func (b *uint64ColumnBuilder) addUint64(val uint64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		if !b.hasVals {
			b.minVal = val
			b.maxVal = val
			b.hasVals = true
		} else {
			if val < b.minVal {
				b.minVal = val
			}
			if val > b.maxVal {
				b.maxVal = val
			}
		}
	}
}

func (b *uint64ColumnBuilder) rowCount() int { return len(b.values) }

func (b *uint64ColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *uint64ColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeUint64 }

func (b *uint64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	// Cheap cardinality estimate for delta encoding decision — no map allocation.
	// shouldUseDeltaEncoding only needs to distinguish ≤2, ≤3, and >3, so we
	// cap at 4 distinct values using a small fixed array.
	if b.hasVals {
		cardinality := cheapCardinalityUint64(b.values, b.present)
		if shouldUseDeltaEncoding(b.minVal, b.maxVal, cardinality) {
			return encodeDeltaUint64(b.values, b.present, nRows, enc)
		}
	}

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	// encodeDictionaryKind auto-upgrades to RLE when dictionary size ≤ rleCardinalityThreshold.
	if sparse {
		return encodeDictionaryKind(
			KindSparseDictionary, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindDictionary, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
	)
}

// cheapCardinalityUint64 counts distinct present values up to a cap of 4.
// This is enough for shouldUseDeltaEncoding which only checks >2 and >3.
func cheapCardinalityUint64(values []uint64, present []bool) int {
	var distinct [4]uint64
	count := 0
	for i, v := range values {
		if !present[i] {
			continue
		}
		found := false
		for j := range count {
			if distinct[j] == v {
				found = true
				break
			}
		}
		if !found {
			if count == 4 {
				return 4
			}
			distinct[count] = v
			count++
		}
	}
	return count
}

// ---- float64ColumnBuilder ----

type float64ColumnBuilder struct {
	values  []float64
	present []bool
}

func (b *float64ColumnBuilder) resetForReuse(_ string) {
	clear(b.present)
	b.values = b.values[:0]
	b.present = b.present[:0]
}

func (b *float64ColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
	} else {
		b.values = make([]float64, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *float64ColumnBuilder) addString(_ string, _ bool) {}
func (b *float64ColumnBuilder) addInt64(_ int64, _ bool)   {}
func (b *float64ColumnBuilder) addUint64(_ uint64, _ bool) {}
func (b *float64ColumnBuilder) addBool(_ bool, _ bool)     {}
func (b *float64ColumnBuilder) addBytes(_ []byte, _ bool)  {}

func (b *float64ColumnBuilder) addFloat64(val float64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
}

func (b *float64ColumnBuilder) rowCount() int { return len(b.values) }

func (b *float64ColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *float64ColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeFloat64 }

func (b *float64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	// encodeDictionaryKind auto-upgrades to RLE when dictionary size ≤ rleCardinalityThreshold.
	if sparse {
		return encodeDictionaryKind(
			KindSparseDictionary, shared.ColumnTypeFloat64, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindDictionary, shared.ColumnTypeFloat64, b.values, b.present, nRows, enc,
	)
}

// ---- boolColumnBuilder ----

type boolColumnBuilder struct {
	values  []bool
	present []bool
}

func (b *boolColumnBuilder) resetForReuse(_ string) {
	clear(b.present)
	b.values = b.values[:0]
	b.present = b.present[:0]
}

func (b *boolColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
	} else {
		b.values = make([]bool, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *boolColumnBuilder) addString(_ string, _ bool)   {}
func (b *boolColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *boolColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *boolColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *boolColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *boolColumnBuilder) addBool(val bool, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
}

func (b *boolColumnBuilder) rowCount() int { return len(b.values) }

func (b *boolColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *boolColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeBool }

func (b *boolColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	// Bool has at most 2 distinct values — always qualifies for RLE.
	if sparse {
		return encodeDictionaryKind(
			KindSparseRLEIndexes, shared.ColumnTypeBool, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindRLEIndexes, shared.ColumnTypeBool, b.values, b.present, nRows, enc,
	)
}

// ---- bytesColumnBuilder ----

type bytesColumnBuilder struct {
	colName string
	values  [][]byte
	present []bool
}

func (b *bytesColumnBuilder) resetForReuse(colName string) {
	clear(b.values)  // zero []byte headers so GC can collect byte data
	clear(b.present) // zero present flags so prepare can extend safely
	b.values = b.values[:0]
	b.present = b.present[:0]
	b.colName = colName
}

func (b *bytesColumnBuilder) prepare(nRows int) {
	if cap(b.values) >= nRows {
		b.values = b.values[:nRows]
		clear(b.values) // zero []byte headers so GC can collect old data
	} else {
		b.values = make([][]byte, nRows)
	}
	if cap(b.present) >= nRows {
		b.present = b.present[:nRows]
		clear(b.present)
	} else {
		b.present = make([]bool, nRows)
	}
}

func (b *bytesColumnBuilder) addString(_ string, _ bool)   {}
func (b *bytesColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *bytesColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *bytesColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *bytesColumnBuilder) addBool(_ bool, _ bool)       {}

func (b *bytesColumnBuilder) addBytes(val []byte, present bool) {
	cp := make([]byte, len(val))
	copy(cp, val)
	b.values = append(b.values, cp)
	b.present = append(b.present, present)
}

func (b *bytesColumnBuilder) rowCount() int { return len(b.values) }

func (b *bytesColumnBuilder) nullCount() int {
	n := 0
	for _, p := range b.present {
		if !p {
			n++
		}
	}
	return n
}

func (b *bytesColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeBytes }

func (b *bytesColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	switch {
	case b.colName == traceIDColumnName:
		if sparse {
			return encodeDeltaDictionaryKind(KindSparseDeltaDictionary, b.values, b.present, nRows, enc)
		}
		return encodeDeltaDictionaryKind(KindDeltaDictionary, b.values, b.present, nRows, enc)

	case isIDColumn(b.colName):
		if sparse {
			return encodeXORBytes(KindSparseXORBytes, b.values, b.present, nRows, enc)
		}
		return encodeXORBytes(KindXORBytes, b.values, b.present, nRows, enc)

	case isURLColumn(b.colName):
		if sparse {
			return encodePrefixBytes(KindSparsePrefixBytes, b.values, b.present, nRows, enc)
		}
		return encodePrefixBytes(KindPrefixBytes, b.values, b.present, nRows, enc)

	default:
		// Array columns and all others: dictionary encoding.
		if sparse {
			return encodeDictionaryKind(
				KindSparseDictionary, shared.ColumnTypeBytes, b.values, b.present, nRows, enc,
			)
		}
		return encodeDictionaryKind(
			KindDictionary, shared.ColumnTypeBytes, b.values, b.present, nRows, enc,
		)
	}
}
