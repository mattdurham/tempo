package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ---- stringColumnBuilder ----

type stringColumnBuilder struct {
	colName string
	stats   stringStats
	values  []string
	present []bool
}

func (b *stringColumnBuilder) addString(val string, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		accumulateStringStats(&b.stats, val)
	}
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

func (b *stringColumnBuilder) colType() shared.ColumnType { return shared.ColumnTypeString }

func (b *stringColumnBuilder) buildStats() []byte { return encodeStringStats(b.stats) }

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

	// Compute cardinality from present values.
	seen := make(map[string]struct{}, nRows)
	for i, v := range b.values {
		if b.present[i] {
			seen[v] = struct{}{}
		}
	}
	cardinality := len(seen)

	if cardinality <= rleCardinalityThreshold {
		if sparse {
			return encodeDictionaryKind(
				KindSparseRLEIndexes, shared.ColumnTypeString, b.values, b.present, nRows, enc,
			)
		}
		return encodeDictionaryKind(
			KindRLEIndexes, shared.ColumnTypeString, b.values, b.present, nRows, enc,
		)
	}

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
	stats   int64Stats
}

func (b *int64ColumnBuilder) addString(_ string, _ bool)   {}
func (b *int64ColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *int64ColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *int64ColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *int64ColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *int64ColumnBuilder) addInt64(val int64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		accumulateInt64Stats(&b.stats, val)
	}
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

func (b *int64ColumnBuilder) buildStats() []byte { return encodeInt64Stats(b.stats) }

func (b *int64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	seen := make(map[int64]struct{}, nRows)
	for i, v := range b.values {
		if b.present[i] {
			seen[v] = struct{}{}
		}
	}
	cardinality := len(seen)

	if cardinality <= rleCardinalityThreshold {
		if sparse {
			return encodeDictionaryKind(
				KindSparseRLEIndexes, shared.ColumnTypeInt64, b.values, b.present, nRows, enc,
			)
		}
		return encodeDictionaryKind(
			KindRLEIndexes, shared.ColumnTypeInt64, b.values, b.present, nRows, enc,
		)
	}

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
	stats   uint64Stats
}

func (b *uint64ColumnBuilder) addString(_ string, _ bool)   {}
func (b *uint64ColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *uint64ColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *uint64ColumnBuilder) addBool(_ bool, _ bool)       {}
func (b *uint64ColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *uint64ColumnBuilder) addUint64(val uint64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		accumulateUint64Stats(&b.stats, val)
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

func (b *uint64ColumnBuilder) buildStats() []byte { return encodeUint64Stats(b.stats) }

func (b *uint64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	// Compute cardinality.
	seen := make(map[uint64]struct{}, nRows)
	for i, v := range b.values {
		if b.present[i] {
			seen[v] = struct{}{}
		}
	}
	cardinality := len(seen)

	if b.stats.hasValues && shouldUseDeltaEncoding(b.stats.min, b.stats.max, cardinality) {
		return encodeDeltaUint64(b.values, b.present, nRows, enc)
	}

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	if cardinality <= rleCardinalityThreshold {
		if sparse {
			return encodeDictionaryKind(
				KindSparseRLEIndexes, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
			)
		}
		return encodeDictionaryKind(
			KindRLEIndexes, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
		)
	}

	if sparse {
		return encodeDictionaryKind(
			KindSparseDictionary, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
		)
	}
	return encodeDictionaryKind(
		KindDictionary, shared.ColumnTypeUint64, b.values, b.present, nRows, enc,
	)
}

// ---- float64ColumnBuilder ----

type float64ColumnBuilder struct {
	values  []float64
	present []bool
	stats   float64Stats
}

func (b *float64ColumnBuilder) addString(_ string, _ bool) {}
func (b *float64ColumnBuilder) addInt64(_ int64, _ bool)   {}
func (b *float64ColumnBuilder) addUint64(_ uint64, _ bool) {}
func (b *float64ColumnBuilder) addBool(_ bool, _ bool)     {}
func (b *float64ColumnBuilder) addBytes(_ []byte, _ bool)  {}

func (b *float64ColumnBuilder) addFloat64(val float64, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		accumulateFloat64Stats(&b.stats, val)
	}
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

func (b *float64ColumnBuilder) buildStats() []byte { return encodeFloat64Stats(b.stats) }

func (b *float64ColumnBuilder) buildData(enc *zstdEncoder) ([]byte, error) {
	nRows := len(b.values)

	nullRatio := 0.0
	if nRows > 0 {
		nullRatio = float64(b.nullCount()) / float64(nRows)
	}
	sparse := nullRatio > sparseNullRatioThreshold

	seen := make(map[float64]struct{}, nRows)
	for i, v := range b.values {
		if b.present[i] {
			seen[v] = struct{}{}
		}
	}
	cardinality := len(seen)

	if cardinality <= rleCardinalityThreshold {
		if sparse {
			return encodeDictionaryKind(
				KindSparseRLEIndexes, shared.ColumnTypeFloat64, b.values, b.present, nRows, enc,
			)
		}
		return encodeDictionaryKind(
			KindRLEIndexes, shared.ColumnTypeFloat64, b.values, b.present, nRows, enc,
		)
	}

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
	stats   boolStats
}

func (b *boolColumnBuilder) addString(_ string, _ bool)   {}
func (b *boolColumnBuilder) addInt64(_ int64, _ bool)     {}
func (b *boolColumnBuilder) addUint64(_ uint64, _ bool)   {}
func (b *boolColumnBuilder) addFloat64(_ float64, _ bool) {}
func (b *boolColumnBuilder) addBytes(_ []byte, _ bool)    {}

func (b *boolColumnBuilder) addBool(val bool, present bool) {
	b.values = append(b.values, val)
	b.present = append(b.present, present)
	if present {
		accumulateBoolStats(&b.stats, val)
	}
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

func (b *boolColumnBuilder) buildStats() []byte { return encodeBoolStats(b.stats) }

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
	stats   bytesStats
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
	if present {
		accumulateBytesStats(&b.stats, val)
	}
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

func (b *bytesColumnBuilder) buildStats() []byte { return encodeBytesStats(b.stats) }

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
