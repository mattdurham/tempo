package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// SetBlockMetasForTest replaces the reader's blockMetas slice with the provided
// synthetic entries. For use in white-box tests only.
func SetBlockMetasForTest(r *Reader, metas []shared.BlockMeta) {
	r.blockMetas = metas
}

// SetIntrinsicDecodedForTest injects pre-built IntrinsicColumn objects into the
// reader's decoded cache, bypassing I/O and decoding. For use in white-box tests
// that need to control the exact column contents (e.g. mismatched BlockRefs).
func SetIntrinsicDecodedForTest(r *Reader, cols map[string]*shared.IntrinsicColumn) {
	r.intrinsicDecoded = cols
}

// SetIntrinsicIndexForTest injects a synthetic intrinsic TOC index into the reader,
// making HasIntrinsicSection() return true and enabling GetIntrinsicColumn lookups
// that are resolved from the injected intrinsicDecoded cache.
func SetIntrinsicIndexForTest(r *Reader, index map[string]shared.IntrinsicColMeta) {
	r.intrinsicIndex = index
}

// CompareRangeKeyExported exposes the private compareRangeKey for white-box tests.
func CompareRangeKeyExported(colType shared.ColumnType, a, b string) int {
	return compareRangeKey(colType, a, b)
}

// DecodeInt64KeyExported exposes decodeInt64Key for white-box tests.
func DecodeInt64KeyExported(key string) int64 { return decodeInt64Key(key) }

// DecodeUint64KeyExported exposes decodeUint64Key for white-box tests.
func DecodeUint64KeyExported(key string) uint64 { return decodeUint64Key(key) }

// DecodeFloat64KeyExported exposes decodeFloat64Key for white-box tests.
func DecodeFloat64KeyExported(key string) float64 { return decodeFloat64Key(key) }
