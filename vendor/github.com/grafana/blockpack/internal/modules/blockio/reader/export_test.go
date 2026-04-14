package reader

import (
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/filecache"
)

// NewBareReaderForTest returns a zero-value Reader with no provider or data.
// Useful for white-box tests that need to verify method behavior when fields are
// unset (nil provider, zero compactLen, nil compactParsed, etc.).
func NewBareReaderForTest() *Reader {
	return &Reader{}
}

// NewBareReaderWithBloomForTest returns a zero-value Reader with fileBloomRaw set
// but fileBloomParsed nil, and fileBloomOnce in its zero (unfired) state.
// Calling FileBloom() on this reader exercises the lazy-parse inner block.
func NewBareReaderWithBloomForTest(fileBloomRaw []byte) *Reader {
	return &Reader{fileBloomRaw: fileBloomRaw}
}

// SetSketchIdxNilForTest clears the reader's sketchIdx field to nil after the
// sketch section has already been loaded. Subsequent ColumnSketch calls see
// sketchIdx==nil because the once has already fired.
func SetSketchIdxNilForTest(r *Reader) {
	r.sketchIdx = nil
}

// SetSignalTypeForTest sets the reader's signalType field directly, bypassing
// file parsing. Used to exercise the SignalType() zero-value path.
func SetSignalTypeForTest(r *Reader, st uint8) {
	r.signalType = st
}

// SetRangeParsedEmptyForTest injects an empty parsedRangeIndex for the named column.
// Subsequent BlocksForRange / BlocksForRangeInterval calls see an index with 0 entries.
func SetRangeParsedEmptyForTest(r *Reader, colName string) {
	if r.rangeParsed == nil {
		r.rangeParsed = make(map[string]parsedRangeIndex)
	}
	r.rangeParsed[colName] = parsedRangeIndex{}
}

// SetCompactParsedNilForTest clears the reader's compactParsed field.
// Used to exercise TraceBloomRaw() / MayContainTraceID() nil-compactParsed paths.
func SetCompactParsedNilForTest(r *Reader) {
	r.compactParsed = nil
}

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

// ParseBlockHeaderExported exposes parseBlockHeader for white-box tests.
func ParseBlockHeaderExported(data []byte) error {
	_, err := parseBlockHeader(data)
	return err
}

// ResetColumnExported exposes resetColumn for white-box tests.
func ResetColumnExported(col *Column) { resetColumn(col) }

// OpenFileCacheForTest opens a temporary FileCache for use in tests that need a non-nil
// *filecache.FileCache to exercise the Cache!=nil code paths.
// Returns nil if the cache cannot be opened (non-fatal for tests).
func OpenFileCacheForTest(t *testing.T) *filecache.FileCache {
	t.Helper()
	dir := t.TempDir()
	fc, err := filecache.Open(filecache.Config{Enabled: true, Path: dir, MaxBytes: 64 * 1024 * 1024})
	if err != nil {
		return nil
	}
	return fc
}
